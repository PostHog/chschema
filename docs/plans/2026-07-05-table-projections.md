# Table PROJECTIONs (#87) — model, introspect, dump, diff, sqlgen

Status: design approved 2026-07-05; implementation in progress.
Issue: https://github.com/PostHog/chschema/issues/87

## Problem

`TableSpec` has no projection field. The chparser fork parses
`PROJECTION` clauses in `create_table_query` (`TableProjection` node),
but the introspect AST walker skips them — **live-verified silent
drop**: a table with two projections introspects without error and
without the projections. Same bug class as #108/#109; a dump-and-
recreate would lose them. `createTableSQL` never emits `PROJECTION`;
diff/sqlgen have no add/drop/materialize handling.

## Reference (clickhouse-docs scan)

- `engines/table-engines/mergetree-family/mergetree.md` §Projections:
  `PROJECTION name (SELECT <column list expr> [GROUP BY] [ORDER BY])`,
  implicit FROM parent; GROUP BY ⇒ inner AggregatingMergeTree storage;
  not used by `SELECT … FINAL`.
- `sql-reference/statements/alter/projection.md`: `ADD PROJECTION
  [IF NOT EXISTS] name (SELECT …) [WITH SETTINGS (k = v, …)]`
  (metadata-only, affects new parts), `DROP PROJECTION` (mutation),
  `MATERIALIZE PROJECTION [IN PARTITION …]` (rebuild mutation),
  `CLEAR PROJECTION` (drops files only).
- **INDEX-form projections** (`PROJECTION p INDEX expr TYPE basic`,
  mergetree.md §Projection indexes): newer than our CH 26.3.12 (live
  server rejects the syntax) and not modeled by chparser — any future
  encounter fails loudly at parse (`-allow-raw` escape hatch). Out of
  scope; file a chparser issue when it becomes real.
- **WITH SETTINGS**: also newer than CH 26.3.12 (live ALTER rejects it)
  but chparser models it (`TableProjection.Settings`); we capture and
  re-emit it to avoid a future silent drop. Unit-tested only.
- Clause order in `SHOW CREATE` (live-probed): columns → INDEX →
  CONSTRAINT → PROJECTION. Emission follows it.

## Design (approved)

- `ProjectionSpec{Name, Query, Settings}`; `TableSpec.Projections`
  (`hcl:"projection,block"`). Query canonicalizes via the existing
  `normalizeQuery`/`beautifyNode` pipeline (load + introspect + dump),
  so formatting never diffs as drift.
- Introspect: `*chparser.TableProjection` case in the create walker →
  `projectionFromAST` (query = beautified SELECT, settings map).
- Dump: `projection` blocks after `index` blocks via `setQueryAttribute`.
- Diff: `TableDiff.AddProjections []ProjectionSpec` /
  `DropProjections []string`; changed = drop+add (no MODIFY exists).
- sqlgen: CREATE emits PROJECTION clauses after constraints; diffs emit
  `ALTER TABLE … ADD/DROP PROJECTION`; ADD on an existing table also
  emits `MATERIALIZE PROJECTION` with `Manual: true` (operator-run,
  mirrors MATERIALIZE INDEX).
- sql2hcl: `AlterTableAddProjection`/`AlterTableDropProjection` cases;
  MATERIALIZE/CLEAR stay rejected as data ops (already are).
- Out of scope: INDEX-form projections, CLEAR PROJECTION, IN PARTITION
  materialize, FINAL-interplay validation.

## Implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans
> (inline; established session pattern). Steps use checkboxes.

**Goal:** Projections round-trip HCL ↔ live with loud failures for
unmodeled forms, and diff-driven ADD/DROP + manual MATERIALIZE DDL.

**Tech stack:** Go 1.26, testify, chparser fork (has all AST nodes).

### Global constraints

- go.mod module name/version untouchable; testify; whole-struct
  asserts; no restating comments; gofmt -s + vet clean.
- Reused helpers (exact anchors): walker case list
  `introspect.go:255` (`*chparser.TableIndex`), `indexFromAST`
  `introspect.go:498`, `beautifyNode` `query_normalize.go:40`,
  `normalizeQuery` loops `query_normalize.go:17,25`,
  `engineSettingsMap` `introspect.go` (SettingsClause → map),
  `setQueryAttribute` `dump.go:158`, index dump loop `dump.go:229`,
  diff index loop `diff.go:749-765` (`indexIndexes`, `sortedKeys`,
  modify = drop+add), TableDiff fields `diff.go:116`, isEmpty
  `diff.go:274`, CREATE parts loop `sqlgen.go:658`, ALTER ops list
  `sqlgen.go:763-767`, manual-materialize block `sqlgen.go:136-145`,
  `materializeIndexSQL` `sqlgen.go:741`, sql_edit index cases
  `sql_edit.go:181,193`. Line numbers drift as tasks land — re-anchor
  by symbol, not number.
- Tests: `go test ./internal/loader/hcl -run <name> -v`; suites
  `go test ./cmd/... ./internal/...`, `go test ./test`, live
  `go test ./test -v -clickhouse` (compose CH is up).

---

### Task 1: model + load-side normalization

**Files:**
- Modify: `internal/loader/hcl/types.go` (ProjectionSpec after
  IndexSpec ~:190; `Projections` field after `Indexes` ~:142)
- Modify: `internal/loader/hcl/query_normalize.go` (normalize loop)
- Create: `internal/loader/hcl/projections_test.go`

**Interfaces:**
- Produces: `ProjectionSpec{Name string; Query string; Settings map[string]string}`
  with hcl tags `name,label` / `query` / `settings,optional`;
  `TableSpec.Projections []ProjectionSpec` (`hcl:"projection,block"`).
  All later tasks consume these exact names.

- [ ] **Step 1: failing test** — `projections_test.go`:

```go
package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Projection queries must normalize to the beautified canonical form on
// load, exactly like view/MV queries, so formatting differences never
// surface as drift.
func TestLoad_ProjectionBlocks_NormalizeQuery(t *testing.T) {
	src := `
database "db" {
  table "events" {
    column "id" { type = "UInt64" }
    column "user_id" { type = "UInt64" }
    projection "by_user" {
      query = "select * order by user_id"
    }
    projection "daily" {
      query    = "SELECT user_id, count() GROUP BY user_id"
      settings = { index_granularity = "4096" }
    }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`
	s := parseHCLString(t, src)
	require.Len(t, s.Databases, 1)
	require.Len(t, s.Databases[0].Tables, 1)
	ps := s.Databases[0].Tables[0].Projections
	require.Len(t, ps, 2)
	assert.Equal(t, "by_user", ps[0].Name)
	// Canonical (beautified) form — keyword case + layout fixed.
	assert.Contains(t, ps[0].Query, "SELECT *")
	assert.Contains(t, ps[0].Query, "ORDER BY user_id")
	assert.Equal(t, map[string]string{"index_granularity": "4096"}, ps[1].Settings)
}
```

If no `parseHCLString` test helper exists, use the package's existing
single-string parse helper (grep `func parse.*string` in `*_test.go`);
adjust the call only — assertions stay.

- [ ] **Step 2: run — FAIL** (unknown block type `projection`):
`go test ./internal/loader/hcl -run TestLoad_ProjectionBlocks -v`

- [ ] **Step 3: implement.** types.go after IndexSpec:

```go
// ProjectionSpec is a table PROJECTION: a named SELECT (implicit FROM
// the parent table) materialized per-part. Settings maps to the newer
// `WITH SETTINGS (…)` clause (projection-level MergeTree settings).
type ProjectionSpec struct {
	Name     string            `hcl:"name,label"`
	Query    string            `hcl:"query"`
	Settings map[string]string `hcl:"settings,optional"`
}
```

TableSpec blocks:

```go
	Columns     []ColumnSpec     `hcl:"column,block"`
	Indexes     []IndexSpec      `hcl:"index,block"`
	Projections []ProjectionSpec `hcl:"projection,block"`
	Constraints []ConstraintSpec `hcl:"constraint,block"`
```

query_normalize.go — extend the existing per-database normalize loop
(the one covering Views at :17 and MaterializedViews at :25) with:

```go
	for ti := range db.Tables {
		for pi := range db.Tables[ti].Projections {
			if q, ok := normalizeQuery(db.Tables[ti].Projections[pi].Query); ok {
				db.Tables[ti].Projections[pi].Query = q
			}
		}
	}
```

Note: `normalizeQuery` parses a full statement; a projection query
(`SELECT * ORDER BY x`, no FROM) is valid ClickHouse SELECT syntax and
parses. If it returns ok=false for any form, keep verbatim (existing
contract: unparseable queries stay as written).

- [ ] **Step 4: run — PASS**, same command.
- [ ] **Step 5: commit** — `hcl: model projection blocks on tables`.

---

### Task 2: introspect — TableProjection capture

**Files:**
- Modify: `internal/loader/hcl/introspect.go` (walker case at the
  `*chparser.TableIndex` switch; new `projectionFromAST` near
  `indexFromAST`)
- Modify: `internal/loader/hcl/projections_test.go`

**Interfaces:**
- Consumes: `ProjectionSpec` (Task 1).
- Produces: `projectionFromAST(p *chparser.TableProjection) ProjectionSpec`.

- [ ] **Step 1: failing tests** (append to projections_test.go):

```go
// The live probe that exposed #87's silent drop, as a regression test:
// both projection forms must be captured, not skipped.
func TestIntrospect_Projections(t *testing.T) {
	rows := &fakeRows{rows: []fakeRow{{
		name: "t",
		sql: "CREATE TABLE db.t (`id` UInt64, `user_id` UInt64, `ts` DateTime, " +
			"PROJECTION by_user (SELECT * ORDER BY user_id), " +
			"PROJECTION daily_agg (SELECT user_id, toDate(ts) AS d, count() GROUP BY user_id, d)) " +
			"ENGINE = MergeTree ORDER BY (id, ts)",
	}}}
	db := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRows(db, "db", rows))
	require.Len(t, db.Tables, 1)
	ps := db.Tables[0].Projections
	require.Len(t, ps, 2)
	assert.Equal(t, "by_user", ps[0].Name)
	assert.Contains(t, ps[0].Query, "SELECT *")
	assert.Contains(t, ps[0].Query, "ORDER BY user_id")
	assert.Equal(t, "daily_agg", ps[1].Name)
	assert.Contains(t, ps[1].Query, "GROUP BY")
	assert.NotContains(t, ps[0].Query, "(SELECT", "outer parens must be stripped")
}
```

- [ ] **Step 2: run — FAIL** (Projections empty):
`go test ./internal/loader/hcl -run TestIntrospect_Projections -v`

- [ ] **Step 3: implement.** In the create-walker switch (sibling of
`case *chparser.TableIndex:`):

```go
		case *chparser.TableProjection:
			t.Projections = append(t.Projections, projectionFromAST(x))
```

(match the switch's actual variable names at the anchor). Near
`indexFromAST`:

```go
// projectionFromAST converts a CREATE-level PROJECTION clause. The
// SELECT body is stored in beautified canonical form without the
// surrounding parentheses; the optional WITH SETTINGS clause maps to
// the Settings attribute.
func projectionFromAST(p *chparser.TableProjection) ProjectionSpec {
	out := ProjectionSpec{Name: formatNode(p.Identifier)}
	q := strings.TrimSpace(beautifyNode(p.Select))
	if strings.HasPrefix(q, "(") && strings.HasSuffix(q, ")") {
		q = strings.TrimSpace(q[1 : len(q)-1])
	}
	out.Query = q
	if p.Settings != nil {
		out.Settings = engineSettingsMap(p.Settings)
	}
	return out
}
```

If `beautifyNode(p.Select)` renders without parens, the trim is a
no-op — the test's `NotContains "(SELECT"` pins the contract either way.

- [ ] **Step 4: run — PASS.**
- [ ] **Step 5: commit** — `introspect: capture table PROJECTION clauses (#87)`.

---

### Task 3: dump — projection blocks

**Files:**
- Modify: `internal/loader/hcl/dump.go` (after the `t.Indexes` loop at
  the `range t.Indexes` anchor)
- Modify: `internal/loader/hcl/testdata/engines_all_kinds.hcl`
- Modify: `internal/loader/hcl/projections_test.go`

- [ ] **Step 1: fixture + failing round-trip.** engines_all_kinds.hcl,
new table after `t_distributed_policy`:

```hcl
  table "t_projected" {
    column "id" { type = "UInt64" }
    column "region" { type = "String" }
    projection "by_region" {
      query = "SELECT id, region ORDER BY region"
    }
    engine "merge_tree" {}
    order_by = ["id"]
  }
```

Run: `go test ./internal/loader/hcl -run TestWrite_RoundTrip -v`
Expected: FAIL — dumped HCL lacks the projection block, reparse loses it.

- [ ] **Step 2: implement.** dump.go after the index loop:

```go
	for _, p := range t.Projections {
		pb := body.AppendNewBlock("projection", []string{p.Name}).Body()
		setQueryAttribute(pb, p.Query)
		if len(p.Settings) > 0 {
			pb.SetAttributeValue("settings", stringMap(p.Settings))
		}
	}
```

(match the surrounding builder variable names; `stringMap` is whatever
helper the settings attr uses elsewhere in dump.go — same call as the
table-level `settings` emission.)

- [ ] **Step 3: run — PASS** (round-trip), plus
`go test ./internal/loader/hcl -run TestLoad_ProjectionBlocks -v`.
- [ ] **Step 4: commit** — `dump: emit projection blocks`.

---

### Task 4: diff — Add/DropProjections

**Files:**
- Modify: `internal/loader/hcl/diff.go` (TableDiff fields ~:116,
  isEmpty ~:274, compare after the index loop ~:765)
- Modify: `internal/loader/hcl/projections_test.go`

**Interfaces:**
- Produces: `TableDiff.AddProjections []ProjectionSpec`,
  `TableDiff.DropProjections []string` (Task 5 consumes).

- [ ] **Step 1: failing tests:**

```go
func TestDiff_Projections_AddDropModify(t *testing.T) {
	base := func(ps ...ProjectionSpec) *Schema {
		return &Schema{Databases: []DatabaseSpec{{
			Name: "db",
			Tables: []TableSpec{{
				Name:        "t",
				OrderBy:     []string{"id"},
				Columns:     []ColumnSpec{{Name: "id", Type: "UInt64"}},
				Projections: ps,
				Engine:      &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
			}},
		}}}
	}
	pA := ProjectionSpec{Name: "p", Query: "SELECT id ORDER BY id"}
	pB := ProjectionSpec{Name: "p", Query: "SELECT id, count() GROUP BY id"}

	d := DiffSchemas(base(), base(pA))
	require.Len(t, d.TableDiffs, 1)
	assert.Equal(t, []ProjectionSpec{pA}, d.TableDiffs[0].AddProjections)
	assert.Empty(t, d.TableDiffs[0].DropProjections)

	d = DiffSchemas(base(pA), base())
	require.Len(t, d.TableDiffs, 1)
	assert.Equal(t, []string{"p"}, d.TableDiffs[0].DropProjections)

	// Changed query ⇒ drop+add (no ALTER MODIFY PROJECTION exists).
	d = DiffSchemas(base(pA), base(pB))
	require.Len(t, d.TableDiffs, 1)
	assert.Equal(t, []string{"p"}, d.TableDiffs[0].DropProjections)
	assert.Equal(t, []ProjectionSpec{pB}, d.TableDiffs[0].AddProjections)

	// Identical projections ⇒ no diff at all.
	d = DiffSchemas(base(pA), base(pA))
	assert.Empty(t, d.TableDiffs)
}
```

Adjust the two entry-point details to the package's actual API before
running: the diff entry function (`DiffSchemas` here) and the table-diff
list field (`TableDiffs`) — copy the names used by existing tests in
`diff_test.go`. Assertions stay as written.

- [ ] **Step 2: run — FAIL** (unknown fields):
`go test ./internal/loader/hcl -run TestDiff_Projections -v`

- [ ] **Step 3: implement.** TableDiff (next to AddIndexes/DropIndexes):

```go
	AddProjections  []ProjectionSpec
	DropProjections []string
```

isEmpty gains `len(td.AddProjections) == 0 && len(td.DropProjections) == 0 &&`.
After the index compare loops, clone the same shape:

```go
	fromProj := indexProjections(from.Projections)
	toProj := indexProjections(to.Projections)
	for _, n := range sortedKeys(toProj) {
		f, ok := fromProj[n]
		if !ok {
			td.AddProjections = append(td.AddProjections, *toProj[n])
			continue
		}
		if !reflect.DeepEqual(*f, *toProj[n]) {
			td.DropProjections = append(td.DropProjections, n)
			td.AddProjections = append(td.AddProjections, *toProj[n])
		}
	}
	for _, n := range sortedKeys(fromProj) {
		if _, ok := toProj[n]; !ok {
			td.DropProjections = append(td.DropProjections, n)
		}
	}
```

with, next to `indexIndexes`:

```go
func indexProjections(ps []ProjectionSpec) map[string]*ProjectionSpec {
	out := make(map[string]*ProjectionSpec, len(ps))
	for i := range ps {
		out[ps[i].Name] = &ps[i]
	}
	return out
}
```

- [ ] **Step 4: run — PASS.**
- [ ] **Step 5: commit** — `diff: cover table projections (add/drop, modify = drop+add)`.

---

### Task 5: sqlgen — CREATE clause, ALTER ops, manual MATERIALIZE

**Files:**
- Modify: `internal/loader/hcl/sqlgen.go` (CREATE parts after
  constraints; ALTER ops next to DROP/ADD INDEX; manual-materialize
  block next to the AddIndexes one; `materializeProjectionSQL` next to
  `materializeIndexSQL`)
- Modify: `internal/loader/hcl/projections_test.go`

**Interfaces:**
- Consumes: `AddProjections`/`DropProjections` (Task 4).
- Produces: `projectionClause(p ProjectionSpec) string`,
  `materializeProjectionSQL(database, table, name string) string`.

- [ ] **Step 1: failing tests:**

```go
func TestSQLGen_ProjectionClause(t *testing.T) {
	assert.Equal(t,
		"PROJECTION by_region (SELECT id, region ORDER BY region)",
		projectionClause(ProjectionSpec{Name: "by_region", Query: "SELECT id, region ORDER BY region"}))
	assert.Equal(t,
		"PROJECTION p (SELECT x ORDER BY x) WITH SETTINGS (index_granularity = 4096)",
		projectionClause(ProjectionSpec{
			Name:     "p",
			Query:    "SELECT x ORDER BY x",
			Settings: map[string]string{"index_granularity": "4096"},
		}))
}

func TestSQLGen_MaterializeProjectionSQL(t *testing.T) {
	assert.Equal(t, "ALTER TABLE db.t MATERIALIZE PROJECTION p",
		materializeProjectionSQL("db", "t", "p"))
}
```

Plus an end-to-end diff→SQL assertion: reuse the package's existing
diff-to-DDL test harness (the one exercising AddIndexes ⇒
`ADD INDEX` + manual `MATERIALIZE INDEX` around `sqlgen.go:136`; copy
that test's shape from sqlgen_test.go) asserting, for an
AddProjections diff on an existing table:
- one op `ALTER TABLE db.t ADD PROJECTION p (SELECT id ORDER BY id)`
  with `Manual == false`
- one op `ALTER TABLE db.t MATERIALIZE PROJECTION p` with
  `Manual == true`
and for DropProjections: `ALTER TABLE db.t DROP PROJECTION p`.

- [ ] **Step 2: run — FAIL** (undefined functions).
- [ ] **Step 3: implement.**

```go
// projectionClause renders the CREATE/ALTER body of one projection.
// The query is embedded verbatim (canonical form); settings render as
// the newer WITH SETTINGS clause, keys sorted for determinism.
func projectionClause(p ProjectionSpec) string {
	s := fmt.Sprintf("PROJECTION %s (%s)", p.Name, p.Query)
	if len(p.Settings) > 0 {
		keys := make([]string, 0, len(p.Settings))
		for k := range p.Settings {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		pairs := make([]string, 0, len(keys))
		for _, k := range keys {
			pairs = append(pairs, fmt.Sprintf("%s = %s", k, p.Settings[k]))
		}
		s += fmt.Sprintf(" WITH SETTINGS (%s)", strings.Join(pairs, ", "))
	}
	return s
}

func materializeProjectionSQL(database, table, projection string) string {
	return fmt.Sprintf("ALTER TABLE %s.%s MATERIALIZE PROJECTION %s", database, table, projection)
}
```

CREATE parts (after the constraints loop, per live SHOW CREATE order
columns → INDEX → CONSTRAINT → PROJECTION):

```go
	for _, p := range t.Projections {
		parts = append(parts, fmt.Sprintf("  %s", projectionClause(p)))
	}
```

ALTER ops (next to DROP/ADD INDEX; drops before adds so a modified
projection re-adds cleanly):

```go
	for _, n := range td.DropProjections {
		ops = append(ops, fmt.Sprintf("DROP PROJECTION %s", n))
	}
	for _, p := range td.AddProjections {
		ops = append(ops, fmt.Sprintf("ADD PROJECTION %s", projectionClause(p)))
	}
```

Manual-materialize (clone the AddIndexes block at the `sqlgen.go:136`
anchor, same Ops plumbing):

```go
	for _, p := range td.AddProjections {
		out = append(out, Op{
			SQL:    materializeProjectionSQL(db, td.Name, p.Name),
			Manual: true,
		})
	}
```

(match the real Op struct/appends of the neighboring MATERIALIZE INDEX
code — copy its exact shape including any comment-marker fields.)

- [ ] **Step 4: run — PASS**; also `go test ./test` (snapshot suite —
no fixture has projections yet, so no snapshot churn expected; if any
snapshot changes, inspect before accepting).
- [ ] **Step 5: commit** — `sqlgen: emit PROJECTION clauses, ADD/DROP, manual MATERIALIZE`.

---

### Task 6: sql2hcl, live regression, docs, sweep, PR

**Files:**
- Modify: `internal/loader/hcl/sql_edit.go` (cases next to
  `AlterTableAddIndex`/`AlterTableDropIndex`)
- Modify: `internal/loader/hcl/projections_test.go` (sql2hcl cases)
- Modify: `test/hcl_introspect_live_test.go`
- Modify: `docs/README.hcl.md`, `CLAUDE.md`

- [ ] **Step 1: sql_edit failing test:**

```go
func TestApplySQL_AddDropProjection(t *testing.T) {
	left := `
database "db" {
  table "t" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`
	out := applySQLString(t, left, "ALTER TABLE db.t ADD PROJECTION p (SELECT id ORDER BY id)")
	require.Len(t, out.Databases[0].Tables[0].Projections, 1)
	assert.Equal(t, "p", out.Databases[0].Tables[0].Projections[0].Name)

	out2 := applySQLString(t, dumpToString(t, out), "ALTER TABLE db.t DROP PROJECTION p")
	assert.Empty(t, out2.Databases[0].Tables[0].Projections)
}
```

(`applySQLString`/`dumpToString`: use sql_edit's existing test helpers —
copy the call pattern from the neighboring `ALTER … ADD INDEX` test in
the sql_edit/sql2hcl test file; assertions stay.)

- [ ] **Step 2: implement** — sibling cases in the alter switch:

```go
	case *chparser.AlterTableAddProjection:
		t.Projections = append(t.Projections, projectionFromAST(x.TableProjection))
	case *chparser.AlterTableDropProjection:
		name := formatNode(x.ProjectionName)
		out := t.Projections[:0]
		for _, p := range t.Projections {
			if p.Name != name {
				out = append(out, p)
			}
		}
		t.Projections = out
```

(field names per the parser AST: check `AlterTableAddProjection`'s
embedded projection field and `AlterTableDropProjection.ProjectionName`
in the module source; mirror the neighboring index cases' IF EXISTS /
missing-name error handling exactly.) Confirm MATERIALIZE/CLEAR
PROJECTION still route to the existing data-op rejection (sql_edit.go
header comment lists them; add a rejection test if none exists).

- [ ] **Step 3: live regression** — in `TestLive_HCLIntrospect`, add to
stmts:

```go
		`CREATE TABLE ` + dbName + `.projected (
			id UInt64,
			user_id UInt64,
			PROJECTION by_user (SELECT * ORDER BY user_id)
		) ENGINE = MergeTree ORDER BY id`,
```

and the expected TableSpec (alphabetical position: after `events`,
before `soft_deleted`):

```go
			{
				Name:    "projected",
				OrderBy: []string{"id"},
				Settings: map[string]string{"index_granularity": "8192"},
				Columns: []hclload.ColumnSpec{
					{Name: "id", Type: "UInt64"},
					{Name: "user_id", Type: "UInt64"},
				},
				Projections: []hclload.ProjectionSpec{
					{Name: "by_user", Query: "SELECT *\nORDER BY user_id"},
				},
				Engine: &hclload.EngineSpec{Kind: "merge_tree", Decoded: hclload.EngineMergeTree{}},
			},
```

The exact canonical Query string (beautified newlines) will come from
the first live run — pin whatever `beautifyNode` produces, then assert
equality; do not loosen to Contains.

Run: `go test ./test -v -clickhouse -run TestLive_HCLIntrospect`

- [ ] **Step 4: docs.** README.hcl.md — document the `projection` block
next to the `index` block section (name label, `query` (one-liner,
heredoc, or `file()`), optional `settings` → `WITH SETTINGS`, note:
add-on-existing-table generates a manual MATERIALIZE PROJECTION).
CLAUDE.md — feature bullet under table blocks: `projection` blocks;
MATERIALIZE PROJECTION is manual like MATERIALIZE INDEX; note INDEX-form
projections unsupported (loud parse failure).

- [ ] **Step 5: sweep** — `go test ./cmd/... ./internal/...`,
`go test ./test`, `go test ./test -v -clickhouse`, gofmt -s check,
`go vet ./...`, `just build` + manual end-to-end: create a projected
table live, `./hclexp introspect` shows the projection block,
`./hclexp diff -sql` from an HCL adding a projection emits ADD +
`-- MANUAL:` MATERIALIZE.

- [ ] **Step 6: commit + push + PR** — `Fixes #87`; PR body: silent-drop
proof, docs-scan scope table (INDEX-form/WITH SETTINGS server-support
notes), test evidence. No AI attribution anywhere.
