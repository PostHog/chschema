# Materialized View Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `TO`-form materialized views a first-class object in the `internal/loader/hcl` package — parsed from HCL, introspected from a live cluster, dumped back to HCL, diffed, and turned into DDL — the same round-trip tables already have.

**Architecture:** A new `MaterializedViewSpec` type lives as a sibling collection on `DatabaseSpec` (parallel to `Tables`), reusing the existing `ColumnSpec`. MVs do *not* participate in the table inheritance system (`extend`/`abstract`/`patch_table`). Each layer of the package — parse, introspect, dump, diff, sqlgen, CLI render — gets an additive code path mirroring the table path. Out-of-scope MV variants (inner-engine, refreshable) fail with clear errors during introspection.

**Tech Stack:** Go, `hashicorp/hcl/v2` + `gohcl` + `hclwrite`, `github.com/AfterShip/clickhouse-sql-parser` (orian fork, `refactor-visitor` branch), `stretchr/testify`.

**Spec:** `docs/superpowers/specs/2026-05-14-materialized-views-design.md` (committed `fa27b0a`).

**Conventions:** Tests use `testify` (`assert`/`require`). Run unit tests with `go test ./internal/... ./cmd/... -v`. Live tests need `-clickhouse` and `docker compose up -d`. The `formatNode(n chparser.Expr) string` helper in `introspect.go` renders an AST node back to SQL via `PrintVisitor`. `unquoteString` and `strPtr` helpers already exist in the package.

---

### Task 1: `MaterializedViewSpec` type + `DatabaseSpec` wiring

**Files:**
- Modify: `internal/loader/hcl/types.go`
- Modify: `internal/loader/hcl/layers.go:69-87` (`mergeIntoDatabase`)
- Create: `internal/loader/hcl/testdata/materialized_view.hcl`
- Test: `internal/loader/hcl/parser_test.go`

- [ ] **Step 1: Write the fixture file**

Create `internal/loader/hcl/testdata/materialized_view.hcl`:

```hcl
database "posthog" {
  materialized_view "app_metrics_mv" {
    to_table = "default.sharded_app_metrics"
    query    = "SELECT team_id, category FROM default.kafka_app_metrics"
    cluster  = "posthog"
    comment  = "rolls metrics up"
    column "team_id"  { type = "Int64" }
    column "category" { type = "LowCardinality(String)" }
  }
}
```

- [ ] **Step 2: Write the failing test**

Add to `internal/loader/hcl/parser_test.go`:

```go
func TestParseFile_MaterializedView(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "materialized_view.hcl"))
	require.NoError(t, err)

	expected := []DatabaseSpec{
		{
			Name: "posthog",
			MaterializedViews: []MaterializedViewSpec{
				{
					Name:    "app_metrics_mv",
					ToTable: "default.sharded_app_metrics",
					Query:   "SELECT team_id, category FROM default.kafka_app_metrics",
					Cluster: ptr("posthog"),
					Comment: ptr("rolls metrics up"),
					Columns: []ColumnSpec{
						{Name: "team_id", Type: "Int64"},
						{Name: "category", Type: "LowCardinality(String)"},
					},
				},
			},
		},
	}
	assert.Equal(t, expected, dbs)
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/loader/hcl -run TestParseFile_MaterializedView -v`
Expected: FAIL — `undefined: MaterializedViewSpec` and `DatabaseSpec` has no field `MaterializedViews`.

- [ ] **Step 4: Add the type and the `DatabaseSpec` field**

In `internal/loader/hcl/types.go`, add the `MaterializedViews` field to `DatabaseSpec` (after `Patches`):

```go
type DatabaseSpec struct {
	Name string `hcl:"name,label"`

	Cluster *string `hcl:"cluster,optional"`

	Tables            []TableSpec            `hcl:"table,block"`
	Patches           []PatchTableSpec       `hcl:"patch_table,block" diff:"-"`
	MaterializedViews []MaterializedViewSpec `hcl:"materialized_view,block"`
}
```

And add the new type at the end of the file:

```go
// MaterializedViewSpec is a TO-form materialized view: it continuously
// transforms inserts into one table and writes the result into a separate
// destination table. Inner-engine, refreshable, and window views are not
// modeled. MVs do not participate in table inheritance.
type MaterializedViewSpec struct {
	Name    string       `hcl:"name,label"`
	ToTable string       `hcl:"to_table"`         // TO <db.>table target (required)
	Columns []ColumnSpec `hcl:"column,block"`     // explicit destination column list
	Query   string       `hcl:"query"`            // the AS SELECT ... body
	Cluster *string      `hcl:"cluster,optional"` // ON CLUSTER target
	Comment *string      `hcl:"comment,optional"`
}
```

- [ ] **Step 5: Carry MVs across layer merges**

In `internal/loader/hcl/layers.go`, `mergeIntoDatabase` currently merges `Tables` and `Patches` but would silently drop MVs declared in a non-first layer. Add MV carry-over with a duplicate-name guard. Replace the final `return nil` of `mergeIntoDatabase` (line 86) with:

```go
	mvByName := make(map[string]bool, len(target.MaterializedViews))
	for _, mv := range target.MaterializedViews {
		mvByName[mv.Name] = true
	}
	for _, mv := range incoming.MaterializedViews {
		if mvByName[mv.Name] {
			return fmt.Errorf("materialized_view %q redeclared across layers", mv.Name)
		}
		mvByName[mv.Name] = true
		target.MaterializedViews = append(target.MaterializedViews, mv)
	}
	return nil
```

- [ ] **Step 6: Run test to verify it passes**

Run: `go test ./internal/loader/hcl -run TestParseFile_MaterializedView -v`
Expected: PASS

- [ ] **Step 7: Run the full package to confirm no regression**

Run: `go build ./... && go test ./internal/loader/hcl`
Expected: PASS — `gohcl` decodes the new block automatically; `Resolve` mutates in place so MVs survive resolution untouched.

- [ ] **Step 8: Commit**

```bash
git add internal/loader/hcl/types.go internal/loader/hcl/layers.go internal/loader/hcl/testdata/materialized_view.hcl internal/loader/hcl/parser_test.go
git commit -m "feat: add MaterializedViewSpec type to the HCL model

Adds a TO-form materialized view spec as a sibling collection on
DatabaseSpec, parsed by gohcl and carried across layer merges."
```

---

### Task 2: Extract `buildTableFromAST` from `buildTableFromCreateSQL`

This is a pure refactor with no behavior change. It lets `Introspect` parse a `create_table_query` once and dispatch on statement type (Task 4) instead of re-parsing.

**Files:**
- Modify: `internal/loader/hcl/introspect.go:55-128`

- [ ] **Step 1: Refactor — split parsing from AST-walking**

In `internal/loader/hcl/introspect.go`, replace `buildTableFromCreateSQL` (lines 55-128) with a thin parsing wrapper plus an AST-walking function. Keep the wrapper's name and signature so existing unit tests (`introspect_test.go`) keep compiling:

```go
// buildTableFromCreateSQL parses a CREATE TABLE statement and turns it into
// a TableSpec.
func buildTableFromCreateSQL(createSQL string) (TableSpec, error) {
	p := chparser.NewParser(createSQL)
	stmts, err := p.ParseStmts()
	if err != nil {
		return TableSpec{}, fmt.Errorf("parser: %w", err)
	}
	for _, s := range stmts {
		if ct, ok := s.(*chparser.CreateTable); ok {
			return buildTableFromAST(ct)
		}
	}
	return TableSpec{}, errors.New("no CREATE TABLE statement found in create_table_query")
}

// buildTableFromAST walks a parsed CREATE TABLE node into a TableSpec.
func buildTableFromAST(ct *chparser.CreateTable) (TableSpec, error) {
	t := TableSpec{}
	// ... the entire body of the old buildTableFromCreateSQL from
	// "if ct.TableSchema != nil {" through "return t, nil" goes here unchanged.
}
```

Move every line of the old function body that operated on `ct` (from `if ct.TableSchema != nil {` to `return t, nil`) into `buildTableFromAST` verbatim. Do not change any of that logic.

- [ ] **Step 2: Run the introspection tests to verify no behavior change**

Run: `go test ./internal/loader/hcl -run TestBuildTableFromCreateSQL -v`
Expected: PASS — all existing introspection unit tests still pass; the refactor is behavior-preserving.

- [ ] **Step 3: Commit**

```bash
git add internal/loader/hcl/introspect.go
git commit -m "refactor: split buildTableFromAST out of buildTableFromCreateSQL

Lets Introspect parse a create_table_query once and dispatch on the
statement type rather than re-parsing per object kind."
```

---

### Task 3: `buildMaterializedViewFromCreateSQL` + unsupported-variant errors

**Files:**
- Modify: `internal/loader/hcl/introspect.go`
- Test: `internal/loader/hcl/introspect_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `internal/loader/hcl/introspect_test.go`:

```go
func TestBuildMaterializedViewFromCreateSQL_ToForm(t *testing.T) {
	src := "CREATE MATERIALIZED VIEW db.app_metrics_mv TO db.sharded_app_metrics " +
		"(`team_id` Int64, `category` LowCardinality(String)) " +
		"AS SELECT team_id, category FROM db.kafka_app_metrics"

	got, err := buildMaterializedViewFromCreateSQL(src)
	require.NoError(t, err)

	assert.Equal(t, "db.sharded_app_metrics", got.ToTable)
	assert.Equal(t, []ColumnSpec{
		{Name: "team_id", Type: "Int64"},
		{Name: "category", Type: "LowCardinality(String)"},
	}, got.Columns)
	assert.Contains(t, got.Query, "SELECT")
	assert.Contains(t, got.Query, "kafka_app_metrics")
}

func TestBuildMaterializedViewFromCreateSQL_InnerEngineUnsupported(t *testing.T) {
	src := "CREATE MATERIALIZED VIEW db.mv ENGINE = MergeTree ORDER BY id " +
		"AS SELECT id FROM db.src"
	_, err := buildMaterializedViewFromCreateSQL(src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inner-engine")
}

func TestBuildMaterializedViewFromCreateSQL_RefreshableUnsupported(t *testing.T) {
	src := "CREATE MATERIALIZED VIEW db.mv REFRESH EVERY 1 HOUR TO db.dest " +
		"AS SELECT id FROM db.src"
	_, err := buildMaterializedViewFromCreateSQL(src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refreshable")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/loader/hcl -run TestBuildMaterializedViewFromCreateSQL -v`
Expected: FAIL — `undefined: buildMaterializedViewFromCreateSQL`.

- [ ] **Step 3: Implement `buildMaterializedViewFromCreateSQL` + `buildMaterializedViewFromAST`**

Add to `internal/loader/hcl/introspect.go`:

```go
// buildMaterializedViewFromCreateSQL parses a CREATE MATERIALIZED VIEW
// statement and turns it into a MaterializedViewSpec.
func buildMaterializedViewFromCreateSQL(createSQL string) (MaterializedViewSpec, error) {
	p := chparser.NewParser(createSQL)
	stmts, err := p.ParseStmts()
	if err != nil {
		return MaterializedViewSpec{}, fmt.Errorf("parser: %w", err)
	}
	for _, s := range stmts {
		if mv, ok := s.(*chparser.CreateMaterializedView); ok {
			return buildMaterializedViewFromAST(mv)
		}
	}
	return MaterializedViewSpec{}, errors.New("no CREATE MATERIALIZED VIEW statement found")
}

// buildMaterializedViewFromAST walks a parsed CREATE MATERIALIZED VIEW node
// into a MaterializedViewSpec. Only the TO-form is supported; inner-engine
// and refreshable views return a clear "unsupported" error.
func buildMaterializedViewFromAST(mv *chparser.CreateMaterializedView) (MaterializedViewSpec, error) {
	if mv.Refresh != nil {
		return MaterializedViewSpec{}, errors.New("unsupported: refreshable materialized view")
	}
	if mv.Destination == nil {
		return MaterializedViewSpec{}, errors.New("unsupported: inner-engine materialized view (no TO clause)")
	}

	out := MaterializedViewSpec{
		ToTable: mv.Destination.TableIdentifier.String(),
		Query:   formatNode(mv.SubQuery),
	}
	if mv.Destination.TableSchema != nil {
		for _, col := range mv.Destination.TableSchema.Columns {
			if cd, ok := col.(*chparser.ColumnDef); ok {
				out.Columns = append(out.Columns, columnFromAST(cd))
			}
		}
	}
	if mv.OnCluster != nil && mv.OnCluster.Expr != nil {
		out.Cluster = strPtr(formatNode(mv.OnCluster.Expr))
	}
	if mv.Comment != nil {
		out.Comment = strPtr(unquoteString(mv.Comment.Literal))
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/loader/hcl -run TestBuildMaterializedViewFromCreateSQL -v`
Expected: PASS — all three tests pass.

Note: if `formatNode(mv.SubQuery)` is a compile error (`*SubQuery` not assignable to `chparser.Expr`), use `formatNode(mv.SubQuery.Select)` instead — `*SelectQuery` is an `Expr`. If the destination identifier prints unexpectedly, `mv.Destination.TableIdentifier.String()` is the guaranteed accessor.

- [ ] **Step 5: Commit**

```bash
git add internal/loader/hcl/introspect.go internal/loader/hcl/introspect_test.go
git commit -m "feat: introspect TO-form materialized views from create SQL

buildMaterializedViewFromCreateSQL parses a CREATE MATERIALIZED VIEW
into a MaterializedViewSpec. Inner-engine and refreshable variants
return a clear unsupported error."
```

---

### Task 4: Dispatch `Introspect` on statement type

**Files:**
- Modify: `internal/loader/hcl/introspect.go:25-51` (`Introspect`)
- Test: `internal/loader/hcl/introspect_test.go`

- [ ] **Step 1: Write the failing test**

`Introspect` needs a live connection, so unit-test the dispatch with a small fake row scanner. Check whether `introspect_test.go` already has a fake conn/rows helper; if it does, reuse it. Otherwise add this minimal one and the test:

```go
// fakeRows feeds canned (name, create_table_query) pairs to introspectRows
// without a live ClickHouse.
type fakeRows struct {
	data [][2]string
	i    int
}

func (r *fakeRows) Next() bool { r.i++; return r.i <= len(r.data) }
func (r *fakeRows) Scan(d ...any) error {
	*(d[0].(*string)) = r.data[r.i-1][0]
	*(d[1].(*string)) = r.data[r.i-1][1]
	return nil
}
func (r *fakeRows) Err() error   { return nil }
func (r *fakeRows) Close() error { return nil }

func TestIntrospect_DispatchesByStatementType(t *testing.T) {
	rows := &fakeRows{data: [][2]string{
		{"events", "CREATE TABLE db.events (`id` UUID) ENGINE = MergeTree ORDER BY id"},
		{"app_metrics_mv", "CREATE MATERIALIZED VIEW db.app_metrics_mv TO db.dest " +
			"(`team_id` Int64) AS SELECT team_id FROM db.src"},
		{"a_plain_view", "CREATE VIEW db.a_plain_view AS SELECT 1"},
	}}

	db, err := introspectRows(rows, "db")
	require.NoError(t, err)

	require.Len(t, db.Tables, 1)
	assert.Equal(t, "events", db.Tables[0].Name)
	require.Len(t, db.MaterializedViews, 1)
	assert.Equal(t, "app_metrics_mv", db.MaterializedViews[0].Name)
	assert.Equal(t, "db.dest", db.MaterializedViews[0].ToTable)
	// The plain view is skipped, not an error.
}
```

The test targets a new `introspectRows` helper rather than mocking the full `driver.Conn` query path. Adjust the fake's method set to whatever row interface the current `Introspect` body uses if it differs.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/loader/hcl -run TestIntrospect_DispatchesByStatementType -v`
Expected: FAIL — `undefined: introspectRows`.

- [ ] **Step 3: Refactor `Introspect` to dispatch by statement type**

In `internal/loader/hcl/introspect.go`, change `Introspect` so the row loop delegates to a new `introspectRows` helper, and that helper parses each `create_table_query` once and switches on statement type:

```go
func Introspect(ctx context.Context, conn driver.Conn, database string) (*DatabaseSpec, error) {
	const q = `SELECT name, create_table_query
		FROM system.tables
		WHERE database = ? AND NOT is_temporary
		ORDER BY name`
	rows, err := conn.Query(ctx, q, database)
	if err != nil {
		return nil, fmt.Errorf("query system.tables: %w", err)
	}
	defer rows.Close()
	return introspectRows(rows, database)
}

// rowScanner is the subset of driver.Rows introspectRows needs.
type rowScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func introspectRows(rows rowScanner, database string) (*DatabaseSpec, error) {
	db := &DatabaseSpec{Name: database}
	for rows.Next() {
		var name, createSQL string
		if err := rows.Scan(&name, &createSQL); err != nil {
			return nil, fmt.Errorf("scan system.tables: %w", err)
		}
		p := chparser.NewParser(createSQL)
		stmts, err := p.ParseStmts()
		if err != nil {
			return nil, fmt.Errorf("parse create_table_query for %s.%s: %w", database, name, err)
		}
		if len(stmts) == 0 {
			return nil, fmt.Errorf("%s.%s: empty create_table_query", database, name)
		}
		switch s := stmts[0].(type) {
		case *chparser.CreateTable:
			ts, err := buildTableFromAST(s)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", database, name, err)
			}
			ts.Name = name
			db.Tables = append(db.Tables, ts)
		case *chparser.CreateMaterializedView:
			mv, err := buildMaterializedViewFromAST(s)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", database, name, err)
			}
			mv.Name = name
			db.MaterializedViews = append(db.MaterializedViews, mv)
		default:
			// Plain views, dictionaries, and other object kinds are not
			// modeled yet — skip them rather than failing the whole run.
			continue
		}
	}
	return db, rows.Err()
}
```

If the existing `Introspect` row type (`conn.Query` return value) does not structurally satisfy `rowScanner`, adjust `rowScanner`'s method set to match it exactly (the `driver.Rows` from `clickhouse-go` already has `Next`, `Scan`, `Err`, `Close`).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/loader/hcl -run TestIntrospect -v`
Expected: PASS

- [ ] **Step 5: Run the full package**

Run: `go build ./... && go test ./internal/loader/hcl`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/loader/hcl/introspect.go internal/loader/hcl/introspect_test.go
git commit -m "feat: dispatch Introspect on statement type

Introspect now parses each create_table_query once and routes CREATE
TABLE / CREATE MATERIALIZED VIEW to their builders; plain views and
other object kinds are skipped. Fixes Introspect crashing on any
database that contains a materialized view."
```

---

### Task 5: Dump materialized views to HCL

**Files:**
- Modify: `internal/loader/hcl/dump.go:34-46` (`writeDatabase`)
- Test: `internal/loader/hcl/dump_test.go`

- [ ] **Step 1: Write the failing round-trip test**

Add to `internal/loader/hcl/dump_test.go`:

```go
func TestWrite_MaterializedViewRoundTrip(t *testing.T) {
	in := []DatabaseSpec{
		{
			Name: "posthog",
			MaterializedViews: []MaterializedViewSpec{
				{
					Name:    "app_metrics_mv",
					ToTable: "default.sharded_app_metrics",
					Query:   "SELECT team_id, category FROM default.kafka_app_metrics",
					Cluster: ptr("posthog"),
					Comment: ptr("rolls metrics up"),
					Columns: []ColumnSpec{
						{Name: "team_id", Type: "Int64"},
						{Name: "category", Type: "LowCardinality(String)"},
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	require.NoError(t, Write(&buf, in))

	path := filepath.Join(t.TempDir(), "dump.hcl")
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))
	out, err := ParseFile(path)
	require.NoError(t, err)

	assert.Equal(t, in, out)
}
```

Ensure `dump_test.go` imports `bytes`, `os`, `path/filepath` (add any that are missing).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/loader/hcl -run TestWrite_MaterializedViewRoundTrip -v`
Expected: FAIL — the dumped HCL has no `materialized_view` block, so `out` is missing `MaterializedViews`.

- [ ] **Step 3: Implement `writeMaterializedView` and call it from `writeDatabase`**

In `internal/loader/hcl/dump.go`, append MV emission to `writeDatabase` (after the table loop), and add the `writeMaterializedView` function:

```go
func writeDatabase(body *hclwrite.Body, db DatabaseSpec) {
	tables := append([]TableSpec(nil), db.Tables...)
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Name < tables[j].Name
	})
	for i, tbl := range tables {
		if i > 0 {
			body.AppendNewline()
		}
		tblBlock := body.AppendNewBlock("table", []string{tbl.Name})
		writeTable(tblBlock.Body(), tbl)
	}

	mvs := append([]MaterializedViewSpec(nil), db.MaterializedViews...)
	sort.Slice(mvs, func(i, j int) bool { return mvs[i].Name < mvs[j].Name })
	for i, mv := range mvs {
		if len(tables) > 0 || i > 0 {
			body.AppendNewline()
		}
		mvBlock := body.AppendNewBlock("materialized_view", []string{mv.Name})
		writeMaterializedView(mvBlock.Body(), mv)
	}
}

func writeMaterializedView(body *hclwrite.Body, mv MaterializedViewSpec) {
	body.SetAttributeValue("to_table", cty.StringVal(mv.ToTable))
	body.SetAttributeValue("query", cty.StringVal(mv.Query))
	if mv.Cluster != nil {
		body.SetAttributeValue("cluster", cty.StringVal(*mv.Cluster))
	}
	if mv.Comment != nil {
		body.SetAttributeValue("comment", cty.StringVal(*mv.Comment))
	}
	for _, c := range mv.Columns {
		colBlock := body.AppendNewBlock("column", []string{c.Name})
		colBlock.Body().SetAttributeValue("type", cty.StringVal(c.Type))
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/loader/hcl -run TestWrite_MaterializedViewRoundTrip -v`
Expected: PASS

- [ ] **Step 5: Run the full package**

Run: `go test ./internal/loader/hcl`
Expected: PASS — confirm existing dump snapshot tests still pass.

- [ ] **Step 6: Commit**

```bash
git add internal/loader/hcl/dump.go internal/loader/hcl/dump_test.go
git commit -m "feat: dump materialized views as canonical HCL

writeDatabase now emits a materialized_view block per MV, sorted by
name, round-tripping cleanly through ParseFile."
```

---

### Task 6: Diff — `MaterializedViewDiff` type and `DatabaseChange` fields

**Files:**
- Modify: `internal/loader/hcl/diff.go`
- Test: `internal/loader/hcl/diff_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/loader/hcl/diff_test.go`:

```go
func TestMaterializedViewDiff_EmptyAndUnsafe(t *testing.T) {
	var empty MaterializedViewDiff
	assert.True(t, empty.IsEmpty())
	assert.False(t, empty.IsUnsafe())

	q := MaterializedViewDiff{Name: "mv", QueryChange: &StringChange{Old: ptr("a"), New: ptr("b")}}
	assert.False(t, q.IsEmpty())
	assert.False(t, q.IsUnsafe())

	r := MaterializedViewDiff{Name: "mv", Recreate: true}
	assert.False(t, r.IsEmpty())
	assert.True(t, r.IsUnsafe())
}

func TestDatabaseChange_IsEmpty_CoversMVs(t *testing.T) {
	dc := DatabaseChange{Database: "posthog",
		AddMaterializedViews: []MaterializedViewSpec{{Name: "mv"}}}
	assert.False(t, dc.IsEmpty())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/loader/hcl -run 'TestMaterializedViewDiff_EmptyAndUnsafe|TestDatabaseChange_IsEmpty_CoversMVs' -v`
Expected: FAIL — `undefined: MaterializedViewDiff`, and `DatabaseChange` has no MV fields.

- [ ] **Step 3: Add the type and fields**

In `internal/loader/hcl/diff.go`, extend `DatabaseChange` (currently lines 14-20):

```go
type DatabaseChange struct {
	Database    string
	AddTables   []TableSpec
	DropTables  []string
	AlterTables []TableDiff

	AddMaterializedViews   []MaterializedViewSpec // -> CREATE MATERIALIZED VIEW
	DropMaterializedViews  []string               // -> DROP VIEW
	AlterMaterializedViews []MaterializedViewDiff
}
```

Add the diff type (near `TableDiff`):

```go
// MaterializedViewDiff describes a change to a materialized view. ClickHouse
// can apply a pure SELECT-body change in place via ALTER TABLE ... MODIFY
// QUERY; a change to the destination table or the column list needs the MV
// dropped and recreated, which is flagged unsafe.
type MaterializedViewDiff struct {
	Name        string
	QueryChange *StringChange // the AS SELECT body changed
	Recreate    bool          // to_table or the column list changed
}

func (d MaterializedViewDiff) IsEmpty() bool  { return d.QueryChange == nil && !d.Recreate }
func (d MaterializedViewDiff) IsUnsafe() bool { return d.Recreate }
```

Extend `DatabaseChange.IsEmpty` (currently lines 91-93) to cover the new fields:

```go
func (dc DatabaseChange) IsEmpty() bool {
	return len(dc.AddTables) == 0 && len(dc.DropTables) == 0 && len(dc.AlterTables) == 0 &&
		len(dc.AddMaterializedViews) == 0 && len(dc.DropMaterializedViews) == 0 &&
		len(dc.AlterMaterializedViews) == 0
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/loader/hcl -run 'TestMaterializedViewDiff_EmptyAndUnsafe|TestDatabaseChange_IsEmpty_CoversMVs' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/loader/hcl/diff.go internal/loader/hcl/diff_test.go
git commit -m "feat: add MaterializedViewDiff and DatabaseChange MV fields"
```

---

### Task 7: Diff — produce MV add / drop / alter entries

**Files:**
- Modify: `internal/loader/hcl/diff.go` (`Diff` lines 117-146, `diffDatabase` lines 172-199, `sortDatabaseChange` lines 391-395, imports)
- Test: `internal/loader/hcl/diff_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `internal/loader/hcl/diff_test.go`. Use a local helper to keep the cases short:

```go
func mkMV(name, toTable, query string, cols ...ColumnSpec) MaterializedViewSpec {
	return MaterializedViewSpec{Name: name, ToTable: toTable, Query: query, Columns: cols}
}

func TestDiff_MaterializedViews(t *testing.T) {
	base := mkMV("mv", "db.dest", "SELECT a FROM db.src", ColumnSpec{Name: "a", Type: "Int64"})

	t.Run("add", func(t *testing.T) {
		from := []DatabaseSpec{{Name: "db"}}
		to := []DatabaseSpec{{Name: "db", MaterializedViews: []MaterializedViewSpec{base}}}
		cs := Diff(from, to)
		require.Len(t, cs.Databases, 1)
		assert.Equal(t, []MaterializedViewSpec{base}, cs.Databases[0].AddMaterializedViews)
	})

	t.Run("drop", func(t *testing.T) {
		from := []DatabaseSpec{{Name: "db", MaterializedViews: []MaterializedViewSpec{base}}}
		to := []DatabaseSpec{{Name: "db"}}
		cs := Diff(from, to)
		require.Len(t, cs.Databases, 1)
		assert.Equal(t, []string{"mv"}, cs.Databases[0].DropMaterializedViews)
	})

	t.Run("query-only change", func(t *testing.T) {
		changed := base
		changed.Query = "SELECT a FROM db.other_src"
		from := []DatabaseSpec{{Name: "db", MaterializedViews: []MaterializedViewSpec{base}}}
		to := []DatabaseSpec{{Name: "db", MaterializedViews: []MaterializedViewSpec{changed}}}
		cs := Diff(from, to)
		require.Len(t, cs.Databases, 1)
		require.Len(t, cs.Databases[0].AlterMaterializedViews, 1)
		d := cs.Databases[0].AlterMaterializedViews[0]
		assert.False(t, d.Recreate)
		require.NotNil(t, d.QueryChange)
		assert.Equal(t, base.Query, *d.QueryChange.Old)
		assert.Equal(t, changed.Query, *d.QueryChange.New)
	})

	t.Run("to_table change is recreate", func(t *testing.T) {
		changed := base
		changed.ToTable = "db.new_dest"
		from := []DatabaseSpec{{Name: "db", MaterializedViews: []MaterializedViewSpec{base}}}
		to := []DatabaseSpec{{Name: "db", MaterializedViews: []MaterializedViewSpec{changed}}}
		cs := Diff(from, to)
		require.Len(t, cs.Databases, 1)
		require.Len(t, cs.Databases[0].AlterMaterializedViews, 1)
		assert.True(t, cs.Databases[0].AlterMaterializedViews[0].Recreate)
	})

	t.Run("column list change is recreate", func(t *testing.T) {
		changed := base
		changed.Columns = []ColumnSpec{{Name: "a", Type: "Int64"}, {Name: "b", Type: "String"}}
		from := []DatabaseSpec{{Name: "db", MaterializedViews: []MaterializedViewSpec{base}}}
		to := []DatabaseSpec{{Name: "db", MaterializedViews: []MaterializedViewSpec{changed}}}
		cs := Diff(from, to)
		require.Len(t, cs.Databases, 1)
		require.Len(t, cs.Databases[0].AlterMaterializedViews, 1)
		assert.True(t, cs.Databases[0].AlterMaterializedViews[0].Recreate)
	})

	t.Run("identical MVs produce no change", func(t *testing.T) {
		dbs := []DatabaseSpec{{Name: "db", MaterializedViews: []MaterializedViewSpec{base}}}
		assert.True(t, Diff(dbs, dbs).IsEmpty())
	})
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/loader/hcl -run TestDiff_MaterializedViews -v`
Expected: FAIL — `diffDatabase` does not populate the MV fields.

- [ ] **Step 3: Implement MV diffing in `diffDatabase`**

In `internal/loader/hcl/diff.go`, add `"reflect"` to the imports. Add an MV index helper near `indexTables` (lines 201-207):

```go
func indexMaterializedViews(mvs []MaterializedViewSpec) map[string]*MaterializedViewSpec {
	out := make(map[string]*MaterializedViewSpec, len(mvs))
	for i := range mvs {
		out[mvs[i].Name] = &mvs[i]
	}
	return out
}

// diffMaterializedView compares two MVs of the same name. A change to the
// destination table or the column list can't be applied in place, so it is
// flagged Recreate; otherwise a changed SELECT body becomes a QueryChange.
func diffMaterializedView(from, to *MaterializedViewSpec) MaterializedViewDiff {
	d := MaterializedViewDiff{Name: to.Name}
	if from.ToTable != to.ToTable || !reflect.DeepEqual(from.Columns, to.Columns) {
		d.Recreate = true
		return d
	}
	if from.Query != to.Query {
		old, nw := from.Query, to.Query
		d.QueryChange = &StringChange{Old: &old, New: &nw}
	}
	return d
}
```

Extend `diffDatabase` — after the existing table add/drop/alter loops, before `return dc`:

```go
	fromMVs := indexMaterializedViews(from.MaterializedViews)
	toMVs := indexMaterializedViews(to.MaterializedViews)

	for _, n := range sortedKeys(toMVs) {
		if _, ok := fromMVs[n]; !ok {
			dc.AddMaterializedViews = append(dc.AddMaterializedViews, *toMVs[n])
		}
	}
	for _, n := range sortedKeys(fromMVs) {
		if _, ok := toMVs[n]; !ok {
			dc.DropMaterializedViews = append(dc.DropMaterializedViews, n)
		}
	}
	for _, n := range sortedKeys(fromMVs) {
		mv, ok := toMVs[n]
		if !ok {
			continue
		}
		d := diffMaterializedView(fromMVs[n], mv)
		if !d.IsEmpty() {
			dc.AlterMaterializedViews = append(dc.AlterMaterializedViews, d)
		}
	}
```

Extend `sortDatabaseChange` (lines 391-395) to sort the MV slices deterministically:

```go
func sortDatabaseChange(dc *DatabaseChange) {
	sort.Slice(dc.AddTables, func(i, j int) bool { return dc.AddTables[i].Name < dc.AddTables[j].Name })
	sort.Strings(dc.DropTables)
	sort.Slice(dc.AlterTables, func(i, j int) bool { return dc.AlterTables[i].Table < dc.AlterTables[j].Table })
	sort.Slice(dc.AddMaterializedViews, func(i, j int) bool {
		return dc.AddMaterializedViews[i].Name < dc.AddMaterializedViews[j].Name
	})
	sort.Strings(dc.DropMaterializedViews)
	sort.Slice(dc.AlterMaterializedViews, func(i, j int) bool {
		return dc.AlterMaterializedViews[i].Name < dc.AlterMaterializedViews[j].Name
	})
}
```

Finally, fix the whole-database add/remove shortcut in `Diff` (lines 130-139): the `!fOK` branch currently builds a `DatabaseChange` with only `AddTables`, and the `!tOK` branch with only `DropTables`, so a brand-new or fully-removed database would lose its MVs. Replace both branches with calls through `diffDatabase` against an empty side:

```go
		switch {
		case !fOK:
			dc = diffDatabase(name, &DatabaseSpec{Name: name}, t)
		case !tOK:
			dc = diffDatabase(name, f, &DatabaseSpec{Name: name})
		default:
			dc = diffDatabase(name, f, t)
		}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/loader/hcl -run TestDiff -v`
Expected: PASS — the new MV cases and all pre-existing diff tests (including whole-database add/drop) pass.

- [ ] **Step 5: Commit**

```bash
git add internal/loader/hcl/diff.go internal/loader/hcl/diff_test.go
git commit -m "feat: diff materialized views (add/drop/query-change/recreate)

diffDatabase now indexes MVs alongside tables. A SELECT-body change is
a QueryChange; a destination-table or column-list change is flagged
Recreate (unsafe)."
```

---

### Task 8: SQL generation for materialized views

**Files:**
- Modify: `internal/loader/hcl/sqlgen.go` (`GenerateSQL` lines 32-55)
- Test: `internal/loader/hcl/sqlgen_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `internal/loader/hcl/sqlgen_test.go`:

```go
func TestSQLGen_CreateMaterializedView(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AddMaterializedViews: []MaterializedViewSpec{{
			Name:    "app_metrics_mv",
			ToTable: "posthog.sharded_app_metrics",
			Query:   "SELECT team_id FROM posthog.kafka_app_metrics",
			Cluster: ptr("posthog"),
			Columns: []ColumnSpec{{Name: "team_id", Type: "Int64"}},
		}},
	}}}

	out := GenerateSQL(cs)
	require.Len(t, out.Statements, 1)
	assert.Equal(t,
		"CREATE MATERIALIZED VIEW posthog.app_metrics_mv ON CLUSTER posthog "+
			"TO posthog.sharded_app_metrics (`team_id` Int64) "+
			"AS SELECT team_id FROM posthog.kafka_app_metrics",
		out.Statements[0])
	assert.Empty(t, out.Unsafe)
}

func TestSQLGen_ModifyMaterializedViewQuery(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AlterMaterializedViews: []MaterializedViewDiff{{
			Name:        "app_metrics_mv",
			QueryChange: &StringChange{Old: ptr("SELECT 1"), New: ptr("SELECT 2 FROM posthog.src")},
		}},
	}}}

	out := GenerateSQL(cs)
	require.Len(t, out.Statements, 1)
	assert.Equal(t,
		"ALTER TABLE posthog.app_metrics_mv MODIFY QUERY SELECT 2 FROM posthog.src",
		out.Statements[0])
	assert.Empty(t, out.Unsafe)
}

func TestSQLGen_RecreateMaterializedViewIsUnsafe(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database:               "posthog",
		AlterMaterializedViews: []MaterializedViewDiff{{Name: "app_metrics_mv", Recreate: true}},
	}}}

	out := GenerateSQL(cs)
	assert.Empty(t, out.Statements)
	require.Len(t, out.Unsafe, 1)
	assert.Equal(t, "posthog", out.Unsafe[0].Database)
	assert.Equal(t, "app_metrics_mv", out.Unsafe[0].Table)
	assert.Contains(t, out.Unsafe[0].Reason, "recreat")
}

func TestSQLGen_DropMaterializedView(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database:              "posthog",
		DropMaterializedViews: []string{"app_metrics_mv"},
	}}}

	out := GenerateSQL(cs)
	require.Len(t, out.Statements, 1)
	assert.Equal(t, "DROP VIEW posthog.app_metrics_mv", out.Statements[0])
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/loader/hcl -run TestSQLGen_.*MaterializedView -v`
Expected: FAIL — `GenerateSQL` ignores the MV fields.

- [ ] **Step 3: Add the SQL helpers and extend `GenerateSQL`**

In `internal/loader/hcl/sqlgen.go`, add the helpers:

```go
func createMaterializedViewSQL(database string, mv MaterializedViewSpec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE MATERIALIZED VIEW %s.%s", database, mv.Name)
	if mv.Cluster != nil {
		fmt.Fprintf(&b, " ON CLUSTER %s", *mv.Cluster)
	}
	fmt.Fprintf(&b, " TO %s", mv.ToTable)
	if len(mv.Columns) > 0 {
		cols := make([]string, len(mv.Columns))
		for i, c := range mv.Columns {
			cols[i] = columnDefSQL(c)
		}
		fmt.Fprintf(&b, " (%s)", strings.Join(cols, ", "))
	}
	if mv.Comment != nil {
		fmt.Fprintf(&b, " COMMENT %s", quoteString(*mv.Comment))
	}
	fmt.Fprintf(&b, " AS %s", mv.Query)
	return b.String()
}

func modifyQuerySQL(database, mvName, query string) string {
	return fmt.Sprintf("ALTER TABLE %s.%s MODIFY QUERY %s", database, mvName, query)
}

func dropMaterializedViewSQL(database, mvName string) string {
	return fmt.Sprintf("DROP VIEW %s.%s", database, mvName)
}
```

Then rewrite `GenerateSQL` so the statement order is CREATE TABLE, CREATE MATERIALIZED VIEW, ALTER TABLE, ALTER ... MODIFY QUERY, DROP VIEW, DROP TABLE:

```go
func GenerateSQL(cs ChangeSet) GeneratedSQL {
	var out GeneratedSQL
	for _, dc := range cs.Databases {
		for _, t := range dc.AddTables {
			out.Statements = append(out.Statements, createTableSQL(dc.Database, t))
		}
	}
	for _, dc := range cs.Databases {
		for _, mv := range dc.AddMaterializedViews {
			out.Statements = append(out.Statements, createMaterializedViewSQL(dc.Database, mv))
		}
	}
	for _, dc := range cs.Databases {
		for _, td := range dc.AlterTables {
			if td.IsUnsafe() {
				out.Unsafe = append(out.Unsafe, unsafeReasons(dc.Database, td)...)
			}
			if stmt := alterTableSQL(dc.Database, td); stmt != "" {
				out.Statements = append(out.Statements, stmt)
			}
		}
	}
	for _, dc := range cs.Databases {
		for _, mvd := range dc.AlterMaterializedViews {
			if mvd.Recreate {
				out.Unsafe = append(out.Unsafe, UnsafeChange{
					Database: dc.Database,
					Table:    mvd.Name,
					Reason:   "materialized view recreation required (to_table or column list changed)",
				})
				continue
			}
			if mvd.QueryChange != nil && mvd.QueryChange.New != nil {
				out.Statements = append(out.Statements,
					modifyQuerySQL(dc.Database, mvd.Name, *mvd.QueryChange.New))
			}
		}
	}
	for _, dc := range cs.Databases {
		for _, name := range dc.DropMaterializedViews {
			out.Statements = append(out.Statements, dropMaterializedViewSQL(dc.Database, name))
		}
	}
	for _, dc := range cs.Databases {
		for _, name := range dc.DropTables {
			out.Statements = append(out.Statements, dropTableSQL(dc.Database, name))
		}
	}
	return out
}
```

If `columnDefSQL` produces output that doesn't match the expected `` `team_id` Int64 `` (e.g. a trailing space or no backticks), adjust the test's expected string to the actual `columnDefSQL` output for a bare `{Name, Type}` column — `columnDefSQL` is the single source of truth for column rendering and must not be changed here.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/loader/hcl -run TestSQLGen -v`
Expected: PASS — new MV cases and all pre-existing sqlgen tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/loader/hcl/sqlgen.go internal/loader/hcl/sqlgen_test.go
git commit -m "feat: generate DDL for materialized views

GenerateSQL emits CREATE MATERIALIZED VIEW after CREATE TABLE and
DROP VIEW before DROP TABLE. A SELECT-body change becomes ALTER TABLE
... MODIFY QUERY; a recreate-class change is reported as unsafe."
```

---

### Task 9: CLI — render MV changes in `hclexp diff`

**Files:**
- Modify: `cmd/hclexp/hclexp.go` (`renderChangeSet` lines 277-291)
- Test: `cmd/hclexp/hclexp_test.go`

- [ ] **Step 1: Write the failing test**

Add to `cmd/hclexp/hclexp_test.go`:

```go
func TestRenderChangeSet_MaterializedViews(t *testing.T) {
	cs := hclload.ChangeSet{Databases: []hclload.DatabaseChange{{
		Database:              "posthog",
		AddMaterializedViews:  []hclload.MaterializedViewSpec{{Name: "new_mv"}},
		DropMaterializedViews: []string{"old_mv"},
		AlterMaterializedViews: []hclload.MaterializedViewDiff{
			{Name: "query_mv", QueryChange: &hclload.StringChange{
				Old: strptr("SELECT 1"), New: strptr("SELECT 2")}},
			{Name: "recreate_mv", Recreate: true},
		},
	}}}

	var buf bytes.Buffer
	renderChangeSet(&buf, cs)

	want := `database "posthog"
  + materialized_view new_mv
  - materialized_view old_mv
  ~ materialized_view query_mv
      ~ query: SELECT 1 -> SELECT 2
  ~ materialized_view recreate_mv (requires recreate)
`
	require.Equal(t, want, buf.String())
}

func strptr(s string) *string { return &s }
```

If `hclexp_test.go` already declares a `strptr`/`ptr` helper, reuse it and drop the local definition.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/hclexp -run TestRenderChangeSet_MaterializedViews -v`
Expected: FAIL — `renderChangeSet` emits nothing for the MV fields.

- [ ] **Step 3: Extend `renderChangeSet`**

In `cmd/hclexp/hclexp.go`, append MV rendering to `renderChangeSet`, after the `AlterTables` loop and before the closing brace of the database loop:

```go
		for _, mv := range dc.AddMaterializedViews {
			fmt.Fprintf(w, "  + materialized_view %s\n", mv.Name)
		}
		for _, name := range dc.DropMaterializedViews {
			fmt.Fprintf(w, "  - materialized_view %s\n", name)
		}
		for _, mvd := range dc.AlterMaterializedViews {
			if mvd.Recreate {
				fmt.Fprintf(w, "  ~ materialized_view %s (requires recreate)\n", mvd.Name)
				continue
			}
			fmt.Fprintf(w, "  ~ materialized_view %s\n", mvd.Name)
			if c := mvd.QueryChange; c != nil {
				fmt.Fprintf(w, "      ~ query: %s -> %s\n", strOrNone(c.Old), strOrNone(c.New))
			}
		}
```

`strOrNone` is the existing helper used by `renderTableDiff` for `*string` change fields.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/hclexp -run TestRenderChangeSet_MaterializedViews -v`
Expected: PASS

- [ ] **Step 5: Run the CLI package**

Run: `go test ./cmd/hclexp`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add cmd/hclexp/hclexp.go cmd/hclexp/hclexp_test.go
git commit -m "feat: render materialized view changes in hclexp diff"
```

---

### Task 10: Live ClickHouse round-trip test for materialized views

**Files:**
- Create: `internal/loader/hcl/materialized_view_live_test.go`

- [ ] **Step 1: Inspect the existing live-test scaffolding**

Read `internal/loader/hcl/introspect_live_test.go` and `internal/loader/hcl/clickhouse_create_table_live_test.go` to confirm the exact names of: the `-clickhouse` flag variable (`clickhouseLive`), the `testhelpers.RequireClickHouse` / `testhelpers.CreateTestDatabase` helpers, and how SQL is executed (`conn.Exec(ctx, sql)`). Use those names verbatim below.

- [ ] **Step 2: Write the live round-trip test**

Create `internal/loader/hcl/materialized_view_live_test.go`:

```go
package hcl

import (
	"context"
	"fmt"
	"testing"

	"github.com/posthog/chschema/internal/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCHLive_MaterializedViewRoundTrip creates a destination table and a
// TO-form materialized view in a real ClickHouse instance, introspects the
// database back, and asserts the MV round-trips.
func TestCHLive_MaterializedViewRoundTrip(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	runSQL := func(sql string) {
		require.NoError(t, conn.Exec(ctx, sql), "rejected by ClickHouse:\n%s", sql)
	}

	// A source table the MV reads from, a destination table it writes to,
	// then the MV itself.
	runSQL(fmt.Sprintf(
		"CREATE TABLE %s.src (`team_id` Int64, `category` String) ENGINE = MergeTree ORDER BY team_id",
		dbName))
	runSQL(fmt.Sprintf(
		"CREATE TABLE %s.dest (`team_id` Int64, `category` String) ENGINE = MergeTree ORDER BY team_id",
		dbName))
	runSQL(fmt.Sprintf(
		"CREATE MATERIALIZED VIEW %s.metrics_mv TO %s.dest "+
			"(`team_id` Int64, `category` String) "+
			"AS SELECT team_id, category FROM %s.src",
		dbName, dbName, dbName))

	db, err := Introspect(ctx, conn, dbName)
	require.NoError(t, err)

	var got *MaterializedViewSpec
	for i := range db.MaterializedViews {
		if db.MaterializedViews[i].Name == "metrics_mv" {
			got = &db.MaterializedViews[i]
			break
		}
	}
	require.NotNil(t, got, "introspected schema has no materialized_view metrics_mv")

	assert.Equal(t, fmt.Sprintf("%s.dest", dbName), got.ToTable)
	assert.Equal(t, []ColumnSpec{
		{Name: "team_id", Type: "Int64"},
		{Name: "category", Type: "String"},
	}, got.Columns)
	assert.Contains(t, got.Query, "SELECT")
	assert.Contains(t, got.Query, "src")

	// The two real tables still introspect fine alongside the MV.
	assert.Len(t, db.Tables, 2)
}
```

If `testhelpers.CreateTestDatabase` registers its own `t.Cleanup` to drop the database, no extra cleanup is needed; otherwise add a `t.Cleanup` that drops `dbName`. Confirm by reading `internal/testhelpers`.

- [ ] **Step 3: Run the live test**

Run: `docker compose up -d && go test ./internal/loader/hcl -run TestCHLive_MaterializedViewRoundTrip -clickhouse -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add internal/loader/hcl/materialized_view_live_test.go
git commit -m "test: live round-trip for TO-form materialized views"
```

---

### Task 11: Document materialized views in the README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a `materialized_view` section**

In `README.md`, after the "Engine blocks" subsection (line 222) and before "## Layering & inheritance" (line 224), insert:

````markdown
### Materialized views

A `materialized_view` block declares a **TO-form** materialized view — it
continuously transforms inserts and writes the result into a separate
destination table.

```hcl
database "posthog" {
  materialized_view "app_metrics_mv" {
    to_table = "posthog.sharded_app_metrics"
    query    = "SELECT team_id, category FROM posthog.kafka_app_metrics"

    column "team_id"  { type = "Int64" }
    column "category" { type = "LowCardinality(String)" }
  }
}
```

| Attribute  | Required | Meaning |
|------------|----------|---------|
| `to_table` | yes      | destination table the MV writes into (`TO <db.>table`) |
| `query`    | yes      | the `AS SELECT ...` body |
| `column`   | yes      | the destination column list (name + type) |
| `cluster`  | no       | `ON CLUSTER` target |
| `comment`  | no       | view comment |

`hclexp diff` reports a changed `query` as an in-place `ALTER TABLE ...
MODIFY QUERY`; a changed `to_table` or column list is flagged unsafe
because it needs the view dropped and recreated.

**Not supported.** These fail introspection with a clear error rather than
being silently mishandled:

- inner-engine materialized views (`CREATE MATERIALIZED VIEW ... ENGINE = ...`)
- refreshable materialized views (`REFRESH EVERY|AFTER ...`)
- window views
- plain (non-materialized) views — skipped during introspection for now
````

- [ ] **Step 2: Verify the build still passes and commit**

Run: `go build ./...`
Expected: PASS (no code changed, sanity only)

```bash
git add README.md
git commit -m "docs: document the materialized_view HCL block"
```

---

## Verification

After all tasks:

1. **Unit + CLI tests:** `go test ./internal/... ./cmd/... -v` — all pass.
2. **Build:** `go build ./...` — clean.
3. **Live tests:** `docker compose up -d && go test ./internal/loader/hcl -clickhouse -v` — the new `TestCHLive_MaterializedViewRoundTrip` and all pre-existing live tests pass.
4. **Manual round-trip:**
   - `go build -o hclexp ./cmd/hclexp`
   - Against a database containing a `TO`-form MV: `./hclexp introspect -database <db>` emits a `materialized_view` block.
   - Write two HCL files differing only in an MV's `query`; `./hclexp diff -left a.hcl -right b.hcl` reports `~ materialized_view ... / ~ query: ...`, and `-sql` emits `ALTER TABLE ... MODIFY QUERY`.
   - Change the `to_table` instead; `-sql` reports the change under `-- UNSAFE`.
5. **Spec check:** every section of `docs/superpowers/specs/2026-05-14-materialized-views-design.md` (data model, parsing, introspection, dump, diff, sqlgen, CLI, out-of-scope errors, testing) maps to a task above.
