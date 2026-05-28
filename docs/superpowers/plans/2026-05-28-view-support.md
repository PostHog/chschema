# Plain Views Support — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `hclexp` round-trip plain (non-materialized) ClickHouse views end-to-end: HCL parsing, introspection, diff, sqlgen, validation, and live tests.

**Architecture:** Add `ViewSpec` (sibling to `MaterializedViewSpec`) and `DatabaseSpec.Views`. Wire the new type into all six pipelines that already handle MVs: HCL decode/encode, introspect, render, diff, sqlgen, validate. Surface DEFINER + SQL SECURITY + column aliases per the spec. Body-only changes diff as `ALTER TABLE ... MODIFY QUERY`; shape changes (aliases, security, cluster) recreate via DROP + CREATE.

**Tech Stack:** Go 1.26, `hashicorp/hcl/v2` (gohcl + hclwrite), `cty`, `clickhouse-sql-parser` fork (chparser), testify, ClickHouse 26.x (live tests).

**Spec:** [`docs/superpowers/specs/2026-05-28-view-support.md`](../specs/2026-05-28-view-support.md)

**File Map:**

| File | What changes |
|---|---|
| `internal/loader/hcl/types.go` | New `ViewSpec`; new `DatabaseSpec.Views` field. |
| `internal/loader/hcl/layers.go` | Cross-layer redeclare check for views (mirror MV). |
| `internal/loader/hcl/parser.go` / `resolver.go` | Verify HCL parse passes through `view` blocks (gohcl auto-handles); add resolver pass-through. |
| `internal/loader/hcl/dump.go` | `writeView`; emit `view "name" { … }` blocks in canonical order. |
| `internal/loader/hcl/introspect.go` | Replace skip-view arm with `buildViewFromCreateView`; reject Live/Refresh/Window. |
| `internal/loader/hcl/diff.go` | `ViewDiff`; `DatabaseChange.{AddViews,DropViews,AlterViews}`; `diffView`; iteration. |
| `internal/loader/hcl/sqlgen.go` | CREATE / DROP / MODIFY QUERY / MODIFY COMMENT for views. |
| `internal/loader/hcl/validate.go` | Iterate `db.Views`; parse query; require sources. |
| `cmd/hclexp/hclexp.go` | Render `+/-/~ view` lines in change-set output. |
| `docs/README.hcl.md` | New `### Views` subsection; move from "Not Yet Supported" list. |
| `README.md` | Mention views in the introspection coverage paragraph. |
| `CLAUDE.md` | Flip Supported Features bullet from ❌ to ✅. |

---

## PR — Plain views support

### Task 1: ViewSpec + DatabaseSpec.Views

**Files:**
- Modify: `internal/loader/hcl/types.go`

- [ ] **Step 1: Add the type and the database field**

Edit `internal/loader/hcl/types.go`. After the `MaterializedViews` field on `DatabaseSpec`, add:

```go
	// Views are plain (non-materialized) views. Like MVs and dictionaries
	// they live as a sibling collection and do not participate in the
	// table inheritance system.
	Views []ViewSpec `hcl:"view,block"`
```

After the `MaterializedViewSpec` type, add:

```go
// ViewSpec models a ClickHouse plain (non-materialized) view: a saved
// SELECT executed on every read of the view. The Query is stored
// verbatim as text. Live views, refreshable materialized views, and
// window views are unsupported and rejected with a clear error during
// introspection.
//
// SQLSecurity is the canonical lowercase form of the SQL SECURITY
// clause: one of "definer", "invoker", or "none". Definer is the user
// the view runs as; required iff SQLSecurity == "definer" with a
// named user. The literal "current_user" is accepted unquoted.
type ViewSpec struct {
	Name           string   `hcl:"name,label"`
	Query          string   `hcl:"query"`
	ColumnAliases  []string `hcl:"column_aliases,optional"`
	SQLSecurity    *string  `hcl:"sql_security,optional"`
	Definer        *string  `hcl:"definer,optional"`
	Cluster        *string  `hcl:"cluster,optional"`
	Comment        *string  `hcl:"comment,optional"`
}
```

- [ ] **Step 2: Run unit suite to confirm clean compile**

```
go build ./...
```

Expected: builds clean.

```
go test ./internal/loader/hcl/... 2>&1 | tail -5
```

Expected: all existing tests still pass (no behaviour change yet — the field is unused).

- [ ] **Step 3: Commit**

```
git add internal/loader/hcl/types.go
git commit -m "feat(hcl): ViewSpec type + DatabaseSpec.Views field

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: HCL parse validation for sql_security / definer pairing

**Files:**
- Modify: `internal/loader/hcl/resolver.go`
- Modify: `internal/loader/hcl/resolver_test.go` (or `parser_test.go` — whichever already houses HCL parse tests; the impl picks the closer fit during step 1)

- [ ] **Step 1: Find where DatabaseSpec is validated after parse**

```
grep -n "func Resolve\|validateDatabase\|MaterializedView" internal/loader/hcl/resolver.go | head -10
```

Identify the function that walks a resolved `Schema` and validates per-object invariants. If a per-MV validation function already exists, add the view validator next to it. If not, add a dedicated `validateView` function and call it from the top-level resolver walk.

- [ ] **Step 2: Write the failing test**

Add to the resolver/parser test file:

```go
func TestViewSpec_DefinerRequiresSQLSecurityDefiner(t *testing.T) {
	src := `
database "posthog" {
  view "v" {
    query   = "SELECT 1"
    definer = "alice"
  }
}`
	_, err := ParseString(src)
	require.Error(t, err)
	require.Contains(t, err.Error(), "definer requires sql_security = \"definer\"")
}

func TestViewSpec_SQLSecurityEnumValidated(t *testing.T) {
	src := `
database "posthog" {
  view "v" {
    query        = "SELECT 1"
    sql_security = "bogus"
  }
}`
	_, err := ParseString(src)
	require.Error(t, err)
	require.Contains(t, err.Error(), `sql_security must be one of "definer", "invoker", "none"`)
}

func TestViewSpec_SQLSecurityCaseInsensitive(t *testing.T) {
	src := `
database "posthog" {
  view "v" {
    query        = "SELECT 1"
    sql_security = "DEFINER"
    definer      = "alice"
  }
}`
	s, err := ParseString(src)
	require.NoError(t, err)
	require.Len(t, s.Databases[0].Views, 1)
	require.Equal(t, "definer", *s.Databases[0].Views[0].SQLSecurity)
}
```

If `ParseString` does not exist, use whichever helper the existing MV-parse tests use (e.g. `parseFromString(t, src)`).

- [ ] **Step 3: Run it**

```
go test ./internal/loader/hcl -run TestViewSpec_ -v
```

Expected: all three FAIL (validation not implemented; SQL_security comes back as `"DEFINER"` not normalised; or no error at all).

- [ ] **Step 4: Implement the validator**

Add to `internal/loader/hcl/resolver.go`:

```go
// validateView normalises and validates a parsed ViewSpec. The HCL
// surface is more permissive than ClickHouse semantics; this catches
// schema mistakes at parse time rather than at apply.
func validateView(v *ViewSpec) error {
	if v.SQLSecurity != nil {
		canonical := strings.ToLower(strings.TrimSpace(*v.SQLSecurity))
		switch canonical {
		case "definer", "invoker", "none":
			v.SQLSecurity = &canonical
		default:
			return fmt.Errorf(`view %q: sql_security must be one of "definer", "invoker", "none"`, v.Name)
		}
	}
	if v.Definer != nil {
		if v.SQLSecurity == nil || *v.SQLSecurity != "definer" {
			return fmt.Errorf(`view %q: definer requires sql_security = "definer"`, v.Name)
		}
	}
	return nil
}
```

Then call it from the function that walks `Schema.Databases`. Locate that function by searching:

```
grep -n "for .* range .*Databases\|range db.MaterializedViews" internal/loader/hcl/resolver.go | head -5
```

In the same per-database loop, add (immediately after MV validation, or right at the start of the per-DB validation work):

```go
		for i := range db.Views {
			if err := validateView(&db.Views[i]); err != nil {
				return nil, err
			}
		}
```

If `strings`, `fmt` are not already imported in the file, add them.

- [ ] **Step 5: Re-run**

```
go test ./internal/loader/hcl -run TestViewSpec_ -v
```

Expected: PASS.

- [ ] **Step 6: Run the full HCL unit suite**

```
go test ./internal/loader/hcl/...
```

Expected: green.

- [ ] **Step 7: Commit**

```
git add internal/loader/hcl/resolver.go internal/loader/hcl/resolver_test.go
git commit -m "feat(hcl): validate view sql_security enum + definer pairing

sql_security must be lowercase one of definer|invoker|none (case-
insensitive on parse). definer is only legal alongside sql_security
= \"definer\".

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Cross-layer redeclare check for views

**Files:**
- Modify: `internal/loader/hcl/layers.go`
- Modify: `internal/loader/hcl/layers_test.go`

- [ ] **Step 1: Find the MV redeclare check**

```
grep -n "materialized_view %q redeclared" internal/loader/hcl/layers.go
```

Read the surrounding loop. The same pattern needs to apply to views.

- [ ] **Step 2: Write the failing test**

Add to `internal/loader/hcl/layers_test.go`:

```go
func TestMergeLayers_ViewRedeclareAcrossLayers(t *testing.T) {
	layerA := writeLayer(t, "a", map[string]string{
		"schema.hcl": `database "posthog" { view "v" { query = "SELECT 1" } }`,
	})
	layerB := writeLayer(t, "b", map[string]string{
		"schema.hcl": `database "posthog" { view "v" { query = "SELECT 2" } }`,
	})

	_, err := LoadLayers([]string{layerA, layerB})
	require.Error(t, err)
	require.Contains(t, err.Error(), `view "v" redeclared across layers`)
}
```

(`writeLayer` is already a helper in `layers_test.go`; reuse it.)

- [ ] **Step 3: Run the test**

```
go test ./internal/loader/hcl -run TestMergeLayers_ViewRedeclareAcrossLayers -v
```

Expected: FAIL — second layer's view replaces the first without an error.

- [ ] **Step 4: Add the redeclare check**

In `internal/loader/hcl/layers.go`, find the loop that runs the MV redeclare check. Right after it, in the same per-database loop, add:

```go
		for _, v := range src.Views {
			if existing := findView(dst.Views, v.Name); existing != nil {
				return fmt.Errorf("view %q redeclared across layers", v.Name)
			}
			dst.Views = append(dst.Views, v)
		}
```

And add the helper next to `findMaterializedView` (or whichever sibling already exists):

```go
func findView(views []ViewSpec, name string) *ViewSpec {
	for i := range views {
		if views[i].Name == name {
			return &views[i]
		}
	}
	return nil
}
```

- [ ] **Step 5: Re-run**

```
go test ./internal/loader/hcl -run TestMergeLayers_ViewRedeclareAcrossLayers -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```
git add internal/loader/hcl/layers.go internal/loader/hcl/layers_test.go
git commit -m "feat(hcl): reject cross-layer view redeclaration

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Canonical HCL emission for view blocks

**Files:**
- Modify: `internal/loader/hcl/dump.go`
- Modify: `internal/loader/hcl/dump_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/loader/hcl/dump_test.go`:

```go
func TestDumpSchema_RendersViewBlocks(t *testing.T) {
	s := &Schema{
		Databases: []DatabaseSpec{{
			Name: "posthog",
			Views: []ViewSpec{{
				Name:          "metrics",
				Query:         "SELECT team_id, count() AS n FROM events GROUP BY team_id",
				ColumnAliases: []string{"team_id", "n"},
				SQLSecurity:   ptr("definer"),
				Definer:       ptr("alice"),
				Comment:       ptr("team-level event counter"),
			}},
		}},
	}

	var buf bytes.Buffer
	require.NoError(t, Write(&buf, s))

	got := buf.String()
	require.Contains(t, got, `view "metrics" {`)
	require.Contains(t, got, `query`)
	require.Contains(t, got, `column_aliases = ["team_id", "n"]`)
	require.Contains(t, got, `sql_security = "definer"`)
	require.Contains(t, got, `definer = "alice"`)
	require.Contains(t, got, `comment = "team-level event counter"`)
}

func ptr[T any](v T) *T { return &v }
```

If `dump_test.go` already declares `ptr`, drop the helper from this test.

- [ ] **Step 2: Run it**

```
go test ./internal/loader/hcl -run TestDumpSchema_RendersViewBlocks -v
```

Expected: FAIL (`view "metrics" {` not in output).

- [ ] **Step 3: Add the emitter**

In `internal/loader/hcl/dump.go`, locate the per-database emission loop (the section that emits MVs after tables and dictionaries after MVs — search for `mvs := append([]MaterializedViewSpec`). Right after the dictionaries loop (or in canonical position: tables → MVs → views → dicts; impl picks the ordering at write time, but the spec says tables → MVs → views → dicts), add:

```go
	views := append([]ViewSpec(nil), db.Views...)
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })
	for i, v := range views {
		if len(tables) > 0 || len(mvs) > 0 || i > 0 {
			body.AppendNewline()
		}
		vBlock := body.AppendNewBlock("view", []string{v.Name})
		writeView(vBlock.Body(), v)
	}
```

(Move the `dicts := …` block to come after this so the canonical order is tables → MVs → views → dicts. If it's already after `mvs`, drop it below `views` instead.)

Add the new `writeView` next to `writeMaterializedView`:

```go
func writeView(body *hclwrite.Body, v ViewSpec) {
	body.SetAttributeValue("query", cty.StringVal(v.Query))
	if len(v.ColumnAliases) > 0 {
		body.SetAttributeValue("column_aliases", stringList(v.ColumnAliases))
	}
	if v.SQLSecurity != nil {
		body.SetAttributeValue("sql_security", cty.StringVal(*v.SQLSecurity))
	}
	if v.Definer != nil {
		body.SetAttributeValue("definer", cty.StringVal(*v.Definer))
	}
	if v.Cluster != nil {
		body.SetAttributeValue("cluster", cty.StringVal(*v.Cluster))
	}
	if v.Comment != nil {
		body.SetAttributeValue("comment", cty.StringVal(*v.Comment))
	}
}
```

- [ ] **Step 4: Re-run**

```
go test ./internal/loader/hcl -run TestDumpSchema_RendersViewBlocks -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```
git add internal/loader/hcl/dump.go internal/loader/hcl/dump_test.go
git commit -m "feat(hcl): canonical view block emission

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Introspect plain views

**Files:**
- Modify: `internal/loader/hcl/introspect.go`
- Modify: `internal/loader/hcl/introspect_test.go`

- [ ] **Step 1: Inspect the chparser CreateView shape**

```
grep -rn "CreateView struct\|type CreateView" vendor/github.com/orian/clickhouse-sql-parser/parser 2>/dev/null || go doc github.com/AfterShip/clickhouse-sql-parser/parser CreateView 2>&1 | head -40
```

Read the AST fields — specifically: how is the SELECT body exposed? Is there a `Materialized bool` / `Refresh` / window-view flag? Is there a `Comment`, `OnCluster`, `Definer`, `SQLSecurity`, or column-alias field?

Make notes; the next steps reference these fields. If the AST does NOT expose `Definer` / `SQLSecurity` (the fork is from older AfterShip code), use `formatNode(stmt)` and regex-extract those clauses from the formatted output as a fallback — but only after confirming the AST gap; the visitor refactor branch may already model them. If neither AST nor format-regex works for a particular field, stop and surface the gap to the user before continuing.

- [ ] **Step 2: Write the failing test for the happy path**

Add to `internal/loader/hcl/introspect_test.go`:

```go
func TestProcessIntrospectRows_PlainView(t *testing.T) {
	rows := &fakeRowScanner{
		data: [][]any{
			{"v_simple", "CREATE VIEW posthog.v_simple AS SELECT team_id FROM posthog.events"},
		},
	}
	db := &DatabaseSpec{Name: "posthog"}
	require.NoError(t, processIntrospectRows(db, "posthog", rows))
	require.Empty(t, db.Tables)
	require.Empty(t, db.MaterializedViews)
	require.Len(t, db.Views, 1)
	require.Equal(t, "v_simple", db.Views[0].Name)
	require.Equal(t, "SELECT team_id FROM posthog.events", db.Views[0].Query)
}

func TestProcessIntrospectRows_ViewWithColumnAliasesAndComment(t *testing.T) {
	rows := &fakeRowScanner{
		data: [][]any{
			{"v_aliased", "CREATE VIEW posthog.v_aliased (team_id, n) AS SELECT team_id, count() AS c FROM posthog.events GROUP BY team_id COMMENT 'aliased view'"},
		},
	}
	db := &DatabaseSpec{Name: "posthog"}
	require.NoError(t, processIntrospectRows(db, "posthog", rows))
	require.Len(t, db.Views, 1)
	require.Equal(t, []string{"team_id", "n"}, db.Views[0].ColumnAliases)
	require.NotNil(t, db.Views[0].Comment)
	require.Equal(t, "aliased view", *db.Views[0].Comment)
}

func TestProcessIntrospectRows_ViewWithSQLSecurityDefiner(t *testing.T) {
	rows := &fakeRowScanner{
		data: [][]any{
			{"v_sec", "CREATE VIEW posthog.v_sec DEFINER = alice SQL SECURITY DEFINER AS SELECT 1"},
		},
	}
	db := &DatabaseSpec{Name: "posthog"}
	require.NoError(t, processIntrospectRows(db, "posthog", rows))
	require.Len(t, db.Views, 1)
	require.NotNil(t, db.Views[0].SQLSecurity)
	require.Equal(t, "definer", *db.Views[0].SQLSecurity)
	require.NotNil(t, db.Views[0].Definer)
	require.Equal(t, "alice", *db.Views[0].Definer)
}
```

`fakeRowScanner` should already be defined in the test file for the existing introspect tests. If it isn't, search:

```
grep -n "type fakeRowScanner\|rowScanner" internal/loader/hcl/introspect_test.go
```

Reuse the existing fake.

- [ ] **Step 3: Run the tests**

```
go test ./internal/loader/hcl -run TestProcessIntrospectRows_ -v
```

Expected: the three new tests FAIL — `db.Views` empty, fields not set.

- [ ] **Step 4: Replace the skip-view case**

In `internal/loader/hcl/introspect.go`, replace the existing CreateView arm:

```go
		case *chparser.CreateView:
			// Plain (non-materialized) views are out of scope; skip them
			// rather than failing the whole introspection.
			continue
```

with:

```go
		case *chparser.CreateView:
			v, err := buildViewFromCreateView(s)
			if err != nil {
				return fmt.Errorf("introspect view %s.%s: %w", database, name, err)
			}
			v.Name = name
			db.Views = append(db.Views, v)
```

- [ ] **Step 5: Implement `buildViewFromCreateView`**

In `internal/loader/hcl/introspect.go`, near `buildMaterializedViewFromCreateMV`, add:

```go
// buildViewFromCreateView walks a parsed CREATE VIEW AST and produces a
// ViewSpec. Live views, refreshable views, and window views are
// rejected with named errors. The SELECT body is rendered back to
// text via formatNode (PrintVisitor), matching the way MV bodies are
// captured.
func buildViewFromCreateView(cv *chparser.CreateView) (ViewSpec, error) {
	// Reject unsupported shapes. The exact AST fields depend on the
	// chparser version; check whichever fields the AST exposes for
	// Refreshable / Live / Window views. If the parser cannot distinguish
	// these from a plain view, the upstream Create call will have
	// landed in a different AST type, so this is a defence-in-depth
	// check, not the primary gate.
	if cv.Refresh != nil {
		return ViewSpec{}, errors.New("unsupported: refreshable materialized view in CREATE VIEW arm")
	}
	if cv.Engine != nil {
		return ViewSpec{}, errors.New("unsupported: live view (CREATE LIVE VIEW or VIEW with ENGINE)")
	}
	// chparser as of orian/refactor-visitor does not separately model
	// window views — they parse as plain CREATE VIEW with a WATERMARK
	// clause embedded in the SELECT. We let those round-trip as
	// opaque text; the WATERMARK keyword inside the query is preserved
	// verbatim.

	v := ViewSpec{}

	if cv.SubQuery != nil {
		v.Query = strings.TrimSpace(formatNode(cv.SubQuery))
	}

	if cv.OnCluster != nil && cv.OnCluster.Expr != nil {
		v.Cluster = strPtr(formatNode(cv.OnCluster.Expr))
	}

	if cv.Comment != nil {
		v.Comment = strPtr(unquoteString(cv.Comment.Literal))
	}

	// Column alias list. ClickHouse's CREATE VIEW v (a, b, c) AS … —
	// the AST exposes the list as cv.TableSchema or cv.Aliases
	// depending on chparser version. Walk whichever is non-nil. If
	// none exists in the version we ship, this branch is dead code
	// and aliases stay nil (which round-trips fine: no aliases is the
	// no-op canonical form).
	if cv.TableSchema != nil {
		for _, c := range cv.TableSchema.Columns {
			cd, ok := c.(*chparser.ColumnDef)
			if !ok {
				continue
			}
			if cd.Name == nil {
				continue
			}
			v.ColumnAliases = append(v.ColumnAliases, stripBackticks(identName(cd.Name)))
		}
	}

	// SQL SECURITY + DEFINER. Mirror the canonical lowercase rule
	// validated at HCL parse time. If chparser exposes these on cv
	// directly, use them; otherwise fall back to regex-extracting from
	// the formatted CREATE statement.
	if cv.SQLSecurity != nil {
		s := strings.ToLower(strings.TrimSpace(formatNode(cv.SQLSecurity)))
		v.SQLSecurity = &s
	}
	if cv.Definer != nil {
		d := formatNode(cv.Definer)
		v.Definer = &d
	}

	return v, nil
}
```

Note: the field names `cv.Refresh`, `cv.Engine`, `cv.SubQuery`, `cv.OnCluster`, `cv.Comment`, `cv.TableSchema`, `cv.SQLSecurity`, `cv.Definer` are the **assumed** names — verify against the actual chparser AST in Step 1. If a field is named differently (e.g. `cv.Select` instead of `cv.SubQuery`), use the actual name. If a clause is not modelled at all by the AST (most likely candidates: Definer / SQLSecurity), regex-extract them from the formatted statement:

```go
// Fallback for chparser versions that don't model DEFINER / SQL SECURITY.
formatted := formatNode(cv)
if m := regexp.MustCompile(`(?i)DEFINER\s*=\s*([A-Za-z_][A-Za-z0-9_]*|CURRENT_USER)`).FindStringSubmatch(formatted); m != nil {
    d := m[1]
    v.Definer = &d
}
if m := regexp.MustCompile(`(?i)SQL\s+SECURITY\s+(DEFINER|INVOKER|NONE)`).FindStringSubmatch(formatted); m != nil {
    s := strings.ToLower(m[1])
    v.SQLSecurity = &s
}
```

Use only one path (AST or regex) per field. If you fall back to regex, leave a comment referencing this paragraph.

- [ ] **Step 6: Re-run**

```
go test ./internal/loader/hcl -run TestProcessIntrospectRows_ -v
```

Expected: all three PASS.

- [ ] **Step 7: Commit**

```
git add internal/loader/hcl/introspect.go internal/loader/hcl/introspect_test.go
git commit -m "feat(hcl): introspect plain views

Replaces the silent skip arm in processIntrospectRows with a real
ViewSpec builder. Captures Query (verbatim), ColumnAliases, Cluster,
Comment, SQLSecurity, and Definer. Refreshable / live shapes rejected
with named errors.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Diff types and per-view diff

**Files:**
- Modify: `internal/loader/hcl/diff.go`
- Modify: `internal/loader/hcl/diff_test.go`

- [ ] **Step 1: Extend DatabaseChange and add ViewDiff**

In `internal/loader/hcl/diff.go`, extend `DatabaseChange` (right after the MV/dict fields):

```go
	AddViews   []ViewSpec
	DropViews  []string
	AlterViews []ViewDiff
```

Then add the new type next to `MaterializedViewDiff`:

```go
// ViewDiff is the set of mutations to a single existing plain view. A
// query-only change is applied in place via ALTER TABLE ... MODIFY
// QUERY. A comment-only change is applied via ALTER TABLE ... MODIFY
// COMMENT. Any change to ColumnAliases / SQLSecurity / Definer /
// Cluster requires recreating the view (DROP + CREATE) and is flagged
// unsafe.
type ViewDiff struct {
	Name        string
	QueryChange *StringChange
	Comment     *StringChange
	Recreate    bool
}

func (vd ViewDiff) IsEmpty() bool {
	return vd.QueryChange == nil && vd.Comment == nil && !vd.Recreate
}

func (vd ViewDiff) IsUnsafe() bool { return vd.Recreate }
```

Extend `DatabaseChange.IsEmpty()` to also check view slices:

```go
		len(dc.AddViews) == 0 && len(dc.DropViews) == 0 &&
		len(dc.AlterViews) == 0 &&
```

(Place this between the MV and dict checks. Match the existing line-wrap style.)

- [ ] **Step 2: Write the failing diff tests**

Add to `internal/loader/hcl/diff_test.go`:

```go
func mkView(name, query string) ViewSpec {
	return ViewSpec{Name: name, Query: query}
}

func TestDiff_ViewAddDrop(t *testing.T) {
	from := &Schema{Databases: []DatabaseSpec{{Name: "posthog", Views: []ViewSpec{mkView("v_old", "SELECT 1")}}}}
	to := &Schema{Databases: []DatabaseSpec{{Name: "posthog", Views: []ViewSpec{mkView("v_new", "SELECT 2")}}}}
	cs := Diff(from, to)
	require.False(t, cs.IsEmpty())
	require.Len(t, cs.Databases, 1)
	dc := cs.Databases[0]
	require.Equal(t, []string{"v_old"}, dc.DropViews)
	require.Len(t, dc.AddViews, 1)
	require.Equal(t, "v_new", dc.AddViews[0].Name)
}

func TestDiff_ViewQueryOnlyChange(t *testing.T) {
	from := &Schema{Databases: []DatabaseSpec{{Name: "posthog", Views: []ViewSpec{mkView("v", "SELECT 1")}}}}
	to := &Schema{Databases: []DatabaseSpec{{Name: "posthog", Views: []ViewSpec{mkView("v", "SELECT 2")}}}}
	cs := Diff(from, to)
	require.Len(t, cs.Databases[0].AlterViews, 1)
	vd := cs.Databases[0].AlterViews[0]
	require.NotNil(t, vd.QueryChange)
	require.False(t, vd.Recreate)
	require.Nil(t, vd.Comment)
}

func TestDiff_ViewCommentOnlyChange(t *testing.T) {
	vFrom := mkView("v", "SELECT 1")
	vFrom.Comment = ptrStr("old")
	vTo := mkView("v", "SELECT 1")
	vTo.Comment = ptrStr("new")
	from := &Schema{Databases: []DatabaseSpec{{Name: "posthog", Views: []ViewSpec{vFrom}}}}
	to := &Schema{Databases: []DatabaseSpec{{Name: "posthog", Views: []ViewSpec{vTo}}}}
	cs := Diff(from, to)
	vd := cs.Databases[0].AlterViews[0]
	require.Nil(t, vd.QueryChange)
	require.NotNil(t, vd.Comment)
	require.False(t, vd.Recreate)
}

func TestDiff_ViewShapeChangeForcesRecreate(t *testing.T) {
	vFrom := mkView("v", "SELECT 1")
	vFrom.ColumnAliases = []string{"a"}
	vTo := mkView("v", "SELECT 1")
	vTo.ColumnAliases = []string{"a", "b"}
	from := &Schema{Databases: []DatabaseSpec{{Name: "posthog", Views: []ViewSpec{vFrom}}}}
	to := &Schema{Databases: []DatabaseSpec{{Name: "posthog", Views: []ViewSpec{vTo}}}}
	cs := Diff(from, to)
	vd := cs.Databases[0].AlterViews[0]
	require.True(t, vd.Recreate)
	require.True(t, vd.IsUnsafe())
}

func TestDiff_IdenticalViewsEmpty(t *testing.T) {
	v := mkView("v", "SELECT 1")
	from := &Schema{Databases: []DatabaseSpec{{Name: "posthog", Views: []ViewSpec{v}}}}
	to := &Schema{Databases: []DatabaseSpec{{Name: "posthog", Views: []ViewSpec{v}}}}
	require.True(t, Diff(from, to).IsEmpty())
}
```

If `ptrStr` is not already in `diff_test.go`, add a local helper `ptrStr := func(s string) *string { return &s }` at the top of the test or use an existing one.

- [ ] **Step 3: Run the tests**

```
go test ./internal/loader/hcl -run TestDiff_View -v
```

Expected: FAIL — diff doesn't process the new field.

- [ ] **Step 4: Implement the view diff iteration**

In `internal/loader/hcl/diff.go`, locate `diffDatabase`. Find where it iterates MVs (`for _, mv := range to.MaterializedViews { … }`). Right after that loop (and before the dictionaries iteration), add:

```go
	// --- Views (plain) ---
	fromViews := indexViews(from.Views)
	toViews := indexViews(to.Views)
	for _, v := range to.Views {
		if existing, ok := fromViews[v.Name]; ok {
			vd := diffView(existing, v)
			if !vd.IsEmpty() {
				dc.AlterViews = append(dc.AlterViews, vd)
			}
		} else {
			dc.AddViews = append(dc.AddViews, v)
		}
	}
	for _, v := range from.Views {
		if _, ok := toViews[v.Name]; !ok {
			dc.DropViews = append(dc.DropViews, v.Name)
		}
	}
```

Add the helpers next to the existing `indexMaterializedViews` / `diffMaterializedView`:

```go
func indexViews(views []ViewSpec) map[string]ViewSpec {
	m := make(map[string]ViewSpec, len(views))
	for _, v := range views {
		m[v.Name] = v
	}
	return m
}

// diffView compares two views with the same name. Query and Comment
// can be modified in place; ColumnAliases, SQLSecurity, Definer, and
// Cluster changes require DROP + CREATE.
func diffView(from, to ViewSpec) ViewDiff {
	vd := ViewDiff{Name: to.Name}
	if from.Query != to.Query {
		vd.QueryChange = &StringChange{Old: strPtrOrNil(from.Query), New: strPtrOrNil(to.Query)}
	}
	if !strPtrEq(from.Comment, to.Comment) {
		vd.Comment = &StringChange{Old: from.Comment, New: to.Comment}
	}
	if !stringSliceEqual(from.ColumnAliases, to.ColumnAliases) ||
		!strPtrEq(from.SQLSecurity, to.SQLSecurity) ||
		!strPtrEq(from.Definer, to.Definer) ||
		!strPtrEq(from.Cluster, to.Cluster) {
		vd.Recreate = true
	}
	return vd
}

// strPtrOrNil wraps a string in a pointer; empty becomes nil so the
// StringChange convention (nil = unset) holds even when one side is
// the zero value.
func strPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
```

If `strPtrEq` or `stringSliceEqual` aren't already in `diff.go` (search to confirm), add them:

```go
func strPtrEq(a, b *string) bool {
	if a == nil && b == nil { return true }
	if a == nil || b == nil { return false }
	return *a == *b
}
```

(For `stringSliceEqual` the same one used elsewhere in the package — search for existing definition before adding.)

Also: in the `Diff()` top level (where each side is empty), make sure `AddViews` is populated when the from-database is empty. Find the `dc = DatabaseChange{...}` literal that already populates `AddTables` / `AddMaterializedViews` / `AddDictionaries` for new databases, and add `AddViews: append([]ViewSpec(nil), t.Views...)`.

- [ ] **Step 5: Re-run**

```
go test ./internal/loader/hcl -run TestDiff_View -v
```

Expected: PASS.

```
go test ./internal/loader/hcl
```

Expected: all green (existing tests still pass).

- [ ] **Step 6: Commit**

```
git add internal/loader/hcl/diff.go internal/loader/hcl/diff_test.go
git commit -m "feat(hcl): diff plain views

AddViews / DropViews / AlterViews on DatabaseChange. Query-only and
comment-only changes diff in place; ColumnAliases / SQLSecurity /
Definer / Cluster changes force Recreate (DROP + CREATE).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: SQL generation for views

**Files:**
- Modify: `internal/loader/hcl/sqlgen.go`
- Modify: `internal/loader/hcl/sqlgen_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `internal/loader/hcl/sqlgen_test.go`:

```go
func TestSQLGen_CreateView_Bare(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AddViews: []ViewSpec{{Name: "v", Query: "SELECT 1"}},
	}}}
	got := GenerateSQL(cs)
	require.Contains(t, got, `CREATE VIEW posthog.v AS SELECT 1`)
}

func TestSQLGen_CreateView_FullForm(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AddViews: []ViewSpec{{
			Name:          "v",
			Query:         "SELECT team_id, count() AS n FROM events GROUP BY team_id",
			ColumnAliases: []string{"team_id", "n"},
			SQLSecurity:   ptrStr("definer"),
			Definer:       ptrStr("alice"),
			Cluster:       ptrStr("posthog"),
			Comment:       ptrStr("team-level"),
		}},
	}}}
	got := GenerateSQL(cs)
	require.Contains(t, got, `CREATE VIEW posthog.v ON CLUSTER posthog (team_id, n) DEFINER = alice SQL SECURITY DEFINER AS SELECT team_id, count() AS n FROM events GROUP BY team_id COMMENT 'team-level'`)
}

func TestSQLGen_DropView(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database:  "posthog",
		DropViews: []string{"old_v"},
	}}}
	got := GenerateSQL(cs)
	require.Contains(t, got, `DROP VIEW IF EXISTS posthog.old_v`)
}

func TestSQLGen_AlterView_ModifyQuery(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AlterViews: []ViewDiff{{
			Name:        "v",
			QueryChange: &StringChange{Old: ptrStr("SELECT 1"), New: ptrStr("SELECT 2")},
		}},
	}}}
	got := GenerateSQL(cs)
	require.Contains(t, got, `ALTER TABLE posthog.v MODIFY QUERY SELECT 2`)
}

func TestSQLGen_AlterView_ModifyComment(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AlterViews: []ViewDiff{{
			Name:    "v",
			Comment: &StringChange{Old: ptrStr("old"), New: ptrStr("new")},
		}},
	}}}
	got := GenerateSQL(cs)
	require.Contains(t, got, `ALTER TABLE posthog.v MODIFY COMMENT 'new'`)
}

func TestSQLGen_AlterView_Recreate(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AlterViews: []ViewDiff{{
			Name:     "v",
			Recreate: true,
		}},
		// Also surface the post-recreate view shape via AddViews — this
		// matches the MV recreate convention.
		AddViews: []ViewSpec{{Name: "v", Query: "SELECT 2"}},
	}}}
	got := GenerateSQL(cs)
	require.Contains(t, got, `DROP VIEW IF EXISTS posthog.v`)
	require.Contains(t, got, `CREATE VIEW posthog.v AS SELECT 2`)
}
```

If `ptrStr` is already defined in the file, drop the helper from the tests above.

- [ ] **Step 2: Run them**

```
go test ./internal/loader/hcl -run TestSQLGen_(CreateView|DropView|AlterView) -v
```

Expected: all FAIL — no view DDL.

- [ ] **Step 3: Implement the emitters**

In `internal/loader/hcl/sqlgen.go`, locate the function that drives per-database DDL emission (search `AddMaterializedViews` for the call site). After the MV emission (in the order: drop dependent objects → drop tables → create tables → create MVs → create views → create dicts; mirror the MV ordering), add a view block.

```go
	for _, name := range dc.DropViews {
		fmt.Fprintf(&buf, "DROP VIEW IF EXISTS %s.%s;\n", dc.Database, name)
	}
	for _, vd := range dc.AlterViews {
		if vd.Recreate {
			fmt.Fprintf(&buf, "DROP VIEW IF EXISTS %s.%s;\n", dc.Database, vd.Name)
			// The post-recreate CREATE is in AddViews — emitted below.
			continue
		}
		if vd.QueryChange != nil && vd.QueryChange.New != nil {
			fmt.Fprintf(&buf, "ALTER TABLE %s.%s MODIFY QUERY %s;\n", dc.Database, vd.Name, *vd.QueryChange.New)
		}
		if vd.Comment != nil && vd.Comment.New != nil {
			fmt.Fprintf(&buf, "ALTER TABLE %s.%s MODIFY COMMENT %s;\n", dc.Database, vd.Name, quoteString(*vd.Comment.New))
		}
	}
	for _, v := range dc.AddViews {
		fmt.Fprintf(&buf, "%s;\n", renderCreateView(dc.Database, v))
	}
```

Add the helper:

```go
// renderCreateView emits the full CREATE VIEW statement for a ViewSpec.
// Field order matches the ClickHouse grammar:
//   CREATE VIEW [ON CLUSTER ...] db.name [(aliases)]
//     [DEFINER = ...] [SQL SECURITY ...]
//     AS <query>
//     [COMMENT '...']
func renderCreateView(database string, v ViewSpec) string {
	var sb strings.Builder
	sb.WriteString("CREATE VIEW ")
	sb.WriteString(database)
	sb.WriteByte('.')
	sb.WriteString(v.Name)
	if v.Cluster != nil {
		sb.WriteString(" ON CLUSTER ")
		sb.WriteString(*v.Cluster)
	}
	if len(v.ColumnAliases) > 0 {
		sb.WriteString(" (")
		sb.WriteString(strings.Join(v.ColumnAliases, ", "))
		sb.WriteByte(')')
	}
	if v.Definer != nil {
		sb.WriteString(" DEFINER = ")
		sb.WriteString(*v.Definer)
	}
	if v.SQLSecurity != nil {
		sb.WriteString(" SQL SECURITY ")
		sb.WriteString(strings.ToUpper(*v.SQLSecurity))
	}
	sb.WriteString(" AS ")
	sb.WriteString(v.Query)
	if v.Comment != nil {
		sb.WriteString(" COMMENT ")
		sb.WriteString(quoteString(*v.Comment))
	}
	return sb.String()
}
```

If `quoteString` doesn't exist yet, search the file for whatever helper escapes ClickHouse single-quoted strings — it's used by MV / dict emission. Reuse it.

- [ ] **Step 4: Re-run**

```
go test ./internal/loader/hcl -run TestSQLGen_(CreateView|DropView|AlterView) -v
```

Expected: all PASS.

```
go test ./internal/loader/hcl
```

Expected: full suite green.

- [ ] **Step 5: Commit**

```
git add internal/loader/hcl/sqlgen.go internal/loader/hcl/sqlgen_test.go
git commit -m "feat(hcl): emit CREATE/DROP/ALTER DDL for plain views

Body-only changes use ALTER TABLE ... MODIFY QUERY; comment-only use
ALTER TABLE ... MODIFY COMMENT; shape changes (aliases / security /
cluster) recreate via DROP + CREATE.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Validate view dependencies

**Files:**
- Modify: `internal/loader/hcl/validate.go`
- Modify: `internal/loader/hcl/validate_test.go`

- [ ] **Step 1: Find the existing MV validation walk**

```
grep -n "for _, mv := range db.MaterializedViews\|materialized_view %s:" internal/loader/hcl/validate.go
```

Note the surrounding code shape — particularly how it builds the declared-objects set and how it formats the "undeclared reference" error.

- [ ] **Step 2: Write the failing tests**

Add to `internal/loader/hcl/validate_test.go`:

```go
func TestValidate_ViewSourceTableMustExist(t *testing.T) {
	s := &Schema{
		Databases: []DatabaseSpec{{
			Name: "posthog",
			Tables: []TableSpec{
				{Name: "events", Columns: []ColumnSpec{{Name: "team_id", Type: "Int64"}}, Engine: &EngineSpec{Kind: "merge_tree"}, OrderBy: []string{"team_id"}},
			},
			Views: []ViewSpec{
				// References a nonexistent source.
				{Name: "v", Query: "SELECT team_id FROM posthog.nonexistent"},
			},
		}},
	}
	err := Validate(s, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), `view "v" references undeclared table posthog.nonexistent`)
}

func TestValidate_ViewSourceTableInSameSchema(t *testing.T) {
	s := &Schema{
		Databases: []DatabaseSpec{{
			Name: "posthog",
			Tables: []TableSpec{
				{Name: "events", Columns: []ColumnSpec{{Name: "team_id", Type: "Int64"}}, Engine: &EngineSpec{Kind: "merge_tree"}, OrderBy: []string{"team_id"}},
			},
			Views: []ViewSpec{
				{Name: "v", Query: "SELECT team_id FROM posthog.events"},
			},
		}},
	}
	require.NoError(t, Validate(s, nil))
}

func TestValidate_ViewSkipByName(t *testing.T) {
	s := &Schema{
		Databases: []DatabaseSpec{{
			Name: "posthog",
			Views: []ViewSpec{{Name: "v", Query: "SELECT 1 FROM posthog.missing"}},
		}},
	}
	require.NoError(t, Validate(s, []string{"v"}))
}

func TestValidate_ViewCTENameNotASource(t *testing.T) {
	s := &Schema{
		Databases: []DatabaseSpec{{
			Name: "posthog",
			Tables: []TableSpec{
				{Name: "events", Columns: []ColumnSpec{{Name: "team_id", Type: "Int64"}}, Engine: &EngineSpec{Kind: "merge_tree"}, OrderBy: []string{"team_id"}},
			},
			Views: []ViewSpec{
				{Name: "v", Query: "WITH stats AS (SELECT team_id FROM posthog.events) SELECT * FROM stats"},
			},
		}},
	}
	require.NoError(t, Validate(s, nil))
}
```

If `Validate`'s signature is different (e.g. it takes the skip list as a `map[string]bool` or no second argument at all), adjust the test calls to match. Check:

```
grep -n "^func Validate" internal/loader/hcl/validate.go
```

- [ ] **Step 3: Run the tests**

```
go test ./internal/loader/hcl -run TestValidate_View -v
```

Expected: FAIL — views aren't walked.

- [ ] **Step 4: Add the view walk**

In `internal/loader/hcl/validate.go`, find the MV iteration. Immediately after it (still inside the per-database loop), add:

```go
		// Views: same dependency contract as MVs — every table named
		// in the SELECT body must be declared somewhere in the loaded
		// schema. CTE names are filtered out by ExtractReferencedTables.
		for _, v := range db.Views {
			if skip[v.Name] {
				continue
			}
			refs, err := ExtractReferencedTables(stitchCreateView(db.Name, v))
			if err != nil {
				return fmt.Errorf("view %q: parsing query: %w", v.Name, err)
			}
			for _, r := range refs {
				dbName := r.Database
				if dbName == "" {
					dbName = db.Name
				}
				if !declared[dbName+"."+r.Name] {
					return fmt.Errorf(`view "%s" references undeclared table %s.%s`, v.Name, dbName, r.Name)
				}
			}
		}
```

Add the helper:

```go
// stitchCreateView builds a minimal CREATE VIEW statement around a
// ViewSpec's Query so ExtractReferencedTables can parse it as a single
// AST. The reconstructed statement does not need to match the exact
// production statement — only its FROM/JOIN structure matters.
func stitchCreateView(database string, v ViewSpec) string {
	return fmt.Sprintf("CREATE VIEW %s.%s AS %s", database, v.Name, v.Query)
}
```

The `declared` set and `skip` map are already maintained in the surrounding function for tables / MVs / dicts. Reuse the same names — if they have different names locally (e.g. `existsByQName`, `skipSet`), substitute appropriately.

- [ ] **Step 5: Re-run**

```
go test ./internal/loader/hcl -run TestValidate_View -v
```

Expected: all four PASS.

```
go test ./internal/loader/hcl
```

Expected: full suite green.

- [ ] **Step 6: Commit**

```
git add internal/loader/hcl/validate.go internal/loader/hcl/validate_test.go
git commit -m "feat(hcl): validate plain view source dependencies

A view's SELECT must reference only declared tables / MVs / views.
CTE names are filtered out by ExtractReferencedTables. The existing
-skip-validation=<name,...> flag honours view names.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 9: Diff ordering — views are created after sources, dropped before

**Files:**
- Modify: `internal/loader/hcl/sqlgen.go` (or wherever the change-set ordering lives — likely in sqlgen)
- Modify: `internal/loader/hcl/sqlgen_test.go`

- [ ] **Step 1: Find the existing MV ordering**

```
grep -n "ordering\|topo\|DependsOn\|MaterializedView" internal/loader/hcl/sqlgen.go | head -10
```

Read whichever function/section currently orders DDL so MVs land after their source tables.

- [ ] **Step 2: Write the failing test**

Add to `internal/loader/hcl/sqlgen_test.go`:

```go
func TestSQLGen_ViewCreatedAfterSourceTable(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AddTables: []TableSpec{
			{Name: "events", Columns: []ColumnSpec{{Name: "team_id", Type: "Int64"}}, Engine: &EngineSpec{Kind: "merge_tree"}, OrderBy: []string{"team_id"}},
		},
		AddViews: []ViewSpec{
			{Name: "v", Query: "SELECT team_id FROM posthog.events"},
		},
	}}}
	got := GenerateSQL(cs)
	tIdx := strings.Index(got, "CREATE TABLE posthog.events")
	vIdx := strings.Index(got, "CREATE VIEW posthog.v")
	require.GreaterOrEqual(t, tIdx, 0)
	require.GreaterOrEqual(t, vIdx, 0)
	require.Less(t, tIdx, vIdx, "view must be created after its source table")
}

func TestSQLGen_ViewDroppedBeforeSourceTable(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		DropTables: []TableSpec{
			{Name: "events", Columns: []ColumnSpec{{Name: "team_id", Type: "Int64"}}, Engine: &EngineSpec{Kind: "merge_tree"}, OrderBy: []string{"team_id"}},
		},
		DropViews: []string{"v"},
	}}}
	// Note: in real use DropViews comes from a diff that already knows
	// what the dropped view referenced. The test here checks the
	// ordering rule independently — view drops must come before any
	// table drop that could be a source.
	got := GenerateSQL(cs)
	vIdx := strings.Index(got, "DROP VIEW IF EXISTS posthog.v")
	tIdx := strings.Index(got, "DROP TABLE")
	require.GreaterOrEqual(t, vIdx, 0)
	require.GreaterOrEqual(t, tIdx, 0)
	require.Less(t, vIdx, tIdx, "view must be dropped before its source table")
}
```

- [ ] **Step 3: Run them**

```
go test ./internal/loader/hcl -run TestSQLGen_View(Created|Dropped) -v
```

Expected: depends on current ordering. If the existing CREATE order is "tables → MVs → views", the create test may already pass. If the DROP order is "MVs → views → tables", the drop test may already pass. Run and see — fix only what's broken.

- [ ] **Step 4: Adjust ordering if needed**

If either test fails, edit the per-database emission order in `sqlgen.go`. The canonical order (matching the spec):

```
DROP VIEW (views)
DROP MATERIALIZED VIEW
DROP DICTIONARY
DROP TABLE
CREATE TABLE
CREATE MATERIALIZED VIEW
CREATE VIEW
CREATE DICTIONARY
ALTER ... (in-place modifies)
```

If a refactor is needed, move the view-related write blocks (added in Task 7) above the MV blocks for DROPs and below them for CREATEs. The exact line edit depends on the current sqlgen.go shape.

- [ ] **Step 5: Re-run**

```
go test ./internal/loader/hcl -run TestSQLGen_View
```

Expected: PASS.

```
go test ./internal/loader/hcl
```

Expected: full suite green.

- [ ] **Step 6: Commit**

```
git add internal/loader/hcl/sqlgen.go internal/loader/hcl/sqlgen_test.go
git commit -m "feat(hcl): order view DDL after source tables on CREATE, before on DROP

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: Render change-set output for views

**Files:**
- Modify: `cmd/hclexp/hclexp.go`
- Modify: `cmd/hclexp/hclexp_test.go`

- [ ] **Step 1: Write the failing test**

Add to `cmd/hclexp/hclexp_test.go`:

```go
func TestRenderChangeSet_Views(t *testing.T) {
	cs := hclload.ChangeSet{
		Databases: []hclload.DatabaseChange{
			{
				Database:   "posthog",
				AddViews:   []hclload.ViewSpec{{Name: "new_v"}},
				DropViews:  []string{"old_v"},
				AlterViews: []hclload.ViewDiff{
					{Name: "modified_q", QueryChange: &hclload.StringChange{Old: ptrStr("SELECT 1"), New: ptrStr("SELECT 2")}},
					{Name: "recreated", Recreate: true},
				},
			},
		},
	}

	var buf bytes.Buffer
	renderChangeSet(&buf, cs)

	want := `database "posthog"
  + view new_v
  - view old_v
  ~ view modified_q
      ~ query changed
  ~ view recreated
      ! requires recreation (column_aliases / sql_security / definer / cluster changed)
`
	require.Equal(t, want, buf.String())
}
```

- [ ] **Step 2: Run it**

```
go test ./cmd/hclexp -run TestRenderChangeSet_Views -v
```

Expected: FAIL — `renderChangeSet` doesn't know about views.

- [ ] **Step 3: Extend renderChangeSet**

In `cmd/hclexp/hclexp.go`, find the existing materialized-view rendering block:

```
grep -n "AddMaterializedViews\|materialized_view %s ->" cmd/hclexp/hclexp.go
```

Right after that block (still inside the per-database loop in `renderChangeSet`), add:

```go
		for _, v := range dc.AddViews {
			fmt.Fprintf(w, "  + view %s\n", v.Name)
		}
		for _, name := range dc.DropViews {
			fmt.Fprintf(w, "  - view %s\n", name)
		}
		for _, vd := range dc.AlterViews {
			fmt.Fprintf(w, "  ~ view %s\n", vd.Name)
			if vd.Recreate {
				fmt.Fprintf(w, "      ! requires recreation (column_aliases / sql_security / definer / cluster changed)\n")
			}
			if vd.QueryChange != nil {
				fmt.Fprintf(w, "      ~ query changed\n")
			}
			if vd.Comment != nil {
				fmt.Fprintf(w, "      ~ comment changed\n")
			}
		}
```

- [ ] **Step 4: Re-run**

```
go test ./cmd/hclexp -run TestRenderChangeSet_Views -v
```

Expected: PASS.

- [ ] **Step 5: Run all CLI tests**

```
go test ./cmd/hclexp
```

Expected: green.

- [ ] **Step 6: Commit**

```
git add cmd/hclexp/hclexp.go cmd/hclexp/hclexp_test.go
git commit -m "feat(hclexp): render +/-/~ view lines in change-set output

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 11: Live introspection test for views

**Files:**
- Modify: `test/introspection_live_test.go` (the `View` arm of the dispatch added by PR #11)
- Modify: `test/testdata/posthog-create-statements/View/*.sql` if needed (verify a few fixtures exist)

- [ ] **Step 1: Verify view fixtures exist**

```
ls test/testdata/posthog-create-statements/View/ 2>&1 | head -10
```

Expected: at least a handful of `.sql` fixtures (PR #11 enabled the directory; PostHog's 17 views should already be there in some form). If the directory is empty or sparse, copy 2-3 simple `CREATE VIEW` fixtures from the production dump produced earlier — the cleaner ones, no exotic clauses. Put one bare view, one with `column_aliases`, one with `SQL SECURITY DEFINER`.

- [ ] **Step 2: Inspect the current View arm**

```
grep -n "groupName == \"View\"\|case \"View\"" test/introspection_live_test.go
```

PR #11 enabled the lookup (`chschema_v1.FindViewByName(state.Views, objectName)`) but didn't round-trip generated DDL. We don't yet emit MV/View DDL via `sqlgen.GenerateCreateTable`, so we keep the assertion to "introspection sees the view" — that's the minimum we can validate.

- [ ] **Step 3: Strengthen the View case to also assert ViewSpec fields**

Find the existing `case "View":` switch arm. Replace with:

```go
				case "View":
					// Find the view in the introspected DatabaseSpec (the
					// hcl-loader pathway, not the legacy protobuf state)
					// to assert the new ViewSpec fields. This complements
					// the legacy chschema_v1 lookup above by exercising
					// the new code path.
					hclState, err := hclload.Introspect(ctx, conn, dbName)
					require.NoError(t, err, "hcl introspect for View assertion")
					var v *hclload.ViewSpec
					for i := range hclState.Views {
						if hclState.Views[i].Name == objectName {
							v = &hclState.Views[i]
							break
						}
					}
					require.NotNil(t, v, "view %q should be in hcl introspection result", objectName)
					require.NotEmpty(t, v.Query, "view query must be captured")

					// Also keep the legacy protobuf-state lookup the
					// existing PR #11 code put in place.
					found := chschema_v1.FindViewByName(state.Views, objectName)
					require.NotNil(t, found, "View '%s' should be found after introspection", objectName)
```

If the existing arm only has the `chschema_v1.FindViewByName` lookup (i.e. PR #11 didn't add the hcl side yet), wrap both checks as shown above.

- [ ] **Step 4: Run the live tests**

Start the docker compose ClickHouse if not running:

```
docker compose up -d --wait
```

Then:

```
ENABLE_CLICKHOUSE=1 go test ./test -run 'TestLive_Introspection_AllStatements/View' -v 2>&1 | tail -20
```

Expected: every View subtest PASSes (or skip-list-entries skip cleanly if any fixture hits a parser gap; that should be visible from the warn output during the View processing arm of the dispatch — but views don't go through the createStubsForFixture path).

- [ ] **Step 5: Commit**

```
git add test/introspection_live_test.go
git commit -m "test(live): assert hcl ViewSpec captured by introspection

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 12: Docs

**Files:**
- Modify: `docs/README.hcl.md`
- Modify: `README.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update CLAUDE.md**

Find the "Not Yet Supported" line that says:
> ❌ `view` and `dictionary` top-level blocks — planned, not implemented

In CLAUDE.md, edit to remove `view` from that list (dictionaries are already done; we're removing the other half here too). Move views into the supported features list:

Find the bullet list of supported HCL features. After the materialized_view entry, add:

```markdown
- ✅ `view` blocks (plain non-materialized views): `query`, optional
  `column_aliases`, `sql_security` (definer | invoker | none),
  `definer`, `cluster`, `comment`
```

Also update the "Not Yet Supported" section by removing the `view` entry and leaving any remaining ones (Refreshable MV etc.) intact.

- [ ] **Step 2: Update docs/README.hcl.md**

Find the section structure heading:

```
grep -n "^### Materialized views\|^### Dictionaries\|^### Views" docs/README.hcl.md
```

If a `### Views` heading doesn't exist, add it immediately after `### Materialized views` and before `### Dictionaries`:

```markdown
### Views

A `view` block declares a ClickHouse **plain** (non-materialized) view —
a saved `SELECT` that's evaluated on every read of the view, returning
freshly-computed rows without persisted storage.

```hcl
database "posthog" {
  view "team_event_counts" {
    query = "SELECT team_id, count() AS n FROM posthog.events GROUP BY team_id"

    column_aliases = ["team_id", "n"]

    sql_security = "definer"
    definer      = "alice"

    cluster = "posthog"
    comment = "team-level event counter"
  }
}
```

| Attribute        | Required | Meaning |
|------------------|----------|---------|
| `query`          | yes      | the `AS SELECT ...` body |
| `column_aliases` | no       | `CREATE VIEW v (a, b, ...) AS ...` |
| `sql_security`   | no       | `SQL SECURITY` clause: `definer`, `invoker`, or `none` (canonical lowercase; case-insensitive on parse) |
| `definer`        | no       | `DEFINER = <user>` or `DEFINER = current_user`; only valid alongside `sql_security = "definer"` |
| `cluster`        | no       | `ON CLUSTER` target |
| `comment`        | no       | view comment |

`hclexp diff` reports a body change as in-place `ALTER TABLE ...
MODIFY QUERY`; a comment-only change becomes `ALTER TABLE ... MODIFY
COMMENT`; any change to `column_aliases` / `sql_security` / `definer`
/ `cluster` requires drop-and-recreate and is flagged unsafe.

**Not supported.** Live views, refreshable materialized views, and
window views fail introspection with a clear error.
```

- [ ] **Step 3: Update README.md**

Find the introspection coverage paragraph (search "Materialized views (TO-form), dictionaries"):

```
grep -n "Materialized views (TO-form)" README.md
```

Update it to also mention views:

```diff
- Materialized views (TO-form), dictionaries (every supported source +
- layout kind), and named collections are dumped in the same pass.
+ Materialized views (TO-form), plain views, dictionaries (every
+ supported source + layout kind), and named collections are dumped
+ in the same pass.
```

In the same file, find the four-mode listing at the top (the "What hclexp does" block):

```
grep -n "Materialized views" README.md | head -3
```

Make sure the four-mode listing already mentions "plain views" as well — if it only says "materialized views", expand to "plain views and materialized views".

- [ ] **Step 4: Commit**

```
git add CLAUDE.md docs/README.hcl.md README.md
git commit -m "docs: view block reference + supported-features update

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 13: Re-dump production schema to verify

**Files:**
- (no source changes; dump artifact only)

- [ ] **Step 1: Build hclexp**

```
go build -o /tmp/hclexp ./cmd/hclexp
```

Expected: builds clean.

- [ ] **Step 2: Dump the local CH posthog database**

```
/tmp/hclexp introspect -host localhost -port 9000 -user default -password "" -database posthog -out /tmp/posthog-with-views.hcl 2>&1 | tail -10
```

Expected: zero ERROR lines from view processing. WARN lines for the three known-broken objects (`adhoc_events_deletion`, `property_values`, `sharded_distinct_id_usage`) only — same as before.

- [ ] **Step 3: Confirm view count**

```
awk '/^database/ {db++} /^  view/ {v++} END {print "databases:"db, "views:"v}' /tmp/posthog-with-views.hcl
```

Expected: `databases:1 views:17` (all 17 PostHog views captured).

- [ ] **Step 4: Spot-check a few**

```
grep -A6 '^  view "custom_metrics"' /tmp/posthog-with-views.hcl | head -10
```

Expected: a `view "custom_metrics" {` block with a `query = ...` attribute. No DEFINER / SQL SECURITY for the PostHog views (we already established none use those).

- [ ] **Step 5: Save the verification dump alongside the previous one**

```
cp /tmp/posthog-with-views.hcl dumps/posthog-schema.hcl
git add dumps/posthog-schema.hcl
```

- [ ] **Step 6: Commit (only if `dumps/` is git-tracked; otherwise add to `.gitignore` instead)**

```
git status dumps/ 2>&1
```

If `dumps/` is untracked and there's already a `.gitignore` rule covering it, skip the commit. If not, run:

```
git commit -m "chore: refresh production schema dump after view support

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 14: PR verification + push

- [ ] **Step 1: Run the full test suite**

```
go test ./internal/... ./config ./cmd/... ./test/... 2>&1 | tail -15
```

Expected: all green.

```
gofmt -s -l .
```

Expected: empty (no files need reformatting).

```
go vet ./...
```

Expected: no issues.

- [ ] **Step 2: Confirm hclexp help still works**

```
go build -o /tmp/hclexp ./cmd/hclexp && /tmp/hclexp diff -h 2>&1 | head -10
```

Expected: usage text intact.

- [ ] **Step 3: Push the branch**

```
git push -u origin HEAD
```

- [ ] **Step 4: STOP — pause for human PR creation/review**

Open the PR via `gh pr create` (or the GitHub UI). The PR body should call out:
- Feature scope (views with full DEFINER / SQL SECURITY / column aliases per the design spec).
- Live verification on PostHog production schema: 17/17 views round-trip.
- One open item: parameterized views round-trip as opaque text (the parameter syntax is preserved verbatim inside the query body; no typed surfacing in HCL).
