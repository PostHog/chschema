# Per-Object Schema Comparison Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One per-object comparison model (`ObjectComparison` with
attribute-level old/new `FieldChange`s + reconciling DDL ops), emitted by
`diff -format json`, `plan`, and a reworked `drift -format json`, with native
`-exclude` filtering on all three and one shared text renderer.

**Architecture:** The model is a serialization of the existing `ChangeSet` —
no second diff engine. `BuildObjectComparisons(cs, gen, left, right)` walks
the same structs `renderChangeSet` walks today and attaches each generated op
to its object. JSON and text render from the same `[]ObjectComparison`, and
`CompareSummary` counts derive from it, so the three views cannot disagree.
Spec: `docs/plans/2026-07-10-object-comparison.md`.

**Tech Stack:** Go, HCL (hashicorp/hcl v2), testify.

## Global Constraints

- Never change the module name or go version in `go.mod`.
- Tests: testify `assert`/`require`; compare whole structs, not
  field-by-field. Run `go test ./internal/... -v` and `go test ./test -v`;
  snapshot refresh is `go test ./test -update-snapshots`.
- Commit style: 1-line summary, blank line, detailed description. NO AI
  attribution of any kind (no Co-Authored-By, no "Generated with" footers).
- The working tree carries unrelated in-flight work
  (`cmd/hclexp/hclexp.go`, `cmd/hclexp/load_manifest*.go`, `CLAUDE.md` for
  the load-manifest feature). Stage ONLY the files you touched, by exact
  path. Never `git add -A`, `-u`, or `.`.
- If `git commit` fails with "agent refused operation", commit signing is
  locked: ask the user to unlock, keep working uncommitted, retry once when
  they say go.
- Line numbers below are anchors from the tree at plan time; re-locate by
  symbol name if they've shifted.
- `Status` is right-relative everywhere: `added` = present only on the right
  side of the `Diff(left, right)` call. `diff`/`plan` put desired on the
  right; `drift` puts the drifter on the right.
- The `field` vocabulary and JSON keys are a public contract; do not improvise
  names beyond the ones specified here.

---

### Task 1: Comparison model types + table field changes

**Files:**
- Create: `internal/loader/hcl/compare.go`
- Test: `internal/loader/hcl/compare_test.go`

**Interfaces:**
- Produces: `ObjectComparison`, `FieldChange`, `CompareSummary`,
  `StatusAdded/StatusDropped/StatusAltered` consts,
  `fieldChangesForTable(td TableDiff) []FieldChange`,
  `columnDesc(c ColumnSpec) string`, `stringChangeField(field string, c *StringChange) FieldChange`.
- Consumes: `TableDiff`, `ColumnSpec`, `StringChange` (diff.go),
  `engineSQL` (sqlgen.go:927), `sortedKeys` (diff.go:654).

- [ ] **Step 1: Write the failing test**

`internal/loader/hcl/compare_test.go`:

```go
package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func strPtr(s string) *string { return &s }

// fieldChangesForTable flattens every TableDiff aspect into the documented
// field vocabulary, in declaration order (columns, indexes, projections,
// constraints, engine, order_by, primary_key, partition_by, sample_by, ttl,
// comment, settings).
func TestFieldChangesForTable(t *testing.T) {
	td := TableDiff{
		Table:         "events",
		RenameColumns: []RenameColumn{{Old: "ts_old", New: "ts"}},
		AddColumns:    []ColumnSpec{{Name: "added", Type: "UInt8", Default: strPtr("1")}},
		DropColumns:   []string{"gone"},
		ModifyColumns: []ColumnChange{{
			Name: "id",
			Old:  ColumnSpec{Name: "id", Type: "UInt32"},
			New:  ColumnSpec{Name: "id", Type: "UInt64", Codec: strPtr("ZSTD")},
		}},
		AddIndexes:        []IndexSpec{{Name: "idx_a", Expr: "id", Type: "minmax", Granularity: 1}},
		DropIndexes:       []string{"idx_b"},
		AddProjections:    []ProjectionSpec{{Name: "p_a", Query: "SELECT id ORDER BY id"}},
		DropProjections:   []string{"p_b"},
		AddConstraints:    []ConstraintSpec{{Name: "c_a", Check: strPtr("id > 0")}},
		DropConstraints:   []string{"c_b"},
		ModifyConstraints: []ConstraintChange{{Name: "c_c"}},
		EngineChange: &EngineChange{
			Old: EngineMergeTree{},
			New: EngineReplacingMergeTree{},
		},
		OrderByChange:     &OrderByChange{Old: []string{"id"}, New: []string{"id", "ts"}},
		PrimaryKeyChange:  &OrderByChange{Old: []string{"id"}, New: []string{"ts"}},
		PartitionByChange: &StringChange{Old: nil, New: strPtr("toYYYYMM(ts)")},
		SampleByChange:    &StringChange{Old: strPtr("id"), New: nil},
		TTLChange:         &StringChange{Old: strPtr("ts + INTERVAL 1 DAY"), New: strPtr("ts + INTERVAL 7 DAY")},
		CommentChange:     &StringChange{Old: strPtr("old"), New: strPtr("new")},
		SettingsAdded:     map[string]string{"ttl_only_drop_parts": "1"},
		SettingsRemoved:   []string{"old_setting"},
		SettingsChanged:   []SettingChange{{Key: "index_granularity", OldValue: "8192", NewValue: "4096"}},
	}

	engOld, _ := engineSQL(EngineMergeTree{})
	engNew, _ := engineSQL(EngineReplacingMergeTree{})

	want := []FieldChange{
		{Field: "column:ts", Change: "rename", Old: "ts_old", New: "ts"},
		{Field: "column:added", Change: "add", New: "UInt8 DEFAULT 1"},
		{Field: "column:gone", Change: "drop"},
		{Field: "column:id", Change: "modify", Old: "UInt32", New: "UInt64 CODEC(ZSTD)"},
		{Field: "index:idx_a", Change: "add"},
		{Field: "index:idx_b", Change: "drop"},
		{Field: "projection:p_a", Change: "add"},
		{Field: "projection:p_b", Change: "drop"},
		{Field: "constraint:c_a", Change: "add"},
		{Field: "constraint:c_b", Change: "drop"},
		{Field: "constraint:c_c", Change: "modify"},
		{Field: "engine", Change: "modify", Old: engOld, New: engNew},
		{Field: "order_by", Change: "modify", Old: "id", New: "id, ts"},
		{Field: "primary_key", Change: "modify", Old: "id", New: "ts"},
		{Field: "partition_by", Change: "modify", New: "toYYYYMM(ts)"},
		{Field: "sample_by", Change: "modify", Old: "id"},
		{Field: "ttl", Change: "modify", Old: "ts + INTERVAL 1 DAY", New: "ts + INTERVAL 7 DAY"},
		{Field: "comment", Change: "modify", Old: "old", New: "new"},
		{Field: "setting:ttl_only_drop_parts", Change: "add", New: "1"},
		{Field: "setting:old_setting", Change: "drop"},
		{Field: "setting:index_granularity", Change: "modify", Old: "8192", New: "4096"},
	}
	assert.Equal(t, want, fieldChangesForTable(td))
}

func TestColumnDesc(t *testing.T) {
	c := ColumnSpec{Name: "v", Type: "String", Nullable: true,
		Materialized: strPtr("upper(s)"), Codec: strPtr("LZ4"),
		TTL: strPtr("d + INTERVAL 1 DAY"), Comment: strPtr("c")}
	assert.Equal(t,
		"Nullable(String) MATERIALIZED upper(s) CODEC(LZ4) TTL d + INTERVAL 1 DAY COMMENT c",
		columnDesc(c))
}
```

If `ConstraintSpec`/`IndexSpec`/`ProjectionSpec` field names differ, fix the
test literals against `internal/loader/hcl/types.go` — do not change the
expected `FieldChange` values. If a `strPtr` helper already exists in the
package tests, reuse it and drop the local definition.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/loader/hcl/ -run 'TestFieldChangesForTable|TestColumnDesc' -v`
Expected: FAIL (compile error: undefined `FieldChange`, `fieldChangesForTable`, `columnDesc`)

- [ ] **Step 3: Write the implementation**

`internal/loader/hcl/compare.go`:

```go
package hcl

import "strings"

// Comparison statuses. Status is right-relative: "added" means the right
// side of the Diff has the object and the left does not.
const (
	StatusAdded   = "added"
	StatusDropped = "dropped"
	StatusAltered = "altered"
)

// ObjectComparison describes how one object differs between two schemas. It
// is a projection of the ChangeSet — the same structs the text summary
// renders — so JSON, text, and counts can never contradict each other.
type ObjectComparison struct {
	Database     string          `json:"database"`    // empty for named collections
	Object       string          `json:"object"`
	ObjectType   string          `json:"object_type"` // table | materialized_view | view | dictionary | named_collection | raw
	Status       string          `json:"status"`      // added | dropped | altered
	Changes      []FieldChange   `json:"changes,omitempty"` // altered only
	Operations   []JSONOperation `json:"operations"`  // the DDL that reconciles this object; may be empty (unsafe-only changes)
	Unsafe       bool            `json:"unsafe"`
	UnsafeReason string          `json:"unsafe_reason,omitempty"`
	Error        string          `json:"error,omitempty"` // unsupported transition (e.g. named collection external<->managed)
}

// FieldChange is one attribute-level difference on an altered object. The
// Field vocabulary is a public contract, documented in docs/README.hcl.md.
type FieldChange struct {
	Field  string `json:"field"`
	Change string `json:"change"` // add | drop | modify | rename
	Old    string `json:"old,omitempty"`
	New    string `json:"new,omitempty"`
}

// CompareSummary counts comparisons by object type and status.
type CompareSummary struct {
	TablesAdded             int `json:"tables_added"`
	TablesDropped           int `json:"tables_dropped"`
	TablesAltered           int `json:"tables_altered"`
	MVsAdded                int `json:"mvs_added"`
	MVsDropped              int `json:"mvs_dropped"`
	MVsAltered              int `json:"mvs_altered"`
	ViewsAdded              int `json:"views_added"`
	ViewsDropped            int `json:"views_dropped"`
	ViewsAltered            int `json:"views_altered"`
	DictsAdded              int `json:"dicts_added"`
	DictsDropped            int `json:"dicts_dropped"`
	DictsAltered            int `json:"dicts_altered"`
	RawsAdded               int `json:"raws_added"`
	RawsDropped             int `json:"raws_dropped"`
	RawsAltered             int `json:"raws_altered"`
	NamedCollectionsChanged int `json:"named_collections_changed"`
}

// fieldChangesForTable flattens a TableDiff into attribute-level changes.
func fieldChangesForTable(td TableDiff) []FieldChange {
	var out []FieldChange
	for _, r := range td.RenameColumns {
		out = append(out, FieldChange{Field: "column:" + r.New, Change: "rename", Old: r.Old, New: r.New})
	}
	for _, c := range td.AddColumns {
		out = append(out, FieldChange{Field: "column:" + c.Name, Change: "add", New: columnDesc(c)})
	}
	for _, name := range td.DropColumns {
		out = append(out, FieldChange{Field: "column:" + name, Change: "drop"})
	}
	for _, c := range td.ModifyColumns {
		out = append(out, FieldChange{Field: "column:" + c.Name, Change: "modify", Old: columnDesc(c.Old), New: columnDesc(c.New)})
	}
	for _, idx := range td.AddIndexes {
		out = append(out, FieldChange{Field: "index:" + idx.Name, Change: "add"})
	}
	for _, name := range td.DropIndexes {
		out = append(out, FieldChange{Field: "index:" + name, Change: "drop"})
	}
	for _, p := range td.AddProjections {
		out = append(out, FieldChange{Field: "projection:" + p.Name, Change: "add"})
	}
	for _, name := range td.DropProjections {
		out = append(out, FieldChange{Field: "projection:" + name, Change: "drop"})
	}
	for _, c := range td.AddConstraints {
		out = append(out, FieldChange{Field: "constraint:" + c.Name, Change: "add"})
	}
	for _, name := range td.DropConstraints {
		out = append(out, FieldChange{Field: "constraint:" + name, Change: "drop"})
	}
	for _, c := range td.ModifyConstraints {
		out = append(out, FieldChange{Field: "constraint:" + c.Name, Change: "modify"})
	}
	if c := td.EngineChange; c != nil {
		oldSQL, _ := engineSQL(c.Old)
		newSQL, _ := engineSQL(c.New)
		out = append(out, FieldChange{Field: "engine", Change: "modify", Old: oldSQL, New: newSQL})
	}
	if c := td.OrderByChange; c != nil {
		out = append(out, FieldChange{Field: "order_by", Change: "modify",
			Old: strings.Join(c.Old, ", "), New: strings.Join(c.New, ", ")})
	}
	if c := td.PrimaryKeyChange; c != nil {
		out = append(out, FieldChange{Field: "primary_key", Change: "modify",
			Old: strings.Join(c.Old, ", "), New: strings.Join(c.New, ", ")})
	}
	if c := td.PartitionByChange; c != nil {
		out = append(out, stringChangeField("partition_by", c))
	}
	if c := td.SampleByChange; c != nil {
		out = append(out, stringChangeField("sample_by", c))
	}
	if c := td.TTLChange; c != nil {
		out = append(out, stringChangeField("ttl", c))
	}
	if c := td.CommentChange; c != nil {
		out = append(out, stringChangeField("comment", c))
	}
	for _, k := range sortedKeys(td.SettingsAdded) {
		out = append(out, FieldChange{Field: "setting:" + k, Change: "add", New: td.SettingsAdded[k]})
	}
	for _, k := range td.SettingsRemoved {
		out = append(out, FieldChange{Field: "setting:" + k, Change: "drop"})
	}
	for _, c := range td.SettingsChanged {
		out = append(out, FieldChange{Field: "setting:" + c.Key, Change: "modify", Old: c.OldValue, New: c.NewValue})
	}
	return out
}

// stringChangeField maps an optional-string transition to a FieldChange; a
// nil side stays empty and is omitted from JSON.
func stringChangeField(field string, c *StringChange) FieldChange {
	fc := FieldChange{Field: field, Change: "modify"}
	if c.Old != nil {
		fc.Old = *c.Old
	}
	if c.New != nil {
		fc.New = *c.New
	}
	return fc
}

// columnDesc renders a compact one-line column descriptor (type plus default
// form and codec/ttl/comment markers) for FieldChange values and the text
// summary.
func columnDesc(c ColumnSpec) string {
	t := c.Type
	if c.Nullable {
		t = "Nullable(" + t + ")"
	}
	switch {
	case c.Alias != nil:
		t += " ALIAS " + *c.Alias
	case c.Materialized != nil:
		t += " MATERIALIZED " + *c.Materialized
	case c.Ephemeral != nil:
		t += " EPHEMERAL"
	case c.Default != nil:
		t += " DEFAULT " + *c.Default
	}
	if c.Codec != nil {
		t += " CODEC(" + *c.Codec + ")"
	}
	if c.TTL != nil {
		t += " TTL " + *c.TTL
	}
	if c.Comment != nil {
		t += " COMMENT " + *c.Comment
	}
	return t
}
```

`columnDesc` is a verbatim port of `colDesc` in `cmd/hclexp/hclexp.go:1161`
(the cmd copy is deleted in Task 5). If `engineSQL`'s clause for
`EngineMergeTree{}` includes an `ENGINE = ` prefix, the test's `engOld`
derivation absorbs it — the assertion uses whatever `engineSQL` returns.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/loader/hcl/ -run 'TestFieldChangesForTable|TestColumnDesc' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/loader/hcl/compare.go internal/loader/hcl/compare_test.go
git commit -m "compare: object comparison model and table field changes

ObjectComparison/FieldChange/CompareSummary types plus the TableDiff
flattener, per docs/plans/2026-07-10-object-comparison.md. The field
vocabulary (column:/index:/projection:/constraint:/setting:/engine/
order_by/...) is the public contract for structured diff output."
```

---

### Task 2: Field changes for MV, view, dictionary, raw, named collection

**Files:**
- Modify: `internal/loader/hcl/diff.go` (MaterializedViewDiff ~line 71,
  ViewDiff ~line 93, `diffView` ~line 601, `diffMaterializedView` ~line 633)
- Modify: `internal/loader/hcl/compare.go`
- Modify: `docs/plans/2026-07-10-object-comparison.md` (spec addendum)
- Test: `internal/loader/hcl/compare_test.go`

**Interfaces:**
- Produces: `fieldChangesForMaterializedView(mvd MaterializedViewDiff) []FieldChange`,
  `fieldChangesForView(vd ViewDiff) []FieldChange`,
  `fieldChangesForDictionary(dd DictionaryDiff) []FieldChange`,
  `fieldChangesForRaw(rc RawChange) []FieldChange`,
  `fieldChangesForNamedCollection(ncc NamedCollectionChange) []FieldChange`;
  new diff fields `MaterializedViewDiff.ToTableChange *StringChange`,
  `MaterializedViewDiff.ColumnsChanged bool`, `ViewDiff.RecreateChanged []string`.
- Consumes: Task 1's `FieldChange`, `stringChangeField`.

Why the diff.go change: `MaterializedViewDiff` today records only
`Recreate bool`, so "to_table changed" vs "columns changed" is
indistinguishable; same for `ViewDiff.Recreate`'s four possible causes. The
diff functions already compute the distinction and throw it away — record it.

- [ ] **Step 1: Write the failing tests** (append to `compare_test.go`)

```go
func TestFieldChangesForMaterializedView(t *testing.T) {
	// Structural change: to_table and columns both differ.
	mvd := diffMaterializedView(
		&MaterializedViewSpec{Name: "mv", ToTable: "t_old", Query: "SELECT 1",
			Columns: []ColumnSpec{{Name: "a", Type: "UInt8"}}},
		&MaterializedViewSpec{Name: "mv", ToTable: "t_new", Query: "SELECT 1",
			Columns: []ColumnSpec{{Name: "b", Type: "UInt8"}}},
	)
	assert.True(t, mvd.Recreate)
	assert.Equal(t, []FieldChange{
		{Field: "to_table", Change: "modify", Old: "t_old", New: "t_new"},
		{Field: "columns", Change: "modify"},
	}, fieldChangesForMaterializedView(mvd))

	// Query-only change.
	mvd = diffMaterializedView(
		&MaterializedViewSpec{Name: "mv", ToTable: "t", Query: "SELECT 1"},
		&MaterializedViewSpec{Name: "mv", ToTable: "t", Query: "SELECT 2"},
	)
	assert.False(t, mvd.Recreate)
	assert.Equal(t, []FieldChange{
		{Field: "query", Change: "modify", Old: "SELECT 1", New: "SELECT 2"},
	}, fieldChangesForMaterializedView(mvd))
}

func TestFieldChangesForView(t *testing.T) {
	// Recreate: sql_security and cluster changed; each surfaces by name.
	vd := diffView(
		&ViewSpec{Name: "v", Query: "SELECT 1", SQLSecurity: strPtr("DEFINER")},
		&ViewSpec{Name: "v", Query: "SELECT 1", Cluster: strPtr("main")},
	)
	assert.True(t, vd.Recreate)
	assert.Equal(t, []FieldChange{
		{Field: "sql_security", Change: "modify"},
		{Field: "cluster", Change: "modify"},
	}, fieldChangesForView(vd))

	// In-place: query + comment.
	vd = diffView(
		&ViewSpec{Name: "v", Query: "SELECT 1", Comment: strPtr("a")},
		&ViewSpec{Name: "v", Query: "SELECT 2", Comment: strPtr("b")},
	)
	assert.Equal(t, []FieldChange{
		{Field: "query", Change: "modify", Old: "SELECT 1", New: "SELECT 2"},
		{Field: "comment", Change: "modify", Old: "a", New: "b"},
	}, fieldChangesForView(vd))
}

func TestFieldChangesForDictionaryRawNamedCollection(t *testing.T) {
	assert.Equal(t, []FieldChange{
		{Field: "layout", Change: "modify"},
		{Field: "source.clickhouse.table", Change: "modify"},
	}, fieldChangesForDictionary(DictionaryDiff{Name: "d", Changed: []string{"layout", "source.clickhouse.table"}}))

	assert.Equal(t, []FieldChange{
		{Field: "sql", Change: "modify", Old: "CREATE TABLE a", New: "CREATE TABLE b"},
	}, fieldChangesForRaw(RawChange{Kind: "table", Name: "r", OldSQL: "CREATE TABLE a", NewSQL: "CREATE TABLE b"}))

	ncc := NamedCollectionChange{
		Name:                  "s3",
		SetParams:             []NamedCollectionParam{{Key: "url", Value: "https://b"}},
		DeleteParams:          []string{"token"},
		SkippedRedactedParams: []string{"secret"},
		CommentChange:         &StringChange{Old: strPtr("a"), New: strPtr("b")},
	}
	assert.Equal(t, []FieldChange{
		{Field: "param:url", Change: "modify", New: "https://b"},
		{Field: "param:token", Change: "drop"},
		{Field: "param:secret", Change: "modify", Old: "[HIDDEN]", New: "[HIDDEN]"},
		{Field: "comment", Change: "modify", Old: "a", New: "b"},
	}, fieldChangesForNamedCollection(ncc))

	assert.Equal(t, []FieldChange{
		{Field: "on_cluster", Change: "modify"},
	}, fieldChangesForNamedCollection(NamedCollectionChange{Name: "s3", Recreate: true}))
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/loader/hcl/ -run 'TestFieldChangesFor' -v`
Expected: FAIL (undefined functions; `ToTableChange` unknown field)

- [ ] **Step 3: Extend the diff structs and functions**

In `internal/loader/hcl/diff.go`, extend `MaterializedViewDiff`:

```go
type MaterializedViewDiff struct {
	Name        string
	QueryChange *StringChange // the AS SELECT body changed
	Recreate    bool          // to_table or the column list changed

	// Set alongside Recreate so consumers can tell WHAT forced it.
	ToTableChange  *StringChange
	ColumnsChanged bool
}
```

Rewrite `diffMaterializedView` (diff.go:633):

```go
func diffMaterializedView(from, to *MaterializedViewSpec) MaterializedViewDiff {
	mvd := MaterializedViewDiff{Name: to.Name}
	if from.ToTable != to.ToTable {
		o, n := from.ToTable, to.ToTable
		mvd.ToTableChange = &StringChange{Old: &o, New: &n}
	}
	if !reflect.DeepEqual(from.Columns, to.Columns) {
		mvd.ColumnsChanged = true
	}
	if mvd.ToTableChange != nil || mvd.ColumnsChanged {
		mvd.Recreate = true
		return mvd
	}
	if from.Query != to.Query {
		q1, q2 := from.Query, to.Query
		mvd.QueryChange = &StringChange{Old: &q1, New: &q2}
	}
	return mvd
}
```

Extend `ViewDiff` with `RecreateChanged []string` (the attributes that forced
recreation, in a fixed order) and rewrite `diffView` (diff.go:601):

```go
func diffView(from, to *ViewSpec) ViewDiff {
	vd := ViewDiff{Name: to.Name}
	if !reflect.DeepEqual(from.ColumnAliases, to.ColumnAliases) {
		vd.RecreateChanged = append(vd.RecreateChanged, "column_aliases")
	}
	if !reflect.DeepEqual(from.SQLSecurity, to.SQLSecurity) {
		vd.RecreateChanged = append(vd.RecreateChanged, "sql_security")
	}
	if !reflect.DeepEqual(from.Definer, to.Definer) {
		vd.RecreateChanged = append(vd.RecreateChanged, "definer")
	}
	if !reflect.DeepEqual(from.Cluster, to.Cluster) {
		vd.RecreateChanged = append(vd.RecreateChanged, "cluster")
	}
	if len(vd.RecreateChanged) > 0 {
		vd.Recreate = true
		return vd
	}
	if from.Query != to.Query {
		q1, q2 := from.Query, to.Query
		vd.QueryChange = &StringChange{Old: &q1, New: &q2}
	}
	if !reflect.DeepEqual(from.Comment, to.Comment) {
		vd.Comment = &StringChange{Old: from.Comment, New: to.Comment}
	}
	return vd
}
```

`IsEmpty`/`IsUnsafe` for both structs stay unchanged (they key off
`Recreate`, which keeps its meaning).

Append to `internal/loader/hcl/compare.go`:

```go
// fieldChangesForMaterializedView flattens an MV diff. A structural change
// (to_table / columns) implies recreation; query-only maps to MODIFY QUERY.
func fieldChangesForMaterializedView(mvd MaterializedViewDiff) []FieldChange {
	var out []FieldChange
	if c := mvd.ToTableChange; c != nil {
		out = append(out, stringChangeField("to_table", c))
	}
	if mvd.ColumnsChanged {
		out = append(out, FieldChange{Field: "columns", Change: "modify"})
	}
	if c := mvd.QueryChange; c != nil {
		out = append(out, stringChangeField("query", c))
	}
	return out
}

func fieldChangesForView(vd ViewDiff) []FieldChange {
	var out []FieldChange
	for _, attr := range vd.RecreateChanged {
		out = append(out, FieldChange{Field: attr, Change: "modify"})
	}
	if c := vd.QueryChange; c != nil {
		out = append(out, stringChangeField("query", c))
	}
	if c := vd.Comment; c != nil {
		out = append(out, stringChangeField("comment", c))
	}
	return out
}

// fieldChangesForDictionary maps each changed field path to one modify entry
// (ClickHouse dictionaries reconcile via CREATE OR REPLACE, so there are no
// per-field old/new values in the diff).
func fieldChangesForDictionary(dd DictionaryDiff) []FieldChange {
	out := make([]FieldChange, 0, len(dd.Changed))
	for _, path := range dd.Changed {
		out = append(out, FieldChange{Field: path, Change: "modify"})
	}
	return out
}

// fieldChangesForRaw: raw SQL is opaque, the whole stored DDL is the value.
func fieldChangesForRaw(rc RawChange) []FieldChange {
	return []FieldChange{{Field: "sql", Change: "modify", Old: rc.OldSQL, New: rc.NewSQL}}
}

// fieldChangesForNamedCollection flattens the surgical NC changes. SetParams
// has no old value (ALTER ... SET overwrites); a redacted param reports
// [HIDDEN] on both sides — the diff could not verify equality.
func fieldChangesForNamedCollection(ncc NamedCollectionChange) []FieldChange {
	var out []FieldChange
	if ncc.Recreate {
		out = append(out, FieldChange{Field: "on_cluster", Change: "modify"})
	}
	for _, p := range ncc.SetParams {
		out = append(out, FieldChange{Field: "param:" + p.Key, Change: "modify", New: p.Value})
	}
	for _, k := range ncc.DeleteParams {
		out = append(out, FieldChange{Field: "param:" + k, Change: "drop"})
	}
	for _, k := range ncc.SkippedRedactedParams {
		out = append(out, FieldChange{Field: "param:" + k, Change: "modify", Old: "[HIDDEN]", New: "[HIDDEN]"})
	}
	if c := ncc.CommentChange; c != nil {
		out = append(out, stringChangeField("comment", c))
	}
	return out
}
```

- [ ] **Step 4: Run the package tests**

Run: `go test ./internal/loader/hcl/ -v`
Expected: PASS (including pre-existing diff tests — the new fields are
additive and `Recreate` semantics are unchanged)

- [ ] **Step 5: Record the spec addendum**

In `docs/plans/2026-07-10-object-comparison.md`, in the `field` vocabulary
section, replace the view row and add the discovered details:
- `query`, `comment` → view (in-place); `column_aliases`, `sql_security`,
  `definer`, `cluster` → view (each forces recreate)
- note that `param:<name>` set-changes carry `new` only (SET overwrites; the
  diff holds no old value)
- note the `MaterializedViewDiff.ToTableChange`/`ColumnsChanged` and
  `ViewDiff.RecreateChanged` diff-engine additions.

- [ ] **Step 6: Commit**

```bash
git add internal/loader/hcl/diff.go internal/loader/hcl/compare.go \
  internal/loader/hcl/compare_test.go docs/plans/2026-07-10-object-comparison.md
git commit -m "compare: field changes for MV, view, dictionary, raw, named collection

MaterializedViewDiff now records which structural attribute (to_table /
columns) forced a recreate, and ViewDiff records the recreate-forcing
attributes by name — the diff functions computed the distinction and
discarded it. The comparison flatteners surface them as documented
field-vocabulary entries."
```

---

### Task 3: BuildObjectComparisons + SummarizeComparisons + OneLiner

**Files:**
- Modify: `internal/loader/hcl/render_json.go` (extract `buildJSONOperations`
  from `RenderDiffJSON`, line 51)
- Modify: `internal/loader/hcl/compare.go`
- Test: `internal/loader/hcl/compare_test.go`

**Interfaces:**
- Produces:
  `BuildObjectComparisons(cs ChangeSet, gen GeneratedSQL, left, right *Schema) []ObjectComparison`,
  `SummarizeComparisons(objs []ObjectComparison) CompareSummary`,
  `(s CompareSummary) OneLiner() string`,
  `buildJSONOperations(gen GeneratedSQL, left, right *Schema) []JSONOperation` (unexported).
- Consumes: Tasks 1–2 flatteners; `unsafeFor`, `engineFor` (render_json.go);
  `KindTable`... consts (render.go:13, sqlgen.go:42); test helpers `mkTable`,
  `mkDB` (existing package test helpers used by render_json_test.go).

- [ ] **Step 1: Write the failing tests** (append to `compare_test.go`)

```go
// Drives the real Diff -> GenerateSQL -> BuildObjectComparisons pipeline:
// one added table (with its CREATE op attached, keeping the global order
// index), one altered table whose ORDER BY change is unsafe, and direction
// pinning (added = present only on the right).
func TestBuildObjectComparisons(t *testing.T) {
	idCol := ColumnSpec{Name: "id", Type: "UInt64"}
	tsCol := ColumnSpec{Name: "ts", Type: "DateTime"}

	sessionsLeft := mkTable("sessions", EngineMergeTree{}, idCol)
	sessionsLeft.OrderBy = []string{"id"}
	sessionsRight := mkTable("sessions", EngineMergeTree{}, idCol)
	sessionsRight.OrderBy = []string{"id", "ts"} // unsafe, no DDL emitted
	archive := mkTable("archive", EngineMergeTree{}, idCol, tsCol)
	archive.OrderBy = []string{"id"}

	left := &Schema{Databases: []DatabaseSpec{mkDB("posthog", sessionsLeft)}}
	right := &Schema{Databases: []DatabaseSpec{mkDB("posthog", archive, sessionsRight)}}

	cs := Diff(left, right)
	gen := GenerateSQL(cs)
	objs := BuildObjectComparisons(cs, gen, left, right)

	byName := map[string]ObjectComparison{}
	for _, o := range objs {
		byName[o.Object] = o
	}

	created := byName["archive"]
	assert.Equal(t, StatusAdded, created.Status)
	assert.Equal(t, KindTable, created.ObjectType)
	assert.Equal(t, "posthog", created.Database)
	require.Len(t, created.Operations, 1)
	assert.Equal(t, OpCreate, created.Operations[0].Kind)
	assert.Contains(t, created.Operations[0].SQL, "CREATE TABLE")
	assert.False(t, created.Unsafe)

	altered := byName["sessions"]
	assert.Equal(t, StatusAltered, altered.Status)
	assert.True(t, altered.Unsafe)
	assert.Contains(t, altered.UnsafeReason, "ORDER BY")
	assert.Empty(t, altered.Operations) // unsafe-only change emits no DDL
	assert.Equal(t, []FieldChange{
		{Field: "order_by", Change: "modify", Old: "id", New: "id, ts"},
	}, altered.Changes)

	// Nested op Order is the index into the global dependency-sorted list.
	globalOps := buildJSONOperations(gen, left, right)
	for _, o := range objs {
		for _, op := range o.Operations {
			assert.Equal(t, globalOps[op.Order], op)
		}
	}
}

func TestSummarizeComparisonsAndOneLiner(t *testing.T) {
	objs := []ObjectComparison{
		{ObjectType: KindTable, Status: StatusAdded},
		{ObjectType: KindTable, Status: StatusAltered},
		{ObjectType: KindMaterializedView, Status: StatusDropped},
		{ObjectType: KindRaw, Status: StatusAltered},
		{ObjectType: KindNamedCollection, Status: StatusAltered},
	}
	s := SummarizeComparisons(objs)
	assert.Equal(t, CompareSummary{
		TablesAdded: 1, TablesAltered: 1,
		MVsDropped: 1, RawsAltered: 1, NamedCollectionsChanged: 1,
	}, s)
	assert.Equal(t, "+1 table, ~1 table, -1 mv, ~1 raw, ~1 named_collection", s.OneLiner())
	assert.Equal(t, "changed", CompareSummary{}.OneLiner())
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/loader/hcl/ -run 'TestBuildObjectComparisons|TestSummarizeComparisons' -v`
Expected: FAIL (undefined `BuildObjectComparisons`, `buildJSONOperations`, ...)

- [ ] **Step 3: Extract buildJSONOperations (pure refactor)**

In `internal/loader/hcl/render_json.go`, move `RenderDiffJSON`'s loop body
into:

```go
// buildJSONOperations enriches the generated ops with their global order,
// engine family (target schema first, falling back to current — an ALTER
// that doesn't change the engine carries none of its own), and unsafe flags.
func buildJSONOperations(gen GeneratedSQL, left, right *Schema) []JSONOperation {
	ops := make([]JSONOperation, 0, len(gen.Ops))
	for i, op := range gen.Ops {
		engine := ""
		if op.ObjectType == KindTable {
			engine = engineFor(op.Database, op.Object, right, left)
		}
		unsafe, reason := unsafeFor(gen.Unsafe, op.Database, op.Object)
		ops = append(ops, JSONOperation{
			Order:        i,
			Kind:         op.Kind,
			ObjectType:   op.ObjectType,
			Database:     op.Database,
			Object:       op.Object,
			Engine:       engine,
			Replicated:   strings.HasPrefix(engine, "Replicated"),
			SQL:          op.SQL,
			Manual:       op.Manual,
			Unsafe:       unsafe,
			UnsafeReason: reason,
		})
	}
	return ops
}
```

and have `RenderDiffJSON` call it (`doc.Operations = buildJSONOperations(gen,
left, right)`). Run `go test ./internal/loader/hcl/ -run TestRenderDiffJSON -v`
— must still PASS before continuing.

- [ ] **Step 4: Implement BuildObjectComparisons and the summary**

Append to `internal/loader/hcl/compare.go` (add `"fmt"` to imports):

```go
// BuildObjectComparisons flattens a ChangeSet into one entry per differing
// object, attaching each generated operation to its object. Status is
// right-relative; nested operations keep their global dependency order, so
// the object view and the flat operation list can never disagree about
// sequencing.
func BuildObjectComparisons(cs ChangeSet, gen GeneratedSQL, left, right *Schema) []ObjectComparison {
	type ref struct{ db, object string }
	opsByRef := map[ref][]JSONOperation{}
	for _, op := range buildJSONOperations(gen, left, right) {
		k := ref{op.Database, op.Object}
		opsByRef[k] = append(opsByRef[k], op)
	}

	var out []ObjectComparison
	add := func(db, name, objType, status string, changes []FieldChange) int {
		oc := ObjectComparison{
			Database: db, Object: name, ObjectType: objType, Status: status,
			Changes: changes, Operations: opsByRef[ref{db, name}],
		}
		if oc.Operations == nil {
			oc.Operations = []JSONOperation{}
		}
		oc.Unsafe, oc.UnsafeReason = unsafeFor(gen.Unsafe, db, name)
		out = append(out, oc)
		return len(out) - 1
	}

	for _, dc := range cs.Databases {
		for _, t := range dc.AddTables {
			add(dc.Database, t.Name, KindTable, StatusAdded, nil)
		}
		for _, t := range dc.DropTables {
			add(dc.Database, t.Name, KindTable, StatusDropped, nil)
		}
		for _, td := range dc.AlterTables {
			add(dc.Database, td.Table, KindTable, StatusAltered, fieldChangesForTable(td))
		}
		for _, mv := range dc.AddMaterializedViews {
			add(dc.Database, mv.Name, KindMaterializedView, StatusAdded, nil)
		}
		for _, name := range dc.DropMaterializedViews {
			add(dc.Database, name, KindMaterializedView, StatusDropped, nil)
		}
		for _, mvd := range dc.AlterMaterializedViews {
			add(dc.Database, mvd.Name, KindMaterializedView, StatusAltered, fieldChangesForMaterializedView(mvd))
		}
		for _, v := range dc.AddViews {
			add(dc.Database, v.Name, KindView, StatusAdded, nil)
		}
		for _, name := range dc.DropViews {
			add(dc.Database, name, KindView, StatusDropped, nil)
		}
		for _, vd := range dc.AlterViews {
			add(dc.Database, vd.Name, KindView, StatusAltered, fieldChangesForView(vd))
		}
		for _, d := range dc.AddDictionaries {
			add(dc.Database, d.Name, KindDictionary, StatusAdded, nil)
		}
		for _, name := range dc.DropDictionaries {
			add(dc.Database, name, KindDictionary, StatusDropped, nil)
		}
		for _, dd := range dc.AlterDictionaries {
			add(dc.Database, dd.Name, KindDictionary, StatusAltered, fieldChangesForDictionary(dd))
		}
		for _, r := range dc.AddRaws {
			add(dc.Database, r.Name, KindRaw, StatusAdded, nil)
		}
		for _, r := range dc.DropRaws {
			add(dc.Database, r.Name, KindRaw, StatusDropped, nil)
		}
		for _, rc := range dc.AlterRaws {
			i := add(dc.Database, rc.Name, KindRaw, StatusAltered, fieldChangesForRaw(rc))
			if rc.IsUnsafe() && !out[i].Unsafe {
				out[i].Unsafe = true
				out[i].UnsafeReason = "recreating a raw table drops its data"
			}
		}
	}

	for _, ncc := range cs.NamedCollections {
		status := StatusAltered
		switch {
		case ncc.Recreate:
			// DROP+CREATE pair, surfaced as altered with an on_cluster change
		case ncc.Add != nil:
			status = StatusAdded
		case ncc.Drop:
			status = StatusDropped
		}
		i := add("", ncc.Name, KindNamedCollection, status, fieldChangesForNamedCollection(ncc))
		out[i].Error = ncc.Error
	}
	return out
}

// SummarizeComparisons counts comparisons by object type and status.
func SummarizeComparisons(objs []ObjectComparison) CompareSummary {
	var s CompareSummary
	for _, o := range objs {
		var added, dropped, altered *int
		switch o.ObjectType {
		case KindTable:
			added, dropped, altered = &s.TablesAdded, &s.TablesDropped, &s.TablesAltered
		case KindMaterializedView:
			added, dropped, altered = &s.MVsAdded, &s.MVsDropped, &s.MVsAltered
		case KindView:
			added, dropped, altered = &s.ViewsAdded, &s.ViewsDropped, &s.ViewsAltered
		case KindDictionary:
			added, dropped, altered = &s.DictsAdded, &s.DictsDropped, &s.DictsAltered
		case KindRaw:
			added, dropped, altered = &s.RawsAdded, &s.RawsDropped, &s.RawsAltered
		case KindNamedCollection:
			s.NamedCollectionsChanged++
			continue
		default:
			continue
		}
		switch o.Status {
		case StatusAdded:
			*added++
		case StatusDropped:
			*dropped++
		case StatusAltered:
			*altered++
		}
	}
	return s
}

// OneLiner renders the summary as the compact drift line, e.g.
// "+1 table, ~2 mv, ~1 raw". An all-zero summary falls back to "changed".
func (s CompareSummary) OneLiner() string {
	var parts []string
	add := func(label string, plus, minus, alt int) {
		if plus > 0 {
			parts = append(parts, fmt.Sprintf("+%d %s", plus, label))
		}
		if minus > 0 {
			parts = append(parts, fmt.Sprintf("-%d %s", minus, label))
		}
		if alt > 0 {
			parts = append(parts, fmt.Sprintf("~%d %s", alt, label))
		}
	}
	add("table", s.TablesAdded, s.TablesDropped, s.TablesAltered)
	add("mv", s.MVsAdded, s.MVsDropped, s.MVsAltered)
	add("dict", s.DictsAdded, s.DictsDropped, s.DictsAltered)
	add("view", s.ViewsAdded, s.ViewsDropped, s.ViewsAltered)
	add("raw", s.RawsAdded, s.RawsDropped, s.RawsAltered)
	if s.NamedCollectionsChanged > 0 {
		parts = append(parts, fmt.Sprintf("~%d named_collection", s.NamedCollectionsChanged))
	}
	if len(parts) == 0 {
		return "changed"
	}
	return strings.Join(parts, ", ")
}
```

Note the OneLiner label order matches the OneLiner test in Step 1: the
expected string there lists parts in struct traversal order
(table, mv, dict, view, raw, named_collection). The Step 1 test expectation
`"+1 table, ~1 table, -1 mv, ~1 raw, ~1 named_collection"` follows from it.

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/loader/hcl/ -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/loader/hcl/compare.go internal/loader/hcl/compare_test.go \
  internal/loader/hcl/render_json.go
git commit -m "compare: build per-object comparisons from a ChangeSet

BuildObjectComparisons attaches each generated op to its object (keeping
the global dependency order index) and derives unsafe flags from the same
gen.Unsafe list the flat view uses. SummarizeComparisons/OneLiner replace
ad-hoc counting; being derived from the object list they count raws and
named collections, closing the drift summarizeChanges gap."
```

---

### Task 4: `diff -format json` emits objects + summary

**Files:**
- Modify: `internal/loader/hcl/render_json.go` (`DiffJSON`, `RenderDiffJSON`)
- Modify: `cmd/hclexp/hclexp.go` (`runDiff`, ~line 813)
- Test: `internal/loader/hcl/render_json_test.go`

**Interfaces:**
- Produces: `RenderDiffJSON(cs ChangeSet, gen GeneratedSQL, left, right *Schema) ([]byte, error)`
  (signature change: `cs` added as first parameter); `DiffJSON` gains
  `Objects []ObjectComparison` and `Summary CompareSummary`.
- Consumes: Task 3's `BuildObjectComparisons`, `SummarizeComparisons`,
  `buildJSONOperations`.

- [ ] **Step 1: Extend the existing JSON test** (in `render_json_test.go`)

In `TestRenderDiffJSON`, update the call site to
`RenderDiffJSON(cs, gen, left, right)` and append assertions after the
existing unsafe checks:

```go
	// The same document carries per-object comparisons and derived counts.
	objByName := map[string]ObjectComparison{}
	for _, o := range doc.Objects {
		objByName[o.Object] = o
	}
	assert.Equal(t, StatusAdded, objByName["query_log_archive"].Status)
	assert.Equal(t, StatusAltered, objByName["events"].Status)
	assert.True(t, objByName["sessions"].Unsafe)
	assert.Equal(t, CompareSummary{TablesAdded: 1, TablesAltered: 2}, doc.Summary)
```

In `TestRenderDiffJSON_ManualMaterializeIndex`, keep `cs` from the `Diff`
call (`cs := Diff(left, right)`) and pass it through.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/loader/hcl/ -run TestRenderDiffJSON -v`
Expected: FAIL (compile: wrong argument count; `doc.Objects` undefined)

- [ ] **Step 3: Implement**

In `render_json.go`:

```go
// DiffJSON is the top-level document emitted by `hclexp diff -format json`.
// Objects is the per-object comparison view; Operations is the flat,
// dependency-ordered execution view of the same diff. Summary counts derive
// from Objects.
type DiffJSON struct {
	Objects    []ObjectComparison `json:"objects"`
	Operations []JSONOperation    `json:"operations"`
	Unsafe     []JSONUnsafe       `json:"unsafe,omitempty"`
	Summary    CompareSummary     `json:"summary"`
}

func RenderDiffJSON(cs ChangeSet, gen GeneratedSQL, left, right *Schema) ([]byte, error) {
	objects := BuildObjectComparisons(cs, gen, left, right)
	doc := DiffJSON{
		Objects:    objects,
		Operations: buildJSONOperations(gen, left, right),
		Summary:    SummarizeComparisons(objects),
	}
	for _, u := range gen.Unsafe {
		doc.Unsafe = append(doc.Unsafe, JSONUnsafe{Database: u.Database, Object: u.Table, Reason: u.Reason})
	}
	return json.MarshalIndent(doc, "", "  ")
}
```

In `cmd/hclexp/hclexp.go` `runDiff`, the JSON branch becomes:

```go
	if *formatFlag == "json" {
		gen := hclload.GenerateSQL(cs)
		out, err := hclload.RenderDiffJSON(cs, gen, left, right)
		...
	}
```

(only the `RenderDiffJSON` call changes; error handling stays as-is).

- [ ] **Step 4: Run all tests; refresh snapshots if diff-JSON is snapshotted**

Run: `go test ./internal/... -v && go test ./test -v`
If `./test` fails only on diff `-format json` snapshot fixtures gaining the
new `objects`/`summary` keys: run `go test ./test -update-snapshots`, then
`git diff` the fixtures and verify the ONLY changes are added `objects` and
`summary` content (operations/unsafe unchanged). Re-run `go test ./test -v`.
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/loader/hcl/render_json.go internal/loader/hcl/render_json_test.go \
  cmd/hclexp/hclexp.go
# plus any snapshot fixture files updated under test/ — add them by exact path
git commit -m "diff: emit per-object comparisons in -format json

DiffJSON gains objects (attribute-level per-object view) and summary
(counts derived from it) alongside the unchanged operations/unsafe lists,
so existing consumers keep working while gates and agents stop doing
string surgery on DDL."
```

---

### Task 5: Shared text renderer replaces renderChangeSet

**Files:**
- Create: `internal/loader/hcl/render_text.go`
- Test: `internal/loader/hcl/render_text_test.go`
- Modify: `cmd/hclexp/hclexp.go` (`runDiff` text branch; delete
  `renderChangeSet`, `renderTableDiff`, `colDesc` at ~lines 1008–1186)
- Modify: `cmd/hclexp/drift.go` (`-details` call, line 115)
- Modify: `cmd/hclexp/hclexp_test.go` (7 `renderChangeSet` call sites)

**Interfaces:**
- Produces: `RenderObjectComparisons(w io.Writer, objs []ObjectComparison)`.
- Consumes: Task 3's model. Later tasks call this from drift.

- [ ] **Step 1: Write the failing test**

`internal/loader/hcl/render_text_test.go`:

```go
package hcl

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderObjectComparisons(t *testing.T) {
	objs := []ObjectComparison{
		{Database: "posthog", Object: "archive", ObjectType: KindTable, Status: StatusAdded},
		{Database: "posthog", Object: "sessions", ObjectType: KindTable, Status: StatusAltered,
			Unsafe: true, UnsafeReason: "ORDER BY changed",
			Changes: []FieldChange{
				{Field: "column:ts", Change: "add", New: "DateTime"},
				{Field: "column:old", Change: "drop"},
				{Field: "column:id", Change: "modify", Old: "UInt32", New: "UInt64"},
				{Field: "column:v", Change: "rename", Old: "v_old", New: "v"},
				{Field: "order_by", Change: "modify", Old: "id", New: "id, ts"},
			}},
		{Database: "posthog", Object: "mv_events", ObjectType: KindMaterializedView, Status: StatusAltered,
			Changes: []FieldChange{{Field: "query", Change: "modify", Old: "SELECT 1", New: "SELECT 2"}}},
		{Object: "s3", ObjectType: KindNamedCollection, Status: StatusAltered,
			Changes: []FieldChange{{Field: "param:url", Change: "modify", New: "https://b"}}},
		{Object: "bad", ObjectType: KindNamedCollection, Status: StatusAltered,
			Error: "cannot change an external collection"},
	}

	var buf bytes.Buffer
	RenderObjectComparisons(&buf, objs)
	assert.Equal(t, `database "posthog"
  + table archive
  ~ table sessions (UNSAFE: ORDER BY changed)
      + column ts = DateTime
      - column old
      ~ column id: UInt32 -> UInt64
      ~ column v (renamed from v_old)
      ~ order_by: id -> id, ts
  ~ materialized_view mv_events
      ~ query changed
named_collections
  ~ named_collection s3
      ~ param url = https://b
  ! named_collection bad: cannot change an external collection
`, buf.String())
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/loader/hcl/ -run TestRenderObjectComparisons -v`
Expected: FAIL (undefined `RenderObjectComparisons`)

- [ ] **Step 3: Implement**

`internal/loader/hcl/render_text.go`:

```go
package hcl

import (
	"fmt"
	"io"
	"strings"
)

// RenderObjectComparisons prints comparisons as the indented, +/-/~ marked
// summary used by `diff` (text mode) and `drift -details`. Objects render in
// input order under a header per database; named collections (empty
// database) render under a "named_collections" header. It consumes the same
// []ObjectComparison the JSON emits, so text and JSON cannot disagree.
func RenderObjectComparisons(w io.Writer, objs []ObjectComparison) {
	statusMark := map[string]string{StatusAdded: "+", StatusDropped: "-", StatusAltered: "~"}
	header := ""
	printed := false
	for _, o := range objs {
		h := fmt.Sprintf("database %q", o.Database)
		if o.ObjectType == KindNamedCollection {
			h = "named_collections"
		}
		if !printed || h != header {
			fmt.Fprintln(w, h)
			header, printed = h, true
		}
		if o.Error != "" {
			fmt.Fprintf(w, "  ! %s %s: %s\n", o.ObjectType, o.Object, o.Error)
			continue
		}
		suffix := ""
		if o.Unsafe {
			suffix = " (UNSAFE: " + o.UnsafeReason + ")"
		}
		fmt.Fprintf(w, "  %s %s %s%s\n", statusMark[o.Status], o.ObjectType, o.Object, suffix)
		for _, fc := range o.Changes {
			renderFieldChange(w, fc)
		}
	}
}

func renderFieldChange(w io.Writer, fc FieldChange) {
	field := strings.Replace(fc.Field, ":", " ", 1)
	switch fc.Change {
	case "add":
		if fc.New != "" {
			fmt.Fprintf(w, "      + %s = %s\n", field, fc.New)
		} else {
			fmt.Fprintf(w, "      + %s\n", field)
		}
	case "drop":
		fmt.Fprintf(w, "      - %s\n", field)
	case "rename":
		fmt.Fprintf(w, "      ~ %s (renamed from %s)\n", field, fc.Old)
	default: // modify
		switch {
		case fc.Field == "query":
			// canonical queries are long; presence is the signal here
			fmt.Fprintf(w, "      ~ query changed\n")
		case fc.Old == "" && fc.New == "":
			fmt.Fprintf(w, "      ~ %s changed\n", field)
		case fc.Old == "":
			fmt.Fprintf(w, "      ~ %s = %s\n", field, fc.New)
		case fc.New == "":
			fmt.Fprintf(w, "      ~ %s: %s -> (unset)\n", field, fc.Old)
		default:
			fmt.Fprintf(w, "      ~ %s: %s -> %s\n", field, fc.Old, fc.New)
		}
	}
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/loader/hcl/ -run TestRenderObjectComparisons -v`
Expected: PASS

- [ ] **Step 5: Switch both callers, delete the old walkers**

In `cmd/hclexp/hclexp.go` `runDiff`, hoist `gen := hclload.GenerateSQL(cs)`
to right after `cs := hclload.Diff(left, right)` (the json/sql branches then
reuse it), and replace the text tail:

```go
	if cs.IsEmpty() {
		fmt.Println("no differences")
		return
	}
	hclload.RenderObjectComparisons(os.Stdout,
		hclload.BuildObjectComparisons(cs, gen, left, right))
```

In `cmd/hclexp/drift.go` line 113–116, replace the `-details` branch:

```go
			if *details {
				cs := changeSets[d.Name]
				gen := hclload.GenerateSQL(cs)
				hclload.RenderObjectComparisons(prefixWriter(os.Stdout, "      "),
					hclload.BuildObjectComparisons(cs, gen, ref.Schema, d.Schema))
			}
```

Delete `renderChangeSet`, `renderTableDiff`, and `colDesc` from
`cmd/hclexp/hclexp.go`. Then `go build ./...`; if `engineKindName`,
`strOrNone`, or `sortedMapKeys` are now unused (compiler/staticcheck will
say), delete them too.

- [ ] **Step 6: Rewrite the cmd render tests**

In `cmd/hclexp/hclexp_test.go`, each of the 7 `renderChangeSet(&buf, cs)`
call sites becomes:

```go
	gen := hclload.GenerateSQL(cs)
	hclload.RenderObjectComparisons(&buf, hclload.BuildObjectComparisons(cs, gen, left, right))
```

(where `left`/`right` are the two schemas the test already diffs; if a test
built its `ChangeSet` by hand without schemas, pass `nil, nil` — engine
enrichment simply stays empty). Update expected strings per the new format:

| old line | new line |
|---|---|
| `  + materialized_view mv -> t` | `  + materialized_view mv` |
| `  ~ raw table r (recreate)` | `  ~ raw r` |
| `  ~ raw table r (recreate) ! UNSAFE: destroys data` | `  ~ raw r (UNSAFE: recreating a raw table drops its data)` |
| `      ~ column x: A -> B (UNSAFE)` | column line loses the marker; the object line gains `(UNSAFE: <reason from gen.Unsafe>)` |
| `      ~ param url (set)` | `      ~ param url = <value>` |
| `      ? param secret (skipped: ...)` | `      ~ param secret: [HIDDEN] -> [HIDDEN]` |
| `      ! requires recreation (to_table or columns changed)` | `      ~ to_table: a -> b` and/or `      ~ columns changed` |
| `      ! requires recreation (column_aliases / ...)` | one `      ~ <attr> changed` line per changed attribute |

Everything else (database headers, `+/-/~ <type> <name>`, column/setting
lines) is format-identical by construction.

- [ ] **Step 7: Run everything**

Run: `go test ./internal/... -v && go test ./test -v && go build -o hclexp ./cmd/hclexp`
Expected: PASS. Spot-check by eye:
`./hclexp diff -left test/testdata/<any fixture pair>` (pick an existing
fixture from `cmd/hclexp` tests) — output shape matches the old summary.

- [ ] **Step 8: Commit**

```bash
git add internal/loader/hcl/render_text.go internal/loader/hcl/render_text_test.go \
  cmd/hclexp/hclexp.go cmd/hclexp/hclexp_test.go cmd/hclexp/drift.go
git commit -m "render: one text renderer over ObjectComparison

diff text mode and drift -details now render the same []ObjectComparison
the JSON emits; renderChangeSet/renderTableDiff/colDesc are deleted. Text
gains the pieces the old walker skipped (projections, constraints,
primary_key, comment) for free."
```

---

### Task 6: Exclude config: object_types + FilterSchema

**Files:**
- Modify: `internal/loader/hcl/exclude.go`
- Test: `internal/loader/hcl/exclude_test.go`
- Modify: `examples/exclude.hcl` (document `object_types`)

**Interfaces:**
- Produces: `FilterSchema(s *Schema, m *ExcludeMatcher)`,
  `(m *ExcludeMatcher) MatchesObject(objectType, database, name string) bool`,
  `NewExcludeMatcherWithTypes(objectTypes []string, patterns ...string) *ExcludeMatcher`;
  exclude config gains `object_types = [...]` (optional).
- Consumes: `Kind*` consts; `Schema`/`DatabaseSpec` collections
  (`Tables`, `MaterializedViews`, `Views`, `Dictionaries`, `Raws`,
  `Schema.NamedCollections`).

- [ ] **Step 1: Write the failing tests** (append to `exclude_test.go`)

```go
func TestFilterSchema(t *testing.T) {
	s := &Schema{
		Databases: []DatabaseSpec{{
			Name: "posthog",
			Tables: []TableSpec{
				{Name: "events"},
				{Name: "tmp_migration"},
			},
			Views: []ViewSpec{{Name: "events_view", Query: "SELECT 1"}},
			Raws:  []RawSpec{{Kind: "table", Name: "legacy_backup", SQL: "CREATE ..."}},
		}},
		NamedCollections: []NamedCollectionSpec{{Name: "s3_creds"}},
	}

	m := NewExcludeMatcherWithTypes(
		[]string{KindNamedCollection},
		"tmp_*", "posthog.*_backup",
	)
	FilterSchema(s, m)

	want := &Schema{
		Databases: []DatabaseSpec{{
			Name:   "posthog",
			Tables: []TableSpec{{Name: "events"}},
			Views:  []ViewSpec{{Name: "events_view", Query: "SELECT 1"}},
			Raws:   []RawSpec{},
		}},
		NamedCollections: []NamedCollectionSpec{},
	}
	assert.Equal(t, want, s)
}

func TestLoadExcludeConfig_ObjectTypes(t *testing.T) {
	path := writeTempExclude(t, `
exclude {
  patterns     = ["tmp_*"]
  object_types = ["named_collection"]
}`)
	m, err := LoadExcludeConfig(path)
	require.NoError(t, err)
	assert.True(t, m.MatchesObject(KindNamedCollection, "", "anything"))
	assert.True(t, m.MatchesObject(KindTable, "db", "tmp_x"))
	assert.False(t, m.MatchesObject(KindTable, "db", "events"))
	assert.False(t, m.Empty())

	_, err = LoadExcludeConfig(writeTempExclude(t, `
exclude {
  patterns     = []
  object_types = ["nonsense"]
}`))
	require.ErrorContains(t, err, "nonsense")
}
```

Reuse the existing temp-file helper in `exclude_test.go` for
`writeTempExclude` (the file already writes temp exclude configs for
`TestLoadExcludeConfig`; extract or mirror that helper — same package, same
pattern). If `assert.Equal` fails only on `nil` vs empty-slice for untouched
collections, adjust the `want` literal to match `FilterSchema`'s in-place
behavior (filtered slices become empty non-nil; untouched slices stay as
built) — the object membership assertions are what matter.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/loader/hcl/ -run 'TestFilterSchema|TestLoadExcludeConfig_ObjectTypes' -v`
Expected: FAIL (undefined `FilterSchema`, `MatchesObject`, ...)

- [ ] **Step 3: Implement** (in `exclude.go`)

```go
type ExcludeMatcher struct {
	patterns    []string
	objectTypes map[string]bool
}

type excludeFile struct {
	Exclude *struct {
		Patterns    []string `hcl:"patterns"`
		ObjectTypes []string `hcl:"object_types,optional"`
	} `hcl:"exclude,block"`
}

// validExcludeObjectTypes are the object_type values `object_types` accepts.
var validExcludeObjectTypes = map[string]bool{
	KindTable: true, KindMaterializedView: true, KindView: true,
	KindDictionary: true, KindRaw: true, KindNamedCollection: true,
}
```

In `LoadExcludeConfig`, after the patterns loop:

```go
	if cfg.Exclude != nil && len(cfg.Exclude.ObjectTypes) > 0 {
		m.objectTypes = make(map[string]bool, len(cfg.Exclude.ObjectTypes))
		for _, ot := range cfg.Exclude.ObjectTypes {
			if !validExcludeObjectTypes[ot] {
				return nil, fmt.Errorf("invalid exclude object_type %q", ot)
			}
			m.objectTypes[ot] = true
		}
	}
```

New constructor, matcher method, `Empty` update, and the filter:

```go
// NewExcludeMatcherWithTypes builds a matcher from explicit object types and
// patterns (used by tests and callers that don't load a config file).
func NewExcludeMatcherWithTypes(objectTypes []string, patterns ...string) *ExcludeMatcher {
	m := &ExcludeMatcher{patterns: patterns}
	if len(objectTypes) > 0 {
		m.objectTypes = make(map[string]bool, len(objectTypes))
		for _, ot := range objectTypes {
			m.objectTypes[ot] = true
		}
	}
	return m
}

// MatchesObject reports whether an object is excluded, by its type or by a
// name pattern.
func (m *ExcludeMatcher) MatchesObject(objectType, database, name string) bool {
	if m == nil {
		return false
	}
	if m.objectTypes[objectType] {
		return true
	}
	return m.Matches(database, name)
}

func (m *ExcludeMatcher) Empty() bool {
	return m == nil || (len(m.patterns) == 0 && len(m.objectTypes) == 0)
}

// FilterSchema removes every object the matcher excludes, in place. Both
// sides of a comparison are filtered before Diff so excluded objects appear
// in no output and no count. Databases are kept even when emptied.
func FilterSchema(s *Schema, m *ExcludeMatcher) {
	if s == nil || m.Empty() {
		return
	}
	for di := range s.Databases {
		db := &s.Databases[di]
		db.Tables = filterSlice(db.Tables, func(t TableSpec) bool {
			return m.MatchesObject(KindTable, db.Name, t.Name)
		})
		db.MaterializedViews = filterSlice(db.MaterializedViews, func(v MaterializedViewSpec) bool {
			return m.MatchesObject(KindMaterializedView, db.Name, v.Name)
		})
		db.Views = filterSlice(db.Views, func(v ViewSpec) bool {
			return m.MatchesObject(KindView, db.Name, v.Name)
		})
		db.Dictionaries = filterSlice(db.Dictionaries, func(d DictionarySpec) bool {
			return m.MatchesObject(KindDictionary, db.Name, d.Name)
		})
		db.Raws = filterSlice(db.Raws, func(r RawSpec) bool {
			return m.MatchesObject(KindRaw, db.Name, r.Name)
		})
	}
	s.NamedCollections = filterSlice(s.NamedCollections, func(nc NamedCollectionSpec) bool {
		return m.MatchesObject(KindNamedCollection, "", nc.Name)
	})
}

func filterSlice[T any](in []T, drop func(T) bool) []T {
	out := in[:0]
	for _, v := range in {
		if !drop(v) {
			out = append(out, v)
		}
	}
	return out
}
```

Also update the doc comments on `ExcludeMatcher` and `LoadExcludeConfig` to
mention `object_types`, and append to `examples/exclude.hcl`:

```hcl
# object_types drops a whole object class regardless of name — e.g. gates
# that manage named collections out of band:
#   object_types = ["named_collection"]
```

- [ ] **Step 4: Run the package tests**

Run: `go test ./internal/loader/hcl/ -v`
Expected: PASS (existing exclude + introspect tests unaffected: `Match` and
`Matches` behavior is unchanged)

- [ ] **Step 5: Commit**

```bash
git add internal/loader/hcl/exclude.go internal/loader/hcl/exclude_test.go examples/exclude.hcl
git commit -m "exclude: object_types and schema-level filtering

The exclude config gains an optional object_types list (drop a whole
class, e.g. named_collection) and FilterSchema applies a matcher to a
resolved schema in place — both sides of a comparison get filtered before
Diff, so excluded objects appear in no view and no count."
```

---

### Task 7: `-exclude` flag on diff, plan, drift

**Files:**
- Modify: `cmd/hclexp/hclexp.go` (`runDiff`)
- Modify: `cmd/hclexp/plan.go` (`runPlan`)
- Modify: `cmd/hclexp/drift.go` (`runDrift`)
- Test: `cmd/hclexp/hclexp_test.go`

**Interfaces:**
- Consumes: Task 6's `LoadExcludeConfig`, `FilterSchema`.
- Produces: `-exclude <file>` flag on all three commands; helper
  `loadExcludeFlag(path string) *hclload.ExcludeMatcher` in cmd.

- [ ] **Step 1: Write the failing test** (append to `cmd/hclexp/hclexp_test.go`)

```go
// A diff whose only difference is an excluded object must be empty after
// FilterSchema — the CLI-level contract behind `-exclude`.
func TestDiffWithExcludeFilteredSchemas(t *testing.T) {
	dir := t.TempDir()
	leftPath := filepath.Join(dir, "left.hcl")
	rightPath := filepath.Join(dir, "right.hcl")
	require.NoError(t, os.WriteFile(leftPath, []byte(`
database "d" {
  table "events" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}
`), 0o644))
	require.NoError(t, os.WriteFile(rightPath, []byte(`
database "d" {
  table "events" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
  table "tmp_scratch" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}
`), 0o644))

	left, err := loadSide(leftPath)
	require.NoError(t, err)
	right, err := loadSide(rightPath)
	require.NoError(t, err)

	m := hclload.NewExcludeMatcherWithTypes(nil, "tmp_*")
	hclload.FilterSchema(left, m)
	hclload.FilterSchema(right, m)
	assert.True(t, hclload.Diff(left, right).IsEmpty())
}
```

(Fixture syntax verified against `internal/loader/hcl/testdata/engines_all_kinds.hcl`:
engine kind labels are snake_case, `order_by` is a string list.)

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/hclexp/ -run TestDiffWithExcludeFilteredSchemas -v`
Expected: FAIL only if fixture syntax is wrong; fix fixture until the test
meaningfully exercises Filter+Diff and PASSES. (The helpers exist after
Task 6 — this test pins the CLI contract before wiring the flag.)

- [ ] **Step 3: Wire the flag into all three commands**

Add to `cmd/hclexp/hclexp.go` (near `loadSide`):

```go
// loadExcludeFlag loads an -exclude config, exiting on error. An empty path
// yields nil (no filtering).
func loadExcludeFlag(path string) *hclload.ExcludeMatcher {
	if path == "" {
		return nil
	}
	m, err := hclload.LoadExcludeConfig(path)
	if err != nil {
		slog.Error("failed to load exclude config", "file", path, "err", err)
		os.Exit(1)
	}
	return m
}
```

`runDiff`: add
`excludeFlag := fs.String("exclude", "", "HCL exclude config: objects matching its patterns/object_types are dropped from both sides before diffing")`
and after both `loadSide` calls:

```go
	if m := loadExcludeFlag(*excludeFlag); m != nil {
		hclload.FilterSchema(left, m)
		hclload.FilterSchema(right, m)
	}
```

`runPlan` (`cmd/hclexp/plan.go`): same flag; resolve
`matcher := loadExcludeFlag(*excludeFlag)` once before the role loop, then
inside the loop after `desired`/`cur` are set:

```go
		hclload.FilterSchema(desired, matcher)
		hclload.FilterSchema(cur, matcher)
```

(`FilterSchema` no-ops on a nil matcher via `m.Empty()`.)

`runDrift` (`cmd/hclexp/drift.go`): same flag (use
`fmt.Fprintln(os.Stderr, ...)` + `os.Exit(1)` error style to match the file);
after `loadDriftNodes` and the `normalizeZKPaths` loop:

```go
	if m := loadExcludeFlag(*excludeFlag); m != nil {
		for i := range nodes {
			hclload.FilterSchema(nodes[i].Schema, m)
		}
	}
```

Note: `loadExcludeFlag` uses slog; drift prints to stderr already via slog in
other paths? It does not — drift uses `fmt.Fprintf(os.Stderr, ...)`. Keep
`loadExcludeFlag` slog-based (it lives in hclexp.go beside runDiff); the
behavioral contract (message + non-zero exit) is what matters.

- [ ] **Step 4: Run tests + build**

Run: `go test ./cmd/hclexp/ -v && go build -o hclexp ./cmd/hclexp`
Expected: PASS. Manual check:
`./hclexp diff -left <fixture> -right <fixture> -exclude examples/exclude.hcl`
runs without error.

- [ ] **Step 5: Commit**

```bash
git add cmd/hclexp/hclexp.go cmd/hclexp/plan.go cmd/hclexp/drift.go cmd/hclexp/hclexp_test.go
git commit -m "cli: -exclude on diff, plan, and drift

All three comparison commands accept the introspect/dump-cluster exclude
config and filter both sides before diffing, replacing downstream JSON
post-filters (e.g. check-live.sh's python filter_drift)."
```

---

### Task 8: `drift -format json` (rework on the shared model)

**Files:**
- Create: `internal/loader/hcl/drift_doc.go`
- Modify: `cmd/hclexp/drift.go` (restructure `runDrift`; delete
  `summarizeChanges`)
- Test: `cmd/hclexp/drift_test.go`

**Interfaces:**
- Produces: `DriftJSON`, `DriftGroup`, `DriftNode`, `DriftRunSummary`
  (internal package); cmd `buildDriftDoc(nodes []driftNode, keys []string) hclload.DriftJSON`;
  `drift -format text|json` flag.
- Consumes: Tasks 3+5 (`BuildObjectComparisons`, `SummarizeComparisons`,
  `OneLiner`, `RenderObjectComparisons`).

Semantics carried over from the superseded plan
(`docs/plans/2026-07-10-drift-json-output.md`): direction is descriptive
(`Diff(ref, drifter)` — added = the drifter has it); exit non-zero iff any
node drifts; JSON to stdout with the per-group prose suppressed; invalid
`-format` exits 2.

- [ ] **Step 1: Write the failing tests** (append to `cmd/hclexp/drift_test.go`)

```go
// writeDriftNode writes one per-node dump file into dir.
func writeDriftNode(t *testing.T, dir, name, hcl string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+".hcl"), []byte(hcl), 0o644))
}

const driftBaseHCL = `
database "d" {
  table "events" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}
`

// An MV present only on the drifter surfaces as status "added" with a CREATE
// operation — pinning the descriptive reference -> drifter direction.
func TestBuildDriftDoc_Direction(t *testing.T) {
	dir := t.TempDir()
	writeDriftNode(t, dir, "node-a", driftBaseHCL)
	writeDriftNode(t, dir, "node-b", driftBaseHCL+`
database "d" {
  materialized_view "mv_events" {
    to_table = "events"
    query    = "SELECT id FROM d.events"
    column "id" { type = "UInt64" }
  }
}
`)
	nodes, err := loadDriftNodes(dir, "*")
	require.NoError(t, err)
	doc := buildDriftDoc(nodes, []string{"role"})

	require.Len(t, doc.Groups, 1)
	g := doc.Groups[0]
	assert.Equal(t, "node-a", g.Reference)
	assert.Equal(t, 2, g.Nodes)
	require.Len(t, g.Drifters, 1)
	d := g.Drifters[0]
	assert.Equal(t, "node-b", d.Node)
	require.Len(t, d.Objects, 1)
	assert.Equal(t, hclload.StatusAdded, d.Objects[0].Status)
	assert.Equal(t, hclload.KindMaterializedView, d.Objects[0].ObjectType)
	require.NotEmpty(t, d.Objects[0].Operations)
	assert.Equal(t, hclload.OpCreate, d.Objects[0].Operations[0].Kind)
	assert.Equal(t, hclload.CompareSummary{MVsAdded: 1}, d.Summary)
	assert.Equal(t, "+1 mv", d.Summary.OneLiner())

	assert.Equal(t, hclload.DriftRunSummary{
		Nodes: 2, Groups: 1, GroupsWithDrift: 1, DriftingNodes: 1,
	}, doc.Summary)
}

// A node drifting ONLY in a raw block yields non-zero raw counts and a
// non-"changed" one-liner — regression for the summarizeChanges gap.
func TestBuildDriftDoc_RawOnlyDrift(t *testing.T) {
	dir := t.TempDir()
	writeDriftNode(t, dir, "node-a", driftBaseHCL+`
database "d" {
  raw "table" "legacy" {
    sql = "CREATE TABLE d.legacy (id UInt64) ENGINE = MergeTree ORDER BY id"
  }
}
`)
	writeDriftNode(t, dir, "node-b", driftBaseHCL+`
database "d" {
  raw "table" "legacy" {
    sql = "CREATE TABLE d.legacy (id UInt32) ENGINE = MergeTree ORDER BY id"
  }
}
`)
	nodes, err := loadDriftNodes(dir, "*")
	require.NoError(t, err)
	doc := buildDriftDoc(nodes, []string{"role"})

	require.Len(t, doc.Groups, 1)
	require.Len(t, doc.Groups[0].Drifters, 1)
	d := doc.Groups[0].Drifters[0]
	assert.Equal(t, hclload.CompareSummary{RawsAltered: 1}, d.Summary)
	assert.Equal(t, "~1 raw", d.Summary.OneLiner())
}

// No drift: groups populated, drifters empty, zero summary counts.
func TestBuildDriftDoc_Clean(t *testing.T) {
	dir := t.TempDir()
	writeDriftNode(t, dir, "node-a", driftBaseHCL)
	writeDriftNode(t, dir, "node-b", driftBaseHCL)
	nodes, err := loadDriftNodes(dir, "*")
	require.NoError(t, err)
	doc := buildDriftDoc(nodes, []string{"role"})
	require.Len(t, doc.Groups, 1)
	assert.Empty(t, doc.Groups[0].Drifters)
	assert.Equal(t, hclload.DriftRunSummary{Nodes: 2, Groups: 1}, doc.Summary)
}
```

```go
// A node drifting ONLY in a named collection: same regression, other kind.
func TestBuildDriftDoc_NamedCollectionOnlyDrift(t *testing.T) {
	dir := t.TempDir()
	writeDriftNode(t, dir, "node-a", driftBaseHCL+`
named_collection "s3_creds" {
  param "url" { value = "https://a" }
}
`)
	writeDriftNode(t, dir, "node-b", driftBaseHCL+`
named_collection "s3_creds" {
  param "url" { value = "https://b" }
}
`)
	nodes, err := loadDriftNodes(dir, "*")
	require.NoError(t, err)
	doc := buildDriftDoc(nodes, []string{"role"})

	require.Len(t, doc.Groups, 1)
	require.Len(t, doc.Groups[0].Drifters, 1)
	d := doc.Groups[0].Drifters[0]
	assert.Equal(t, hclload.CompareSummary{NamedCollectionsChanged: 1}, d.Summary)
	assert.Equal(t, "~1 named_collection", d.Summary.OneLiner())
	require.Len(t, d.Objects, 1)
	assert.Equal(t, hclload.StatusAltered, d.Objects[0].Status)
	assert.Equal(t, []hclload.FieldChange{
		{Field: "param:url", Change: "modify", New: "https://b"},
	}, d.Objects[0].Changes)
}
```

Check existing imports of `drift_test.go` and add `os`, `path/filepath`,
`hclload`, `require` as needed. If the existing tests reference
`summarizeChanges`, delete those tests in Step 3 (the OneLiner tests in
Task 3 replaced them).

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/hclexp/ -run TestBuildDriftDoc -v`
Expected: FAIL (undefined `buildDriftDoc`, `hclload.DriftJSON`)

- [ ] **Step 3: Implement**

`internal/loader/hcl/drift_doc.go`:

```go
package hcl

// Drift document types for `hclexp drift -format json`. Direction is
// descriptive: each drifter's objects/operations describe the DDL that would
// transform the group reference into that node — NOT a fix script. The
// reference is merely the lexically-first group member; authoritative
// desired state comes from `plan`/`diff` against HCL.

// DriftNode is one drifting node's comparison against its group reference.
type DriftNode struct {
	Node    string             `json:"node"`
	File    string             `json:"file"`
	Macros  map[string]string  `json:"macros,omitempty"`
	Objects []ObjectComparison `json:"objects"`
	Summary CompareSummary     `json:"summary"`
}

// DriftGroup is one set of nodes expected to share a schema.
type DriftGroup struct {
	Key       string      `json:"key"`
	Reference string      `json:"reference"`
	Nodes     int         `json:"nodes"`
	Drifters  []DriftNode `json:"drifters"`
}

// DriftRunSummary aggregates a whole drift run.
type DriftRunSummary struct {
	Nodes           int `json:"nodes"`
	Groups          int `json:"groups"`
	GroupsWithDrift int `json:"groups_with_drift"`
	DriftingNodes   int `json:"drifting_nodes"`
}

// DriftJSON is the document emitted by `hclexp drift -format json`.
type DriftJSON struct {
	Groups  []DriftGroup    `json:"groups"`
	Summary DriftRunSummary `json:"summary"`
}
```

In `cmd/hclexp/drift.go`, add the flag, restructure `runDrift` around a doc
builder, and delete `summarizeChanges`:

```go
	formatFlag := fs.String("format", "text", "output format: text (default) or json")
```

```go
	switch *formatFlag {
	case "text", "json":
	default:
		fmt.Fprintf(os.Stderr, "drift: invalid -format %q (want text|json)\n", *formatFlag)
		os.Exit(2)
	}
```

(place it beside the `-zk-paths` validation). Replace everything in
`runDrift` from `keys := splitList(*groupByFlag)` to the end with:

```go
	keys := splitList(*groupByFlag)
	doc := buildDriftDoc(nodes, keys)

	if *formatFlag == "json" {
		out, err := json.MarshalIndent(doc, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "drift: render JSON: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	} else {
		renderDriftText(os.Stdout, doc, *details)
	}
	if doc.Summary.DriftingNodes > 0 {
		os.Exit(1)
	}
```

New functions in `drift.go` (add `"encoding/json"` to imports):

```go
// buildDriftDoc groups nodes and diffs each group against its
// lexically-first reference, returning the full drift document. Groups with
// a single node get no drifters (nothing to compare against).
func buildDriftDoc(nodes []driftNode, keys []string) hclload.DriftJSON {
	groups, order := groupNodes(nodes, keys)
	doc := hclload.DriftJSON{Summary: hclload.DriftRunSummary{Nodes: len(nodes), Groups: len(order)}}
	for _, key := range order {
		members := groups[key]
		sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
		ref := members[0]
		g := hclload.DriftGroup{Key: key, Reference: ref.Name, Nodes: len(members)}
		for _, m := range members[1:] {
			cs := hclload.Diff(ref.Schema, m.Schema)
			if cs.IsEmpty() {
				continue
			}
			gen := hclload.GenerateSQL(cs)
			objs := hclload.BuildObjectComparisons(cs, gen, ref.Schema, m.Schema)
			g.Drifters = append(g.Drifters, hclload.DriftNode{
				Node: m.Name, File: m.File, Macros: m.Macros,
				Objects: objs, Summary: hclload.SummarizeComparisons(objs),
			})
		}
		if len(g.Drifters) > 0 {
			doc.Summary.GroupsWithDrift++
			doc.Summary.DriftingNodes += len(g.Drifters)
		}
		doc.Groups = append(doc.Groups, g)
	}
	return doc
}

// renderDriftText prints the classic prose report from the drift document.
func renderDriftText(w *os.File, doc hclload.DriftJSON, details bool) {
	for _, g := range doc.Groups {
		if g.Nodes < 2 {
			fmt.Fprintf(w, "group %q — 1 node (no peers to compare): %s\n", g.Key, g.Reference)
			continue
		}
		fmt.Fprintf(w, "group %q — %d nodes, reference %s", g.Key, g.Nodes, g.Reference)
		if len(g.Drifters) == 0 {
			fmt.Fprintf(w, " — OK (all identical)\n")
			continue
		}
		fmt.Fprintf(w, " — %d drifting\n", len(g.Drifters))
		for _, d := range g.Drifters {
			fmt.Fprintf(w, "  ✗ %s: %s\n", d.Node, d.Summary.OneLiner())
			if details {
				hclload.RenderObjectComparisons(prefixWriter(w, "      "), d.Objects)
			}
		}
	}
	fmt.Fprintf(w, "\nsummary: %d nodes, %d groups, %d groups with drift, %d drifting nodes\n",
		doc.Summary.Nodes, doc.Summary.Groups, doc.Summary.GroupsWithDrift, doc.Summary.DriftingNodes)
}
```

Delete `summarizeChanges` (drift.go:295) and any tests that exercised it.
This also removes the Task 5-era `changeSets` map — `-details` now reads
`d.Objects` straight from the doc.

- [ ] **Step 4: Run tests + end-to-end**

Run: `go test ./cmd/hclexp/ -v && go test ./internal/... -v && go build -o hclexp ./cmd/hclexp`
Expected: PASS. Then, with a scratch dir from the raw-only test fixtures:

```bash
mkdir -p /tmp/drift-check && cp <the two node .hcl fixtures> /tmp/drift-check/
./hclexp drift -dir /tmp/drift-check -group-by role -format json | jq '.summary'
# -> {"nodes":2,"groups":1,"groups_with_drift":1,"drifting_nodes":1}; exit code 1
./hclexp drift -dir /tmp/drift-check -group-by role
# -> classic text with "~1 raw" (not "changed"); exit code 1
./hclexp drift -dir /tmp/drift-check -format yaml; echo $?
# -> "drift: invalid -format ..." on stderr; 2
```

- [ ] **Step 5: Commit**

```bash
git add internal/loader/hcl/drift_doc.go cmd/hclexp/drift.go cmd/hclexp/drift_test.go
git commit -m "drift: -format json on the shared object-comparison model

Each drifter carries the same per-object document diff emits (descriptive
direction: reference -> drifter, per the recorded decision). Text mode's
one-liner derives from CompareSummary, so raw-block and named-collection
drift now count instead of printing the bare 'changed' fallback;
summarizeChanges is deleted."
```

---

### Task 9: `plan` per-role object comparisons

**Files:**
- Modify: `internal/loader/hcl/plan.go` (`PlanResult`, `BuildPlan`)
- Test: `internal/loader/hcl/plan_test.go`

**Interfaces:**
- Produces: `PlanResult.Roles []RoleComparison`;
  `RoleComparison{Role string; Objects []ObjectComparison; Summary CompareSummary}`
  with JSON tags `role`, `objects`, `summary`.
- Consumes: Task 3's builders.

- [ ] **Step 1: Write the failing test** (append to `internal/loader/hcl/plan_test.go`,
  reusing its existing schema-construction helpers — check how the file
  builds `RoleDiff`s and mirror it)

```go
// Each role gets its own (non-deduped) object comparison list; a shared
// object drifting on two roles appears under both, and nested op orders
// reference the merged global list.
func TestBuildPlanRoleComparisons(t *testing.T) {
	idCol := ColumnSpec{Name: "id", Type: "UInt64"}
	shared := mkTable("shared_t", EngineMergeTree{}, idCol)
	shared.OrderBy = []string{"id"}

	empty := &Schema{}
	desired := &Schema{Databases: []DatabaseSpec{mkDB("d", shared)}}

	plan := BuildPlan([]RoleDiff{
		{Role: "ops", Desired: desired, Current: empty},
		{Role: "logs", Desired: desired, Current: empty},
	})

	require.Len(t, plan.Roles, 2)
	for i, role := range []string{"ops", "logs"} {
		rc := plan.Roles[i]
		assert.Equal(t, role, rc.Role)
		assert.Equal(t, CompareSummary{TablesAdded: 1}, rc.Summary)
		require.Len(t, rc.Objects, 1)
		assert.Equal(t, StatusAdded, rc.Objects[0].Status)
		require.Len(t, rc.Objects[0].Operations, 1)
	}

	// The identical CREATE dedupes to ONE global operation; both roles' nested
	// op carries that operation's global order.
	require.Len(t, plan.Operations, 1)
	assert.Equal(t, []string{"ops", "logs"}, plan.Operations[0].Roles)
	assert.Equal(t, plan.Operations[0].Order, plan.Roles[0].Objects[0].Operations[0].Order)
	assert.Equal(t, plan.Operations[0].Order, plan.Roles[1].Objects[0].Operations[0].Order)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/loader/hcl/ -run TestBuildPlanRoleComparisons -v`
Expected: FAIL (`plan.Roles` undefined)

- [ ] **Step 3: Implement**

In `internal/loader/hcl/plan.go`:

```go
// RoleComparison is one role's per-object view of its diff. Unlike the
// merged Operations list it is deliberately NOT deduped across roles —
// triage is per (env, role), so a shared object drifting on two roles
// appears under both.
type RoleComparison struct {
	Role    string             `json:"role"`
	Objects []ObjectComparison `json:"objects"`
	Summary CompareSummary     `json:"summary"`
}

type PlanResult struct {
	Operations []PlanOperation  `json:"operations"`
	Unsafe     []JSONUnsafe     `json:"unsafe,omitempty"`
	Roles      []RoleComparison `json:"roles"`
}
```

In `BuildPlan`, restructure the per-role loop (plan.go:63) to keep the
ChangeSet and collect comparisons:

```go
	var roleComparisons []RoleComparison
	for _, rd := range roles {
		cs := Diff(rd.Current, rd.Desired)
		gen := GenerateSQL(cs)
		objs := BuildObjectComparisons(cs, gen, rd.Current, rd.Desired)
		roleComparisons = append(roleComparisons, RoleComparison{
			Role: rd.Role, Objects: objs, Summary: SummarizeComparisons(objs),
		})
		for _, op := range gen.Ops {
			// ... existing merge body unchanged ...
		}
		for _, u := range gen.Unsafe {
			// ... existing unchanged ...
		}
	}
```

After the final `result.Unsafe` sort (plan.go:132–137) and before `return`,
rewrite nested op orders to the merged global order and attach the roles:

```go
	orderByKey := map[opKey]int{}
	for _, po := range result.Operations {
		orderByKey[opKey{po.Kind, po.Database, po.Object, po.SQL}] = po.Order
	}
	for ri := range roleComparisons {
		for oi := range roleComparisons[ri].Objects {
			ops := roleComparisons[ri].Objects[oi].Operations
			for pi := range ops {
				ops[pi].Order = orderByKey[opKey{ops[pi].Kind, ops[pi].Database, ops[pi].Object, ops[pi].SQL}]
			}
		}
	}
	result.Roles = roleComparisons
	return result
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/loader/hcl/ -v && go test ./cmd/hclexp/ -v && go test ./test -v`
Expected: PASS. If a plan JSON snapshot or cmd plan test asserts the exact
top-level document, extend the expectation with the new `roles` key (content
per this task) rather than regenerating blindly.

- [ ] **Step 5: Commit**

```bash
git add internal/loader/hcl/plan.go internal/loader/hcl/plan_test.go
# plus any updated plan test/snapshot files by exact path
git commit -m "plan: per-role object comparisons alongside the merged ops

PlanResult gains roles: each role's non-deduped ObjectComparison list with
derived counts, nested ops carrying the merged global order. Triage is per
(env, role); execution stays on the deduped global list."
```

---

### Task 10: Documentation

**Files:**
- Modify: `docs/README.hcl.md`
- Modify: `CLAUDE.md`
- Modify: `cmd/hclexp/hclexp.go` (usage text, ~line 97 `drift` line)

**Interfaces:** none (docs only).

- [ ] **Step 1: docs/README.hcl.md**

Add a "Structured comparison output" section (near the existing diff/plan
docs) covering, with a JSON example built from Task 3's test scenario:
- the `objects` document: `ObjectComparison` keys, the right-relative status
  rule stated exactly as: *"`added` means the right side of the comparison
  has the object and the left does not; `diff`/`plan` put the desired schema
  on the right (so `added` will be CREATEd), `drift` puts the drifter on the
  right (descriptive, not a fix script)"*
- the full `field` vocabulary table from the spec
  (`docs/plans/2026-07-10-object-comparison.md`, "field vocabulary" section,
  including the Task 2 addendum rows), plus `change` values and the
  old/new rendering rules (columns as compact descriptors, engine as its SQL
  clause, order_by/primary_key comma-joined, `param:` set-changes new-only,
  redacted params `[HIDDEN]` both sides)
- `summary` count keys
- `-exclude` on `diff`/`plan`/`drift` and the `object_types` config attribute
- `drift -format json` document shape (groups/reference/drifters/summary).

- [ ] **Step 2: CLAUDE.md**

Update the feature bullets:
- "Structured diff output": mention `objects` + `summary` alongside
  `operations`, and `-exclude`.
- "Cross-Node Drift": add `-format text|json` (per-drifter object
  comparisons + counts; raw/named-collection drift now counted) and
  `-exclude`.
- "Cross-role planning": add per-role `roles` comparisons and `-exclude`.
- "Exclude patterns" bullet: mention `object_types`.

- [ ] **Step 3: usage text**

In `cmd/hclexp/hclexp.go` `usage()` the drift line becomes:

```
  drift        detect cross-node schema drift across per-node HCL dumps (-format json for structured output)
```

- [ ] **Step 4: Verify docs build nothing is broken**

Run: `go build ./... && go test ./cmd/hclexp/ -run TestUsage -v` (if a usage
snapshot test exists; otherwise just build) and `git diff --stat` to eyeball
docs-only changes.

- [ ] **Step 5: Commit**

```bash
git add docs/README.hcl.md CLAUDE.md cmd/hclexp/hclexp.go
git commit -m "docs: structured per-object comparison output

README.hcl.md documents the objects/summary JSON contract (field
vocabulary, right-relative status rule, exclude object_types) for diff,
plan, and drift; CLAUDE.md bullets and the usage text follow."
```

---

## Final verification (after all tasks)

```bash
go build -o hclexp ./cmd/hclexp
go test ./internal/... -v
go test ./cmd/hclexp/ -v
go test ./test -v
./hclexp diff -left <fixture> -right <fixture> -format json | jq '.summary'
./hclexp drift -dir <fixture-dir> -format json | jq '.groups[].drifters[].summary'
```

Follow-ups recorded in the spec, NOT in this plan's scope: check-live.sh
simplification (posthog repo), `hclexp web` diff view.
