package hcl

import (
	"bytes"
	"testing"

	"github.com/posthog/chschema/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// columnOrder flattens a column list to names for order assertions.
func columnOrder(cols []ColumnSpec) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}

// The issue #158 scenario in miniature: dev declares is_deleted and version
// adjacent; prod's pmat_* columns interleave between them. The patch places
// them, and the composed table is byte-identical to the same table declared
// plainly in that order — the golden-parity acceptance.
func TestPatchTable_PositionedAdds(t *testing.T) {
	root := t.TempDir()
	base := writePatchLayer(t, root, "base/person.hcl", `
database "posthog" {
  table "person" {
    order_by = ["id"]
    column "id"         { type = "UInt64" }
    column "properties" { type = "String" }
    column "is_deleted" { type = "UInt8" }
    column "version"    { type = "UInt64" }
    engine "merge_tree" {}
  }
}`)
	patch := writePatchLayer(t, root, "prod/patch.hcl", `
database "posthog" {
  patch_table "person" {
    column "pmat_email" {
      type  = "String"
      after = "is_deleted"
    }
    column "pmat_name" {
      type  = "String"
      after = "pmat_email"
    }
  }
}`)
	flat := writePatchLayer(t, root, "flat/person.hcl", `
database "posthog" {
  table "person" {
    order_by = ["id"]
    column "id"         { type = "UInt64" }
    column "properties" { type = "String" }
    column "is_deleted" { type = "UInt8" }
    column "pmat_email" { type = "String" }
    column "pmat_name"  { type = "String" }
    column "version"    { type = "UInt64" }
    engine "merge_tree" {}
  }
}`)

	composed, err := LoadLayers([]string{base, patch})
	require.NoError(t, err)
	require.NoError(t, Resolve(composed))
	assert.Equal(t, []string{"id", "properties", "is_deleted", "pmat_email", "pmat_name", "version"},
		columnOrder(composed.Databases[0].Tables[0].Columns),
		"chained after: each add resolves against the post-previous-add state")

	declared, err := LoadLayers([]string{flat})
	require.NoError(t, err)
	require.NoError(t, Resolve(declared))

	var composedOut, declaredOut bytes.Buffer
	require.NoError(t, Write(&composedOut, composed))
	require.NoError(t, Write(&declaredOut, declared))
	assert.Equal(t, declaredOut.String(), composedOut.String(),
		"base+patch renders byte-identical to the flat declaration (golden parity)")
}

// first = true inserts at the front; a plain add still appends.
func TestPatchTable_FirstAndAppend(t *testing.T) {
	db := DatabaseSpec{
		Name: "posthog",
		Tables: []TableSpec{{
			Name:    "t",
			Columns: []ColumnSpec{{Name: "a", Type: "UInt8"}, {Name: "b", Type: "UInt8"}},
		}},
		Patches: []PatchTableSpec{{
			Name: "t",
			Columns: []ColumnSpec{
				{Name: "z", Type: "UInt8", First: true},
				{Name: "tail", Type: "UInt8"},
			},
		}},
	}
	require.NoError(t, applyPatches(&db))
	assert.Equal(t, []string{"z", "a", "b", "tail"}, columnOrder(db.Tables[0].Columns))
	for _, c := range db.Tables[0].Columns {
		assert.False(t, c.First, "placement is cleared on application")
		assert.Nil(t, c.After)
	}
}

// after resolves against the post-modify/drop state: it can follow a column
// modified in the same patch, and naming a just-dropped column errors.
func TestPatchTable_AfterSeesPostDropState(t *testing.T) {
	base := func() DatabaseSpec {
		return DatabaseSpec{
			Name: "posthog",
			Tables: []TableSpec{{
				Name: "t",
				Columns: []ColumnSpec{
					{Name: "a", Type: "UInt8"},
					{Name: "b", Type: "UInt8"},
				},
			}},
		}
	}

	ok := base()
	ok.Patches = []PatchTableSpec{{
		Name:          "t",
		ModifyColumns: []ColumnSpec{{Name: "a", Type: "UInt64"}},
		DropColumns:   []string{"b"},
		Columns:       []ColumnSpec{{Name: "c", Type: "UInt8", After: utils.Ptr("a")}},
	}}
	require.NoError(t, applyPatches(&ok))
	assert.Equal(t, []string{"a", "c"}, columnOrder(ok.Tables[0].Columns))

	bad := base()
	bad.Patches = []PatchTableSpec{{
		Name:        "t",
		DropColumns: []string{"b"},
		Columns:     []ColumnSpec{{Name: "c", Type: "UInt8", After: utils.Ptr("b")}},
	}}
	require.ErrorContains(t, applyPatches(&bad), `after references unknown column "b"`)
}

// Every misuse errors: both placements at once, unknown target, placement on
// modify_column, and placement on declared table / abstract-inherited / MV
// columns.
func TestPatchTable_PositionErrors(t *testing.T) {
	patchCase := func(patch PatchTableSpec) error {
		db := DatabaseSpec{
			Name:    "posthog",
			Tables:  []TableSpec{{Name: "t", Columns: []ColumnSpec{{Name: "a", Type: "UInt8"}}}},
			Patches: []PatchTableSpec{patch},
		}
		return applyPatches(&db)
	}

	require.ErrorContains(t, patchCase(PatchTableSpec{
		Name:    "t",
		Columns: []ColumnSpec{{Name: "x", Type: "UInt8", First: true, After: utils.Ptr("a")}},
	}), "first and after are mutually exclusive")

	require.ErrorContains(t, patchCase(PatchTableSpec{
		Name:    "t",
		Columns: []ColumnSpec{{Name: "x", Type: "UInt8", After: utils.Ptr("nope")}},
	}), `after references unknown column "nope"`)

	require.ErrorContains(t, patchCase(PatchTableSpec{
		Name:          "t",
		ModifyColumns: []ColumnSpec{{Name: "a", Type: "UInt64", First: true}},
	}), "after/first position an add, not a modify")

	declared := &Schema{Databases: []DatabaseSpec{{
		Name: "posthog",
		Tables: []TableSpec{{
			Name:    "t",
			OrderBy: []string{"a"},
			Columns: []ColumnSpec{{Name: "a", Type: "UInt8", First: true}},
			Engine:  &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
		}},
	}}}
	require.ErrorContains(t, Resolve(declared), "on a declared column the declaration order is the order")

	inherited := &Schema{Databases: []DatabaseSpec{{
		Name: "posthog",
		Tables: []TableSpec{
			{
				Name:     "base",
				Abstract: true,
				Columns:  []ColumnSpec{{Name: "a", Type: "UInt8", After: utils.Ptr("x")}},
			},
			{
				Name:    "child",
				Extend:  utils.Ptr("base"),
				OrderBy: []string{"a"},
				Engine:  &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
			},
		},
	}}}
	require.ErrorContains(t, Resolve(inherited), "after/first position a patch_table column add",
		"placement inherited from an abstract base still errors")

	mv := &Schema{Databases: []DatabaseSpec{{
		Name: "posthog",
		MaterializedViews: []MaterializedViewSpec{{
			Name:    "mv",
			ToTable: "t",
			Query:   "SELECT 1",
			Columns: []ColumnSpec{{Name: "a", Type: "UInt8", First: true}},
		}},
	}}}
	require.ErrorContains(t, Resolve(mv), "not valid on a materialized_view column")
}

// indexOrder flattens an index list to names for order assertions.
func indexOrder(idxs []IndexSpec) []string {
	out := make([]string, len(idxs))
	for i, x := range idxs {
		out[i] = x.Name
	}
	return out
}

// The issue #160 scenario in miniature: sharded_events' per-env minmax_mat_*
// indexes interleave mid-list. The positioned patch reproduces the order and
// the composed table renders byte-identical to the flat declaration.
func TestPatchTable_PositionedIndexAdds(t *testing.T) {
	root := t.TempDir()
	base := writePatchLayer(t, root, "base/events.hcl", `
database "posthog" {
  table "sharded_events" {
    order_by = ["id"]
    column "id"    { type = "UInt64" }
    column "mat_a" { type = "String" }
    column "mat_b" { type = "String" }
    index "minmax_mat_a" {
      expr        = "mat_a"
      type        = "minmax"
      granularity = 1
    }
    index "minmax_ts" {
      expr        = "id"
      type        = "minmax"
      granularity = 1
    }
    engine "merge_tree" {}
  }
}`)
	patch := writePatchLayer(t, root, "prod/patch.hcl", `
database "posthog" {
  patch_table "sharded_events" {
    index "minmax_mat_b" {
      expr        = "mat_b"
      type        = "minmax"
      granularity = 1
      after       = "minmax_mat_a"
    }
  }
}`)
	flat := writePatchLayer(t, root, "flat/events.hcl", `
database "posthog" {
  table "sharded_events" {
    order_by = ["id"]
    column "id"    { type = "UInt64" }
    column "mat_a" { type = "String" }
    column "mat_b" { type = "String" }
    index "minmax_mat_a" {
      expr        = "mat_a"
      type        = "minmax"
      granularity = 1
    }
    index "minmax_mat_b" {
      expr        = "mat_b"
      type        = "minmax"
      granularity = 1
    }
    index "minmax_ts" {
      expr        = "id"
      type        = "minmax"
      granularity = 1
    }
    engine "merge_tree" {}
  }
}`)

	composed, err := LoadLayers([]string{base, patch})
	require.NoError(t, err)
	require.NoError(t, Resolve(composed))
	assert.Equal(t, []string{"minmax_mat_a", "minmax_mat_b", "minmax_ts"},
		indexOrder(composed.Databases[0].Tables[0].Indexes))

	declared, err := LoadLayers([]string{flat})
	require.NoError(t, err)
	require.NoError(t, Resolve(declared))

	var composedOut, declaredOut bytes.Buffer
	require.NoError(t, Write(&composedOut, composed))
	require.NoError(t, Write(&declaredOut, declared))
	assert.Equal(t, declaredOut.String(), composedOut.String(),
		"base+patch renders byte-identical to the flat declaration (golden parity)")
}

// first, chained after, and a positioned drop+add redefine — drop+add is the
// sanctioned index redefine, and a redefine lands where you put it.
func TestPatchTable_IndexPlacementForms(t *testing.T) {
	idx := func(name string) IndexSpec {
		return IndexSpec{Name: name, Expr: "x", Type: "minmax", Granularity: 1}
	}
	withPos := func(name string, after *string, first bool) IndexSpec {
		i := idx(name)
		i.After, i.First = after, first
		return i
	}

	db := DatabaseSpec{
		Name: "posthog",
		Tables: []TableSpec{{
			Name:    "t",
			Indexes: []IndexSpec{idx("a"), idx("b")},
		}},
		Patches: []PatchTableSpec{{
			Name:        "t",
			DropIndexes: []string{"b"},
			Indexes: []IndexSpec{
				withPos("z", nil, true),               // first
				withPos("a2", utils.Ptr("a"), false),  // after base index
				withPos("a3", utils.Ptr("a2"), false), // chained after same-patch add
				withPos("b", utils.Ptr("z"), false),   // positioned drop+add redefine
			},
		}},
	}
	require.NoError(t, applyPatches(&db))
	got := db.Tables[0].Indexes
	assert.Equal(t, []string{"z", "b", "a", "a2", "a3"}, indexOrder(got))
	for _, x := range got {
		assert.Nil(t, x.After, "placement is cleared on application")
		assert.False(t, x.First)
	}
}

// Index placement misuse errors: both set, unknown/dropped target, and
// placement on a declared index (including inherited from an abstract base).
func TestPatchTable_IndexPositionErrors(t *testing.T) {
	patchCase := func(patch PatchTableSpec) error {
		db := DatabaseSpec{
			Name: "posthog",
			Tables: []TableSpec{{
				Name:    "t",
				Indexes: []IndexSpec{{Name: "a", Expr: "x", Type: "minmax"}},
			}},
			Patches: []PatchTableSpec{patch},
		}
		return applyPatches(&db)
	}

	require.ErrorContains(t, patchCase(PatchTableSpec{
		Name:    "t",
		Indexes: []IndexSpec{{Name: "i", Expr: "x", Type: "minmax", First: true, After: utils.Ptr("a")}},
	}), "first and after are mutually exclusive")

	require.ErrorContains(t, patchCase(PatchTableSpec{
		Name:    "t",
		Indexes: []IndexSpec{{Name: "i", Expr: "x", Type: "minmax", After: utils.Ptr("nope")}},
	}), `after references unknown index "nope"`)

	require.ErrorContains(t, patchCase(PatchTableSpec{
		Name:        "t",
		DropIndexes: []string{"a"},
		Indexes:     []IndexSpec{{Name: "i", Expr: "x", Type: "minmax", After: utils.Ptr("a")}},
	}), `after references unknown index "a"`)

	declared := &Schema{Databases: []DatabaseSpec{{
		Name: "posthog",
		Tables: []TableSpec{{
			Name:    "t",
			OrderBy: []string{"c"},
			Columns: []ColumnSpec{{Name: "c", Type: "UInt8"}},
			Indexes: []IndexSpec{{Name: "i", Expr: "c", Type: "minmax", First: true}},
			Engine:  &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
		}},
	}}}
	require.ErrorContains(t, Resolve(declared), "on a declared index the declaration order is the order")

	inherited := &Schema{Databases: []DatabaseSpec{{
		Name: "posthog",
		Tables: []TableSpec{
			{
				Name:     "base",
				Abstract: true,
				Columns:  []ColumnSpec{{Name: "c", Type: "UInt8"}},
				Indexes:  []IndexSpec{{Name: "i", Expr: "c", Type: "minmax", After: utils.Ptr("x")}},
			},
			{
				Name:    "child",
				Extend:  utils.Ptr("base"),
				OrderBy: []string{"c"},
				Engine:  &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
			},
		},
	}}}
	require.ErrorContains(t, Resolve(inherited), "on a declared index the declaration order is the order",
		"placement inherited from an abstract base still errors")
}
