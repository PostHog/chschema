package hcl

import (
	"testing"

	"github.com/posthog/chschema/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The issue #154 headline: a Distributed table whose target moves with the
// env's topology. The table is declared once; the env patch replaces the
// engine block wholesale and nothing else.
func TestPatchTable_EngineReplace(t *testing.T) {
	root := t.TempDir()
	base := writePatchLayer(t, root, "cloud/raw_sessions.hcl", `
database "posthog" {
  table "raw_sessions" {
    order_by = ["session_id_v7"]
    column "session_id_v7" { type = "UInt128" }
    engine "distributed" {
      cluster_name    = "sessions"
      remote_database = "posthog"
      remote_table    = "raw_sessions"
      sharding_key    = "cityHash64(session_id_v7)"
    }
  }
}`)
	dev := writePatchLayer(t, root, "dev/patch.hcl", `
database "posthog" {
  patch_table "raw_sessions" {
    engine "distributed" {
      cluster_name    = "posthog"
      remote_database = "posthog"
      remote_table    = "sharded_raw_sessions"
      sharding_key    = "cityHash64(session_id_v7)"
    }
  }
}`)

	prod, err := LoadLayers([]string{base})
	require.NoError(t, err)
	require.NoError(t, Resolve(prod))

	composed, err := LoadLayers([]string{base, dev})
	require.NoError(t, err)
	require.NoError(t, Resolve(composed))

	got := composed.Databases[0].Tables[0]
	require.NotNil(t, got.Engine)
	assert.Equal(t, EngineDistributed{
		ClusterName:    "posthog",
		RemoteDatabase: "posthog",
		RemoteTable:    "sharded_raw_sessions",
		ShardingKey:    utils.Ptr("cityHash64(session_id_v7)"),
	}, got.Engine.Decoded, "the patch replaces the engine block wholesale")
	assert.Equal(t, []string{"session_id_v7"}, got.OrderBy, "everything else is untouched")

	// The cross-env diff is exactly the engine change.
	cs := Diff(prod, composed)
	require.Len(t, cs.Databases, 1)
	require.Len(t, cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]
	require.NotNil(t, td.EngineChange)
	assert.Empty(t, td.AddColumns)
	assert.Empty(t, td.ModifyColumns)
}

// order_by / partition_by / sample_by / ttl replace when the patch sets
// them; unset fields keep the target's values.
func TestPatchTable_ScalarClausesReplace(t *testing.T) {
	db := DatabaseSpec{
		Name: "posthog",
		Tables: []TableSpec{{
			Name:        "t",
			OrderBy:     []string{"id"},
			PartitionBy: utils.Ptr("toYYYYMM(ts)"),
			SampleBy:    utils.Ptr("id"),
			TTL:         utils.Ptr("ts + INTERVAL 1 MONTH"),
		}},
		Patches: []PatchTableSpec{{
			Name:    "t",
			OrderBy: []string{"team_id", "id"},
			TTL:     utils.Ptr("ts + INTERVAL 6 MONTH"),
		}},
	}
	require.NoError(t, applyPatches(&db))
	got := db.Tables[0]
	assert.Equal(t, []string{"team_id", "id"}, got.OrderBy)
	assert.Equal(t, "ts + INTERVAL 6 MONTH", *got.TTL)
	assert.Equal(t, "toYYYYMM(ts)", *got.PartitionBy, "unset patch fields keep the target's values")
	assert.Equal(t, "id", *got.SampleBy)
}

// modify_column replaces in place (position kept); drop_columns removes;
// adds see the post-drop state, so drop+add moves a column to the end while
// modify changes it where it stands.
func TestPatchTable_ColumnModifyDropAdd(t *testing.T) {
	db := DatabaseSpec{
		Name: "posthog",
		Tables: []TableSpec{{
			Name: "t",
			Columns: []ColumnSpec{
				{Name: "a", Type: "UInt32"},
				{Name: "b", Type: "String"},
				{Name: "c", Type: "UInt8"},
			},
		}},
		Patches: []PatchTableSpec{{
			Name:          "t",
			ModifyColumns: []ColumnSpec{{Name: "a", Type: "UInt64"}},
			DropColumns:   []string{"c"},
			Columns:       []ColumnSpec{{Name: "d", Type: "Date"}},
		}},
	}
	require.NoError(t, applyPatches(&db))
	got := db.Tables[0].Columns
	require.Len(t, got, 3)
	assert.Equal(t, ColumnSpec{Name: "a", Type: "UInt64"}, got[0], "modified in place, position kept")
	assert.Equal(t, "b", got[1].Name)
	assert.Equal(t, "d", got[2].Name)

	for name, patch := range map[string]PatchTableSpec{
		"modify unknown": {Name: "t", ModifyColumns: []ColumnSpec{{Name: "nope", Type: "UInt8"}}},
		"drop unknown":   {Name: "t", DropColumns: []string{"nope"}},
		"add existing":   {Name: "t", Columns: []ColumnSpec{{Name: "b", Type: "String"}}},
	} {
		bad := DatabaseSpec{
			Name:    "posthog",
			Tables:  []TableSpec{{Name: "t", Columns: []ColumnSpec{{Name: "b", Type: "String"}}}},
			Patches: []PatchTableSpec{patch},
		}
		assert.Error(t, applyPatches(&bad), name)
	}
}

// Index drops apply before adds, so a drop+add pair in one patch redefines
// an index; adding an existing name without the drop errors.
func TestPatchTable_IndexDropThenAdd(t *testing.T) {
	base := func() DatabaseSpec {
		return DatabaseSpec{
			Name: "posthog",
			Tables: []TableSpec{{
				Name:    "t",
				Indexes: []IndexSpec{{Name: "idx", Expr: "a", Type: "minmax", Granularity: 1}},
			}},
		}
	}

	db := base()
	db.Patches = []PatchTableSpec{{
		Name:        "t",
		DropIndexes: []string{"idx"},
		Indexes:     []IndexSpec{{Name: "idx", Expr: "a", Type: "minmax", Granularity: 4}},
	}}
	require.NoError(t, applyPatches(&db))
	require.Len(t, db.Tables[0].Indexes, 1)
	assert.Equal(t, 4, db.Tables[0].Indexes[0].Granularity, "drop+add in one patch redefines the index")

	dup := base()
	dup.Patches = []PatchTableSpec{{Name: "t", Indexes: []IndexSpec{{Name: "idx", Expr: "a", Type: "minmax"}}}}
	require.ErrorContains(t, applyPatches(&dup), "already exists on target (drop it in the same patch to redefine)")

	missing := base()
	missing.Patches = []PatchTableSpec{{Name: "t", DropIndexes: []string{"nope"}}}
	require.ErrorContains(t, applyPatches(&missing), `drop_indexes "nope" does not exist`)
}

// patch_view replaces the query, and the patched query normalizes to the
// same canonical form as a declared one — a heredoc patch and a one-liner
// declaration of the same SQL diff clean.
func TestPatchView_QueryReplaceAndNormalize(t *testing.T) {
	root := t.TempDir()
	oneLiner := writePatchLayer(t, root, "a/v.hcl", `
database "posthog" {
  view "v" {
    query = "SELECT a, b FROM t WHERE x = 1"
  }
}`)
	patched := writePatchLayer(t, root, "b/v.hcl", `
database "posthog" {
  view "v" {
    query = "SELECT 1"
  }
}`)
	patch := writePatchLayer(t, root, "c/patch.hcl", `
database "posthog" {
  patch_view "v" {
    query = <<-SQL
      SELECT a, b
      FROM t
      WHERE x = 1
    SQL
  }
}`)

	left, err := LoadLayers([]string{oneLiner})
	require.NoError(t, err)
	require.NoError(t, Resolve(left))

	right, err := LoadLayers([]string{patched, patch})
	require.NoError(t, err)
	require.NoError(t, Resolve(right))

	assert.True(t, Diff(left, right).IsEmpty(),
		"a heredoc patch_view query and the equivalent declared one-liner must converge")
}

func TestPatchView_CommentAndUnknownTarget(t *testing.T) {
	db := DatabaseSpec{
		Name:        "posthog",
		Views:       []ViewSpec{{Name: "v", Query: "SELECT 1", Comment: utils.Ptr("old")}},
		ViewPatches: []PatchViewSpec{{Name: "v", Comment: utils.Ptr("new")}},
	}
	require.NoError(t, applyViewPatches(&db))
	assert.Equal(t, "new", *db.Views[0].Comment)
	assert.Equal(t, "SELECT 1", db.Views[0].Query, "unset patch fields keep the target's values")

	bad := DatabaseSpec{Name: "posthog", ViewPatches: []PatchViewSpec{{Name: "nope"}}}
	require.ErrorContains(t, applyViewPatches(&bad), `patch_view "nope" references unknown view`)
}

// patch_dictionary replaces source/layout/lifetime wholesale (decoded at
// parse) and merges settings patch-wins.
func TestPatchDictionary_SourceReplace(t *testing.T) {
	root := t.TempDir()
	base := writePatchLayer(t, root, "base/d.hcl", `
database "posthog" {
  dictionary "geo" {
    primary_key = ["id"]
    attribute "id" { type = "UInt64" }
    source "clickhouse" { table = "geo_src" }
    layout "flat" {}
    lifetime { min = 0 }
    settings = { max_threads = "1" }
  }
}`)
	env := writePatchLayer(t, root, "env/patch.hcl", `
database "posthog" {
  patch_dictionary "geo" {
    source "clickhouse" { table = "geo_src_dev" }
    lifetime { min = 600 }
    settings = { max_block_size = "8192" }
  }
}`)

	s, err := LoadLayers([]string{base, env})
	require.NoError(t, err)
	require.NoError(t, Resolve(s))

	d := s.Databases[0].Dictionaries[0]
	require.NotNil(t, d.Source)
	src, ok := d.Source.Decoded.(SourceClickHouse)
	require.True(t, ok, "the patched source is decoded")
	assert.Equal(t, utils.Ptr("geo_src_dev"), src.Table)
	assert.Equal(t, "flat", d.Layout.Kind, "unset patch fields keep the target's values")
	require.NotNil(t, d.Lifetime)
	assert.Equal(t, int64(600), *d.Lifetime.Min)
	assert.Equal(t, map[string]string{"max_threads": "1", "max_block_size": "8192"}, d.Settings,
		"dictionary settings merge patch-wins, like patch_table's")

	bad := DatabaseSpec{Name: "posthog", DictionaryPatches: []PatchDictionarySpec{{Name: "nope"}}}
	require.ErrorContains(t, applyDictionaryPatches(&bad), `patch_dictionary "nope" references unknown dictionary`)
}

// patch_view / patch_dictionary sites scan as patches, so the once-only
// audit (locate -duplicates) does not count them as redeclarations.
func TestPatchViewDictionary_LocateExempt(t *testing.T) {
	dir := t.TempDir()
	path := writeHCL(t, dir, "s.hcl", `
database "posthog" {
  view "v" { query = "SELECT 1" }
  patch_view "v" { query = "SELECT 2" }

  dictionary "geo" { primary_key = ["id"] }
  patch_dictionary "geo" { settings = { max_threads = "2" } }
}
`)
	decls, err := ScanDeclarations([]string{path})
	require.NoError(t, err)
	require.Len(t, decls, 4)
	assert.True(t, decls[1].Patch, "patch_view is a patch site")
	assert.Equal(t, KindView, decls[1].ObjectType)
	assert.True(t, decls[3].Patch, "patch_dictionary is a patch site")
	assert.Equal(t, KindDictionary, decls[3].ObjectType)

	assert.Empty(t, FindDuplicates(decls), "patches never count toward the once-only audit")
}
