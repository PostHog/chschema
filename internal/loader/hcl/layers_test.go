package hcl

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func layerPath(scenario, layer string) string {
	return filepath.Join("testdata", "layers", scenario, layer)
}

func TestLoadLayers_BasicPatchFromHigherLayer(t *testing.T) {
	schema, err := LoadLayers([]string{
		layerPath("basic_patch", "base"),
		layerPath("basic_patch", "env_us"),
	})
	require.NoError(t, err)
	require.NoError(t, Resolve(schema))
	stripEngineBodies(schema.Databases)

	expected := []DatabaseSpec{
		{
			Name: "posthog",
			Tables: []TableSpec{
				{
					Name:    "events",
					OrderBy: []string{"timestamp", "team_id"},
					Columns: []ColumnSpec{
						{Name: "timestamp", Type: "DateTime"},
						{Name: "team_id", Type: "UInt64"},
						{Name: "us_session_id", Type: "String"},
					},
					Engine: &EngineSpec{
						Kind:    "merge_tree",
						Decoded: EngineMergeTree{},
					},
				},
			},
		},
	}
	assert.Equal(t, expected, schema.Databases)
}

func TestLoadLayers_OverrideAcrossLayers(t *testing.T) {
	schema, err := LoadLayers([]string{
		layerPath("override", "base"),
		layerPath("override", "env_dev"),
	})
	require.NoError(t, err)
	require.NoError(t, Resolve(schema))
	stripEngineBodies(schema.Databases)

	require.Len(t, schema.Databases, 1)
	require.Len(t, schema.Databases[0].Tables, 1)
	tbl := schema.Databases[0].Tables[0]
	assert.Equal(t, "events", tbl.Name)
	assert.Equal(t, []ColumnSpec{{Name: "dev_id", Type: "UInt32"}}, tbl.Columns)
	require.NotNil(t, tbl.Engine)
	assert.Equal(t, EngineLog{}, tbl.Engine.Decoded)
}

func TestLoadLayers_CollisionWithoutOverrideErrors(t *testing.T) {
	_, err := LoadLayers([]string{
		layerPath("collision_no_override", "base"),
		layerPath("collision_no_override", "env_dev"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "override")
}

func TestLoadLayers_ViewRedeclareAcrossLayers(t *testing.T) {
	_, err := LoadLayers([]string{
		layerPath("view_redeclare", "base"),
		layerPath("view_redeclare", "env_dev"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `view "v" redeclared across layers`)
}

func TestLoadLayers_PatchPropagatesThroughExtend(t *testing.T) {
	schema, err := LoadLayers([]string{
		layerPath("patch_with_extend", "base"),
		layerPath("patch_with_extend", "env_us"),
	})
	require.NoError(t, err)
	require.NoError(t, Resolve(schema))
	stripEngineBodies(schema.Databases)

	require.Len(t, schema.Databases, 1)
	// Abstract _event_base has been dropped; events_local remains.
	require.Len(t, schema.Databases[0].Tables, 1)
	tbl := schema.Databases[0].Tables[0]
	assert.Equal(t, "events_local", tbl.Name)
	// us_session_id was patched onto _event_base and inherited via extend.
	assert.Equal(t, []ColumnSpec{
		{Name: "timestamp", Type: "DateTime"},
		{Name: "team_id", Type: "UInt64"},
		{Name: "us_session_id", Type: "String"},
	}, tbl.Columns)
}

// TestLoadLayers_LaterLayerKeepsAllObjectTypes locks issue #80: an object
// declared in a LATER layer, for a database already seen in an EARLIER layer,
// must survive the merge. Before the fix, mergeIntoDatabase never iterated
// incoming.Dictionaries or incoming.Raws, so those two sibling collections
// were silently dropped when the shared layer was composed first; tables,
// materialized views and views were kept. This drives one subtest per
// per-database object type so the whole class of bug is covered.
func TestLoadLayers_LaterLayerKeepsAllObjectTypes(t *testing.T) {
	// The shared layer pre-seeds database "posthog" so the later layer takes
	// the merge path (mergeIntoDatabase) instead of the first-seen copy path.
	const shared = `database "posthog" {
  table "events" {
    column "timestamp" { type = "DateTime" }
    engine "merge_tree" {}
    order_by = ["timestamp"]
  }
}
`
	cases := []struct {
		name  string
		local string
		// count returns how many objects of the type under test resolved,
		// discounting anything the shared layer contributes.
		count func(DatabaseSpec) int
	}{
		{
			name: "table",
			local: `database "posthog" {
  table "events2" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}
`,
			count: func(db DatabaseSpec) int { return len(db.Tables) - 1 }, // minus baseline events
		},
		{
			name: "materialized_view",
			local: `database "posthog" {
  materialized_view "mv" {
    to_table = "posthog.events"
    query    = "SELECT timestamp FROM posthog.events"
    column "timestamp" { type = "DateTime" }
  }
}
`,
			count: func(db DatabaseSpec) int { return len(db.MaterializedViews) },
		},
		{
			name: "view",
			local: `database "posthog" {
  view "v" {
    query = "SELECT 1"
  }
}
`,
			count: func(db DatabaseSpec) int { return len(db.Views) },
		},
		{
			name: "dictionary",
			local: `database "posthog" {
  dictionary "d" {
    primary_key = ["id"]
    attribute "id"  { type = "UInt64" }
    attribute "val" { type = "String" }
    source "null" {}
    layout "flat" {}
  }
}
`,
			count: func(db DatabaseSpec) int { return len(db.Dictionaries) },
		},
		{
			name: "raw",
			local: `database "posthog" {
  raw "dictionary" "r" {
    sql = "CREATE DICTIONARY posthog.r (id UInt64) PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)"
  }
}
`,
			count: func(db DatabaseSpec) int { return len(db.Raws) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sharedDir := t.TempDir()
			writeLayerFile(t, sharedDir, "shared.hcl", shared)
			localDir := t.TempDir()
			writeLayerFile(t, localDir, "local.hcl", tc.local)

			// shared FIRST — the order that triggered the bug.
			schema, err := LoadLayers([]string{sharedDir, localDir})
			require.NoError(t, err)
			require.Len(t, schema.Databases, 1)
			assert.Equal(t, 1, tc.count(schema.Databases[0]),
				"object from the later layer must survive the merge (shared,local)")

			// Reverse order must resolve to the same object set.
			rev, err := LoadLayers([]string{localDir, sharedDir})
			require.NoError(t, err)
			require.Len(t, rev.Databases, 1)
			assert.Equal(t, 1, tc.count(rev.Databases[0]),
				"layer order must not change which objects resolve (local,shared)")
		})
	}
}

// TestLoadLayers_DictionaryLaterLayer is the exact issue-#80 repro: a
// dictionary defined only in a later layer must not be dropped.
func TestLoadLayers_DictionaryLaterLayer(t *testing.T) {
	sharedDir := t.TempDir()
	writeLayerFile(t, sharedDir, "shared.hcl", `database "posthog" {
  table "events" {
    column "timestamp" { type = "DateTime" }
    engine "merge_tree" {}
    order_by = ["timestamp"]
  }
}
`)
	localDir := t.TempDir()
	writeLayerFile(t, localDir, "dict.hcl", `database "posthog" {
  dictionary "web_bot_definition_dict" {
    primary_key = ["id"]
    attribute "id" { type = "UInt64" }
    source "null" {}
    layout "flat" {}
  }
}
`)

	schema, err := LoadLayers([]string{sharedDir, localDir})
	require.NoError(t, err)
	require.Len(t, schema.Databases, 1)
	require.Len(t, schema.Databases[0].Dictionaries, 1,
		"dictionary from a later layer must be kept")
	assert.Equal(t, "web_bot_definition_dict", schema.Databases[0].Dictionaries[0].Name)
}

// TestLoadLayers_DictionaryRedeclareAcrossLayers asserts the redeclare guard:
// the same dictionary name in two layers is an error (mirrors views/MVs).
func TestLoadLayers_DictionaryRedeclareAcrossLayers(t *testing.T) {
	const dictLayer = `database "posthog" {
  dictionary "d" {
    primary_key = ["id"]
    attribute "id" { type = "UInt64" }
    source "null" {}
    layout "flat" {}
  }
}
`
	a := t.TempDir()
	writeLayerFile(t, a, "a.hcl", dictLayer)
	b := t.TempDir()
	writeLayerFile(t, b, "b.hcl", dictLayer)

	_, err := LoadLayers([]string{a, b})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `dictionary "d" redeclared across layers`)
}

// TestLoadLayers_RawRedeclareAcrossLayers asserts the redeclare guard for raw
// blocks, keyed by (kind, name) like the diff engine.
func TestLoadLayers_RawRedeclareAcrossLayers(t *testing.T) {
	const rawLayer = `database "posthog" {
  raw "dictionary" "r" {
    sql = "CREATE DICTIONARY posthog.r (id UInt64) PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)"
  }
}
`
	a := t.TempDir()
	writeLayerFile(t, a, "a.hcl", rawLayer)
	b := t.TempDir()
	writeLayerFile(t, b, "b.hcl", rawLayer)

	_, err := LoadLayers([]string{a, b})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `raw "r" (dictionary) redeclared across layers`)
}

func TestResolve_PatchUnknownTarget(t *testing.T) {
	schema, err := ParseFile(filepath.Join("testdata", "patch_unknown_target.hcl"))
	require.NoError(t, err)
	err = Resolve(schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does_not_exist")
}

func TestResolve_PatchColumnCollision(t *testing.T) {
	schema, err := ParseFile(filepath.Join("testdata", "patch_column_collision.hcl"))
	require.NoError(t, err)
	err = Resolve(schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestParseFile_PatchDisallowedAttribute(t *testing.T) {
	_, err := ParseFile(filepath.Join("testdata", "patch_disallowed_attr.hcl"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "engine")
}
