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
	dbs, err := LoadLayers([]string{
		layerPath("basic_patch", "base"),
		layerPath("basic_patch", "env_us"),
	})
	require.NoError(t, err)
	require.NoError(t, Resolve(dbs))
	stripEngineBodies(dbs)

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
	assert.Equal(t, expected, dbs)
}

func TestLoadLayers_OverrideAcrossLayers(t *testing.T) {
	dbs, err := LoadLayers([]string{
		layerPath("override", "base"),
		layerPath("override", "env_dev"),
	})
	require.NoError(t, err)
	require.NoError(t, Resolve(dbs))
	stripEngineBodies(dbs)

	require.Len(t, dbs, 1)
	require.Len(t, dbs[0].Tables, 1)
	tbl := dbs[0].Tables[0]
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

func TestLoadLayers_PatchPropagatesThroughExtend(t *testing.T) {
	dbs, err := LoadLayers([]string{
		layerPath("patch_with_extend", "base"),
		layerPath("patch_with_extend", "env_us"),
	})
	require.NoError(t, err)
	require.NoError(t, Resolve(dbs))
	stripEngineBodies(dbs)

	require.Len(t, dbs, 1)
	// Abstract _event_base has been dropped; events_local remains.
	require.Len(t, dbs[0].Tables, 1)
	tbl := dbs[0].Tables[0]
	assert.Equal(t, "events_local", tbl.Name)
	// us_session_id was patched onto _event_base and inherited via extend.
	assert.Equal(t, []ColumnSpec{
		{Name: "timestamp", Type: "DateTime"},
		{Name: "team_id", Type: "UInt64"},
		{Name: "us_session_id", Type: "String"},
	}, tbl.Columns)
}

func TestResolve_PatchUnknownTarget(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "patch_unknown_target.hcl"))
	require.NoError(t, err)
	err = Resolve(dbs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does_not_exist")
}

func TestResolve_PatchColumnCollision(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "patch_column_collision.hcl"))
	require.NoError(t, err)
	err = Resolve(dbs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestParseFile_PatchDisallowedAttribute(t *testing.T) {
	_, err := ParseFile(filepath.Join("testdata", "patch_disallowed_attr.hcl"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "engine")
}
