package hcl

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stripEngineBodies clears the opaque hcl.Body on every engine so resolved
// structures can be compared whole. Decoded values remain.
func stripEngineBodies(dbs []DatabaseSpec) {
	for di := range dbs {
		for ti := range dbs[di].Tables {
			tbl := &dbs[di].Tables[ti]
			if tbl.Engine != nil {
				tbl.Engine.Body = nil
			}
		}
	}
}

func TestResolve_BasicHappyPath(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "resolve_basic.hcl"))
	require.NoError(t, err)
	require.NoError(t, Resolve(dbs))
	stripEngineBodies(dbs)

	rmtEngine := &EngineSpec{
		Kind: "replicated_merge_tree",
		Decoded: EngineReplicatedMergeTree{
			ZooPath:     "/clickhouse/tables/{shard}/events_local",
			ReplicaName: "{replica}",
		},
	}
	commonCols := []ColumnSpec{
		{Name: "timestamp", Type: "DateTime"},
		{Name: "team_id", Type: "UInt64"},
		{Name: "event", Type: "String"},
	}

	expected := []DatabaseSpec{
		{
			Name: "posthog",
			Tables: []TableSpec{
				{
					Name:    "events_local",
					OrderBy: []string{"timestamp", "team_id"},
					Columns: commonCols,
					Engine:  rmtEngine,
				},
				{
					Name:    "events_by_team",
					OrderBy: []string{"team_id", "timestamp"},
					Columns: commonCols,
					Engine:  rmtEngine,
				},
				{
					Name:    "events_distributed",
					OrderBy: []string{"timestamp", "team_id"},
					Columns: commonCols,
					Engine: &EngineSpec{
						Kind: "distributed",
						Decoded: EngineDistributed{
							ClusterName:    "posthog",
							RemoteDatabase: "default",
							RemoteTable:    "events_local",
						},
					},
				},
			},
		},
	}
	assert.Equal(t, expected, dbs)
}

func TestResolve_Cycle(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "resolve_cycle.hcl"))
	require.NoError(t, err)
	err = Resolve(dbs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle")
}

func TestResolve_SelfCycle(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "resolve_self_cycle.hcl"))
	require.NoError(t, err)
	err = Resolve(dbs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle")
}

func TestResolve_MissingParent(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "resolve_missing_parent.hcl"))
	require.NoError(t, err)
	err = Resolve(dbs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does_not_exist")
}

func TestResolve_ColumnCollision(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "resolve_column_collision.hcl"))
	require.NoError(t, err)
	err = Resolve(dbs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "collides")
}

func TestResolve_NoEngineOnNonAbstract(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "resolve_no_engine.hcl"))
	require.NoError(t, err)
	err = Resolve(dbs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "engine")
}
