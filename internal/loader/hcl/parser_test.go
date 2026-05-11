package hcl

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ptr[T any](v T) *T { return &v }

func TestParseFile_BasicTable(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "table_basic.hcl"))
	require.NoError(t, err)

	expected := []DatabaseSpec{
		{
			Name: "posthog",
			Tables: []TableSpec{
				{
					Name: "events",
					Columns: []ColumnSpec{
						{Name: "id", Type: "UUID"},
					},
				},
			},
		},
	}
	assert.Equal(t, expected, dbs)
}

func TestParseFile_FullTable(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "table_full.hcl"))
	require.NoError(t, err)
	require.Len(t, dbs, 1)
	require.Len(t, dbs[0].Tables, 1)

	tbl := dbs[0].Tables[0]

	// Engine.Body is an opaque hcl.Body; assert the Decoded value
	// separately, then strip Body so the rest can be compared whole.
	require.NotNil(t, tbl.Engine)
	assert.NotNil(t, tbl.Engine.Body)
	assert.Equal(t, EngineReplicatedMergeTree{
		ZooPath:     "/clickhouse/tables/{shard}/events",
		ReplicaName: "{replica}",
	}, tbl.Engine.Decoded)
	tbl.Engine = &EngineSpec{
		Kind:    tbl.Engine.Kind,
		Decoded: tbl.Engine.Decoded,
	}

	expected := TableSpec{
		Name:        "events",
		Extend:      ptr("_event_base"),
		Abstract:    false,
		Override:    true,
		OrderBy:     []string{"timestamp", "team_id"},
		PartitionBy: ptr("toYYYYMM(timestamp)"),
		SampleBy:    ptr("team_id"),
		TTL:         ptr("timestamp + INTERVAL 2 YEARS"),
		Settings: map[string]string{
			"ttl_only_drop_parts": "1",
			"index_granularity":   "8192",
		},
		Columns: []ColumnSpec{
			{Name: "timestamp", Type: "DateTime"},
			{Name: "team_id", Type: "UInt64"},
			{Name: "event", Type: "String"},
		},
		Indexes: []IndexSpec{
			{Name: "idx_team", Expr: "team_id", Type: "minmax", Granularity: 4},
		},
		Engine: &EngineSpec{
			Kind: "replicated_merge_tree",
			Decoded: EngineReplicatedMergeTree{
				ZooPath:     "/clickhouse/tables/{shard}/events",
				ReplicaName: "{replica}",
			},
		},
	}
	assert.Equal(t, expected, tbl)
}

func TestParseFile_MissingColumnType(t *testing.T) {
	_, err := ParseFile(filepath.Join("testdata", "table_invalid_missing_type.hcl"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type")
}

func TestParseFile_UnknownAttribute(t *testing.T) {
	_, err := ParseFile(filepath.Join("testdata", "table_invalid_unknown_attr.hcl"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not_a_real_attr")
}
