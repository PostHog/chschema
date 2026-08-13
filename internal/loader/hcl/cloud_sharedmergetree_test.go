package hcl

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	sharedPath    = "/clickhouse/tables/{uuid}/{shard}"
	sharedReplica = "{replica}"
)

func TestSharedMergeTreeFamilies_SQLRoundTrip(t *testing.T) {
	version := "version"
	isDeleted := "is_deleted"
	tests := []struct {
		name       string
		engineSQL  string
		wantEngine Engine
		wantSQL    string
	}{
		{
			name:       "merge tree",
			engineSQL:  "SharedMergeTree('" + sharedPath + "', '" + sharedReplica + "')",
			wantEngine: EngineSharedMergeTree{ZooPath: sharedPath, ReplicaName: sharedReplica},
			wantSQL:    "SharedMergeTree('" + sharedPath + "', '" + sharedReplica + "')",
		},
		{
			name:       "replacing without version",
			engineSQL:  "SharedReplacingMergeTree('" + sharedPath + "', '" + sharedReplica + "')",
			wantEngine: EngineSharedReplacingMergeTree{ZooPath: sharedPath, ReplicaName: sharedReplica},
			wantSQL:    "SharedReplacingMergeTree('" + sharedPath + "', '" + sharedReplica + "')",
		},
		{
			name:      "replacing with version and is_deleted",
			engineSQL: "SharedReplacingMergeTree('" + sharedPath + "', '" + sharedReplica + "', version, is_deleted)",
			wantEngine: EngineSharedReplacingMergeTree{
				ZooPath: sharedPath, ReplicaName: sharedReplica,
				VersionColumn: &version, IsDeletedColumn: &isDeleted,
			},
			wantSQL: "SharedReplacingMergeTree('" + sharedPath + "', '" + sharedReplica + "', version, is_deleted)",
		},
		{
			name:      "summing",
			engineSQL: "SharedSummingMergeTree('" + sharedPath + "', '" + sharedReplica + "', (total, other))",
			wantEngine: EngineSharedSummingMergeTree{
				ZooPath: sharedPath, ReplicaName: sharedReplica, SumColumns: []string{"total", "other"},
			},
			wantSQL: "SharedSummingMergeTree('" + sharedPath + "', '" + sharedReplica + "', (total, other))",
		},
		{
			name:      "collapsing",
			engineSQL: "SharedCollapsingMergeTree('" + sharedPath + "', '" + sharedReplica + "', sign)",
			wantEngine: EngineSharedCollapsingMergeTree{
				ZooPath: sharedPath, ReplicaName: sharedReplica, SignColumn: "sign",
			},
			wantSQL: "SharedCollapsingMergeTree('" + sharedPath + "', '" + sharedReplica + "', sign)",
		},
		{
			name:      "aggregating",
			engineSQL: "SharedAggregatingMergeTree('" + sharedPath + "', '" + sharedReplica + "')",
			wantEngine: EngineSharedAggregatingMergeTree{
				ZooPath: sharedPath, ReplicaName: sharedReplica,
			},
			wantSQL: "SharedAggregatingMergeTree('" + sharedPath + "', '" + sharedReplica + "')",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ddl := "CREATE TABLE db.events (id UUID, version UInt64, is_deleted UInt8, total UInt64, other UInt64, sign Int8) ENGINE = " + tc.engineSQL + " ORDER BY id"
			table, err := buildTableFromCreateSQL(ddl)
			require.NoError(t, err)
			require.NotNil(t, table.Engine)
			assert.Equal(t, tc.wantEngine, table.Engine.Decoded)

			gotSQL, extra := engineSQL(table.Engine.Decoded)
			assert.Equal(t, tc.wantSQL, gotSQL)
			assert.Empty(t, extra)
		})
	}
}

func TestSharedMergeTreeFamilies_RejectUnknownArguments(t *testing.T) {
	tests := []string{
		"SharedMergeTree('p', 'r', unexpected)",
		"SharedReplacingMergeTree('p')",
		"SharedReplacingMergeTree('p', 'r', ver, deleted, unexpected)",
		"SharedSummingMergeTree('p')",
		"SharedCollapsingMergeTree('p', 'r')",
		"SharedAggregatingMergeTree('p', 'r', unexpected)",
	}

	for _, engine := range tests {
		t.Run(engine, func(t *testing.T) {
			_, err := buildTableFromCreateSQL("CREATE TABLE db.t (id UUID) ENGINE = " + engine + " ORDER BY id")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "Shared")
		})
	}
}

func TestSharedMergeTreeFamilies_HCLRoundTrip(t *testing.T) {
	version := "version"
	isDeleted := "is_deleted"
	want := []Engine{
		EngineSharedMergeTree{ZooPath: sharedPath, ReplicaName: sharedReplica},
		EngineSharedReplacingMergeTree{
			ZooPath: sharedPath, ReplicaName: sharedReplica,
			VersionColumn: &version, IsDeletedColumn: &isDeleted,
		},
		EngineSharedSummingMergeTree{
			ZooPath: sharedPath, ReplicaName: sharedReplica, SumColumns: []string{"total", "other"},
		},
		EngineSharedCollapsingMergeTree{
			ZooPath: sharedPath, ReplicaName: sharedReplica, SignColumn: "sign",
		},
		EngineSharedAggregatingMergeTree{ZooPath: sharedPath, ReplicaName: sharedReplica},
	}

	db := DatabaseSpec{Name: "db"}
	for i, engine := range want {
		db.Tables = append(db.Tables, TableSpec{
			Name:    "table_" + string(rune('a'+i)),
			Columns: []ColumnSpec{{Name: "id", Type: "UUID"}},
			Engine:  &EngineSpec{Kind: engine.Kind(), Decoded: engine},
		})
	}

	var out bytes.Buffer
	require.NoError(t, Write(&out, &Schema{Databases: []DatabaseSpec{db}}))
	for _, engine := range want {
		assert.Contains(t, out.String(), `engine "`+engine.Kind()+`"`)
	}
	assert.Contains(t, out.String(), `zoo_path     = "`+sharedPath+`"`)
	assert.Contains(t, out.String(), `replica_name = "`+sharedReplica+`"`)

	path := filepath.Join(t.TempDir(), "shared.hcl")
	require.NoError(t, os.WriteFile(path, out.Bytes(), 0o600))
	got, err := ParseFile(path)
	require.NoError(t, err)
	require.Len(t, got.Databases, 1)
	require.Len(t, got.Databases[0].Tables, len(want))
	for i, table := range got.Databases[0].Tables {
		assert.Equal(t, want[i], table.Engine.Decoded)
	}
}

func TestSQL2HCLAcceptsSharedMergeTree(t *testing.T) {
	schema := &Schema{}
	applied, err := ApplySQL(schema, "CREATE TABLE db.events (id UUID) ENGINE = SharedMergeTree('"+sharedPath+"', '"+sharedReplica+"') ORDER BY id", "db", false)
	require.NoError(t, err)
	assert.Equal(t, 1, applied)
	require.Len(t, schema.Databases, 1)
	require.Len(t, schema.Databases[0].Tables, 1)
	assert.Equal(t,
		EngineSharedMergeTree{ZooPath: sharedPath, ReplicaName: sharedReplica},
		schema.Databases[0].Tables[0].Engine.Decoded)
}
