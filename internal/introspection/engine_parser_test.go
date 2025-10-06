package introspection

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseEngine_MergeTree(t *testing.T) {
	tests := []struct {
		name       string
		engineName string
		engineFull string
	}{
		{
			name:       "simple MergeTree",
			engineName: "MergeTree",
			engineFull: "MergeTree ORDER BY id SETTINGS index_granularity = 8192",
		},
		{
			name:       "MergeTree with parens",
			engineName: "MergeTree",
			engineFull: "MergeTree() ORDER BY id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, err := ParseEngine(tt.engineName, tt.engineFull)
			require.NoError(t, err)
			require.NotNil(t, engine)
			require.NotNil(t, engine.GetMergeTree())
		})
	}
}

func TestParseEngine_ReplicatedMergeTree(t *testing.T) {
	engineName := "ReplicatedMergeTree"
	engineFull := "ReplicatedMergeTree('/clickhouse/tables/{shard}/test', '{replica}') ORDER BY id"

	engine, err := ParseEngine(engineName, engineFull)
	require.NoError(t, err)
	require.NotNil(t, engine)

	rmt := engine.GetReplicatedMergeTree()
	require.NotNil(t, rmt)
	require.Equal(t, "/clickhouse/tables/{shard}/test", rmt.ZooPath)
	require.Equal(t, "{replica}", rmt.ReplicaName)
}

func TestParseEngine_ReplacingMergeTree(t *testing.T) {
	tests := []struct {
		name          string
		engineName    string
		engineFull    string
		expectVersion *string
	}{
		{
			name:          "without version column",
			engineName:    "ReplacingMergeTree",
			engineFull:    "ReplacingMergeTree ORDER BY id",
			expectVersion: nil,
		},
		{
			name:          "with version column",
			engineName:    "ReplacingMergeTree",
			engineFull:    "ReplacingMergeTree(version) ORDER BY id",
			expectVersion: stringPtr("version"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, err := ParseEngine(tt.engineName, tt.engineFull)
			require.NoError(t, err)
			require.NotNil(t, engine)

			rmt := engine.GetReplacingMergeTree()
			require.NotNil(t, rmt)

			if tt.expectVersion == nil {
				require.Nil(t, rmt.VersionColumn)
			} else {
				require.NotNil(t, rmt.VersionColumn)
				require.Equal(t, *tt.expectVersion, *rmt.VersionColumn)
			}
		})
	}
}

func TestParseEngine_ReplicatedReplacingMergeTree(t *testing.T) {
	tests := []struct {
		name          string
		engineFull    string
		expectZooPath string
		expectReplica string
		expectVersion *string
	}{
		{
			name:          "without version column",
			engineFull:    "ReplicatedReplacingMergeTree('/path', 'replica1') ORDER BY id",
			expectZooPath: "/path",
			expectReplica: "replica1",
			expectVersion: nil,
		},
		{
			name:          "with version column",
			engineFull:    "ReplicatedReplacingMergeTree('/path', 'replica1', version) ORDER BY id",
			expectZooPath: "/path",
			expectReplica: "replica1",
			expectVersion: stringPtr("version"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, err := ParseEngine("ReplicatedReplacingMergeTree", tt.engineFull)
			require.NoError(t, err)
			require.NotNil(t, engine)

			rrmt := engine.GetReplicatedReplacingMergeTree()
			require.NotNil(t, rrmt)
			require.Equal(t, tt.expectZooPath, rrmt.ZooPath)
			require.Equal(t, tt.expectReplica, rrmt.ReplicaName)

			if tt.expectVersion == nil {
				require.Nil(t, rrmt.VersionColumn)
			} else {
				require.NotNil(t, rrmt.VersionColumn)
				require.Equal(t, *tt.expectVersion, *rrmt.VersionColumn)
			}
		})
	}
}

func TestParseEngine_SummingMergeTree(t *testing.T) {
	tests := []struct {
		name          string
		engineFull    string
		expectColumns []string
	}{
		{
			name:          "without columns",
			engineFull:    "SummingMergeTree ORDER BY id",
			expectColumns: nil,
		},
		{
			name:          "with single column",
			engineFull:    "SummingMergeTree((value)) ORDER BY id",
			expectColumns: []string{"value"},
		},
		{
			name:          "with multiple columns",
			engineFull:    "SummingMergeTree((val1, val2)) ORDER BY id",
			expectColumns: []string{"val1", "val2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, err := ParseEngine("SummingMergeTree", tt.engineFull)
			require.NoError(t, err)
			require.NotNil(t, engine)

			smt := engine.GetSummingMergeTree()
			require.NotNil(t, smt)

			if tt.expectColumns == nil {
				require.Empty(t, smt.SumColumns)
			} else {
				require.Equal(t, tt.expectColumns, smt.SumColumns)
			}
		})
	}
}

func TestParseEngine_Distributed(t *testing.T) {
	tests := []struct {
		name           string
		engineFull     string
		expectCluster  string
		expectDB       string
		expectTable    string
		expectSharding *string
	}{
		{
			name:           "without sharding key",
			engineFull:     "Distributed(cluster1, mydb, mytable) ORDER BY id",
			expectCluster:  "cluster1",
			expectDB:       "mydb",
			expectTable:    "mytable",
			expectSharding: nil,
		},
		{
			name:           "with sharding key",
			engineFull:     "Distributed(cluster1, mydb, mytable, cityHash64(id)) ORDER BY id",
			expectCluster:  "cluster1",
			expectDB:       "mydb",
			expectTable:    "mytable",
			expectSharding: stringPtr("cityHash64(id)"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, err := ParseEngine("Distributed", tt.engineFull)
			require.NoError(t, err)
			require.NotNil(t, engine)

			dist := engine.GetDistributed()
			require.NotNil(t, dist)
			require.Equal(t, tt.expectCluster, dist.ClusterName)
			require.Equal(t, tt.expectDB, dist.RemoteDatabase)
			require.Equal(t, tt.expectTable, dist.RemoteTable)

			if tt.expectSharding == nil {
				require.Nil(t, dist.ShardingKey)
			} else {
				require.NotNil(t, dist.ShardingKey)
				require.Equal(t, *tt.expectSharding, *dist.ShardingKey)
			}
		})
	}
}

func TestParseEngine_Log(t *testing.T) {
	engine, err := ParseEngine("Log", "Log ORDER BY id")
	require.NoError(t, err)
	require.NotNil(t, engine)
	require.NotNil(t, engine.GetLog())
}

func TestParseEngine_UnsupportedEngine(t *testing.T) {
	_, err := ParseEngine("UnsupportedEngine", "UnsupportedEngine()")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported engine type")
}

func TestExtractParameters(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect []string
	}{
		{
			name:   "no parameters",
			input:  "MergeTree",
			expect: []string{},
		},
		{
			name:   "empty parameters",
			input:  "MergeTree()",
			expect: []string{},
		},
		{
			name:   "single quoted parameter",
			input:  "ReplacingMergeTree('version')",
			expect: []string{"version"},
		},
		{
			name:   "multiple quoted parameters",
			input:  "ReplicatedMergeTree('/path', 'replica')",
			expect: []string{"/path", "replica"},
		},
		{
			name:   "mixed quotes",
			input:  `ReplicatedMergeTree("/path", 'replica')`,
			expect: []string{"/path", "replica"},
		},
		{
			name:   "parameter with comma inside quotes",
			input:  `Distributed('cluster', 'db', 'table', 'shardFunc(a, b)')`,
			expect: []string{"cluster", "db", "table", "shardFunc(a, b)"},
		},
		{
			name:   "unquoted parameters",
			input:  "ReplacingMergeTree(version)",
			expect: []string{"version"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params, err := extractParameters(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.expect, params)
		})
	}
}

func TestParseEngine_CollapsingMergeTree(t *testing.T) {
	engineName := "CollapsingMergeTree"
	engineFull := "CollapsingMergeTree(sign) ORDER BY id"

	engine, err := ParseEngine(engineName, engineFull)
	require.NoError(t, err)
	require.NotNil(t, engine)

	cmt := engine.GetCollapsingMergeTree()
	require.NotNil(t, cmt)
	require.Equal(t, "sign", cmt.SignColumn)
}

func TestParseEngine_ReplicatedCollapsingMergeTree(t *testing.T) {
	engineName := "ReplicatedCollapsingMergeTree"
	engineFull := "ReplicatedCollapsingMergeTree('/clickhouse/tables/{shard}/test', '{replica}', sign) ORDER BY id"

	engine, err := ParseEngine(engineName, engineFull)
	require.NoError(t, err)
	require.NotNil(t, engine)

	rcmt := engine.GetReplicatedCollapsingMergeTree()
	require.NotNil(t, rcmt)
	require.Equal(t, "/clickhouse/tables/{shard}/test", rcmt.ZooPath)
	require.Equal(t, "{replica}", rcmt.ReplicaName)
	require.Equal(t, "sign", rcmt.SignColumn)
}

func TestParseEngine_AggregatingMergeTree(t *testing.T) {
	tests := []struct {
		name       string
		engineName string
		engineFull string
	}{
		{
			name:       "simple AggregatingMergeTree",
			engineName: "AggregatingMergeTree",
			engineFull: "AggregatingMergeTree ORDER BY id",
		},
		{
			name:       "AggregatingMergeTree with parens",
			engineName: "AggregatingMergeTree",
			engineFull: "AggregatingMergeTree() ORDER BY id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, err := ParseEngine(tt.engineName, tt.engineFull)
			require.NoError(t, err)
			require.NotNil(t, engine)
			require.NotNil(t, engine.GetAggregatingMergeTree())
		})
	}
}

func TestParseEngine_ReplicatedAggregatingMergeTree(t *testing.T) {
	engineName := "ReplicatedAggregatingMergeTree"
	engineFull := "ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/test', '{replica}') ORDER BY id"

	engine, err := ParseEngine(engineName, engineFull)
	require.NoError(t, err)
	require.NotNil(t, engine)

	ramt := engine.GetReplicatedAggregatingMergeTree()
	require.NotNil(t, ramt)
	require.Equal(t, "/clickhouse/tables/{shard}/test", ramt.ZooPath)
	require.Equal(t, "{replica}", ramt.ReplicaName)
}

func stringPtr(s string) *string {
	return &s
}
