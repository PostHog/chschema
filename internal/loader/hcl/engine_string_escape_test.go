package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuoteString_EscapesApostrophesAndBackslashes(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "plain", value: "plain", want: "'plain'"},
		{name: "apostrophe", value: "O'Reilly", want: "'O\\'Reilly'"},
		{name: "backslashes", value: "C:\\logs\\events", want: "'C:\\\\logs\\\\events'"},
		{name: "both", value: "C:\\O'Reilly", want: "'C:\\\\O\\'Reilly'"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			quoted := quoteString(tc.value)
			assert.Equal(t, tc.want, quoted)
			assert.Equal(t, tc.value, unquoteString(quoted))
		})
	}

	assert.Equal(t, "'value\\\\with\\'quote'", formatSettingValue("value\\with'quote"))
}

func assertEngineStringRoundTrip(t *testing.T, engine Engine, wantClause string) {
	t.Helper()
	clause, settings := engineSQL(engine)
	assert.Equal(t, wantClause, clause)
	assert.Empty(t, settings)

	sql := "CREATE TABLE db.t (x UInt8, version UInt64, is_deleted UInt8, value UInt64, other UInt64, sign Int8) ENGINE = " + clause
	table, err := buildTableFromCreateSQL(sql)
	require.NoError(t, err, "generated engine SQL did not parse: %s", clause)
	require.NotNil(t, table.Engine)
	assert.Equal(t, engine, table.Engine.Decoded)
}

func TestEngineSQL_EscapesReplicatedAndSharedKeeperStrings(t *testing.T) {
	const keeperPath = "/clickhouse\\path/o'reilly"
	const replicaName = "replica\\zone'1"
	quotedPath := quoteString(keeperPath)
	quotedReplica := quoteString(replicaName)

	tests := []struct {
		name   string
		engine Engine
		want   string
	}{
		{name: "replicated merge", engine: EngineReplicatedMergeTree{ZooPath: keeperPath, ReplicaName: replicaName}, want: "ReplicatedMergeTree(" + quotedPath + ", " + quotedReplica + ")"},
		{name: "shared merge", engine: EngineSharedMergeTree{ZooPath: keeperPath, ReplicaName: replicaName}, want: "SharedMergeTree(" + quotedPath + ", " + quotedReplica + ")"},
		{name: "replicated replacing", engine: EngineReplicatedReplacingMergeTree{ZooPath: keeperPath, ReplicaName: replicaName, VersionColumn: ptr("version"), IsDeletedColumn: ptr("is_deleted")}, want: "ReplicatedReplacingMergeTree(" + quotedPath + ", " + quotedReplica + ", version, is_deleted)"},
		{name: "shared replacing", engine: EngineSharedReplacingMergeTree{ZooPath: keeperPath, ReplicaName: replicaName, VersionColumn: ptr("version"), IsDeletedColumn: ptr("is_deleted")}, want: "SharedReplacingMergeTree(" + quotedPath + ", " + quotedReplica + ", version, is_deleted)"},
		{name: "replicated summing", engine: EngineReplicatedSummingMergeTree{ZooPath: keeperPath, ReplicaName: replicaName, SumColumns: []string{"value", "other"}}, want: "ReplicatedSummingMergeTree(" + quotedPath + ", " + quotedReplica + ", (value, other))"},
		{name: "shared summing", engine: EngineSharedSummingMergeTree{ZooPath: keeperPath, ReplicaName: replicaName, SumColumns: []string{"value", "other"}}, want: "SharedSummingMergeTree(" + quotedPath + ", " + quotedReplica + ", (value, other))"},
		{name: "replicated collapsing", engine: EngineReplicatedCollapsingMergeTree{ZooPath: keeperPath, ReplicaName: replicaName, SignColumn: "sign"}, want: "ReplicatedCollapsingMergeTree(" + quotedPath + ", " + quotedReplica + ", sign)"},
		{name: "shared collapsing", engine: EngineSharedCollapsingMergeTree{ZooPath: keeperPath, ReplicaName: replicaName, SignColumn: "sign"}, want: "SharedCollapsingMergeTree(" + quotedPath + ", " + quotedReplica + ", sign)"},
		{name: "replicated aggregating", engine: EngineReplicatedAggregatingMergeTree{ZooPath: keeperPath, ReplicaName: replicaName}, want: "ReplicatedAggregatingMergeTree(" + quotedPath + ", " + quotedReplica + ")"},
		{name: "shared aggregating", engine: EngineSharedAggregatingMergeTree{ZooPath: keeperPath, ReplicaName: replicaName}, want: "SharedAggregatingMergeTree(" + quotedPath + ", " + quotedReplica + ")"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertEngineStringRoundTrip(t, tc.engine, tc.want)
		})
	}
}

func TestEngineSQL_EscapesOtherStringArgumentFamilies(t *testing.T) {
	t.Run("distributed leaves sharding expression bare", func(t *testing.T) {
		engine := EngineDistributed{
			ClusterName:    "cluster\\one'two",
			RemoteDatabase: "database\\one'two",
			RemoteTable:    "table\\one'two",
			ShardingKey:    ptr("sipHash64(x)"),
			PolicyName:     ptr("policy\\one'two"),
		}
		want := "Distributed(" +
			quoteString(engine.ClusterName) + ", " +
			quoteString(engine.RemoteDatabase) + ", " +
			quoteString(engine.RemoteTable) + ", sipHash64(x), " +
			quoteString(*engine.PolicyName) + ")"
		assertEngineStringRoundTrip(t, engine, want)
	})

	t.Run("merge regexes", func(t *testing.T) {
		engine := EngineMerge{DBRegex: "audit\\db'one", TableRegex: "^foo\\bar'baz$"}
		want := "Merge(" + quoteString(engine.DBRegex) + ", " + quoteString(engine.TableRegex) + ")"
		assertEngineStringRoundTrip(t, engine, want)
	})

	t.Run("buffer database and table", func(t *testing.T) {
		engine := EngineBuffer{
			Database: "audit\\db'one", Table: "buffer\\table'one", NumLayers: 2,
			MinTime: 1, MaxTime: 10, MinRows: 100, MaxRows: 1000, MinBytes: 1024, MaxBytes: 2048,
		}
		want := "Buffer(" + quoteString(engine.Database) + ", " + quoteString(engine.Table) +
			", 2, 1, 10, 100, 1000, 1024, 2048)"
		assertEngineStringRoundTrip(t, engine, want)
	})
}
