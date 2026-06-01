package hcl

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseFile_AllEngineKinds(t *testing.T) {
	schema, err := ParseFile(filepath.Join("testdata", "engines_all_kinds.hcl"))
	require.NoError(t, err)
	require.Len(t, schema.Databases, 1)
	tables := schema.Databases[0].Tables

	byName := map[string]Engine{}
	for _, tbl := range tables {
		require.NotNil(t, tbl.Engine, "table %s missing engine", tbl.Name)
		require.NotNil(t, tbl.Engine.Decoded, "table %s engine not decoded", tbl.Name)
		byName[tbl.Name] = tbl.Engine.Decoded
	}

	assert.Equal(t, EngineMergeTree{}, byName["t_merge_tree"])

	assert.Equal(t, EngineReplicatedMergeTree{
		ZooPath:     "/clickhouse/tables/{shard}/t_replicated_merge_tree",
		ReplicaName: "{replica}",
	}, byName["t_replicated_merge_tree"])

	assert.Equal(t, EngineReplacingMergeTree{
		VersionColumn: ptr("ver"),
	}, byName["t_replacing_merge_tree"])

	assert.Equal(t, EngineReplicatedReplacingMergeTree{
		ZooPath:       "/clickhouse/tables/{shard}/t_rrmt",
		ReplicaName:   "{replica}",
		VersionColumn: ptr("ver"),
	}, byName["t_replicated_replacing_merge_tree"])

	assert.Equal(t, EngineSummingMergeTree{
		SumColumns: []string{"a", "b"},
	}, byName["t_summing_merge_tree"])

	assert.Equal(t, EngineReplicatedSummingMergeTree{
		ZooPath:     "/clickhouse/tables/{shard}/t_rsmt",
		ReplicaName: "{replica}",
		SumColumns:  []string{"a", "b"},
	}, byName["t_replicated_summing_merge_tree"])

	assert.Equal(t, EngineCollapsingMergeTree{
		SignColumn: "sign",
	}, byName["t_collapsing_merge_tree"])

	assert.Equal(t, EngineReplicatedCollapsingMergeTree{
		ZooPath:     "/clickhouse/tables/{shard}/t_rcmt",
		ReplicaName: "{replica}",
		SignColumn:  "sign",
	}, byName["t_replicated_collapsing_merge_tree"])

	assert.Equal(t, EngineAggregatingMergeTree{}, byName["t_aggregating_merge_tree"])

	assert.Equal(t, EngineReplicatedAggregatingMergeTree{
		ZooPath:     "/clickhouse/tables/{shard}/t_ramt",
		ReplicaName: "{replica}",
	}, byName["t_replicated_aggregating_merge_tree"])

	assert.Equal(t, EngineDistributed{
		ClusterName:    "posthog",
		RemoteDatabase: "default",
		RemoteTable:    "t_merge_tree",
		ShardingKey:    ptr("sipHash64(id)"),
	}, byName["t_distributed"])

	assert.Equal(t, EngineLog{}, byName["t_log"])

	assert.Equal(t, EngineNull{}, byName["t_null"])
	assert.Equal(t, EngineMemory{}, byName["t_memory"])
	assert.Equal(t, EngineMerge{
		DBRegex:    "default",
		TableRegex: "^shard_.*",
	}, byName["t_merge"])

	flushTime := int64(30)
	assert.Equal(t, EngineBuffer{
		Database:  "",
		Table:     "t_merge_tree",
		NumLayers: 16,
		MinTime:   10,
		MaxTime:   100,
		MinRows:   10000,
		MaxRows:   1000000,
		MinBytes:  10000000,
		MaxBytes:  100000000,
		FlushTime: &flushTime,
	}, byName["t_buffer"])

	assert.Equal(t, EngineKafka{
		BrokerList: ptr("kafka:9092"),
		TopicList:  ptr("events"),
		GroupName:  ptr("ingest"),
		Format:     ptr("JSONEachRow"),
	}, byName["t_kafka"])

	assert.Equal(t, EngineJoin{
		Strictness: "ANY",
		JoinType:   "LEFT",
		Keys:       []string{"user_id", "session_id"},
	}, byName["t_join"])

	tgtData := "default.t_time_series_data"
	tgtTags := "default.t_time_series_tags"
	tgtMetrics := "default.t_time_series_metrics"
	assert.Equal(t, EngineTimeSeries{
		Settings:      map[string]string{"id_generator": "sipHash64(metric_name, all_tags)"},
		TagsToColumns: map[string]string{"instance": "instance", "job": "job"},
		Samples:       &TimeSeriesTarget{Target: &tgtData},
		Tags:          &TimeSeriesTarget{Target: &tgtTags},
		Metrics:       &TimeSeriesTarget{Target: &tgtMetrics},
	}, byName["t_time_series"])
}

func TestParseFile_UnknownEngineKind(t *testing.T) {
	_, err := ParseFile(filepath.Join("testdata", "engine_invalid_kind.hcl"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not_a_real_kind")
}

func TestParseFile_EngineMissingRequired(t *testing.T) {
	_, err := ParseFile(filepath.Join("testdata", "engine_missing_required.hcl"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replica_name")
}

func TestParseFile_TimeSeries_InnerForm(t *testing.T) {
	schema, err := ParseFile(filepath.Join("testdata", "engine_time_series_inner.hcl"))
	require.NoError(t, err)
	require.Len(t, schema.Databases, 1)
	require.Len(t, schema.Databases[0].Tables, 1)

	tbl := schema.Databases[0].Tables[0]
	require.NotNil(t, tbl.Engine)
	require.NotNil(t, tbl.Engine.Decoded)
	e := tbl.Engine.Decoded.(EngineTimeSeries)

	require.NotNil(t, e.Samples)
	require.Nil(t, e.Samples.Target)
	require.NotNil(t, e.Samples.Inner)
	assert.Equal(t, []string{"id", "timestamp"}, e.Samples.Inner.OrderBy)
	require.Len(t, e.Samples.Inner.Columns, 3)

	require.NotNil(t, e.Samples.Inner.Engine)
	_, ok := e.Samples.Inner.Engine.Decoded.(EngineMergeTree)
	assert.True(t, ok, "inner engine should be decoded recursively (got %T)", e.Samples.Inner.Engine.Decoded)
}

func TestEngine_KindMethods(t *testing.T) {
	cases := []struct {
		engine Engine
		want   string
	}{
		{EngineMergeTree{}, "merge_tree"},
		{EngineReplicatedMergeTree{}, "replicated_merge_tree"},
		{EngineReplacingMergeTree{}, "replacing_merge_tree"},
		{EngineReplicatedReplacingMergeTree{}, "replicated_replacing_merge_tree"},
		{EngineSummingMergeTree{}, "summing_merge_tree"},
		{EngineReplicatedSummingMergeTree{}, "replicated_summing_merge_tree"},
		{EngineCollapsingMergeTree{}, "collapsing_merge_tree"},
		{EngineReplicatedCollapsingMergeTree{}, "replicated_collapsing_merge_tree"},
		{EngineAggregatingMergeTree{}, "aggregating_merge_tree"},
		{EngineReplicatedAggregatingMergeTree{}, "replicated_aggregating_merge_tree"},
		{EngineDistributed{}, "distributed"},
		{EngineLog{}, "log"},
		{EngineKafka{}, "kafka"},
		{EngineTimeSeries{}, "time_series"},
		{EngineJoin{}, "join"},
		{EngineNull{}, "null"},
		{EngineMemory{}, "memory"},
		{EngineMerge{}, "merge"},
		{EngineBuffer{}, "buffer"},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, c.engine.Kind())
	}
}
