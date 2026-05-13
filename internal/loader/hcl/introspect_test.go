package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseEngineString(t *testing.T) {
	ptr := func(s string) *string { return &s }

	cases := []struct {
		name  string
		input string
		want  Engine
	}{
		{"merge_tree_no_parens", "MergeTree", EngineMergeTree{}},
		{"merge_tree_empty_parens", "MergeTree()", EngineMergeTree{}},
		{"merge_tree_with_order_by", "MergeTree() ORDER BY id", EngineMergeTree{}},
		{
			"replicated_merge_tree",
			"ReplicatedMergeTree('/clickhouse/tables/{shard}/t', '{replica}') ORDER BY id",
			EngineReplicatedMergeTree{
				ZooPath:     "/clickhouse/tables/{shard}/t",
				ReplicaName: "{replica}",
			},
		},
		{
			"replacing_merge_tree_with_version",
			"ReplacingMergeTree(ver) ORDER BY id",
			EngineReplacingMergeTree{VersionColumn: ptr("ver")},
		},
		{"replacing_merge_tree_no_args", "ReplacingMergeTree", EngineReplacingMergeTree{}},
		{
			"replicated_replacing_merge_tree_with_version",
			"ReplicatedReplacingMergeTree('/p', '{replica}', ver) ORDER BY id",
			EngineReplicatedReplacingMergeTree{
				ZooPath:       "/p",
				ReplicaName:   "{replica}",
				VersionColumn: ptr("ver"),
			},
		},
		{
			"summing_merge_tree",
			"SummingMergeTree((a, b)) ORDER BY id",
			EngineSummingMergeTree{SumColumns: []string{"a", "b"}},
		},
		{"summing_merge_tree_empty", "SummingMergeTree", EngineSummingMergeTree{}},
		{
			"collapsing_merge_tree",
			"CollapsingMergeTree(sign) ORDER BY id",
			EngineCollapsingMergeTree{SignColumn: "sign"},
		},
		{
			"replicated_collapsing_merge_tree",
			"ReplicatedCollapsingMergeTree('/p', '{replica}', sign) ORDER BY id",
			EngineReplicatedCollapsingMergeTree{
				ZooPath: "/p", ReplicaName: "{replica}", SignColumn: "sign",
			},
		},
		{"aggregating_merge_tree", "AggregatingMergeTree() ORDER BY id", EngineAggregatingMergeTree{}},
		{
			"replicated_aggregating_merge_tree",
			"ReplicatedAggregatingMergeTree('/p', '{replica}') ORDER BY id",
			EngineReplicatedAggregatingMergeTree{ZooPath: "/p", ReplicaName: "{replica}"},
		},
		{
			"distributed",
			"Distributed('clstr', 'db', 't')",
			EngineDistributed{ClusterName: "clstr", RemoteDatabase: "db", RemoteTable: "t"},
		},
		{
			"distributed_with_sharding_key",
			"Distributed('clstr', 'db', 't', sipHash64(id))",
			EngineDistributed{
				ClusterName: "clstr", RemoteDatabase: "db", RemoteTable: "t",
				ShardingKey: ptr("sipHash64(id)"),
			},
		},
		{"log", "Log", EngineLog{}},
		{
			"kafka_settings_form",
			"Kafka SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'events', kafka_group_name = 'g1', kafka_format = 'JSONEachRow'",
			EngineKafka{
				BrokerList:    []string{"kafka:9092"},
				Topic:         "events",
				ConsumerGroup: "g1",
				Format:        "JSONEachRow",
			},
		},
		{
			"kafka_constructor_form",
			"Kafka('kafka:9092', 'events', 'g1', 'JSONEachRow')",
			EngineKafka{
				BrokerList:    []string{"kafka:9092"},
				Topic:         "events",
				ConsumerGroup: "g1",
				Format:        "JSONEachRow",
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ParseEngineString(c.input)
			require.NoError(t, err)
			assert.Equal(t, c.want, got)
		})
	}
}

func TestParseEngineString_UnknownErrors(t *testing.T) {
	_, err := ParseEngineString("SomethingWeird")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported")
}

func TestSplitKeyList(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"id", []string{"id"}},
		{"id, ts", []string{"id", "ts"}},
		{"(id, ts)", []string{"id", "ts"}},
		{" ( id , ts ) ", []string{"id", "ts"}},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, splitKeyList(c.in), "input=%q", c.in)
	}
}

func TestBuildTableFromCreateSQL_FullTable(t *testing.T) {
	src := `CREATE TABLE db.events
(
    ` + "`id`" + ` UInt64,
    ` + "`ts`" + ` DateTime TTL toDate(ts) + INTERVAL 1 MONTH,
    ` + "`val`" + ` Float32 CODEC(LZ4),
    ` + "`name`" + ` String DEFAULT 'unknown' COMMENT 'display name',
    INDEX idx_ts ts TYPE minmax GRANULARITY 4,
    CONSTRAINT c1 CHECK id > 0
)
ENGINE = MergeTree
ORDER BY id
PARTITION BY toYYYYMM(ts)
TTL ts + INTERVAL 6 MONTH
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1`

	got, err := buildTableFromCreateSQL(src)
	require.NoError(t, err)

	assert.Equal(t, []string{"id"}, got.OrderBy)
	if assert.NotNil(t, got.PartitionBy) {
		assert.Equal(t, "toYYYYMM(ts)", *got.PartitionBy)
	}
	if assert.NotNil(t, got.TTL) {
		assert.Contains(t, *got.TTL, "ts")
	}
	assert.Equal(t, map[string]string{
		"index_granularity":   "8192",
		"ttl_only_drop_parts": "1",
	}, got.Settings)

	// Engine
	require.NotNil(t, got.Engine)
	assert.Equal(t, EngineMergeTree{}, got.Engine.Decoded)

	// Columns
	require.Len(t, got.Columns, 4)
	assert.Equal(t, "id", got.Columns[0].Name)
	assert.Equal(t, "ts", got.Columns[1].Name)
	if assert.NotNil(t, got.Columns[1].TTL) {
		assert.Contains(t, *got.Columns[1].TTL, "ts")
	}
	assert.Equal(t, "val", got.Columns[2].Name)
	if assert.NotNil(t, got.Columns[2].Codec) {
		assert.Equal(t, "LZ4", *got.Columns[2].Codec)
	}
	assert.Equal(t, "name", got.Columns[3].Name)
	if assert.NotNil(t, got.Columns[3].Default) {
		assert.Contains(t, *got.Columns[3].Default, "unknown")
	}
	if assert.NotNil(t, got.Columns[3].Comment) {
		assert.Equal(t, "display name", *got.Columns[3].Comment)
	}

	// Index
	require.Len(t, got.Indexes, 1)
	assert.Equal(t, "idx_ts", got.Indexes[0].Name)
	assert.Equal(t, "ts", got.Indexes[0].Expr)
	assert.Equal(t, "minmax", got.Indexes[0].Type)
	assert.Equal(t, 4, got.Indexes[0].Granularity)

	// Constraint
	require.Len(t, got.Constraints, 1)
	assert.Equal(t, "c1", got.Constraints[0].Name)
	if assert.NotNil(t, got.Constraints[0].Check) {
		assert.Contains(t, *got.Constraints[0].Check, "id")
	}
}

func TestBuildTableFromCreateSQL_ReplicatedMergeTreeArgs(t *testing.T) {
	src := `CREATE TABLE db.t
(
    ` + "`id`" + ` UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t', '{replica}')
ORDER BY id`
	got, err := buildTableFromCreateSQL(src)
	require.NoError(t, err)
	require.NotNil(t, got.Engine)
	assert.Equal(t, EngineReplicatedMergeTree{
		ZooPath:     "/clickhouse/tables/{shard}/t",
		ReplicaName: "{replica}",
	}, got.Engine.Decoded)
}

func TestBuildTableFromCreateSQL_KafkaSettingsForm(t *testing.T) {
	src := `CREATE TABLE db.t
(
    ` + "`id`" + ` UInt64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'events', kafka_group_name = 'g', kafka_format = 'JSONEachRow', stream_flush_interval_ms = 7500`
	got, err := buildTableFromCreateSQL(src)
	require.NoError(t, err)
	require.NotNil(t, got.Engine)
	assert.Equal(t, EngineKafka{
		BrokerList:    []string{"kafka:9092"},
		Topic:         "events",
		ConsumerGroup: "g",
		Format:        "JSONEachRow",
	}, got.Engine.Decoded)
	// Non-kafka settings should end up in t.Settings; kafka_* are folded
	// into the engine and removed from Settings.
	assert.Equal(t, map[string]string{
		"stream_flush_interval_ms": "7500",
	}, got.Settings)
}

func TestParseCodecExpression(t *testing.T) {
	ptr := func(s string) *string { return &s }
	cases := []struct {
		in   string
		want *string
	}{
		{"", nil},
		{"CODEC(LZ4)", ptr("LZ4")},
		{"CODEC(Delta, ZSTD)", ptr("Delta, ZSTD")},
		{"CODEC(LZ4HC(9))", ptr("LZ4HC(9)")},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, parseCodecExpression(c.in), "input=%q", c.in)
	}
}
