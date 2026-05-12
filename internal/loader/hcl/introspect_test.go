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
