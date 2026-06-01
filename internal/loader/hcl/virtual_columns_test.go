package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVirtualColumnsFor_NoEngine(t *testing.T) {
	assert.Nil(t, VirtualColumnsFor(nil, nil))
	assert.Nil(t, VirtualColumnsFor(EngineLog{}, nil), "Log engine has no virtuals")
}

func TestVirtualColumnsFor_MergeTreeFamilyStableSet(t *testing.T) {
	for _, e := range []Engine{
		EngineMergeTree{},
		EngineReplicatedMergeTree{},
		EngineReplacingMergeTree{},
		EngineReplicatedReplacingMergeTree{},
		EngineSummingMergeTree{},
		EngineReplicatedSummingMergeTree{},
		EngineCollapsingMergeTree{},
		EngineReplicatedCollapsingMergeTree{},
		EngineAggregatingMergeTree{},
		EngineReplicatedAggregatingMergeTree{},
	} {
		t.Run(e.Kind(), func(t *testing.T) {
			cols := VirtualColumnsFor(e, nil)
			require.Len(t, cols, 7)
			names := columnNames(cols)
			assert.Equal(t, []string{
				"_part", "_part_index", "_part_uuid",
				"_partition_id", "_partition_value",
				"_sample_factor", "_part_offset",
			}, names)
		})
	}
}

func TestVirtualColumnsFor_Kafka_BaseSet(t *testing.T) {
	cols := VirtualColumnsFor(EngineKafka{}, nil)
	names := columnNames(cols)
	assert.Equal(t, []string{
		"_topic", "_key", "_offset", "_partition",
		"_timestamp", "_timestamp_ms",
		"_headers.name", "_headers.value",
	}, names)
	// Type spot-check: _topic should be LowCardinality(String), not bare String.
	for _, c := range cols {
		if c.Name == "_topic" {
			assert.Equal(t, "LowCardinality(String)", c.Type)
		}
	}
}

func TestVirtualColumnsFor_Kafka_StreamModeAddsExtras(t *testing.T) {
	stream := "stream"
	cols := VirtualColumnsFor(EngineKafka{HandleErrorMode: &stream}, nil)
	names := columnNames(cols)
	assert.Contains(t, names, "_raw_message")
	assert.Contains(t, names, "_error")
}

func TestVirtualColumnsFor_Kafka_DefaultModeOmitsStreamExtras(t *testing.T) {
	defaultMode := "default"
	cols := VirtualColumnsFor(EngineKafka{HandleErrorMode: &defaultMode}, nil)
	names := columnNames(cols)
	assert.NotContains(t, names, "_raw_message")
	assert.NotContains(t, names, "_error")
}

func TestIsVirtualColumn_Kafka_RecognisesStreamNamesRegardlessOfMode(t *testing.T) {
	// IsVirtualColumn is mode-independent: _raw_message/_error are
	// virtual on Kafka in every mode for membership-test purposes.
	assert.True(t, IsVirtualColumn(EngineKafka{}, "_raw_message"))
	assert.True(t, IsVirtualColumn(EngineKafka{}, "_error"))
}

func TestIsVirtualColumn_Kafka_RecognisesHeadersNestedParent(t *testing.T) {
	assert.True(t, IsVirtualColumn(EngineKafka{}, "_headers"))
	assert.True(t, IsVirtualColumn(EngineKafka{}, "_headers.name"))
	assert.True(t, IsVirtualColumn(EngineKafka{}, "_headers.value"))
}

func TestIsVirtualColumn_NonKafka_HasNoHeaders(t *testing.T) {
	assert.False(t, IsVirtualColumn(EngineMergeTree{}, "_headers"))
}

func TestIsVirtualColumn_MissAndUnknownEngine(t *testing.T) {
	assert.False(t, IsVirtualColumn(EngineMergeTree{}, "_offset"), "_offset is Kafka-only")
	assert.False(t, IsVirtualColumn(EngineLog{}, "_part"), "Log engine has no virtuals")
	assert.False(t, IsVirtualColumn(nil, "_part"))
}

func TestColumnsProvidedBy_DeclaredOnlyWhenNoEngine(t *testing.T) {
	t1 := TableSpec{Columns: []ColumnSpec{{Name: "id", Type: "UInt64"}}}
	cols := ColumnsProvidedBy(t1, nil)
	assert.Equal(t, []DeclaredColumn{{Name: "id", Type: "UInt64"}}, cols)
}

func TestColumnsProvidedBy_KafkaAppendsVirtuals(t *testing.T) {
	t1 := TableSpec{
		Columns: []ColumnSpec{{Name: "team_id", Type: "UInt32"}},
		Engine:  &EngineSpec{Kind: "kafka", Decoded: EngineKafka{}},
	}
	cols := ColumnsProvidedBy(t1, nil)
	names := columnNames(cols)
	// Declared first, virtuals appended.
	assert.Equal(t, "team_id", names[0])
	assert.Contains(t, names, "_topic")
	assert.Contains(t, names, "_offset")
}

func TestColumnsProvidedBy_DeclaredWinsOverVirtual(t *testing.T) {
	// User declares _key as a real column (e.g. via
	// kafka_map_virtual_columns_on_write). The declared form must win
	// — appear once, with the user's type, and the virtual must not
	// be appended again.
	t1 := TableSpec{
		Columns: []ColumnSpec{{Name: "_key", Type: "FixedString(8)"}},
		Engine:  &EngineSpec{Kind: "kafka", Decoded: EngineKafka{}},
	}
	cols := ColumnsProvidedBy(t1, nil)
	var keyCount int
	for _, c := range cols {
		if c.Name == "_key" {
			keyCount++
			assert.Equal(t, "FixedString(8)", c.Type, "declared type must win")
		}
	}
	assert.Equal(t, 1, keyCount, "_key must appear exactly once")
}

func TestVirtualColumnsFor_DistributedNoResolver_FallbackToShardNum(t *testing.T) {
	cols := VirtualColumnsFor(EngineDistributed{RemoteDatabase: "default", RemoteTable: "t_local"}, nil)
	assert.Equal(t, []DeclaredColumn{{Name: "_shard_num", Type: "UInt32"}}, cols)
}

func TestVirtualColumnsFor_DistributedOverMergeTree_InheritsVirtuals(t *testing.T) {
	dbs := []DatabaseSpec{{
		Name: "default",
		Tables: []TableSpec{{
			Name:   "events_local",
			Engine: &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
		}},
	}}
	r := NewSchemaResolver(dbs)
	cols := VirtualColumnsFor(
		EngineDistributed{RemoteDatabase: "default", RemoteTable: "events_local"},
		r,
	)
	names := columnNames(cols)
	assert.Equal(t, "_shard_num", names[0], "_shard_num always first")
	assert.Contains(t, names, "_part")
	assert.Contains(t, names, "_partition_id")
	assert.NotContains(t, names, "_topic", "should not include Kafka virtuals")
}

func TestVirtualColumnsFor_DistributedOverDistributed_RecursesAndDeduplicates(t *testing.T) {
	dbs := []DatabaseSpec{{
		Name: "default",
		Tables: []TableSpec{
			{
				Name:   "events_inner_local",
				Engine: &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
			},
			{
				Name: "events_inner_dist",
				Engine: &EngineSpec{Kind: "distributed", Decoded: EngineDistributed{
					RemoteDatabase: "default", RemoteTable: "events_inner_local",
				}},
			},
		},
	}}
	r := NewSchemaResolver(dbs)
	cols := VirtualColumnsFor(
		EngineDistributed{RemoteDatabase: "default", RemoteTable: "events_inner_dist"},
		r,
	)
	names := columnNames(cols)
	// _shard_num appears once even though both layers contribute it.
	var shardCount int
	for _, n := range names {
		if n == "_shard_num" {
			shardCount++
		}
	}
	assert.Equal(t, 1, shardCount, "_shard_num must be deduplicated across chained Distributeds")
	assert.Contains(t, names, "_part", "MergeTree virtuals propagated through both layers")
}

func TestVirtualColumnsFor_DistributedCycle_NoInfiniteLoop(t *testing.T) {
	// A pathological Distributed -> Distributed -> Distributed cycle.
	// We must terminate and return at least _shard_num.
	dbs := []DatabaseSpec{{
		Name: "default",
		Tables: []TableSpec{
			{
				Name: "a",
				Engine: &EngineSpec{Kind: "distributed", Decoded: EngineDistributed{
					RemoteDatabase: "default", RemoteTable: "b",
				}},
			},
			{
				Name: "b",
				Engine: &EngineSpec{Kind: "distributed", Decoded: EngineDistributed{
					RemoteDatabase: "default", RemoteTable: "a",
				}},
			},
		},
	}}
	r := NewSchemaResolver(dbs)
	cols := VirtualColumnsFor(
		EngineDistributed{RemoteDatabase: "default", RemoteTable: "a"},
		r,
	)
	require.NotEmpty(t, cols)
	assert.Equal(t, "_shard_num", cols[0].Name)
}

func TestVirtualColumnsFor_DistributedMissingRemote_FallbackToShardNum(t *testing.T) {
	r := NewSchemaResolver(nil)
	cols := VirtualColumnsFor(
		EngineDistributed{RemoteDatabase: "other_db", RemoteTable: "nope"},
		r,
	)
	assert.Equal(t, []DeclaredColumn{{Name: "_shard_num", Type: "UInt32"}}, cols)
}

func TestSchemaResolver_LookupByQualifiedName(t *testing.T) {
	dbs := []DatabaseSpec{{
		Name: "posthog",
		Tables: []TableSpec{
			{Name: "events", Engine: &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}}},
		},
	}}
	r := NewSchemaResolver(dbs)
	got, ok := r.LookupTable("posthog", "events")
	require.True(t, ok)
	assert.Equal(t, "events", got.Name)
	_, ok = r.LookupTable("posthog", "missing")
	assert.False(t, ok)
	_, ok = r.LookupTable("other", "events")
	assert.False(t, ok)
}

func columnNames(cols []DeclaredColumn) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}
