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

func TestBuildMaterializedViewFromCreateSQL_ToForm(t *testing.T) {
	src := `CREATE MATERIALIZED VIEW db.app_metrics_mv TO db.sharded_app_metrics ` +
		"(`team_id` Int64, `category` LowCardinality(String)) " +
		`AS SELECT team_id, category FROM db.kafka_app_metrics`

	got, err := buildMaterializedViewFromCreateSQL(src)
	require.NoError(t, err)

	assert.Equal(t, "db.sharded_app_metrics", got.ToTable)
	assert.Equal(t, []ColumnSpec{
		{Name: "team_id", Type: "Int64"},
		{Name: "category", Type: "LowCardinality(String)"},
	}, got.Columns)
	assert.Contains(t, got.Query, "team_id")
	assert.Contains(t, got.Query, "kafka_app_metrics")
	assert.Nil(t, got.Cluster)
}

func TestBuildMaterializedViewFromCreateSQL_InnerEngineUnsupported(t *testing.T) {
	src := `CREATE MATERIALIZED VIEW db.mv ENGINE = MergeTree ORDER BY id ` +
		`AS SELECT id FROM db.src`
	_, err := buildMaterializedViewFromCreateSQL(src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported")
	assert.Contains(t, err.Error(), "inner-engine")
}

func TestBuildMaterializedViewFromCreateSQL_RefreshableUnsupported(t *testing.T) {
	src := `CREATE MATERIALIZED VIEW db.mv REFRESH EVERY 1 HOUR TO db.target ` +
		`AS SELECT id FROM db.src`
	_, err := buildMaterializedViewFromCreateSQL(src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported")
	assert.Contains(t, err.Error(), "refreshable")
}

// fakeRows is a minimal rowScanner backed by a slice of (name, createSQL)
// pairs, used to test processIntrospectRows without a live ClickHouse.
type fakeRows struct {
	rows []struct{ name, sql string }
	pos  int
}

func (r *fakeRows) Next() bool {
	r.pos++
	return r.pos <= len(r.rows)
}

func (r *fakeRows) Scan(dest ...any) error {
	row := r.rows[r.pos-1]
	*dest[0].(*string) = row.name
	*dest[1].(*string) = row.sql
	return nil
}

func (r *fakeRows) Err() error { return nil }

// TestProcessIntrospectRows_Dispatch exercises the statement-type dispatch in
// processIntrospectRows: a CREATE TABLE, a CREATE MATERIALIZED VIEW (TO form),
// and a plain CREATE VIEW must all be processed in one call without error, and
// each must land in the correct collection (or be silently skipped for views).
func TestProcessIntrospectRows_Dispatch(t *testing.T) {
	rows := &fakeRows{rows: []struct{ name, sql string }{
		{
			name: "events",
			sql: `CREATE TABLE db.events (` +
				"`id` UInt64" +
				`) ENGINE = MergeTree ORDER BY id`,
		},
		{
			name: "metrics_mv",
			sql: `CREATE MATERIALIZED VIEW db.metrics_mv TO db.metrics ` +
				"(`team_id` Int64) " +
				`AS SELECT team_id FROM db.events`,
		},
		{
			name: "events_view",
			sql:  `CREATE VIEW db.events_view AS SELECT id FROM db.events`,
		},
	}}

	db := &DatabaseSpec{Name: "db"}
	err := processIntrospectRows(db, "db", rows)
	require.NoError(t, err)

	// One table, one MV; the plain view is silently skipped.
	require.Len(t, db.Tables, 1, "expected exactly one table")
	assert.Equal(t, "events", db.Tables[0].Name)

	require.Len(t, db.MaterializedViews, 1, "expected exactly one materialized view")
	assert.Equal(t, "metrics_mv", db.MaterializedViews[0].Name)
	assert.Equal(t, "db.metrics", db.MaterializedViews[0].ToTable)
	assert.Contains(t, db.MaterializedViews[0].Query, "team_id")
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

func TestBuildDictionaryFromAST_Full(t *testing.T) {
	src := `CREATE DICTIONARY db.exchange_rate_dict (
    ` + "`currency`" + ` String,
    ` + "`start_date`" + ` Date,
    ` + "`end_date`" + ` Nullable(Date),
    ` + "`rate`" + ` Decimal64(10)
) PRIMARY KEY currency
SOURCE(CLICKHOUSE(QUERY 'SELECT currency, start_date, end_date, rate FROM db.exchange_rate' USER 'default' PASSWORD '[HIDDEN]'))
LIFETIME(MIN 3000 MAX 3600)
LAYOUT(COMPLEX_KEY_RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'max'))
RANGE(MIN start_date MAX end_date)
COMMENT 'fx rates by date'`

	got, err := buildDictionaryFromCreateSQL(src)
	require.NoError(t, err)

	assert.Equal(t, []string{"currency"}, got.PrimaryKey)
	assert.Equal(t, []DictionaryAttribute{
		{Name: "currency", Type: "String"},
		{Name: "start_date", Type: "Date"},
		{Name: "end_date", Type: "Nullable(Date)"},
		{Name: "rate", Type: "Decimal64(10)"},
	}, got.Attributes)

	require.NotNil(t, got.Source)
	assert.Equal(t, "clickhouse", got.Source.Kind)
	assert.Equal(t, SourceClickHouse{
		Query:    ptr("SELECT currency, start_date, end_date, rate FROM db.exchange_rate"),
		User:     ptr("default"),
		Password: ptr("[HIDDEN]"),
	}, got.Source.Decoded)

	require.NotNil(t, got.Layout)
	assert.Equal(t, "complex_key_range_hashed", got.Layout.Kind)
	assert.Equal(t, LayoutComplexKeyRangeHashed{RangeLookupStrategy: ptr("max")}, got.Layout.Decoded)

	require.NotNil(t, got.Lifetime)
	assert.Equal(t, &DictionaryLifetime{Min: ptr(int64(3000)), Max: ptr(int64(3600))}, got.Lifetime)

	require.NotNil(t, got.Range)
	assert.Equal(t, &DictionaryRange{Min: "start_date", Max: "end_date"}, got.Range)

	require.NotNil(t, got.Comment)
	assert.Equal(t, "fx rates by date", *got.Comment)
}

func TestBuildDictionaryFromAST_LifetimeSimpleForm(t *testing.T) {
	src := `CREATE DICTIONARY db.d (
    ` + "`k`" + ` UInt64,
    ` + "`v`" + ` String
) PRIMARY KEY k
SOURCE(NULL())
LIFETIME(300)
LAYOUT(FLAT())`
	got, err := buildDictionaryFromCreateSQL(src)
	require.NoError(t, err)
	require.NotNil(t, got.Lifetime)
	assert.Equal(t, &DictionaryLifetime{Min: ptr(int64(300))}, got.Lifetime)
}

func TestBuildDictionaryFromAST_UnsupportedSource(t *testing.T) {
	src := `CREATE DICTIONARY db.d (` + "`k`" + ` UInt64, ` + "`v`" + ` String) PRIMARY KEY k
SOURCE(MONGODB(connection_string 'mongodb://x'))
LAYOUT(HASHED())
LIFETIME(0)`
	_, err := buildDictionaryFromCreateSQL(src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported dictionary source kind")
}

func TestBuildDictionaryFromAST_UnsupportedLayout(t *testing.T) {
	src := `CREATE DICTIONARY db.d (` + "`k`" + ` UInt64, ` + "`v`" + ` String) PRIMARY KEY k
SOURCE(NULL())
LAYOUT(HASHED_ARRAY())
LIFETIME(0)`
	_, err := buildDictionaryFromCreateSQL(src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported dictionary layout kind")
}

func TestProcessIntrospectRows_DispatchesDictionary(t *testing.T) {
	rows := &fakeRows{rows: []struct{ name, sql string }{
		{name: "events", sql: "CREATE TABLE db.events (`id` UUID) ENGINE = MergeTree ORDER BY id"},
		{name: "d", sql: "CREATE DICTIONARY db.d (`k` UInt64, `v` String) PRIMARY KEY k SOURCE(NULL()) LAYOUT(HASHED()) LIFETIME(0)"},
	}}
	db := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRows(db, "db", rows))
	require.Len(t, db.Tables, 1)
	require.Len(t, db.Dictionaries, 1)
	assert.Equal(t, "d", db.Dictionaries[0].Name)
	require.NotNil(t, db.Dictionaries[0].Source)
	assert.Equal(t, "null", db.Dictionaries[0].Source.Kind)
	require.NotNil(t, db.Dictionaries[0].Layout)
	assert.Equal(t, "hashed", db.Dictionaries[0].Layout.Kind)
}
