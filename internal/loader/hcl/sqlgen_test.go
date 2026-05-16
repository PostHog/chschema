package hcl

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLGen_EmptyChangeSet(t *testing.T) {
	out := GenerateSQL(ChangeSet{})
	assert.Empty(t, out.Statements)
	assert.Empty(t, out.Unsafe)
}

func TestSQLGen_CreateMergeTree(t *testing.T) {
	tbl := mkTable("events", EngineMergeTree{},
		ColumnSpec{Name: "id", Type: "UUID"},
		ColumnSpec{Name: "ts", Type: "DateTime"},
	)
	tbl.OrderBy = []string{"ts", "id"}

	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AddTables: []TableSpec{tbl}},
	}})

	expected := `CREATE TABLE posthog.events (
  id UUID,
  ts DateTime
) ENGINE = MergeTree() ORDER BY (ts, id)`
	assert.Equal(t, []string{expected}, out.Statements)
}

func TestSQLGen_CreateReplicatedMergeTreeWithAllOptions(t *testing.T) {
	pt := func(s string) *string { return &s }
	tbl := mkTable("events",
		EngineReplicatedMergeTree{ZooPath: "/clickhouse/tables/{shard}/events", ReplicaName: "{replica}"},
		ColumnSpec{Name: "id", Type: "UUID"},
		ColumnSpec{Name: "ts", Type: "DateTime"},
	)
	tbl.OrderBy = []string{"ts", "id"}
	tbl.PartitionBy = pt("toYYYYMM(ts)")
	tbl.SampleBy = pt("id")
	tbl.TTL = pt("ts + INTERVAL 1 YEAR")
	tbl.Settings = map[string]string{"index_granularity": "8192", "ttl_only_drop_parts": "1"}
	tbl.Indexes = []IndexSpec{{Name: "idx_ts", Expr: "ts", Type: "minmax", Granularity: 4}}

	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AddTables: []TableSpec{tbl}},
	}})

	expected := `CREATE TABLE posthog.events (
  id UUID,
  ts DateTime,
  INDEX idx_ts ts TYPE minmax GRANULARITY 4
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}') ORDER BY (ts, id) PARTITION BY toYYYYMM(ts) SAMPLE BY id TTL ts + INTERVAL 1 YEAR SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1`
	assert.Equal(t, []string{expected}, out.Statements)
}

func TestSQLGen_CreateDistributedWithShardingKey(t *testing.T) {
	pt := func(s string) *string { return &s }
	tbl := mkTable("events_dist", EngineDistributed{
		ClusterName:    "posthog",
		RemoteDatabase: "default",
		RemoteTable:    "events",
		ShardingKey:    pt("sipHash64(id)"),
	}, ColumnSpec{Name: "id", Type: "UUID"})

	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AddTables: []TableSpec{tbl}},
	}})

	expected := `CREATE TABLE posthog.events_dist (
  id UUID
) ENGINE = Distributed('posthog', 'default', 'events', sipHash64(id))`
	assert.Equal(t, []string{expected}, out.Statements)
}

func TestSQLGen_CreateKafkaFoldsSettings(t *testing.T) {
	tbl := mkTable("ingest", EngineKafka{
		BrokerList:    []string{"kafka:9092"},
		Topic:         "events",
		ConsumerGroup: "group1",
		Format:        "JSONEachRow",
	}, ColumnSpec{Name: "id", Type: "UUID"})

	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AddTables: []TableSpec{tbl}},
	}})

	expected := `CREATE TABLE posthog.ingest (
  id UUID
) ENGINE = Kafka() SETTINGS kafka_broker_list = 'kafka:9092', kafka_format = 'JSONEachRow', kafka_group_name = 'group1', kafka_topic_list = 'events'`
	assert.Equal(t, []string{expected}, out.Statements)
}

func TestSQLGen_DropTable(t *testing.T) {
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", DropTables: []TableSpec{{Name: "old_events"}}},
	}})
	assert.Equal(t, []string{"DROP TABLE posthog.old_events"}, out.Statements)
}

func TestSQLGen_AlterTableMultipleOps(t *testing.T) {
	td := TableDiff{
		Table:         "events",
		AddColumns:    []ColumnSpec{{Name: "new_col", Type: "UInt64"}},
		DropColumns:   []string{"old_col"},
		ModifyColumns: []ColumnChange{{Name: "count", OldType: "UInt32", NewType: "UInt64"}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AlterTables: []TableDiff{td}},
	}})

	expected := "ALTER TABLE posthog.events ADD COLUMN new_col UInt64, DROP COLUMN old_col, MODIFY COLUMN count UInt64"
	assert.Equal(t, []string{expected}, out.Statements)
}

func TestSQLGen_AlterSettingsAddRemoveChange(t *testing.T) {
	td := TableDiff{
		Table:           "events",
		SettingsAdded:   map[string]string{"new_k": "v"},
		SettingsRemoved: []string{"gone"},
		SettingsChanged: []SettingChange{{Key: "size", OldValue: "100", NewValue: "200"}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AlterTables: []TableDiff{td}},
	}})

	expected := "ALTER TABLE posthog.events MODIFY SETTING new_k = 'v', MODIFY SETTING size = 200, RESET SETTING gone"
	assert.Equal(t, []string{expected}, out.Statements)
}

func TestSQLGen_AlterTTLChange(t *testing.T) {
	pt := func(s string) *string { return &s }
	td := TableDiff{Table: "events", TTLChange: &StringChange{Old: nil, New: pt("ts + INTERVAL 1 YEAR")}}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AlterTables: []TableDiff{td}},
	}})
	assert.Equal(t, []string{"ALTER TABLE posthog.events MODIFY TTL ts + INTERVAL 1 YEAR"}, out.Statements)

	td = TableDiff{Table: "events", TTLChange: &StringChange{Old: pt("x"), New: nil}}
	out = GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AlterTables: []TableDiff{td}},
	}})
	assert.Equal(t, []string{"ALTER TABLE posthog.events REMOVE TTL"}, out.Statements)
}

func TestSQLGen_AlterIndexes(t *testing.T) {
	td := TableDiff{
		Table:       "events",
		AddIndexes:  []IndexSpec{{Name: "idx_ts", Expr: "ts", Type: "minmax", Granularity: 4}},
		DropIndexes: []string{"old_idx"},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AlterTables: []TableDiff{td}},
	}})
	expected := "ALTER TABLE posthog.events DROP INDEX old_idx, ADD INDEX idx_ts ts TYPE minmax GRANULARITY 4"
	assert.Equal(t, []string{expected}, out.Statements)
}

func TestSQLGen_UnsafeChangesReported(t *testing.T) {
	td := TableDiff{
		Table:         "events",
		EngineChange:  &EngineChange{Old: EngineMergeTree{}, New: EngineLog{}},
		OrderByChange: &OrderByChange{Old: []string{"a"}, New: []string{"b"}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AlterTables: []TableDiff{td}},
	}})

	assert.Empty(t, out.Statements)
	assert.Len(t, out.Unsafe, 2)
	assert.Equal(t, "posthog", out.Unsafe[0].Database)
	assert.Equal(t, "events", out.Unsafe[0].Table)
	assert.Contains(t, out.Unsafe[0].Reason, "engine change")
	assert.Contains(t, out.Unsafe[1].Reason, "ORDER BY")
}

func TestSQLGen_StatementOrderingCreateAlterDrop(t *testing.T) {
	createTbl := mkTable("new_table", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"})
	td := TableDiff{Table: "alter_me", AddColumns: []ColumnSpec{{Name: "x", Type: "UInt64"}}}

	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{
			Database:    "posthog",
			AddTables:   []TableSpec{createTbl},
			AlterTables: []TableDiff{td},
			DropTables:  []TableSpec{{Name: "drop_me"}},
		},
	}})

	require := assert.New(t)
	require.Len(out.Statements, 3)
	assert.Contains(t, out.Statements[0], "CREATE TABLE")
	assert.Contains(t, out.Statements[1], "ALTER TABLE")
	assert.Contains(t, out.Statements[2], "DROP TABLE")
}

func TestSQLGen_CreateMaterializedView(t *testing.T) {
	mv := MaterializedViewSpec{
		Name:    "metrics_mv",
		ToTable: "default.metrics",
		Query:   "SELECT team_id, count() FROM default.events GROUP BY team_id",
		Columns: []ColumnSpec{
			{Name: "team_id", Type: "Int64"},
			{Name: "cnt", Type: "UInt64"},
		},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AddMaterializedViews: []MaterializedViewSpec{mv}},
	}})
	expected := "CREATE MATERIALIZED VIEW posthog.metrics_mv TO default.metrics " +
		"(team_id Int64, cnt UInt64) AS SELECT team_id, count() FROM default.events GROUP BY team_id"
	assert.Equal(t, []string{expected}, out.Statements)
	assert.Empty(t, out.Unsafe)
}

func TestSQLGen_CreateMaterializedViewNoColumns(t *testing.T) {
	mv := MaterializedViewSpec{
		Name:    "metrics_mv",
		ToTable: "default.metrics",
		Query:   "SELECT id FROM default.src",
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AddMaterializedViews: []MaterializedViewSpec{mv}},
	}})
	assert.Equal(t, []string{
		"CREATE MATERIALIZED VIEW posthog.metrics_mv TO default.metrics AS SELECT id FROM default.src",
	}, out.Statements)
}

func TestSQLGen_CreateMaterializedViewWithClusterAndComment(t *testing.T) {
	pt := func(s string) *string { return &s }
	mv := MaterializedViewSpec{
		Name:    "metrics_mv",
		ToTable: "default.metrics",
		Query:   "SELECT id FROM default.src",
		Cluster: pt("posthog"),
		Comment: pt("rolls metrics up"),
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AddMaterializedViews: []MaterializedViewSpec{mv}},
	}})
	expected := "CREATE MATERIALIZED VIEW posthog.metrics_mv ON CLUSTER posthog " +
		"TO default.metrics AS SELECT id FROM default.src COMMENT 'rolls metrics up'"
	assert.Equal(t, []string{expected}, out.Statements)
}

func TestSQLGen_ModifyQuery(t *testing.T) {
	mvd := MaterializedViewDiff{
		Name:        "metrics_mv",
		QueryChange: &StringChange{Old: ptr("SELECT id FROM default.src"), New: ptr("SELECT id, ts FROM default.src")},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AlterMaterializedViews: []MaterializedViewDiff{mvd}},
	}})
	assert.Equal(t, []string{
		"ALTER TABLE posthog.metrics_mv MODIFY QUERY SELECT id, ts FROM default.src",
	}, out.Statements)
	assert.Empty(t, out.Unsafe)
}

func TestSQLGen_DropView(t *testing.T) {
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", DropMaterializedViews: []string{"metrics_mv"}},
	}})
	assert.Equal(t, []string{"DROP VIEW posthog.metrics_mv"}, out.Statements)
}

func TestSQLGen_MaterializedViewRecreateUnsafe(t *testing.T) {
	mvd := MaterializedViewDiff{Name: "metrics_mv", Recreate: true}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AlterMaterializedViews: []MaterializedViewDiff{mvd}},
	}})
	assert.Empty(t, out.Statements)
	require := assert.New(t)
	require.Len(out.Unsafe, 1)
	assert.Equal(t, "posthog", out.Unsafe[0].Database)
	assert.Equal(t, "metrics_mv", out.Unsafe[0].Table)
	assert.Contains(t, out.Unsafe[0].Reason, "recreating")
}

func TestSQLGen_StatementOrderingWithMaterializedViews(t *testing.T) {
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{
			Database:  "posthog",
			AddTables: []TableSpec{mkTable("new_table", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"})},
			AddMaterializedViews: []MaterializedViewSpec{
				{Name: "new_mv", ToTable: "posthog.new_table", Query: "SELECT id FROM posthog.src"},
			},
			AlterMaterializedViews: []MaterializedViewDiff{
				{Name: "alter_mv", QueryChange: &StringChange{New: ptr("SELECT 1")}},
			},
			DropMaterializedViews: []string{"drop_mv"},
			DropTables:            []TableSpec{{Name: "drop_table"}},
		},
	}})
	require := assert.New(t)
	require.Len(out.Statements, 5)
	assert.Contains(t, out.Statements[0], "CREATE TABLE")
	assert.Contains(t, out.Statements[1], "CREATE MATERIALIZED VIEW")
	assert.Contains(t, out.Statements[2], "MODIFY QUERY")
	assert.Contains(t, out.Statements[3], "DROP VIEW")
	assert.Contains(t, out.Statements[4], "DROP TABLE")
}

func TestSQLGen_EndToEndDiffToSQL(t *testing.T) {
	// from: one table with one column.
	from := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"}))}
	// to: add a column.
	to := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{},
		ColumnSpec{Name: "id", Type: "UUID"},
		ColumnSpec{Name: "ts", Type: "DateTime"},
	))}

	cs := Diff(from, to)
	out := GenerateSQL(cs)

	assert.Equal(t, []string{"ALTER TABLE posthog.events ADD COLUMN ts DateTime"}, out.Statements)
}

// stmtIndex returns the index of the first statement containing substr, or -1.
func stmtIndex(stmts []string, substr string) int {
	for i, s := range stmts {
		if strings.Contains(s, substr) {
			return i
		}
	}
	return -1
}

func TestSQLGen_CreateOrdersDistributedAfterRemote(t *testing.T) {
	// The Distributed table is listed first; GenerateSQL must still emit the
	// local table it forwards to before it.
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AddTables: []TableSpec{
			mkDistTable("events_dist", "posthog", "events_local"),
			mkTable("events_local", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"}),
		},
	}}})

	local := stmtIndex(out.Statements, "CREATE TABLE posthog.events_local")
	dist := stmtIndex(out.Statements, "CREATE TABLE posthog.events_dist")
	require.NotEqual(t, -1, local)
	require.NotEqual(t, -1, dist)
	assert.Less(t, local, dist, "remote table must be created before the Distributed table")
}

func TestSQLGen_DropOrdersDistributedBeforeRemote(t *testing.T) {
	// The local table is listed first; GenerateSQL must still drop the
	// Distributed table that forwards to it before dropping the local table.
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		DropTables: []TableSpec{
			mkTable("events_local", EngineMergeTree{}),
			mkDistTable("events_dist", "posthog", "events_local"),
		},
	}}})

	dist := stmtIndex(out.Statements, "DROP TABLE posthog.events_dist")
	local := stmtIndex(out.Statements, "DROP TABLE posthog.events_local")
	require.NotEqual(t, -1, dist)
	require.NotEqual(t, -1, local)
	assert.Less(t, dist, local, "Distributed table must be dropped before its remote table")
}

func TestSQLGen_OrderingAcrossDatabases(t *testing.T) {
	// A Distributed table can forward to a table in another database; the
	// ordering must hold across the whole ChangeSet, not just per-database.
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{
			Database:  "edge",
			AddTables: []TableSpec{mkDistTable("events_dist", "core", "events_local")},
		},
		{
			Database:  "core",
			AddTables: []TableSpec{mkTable("events_local", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"})},
		},
	}})

	local := stmtIndex(out.Statements, "CREATE TABLE core.events_local")
	dist := stmtIndex(out.Statements, "CREATE TABLE edge.events_dist")
	assert.Less(t, local, dist)
}

func TestSQLGen_CreateOrReplaceDictionary(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "default",
		AddDictionaries: []DictionarySpec{{
			Name:       "exchange_rate_dict",
			PrimaryKey: []string{"currency"},
			Attributes: []DictionaryAttribute{
				{Name: "currency", Type: "String"},
				{Name: "start_date", Type: "Date"},
				{Name: "end_date", Type: "Nullable(Date)"},
				{Name: "rate", Type: "Decimal64(10)"},
			},
			Source: &DictionarySourceSpec{
				Kind: "clickhouse",
				Decoded: SourceClickHouse{
					Query:    ptr("SELECT ... FROM default.exchange_rate"),
					User:     ptr("default"),
					Password: ptr("[HIDDEN]"),
				},
			},
			Layout: &DictionaryLayoutSpec{
				Kind:    "complex_key_range_hashed",
				Decoded: LayoutComplexKeyRangeHashed{RangeLookupStrategy: ptr("max")},
			},
			Lifetime: &DictionaryLifetime{Min: ptr(int64(3000)), Max: ptr(int64(3600))},
			Range:    &DictionaryRange{Min: "start_date", Max: "end_date"},
		}},
	}}}

	out := GenerateSQL(cs)
	require.Len(t, out.Statements, 1)
	want := "CREATE OR REPLACE DICTIONARY default.exchange_rate_dict (" +
		"`currency` String, `start_date` Date, `end_date` Nullable(Date), `rate` Decimal64(10)" +
		") PRIMARY KEY currency " +
		"SOURCE(CLICKHOUSE(USER 'default' PASSWORD '[HIDDEN]' QUERY 'SELECT ... FROM default.exchange_rate')) " +
		"LAYOUT(COMPLEX_KEY_RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'max')) " +
		"LIFETIME(MIN 3000 MAX 3600) " +
		"RANGE(MIN start_date MAX end_date)"
	assert.Equal(t, want, out.Statements[0])
	assert.Empty(t, out.Unsafe)
}

func TestSQLGen_CreateOrReplaceDictionary_Simple(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "db",
		AddDictionaries: []DictionarySpec{{
			Name:       "d",
			PrimaryKey: []string{"k"},
			Attributes: []DictionaryAttribute{{Name: "k", Type: "UInt64"}, {Name: "v", Type: "String"}},
			Source:     &DictionarySourceSpec{Kind: "null", Decoded: SourceNull{}},
			Layout:     &DictionaryLayoutSpec{Kind: "hashed", Decoded: LayoutHashed{}},
		}},
	}}}
	out := GenerateSQL(cs)
	require.Len(t, out.Statements, 1)
	assert.Equal(t,
		"CREATE OR REPLACE DICTIONARY db.d (`k` UInt64, `v` String) PRIMARY KEY k SOURCE(NULL()) LAYOUT(HASHED())",
		out.Statements[0])
}

func TestSQLGen_AlterDictionary_EmitsUnsafe(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database:          "db",
		AlterDictionaries: []DictionaryDiff{{Name: "d", Changed: []string{"query"}}},
	}}}
	out := GenerateSQL(cs)
	assert.Empty(t, out.Statements)
	require.Len(t, out.Unsafe, 1)
	assert.Equal(t, "db", out.Unsafe[0].Database)
	assert.Equal(t, "d", out.Unsafe[0].Table)
	assert.Contains(t, out.Unsafe[0].Reason, "CREATE OR REPLACE")
}

func TestSQLGen_DropDictionary(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database:         "db",
		DropDictionaries: []string{"d"},
	}}}
	out := GenerateSQL(cs)
	require.Len(t, out.Statements, 1)
	assert.Equal(t, "DROP DICTIONARY db.d", out.Statements[0])
}

func TestSQLGen_Dictionary_LayoutSQL_AllKinds(t *testing.T) {
	cases := []struct {
		name string
		in   DictionaryLayout
		want string
	}{
		{"flat", LayoutFlat{}, "FLAT()"},
		{"hashed", LayoutHashed{}, "HASHED()"},
		{"sparse_hashed", LayoutSparseHashed{}, "SPARSE_HASHED()"},
		{"complex_key_hashed (no preallocate)", LayoutComplexKeyHashed{}, "COMPLEX_KEY_HASHED()"},
		{"complex_key_hashed (preallocate)", LayoutComplexKeyHashed{Preallocate: ptr(int64(1))}, "COMPLEX_KEY_HASHED(PREALLOCATE 1)"},
		{"complex_key_sparse_hashed", LayoutComplexKeySparseHashed{}, "COMPLEX_KEY_SPARSE_HASHED()"},
		{"range_hashed", LayoutRangeHashed{}, "RANGE_HASHED()"},
		{"range_hashed (lookup strategy)", LayoutRangeHashed{RangeLookupStrategy: ptr("max")}, "RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'max')"},
		{"complex_key_range_hashed (lookup strategy)", LayoutComplexKeyRangeHashed{RangeLookupStrategy: ptr("min")}, "COMPLEX_KEY_RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'min')"},
		{"cache", LayoutCache{SizeInCells: 1000}, "CACHE(SIZE_IN_CELLS 1000)"},
		{"ip_trie", LayoutIPTrie{}, "IP_TRIE()"},
		{"ip_trie (access_to_key_from_attributes)", LayoutIPTrie{AccessToKeyFromAttributes: ptr(true)}, "IP_TRIE(ACCESS_TO_KEY_FROM_ATTRIBUTES true)"},
		{"direct", LayoutDirect{}, "DIRECT()"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, layoutSQL(tc.in))
		})
	}
}

func TestSQLGen_Dictionary_SourceSQL_AllKinds(t *testing.T) {
	cases := []struct {
		name string
		in   DictionarySource
		want string
	}{
		{
			"null", SourceNull{}, "NULL()",
		},
		{
			"clickhouse (full)", SourceClickHouse{
				Host: ptr("h"), Port: ptr(int64(9000)), User: ptr("u"), Password: ptr("p"),
				DB: ptr("d"), Table: ptr("t"), Query: ptr("SELECT 1"),
				InvalidateQuery: ptr("SELECT max(ts)"),
				UpdateField:     ptr("ts"), UpdateLag: ptr(int64(5)),
			},
			"CLICKHOUSE(HOST 'h' PORT 9000 USER 'u' PASSWORD 'p' DB 'd' TABLE 't' QUERY 'SELECT 1' INVALIDATE_QUERY 'SELECT max(ts)' UPDATE_FIELD 'ts' UPDATE_LAG 5)",
		},
		{
			"clickhouse (sparse)", SourceClickHouse{Query: ptr("SELECT 1")},
			"CLICKHOUSE(QUERY 'SELECT 1')",
		},
		{
			"mysql", SourceMySQL{Host: ptr("h"), Port: ptr(int64(3306)), DB: ptr("d"), Table: ptr("t")},
			"MYSQL(HOST 'h' PORT 3306 DB 'd' TABLE 't')",
		},
		{
			"postgresql", SourcePostgreSQL{Host: ptr("h"), Port: ptr(int64(5432)), DB: ptr("d"), Table: ptr("t")},
			"POSTGRESQL(HOST 'h' PORT 5432 DB 'd' TABLE 't')",
		},
		{
			"http", SourceHTTP{URL: "https://x/y", Format: "JSONEachRow", CredentialsUser: ptr("u"), CredentialsPassword: ptr("p")},
			"HTTP(URL 'https://x/y' FORMAT 'JSONEachRow' CREDENTIALS_USER 'u' CREDENTIALS_PASSWORD 'p')",
		},
		{
			"file", SourceFile{Path: "/data/x.csv", Format: "CSV"},
			"FILE(PATH '/data/x.csv' FORMAT 'CSV')",
		},
		{
			"executable", SourceExecutable{Command: "/bin/dump", Format: "TabSeparated", ImplicitKey: ptr(true)},
			"EXECUTABLE(COMMAND '/bin/dump' FORMAT 'TabSeparated' IMPLICIT_KEY true)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, sourceSQL(tc.in))
		})
	}
}

func TestSQLGen_Dictionary_LifetimeSQL_Forms(t *testing.T) {
	cases := []struct {
		name string
		in   DictionaryLifetime
		want string
	}{
		{"simple (Min only)", DictionaryLifetime{Min: ptr(int64(300))}, "300"},
		{"range (Min and Max)", DictionaryLifetime{Min: ptr(int64(300)), Max: ptr(int64(600))}, "MIN 300 MAX 600"},
		{"max only", DictionaryLifetime{Max: ptr(int64(600))}, "MAX 600"},
		{"empty falls back to 0", DictionaryLifetime{}, "0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, lifetimeSQL(tc.in))
		})
	}
}

func TestSQLGen_Dictionary_AttributeSQL_AllFlags(t *testing.T) {
	cases := []struct {
		name string
		in   DictionaryAttribute
		want string
	}{
		{"basic", DictionaryAttribute{Name: "k", Type: "UInt64"}, "`k` UInt64"},
		{"default", DictionaryAttribute{Name: "v", Type: "String", Default: ptr("''")}, "`v` String DEFAULT ''"},
		{"expression", DictionaryAttribute{Name: "v", Type: "String", Expression: ptr("upper(raw)")}, "`v` String EXPRESSION upper(raw)"},
		{"hierarchical", DictionaryAttribute{Name: "parent", Type: "UInt64", Hierarchical: true}, "`parent` UInt64 HIERARCHICAL"},
		{"injective", DictionaryAttribute{Name: "label", Type: "String", Injective: true}, "`label` String INJECTIVE"},
		{"is_object_id", DictionaryAttribute{Name: "_id", Type: "String", IsObjectID: true}, "`_id` String IS_OBJECT_ID"},
		{
			"all flags",
			DictionaryAttribute{Name: "x", Type: "UInt64", Default: ptr("0"), Hierarchical: true, Injective: true, IsObjectID: true},
			"`x` UInt64 DEFAULT 0 HIERARCHICAL INJECTIVE IS_OBJECT_ID",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, dictionaryAttributeSQL(tc.in))
		})
	}
}

func TestSQLGen_Dictionary_DirectLayoutOmitsLifetime(t *testing.T) {
	// ClickHouse rejects LIFETIME on direct/complex_key_direct layouts.
	// Even if a spec carries one, sqlgen must skip it.
	d := DictionarySpec{
		Name:       "d",
		PrimaryKey: []string{"k"},
		Attributes: []DictionaryAttribute{{Name: "k", Type: "UInt64"}},
		Source:     &DictionarySourceSpec{Kind: "null", Decoded: SourceNull{}},
		Layout:     &DictionaryLayoutSpec{Kind: "direct", Decoded: LayoutDirect{}},
		Lifetime:   &DictionaryLifetime{Min: ptr(int64(300))},
	}
	got := createDictionarySQL("db", d)
	assert.NotContains(t, got, "LIFETIME", "direct layout must not emit LIFETIME")
	assert.Contains(t, got, "LAYOUT(DIRECT())")
}
