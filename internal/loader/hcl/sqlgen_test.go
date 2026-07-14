package hcl

import (
	"path/filepath"
	"strings"
	"testing"

	chparser "github.com/orian/clickhouse-sql-parser/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateSQL_Raw_AddAndDrop(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "db",
		AddRaws: []RawSpec{
			{Kind: "dictionary", Name: "d", SQL: "CREATE DICTIONARY db.d (k UInt64) LAYOUT(FLAT)\n"},
		},
		DropRaws: []RawSpec{
			{Kind: "view", Name: "v", SQL: "CREATE VIEW db.v AS SELECT 1\n"},
		},
	}}}
	out := GenerateSQL(cs)
	joined := strings.Join(out.Statements, "\n")
	assert.Contains(t, joined, "CREATE DICTIONARY db.d")
	assert.Contains(t, joined, "DROP VIEW IF EXISTS db.v")
	assert.Empty(t, out.Unsafe)
}

func TestGenerateSQL_Raw_DictRecreateIsSafe(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "db",
		AlterRaws: []RawChange{
			{Kind: "dictionary", Name: "d", OldSQL: "CREATE DICTIONARY db.d LAYOUT(FLAT)\n", NewSQL: "CREATE DICTIONARY db.d LAYOUT(HASHED)\n"},
		},
	}}}
	out := GenerateSQL(cs)
	joined := strings.Join(out.Statements, "\n")
	assert.Contains(t, joined, "DROP DICTIONARY IF EXISTS db.d")
	assert.Contains(t, joined, "LAYOUT(HASHED)")
	assert.Empty(t, out.Unsafe, "recreating a dictionary loses no data")
}

func TestGenerateSQL_Raw_TableRecreateIsUnsafeAndNotAutoEmitted(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "db",
		AlterRaws: []RawChange{
			{Kind: "table", Name: "t", OldSQL: "CREATE TABLE db.t (a UInt64) ENGINE = Log\n", NewSQL: "CREATE TABLE db.t (a UInt64, b UInt64) ENGINE = Log\n"},
		},
	}}}
	out := GenerateSQL(cs)
	require.Len(t, out.Unsafe, 1)
	assert.Equal(t, "t", out.Unsafe[0].Table)
	joined := strings.Join(out.Statements, "\n")
	assert.NotContains(t, joined, "DROP TABLE", "a destructive table recreate is never auto-emitted")
}

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
		BrokerList: ptr("kafka:9092"),
		TopicList:  ptr("events"),
		GroupName:  ptr("group1"),
		Format:     ptr("JSONEachRow"),
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
		ModifyColumns: []ColumnChange{{Name: "count", Old: ColumnSpec{Name: "count", Type: "UInt32"}, New: ColumnSpec{Name: "count", Type: "UInt64"}}},
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
	expected := []string{
		"ALTER TABLE posthog.events DROP INDEX old_idx, ADD INDEX idx_ts ts TYPE minmax GRANULARITY 4",
		"ALTER TABLE posthog.events MATERIALIZE INDEX idx_ts",
	}
	assert.Equal(t, expected, out.Statements)
	assert.False(t, out.Ops[0].Manual)
	assert.True(t, out.Ops[1].Manual, "MATERIALIZE INDEX must be operator-run, never auto-executed")
}

// A dropped index needs no materialization, and a brand-new table's indexes are
// built as data arrives — only ADD INDEX on an existing table emits the manual
// MATERIALIZE INDEX companion.
func TestSQLGen_MaterializeIndexOnlyForAddedIndexes(t *testing.T) {
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AlterTables: []TableDiff{{Table: "events", DropIndexes: []string{"old_idx"}}}},
	}})
	assert.Equal(t, []string{"ALTER TABLE posthog.events DROP INDEX old_idx"}, out.Statements)

	tbl := TableSpec{
		Name:    "fresh",
		Columns: []ColumnSpec{{Name: "ts", Type: "DateTime"}},
		Indexes: []IndexSpec{{Name: "idx_ts", Expr: "ts", Type: "minmax", Granularity: 4}},
		Engine:  &EngineSpec{Decoded: EngineMergeTree{}},
		OrderBy: []string{"ts"},
	}
	out = GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AddTables: []TableSpec{tbl}},
	}})
	for _, op := range out.Ops {
		assert.False(t, op.Manual)
		assert.NotContains(t, op.SQL, "MATERIALIZE INDEX")
	}
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

// TestSQLGen_FourTierMVExtend exercises the Kafka -> MV -> local -> Distributed
// pipeline where all four objects share columns via one abstract base. It is
// the end-to-end check that `extend` on materialized_view emits the inherited
// column list in the generated DDL.
func TestSQLGen_FourTierMVExtend(t *testing.T) {
	schema, err := ParseFile(filepath.Join("testdata", "resolve_mv_extend.hcl"))
	require.NoError(t, err)
	require.NoError(t, Resolve(schema))

	out := GenerateSQL(Diff(nil, schema))
	require.Empty(t, out.Unsafe)
	require.Len(t, out.Statements, 4)
	joined := strings.Join(out.Statements, "\n")

	assert.Contains(t, joined, "CREATE TABLE default.events_kafka")
	assert.Contains(t, joined, "CREATE TABLE default.events_local")
	assert.Contains(t, joined, "CREATE TABLE default.events_distributed")
	assert.Contains(t, joined, "CREATE MATERIALIZED VIEW default.events_mv")

	// Every inherited column appears on all four objects (3 tables emit
	// multi-line CREATE column lists, MV emits inline; assert per-column
	// count = 4 against the joined output).
	for _, col := range []string{"timestamp DateTime64(3)", "team_id UInt32", "event String", "properties String CODEC(ZSTD(3))"} {
		assert.Equal(t, 4, strings.Count(joined, col), "column fragment %q should appear once per object", col)
	}

	// MV emits the column list inline between TO <table> and AS.
	assert.Contains(t, joined, "TO events_local (timestamp DateTime64(3), team_id UInt32, event String, properties String CODEC(ZSTD(3))) AS SELECT")

	// db.Cluster cascades to ON CLUSTER on every emitted object.
	assert.Equal(t, 4, strings.Count(joined, "ON CLUSTER main"))

	// Distributed table is created after the local table it points at;
	// MV is created after both source (Kafka) and destination (local).
	localIdx := strings.Index(joined, "CREATE TABLE default.events_local")
	distIdx := strings.Index(joined, "CREATE TABLE default.events_distributed")
	mvIdx := strings.Index(joined, "CREATE MATERIALIZED VIEW default.events_mv")
	kafkaIdx := strings.Index(joined, "CREATE TABLE default.events_kafka")
	assert.Less(t, localIdx, distIdx, "local must precede Distributed")
	assert.Less(t, localIdx, mvIdx, "local (MV destination) must precede MV")
	assert.Less(t, kafkaIdx, mvIdx, "kafka (MV source) must precede MV")
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

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
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

func TestSQLGen_CreateOrder_ViewDependsOnView(t *testing.T) {
	// v_outer reads from v_inner; both are created in this change set. v_inner
	// must be emitted first even though it is declared second.
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AddViews: []ViewSpec{
			{Name: "v_outer", Query: "SELECT * FROM posthog.v_inner"},
			{Name: "v_inner", Query: "SELECT id FROM posthog.base"},
		},
	}}})
	inner := stmtIndex(out.Statements, "CREATE VIEW posthog.v_inner")
	outer := stmtIndex(out.Statements, "CREATE VIEW posthog.v_outer")
	require.NotEqual(t, -1, inner)
	require.NotEqual(t, -1, outer)
	assert.Less(t, inner, outer, "v_inner must be created before v_outer")
}

func TestSQLGen_CreateOrder_BufferAfterDestination(t *testing.T) {
	buf := TableSpec{
		Name:    "buf",
		Columns: []ColumnSpec{{Name: "id", Type: "UInt64"}},
		Engine: &EngineSpec{Kind: "buffer", Decoded: EngineBuffer{
			Database: "posthog", Table: "dest", NumLayers: 1,
			MinTime: 1, MaxTime: 10, MinRows: 1, MaxRows: 10, MinBytes: 1, MaxBytes: 10,
		}},
	}
	dest := mkTable("dest", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UInt64"})
	dest.OrderBy = []string{"id"}
	// Buffer declared first; it must still be emitted after its destination.
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog", AddTables: []TableSpec{buf, dest},
	}}})
	d := stmtIndex(out.Statements, "CREATE TABLE posthog.dest")
	b := stmtIndex(out.Statements, "CREATE TABLE posthog.buf")
	require.NotEqual(t, -1, d)
	require.NotEqual(t, -1, b)
	assert.Less(t, d, b, "destination table must be created before the Buffer")
}

func TestSQLGen_CreateOrder_DictionaryAfterSourceTable(t *testing.T) {
	dict := mkDict("d", SourceClickHouse{Table: ptr("src")}, LayoutFlat{},
		DictionaryAttribute{Name: "v", Type: "String"})
	src := mkTable("src", EngineMergeTree{}, ColumnSpec{Name: "k", Type: "UInt64"})
	src.OrderBy = []string{"k"}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog", AddTables: []TableSpec{src}, AddDictionaries: []DictionarySpec{dict},
	}}})
	s := stmtIndex(out.Statements, "posthog.src")
	d := stmtIndex(out.Statements, "DICTIONARY posthog.d")
	require.NotEqual(t, -1, s)
	require.NotEqual(t, -1, d)
	assert.Less(t, s, d, "source table must be created before the dictionary")
}

func TestSQLGen_CreateOrder_DictionaryDependsOnDictionary(t *testing.T) {
	// Dictionary 'a' sources from dictionary 'b'. By name they sort a, b; the
	// dependency must still emit b before a.
	a := mkDict("a", SourceClickHouse{Table: ptr("b")}, LayoutFlat{}, DictionaryAttribute{Name: "v", Type: "String"})
	b := mkDict("b", SourceClickHouse{Table: ptr("base")}, LayoutFlat{}, DictionaryAttribute{Name: "v", Type: "String"})
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog", AddDictionaries: []DictionarySpec{a, b},
	}}})
	ib := stmtIndex(out.Statements, "DICTIONARY posthog.b")
	ia := stmtIndex(out.Statements, "DICTIONARY posthog.a")
	require.NotEqual(t, -1, ib)
	require.NotEqual(t, -1, ia)
	assert.Less(t, ib, ia, "dict b must be created before dict a which sources from it")
}

func TestSQLGen_CreateOrder_CrossKind_MVSourceIsView(t *testing.T) {
	// A materialized view reads from a plain view; the view must be created
	// before the MV even though MVs are emitted before views by default.
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AddMaterializedViews: []MaterializedViewSpec{
			{Name: "mv", ToTable: "posthog.dest", Query: "SELECT id FROM posthog.src_view"},
		},
		AddViews: []ViewSpec{
			{Name: "src_view", Query: "SELECT id FROM posthog.base"},
		},
	}}})
	v := stmtIndex(out.Statements, "CREATE VIEW posthog.src_view")
	mv := stmtIndex(out.Statements, "posthog.mv")
	require.NotEqual(t, -1, v)
	require.NotEqual(t, -1, mv)
	assert.Less(t, v, mv, "the source view must be created before the materialized view that reads it")
}

func TestSQLGen_CreateOrder_CycleDoesNotHang(t *testing.T) {
	// Two views reference each other. The generator must not hang or drop a
	// statement; it breaks the cycle by falling back to input order.
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AddViews: []ViewSpec{
			{Name: "v1", Query: "SELECT * FROM posthog.v2"},
			{Name: "v2", Query: "SELECT * FROM posthog.v1"},
		},
	}}})
	assert.Len(t, out.Statements, 2)
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
					Password: ptr("s3cret"),
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
		"SOURCE(CLICKHOUSE(USER 'default' PASSWORD 's3cret' QUERY 'SELECT ... FROM default.exchange_rate')) " +
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

// dictSpec is a minimal valid dictionary, for the alter/guard tests below.
func dictSpec(name string, src DictionarySource) DictionarySpec {
	return DictionarySpec{
		Name:       name,
		PrimaryKey: []string{"k"},
		Attributes: []DictionaryAttribute{{Name: "k", Type: "UInt64"}, {Name: "v", Type: "String"}},
		Source:     &DictionarySourceSpec{Kind: src.Kind(), Decoded: src},
		Layout:     &DictionaryLayoutSpec{Kind: "hashed", Decoded: LayoutHashed{}},
		Lifetime:   &DictionaryLifetime{Min: ptr(int64(60)), Max: ptr(int64(600))},
	}
}

// A changed dictionary is reconciled by rewriting it whole. Before #140 this
// emitted NO DDL and an UNSAFE line — the migration silently left the
// dictionary as it was, contradicting DictionaryDiff.IsUnsafe() == false.
func TestSQLGen_AlterDictionary_EmitsCreateOrReplace(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "db",
		AlterDictionaries: []DictionaryDiff{{
			Name:    "d",
			Changed: []string{"lifetime"},
			New:     dictSpec("d", SourceNull{}),
		}},
	}}}
	out := GenerateSQL(cs)
	require.Len(t, out.Statements, 1)
	assert.Equal(t,
		"CREATE OR REPLACE DICTIONARY db.d (`k` UInt64, `v` String) PRIMARY KEY k "+
			"SOURCE(NULL()) LAYOUT(HASHED()) LIFETIME(MIN 60 MAX 600)",
		out.Statements[0])
	assert.Empty(t, out.Unsafe, "recreating a dictionary loses no data — it reloads from its source")

	require.Len(t, out.Ops, 1)
	assert.Equal(t, OpCreate, out.Ops[0].Kind, "the op kind is the verb of the statement")
	assert.Equal(t, KindDictionary, out.Ops[0].ObjectType)
}

// The target spec is what gets written, so an unknown secret on the target
// means the rewrite would install the dictionary without its credential.
func TestSQLGen_AlterDictionary_RedactedTargetIsBlocked(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "db",
		AlterDictionaries: []DictionaryDiff{{
			Name:    "d",
			Changed: []string{"lifetime"},
			New:     dictSpec("d", SourceMySQL{Host: ptr("h"), Password: ptr(RedactedValue)}),
		}},
	}}}
	out := GenerateSQL(cs)
	assert.Empty(t, out.Statements)
	require.Len(t, out.Unsafe, 1)
	assert.Equal(t, "db", out.Unsafe[0].Database)
	assert.Equal(t, "d", out.Unsafe[0].Table)
	assert.Contains(t, out.Unsafe[0].Reason, `dictionary source secret "password" is unknown to hclexp`)
}

func TestSQLGen_AddDictionary_RedactedSecretIsBlocked(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database:        "db",
		AddDictionaries: []DictionarySpec{dictSpec("d", SourceHTTP{URL: "u", Format: "CSV", CredentialsPassword: ptr(RedactedValue)})},
	}}}
	out := GenerateSQL(cs)
	assert.Empty(t, out.Statements, "a CREATE would install the dictionary with no credential at all")
	require.Len(t, out.Unsafe, 1)
	assert.Contains(t, out.Unsafe[0].Reason, `dictionary source secret "credentials_password" is unknown to hclexp`)
}

// A dictionary whose ONLY difference is an unverifiable secret is reported
// (see BuildObjectComparisons) but has nothing to reconcile.
func TestSQLGen_AlterDictionary_SkippedSecretOnly_EmitsNothing(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "db",
		AlterDictionaries: []DictionaryDiff{{
			Name:                   "d",
			SkippedRedactedSecrets: []string{"password"},
			New:                    dictSpec("d", SourceMySQL{Host: ptr("h"), Password: ptr("real")}),
		}},
	}}}
	out := GenerateSQL(cs)
	assert.Empty(t, out.Statements)
	assert.Empty(t, out.Unsafe)
}

// Backstop: whatever path builds a statement, the redaction marker never
// leaves hclexp. Named collections still let it through their per-param diff
// (an added NC with redacted params, #141) — emit() is what stops it.
// Backstop: whatever path builds a statement, the redaction marker never
// leaves hclexp. The NC diff (three-verdict handling) and the CREATE
// guards stop it upstream, so drive emit() directly with a hand-built SET —
// the backstop is the last line of defence for paths no guard anticipated.
func TestSQLGen_EmitRefusesStatementCarryingRedactionMarker(t *testing.T) {
	cs := ChangeSet{NamedCollections: []NamedCollectionChange{{
		Name:      "nc",
		SetParams: []NamedCollectionParam{{Key: "password", Value: RedactedValue}},
	}}}
	out := GenerateSQL(cs)
	assert.Empty(t, out.Statements, "writing the literal [HIDDEN] would clobber the real secret")
	require.Len(t, out.Unsafe, 1)
	assert.Equal(t, "nc", out.Unsafe[0].Table)
	assert.Contains(t, out.Unsafe[0].Reason, "refusing to write it to a cluster")
}

func TestSQLGen_NamedCollectionAdd_RedactedParamBlocked(t *testing.T) {
	cs := ChangeSet{NamedCollections: []NamedCollectionChange{{
		Name: "nc",
		Add: &NamedCollectionSpec{
			Name: "nc",
			Params: []NamedCollectionParam{
				{Key: "url", Value: "https://b"},
				{Key: "password", Value: RedactedValue},
			},
		},
	}}}
	out := GenerateSQL(cs)
	assert.Empty(t, out.Statements)
	require.Len(t, out.Unsafe, 1)
	assert.Equal(t, "nc", out.Unsafe[0].Table)
	assert.Contains(t, out.Unsafe[0].Reason, "named collection param(s) [password]")
	assert.Contains(t, out.Unsafe[0].Reason, "displaySecretsInShowAndSelect")
}

func TestSQLGen_NamedCollectionRecreate_RedactedParamBlocksPair(t *testing.T) {
	cluster := "main"
	cs := ChangeSet{NamedCollections: []NamedCollectionChange{{
		Name: "nc", Recreate: true, Drop: true,
		Add: &NamedCollectionSpec{
			Name:    "nc",
			Cluster: &cluster,
			Params:  []NamedCollectionParam{{Key: "password", Value: RedactedValue}},
		},
	}}}
	out := GenerateSQL(cs)
	assert.Empty(t, out.Statements, "no DROP either — dropping without recreating destroys the collection")
	require.Len(t, out.Unsafe, 1)
	assert.Contains(t, out.Unsafe[0].Reason, "named collection param(s) [password]")
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

func TestSQLGen_CreateView_Bare(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AddViews: []ViewSpec{{Name: "v", Query: "SELECT 1"}},
	}}}
	got := GenerateSQL(cs)
	require.Len(t, got.Statements, 1)
	assert.Equal(t, "CREATE VIEW posthog.v AS SELECT 1", got.Statements[0])
}

func TestSQLGen_CreateView_FullForm(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AddViews: []ViewSpec{{
			Name:          "v",
			Query:         "SELECT team_id, count() AS n FROM events GROUP BY team_id",
			ColumnAliases: []string{"team_id", "n"},
			SQLSecurity:   ptr("definer"),
			Definer:       ptr("alice"),
			Cluster:       ptr("posthog"),
			Comment:       ptr("team-level"),
		}},
	}}}
	got := GenerateSQL(cs)
	require.Len(t, got.Statements, 1)
	assert.Equal(t,
		`CREATE VIEW posthog.v ON CLUSTER posthog (team_id, n) DEFINER = alice SQL SECURITY DEFINER AS SELECT team_id, count() AS n FROM events GROUP BY team_id COMMENT 'team-level'`,
		got.Statements[0])
}

// A view whose body projects a star cannot carry an explicit column-alias list:
// ClickHouse rejects `CREATE VIEW v (a, b) AS SELECT * ...` because the column
// count isn't statically known. The generator must omit the inferred alias list
// for such views. See issue #41.
func TestSQLGen_CreateView_OmitsAliasesWhenBodyHasStar(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		keepAliases bool
	}{
		{
			name:        "plain select star",
			query:       "SELECT * FROM custom_metrics_test",
			keepAliases: false,
		},
		{
			name:        "select star with REPLACE and UNION ALL (repro)",
			query:       "SELECT * REPLACE(toFloat64(value) AS value) FROM custom_metrics_test UNION ALL SELECT 'X' AS name, map('instance', hostname()) AS labels, toFloat64(count()) AS value, 'h' AS help, 'gauge' AS type FROM system.part_log",
			keepAliases: false,
		},
		{
			name:        "qualified star",
			query:       "SELECT t.* FROM custom_metrics_test AS t",
			keepAliases: false,
		},
		{
			name:        "star only in a non-first union branch",
			query:       "SELECT 'a' AS name FROM x UNION ALL SELECT * FROM y",
			keepAliases: false,
		},
		{
			name:        "static projection keeps aliases",
			query:       "SELECT team_id, count() AS n FROM events GROUP BY team_id",
			keepAliases: true,
		},
		{
			name:        "count star is not a projection star",
			query:       "SELECT count(*) AS n FROM events",
			keepAliases: true,
		},
		{
			name:        "multiplication is not a projection star",
			query:       "SELECT a * b AS p FROM events",
			keepAliases: true,
		},
		{
			name:        "star only inside a from-subquery keeps aliases",
			query:       "SELECT a AS x FROM (SELECT * FROM t)",
			keepAliases: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := ViewSpec{Name: "v", Query: tc.query, ColumnAliases: []string{"name", "labels", "value", "help", "type"}}
			got := createViewSQL("posthog", v)
			if tc.keepAliases {
				assert.Contains(t, got, "(name, labels, value, help, type)", "expected alias list to be kept")
			} else {
				assert.NotContains(t, got, "(name, labels, value, help, type)", "expected alias list to be omitted")
				assert.Contains(t, got, tc.query, "query body must still be present")
			}
		})
	}
}

// TestSQLGen_CreateView_StarReplace_Reappliable mirrors what introspect captures
// for the issue #41 view: ClickHouse stores its own inferred column list in
// create_table_query, so the ViewSpec carries ColumnAliases alongside a starred
// body. The regenerated DDL must equal the form ClickHouse actually accepts (no
// column list) and must itself parse as a valid CREATE VIEW.
func TestSQLGen_CreateView_StarReplace_Reappliable(t *testing.T) {
	v := ViewSpec{
		Name:          "custom_metrics",
		Query:         "SELECT * REPLACE(toFloat64(value) AS value) FROM posthog.custom_metrics_test",
		ColumnAliases: []string{"name", "labels", "value", "help", "type"},
	}
	got := createViewSQL("posthog", v)

	assert.Equal(t,
		"CREATE VIEW posthog.custom_metrics AS SELECT * REPLACE(toFloat64(value) AS value) FROM posthog.custom_metrics_test",
		got)

	// The regenerated statement must be syntactically valid CREATE VIEW.
	stmts, err := chparser.NewParser(got).ParseStmts()
	require.NoError(t, err, "regenerated CREATE VIEW must parse")
	require.Len(t, stmts, 1)
	_, ok := stmts[0].(*chparser.CreateView)
	assert.True(t, ok, "regenerated statement must be a CREATE VIEW")
}

func TestSQLGen_DropPlainView(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database:  "posthog",
		DropViews: []string{"old_v"},
	}}}
	got := GenerateSQL(cs)
	require.Len(t, got.Statements, 1)
	assert.Equal(t, "DROP VIEW posthog.old_v", got.Statements[0])
}

func TestSQLGen_AlterView_ModifyQuery(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AlterViews: []ViewDiff{{
			Name:        "v",
			QueryChange: &StringChange{Old: ptr("SELECT 1"), New: ptr("SELECT 2")},
		}},
	}}}
	got := GenerateSQL(cs)
	require.Len(t, got.Statements, 1)
	assert.Equal(t, "ALTER TABLE posthog.v MODIFY QUERY SELECT 2", got.Statements[0])
}

func TestSQLGen_AlterView_ModifyComment(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AlterViews: []ViewDiff{{
			Name:    "v",
			Comment: &StringChange{Old: ptr("old"), New: ptr("new")},
		}},
	}}}
	got := GenerateSQL(cs)
	require.Len(t, got.Statements, 1)
	assert.Equal(t, "ALTER TABLE posthog.v MODIFY COMMENT 'new'", got.Statements[0])
}

func TestSQLGen_AlterView_RecreateIsUnsafe(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AlterViews: []ViewDiff{{
			Name:     "v",
			Recreate: true,
		}},
	}}}
	got := GenerateSQL(cs)
	assert.Empty(t, got.Statements)
	require.Len(t, got.Unsafe, 1)
	assert.Equal(t, "v", got.Unsafe[0].Table)
	assert.Contains(t, got.Unsafe[0].Reason, "recreating")
}

func TestSQLGen_ViewCreatedAfterSourceTable(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		AddTables: []TableSpec{{
			Name:    "events",
			Columns: []ColumnSpec{{Name: "team_id", Type: "Int64"}},
			OrderBy: []string{"team_id"},
			Engine:  &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
		}},
		AddViews: []ViewSpec{{Name: "v", Query: "SELECT team_id FROM posthog.events"}},
	}}}
	got := GenerateSQL(cs)
	joined := strings.Join(got.Statements, "\n")
	tIdx := strings.Index(joined, "CREATE TABLE posthog.events")
	vIdx := strings.Index(joined, "CREATE VIEW posthog.v")
	require.GreaterOrEqual(t, tIdx, 0)
	require.GreaterOrEqual(t, vIdx, 0)
	assert.Less(t, tIdx, vIdx, "view must be created after its source table")
}

func TestSQLGen_ViewDroppedBeforeSourceTable(t *testing.T) {
	cs := ChangeSet{Databases: []DatabaseChange{{
		Database: "posthog",
		DropTables: []TableSpec{{
			Name:    "events",
			Columns: []ColumnSpec{{Name: "team_id", Type: "Int64"}},
			OrderBy: []string{"team_id"},
			Engine:  &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
		}},
		DropViews: []string{"v"},
	}}}
	got := GenerateSQL(cs)
	joined := strings.Join(got.Statements, "\n")
	vIdx := strings.Index(joined, "DROP VIEW posthog.v")
	tIdx := strings.Index(joined, "DROP TABLE")
	require.GreaterOrEqual(t, vIdx, 0)
	require.GreaterOrEqual(t, tIdx, 0)
	assert.Less(t, vIdx, tIdx, "view must be dropped before its source table")
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

func TestSQLGen_Engine_ReplicatedSummingMergeTree(t *testing.T) {
	t.Run("with sum_columns", func(t *testing.T) {
		clause, extra := engineSQL(EngineReplicatedSummingMergeTree{
			ZooPath:     "/clickhouse/tables/{shard}/distinct_id_usage",
			ReplicaName: "{replica}",
			SumColumns:  []string{"count"},
		})
		assert.Equal(t, "ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/distinct_id_usage', '{replica}', (count))", clause)
		assert.Nil(t, extra)
	})

	t.Run("without sum_columns", func(t *testing.T) {
		clause, extra := engineSQL(EngineReplicatedSummingMergeTree{
			ZooPath:     "/clickhouse/tables/{shard}/distinct_id_usage",
			ReplicaName: "{replica}",
		})
		assert.Equal(t, "ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/distinct_id_usage', '{replica}')", clause)
		assert.Nil(t, extra)
	})

	t.Run("with multiple sum_columns", func(t *testing.T) {
		clause, extra := engineSQL(EngineReplicatedSummingMergeTree{
			ZooPath:     "/zk",
			ReplicaName: "r",
			SumColumns:  []string{"a", "b"},
		})
		assert.Equal(t, "ReplicatedSummingMergeTree('/zk', 'r', (a, b))", clause)
		assert.Nil(t, extra)
	})
}

func TestSQLGen_TimeSeries_External_DataAlias(t *testing.T) {
	tgtData := "default.m_data"
	tgtTags := "default.m_tags"
	tgtMetrics := "default.m_metrics"
	ts := TableSpec{Name: "m",
		Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
		Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
			Samples:     &TimeSeriesTarget{Target: &tgtData},
			Tags:        &TimeSeriesTarget{Target: &tgtTags},
			Metrics:     &TimeSeriesTarget{Target: &tgtMetrics},
			KeywordHint: "DATA",
		}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "default", AddTables: []TableSpec{ts},
	}}})
	require.Len(t, out.Statements, 1)
	stmt := out.Statements[0]
	assert.Contains(t, stmt, "ENGINE = TimeSeries")
	assert.Contains(t, stmt, "DATA default.m_data")
	assert.Contains(t, stmt, "TAGS default.m_tags")
	assert.Contains(t, stmt, "METRICS default.m_metrics")
	assert.NotContains(t, stmt, "SAMPLES")
}

func TestSQLGen_TimeSeries_External_DefaultsToSamplesKeyword(t *testing.T) {
	tgt := "default.m_data"
	ts := TableSpec{Name: "m",
		Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
		Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
			Samples: &TimeSeriesTarget{Target: &tgt},
		}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "default", AddTables: []TableSpec{ts},
	}}})
	stmt := out.Statements[0]
	assert.Contains(t, stmt, "SAMPLES default.m_data")
	assert.NotContains(t, stmt, "DATA default.m_data")
}

func TestSQLGen_TimeSeries_Inner(t *testing.T) {
	inner := &TimeSeriesInnerTable{
		Columns: []ColumnSpec{
			{Name: "id", Type: "UUID"},
			{Name: "timestamp", Type: "DateTime64(3)"},
			{Name: "value", Type: "Float64"},
		},
		Engine:  &EngineSpec{Decoded: EngineMergeTree{}},
		OrderBy: []string{"id", "timestamp"},
	}
	ts := TableSpec{Name: "m",
		Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
		Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
			Samples: &TimeSeriesTarget{Inner: inner},
		}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "default", AddTables: []TableSpec{ts},
	}}})
	stmt := out.Statements[0]
	assert.Contains(t, stmt, "SAMPLES INNER COLUMNS (id UUID, timestamp DateTime64(3), value Float64)")
	assert.Contains(t, stmt, "SAMPLES INNER ENGINE = MergeTree()")
	assert.Contains(t, stmt, "ORDER BY (id, timestamp)")
}

func TestSQLGen_TimeSeries_TagsToColumnsFolded(t *testing.T) {
	ts := TableSpec{Name: "m",
		Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
		Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
			TagsToColumns: map[string]string{"instance": "instance", "job": "job"},
		}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "default", AddTables: []TableSpec{ts},
	}}})
	stmt := out.Statements[0]
	assert.Contains(t, stmt, "tags_to_columns = {'instance':'instance', 'job':'job'}")
}

func TestSQLGen_Join_SingleKey(t *testing.T) {
	ts := TableSpec{Name: "j",
		Columns: []ColumnSpec{{Name: "id", Type: "UInt64"}, {Name: "value", Type: "String"}},
		Engine:  &EngineSpec{Kind: "join", Decoded: EngineJoin{Strictness: "ANY", JoinType: "LEFT", Keys: []string{"id"}}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "default", AddTables: []TableSpec{ts},
	}}})
	assert.Contains(t, out.Statements[0], "ENGINE = Join(ANY, LEFT, id)")
}

func TestSQLGen_Join_MultiKey(t *testing.T) {
	ts := TableSpec{Name: "j",
		Columns: []ColumnSpec{{Name: "a", Type: "UInt64"}, {Name: "b", Type: "UInt64"}, {Name: "v", Type: "String"}},
		Engine:  &EngineSpec{Kind: "join", Decoded: EngineJoin{Strictness: "ALL", JoinType: "INNER", Keys: []string{"a", "b"}}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "default", AddTables: []TableSpec{ts},
	}}})
	assert.Contains(t, out.Statements[0], "ENGINE = Join(ALL, INNER, a, b)")
}

func TestSQLGen_Buffer_BareArgs(t *testing.T) {
	ts := TableSpec{Name: "buf",
		Columns: []ColumnSpec{{Name: "id", Type: "UUID"}},
		Engine: &EngineSpec{Kind: "buffer", Decoded: EngineBuffer{
			Database: "", Table: "dest", NumLayers: 16,
			MinTime: 10, MaxTime: 100,
			MinRows: 10000, MaxRows: 1000000,
			MinBytes: 10000000, MaxBytes: 100000000,
		}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "default", AddTables: []TableSpec{ts},
	}}})
	assert.Contains(t, out.Statements[0], "ENGINE = Buffer('', 'dest', 16, 10, 100, 10000, 1000000, 10000000, 100000000)")
}

func TestSQLGen_Buffer_WithFlushTriplet(t *testing.T) {
	ft, fr, fb := int64(30), int64(200), int64(50000)
	ts := TableSpec{Name: "buf",
		Columns: []ColumnSpec{{Name: "id", Type: "UUID"}},
		Engine: &EngineSpec{Kind: "buffer", Decoded: EngineBuffer{
			Database: "default", Table: "dest", NumLayers: 1,
			MinTime: 5, MaxTime: 60, MinRows: 100, MaxRows: 10000,
			MinBytes: 1000, MaxBytes: 100000,
			FlushTime: &ft, FlushRows: &fr, FlushBytes: &fb,
		}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "default", AddTables: []TableSpec{ts},
	}}})
	assert.Contains(t, out.Statements[0], "Buffer('default', 'dest', 1, 5, 60, 100, 10000, 1000, 100000, 30, 200, 50000)")
}

func TestSQLGen_Null_Memory_Merge(t *testing.T) {
	for _, c := range []struct {
		dec    Engine
		expect string
	}{
		{EngineNull{}, "ENGINE = Null()"},
		{EngineMemory{}, "ENGINE = Memory()"},
		{EngineMerge{DBRegex: "default", TableRegex: "^shard_.*"}, "ENGINE = Merge('default', '^shard_.*')"},
	} {
		ts := TableSpec{Name: "t",
			Columns: []ColumnSpec{{Name: "id", Type: "UUID"}},
			Engine:  &EngineSpec{Kind: c.dec.Kind(), Decoded: c.dec},
		}
		out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
			Database: "default", AddTables: []TableSpec{ts},
		}}})
		assert.Contains(t, out.Statements[0], c.expect)
	}
}

func TestSQLGen_ColumnModifierChanges(t *testing.T) {
	alter := func(mc ColumnChange) GeneratedSQL {
		return GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
			Database: "d", AlterTables: []TableDiff{{Table: "t", ModifyColumns: []ColumnChange{mc}}},
		}}})
	}

	t.Run("safe COMMENT change emits MODIFY COLUMN with the modifier", func(t *testing.T) {
		out := alter(ColumnChange{Name: "y",
			Old: ColumnSpec{Name: "y", Type: "Int64"},
			New: ColumnSpec{Name: "y", Type: "Int64", Comment: ptr("hi")}})
		require.Len(t, out.Statements, 1)
		assert.Contains(t, out.Statements[0], "MODIFY COLUMN y Int64 COMMENT 'hi'")
		assert.Empty(t, out.Unsafe)
	})

	t.Run("unsafe ALIAS switch is flagged and NOT auto-emitted", func(t *testing.T) {
		out := alter(ColumnChange{Name: "y",
			Old: ColumnSpec{Name: "y", Type: "Int64"},
			New: ColumnSpec{Name: "y", Type: "Int64", Alias: ptr("x")}})
		assert.Empty(t, out.Statements, "destructive storage-class switch must not be auto-emitted")
		require.Len(t, out.Unsafe, 1)
		assert.Contains(t, out.Unsafe[0].Reason, "storage class")
	})

	t.Run("ADD COLUMN carries modifiers", func(t *testing.T) {
		out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
			Database: "d", AlterTables: []TableDiff{{Table: "t",
				AddColumns: []ColumnSpec{{Name: "z", Type: "Int64", Alias: ptr("x")}}}},
		}}})
		require.Len(t, out.Statements, 1)
		assert.Contains(t, out.Statements[0], "ADD COLUMN z Int64 ALIAS x")
	})
}
