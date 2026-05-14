package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
		{Database: "posthog", DropTables: []string{"old_events"}},
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
			DropTables:  []string{"drop_me"},
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
			DropTables:            []string{"drop_table"},
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
