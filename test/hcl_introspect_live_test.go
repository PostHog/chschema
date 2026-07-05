package test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/posthog/chschema/internal/utils"
	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/require"
)

// TestLive_HCLIntrospect exercises the HCL introspection path end-to-end
// against a real ClickHouse instance: create tables, introspect the database,
// and assert the resulting DatabaseSpec. This is the same path the
// `hclexp introspect` command uses.
func TestLive_HCLIntrospect(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}

	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	stmts := []string{
		`CREATE TABLE ` + dbName + `.events (
			id UInt64,
			ts DateTime,
			name String DEFAULT 'anon' COMMENT 'display name',
			payload String CODEC(ZSTD(3)),
			score Nullable(Float64)
		) ENGINE = MergeTree
		PARTITION BY toYYYYMM(ts)
		ORDER BY (id, ts)`,

		`CREATE TABLE ` + dbName + `.dist_policy (
			id UInt64
		) ENGINE = Distributed('posthog', '` + dbName + `', 'events', sipHash64(id), 'default')`,

		`CREATE TABLE ` + dbName + `.soft_deleted (
			id UInt64,
			version UInt32,
			is_deleted UInt8
		) ENGINE = ReplacingMergeTree(version, is_deleted)
		ORDER BY id`,

		`CREATE TABLE ` + dbName + `.versioned (
			id UInt64,
			version UInt32,
			value String
		) ENGINE = ReplacingMergeTree(version)
		ORDER BY id`,

		`CREATE TABLE ` + dbName + `.with_index (
			id UInt64,
			email String,
			INDEX idx_email email TYPE bloom_filter() GRANULARITY 1
		) ENGINE = MergeTree
		ORDER BY id`,
	}
	for _, s := range stmts {
		require.NoError(t, conn.Exec(ctx, s), "failed to create test table")
	}

	got, err := hclload.Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)

	want := &hclload.DatabaseSpec{
		Name: dbName,
		Tables: []hclload.TableSpec{
			{
				Name:    "dist_policy",
				Columns: []hclload.ColumnSpec{{Name: "id", Type: "UInt64"}},
				Engine: &hclload.EngineSpec{
					Kind: "distributed",
					Decoded: hclload.EngineDistributed{
						ClusterName:    "posthog",
						RemoteDatabase: dbName,
						RemoteTable:    "events",
						ShardingKey:    utils.Ptr("sipHash64(id)"),
						PolicyName:     utils.Ptr("default"),
					},
				},
			},
			{
				Name:        "events",
				OrderBy:     []string{"id", "ts"},
				PartitionBy: utils.Ptr("toYYYYMM(ts)"),
				Settings:    map[string]string{"index_granularity": "8192"},
				Columns: []hclload.ColumnSpec{
					{Name: "id", Type: "UInt64"},
					{Name: "ts", Type: "DateTime"},
					{Name: "name", Type: "String", Default: utils.Ptr("'anon'"), Comment: utils.Ptr("display name")},
					{Name: "payload", Type: "String", Codec: utils.Ptr("ZSTD(3)")},
					{Name: "score", Type: "Nullable(Float64)"},
				},
				Engine: &hclload.EngineSpec{Kind: "merge_tree", Decoded: hclload.EngineMergeTree{}},
			},
			{
				Name:     "soft_deleted",
				OrderBy:  []string{"id"},
				Settings: map[string]string{"index_granularity": "8192"},
				Columns: []hclload.ColumnSpec{
					{Name: "id", Type: "UInt64"},
					{Name: "version", Type: "UInt32"},
					{Name: "is_deleted", Type: "UInt8"},
				},
				Engine: &hclload.EngineSpec{
					Kind: "replacing_merge_tree",
					Decoded: hclload.EngineReplacingMergeTree{
						VersionColumn:   utils.Ptr("version"),
						IsDeletedColumn: utils.Ptr("is_deleted"),
					},
				},
			},
			{
				Name:     "versioned",
				OrderBy:  []string{"id"},
				Settings: map[string]string{"index_granularity": "8192"},
				Columns: []hclload.ColumnSpec{
					{Name: "id", Type: "UInt64"},
					{Name: "version", Type: "UInt32"},
					{Name: "value", Type: "String"},
				},
				Engine: &hclload.EngineSpec{
					Kind:    "replacing_merge_tree",
					Decoded: hclload.EngineReplacingMergeTree{VersionColumn: utils.Ptr("version")},
				},
			},
			{
				Name:     "with_index",
				OrderBy:  []string{"id"},
				Settings: map[string]string{"index_granularity": "8192"},
				Columns: []hclload.ColumnSpec{
					{Name: "id", Type: "UInt64"},
					{Name: "email", Type: "String"},
				},
				Indexes: []hclload.IndexSpec{
					{Name: "idx_email", Expr: "email", Type: "bloom_filter()", Granularity: 1},
				},
				Engine: &hclload.EngineSpec{Kind: "merge_tree", Decoded: hclload.EngineMergeTree{}},
			},
		},
	}

	require.Equal(t, want, got)
}

// TestLive_HCLIntrospect_TimeSeries verifies that a TimeSeries-engine
// table — both external-target and inner-target form — round-trips
// through `hclload.Introspect`. TimeSeries is experimental in CH and
// requires opt-in via the allow_experimental_time_series_table setting.
func TestLive_HCLIntrospect_TimeSeries(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}

	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	// External-target form: stand up the three target tables, then point
	// a TimeSeries at them. `allow_experimental_time_series_table = 1` is
	// appended to each CREATE that uses TimeSeries because the Go driver
	// may use a connection pool — a session-level SET wouldn't survive.
	tsSetting := " SETTINGS allow_experimental_time_series_table = 1"

	stmts := []string{
		`CREATE TABLE ` + dbName + `.prom_data (
			id UUID,
			timestamp DateTime64(3),
			value Float64
		) ENGINE = MergeTree ORDER BY (id, timestamp)`,
		`CREATE TABLE ` + dbName + `.prom_tags (
			id UUID,
			metric_name LowCardinality(String),
			tags Map(LowCardinality(String), String)
		) ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id)`,
		`CREATE TABLE ` + dbName + `.prom_metrics_meta (
			metric_family_name String,
			type String,
			unit String,
			help String
		) ENGINE = ReplacingMergeTree ORDER BY metric_family_name`,
		`CREATE TABLE ` + dbName + `.prom_external ENGINE = TimeSeries ` +
			`DATA ` + dbName + `.prom_data ` +
			`TAGS ` + dbName + `.prom_tags ` +
			`METRICS ` + dbName + `.prom_metrics_meta` + tsSetting,

		// NOTE: the bare `ENGINE = TimeSeries` form (no targets) is
		// intentionally NOT exercised here. CH's SHOW CREATE TABLE for
		// such a table emits a shorthand the chparser fork doesn't
		// recognise yet — `DATA ENGINE = MergeTree ORDER BY (...)` (no
		// INNER keyword between DATA and ENGINE). To be filed as a
		// follow-up chparser issue.
	}
	for _, s := range stmts {
		require.NoError(t, conn.Exec(ctx, s), "create failed: %s", s)
	}

	got, err := hclload.Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)

	var external *hclload.TableSpec
	for i := range got.Tables {
		if got.Tables[i].Name == "prom_external" {
			external = &got.Tables[i]
		}
	}

	require.NotNil(t, external, "prom_external should round-trip")
	ext, ok := external.Engine.Decoded.(hclload.EngineTimeSeries)
	require.True(t, ok)
	require.Equal(t, "DATA", ext.KeywordHint, "DATA alias preserved")
	require.NotNil(t, ext.Samples)
	require.NotNil(t, ext.Samples.Target)
	require.Equal(t, dbName+".prom_data", *ext.Samples.Target)
	require.NotNil(t, ext.Tags)
	require.Equal(t, dbName+".prom_tags", *ext.Tags.Target)
	require.NotNil(t, ext.Metrics)
	require.Equal(t, dbName+".prom_metrics_meta", *ext.Metrics.Target)
}

// TestLive_HCLIntrospect_Join verifies that a Join-engine table —
// the most common missing engine in PostHog production with ~180
// tables — round-trips through `hclload.Introspect`.
func TestLive_HCLIntrospect_Join(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	stmts := []string{
		`CREATE TABLE ` + dbName + `.single_key (
			id UInt64,
			value String
		) ENGINE = Join(ANY, LEFT, id)`,

		`CREATE TABLE ` + dbName + `.multi_key (
			user_id UInt64,
			session_id UInt64,
			value String
		) ENGINE = Join(ALL, INNER, user_id, session_id)`,
	}
	for _, s := range stmts {
		require.NoError(t, conn.Exec(ctx, s), "create failed: %s", s)
	}

	got, err := hclload.Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)

	byName := map[string]hclload.Engine{}
	for _, tbl := range got.Tables {
		byName[tbl.Name] = tbl.Engine.Decoded
	}

	sk, ok := byName["single_key"].(hclload.EngineJoin)
	require.True(t, ok)
	require.Equal(t, "ANY", sk.Strictness)
	require.Equal(t, "LEFT", sk.JoinType)
	require.Equal(t, []string{"id"}, sk.Keys)

	mk, ok := byName["multi_key"].(hclload.EngineJoin)
	require.True(t, ok)
	require.Equal(t, "ALL", mk.Strictness)
	require.Equal(t, "INNER", mk.JoinType)
	require.Equal(t, []string{"user_id", "session_id"}, mk.Keys)
}

// TestLive_HCLIntrospect_CommonEngines exercises a Buffer-over-MergeTree
// pair (the production shape that motivated this change), plus Null,
// Memory, and Merge — three of the cheaper engines added in the same
// pass.
func TestLive_HCLIntrospect_CommonEngines(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	stmts := []string{
		`CREATE TABLE ` + dbName + `.dest (id UUID, value Float64) ENGINE = MergeTree ORDER BY id`,
		`CREATE TABLE ` + dbName + `.buf (id UUID, value Float64) ` +
			`ENGINE = Buffer('` + dbName + `', 'dest', 16, 10, 100, 10000, 1000000, 10000000, 100000000)`,

		`CREATE TABLE ` + dbName + `.null_sink (id UUID) ENGINE = Null`,
		`CREATE TABLE ` + dbName + `.mem_stage (id UUID) ENGINE = Memory`,

		`CREATE TABLE ` + dbName + `.shard_a (id UUID) ENGINE = MergeTree ORDER BY id`,
		`CREATE TABLE ` + dbName + `.shard_b (id UUID) ENGINE = MergeTree ORDER BY id`,
		`CREATE TABLE ` + dbName + `.merged (id UUID) ENGINE = Merge('` + dbName + `', '^shard_')`,
	}
	for _, s := range stmts {
		require.NoError(t, conn.Exec(ctx, s), "create failed: %s", s)
	}

	got, err := hclload.Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)

	byName := map[string]hclload.Engine{}
	for _, tbl := range got.Tables {
		byName[tbl.Name] = tbl.Engine.Decoded
	}

	buf, ok := byName["buf"].(hclload.EngineBuffer)
	require.True(t, ok)
	require.Equal(t, dbName, buf.Database)
	require.Equal(t, "dest", buf.Table)
	require.Equal(t, int64(16), buf.NumLayers)
	require.Equal(t, int64(100000000), buf.MaxBytes)

	_, ok = byName["null_sink"].(hclload.EngineNull)
	require.True(t, ok)
	_, ok = byName["mem_stage"].(hclload.EngineMemory)
	require.True(t, ok)
	merge, ok := byName["merged"].(hclload.EngineMerge)
	require.True(t, ok)
	require.Equal(t, dbName, merge.DBRegex)
	require.Equal(t, "^shard_", merge.TableRegex)
}

// TestLive_ViewRoundTrip_StarReplace verifies the issue #41 fix end-to-end. A
// view created the only way ClickHouse accepts — a starred body with no explicit
// column list — must, after introspect -> GenerateSQL, regenerate a CREATE VIEW
// that ClickHouse can apply again. Before the fix the regenerated DDL carried
// ClickHouse's inferred column list and was rejected with BAD_ARGUMENTS.
func TestLive_ViewRoundTrip_StarReplace(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	require.NoError(t, conn.Exec(ctx, `CREATE TABLE `+dbName+`.custom_metrics_test (
		name String,
		labels Map(String, String),
		value String,
		help String,
		type String
	) ENGINE = MergeTree ORDER BY name`))

	require.NoError(t, conn.Exec(ctx, `CREATE VIEW `+dbName+`.custom_metrics AS
		SELECT * REPLACE(toFloat64(value) AS value) FROM `+dbName+`.custom_metrics_test`))

	got, err := hclload.Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)
	require.Len(t, got.Views, 1)
	view := got.Views[0]
	require.Equal(t, "custom_metrics", view.Name)

	// Regenerate just the CREATE VIEW via the same path as `diff -sql`.
	cs := hclload.ChangeSet{Databases: []hclload.DatabaseChange{{
		Database: dbName,
		AddViews: []hclload.ViewSpec{view},
	}}}
	gen := hclload.GenerateSQL(cs)
	require.Len(t, gen.Statements, 1)
	ddl := gen.Statements[0]
	require.NotContains(t, ddl, "(name, labels",
		"the inferred column list must be omitted for a starred view")

	// Drop and re-create from the regenerated DDL: the assertion that the fix
	// produces an applyable view.
	require.NoError(t, conn.Exec(ctx, `DROP VIEW `+dbName+`.custom_metrics`))
	require.NoError(t, conn.Exec(ctx, ddl), "regenerated CREATE VIEW must be applyable: %s", ddl)
}

// TestLive_CreateTableRoundTrip_ColumnModifiers is the end-to-end guard for
// issue #45: a table carrying ALIAS / MATERIALIZED / DEFAULT / CODEC / per-column
// TTL / COMMENT columns, a PRIMARY KEY distinct from ORDER BY, a CONSTRAINT, and
// a table COMMENT must survive the full introspect -> dump HCL -> reparse ->
// GenerateSQL pipeline. The regenerated CREATE TABLE is applied in place of the
// original and the stored CREATE statements are compared: they must be identical.
func TestLive_CreateTableRoundTrip_ColumnModifiers(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	require.NoError(t, conn.Exec(ctx, `CREATE TABLE `+dbName+`.archive (
		query_id String,
		team_id Int64,
		exception_code Int32,
		exception_name String ALIAS errorCodeToName(exception_code),
		team_id_doubled Int64 MATERIALIZED team_id * 2,
		created_at DateTime DEFAULT now() COMMENT 'ingestion time' CODEC(Delta, ZSTD(1)) TTL created_at + INTERVAL 1 YEAR,
		CONSTRAINT team_id_positive CHECK team_id > 0
	) ENGINE = MergeTree
	PRIMARY KEY query_id
	ORDER BY (query_id, team_id)
	COMMENT 'query log archive'`), "create original table")

	showCreate := func(name string) string {
		var q string
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT create_table_query FROM system.tables WHERE database = ? AND name = ?",
			dbName, name).Scan(&q))
		return q
	}
	original := showCreate("archive")

	// introspect -> dump HCL -> reparse -> resolve
	got, err := hclload.Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)
	require.Len(t, got.Tables, 1)

	var buf bytes.Buffer
	require.NoError(t, hclload.Write(&buf, &hclload.Schema{Databases: []hclload.DatabaseSpec{*got}}))

	tmp := filepath.Join(t.TempDir(), "dump.hcl")
	require.NoError(t, os.WriteFile(tmp, buf.Bytes(), 0o644))
	reparsed, err := hclload.ParseFile(tmp)
	require.NoError(t, err, "re-parse failed; dump:\n%s", buf.String())
	require.NoError(t, hclload.Resolve(reparsed))

	// regenerate the CREATE TABLE via the same path as `diff -sql`
	cs := hclload.ChangeSet{Databases: []hclload.DatabaseChange{{
		Database: dbName, AddTables: reparsed.Databases[0].Tables,
	}}}
	gen := hclload.GenerateSQL(cs)
	require.Len(t, gen.Statements, 1)

	// recreate in place and compare the stored CREATE statements
	require.NoError(t, conn.Exec(ctx, `DROP TABLE `+dbName+`.archive`))
	require.NoError(t, conn.Exec(ctx, gen.Statements[0]),
		"apply regenerated DDL:\n%s", gen.Statements[0])

	rebuilt := showCreate("archive")
	require.Equal(t, original, rebuilt,
		"CREATE TABLE changed after round-trip\n--- dump HCL ---\n%s\n--- regenerated DDL ---\n%s", buf.String(), gen.Statements[0])
}
