package test

import (
	"context"
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

	got, err := hclload.Introspect(ctx, conn, dbName)
	require.NoError(t, err)

	want := &hclload.DatabaseSpec{
		Name: dbName,
		Tables: []hclload.TableSpec{
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

	got, err := hclload.Introspect(ctx, conn, dbName)
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
