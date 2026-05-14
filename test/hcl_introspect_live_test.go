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
