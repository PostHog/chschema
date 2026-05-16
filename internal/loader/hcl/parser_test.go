package hcl

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ptr[T any](v T) *T { return &v }

func TestParseFile_BasicTable(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "table_basic.hcl"))
	require.NoError(t, err)

	expected := []DatabaseSpec{
		{
			Name: "posthog",
			Tables: []TableSpec{
				{
					Name: "events",
					Columns: []ColumnSpec{
						{Name: "id", Type: "UUID"},
					},
				},
			},
		},
	}
	assert.Equal(t, expected, dbs)
}

func TestParseFile_FullTable(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "table_full.hcl"))
	require.NoError(t, err)
	require.Len(t, dbs, 1)
	require.Len(t, dbs[0].Tables, 1)

	tbl := dbs[0].Tables[0]

	// Engine.Body is an opaque hcl.Body; assert the Decoded value
	// separately, then strip Body so the rest can be compared whole.
	require.NotNil(t, tbl.Engine)
	assert.NotNil(t, tbl.Engine.Body)
	assert.Equal(t, EngineReplicatedMergeTree{
		ZooPath:     "/clickhouse/tables/{shard}/events",
		ReplicaName: "{replica}",
	}, tbl.Engine.Decoded)
	tbl.Engine = &EngineSpec{
		Kind:    tbl.Engine.Kind,
		Decoded: tbl.Engine.Decoded,
	}

	expected := TableSpec{
		Name:        "events",
		Extend:      ptr("_event_base"),
		Abstract:    false,
		Override:    true,
		OrderBy:     []string{"timestamp", "team_id"},
		PartitionBy: ptr("toYYYYMM(timestamp)"),
		SampleBy:    ptr("team_id"),
		TTL:         ptr("timestamp + INTERVAL 2 YEARS"),
		Settings: map[string]string{
			"ttl_only_drop_parts": "1",
			"index_granularity":   "8192",
		},
		Columns: []ColumnSpec{
			{Name: "timestamp", Type: "DateTime"},
			{Name: "team_id", Type: "UInt64"},
			{Name: "event", Type: "String"},
		},
		Indexes: []IndexSpec{
			{Name: "idx_team", Expr: "team_id", Type: "minmax", Granularity: 4},
		},
		Engine: &EngineSpec{
			Kind: "replicated_merge_tree",
			Decoded: EngineReplicatedMergeTree{
				ZooPath:     "/clickhouse/tables/{shard}/events",
				ReplicaName: "{replica}",
			},
		},
	}
	assert.Equal(t, expected, tbl)
}

func TestParseFile_MissingColumnType(t *testing.T) {
	_, err := ParseFile(filepath.Join("testdata", "table_invalid_missing_type.hcl"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type")
}

func TestParseFile_UnknownAttribute(t *testing.T) {
	_, err := ParseFile(filepath.Join("testdata", "table_invalid_unknown_attr.hcl"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not_a_real_attr")
}

func TestParseFile_MaterializedView(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "materialized_view.hcl"))
	require.NoError(t, err)

	expected := []DatabaseSpec{
		{
			Name: "posthog",
			MaterializedViews: []MaterializedViewSpec{
				{
					Name:    "app_metrics_mv",
					ToTable: "default.sharded_app_metrics",
					Query:   "SELECT team_id, category FROM default.kafka_app_metrics",
					Cluster: ptr("posthog"),
					Comment: ptr("rolls metrics up"),
					Columns: []ColumnSpec{
						{Name: "team_id", Type: "Int64"},
						{Name: "category", Type: "LowCardinality(String)"},
					},
				},
			},
		},
	}
	assert.Equal(t, expected, dbs)
}

func TestParseFile_Dictionary(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "dictionary.hcl"))
	require.NoError(t, err)

	expected := []DatabaseSpec{
		{
			Name: "posthog",
			Dictionaries: []DictionarySpec{
				{
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
							Query:    ptr("SELECT currency, start_date, end_date, rate FROM default.exchange_rate"),
							User:     ptr("default"),
							Password: ptr("[HIDDEN]"),
						},
					},
					Layout: &DictionaryLayoutSpec{
						Kind: "complex_key_range_hashed",
						Decoded: LayoutComplexKeyRangeHashed{
							RangeLookupStrategy: ptr("max"),
						},
					},
					Lifetime: &DictionaryLifetime{Min: ptr(int64(3000)), Max: ptr(int64(3600))},
					Range:    &DictionaryRange{Min: "start_date", Max: "end_date"},
					Settings: map[string]string{"format_csv_allow_single_quotes": "1"},
					Cluster:  ptr("posthog"),
					Comment:  ptr("fx rates by date"),
				},
			},
		},
	}

	// Body is an opaque hcl.Body; strip it before equality (mirrors the MV pattern).
	require.Len(t, dbs, 1)
	require.Len(t, dbs[0].Dictionaries, 1)
	d := &dbs[0].Dictionaries[0]
	require.NotNil(t, d.Source)
	require.NotNil(t, d.Layout)
	d.Source = &DictionarySourceSpec{Kind: d.Source.Kind, Decoded: d.Source.Decoded}
	d.Layout = &DictionaryLayoutSpec{Kind: d.Layout.Kind, Decoded: d.Layout.Decoded}

	assert.Equal(t, expected, dbs)
}
