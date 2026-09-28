package hcl

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tsDefault268SQL is SHOW CREATE for `CREATE TABLE db.m ENGINE = TimeSeries`
// on ClickHouse 26.8: every default target is spelled out, including the
// RECENT SAMPLES target (new in 26.8) with its PARTITION BY, TTL and SETTINGS.
const tsDefault268SQL = "CREATE TABLE db.m (`metric_name` String, `tags` Map(String, String), " +
	"`time_series` Array(Tuple(DateTime64(3), Float64)), `metric_family` String, `type` String, " +
	"`unit` String, `help` String) ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 345600 " +
	"SAMPLES INNER COLUMNS (`id` Tuple(UInt64, UUID), `timestamp` DateTime64(3) CODEC(DoubleDelta, ZSTD(1)), `value` Float64 CODEC(ZSTD(3))) " +
	"SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS index_granularity = 32768 " +
	"TAGS INNER COLUMNS (`id` Tuple(UInt64, UUID) DEFAULT tuple(sipHash64(metric_name), reinterpretAsUUID(sipHash128(tags))), " +
	"`metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String), " +
	"`min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))), `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))) " +
	"TAGS INNER ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY tuple(metric_name, id) " +
	"SETTINGS index_granularity = 8192, allow_dimensions_outside_sorting_key = 1 " +
	"METRICS INNER COLUMNS (`metric_family_name` String, `type` LowCardinality(String), `unit` LowCardinality(String), `help` String) " +
	"METRICS INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name " +
	"RECENT SAMPLES INNER COLUMNS (`id` Tuple(UInt64, UUID), `timestamp` DateTime64(3) CODEC(DoubleDelta, ZSTD(1)), `value` Float64 CODEC(ZSTD(3))) " +
	"RECENT SAMPLES INNER ENGINE = MergeTree PARTITION BY toStartOfInterval(toDateTime(timestamp), toIntervalHour(5)) " +
	"ORDER BY (id, timestamp) TTL toDateTime(timestamp) + toIntervalSecond(345600) " +
	"SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1"

func tsIntrospect(t *testing.T, sql string) TableSpec {
	t.Helper()
	db := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRows(db, "db", &fakeRows{rows: []fakeRow{{name: "m", sql: sql}}}))
	require.Len(t, db.Tables, 1)
	return db.Tables[0]
}

func TestIntrospect_TimeSeries_RecentSamples(t *testing.T) {
	tbl := tsIntrospect(t, tsDefault268SQL)
	e := tbl.Engine.Decoded.(EngineTimeSeries)
	assert.Equal(t, "345600", e.Settings["recent_samples_ttl_seconds"])

	require.NotNil(t, e.RecentSamples)
	rs := e.RecentSamples.Inner
	require.NotNil(t, rs)
	require.Len(t, rs.Columns, 3)
	require.NotNil(t, rs.Columns[1].Codec, "inner column codec kept")
	assert.Equal(t, "merge_tree", rs.Engine.Kind)
	assert.Equal(t, []string{"id", "timestamp"}, rs.OrderBy)
	require.NotNil(t, rs.PartitionBy)
	assert.Equal(t, "toStartOfInterval(toDateTime(timestamp), toIntervalHour(5))", *rs.PartitionBy)
	require.NotNil(t, rs.TTL, "the TTL is what expires recent samples; dropping it would keep them forever")
	assert.Equal(t, "toDateTime(timestamp) + toIntervalSecond(345600)", *rs.TTL)
	assert.Equal(t, map[string]string{"index_granularity": "8192", "ttl_only_drop_parts": "1"}, rs.Settings)

	require.NotNil(t, e.Tags.Inner)
	assert.Equal(t, "1", e.Tags.Inner.Settings["allow_dimensions_outside_sorting_key"])
	require.NotNil(t, e.Tags.Inner.Columns[0].Default, "inner column default kept")
}

// The generated CREATE must re-introspect to the same table: every target,
// inner TTL and inner settings survive, and the engine SETTINGS precede the
// targets (after them they would be query-level, and ClickHouse rejects
// recent_samples_ttl_seconds there).
func TestSQLGen_TimeSeries_RecentSamplesRoundTrip(t *testing.T) {
	tbl := tsIntrospect(t, tsDefault268SQL)
	sql := createTableSQL("db", tbl)

	settingsAt := strings.Index(sql, "SETTINGS recent_samples_ttl_seconds")
	require.GreaterOrEqual(t, settingsAt, 0, sql)
	assert.Less(t, settingsAt, strings.Index(sql, " SAMPLES INNER"), sql)
	assert.Contains(t, sql, " RECENT SAMPLES INNER ENGINE = MergeTree")

	again := tsIntrospect(t, sql)
	assert.Equal(t, tbl.Engine.Decoded, again.Engine.Decoded)
}

// A dump of the introspected table must load back unchanged, so the
// recent_samples block and the inner ttl are part of the HCL surface.
func TestDumpLoad_TimeSeries_RecentSamples(t *testing.T) {
	tbl := tsIntrospect(t, tsDefault268SQL)
	live := &Schema{Databases: []DatabaseSpec{{Name: "db", Tables: []TableSpec{tbl}}}}

	var buf strings.Builder
	require.NoError(t, Write(&buf, live))
	flat := rtFlatWS(buf.String())
	assert.Contains(t, flat, "recent_samples {")
	assert.Contains(t, flat, `ttl = "toDateTime(timestamp) + toIntervalSecond(345600)"`)

	path := filepath.Join(t.TempDir(), "dump.hcl")
	require.NoError(t, os.WriteFile(path, []byte(buf.String()), 0o644))
	loaded, err := LoadLayers([]string{path})
	require.NoError(t, err)

	cs := Diff(live, loaded)
	assert.Empty(t, cs.Databases, "dump → load must not drift")
}

// An authored inner TTL in INTERVAL form must compare equal to the
// toInterval*() form ClickHouse stores, like a table TTL does.
func TestCanonicalize_TimeSeries_InnerTTL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.hcl")
	require.NoError(t, os.WriteFile(path, []byte(`database "db" {
  table "m" {
    engine "time_series" {
      recent_samples {
        inner {
          column "id" {
            type = "Tuple(UInt64, UUID)"
          }
          column "timestamp" {
            type = "DateTime64(3)"
          }
          column "value" {
            type = "Float64"
          }
          engine "merge_tree" {}
          order_by = ["id", "timestamp"]
          ttl      = "toDateTime(timestamp) + INTERVAL 4 DAY"
        }
      }
    }
  }
}
`), 0o644))
	loaded, err := LoadLayers([]string{path})
	require.NoError(t, err)
	rs := loaded.Databases[0].Tables[0].Engine.Decoded.(EngineTimeSeries).RecentSamples
	require.NotNil(t, rs)
	require.NotNil(t, rs.Inner.TTL)
	assert.Equal(t, "toDateTime(timestamp) + toIntervalDay(4)", *rs.Inner.TTL)
}
