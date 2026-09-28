package hcl

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntrospect_TimeSeriesTagsToColumns_EscapedStrings(t *testing.T) {
	sql := `CREATE TABLE default.m
(
    id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    timestamp DateTime64(3),
    value Float64,
    metric_name LowCardinality(String),
    foo_bar String,
    tags Map(LowCardinality(String), String),
    all_tags Map(String, String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3)),
    metric_family_name String,
    type String,
    unit String,
    help String
)
ENGINE = TimeSeries
SETTINGS tags_to_columns = {'foo\'bar':'foo_bar'} DATA
ENGINE = MergeTree
ORDER BY (id, timestamp) TAGS
ENGINE = AggregatingMergeTree
PRIMARY KEY metric_name
ORDER BY tuple(metric_name, id) METRICS
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name`
	db := &DatabaseSpec{Name: "default"}

	require.NoError(t, processIntrospectRows(db, "default", &fakeRows{rows: []fakeRow{{name: "m", sql: sql}}}))
	require.Len(t, db.Tables, 1)
	engine, ok := db.Tables[0].Engine.Decoded.(EngineTimeSeries)
	require.True(t, ok)
	assert.Equal(t, map[string]string{"foo'bar": "foo_bar"}, engine.TagsToColumns)
	assert.Nil(t, engine.Samples)
	assert.Nil(t, engine.Tags)
	assert.Nil(t, engine.Metrics)

	// SHOW CREATE's default targets must not introduce drift from the same
	// table declared without explicit targets, regardless of parser support.
	bareSQL := strings.Split(sql, " DATA\n")[0]
	bare := &DatabaseSpec{Name: "default"}
	require.NoError(t, processIntrospectRows(bare, "default", &fakeRows{rows: []fakeRow{{name: "m", sql: bareSQL}}}))
	changes := Diff(&Schema{Databases: []DatabaseSpec{*bare}}, &Schema{Databases: []DatabaseSpec{*db}})
	assert.Empty(t, changes.Databases)

	// A custom target must not be silently normalized away: an engine-only
	// (shorthand) target is kept as an inner table without columns.
	customSQL := strings.Replace(sql, "ORDER BY (id, timestamp)", "ORDER BY (timestamp, id)", 1)
	custom, err := buildTableFromCreateSQL(customSQL)
	require.NoError(t, err)
	samples := custom.Engine.Decoded.(EngineTimeSeries).Samples
	require.NotNil(t, samples)
	require.NotNil(t, samples.Inner)
	assert.Empty(t, samples.Inner.Columns)
	assert.Equal(t, "merge_tree", samples.Inner.Engine.Kind)
	assert.Equal(t, []string{"timestamp", "id"}, samples.Inner.OrderBy)
}

func TestSQLGen_TimeSeriesTagsToColumns_HCLFirstEscaping(t *testing.T) {
	db := mustParseResolve(t, `
database "default" {
  table "m" {
    engine "time_series" {
      tags_to_columns = {
        "foo'bar\\tag" = "column'value\\path"
      }
    }
  }
}
`)

	generated := GenerateSQL(Diff(nil, &Schema{Databases: []DatabaseSpec{*db}}))
	require.Len(t, generated.Statements, 1)
	assert.NotContains(t, generated.Statements[0], "default.m (")
	assert.Contains(t, generated.Statements[0],
		`tags_to_columns = {'foo\'bar\\tag':'column\'value\\path'}`)
}

func TestRenderTagsToColumnsMap_EscapesKeysAndValues(t *testing.T) {
	got := renderTagsToColumnsMap(map[string]string{
		"foo'bar\\tag": "column'value\\path",
	})

	assert.Equal(t, `{'foo\'bar\\tag':'column\'value\\path'}`, got)
}
