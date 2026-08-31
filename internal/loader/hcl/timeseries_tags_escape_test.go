package hcl

import (
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
