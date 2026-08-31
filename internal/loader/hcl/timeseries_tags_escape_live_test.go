package hcl

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var timeSeriesQuotedTags = map[string]string{"foo'bar": "foo_bar"}

func TestCHLive_TimeSeriesTagsToColumnsEscaping_RawSQLFirst(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"allow_experimental_time_series_table": 1,
	}))

	rawCreate := fmt.Sprintf("CREATE TABLE %s.timeseries_quote ENGINE = TimeSeries "+
		"SETTINGS tags_to_columns = {'foo''bar':'foo_bar'}", dbName)
	require.NoError(t, conn.Exec(ctx, rawCreate))

	introspected, err := Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)
	got := findTimeSeriesTable(t, introspected, "timeseries_quote")
	assert.Equal(t, timeSeriesQuotedTags, got.Engine.Decoded.(EngineTimeSeries).TagsToColumns)

	dumped, err := RenderObjectHCL(dbName, KindTable, got.Name, introspected)
	require.NoError(t, err)
	assert.Contains(t, dumped, `"foo'bar" = "foo_bar"`)
	assert.NotContains(t, dumped, `foo\\'bar`)

	schema := &Schema{Databases: []DatabaseSpec{{Name: dbName, Tables: []TableSpec{*got}}}}
	require.NoError(t, Resolve(schema))
	generated := GenerateSQL(Diff(nil, schema))
	require.Len(t, generated.Statements, 1)
	assert.Contains(t, generated.Statements[0],
		`tags_to_columns = {'foo\'bar':'foo_bar'}`)

	require.NoError(t, conn.Exec(ctx, fmt.Sprintf("DROP TABLE %s.timeseries_quote SYNC", dbName)))
	require.NoError(t, conn.Exec(ctx, generated.Statements[0]),
		"generated SQL did not recreate the table:\n%s", generated.Statements[0])

	recreated, err := Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)
	got = findTimeSeriesTable(t, recreated, "timeseries_quote")
	assert.Equal(t, timeSeriesQuotedTags, got.Engine.Decoded.(EngineTimeSeries).TagsToColumns)
}

func TestCHLive_TimeSeriesTagsToColumnsEscaping_HCLFirst(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"allow_experimental_time_series_table": 1,
	}))

	authored := mustParseResolve(t, fmt.Sprintf(`
database %q {
  table "timeseries_quote" {
    engine "time_series" {
      tags_to_columns = {
        "foo'bar" = "foo_bar"
      }
    }
  }
}
`, dbName))
	require.Len(t, authored.Tables, 1)
	assert.Equal(t, timeSeriesQuotedTags,
		authored.Tables[0].Engine.Decoded.(EngineTimeSeries).TagsToColumns)

	schema := &Schema{Databases: []DatabaseSpec{*authored}}
	generated := GenerateSQL(Diff(nil, schema))
	require.Len(t, generated.Statements, 1)
	assert.Contains(t, generated.Statements[0],
		`tags_to_columns = {'foo\'bar':'foo_bar'}`)
	require.NoError(t, conn.Exec(ctx, generated.Statements[0]),
		"generated SQL was rejected:\n%s", generated.Statements[0])

	introspected, err := Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)
	got := findTimeSeriesTable(t, introspected, "timeseries_quote")
	assert.Equal(t, timeSeriesQuotedTags, got.Engine.Decoded.(EngineTimeSeries).TagsToColumns)
}

func findTimeSeriesTable(t *testing.T, database *DatabaseSpec, name string) *TableSpec {
	t.Helper()
	for i := range database.Tables {
		if database.Tables[i].Name != name {
			continue
		}
		_, ok := database.Tables[i].Engine.Decoded.(EngineTimeSeries)
		require.True(t, ok)
		return &database.Tables[i]
	}
	require.FailNow(t, "TimeSeries table not found", name)
	return nil
}
