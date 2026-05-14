package hcl

import (
	"context"
	"fmt"
	"testing"

	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCHLive_MaterializedViewRoundTrip creates a source table, a destination
// table, and a TO-form materialized view (with an explicit column list) in a
// real ClickHouse instance, introspects the database back via Introspect, and
// asserts the MV round-trips correctly. Gated by the -clickhouse flag.
func TestCHLive_MaterializedViewRoundTrip(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	// Create source table.
	require.NoError(t,
		conn.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE %s.src (`team_id` Int64, `category` String) ENGINE = MergeTree ORDER BY team_id",
			dbName,
		)),
		"CREATE TABLE src rejected",
	)

	// Create destination table.
	require.NoError(t,
		conn.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE %s.dest (`team_id` Int64, `category` String) ENGINE = MergeTree ORDER BY team_id",
			dbName,
		)),
		"CREATE TABLE dest rejected",
	)

	// Create TO-form materialized view with an explicit column list.
	require.NoError(t,
		conn.Exec(ctx, fmt.Sprintf(
			"CREATE MATERIALIZED VIEW %s.metrics_mv TO %s.dest (`team_id` Int64, `category` String) AS SELECT team_id, category FROM %s.src",
			dbName, dbName, dbName,
		)),
		"CREATE MATERIALIZED VIEW metrics_mv rejected",
	)

	// Introspect the database.
	db, err := Introspect(ctx, conn, dbName)
	require.NoError(t, err)

	// Find the materialized view.
	require.Len(t, db.MaterializedViews, 1)
	mv := db.MaterializedViews[0]
	require.NotNil(t, &mv, "introspected schema has no materialized view %q", "metrics_mv")
	assert.Equal(t, "metrics_mv", mv.Name)

	// Assert ToTable.
	assert.Equal(t, dbName+".dest", mv.ToTable)

	// Assert Columns.
	assert.Equal(t, []ColumnSpec{
		{Name: "team_id", Type: "Int64"},
		{Name: "category", Type: "String"},
	}, mv.Columns)

	// Assert Query contains the expected identifiers.
	assert.Contains(t, mv.Query, "SELECT")
	assert.Contains(t, mv.Query, "src")

	// The source and destination tables still introspect alongside the MV.
	assert.Len(t, db.Tables, 2, "expected src and dest tables to be introspected")
}
