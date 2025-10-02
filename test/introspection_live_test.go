package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/posthog/chschema/gen/chschema_v1"
	"github.com/posthog/chschema/internal/introspection"
	"github.com/posthog/chschema/internal/utils"
	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestLive_Introspection_Engine(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}

	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	// Create a simple MergeTree table
	createSQL := `
		CREATE TABLE ` + dbName + `.test_table (
			id UInt64,
			name String,
			age Nullable(UInt8),
			pineapple_on_pizza Bool DEFAULT TRUE
		) ENGINE = MergeTree()
		ORDER BY id
	`
	err := conn.Exec(ctx, createSQL)
	require.NoError(t, err, "Failed to create test table")

	// Introspect the database
	intro := introspection.NewIntrospector(conn)
	state, err := intro.GetCurrentState(ctx)
	require.NoError(t, err, "Failed to introspect database")

	// Check that our table was found
	table := chschema_v1.FindTableByName(state.Tables, "test_table")
	require.NotNil(t, table, "test_table should be found")

	// Build expected table structure
	want := &chschema_v1.Table{
		Name:     "test_table",
		Database: &dbName,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "name", Type: "String"},
			{Name: "age", Type: "Nullable(UInt8)"},
			{Name: "pineapple_on_pizza", Type: "Bool", DefaultExpression: utils.Ptr("true")},
		},
		Engine: &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_MergeTree{
				MergeTree: &chschema_v1.MergeTree{},
			},
		},
		OrderBy: []string{"id"},
	}

	// Compare using protocmp for proper protobuf comparison
	diff := cmp.Diff(want, table, protocmp.Transform())
	require.Empty(t, diff, "Table should match expected structure. Diff:\n%s", diff)
}
