package test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/posthog/chschema/gen/chschema_v1"
	"github.com/posthog/chschema/internal/diff"
	"github.com/posthog/chschema/internal/executor"
	"github.com/posthog/chschema/internal/introspection"
	"github.com/posthog/chschema/internal/loader"
	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/require"
)

func select1(t *testing.T, conn driver.Conn) {
	var one uint8
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT 1").Scan(&one))
	require.EqualValues(t, 1, one)
}

func TestLive_BasicConnectivity(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}
	conn := testhelpers.RequireClickHouse(t)

	// Test basic ping
	err := testhelpers.PingClickHouse(conn)
	require.NoError(t, err, "ClickHouse ping should succeed")

	select1(t, conn)
}

func TestLive_DatabaseCreation(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}
	conn := testhelpers.RequireClickHouse(t)

	dbName := testhelpers.CreateTestDatabase(t, conn)
	require.NotEmpty(t, dbName, "Test database name should not be empty")

	query := "SELECT count() FROM system.databases WHERE name = $1"
	var count uint64
	require.NoError(t, conn.QueryRow(context.Background(), query, dbName).Scan(&count), "Database existence query should succeed")
	require.Equal(t, uint64(1), count, "Database should exist")

	select1(t, conn)
}

func TestLive_EndToEnd_SchemaApply(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}

	// 1. Get connection and create test database
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)

	// 2. Load desired schema from YAML files
	schemaDir := filepath.Join("testdata", "live")
	schemaLoader := loader.NewSchemaLoader(schemaDir)
	desiredState, err := schemaLoader.Load()
	require.NoError(t, err, "Failed to load desired schema")

	// Update table database to use test database
	for _, table := range desiredState.Tables {
		table.Database = &dbName
	}

	// 3. Get current state (should be empty - no tables in new database)
	ctx := context.Background()
	introspector := introspection.NewIntrospector(conn)
	currentState, err := introspector.GetCurrentState(ctx)
	require.NoError(t, err, "Failed to introspect current state")

	// 4. Compute diff between empty state and desired state
	differ := diff.NewDiffer()
	plan, err := differ.Plan(desiredState, currentState)
	require.NoError(t, err, "Failed to create diff plan")
	require.NotEmpty(t, plan.Actions, "Plan should have actions to create tables")

	// 5. Execute plan against ClickHouse
	exec := executor.NewExecutor(conn)
	err = exec.Execute(ctx, plan)
	require.NoError(t, err, "Failed to execute plan")

	// 7. Introspect the database again to get actual state
	actualState, err := introspector.GetCurrentState(ctx)
	require.NoError(t, err, "Failed to introspect actual state after apply")

	// 8. Compare actual state with desired state
	// The tables should match
	require.Equal(t, len(desiredState.Tables), len(actualState.Tables),
		"Number of tables should match")

	for _, desiredTable := range desiredState.Tables {
		tableName := desiredTable.Name
		actualTable := chschema_v1.FindTableByName(actualState.Tables, tableName)
		require.NotNil(t, actualTable, "Table %s should exist in actual state", tableName)

		// Compare basic table properties
		require.Equal(t, desiredTable.Name, actualTable.Name, "Table names should match")
		require.Equal(t, *desiredTable.Database, *actualTable.Database, "Database names should match")

		// Compare columns
		require.Equal(t, len(desiredTable.Columns), len(actualTable.Columns),
			"Number of columns should match for table %s", tableName)

		for i, desiredCol := range desiredTable.Columns {
			actualCol := actualTable.Columns[i]
			require.Equal(t, desiredCol.Name, actualCol.Name,
				"Column %d name should match", i)
			require.Equal(t, desiredCol.Type, actualCol.Type,
				"Column %d type should match", i)
		}

		// Compare ORDER BY
		require.Equal(t, desiredTable.OrderBy, actualTable.OrderBy,
			"ORDER BY should match for table %s", tableName)

		// Compare engine
		require.NotNil(t, actualTable.Engine, "Engine should be set for table %s", tableName)
		require.EqualValues(t, desiredTable.Engine, actualTable.Engine,
			"Engine should match for table %s", tableName)

		// TODO: Enable settings comparison once settings introspection is implemented
		// Currently settings introspection is placeholder (see introspector.go:149)
		if len(actualTable.Settings) > 0 {
			require.Equal(t, desiredTable.Settings, actualTable.Settings,
				"Settings should match for table %s", tableName)
		}
	}
}
