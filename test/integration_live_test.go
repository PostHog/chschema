package test

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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
