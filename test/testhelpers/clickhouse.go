package testhelpers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/posthog/chschema/config"
	"github.com/posthog/chschema/internal/logger"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var (
	testConn     driver.Conn
	testConnOnce sync.Once
	testConnErr  error
	loggerOnce   sync.Once
)

// InitTestLogger initializes the logger in test mode (suppresses output)
func InitTestLogger() {
	loggerOnce.Do(func() {
		// Initialize logger at error level for tests to reduce noise
		_ = logger.Init("error", "console", "", true)
	})
}

// GetTestConnection returns a shared ClickHouse connection for tests
// The connection is initialized once and reused across all tests
func GetTestConnection(t *testing.T) driver.Conn {
	// Initialize test logger first
	InitTestLogger()

	testConnOnce.Do(func() {
		cfg := config.GetDefaultConfig()
		testConn, testConnErr = config.NewConnection(cfg)
	})

	if testConnErr != nil {
		t.Skipf("ClickHouse not available: %v", testConnErr)
	}

	return testConn
}

// RequireClickHouse skips the test if ClickHouse is not available
func RequireClickHouse(t *testing.T) driver.Conn {
	return GetTestConnection(t)
}

// PingClickHouse performs a basic connectivity test
func PingClickHouse(conn driver.Conn) error {
	ctx := context.Background()
	return conn.Ping(ctx)
}

// CreateTestDatabase creates a uniquely-named test database and registers
// a t.Cleanup that drops it synchronously.
//
// The unique suffix is 64 random bits from crypto/rand. Earlier versions
// derived the suffix from `unsafe.Pointer(t)`, which is unreliable: Go
// may reuse heap addresses for *testing.T after earlier tests complete,
// so two tests that happen to land on the same address get the same
// dbName. That manifested as intermittent "Replica /clickhouse/<db>/...
// already exists" failures on ReplicatedMergeTree fixtures shared by
// multiple tests (e.g. query_log_archive, sharded_events in
// TestEnd2End vs. TestLive_Introspection_AllStatements) because their
// ZK paths embed dbName.
//
// Cleanup uses DROP DATABASE ... SYNC so the database's ZooKeeper
// metadata is released before the test returns — without SYNC,
// ClickHouse delays the drop by `database_atomic_delay_before_drop_table_sec`
// (default 480s), which leaves Replica entries lingering and racing
// later tests that happen to pick a colliding name.
func CreateTestDatabase(t *testing.T, conn driver.Conn) string {
	var rb [8]byte
	if _, err := rand.Read(rb[:]); err != nil {
		t.Fatalf("CreateTestDatabase: read random suffix: %v", err)
	}
	dbName := "chschema_test_" + hex.EncodeToString(rb[:])

	ctx := context.Background()
	if err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)); err != nil {
		t.Fatalf("Failed to create test database %s: %v", dbName, err)
	}

	t.Cleanup(func() {
		_ = conn.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s SYNC", dbName))
	})

	return dbName
}

func EqualProto(t *testing.T, want, got proto.Message) {
	diff := cmp.Diff(want, got, protocmp.Transform())
	require.Empty(t, diff, "Protos should match. Diff:\n%s", diff)
}
