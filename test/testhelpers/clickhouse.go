package testhelpers

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"unsafe"

	"github.com/posthog/chschema/config"
	"github.com/posthog/chschema/internal/logger"

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

// CreateTestDatabase creates a unique test database for isolation
func CreateTestDatabase(t *testing.T, conn driver.Conn) string {
	dbName := fmt.Sprintf("chschema_test_%s", t.Name())
	// Replace any invalid characters for database names
	dbName = "chschema_test_" + strconv.FormatInt(int64(uintptr(unsafe.Pointer(t))), 36)

	ctx := context.Background()
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)

	err := conn.Exec(ctx, query)
	if err != nil {
		t.Fatalf("Failed to create test database %s: %v", dbName, err)
	}

	// Register cleanup
	t.Cleanup(func() {
		dropQuery := fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName)
		_ = conn.Exec(context.Background(), dropQuery)
	})

	return dbName
}
