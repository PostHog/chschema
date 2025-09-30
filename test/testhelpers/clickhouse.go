package testhelpers

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var (
	testConn     driver.Conn
	testConnOnce sync.Once
	testConnErr  error
)

// ClickHouseConfig holds the connection configuration for tests
type ClickHouseConfig struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
}

// GetDefaultConfig returns default configuration based on docker-compose.yml
func GetDefaultConfig() ClickHouseConfig {
	return ClickHouseConfig{
		Host:     getEnvOrDefault("CLICKHOUSE_HOST", "localhost"),
		Port:     getEnvIntOrDefault("CLICKHOUSE_PORT", 9000),
		Database: getEnvOrDefault("CLICKHOUSE_DB", "migration_test"),
		User:     getEnvOrDefault("CLICKHOUSE_USER", "user1"),
		Password: getEnvOrDefault("CLICKHOUSE_PASSWORD", "pass1"),
	}
}

// GetTestConnection returns a shared ClickHouse connection for tests
// The connection is initialized once and reused across all tests
func GetTestConnection(t *testing.T) driver.Conn {
	testConnOnce.Do(func() {
		config := GetDefaultConfig()

		options := &clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
			Auth: clickhouse.Auth{
				Database: config.Database,
				Username: config.User,
				Password: config.Password,
			},
		}

		testConn, testConnErr = clickhouse.Open(options)
		if testConnErr != nil {
			return
		}

		// Test the connection
		ctx := context.Background()
		testConnErr = testConn.Ping(ctx)
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

// Helper functions for environment variables
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvIntOrDefault(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}
