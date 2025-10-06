package config

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ClickHouseConfig holds the connection configuration for ClickHouse
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

// NewConnection creates a new ClickHouse connection from the config
func NewConnection(cfg ClickHouseConfig) (driver.Conn, error) {
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, err
	}

	// Test the connection
	ctx := context.Background()
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}

	return conn, nil
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
