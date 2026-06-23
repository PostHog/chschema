package config

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ClickHouseConfig holds the connection configuration for ClickHouse.
type ClickHouseConfig struct {
	Host          string
	Port          int
	Database      string
	User          string
	Password      string
	Secure        bool // connect over native TLS (port 9440 by convention)
	TLSSkipVerify bool // skip server certificate verification (internal-CA clusters)

	// ShowSecrets enables the format_display_secrets_in_show_and_select session
	// setting so create_table_query / SHOW CREATE / system.named_collections
	// return real secret values (passwords, broker lists) instead of the
	// redacted "[HIDDEN]" placeholder. It only takes effect when the server is
	// configured with display_secrets_in_show_and_select = 1 and the connecting
	// user holds the displaySecretsInShowAndSelect privilege; otherwise values
	// stay redacted. Off by default so secrets are never captured into a dump
	// unintentionally.
	ShowSecrets bool
}

// GetDefaultConfig returns default configuration based on environment.
func GetDefaultConfig() ClickHouseConfig {
	return ClickHouseConfig{
		Host:          getEnvOrDefault("CLICKHOUSE_HOST", "localhost"),
		Port:          getEnvIntOrDefault("CLICKHOUSE_PORT", 9000),
		Database:      getEnvOrDefault("CLICKHOUSE_DB", "migration_test"),
		User:          getEnvOrDefault("CLICKHOUSE_USER", "user1"),
		Password:      getEnvOrDefault("CLICKHOUSE_PASSWORD", "pass1"),
		Secure:        getEnvBoolOrDefault("CLICKHOUSE_SECURE", false),
		TLSSkipVerify: getEnvBoolOrDefault("CLICKHOUSE_TLS_SKIP_VERIFY", false),
	}
}

// NewConnection creates a new ClickHouse connection from the config.
func NewConnection(cfg ClickHouseConfig) (driver.Conn, error) {
	conn, err := clickhouse.Open(buildOptions(cfg))
	if err != nil {
		return nil, err
	}

	// Test the connection.
	ctx := context.Background()
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}

	return conn, nil
}

// buildOptions translates a ClickHouseConfig into clickhouse-go options.
// Extracted so the TLS branch can be unit-tested without dialing.
func buildOptions(cfg ClickHouseConfig) *clickhouse.Options {
	opts := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
	}
	if cfg.Secure {
		opts.TLS = &tls.Config{
			InsecureSkipVerify: cfg.TLSSkipVerify, //nolint:gosec // opted in via -tls-skip-verify
		}
	}
	if cfg.ShowSecrets {
		// Session-level format setting; the server config + grant still gate
		// whether secrets are actually revealed.
		opts.Settings = clickhouse.Settings{"format_display_secrets_in_show_and_select": 1}
	}
	return opts
}

// Helper functions for environment variables.
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

// getEnvBoolOrDefault interprets common boolean string spellings. Unknown
// values fall back to defaultValue rather than failing — the caller can
// still flip via flag.
func getEnvBoolOrDefault(key string, defaultValue bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return defaultValue
	}
}
