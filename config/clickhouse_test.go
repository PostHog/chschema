package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetDefaultConfig_TLSEnvVars(t *testing.T) {
	t.Run("defaults are false when env unset", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_SECURE", "")
		t.Setenv("CLICKHOUSE_TLS_SKIP_VERIFY", "")
		cfg := GetDefaultConfig()
		require.False(t, cfg.Secure)
		require.False(t, cfg.TLSSkipVerify)
	})

	t.Run("CLICKHOUSE_SECURE=true enables TLS", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_SECURE", "true")
		cfg := GetDefaultConfig()
		require.True(t, cfg.Secure)
	})

	t.Run("CLICKHOUSE_SECURE=1 enables TLS", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_SECURE", "1")
		cfg := GetDefaultConfig()
		require.True(t, cfg.Secure)
	})

	t.Run("CLICKHOUSE_TLS_SKIP_VERIFY=true sets the skip flag", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_TLS_SKIP_VERIFY", "true")
		cfg := GetDefaultConfig()
		require.True(t, cfg.TLSSkipVerify)
	})

	t.Run("garbage values fall back to false", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_SECURE", "banana")
		cfg := GetDefaultConfig()
		require.False(t, cfg.Secure)
	})
}

// guard against accidental import shadowing
var _ = os.Getenv
