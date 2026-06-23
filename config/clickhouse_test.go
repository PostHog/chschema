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

func TestBuildOptions_TLS(t *testing.T) {
	t.Run("plaintext leaves TLS nil", func(t *testing.T) {
		opts := buildOptions(ClickHouseConfig{Host: "h", Port: 9000, User: "u", Database: "d"})
		require.Nil(t, opts.TLS)
	})

	t.Run("Secure=true sets verifying TLS config", func(t *testing.T) {
		opts := buildOptions(ClickHouseConfig{Host: "h", Port: 9440, User: "u", Database: "d", Secure: true})
		require.NotNil(t, opts.TLS)
		require.False(t, opts.TLS.InsecureSkipVerify)
	})

	t.Run("Secure+TLSSkipVerify sets InsecureSkipVerify", func(t *testing.T) {
		opts := buildOptions(ClickHouseConfig{Host: "h", Port: 9440, User: "u", Database: "d", Secure: true, TLSSkipVerify: true})
		require.NotNil(t, opts.TLS)
		require.True(t, opts.TLS.InsecureSkipVerify)
	})

	t.Run("TLSSkipVerify alone is ignored without Secure", func(t *testing.T) {
		// NewConnection / applyTLSFlags reject this combo at the call site;
		// the builder itself just treats Secure as the gate.
		opts := buildOptions(ClickHouseConfig{Host: "h", Port: 9000, User: "u", Database: "d", TLSSkipVerify: true})
		require.Nil(t, opts.TLS)
	})

	t.Run("ShowSecrets off by default", func(t *testing.T) {
		opts := buildOptions(ClickHouseConfig{Host: "h", Port: 9000})
		require.Nil(t, opts.Settings["format_display_secrets_in_show_and_select"])
	})

	t.Run("ShowSecrets sets the session format setting", func(t *testing.T) {
		opts := buildOptions(ClickHouseConfig{Host: "h", Port: 9000, ShowSecrets: true})
		require.Equal(t, 1, opts.Settings["format_display_secrets_in_show_and_select"])
	})
}

// guard against accidental import shadowing
var _ = os.Getenv
