package hcl

import (
	"testing"

	chparser "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// TestChparser_EphemeralCastUnsupported documents a chparser fork
// limitation that the live introspection harness depends on.
// Tracked upstream as https://github.com/orian/clickhouse-sql-parser/issues/1.
// ExtractDeclaredColumns is used to build Null-engine stub tables that
// satisfy MV CREATE-time schema checks; when chparser bails on a real
// PostHog fixture (sharded_events.sql), stubs lose typed columns and
// downstream MV CREATEs fail with type-mismatch errors at aggregate
// expressions like `max(coalesce(inserted_at, now64()))`.
//
// The minimal failing input below reproduces the error. ClickHouse
// itself accepts this CREATE without complaint; only the chparser fork
// rejects it.
//
// Expected behavior once chparser is fixed: ParseStmts returns no
// error and the parsed CreateTable has a column named
// "properties_map_ephemeral" with type "Map(String, String)" and an
// EPHEMERAL CAST(...) clause we can ignore.
//
// Until then, the test harness's extractColumnNamesPermissive fallback
// papers over the issue with Nullable(String) columns — which lets
// most MVs build but not those whose SELECT does typed aggregates over
// columns from sharded_events (e.g. raw_sessions_mv, sessions_mv).
func TestChparser_EphemeralCastUnsupported(t *testing.T) {
	const create = "CREATE TABLE t (" +
		"`properties` String," +
		"`properties_map_ephemeral` Map(String, String) EPHEMERAL CAST(JSONExtractKeysAndValues(properties, 'String'), 'Map(String, String)')" +
		") ENGINE = Memory"

	_, err := chparser.NewParser(create).ParseStmts()
	if err == nil {
		t.Fatalf("expected current chparser fork to fail on EPHEMERAL CAST; got nil error. " +
			"Once chparser handles this, remove the extractColumnNamesPermissive " +
			"fallback in test/integration_live_test.go and re-enable the skipped " +
			"raw_sessions_mv / sessions_mv subtests.")
	}
	t.Logf("known chparser limitation: %v", err)
}
