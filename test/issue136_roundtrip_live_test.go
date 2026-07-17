package test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLive_Issue136_IntrospectDiffsCleanAgainstLoad is the acceptance test for
// issue #136: a cluster migrated to a layer must diff clean against that layer.
// It composes a small schema (a MATERIALIZED column, a minmax skip index, and a
// view with a parenthesised HAVING — the constructs ClickHouse's SHOW CREATE
// renders with redundant parentheses), applies it to a live database, then
// introspects and diffs against the loaded layer. Both sides run canonicalize,
// so the round trip must report zero operations.
func TestLive_Issue136_IntrospectDiffsCleanAgainstLoad(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}
	conn := testhelpers.RequireClickHouse(t)
	ctx := context.Background()
	const dbName = "issue136_roundtrip"

	hcl := `database "` + dbName + `" {
  table "t" {
    engine "merge_tree" {}
    order_by = ["a"]
    settings = {
      index_granularity = "8192"
    }

    column "a" { type = "UInt64" }
    column "b" { type = "UInt64" }
    column "x" {
      type         = "UInt64"
      materialized = "a + b"
    }

    index "idx" {
      expr        = "x"
      type        = "minmax"
      granularity = 1
    }
  }

  view "v" {
    query = "SELECT a FROM ` + dbName + `.t GROUP BY a HAVING (a >= 1) AND (max(b) < 2)"
  }
}`
	tmp := filepath.Join(t.TempDir(), "schema.hcl")
	require.NoError(t, os.WriteFile(tmp, []byte(hcl), 0o644))

	loaded, err := hclload.ParseFile(tmp)
	require.NoError(t, err)
	require.NoError(t, hclload.Resolve(loaded))

	// Apply the composed layer to a fresh live database.
	resetDatabase(t, ctx, conn, dbName)
	t.Cleanup(func() { _ = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName) })
	gen := hclload.GenerateSQL(hclload.Diff(&hclload.Schema{}, loaded))
	require.NotEmpty(t, gen.Statements)
	for _, s := range gen.Statements {
		require.NoError(t, conn.Exec(ctx, s), "apply failed:\n%s", s)
	}

	// introspect(apply(layer)) must diff clean against load(layer).
	got, err := hclload.Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)

	cs := hclload.Diff(loaded, &hclload.Schema{Databases: []hclload.DatabaseSpec{*got}})
	assert.True(t, cs.IsEmpty(),
		"introspect(apply(layer)) must diff clean against load(layer) (#136); change set: %+v", cs)
}

// TestLive_Issue136_SystemProxyColumnSubset is the acceptance test for #136
// item 4: the layer declares a Distributed proxy over system.processes with a
// curated column subset, while the live proxy — as if created from the full
// column set on some past server version — carries columns the layer omits.
// The proxy only forwards reads and the server owns system.processes'
// columns, so the extra live column is not drift and the diff must be clean.
func TestLive_Issue136_SystemProxyColumnSubset(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}
	conn := testhelpers.RequireClickHouse(t)
	ctx := context.Background()
	const dbName = "issue136_system_proxy"

	hcl := `database "` + dbName + `" {
  table "distributed_system_processes" {
    engine "distributed" {
      cluster_name    = "posthog"
      remote_database = "system"
      remote_table    = "processes"
    }

    column "query_id" { type = "String" }
    column "user" { type = "String" }
    column "elapsed" { type = "Float64" }
  }
}`
	tmp := filepath.Join(t.TempDir(), "schema.hcl")
	require.NoError(t, os.WriteFile(tmp, []byte(hcl), 0o644))

	loaded, err := hclload.ParseFile(tmp)
	require.NoError(t, err)
	require.NoError(t, hclload.Resolve(loaded))

	resetDatabase(t, ctx, conn, dbName)
	t.Cleanup(func() { _ = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName) })
	gen := hclload.GenerateSQL(hclload.Diff(&hclload.Schema{}, loaded))
	require.NotEmpty(t, gen.Statements)
	for _, s := range gen.Statements {
		require.NoError(t, conn.Exec(ctx, s), "apply failed:\n%s", s)
	}

	// Widen the live proxy with a real system.processes column the layer
	// omits — the state a proxy created via `AS system.processes` on a newer
	// server ends up in.
	require.NoError(t, conn.Exec(ctx,
		"ALTER TABLE "+dbName+".distributed_system_processes ADD COLUMN memory_usage Int64"))

	got, err := hclload.Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)

	cs := hclload.Diff(loaded, &hclload.Schema{Databases: []hclload.DatabaseSpec{*got}})
	assert.True(t, cs.IsEmpty(),
		"a system proxy declaring a column subset must diff clean against the widened live proxy (#136 item 4); change set: %+v", cs)
}
