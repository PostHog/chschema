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
