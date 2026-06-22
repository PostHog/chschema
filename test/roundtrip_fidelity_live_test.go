package test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/require"
)

// TestLive_RoundTripFidelity is the full schema-fidelity guard. Against a local
// ClickHouse it:
//  1. seeds a database from a fixture of CREATE statements,
//  2. captures ClickHouse's canonical CREATE for every object (the golden),
//  3. round-trips the schema through hclexp: Introspect -> dump HCL -> reparse ->
//     Resolve -> GenerateSQL (going through the HCL *text*, where past fidelity
//     bugs lived),
//  4. drops and recreates the database from the generated DDL,
//  5. asserts ClickHouse's canonical CREATE for every object is byte-identical.
//
// The default fixture runs in CI. Point ROUNDTRIP_FIXTURE at a schema captured
// from production with `hclexp dump-sql` to verify it round-trips, locally.
func TestLive_RoundTripFidelity(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}
	conn := testhelpers.RequireClickHouse(t)
	ctx := context.Background()

	path := os.Getenv("ROUNDTRIP_FIXTURE")
	if path == "" {
		path = filepath.Join("testdata", "roundtrip", "schema.sql")
	}
	raw, err := os.ReadFile(path)
	require.NoError(t, err, "read fixture %s", path)

	dbName, stmts := parseSQLFixture(string(raw))
	require.NotEmpty(t, dbName, "fixture must start with a `-- database: <name>` header")
	require.NotEmpty(t, stmts, "fixture has no statements")

	// 1. Seed.
	resetDatabase(t, ctx, conn, dbName)
	t.Cleanup(func() { _ = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName) })
	for _, s := range stmts {
		require.NoError(t, conn.Exec(ctx, s), "seed statement failed:\n%s", s)
	}

	// 2. Golden.
	golden := showCreateAll(t, ctx, conn, dbName)
	require.NotEmpty(t, golden, "no objects after seeding")

	// 3. Round-trip through HCL text.
	got, err := hclload.Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, hclload.Write(&buf, &hclload.Schema{Databases: []hclload.DatabaseSpec{*got}}))

	tmp := filepath.Join(t.TempDir(), "dump.hcl")
	require.NoError(t, os.WriteFile(tmp, buf.Bytes(), 0o644))
	reparsed, err := hclload.ParseFile(tmp)
	require.NoError(t, err, "reparse failed; dumped HCL:\n%s", buf.String())
	require.NoError(t, hclload.Resolve(reparsed))

	gen := hclload.GenerateSQL(hclload.Diff(&hclload.Schema{}, reparsed))
	require.Empty(t, gen.Unsafe, "fresh recreate must have no unsafe changes")
	require.NotEmpty(t, gen.Statements)

	// 4. Recreate.
	resetDatabase(t, ctx, conn, dbName)
	for _, s := range gen.Statements {
		require.NoError(t, conn.Exec(ctx, s), "regenerated statement failed:\n%s", s)
	}

	// 5. Compare.
	rebuilt := showCreateAll(t, ctx, conn, dbName)
	require.Equal(t, golden, rebuilt,
		"schema changed after introspect -> dump -> reparse -> generate\n--- dumped HCL ---\n%s", buf.String())
}

// TestParseSQLFixture validates the fixture parser against the checked-in
// default fixture. It is not gated — it runs in the normal test job.
func TestParseSQLFixture(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "roundtrip", "schema.sql"))
	require.NoError(t, err)

	db, stmts := parseSQLFixture(string(raw))
	require.Equal(t, "roundtrip", db)
	require.Len(t, stmts, 5, "expected 5 objects in the default fixture")
	for _, s := range stmts {
		require.True(t, strings.HasPrefix(s, "CREATE "), "statement should start with CREATE:\n%s", s)
		require.NotContains(t, s, "--", "comment lines must be stripped")
	}
	// Spot-check the modifier-heavy table is intact.
	require.Contains(t, stmts[1], "ALIAS team_id")
	require.Contains(t, stmts[1], "MATERIALIZED 1")
	require.Contains(t, stmts[1], "CONSTRAINT team_id_positive")
}

// parseSQLFixture extracts the `-- database: <name>` header and the individual
// statements (separated by a `;` on its own line) from a dump-sql fixture,
// dropping comment lines.
func parseSQLFixture(content string) (string, []string) {
	var db string
	for _, l := range strings.Split(content, "\n") {
		tl := strings.TrimSpace(l)
		if strings.HasPrefix(tl, "-- database:") {
			db = strings.TrimSpace(strings.TrimPrefix(tl, "-- database:"))
			break
		}
	}

	var stmts []string
	for _, chunk := range strings.Split(content, "\n;\n") {
		var keep []string
		for _, l := range strings.Split(chunk, "\n") {
			if strings.HasPrefix(strings.TrimSpace(l), "--") {
				continue
			}
			keep = append(keep, l)
		}
		if s := strings.TrimSpace(strings.Join(keep, "\n")); s != "" {
			stmts = append(stmts, s)
		}
	}
	return db, stmts
}

// resetDatabase drops and recreates a database so each phase starts clean.
func resetDatabase(t *testing.T, ctx context.Context, conn driver.Conn, db string) {
	t.Helper()
	require.NoError(t, conn.Exec(ctx, "DROP DATABASE IF EXISTS "+db))
	require.NoError(t, conn.Exec(ctx, "CREATE DATABASE "+db))
}

// showCreateAll returns name -> create_table_query for every object in db, the
// ClickHouse-canonical CREATE statement used to compare before and after.
func showCreateAll(t *testing.T, ctx context.Context, conn driver.Conn, db string) map[string]string {
	t.Helper()
	rows, err := conn.Query(ctx,
		"SELECT name, create_table_query FROM system.tables WHERE database = ? AND NOT is_temporary AND create_table_query != ''",
		db)
	require.NoError(t, err)
	defer rows.Close()

	out := map[string]string{}
	for rows.Next() {
		var name, create string
		require.NoError(t, rows.Scan(&name, &create))
		out[name] = create
	}
	require.NoError(t, rows.Err())
	return out
}
