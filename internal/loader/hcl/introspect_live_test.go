package hcl

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCHLive_Introspect drives every non-skipLive createTableCase through
// the full round-trip:
//
//	HCL → resolve → SQL → exec on CH → introspect → compare.
//
// The introspected TableSpec is expected to match the HCL-resolved one for
// every field the live ClickHouse can faithfully echo back. Fields not yet
// covered by introspection (constraints, cluster) are nulled out on the
// expected side before comparison.
func TestCHLive_Introspect(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	for _, tc := range createTableCases {
		tc := tc
		if tc.skipLive || tc.skipIntrospect {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() {
				_ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.t", dbName))
			})

			src := strings.Replace(tc.hcl, `database "db"`, fmt.Sprintf(`database %q`, dbName), 1)

			// 1. Parse + resolve to get the expected, post-resolution table.
			parsed := mustParseResolve(t, src)
			require.Len(t, parsed.Tables, 1)
			expected := parsed.Tables[0]

			// 2. Create the table in CH via generated DDL.
			sql := parseAndGenerate(t, src)
			require.NoError(t, conn.Exec(ctx, sql), "CREATE TABLE rejected:\n%s", sql)

			// 3. Introspect back.
			db, err := Introspect(ctx, conn, dbName)
			require.NoError(t, err)
			var got *TableSpec
			for i := range db.Tables {
				if db.Tables[i].Name == "t" {
					got = &db.Tables[i]
					break
				}
			}
			require.NotNil(t, got, "introspected schema has no table %q", "t")

			// 4. Normalize both sides for comparison.
			normalizeForCompare(&expected)
			normalizeForCompare(got)
			alignEphemeralDefaults(&expected, got)
			alignCodecDefaults(&expected, got)
			alignColumnTTLs(&expected, got)
			alignSettings(&expected, got)
			assert.Equal(t, expected, *got)
		})
	}
}

// TestCHLive_IntrospectMaterializedView creates a destination table, a source
// table, and a TO-form materialized view on a live ClickHouse instance, then
// introspects the database and asserts the MV round-trips. It also exercises
// the original bug: introspecting a database that contains a materialized
// view must not fail.
func TestCHLive_IntrospectMaterializedView(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.metrics (team_id Int64, cnt UInt64) ENGINE = MergeTree ORDER BY team_id", dbName)))
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.events (team_id Int64) ENGINE = MergeTree ORDER BY team_id", dbName)))
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE MATERIALIZED VIEW %s.metrics_mv TO %s.metrics "+
			"AS SELECT team_id, count() AS cnt FROM %s.events GROUP BY team_id",
		dbName, dbName, dbName)))

	db, err := Introspect(ctx, conn, dbName)
	require.NoError(t, err, "introspecting a database with a materialized view must not fail")

	require.Len(t, db.MaterializedViews, 1)
	mv := db.MaterializedViews[0]
	assert.Equal(t, "metrics_mv", mv.Name)
	assert.Equal(t, dbName+".metrics", mv.ToTable)
	assert.Contains(t, mv.Query, "team_id")
	assert.Contains(t, mv.Query, "events")

	// The destination and source tables still introspect alongside the MV.
	var tableNames []string
	for _, tbl := range db.Tables {
		tableNames = append(tableNames, tbl.Name)
	}
	assert.ElementsMatch(t, []string{"metrics", "events"}, tableNames)
}

// mustParseResolve parses HCL source from a literal string by writing it to
// a temp file, then runs Resolve, returning the single DatabaseSpec.
func mustParseResolve(t *testing.T, src string) *DatabaseSpec {
	t.Helper()
	dbs, err := parseSource(t, src)
	require.NoError(t, err)
	require.NoError(t, Resolve(dbs))
	require.Len(t, dbs, 1)
	return &dbs[0]
}

func parseSource(t *testing.T, src string) ([]DatabaseSpec, error) {
	t.Helper()
	tmp := t.TempDir() + "/spec.hcl"
	require.NoError(t, os.WriteFile(tmp, []byte(src), 0o644))
	return ParseFile(tmp)
}

// alignColumnTTLs copies the introspected per-column TTL onto expected when
// expected has a TTL set. ClickHouse rewrites interval expressions (e.g.
// `INTERVAL 1 MONTH` → `toIntervalMonth(1)`), so the textual form differs
// from the HCL fixture even though the meaning is identical. Presence is
// what the introspection round-trip asserts.
func alignColumnTTLs(expected, got *TableSpec) {
	byName := make(map[string]*ColumnSpec, len(got.Columns))
	for i := range got.Columns {
		byName[got.Columns[i].Name] = &got.Columns[i]
	}
	for i := range expected.Columns {
		ec := &expected.Columns[i]
		if ec.TTL == nil {
			continue
		}
		if gc, ok := byName[ec.Name]; ok && gc.TTL != nil {
			v := *gc.TTL
			ec.TTL = &v
		}
	}
	// Table-level TTL: same interval-canonicalization issue.
	if expected.TTL != nil && got.TTL != nil {
		v := *got.TTL
		expected.TTL = &v
	}
}

// alignSettings filters the introspected Settings map down to the keys
// expected actually declared. ClickHouse always emits server defaults like
// `index_granularity = 8192` in engine_full even when the user didn't write
// them; this filter keeps the assertion focused on what the HCL specifies.
func alignSettings(expected, got *TableSpec) {
	if got.Settings == nil {
		return
	}
	if len(expected.Settings) == 0 {
		got.Settings = nil
		return
	}
	filtered := make(map[string]string, len(expected.Settings))
	for k, v := range got.Settings {
		if _, ok := expected.Settings[k]; ok {
			filtered[k] = v
		}
	}
	got.Settings = filtered
}

// alignCodecDefaults copies the introspected codec onto the expected side
// whenever the expected has a codec set. ClickHouse fills in default args
// (e.g. ZSTD → ZSTD(1), Delta → Delta(4)) so the introspected text differs
// from the HCL fixture's short form. The test still asserts a codec is
// present and that it parses back; the exact textual form is validated by
// the static TestCH_Column_Codec* tests.
func alignCodecDefaults(expected, got *TableSpec) {
	byName := make(map[string]*ColumnSpec, len(got.Columns))
	for i := range got.Columns {
		byName[got.Columns[i].Name] = &got.Columns[i]
	}
	for i := range expected.Columns {
		ec := &expected.Columns[i]
		if ec.Codec == nil {
			continue
		}
		if gc, ok := byName[ec.Name]; ok && gc.Codec != nil {
			v := *gc.Codec
			ec.Codec = &v
		}
	}
}

// alignEphemeralDefaults handles bare-EPHEMERAL columns: ClickHouse expands
// `EPHEMERAL` with no expression into `defaultValueOfTypeName('Type')`. We
// preserve the introspected form by copying it onto the expected side.
func alignEphemeralDefaults(expected, got *TableSpec) {
	byName := make(map[string]*ColumnSpec, len(got.Columns))
	for i := range got.Columns {
		byName[got.Columns[i].Name] = &got.Columns[i]
	}
	for i := range expected.Columns {
		ec := &expected.Columns[i]
		if ec.Ephemeral == nil || *ec.Ephemeral != "" {
			continue
		}
		if gc, ok := byName[ec.Name]; ok && gc.Ephemeral != nil {
			v := *gc.Ephemeral
			ec.Ephemeral = &v
		}
	}
}

// normalizeForCompare collapses HCL spellings that ClickHouse stores in a
// single canonical form (so the introspector echoes one shape), and nulls
// out fields the introspection layer doesn't (yet) recover from CH.
func normalizeForCompare(t *TableSpec) {
	t.Constraints = nil // not introspected (no system table)
	t.Cluster = nil     // not introspected (cluster topology lives in server config)
	for i := range t.Columns {
		c := &t.Columns[i]
		// nullable = true is stored by CH inside the type itself; canonicalize.
		if c.Nullable && !strings.HasPrefix(c.Type, "Nullable(") {
			c.Type = "Nullable(" + c.Type + ")"
			c.Nullable = false
		}
	}
	if t.Engine != nil {
		t.Engine.Body = nil
	}
}
