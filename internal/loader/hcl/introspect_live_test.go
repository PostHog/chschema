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
		if tc.skipLive {
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
			assert.Equal(t, expected, *got)
		})
	}
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
	t.TTL = nil         // table-level TTL not yet introspected
	t.Settings = nil    // settings introspection still TODO
	for i := range t.Columns {
		c := &t.Columns[i]
		c.TTL = nil // per-column TTL not yet introspected
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
