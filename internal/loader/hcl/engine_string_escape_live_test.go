package hcl

import (
	"context"
	"fmt"
	"testing"

	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCHLive_MergeEngineStringEscaping_RawRoundTrip(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	const wantDBRegex = "audit"
	const wantTableRegex = "^foo'bar\\baz$"
	rawCreate := fmt.Sprintf("CREATE TABLE %s.merge_quote (x UInt8) ENGINE = Merge('audit', '^foo\\'bar\\\\baz$')", dbName)
	require.NoError(t, conn.Exec(ctx, rawCreate))

	introspected, err := Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)
	require.Len(t, introspected.Tables, 1)
	got, ok := introspected.Tables[0].Engine.Decoded.(EngineMerge)
	require.True(t, ok)
	assert.Equal(t, EngineMerge{DBRegex: wantDBRegex, TableRegex: wantTableRegex}, got)

	schema := &Schema{Databases: []DatabaseSpec{*introspected}}
	require.NoError(t, Resolve(schema))
	generated := GenerateSQL(Diff(nil, schema))
	require.Len(t, generated.Statements, 1)
	assert.Contains(t, generated.Statements[0], "Merge('audit', '^foo\\'bar\\\\baz$')")

	require.NoError(t, conn.Exec(ctx, fmt.Sprintf("DROP TABLE %s.merge_quote SYNC", dbName)))
	require.NoError(t, conn.Exec(ctx, generated.Statements[0]), "generated SQL did not recreate the table:\n%s", generated.Statements[0])

	recreated, err := Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)
	require.Len(t, recreated.Tables, 1)
	recreatedMerge, ok := recreated.Tables[0].Engine.Decoded.(EngineMerge)
	require.True(t, ok)
	assert.Equal(t, EngineMerge{DBRegex: wantDBRegex, TableRegex: wantTableRegex}, recreatedMerge)
}
