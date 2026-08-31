package hcl

import (
	"context"
	"fmt"
	"testing"

	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dictionaryFileQuotePath = "chschema_audit_file'quote.tsv"

func TestCHLive_DictionaryFilePathEscaping_RawSQLFirst(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	rawCreate := fmt.Sprintf(`CREATE DICTIONARY %s.dict_file_quote
		(id UInt64, value String)
		PRIMARY KEY id
		SOURCE(FILE(PATH 'chschema_audit_file''quote.tsv' FORMAT 'TabSeparated'))
		LAYOUT(FLAT())
		LIFETIME(0)`, dbName)
	require.NoError(t, conn.Exec(ctx, rawCreate))

	introspected, err := Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)
	got := findDictByName(introspected.Dictionaries, "dict_file_quote")
	require.NotNil(t, got)
	assertDictionaryFilePath(t, *got, dictionaryFileQuotePath)

	dumped, err := RenderObjectHCL(dbName, KindDictionary, got.Name, introspected)
	require.NoError(t, err)
	assert.Contains(t, dumped, `path   = "chschema_audit_file'quote.tsv"`)
	assert.NotContains(t, dumped, `file\\'quote`)

	schema := &Schema{Databases: []DatabaseSpec{*introspected}}
	require.NoError(t, Resolve(schema))
	generated := GenerateSQL(Diff(nil, schema))
	require.Len(t, generated.Statements, 1)
	assert.Contains(t, generated.Statements[0], `FILE(PATH 'chschema_audit_file\'quote.tsv' FORMAT 'TabSeparated')`)

	require.NoError(t, conn.Exec(ctx, fmt.Sprintf("DROP DICTIONARY %s.dict_file_quote SYNC", dbName)))
	require.NoError(t, conn.Exec(ctx, generated.Statements[0]),
		"generated SQL did not recreate the dictionary:\n%s", generated.Statements[0])

	recreated, err := Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)
	got = findDictByName(recreated.Dictionaries, "dict_file_quote")
	require.NotNil(t, got)
	assertDictionaryFilePath(t, *got, dictionaryFileQuotePath)
}

func TestCHLive_DictionaryFilePathEscaping_HCLFirst(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	authored := mustParseResolve(t, fmt.Sprintf(`
database %q {
  dictionary "dict_file_quote" {
    primary_key = ["id"]

    attribute "id"    { type = "UInt64" }
    attribute "value" { type = "String" }

    source "file" {
      path   = "chschema_audit_file'quote.tsv"
      format = "TabSeparated"
    }

    layout "flat" {}
    lifetime { min = 0 }
  }
}
`, dbName))
	require.Len(t, authored.Dictionaries, 1)
	assertDictionaryFilePath(t, authored.Dictionaries[0], dictionaryFileQuotePath)

	schema := &Schema{Databases: []DatabaseSpec{*authored}}
	generated := GenerateSQL(Diff(nil, schema))
	require.Len(t, generated.Statements, 1)
	assert.Contains(t, generated.Statements[0], `FILE(PATH 'chschema_audit_file\'quote.tsv' FORMAT 'TabSeparated')`)
	require.NoError(t, conn.Exec(ctx, generated.Statements[0]),
		"generated SQL was rejected:\n%s", generated.Statements[0])

	introspected, err := Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)
	got := findDictByName(introspected.Dictionaries, "dict_file_quote")
	require.NotNil(t, got)
	assertDictionaryFilePath(t, *got, dictionaryFileQuotePath)
}

func assertDictionaryFilePath(t *testing.T, dictionary DictionarySpec, want string) {
	t.Helper()
	require.NotNil(t, dictionary.Source)
	fileSource, ok := dictionary.Source.Decoded.(SourceFile)
	require.True(t, ok)
	assert.Equal(t, want, fileSource.Path)
	assert.Equal(t, "TabSeparated", fileSource.Format)
}
