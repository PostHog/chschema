package hcl

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var deprecatedMergeTreeDeclarations = []struct {
	name string
	decl string
}{
	{name: "MergeTree", decl: "MergeTree(d, (x), 8192)"},
	{name: "AggregatingMergeTree", decl: "AggregatingMergeTree(d, (x), 8192)"},
	{name: "ReplicatedMergeTree", decl: "ReplicatedMergeTree('/p', 'r', d, (x), 8192)"},
	{name: "ReplicatedAggregatingMergeTree", decl: "ReplicatedAggregatingMergeTree('/p', 'r', d, (x), 8192)"},
	{name: "SummingMergeTree", decl: "SummingMergeTree(d, (x), 8192, (v))"},
	{name: "ReplicatedSummingMergeTree", decl: "ReplicatedSummingMergeTree('/p', 'r', d, (x), 8192, (v))"},
}

func TestDeprecatedMergeTreeConstructors_ASTRejectsAllVariants(t *testing.T) {
	for _, tc := range deprecatedMergeTreeDeclarations {
		t.Run(tc.name, func(t *testing.T) {
			sql := "CREATE TABLE db.t (d Date, x UInt64, v UInt64) ENGINE = " + tc.decl
			_, err := buildTableFromCreateSQL(sql)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.name)
			assert.Contains(t, err.Error(), "deprecated MergeTree constructor")
			assert.Contains(t, err.Error(), "refusing to drop")
		})
	}
}

func TestDeprecatedMergeTreeConstructors_LegacyParserRejectsAllVariants(t *testing.T) {
	for _, tc := range deprecatedMergeTreeDeclarations {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseEngineString(tc.decl)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.name)
			assert.Contains(t, err.Error(), "deprecated MergeTree constructor")
			assert.Contains(t, err.Error(), "refusing to drop")
		})
	}
}

func TestDeprecatedMergeTreeConstructors_ModernFormsRemainSupported(t *testing.T) {
	modern := []struct {
		name string
		decl string
	}{
		{name: "MergeTree", decl: "MergeTree()"},
		{name: "AggregatingMergeTree", decl: "AggregatingMergeTree()"},
		{name: "ReplicatedMergeTree", decl: "ReplicatedMergeTree('/p', 'r')"},
		{name: "ReplicatedAggregatingMergeTree", decl: "ReplicatedAggregatingMergeTree('/p', 'r')"},
		{name: "SummingMergeTree", decl: "SummingMergeTree((v, x))"},
		{name: "ReplicatedSummingMergeTree", decl: "ReplicatedSummingMergeTree('/p', 'r', (v, x))"},
	}

	for _, tc := range modern {
		t.Run(tc.name, func(t *testing.T) {
			sql := "CREATE TABLE db.t (d Date, x UInt64, v UInt64) ENGINE = " + tc.decl
			_, err := buildTableFromCreateSQL(sql)
			require.NoError(t, err, "AST parser rejected modern form")
			_, err = ParseEngineString(tc.decl)
			require.NoError(t, err, "legacy parser rejected modern form")
		})
	}
}

func TestDeprecatedMergeTreeConstructors_RawFallbackRoundTrip(t *testing.T) {
	const sql = "CREATE TABLE db.legacy_summing (d Date, x UInt64, v UInt64) ENGINE = SummingMergeTree(d, (x), 8192, (v))"

	strictDB := &DatabaseSpec{Name: "db"}
	strictRows := &fakeRows{rows: []fakeRow{{name: "legacy_summing", sql: sql, engine: "SummingMergeTree"}}}
	err := processIntrospectRowsOpt(strictDB, "db", strictRows, false, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deprecated MergeTree constructor")
	assert.Contains(t, err.Error(), "-allow-raw")
	assert.Empty(t, strictDB.Tables)
	assert.Empty(t, strictDB.Raws)

	rawDB := &DatabaseSpec{Name: "db"}
	rawRows := &fakeRows{rows: []fakeRow{{name: "legacy_summing", sql: sql, engine: "SummingMergeTree"}}}
	require.NoError(t, processIntrospectRowsOpt(rawDB, "db", rawRows, true, nil))
	require.Len(t, rawDB.Raws, 1)
	assert.Equal(t, sql+"\n", rawDB.Raws[0].SQL)

	schema := &Schema{Databases: []DatabaseSpec{*rawDB}}
	var dumped bytes.Buffer
	require.NoError(t, Write(&dumped, schema))
	path := filepath.Join(t.TempDir(), "schema.hcl")
	require.NoError(t, os.WriteFile(path, dumped.Bytes(), 0o600))
	reloaded, err := ParseFile(path)
	require.NoError(t, err, "re-parse failed; dump output:\n%s", dumped.String())
	require.NoError(t, Resolve(reloaded))

	generated := GenerateSQL(Diff(nil, reloaded))
	require.Len(t, generated.Statements, 1)
	assert.Equal(t, sql, generated.Statements[0])
}
