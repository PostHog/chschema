package hcl

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	chparser "github.com/orian/clickhouse-sql-parser/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// spaceshipDDL is the regression from upstream issue #21. Parser revision
// 0a672f5bb552 panicked on the null-safe equality operator in a column DEFAULT;
// revision 768a69c3d95a parses it natively.
const spaceshipDDL = "CREATE TABLE db.things (`name` String, " +
	"`matches` Bool DEFAULT other_name <=> name, " +
	"`other_name` Nullable(String)) ENGINE = MergeTree ORDER BY name"

func TestSafeParseStmts_RecoversPanic(t *testing.T) {
	stmts, err := recoverParserPanic(func() ([]chparser.Expr, error) {
		panic("synthetic parser failure")
	})
	require.Error(t, err)
	assert.Nil(t, stmts)
	assert.Contains(t, err.Error(), "SQL parser panicked")
	assert.Contains(t, err.Error(), "synthetic parser failure")
}

func TestSafeParseStmts_ParsesNullSafeEquality(t *testing.T) {
	stmts, err := safeParseStmts(spaceshipDDL)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
}

// Every entry point accepting CREATE DDL must use the upgraded parser and
// accept the issue #21 regression syntax.
func TestDDLCallersParseNullSafeEquality(t *testing.T) {
	t.Run("ExtractReferencedTables", func(t *testing.T) {
		_, err := ExtractReferencedTables(spaceshipDDL)
		assert.NoError(t, err)
	})

	t.Run("ExtractDeclaredColumns", func(t *testing.T) {
		_, err := ExtractDeclaredColumns(spaceshipDDL)
		assert.NoError(t, err)
	})

	t.Run("ApplySQL", func(t *testing.T) {
		_, err := ApplySQL(&Schema{}, spaceshipDDL, "db", false)
		assert.NoError(t, err)
	})

	t.Run("parseCreateStatement", func(t *testing.T) {
		_, err := parseCreateStatement(spaceshipDDL)
		assert.NoError(t, err)
	})
}

func TestProcessIntrospectRows_ParsesNullSafeEquality(t *testing.T) {
	newRows := func() *fakeRows {
		return &fakeRows{rows: []fakeRow{
			{name: "events", sql: "CREATE TABLE db.events (`id` UUID) ENGINE = MergeTree ORDER BY id", engine: "MergeTree"},
			{name: "things", sql: spaceshipDDL, engine: "MergeTree"},
		}}
	}

	db := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRowsOpt(db, "db", newRows(), false, nil))
	require.Len(t, db.Tables, 2)
	assert.Empty(t, db.Raws)
}

// Query readers have best-effort and error-returning contracts, but neither
// may panic on syntax the parser does not support.
func TestQueryCallersDegradeOnUnparseableInput(t *testing.T) {
	const unparseable = "SELECT ("

	_, err := extractSourceTables(unparseable)
	assert.Error(t, err)
	assert.False(t, viewQueryProjectsStar(unparseable))
	_, ok := mvVirtualPrefixedRefs(unparseable)
	assert.False(t, ok)
}

// The parser panic surface is not enumerable. Keep all production parser
// calls behind safeParseStmts so future unsupported syntax cannot bypass the
// containment added for upstream issue #21.
func TestNoDirectParserCalls(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, "_test.go") || name == "parse.go" {
			continue
		}
		body, err := os.ReadFile(filepath.Join(".", name))
		require.NoError(t, err)
		checked++
		if strings.Contains(string(body), "chparser.NewParser") {
			assert.Fail(t, "direct SQL parser call in "+name,
				"use safeParseStmts so parser panics become ordinary errors")
		}
	}
	require.NotZero(t, checked, "scanned no source files")
}
