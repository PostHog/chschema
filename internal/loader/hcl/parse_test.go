package hcl

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// spaceshipDDL uses ClickHouse's null-safe equality operator in a column
// DEFAULT. Parser revision 0a672f5bb552 panics in parseTableColumnExpr instead
// of returning its unsupported-syntax error (upstream issue #21).
const spaceshipDDL = "CREATE TABLE db.things (`name` String, " +
	"`matches` Bool DEFAULT other_name <=> name, " +
	"`other_name` Nullable(String)) ENGINE = MergeTree ORDER BY name"

func TestSafeParseStmts_RecoversPanic(t *testing.T) {
	stmts, err := safeParseStmts(spaceshipDDL)
	require.Error(t, err)
	assert.Nil(t, stmts)
	assert.Contains(t, err.Error(), "SQL parser panicked")
}

func TestSafeParseStmts_ParsesNormalSQL(t *testing.T) {
	stmts, err := safeParseStmts("CREATE TABLE db.t (`id` UUID) ENGINE = MergeTree ORDER BY id")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
}

// Every entry point accepting CREATE DDL must return an error rather than let
// the third-party parser unwind the process.
func TestSafeParseStmts_DDLCallersSurvivePanic(t *testing.T) {
	t.Run("ExtractReferencedTables", func(t *testing.T) {
		_, err := ExtractReferencedTables(spaceshipDDL)
		assert.Error(t, err)
	})

	t.Run("ExtractDeclaredColumns", func(t *testing.T) {
		_, err := ExtractDeclaredColumns(spaceshipDDL)
		assert.Error(t, err)
	})

	t.Run("ApplySQL", func(t *testing.T) {
		_, err := ApplySQL(&Schema{}, spaceshipDDL, "db", false)
		assert.Error(t, err)
	})

	t.Run("parseCreateStatement", func(t *testing.T) {
		_, err := parseCreateStatement(spaceshipDDL)
		assert.Error(t, err)
	})
}

func TestProcessIntrospectRows_ParserPanicIsRawCapturable(t *testing.T) {
	newRows := func() *fakeRows {
		return &fakeRows{rows: []fakeRow{
			{name: "events", sql: "CREATE TABLE db.events (`id` UUID) ENGINE = MergeTree ORDER BY id", engine: "MergeTree"},
			{name: "things", sql: spaceshipDDL, engine: "MergeTree"},
		}}
	}

	strictErr := processIntrospectRowsOpt(&DatabaseSpec{Name: "db"}, "db", newRows(), false, nil)
	require.Error(t, strictErr)
	assert.Contains(t, strictErr.Error(), "-allow-raw")

	db := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRowsOpt(db, "db", newRows(), true, nil))
	require.Len(t, db.Tables, 1, "the parseable table is still introspected")
	require.Len(t, db.Raws, 1)
	assert.Equal(t, "things", db.Raws[0].Name)
	assert.Equal(t, normalizeRawSQL(spaceshipDDL), db.Raws[0].SQL)
}

// Query readers have best-effort and error-returning contracts, but neither
// may panic on syntax the parser does not support.
func TestQueryCallersDegradeOnUnparseableInput(t *testing.T) {
	const unparseable = "SELECT a <=> b AS matches FROM db.t"

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
