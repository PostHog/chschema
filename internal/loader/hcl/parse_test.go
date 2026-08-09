package hcl

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// spaceshipDDL uses ClickHouse's `<=>` operator in a column DEFAULT, which the
// third-party parser panics on (nil dereference in parseTableColumnExpr).
//
// It is the only panicking input we know of, and the trigger is the column
// list: the same operator inside a SELECT — including a CREATE VIEW's SELECT —
// produces an ordinary parse error, not a panic. So the callers that take a
// bare query have no demonstrable panic case, and their protection is
// structural: they route through safeParseStmts, which TestNoDirectParserCalls
// enforces. The parser's panic surface is not enumerable, so that rule is the
// guarantee, not this one input.
const spaceshipDDL = "CREATE TABLE db.things (`name` String, " +
	"`matches` Bool DEFAULT other_name <=> name, " +
	"`other_name` Nullable(String)) ENGINE = MergeTree ORDER BY name"

func TestSafeParseStmts_RecoversPanic(t *testing.T) {
	stmts, err := safeParseStmts(spaceshipDDL)
	require.Error(t, err)
	assert.Nil(t, stmts)
	assert.Contains(t, err.Error(), "parser panicked")
}

func TestSafeParseStmts_ParsesNormalSQL(t *testing.T) {
	stmts, err := safeParseStmts("CREATE TABLE db.t (`id` UUID) ENGINE = MergeTree ORDER BY id")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
}

// TestSafeParseStmts_CallersTakingDDLSurvivePanic covers every exported or
// internal entry point that accepts a CREATE statement, which is where the
// known panic reaches. Each must return an error rather than unwind.
func TestSafeParseStmts_CallersTakingDDLSurvivePanic(t *testing.T) {
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

// TestProcessIntrospectRows_ParserPanicIsRawCapturable is the reason the
// recover matters most: a recovered panic rejoins the normal
// unparseable-object path, so -allow-raw captures the object and strict mode
// names the flag. An unrecovered panic would escape both, taking down a whole
// introspection run over one table.
func TestProcessIntrospectRows_ParserPanicIsRawCapturable(t *testing.T) {
	newRows := func() *fakeRows {
		return &fakeRows{rows: []fakeRow{
			{name: "events", sql: "CREATE TABLE db.events (`id` UUID) ENGINE = MergeTree ORDER BY id", engine: "MergeTree"},
			{name: "things", sql: spaceshipDDL, engine: "MergeTree"},
		}}
	}

	strictErr := processIntrospectRowsOpt(&DatabaseSpec{Name: "db"}, "db", newRows(), IntrospectOptions{})
	require.Error(t, strictErr)
	assert.Contains(t, strictErr.Error(), "-allow-raw")

	db := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRowsOpt(db, "db", newRows(), IntrospectOptions{AllowRaw: true}))
	require.Len(t, db.Tables, 1, "the parseable table is still introspected")
	require.Len(t, db.Raws, 1)
	assert.Equal(t, "things", db.Raws[0].Name)
}

// TestQueryCallersDegradeOnUnparseableInput pins the best-effort contract of
// the query readers. viewQueryProjectsStar and mvVirtualPrefixedRefs answer
// "no" on input they cannot parse rather than failing the run — which is why
// an unrecovered panic there would be especially wrong.
func TestQueryCallersDegradeOnUnparseableInput(t *testing.T) {
	const unparseable = "SELECT a <=> b AS matches FROM db.t"

	_, err := extractSourceTables(unparseable)
	assert.Error(t, err)

	assert.False(t, viewQueryProjectsStar(unparseable))

	_, ok := mvVirtualPrefixedRefs(unparseable)
	assert.False(t, ok)
}

// TestNoDirectParserCalls enforces the rule safeParseStmts exists to carry:
// nothing may call the third-party parser directly, because such a call is
// unprotected against a panic. The original bug was exactly this shape — a
// recover on one entry point while eight others went without.
func TestNoDirectParserCalls(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	checked := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, "_test.go") || name == "parse.go" {
			continue
		}
		body, err := os.ReadFile(filepath.Join(".", name))
		require.NoError(t, err)
		checked++
		// Tested with strings.Contains rather than assert.NotContains: the
		// latter prints the whole haystack, and these files run to tens of
		// kilobytes.
		if strings.Contains(string(body), "chparser.NewParser") {
			assert.Fail(t, "direct SQL parser call in "+name,
				"use safeParseStmts so a parser panic becomes an error instead of "+
					"unwinding the run")
		}
	}
	require.NotZero(t, checked, "scanned no source files — the guard would pass vacuously")
}
