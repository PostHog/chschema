package hcl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeQuery_BeautifiesAndIsIdempotent(t *testing.T) {
	got, ok := normalizeQuery("SELECT a, b FROM t WHERE x = 1")
	require.True(t, ok)
	assert.Equal(t, "SELECT a, b\nFROM t\nWHERE x = 1", got)

	again, ok := normalizeQuery(got)
	require.True(t, ok)
	assert.Equal(t, got, again, "normalization must be idempotent")
}

// TestNormalizeExpr_StripsRedundantRootParens covers the scalar-expression
// canonicalizer used for column DEFAULT/MATERIALIZED/ALIAS and index
// expressions. ClickHouse's SHOW CREATE wraps some of these in redundant
// outermost parens; compose stores the authored form. Both must reduce to the
// same string or diff reports phantom drift (issue #136 items 2 and 3).
func TestNormalizeExpr_StripsRedundantRootParens(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"(is_deleted)", "is_deleted"},
		{"is_deleted", "is_deleted"},
		{"((is_deleted))", "is_deleted"},
		// A real MATERIALIZED expression: the outer wrapper is redundant, inner
		// call parens are preserved.
		{"(bitShiftLeft(toUInt64(x), 48) + toUInt64(y))", "bitShiftLeft(toUInt64(x), 48) + toUInt64(y)"},
	}
	for _, c := range cases {
		got, ok := normalizeExpr(c.in)
		require.True(t, ok, "normalizeExpr(%q)", c.in)
		assert.Equal(t, c.want, got, "normalizeExpr(%q)", c.in)

		again, ok := normalizeExpr(got)
		require.True(t, ok)
		assert.Equal(t, got, again, "normalizeExpr must be idempotent for %q", c.in)
	}
}

// TestNormalizeExpr_KeepsMeaningfulParens verifies we never strip parens whose
// removal would change meaning: a tuple, or an inner paren that groups against
// a higher-precedence operator.
func TestNormalizeExpr_KeepsMeaningfulParens(t *testing.T) {
	// Tuple: (a, b) is a two-element ParamExprList, not a redundant wrapper.
	got, ok := normalizeExpr("(a, b)")
	require.True(t, ok)
	assert.Equal(t, "(a, b)", got)

	// Inner precedence group must survive: (a + b) * c != a + b * c.
	got, ok = normalizeExpr("(a + b) * c")
	require.True(t, ok)
	assert.Equal(t, "(a + b) * c", got)
}

// TestNormalizeQuery_StripsRedundantHavingParens is issue #136 item 1: CH's
// SHOW CREATE renders HAVING ((a) AND (b)); the authored form is
// HAVING (a) AND (b). Both must normalize to the same query, including when the
// HAVING sits inside a CTE / subquery SELECT.
func TestNormalizeQuery_StripsRedundantHavingParens(t *testing.T) {
	double, ok := normalizeQuery("SELECT id FROM t GROUP BY id HAVING ((a >= 1) AND (b < 2))")
	require.True(t, ok)
	single, ok := normalizeQuery("SELECT id FROM t GROUP BY id HAVING (a >= 1) AND (b < 2)")
	require.True(t, ok)
	assert.Equal(t, single, double, "redundant outer HAVING parens must canonicalize away")
	assert.NotContains(t, double, "((", "the extra outer paren pair is gone")

	// The HAVING lives inside a subquery — the strip must reach nested SELECTs.
	nested, ok := normalizeQuery("SELECT x FROM (SELECT id AS x FROM t GROUP BY id HAVING ((a >= 1) AND (b < 2)))")
	require.True(t, ok)
	assert.NotContains(t, nested, "((", "nested HAVING is canonicalized too")
}

func TestBeautifySQL(t *testing.T) {
	got, ok := BeautifySQL("CREATE VIEW posthog.v AS SELECT a, b FROM posthog.events WHERE team_id = 1")
	require.True(t, ok)
	assert.Contains(t, got, "\n", "a CREATE VIEW is rendered multi-line")
	assert.Contains(t, got, "CREATE VIEW posthog.v")
	assert.Contains(t, got, "SELECT a, b")
	assert.Contains(t, got, "FROM posthog.events")

	// Idempotent: beautifying already-beautified DDL is stable.
	again, ok := BeautifySQL(got)
	require.True(t, ok)
	assert.Equal(t, got, again)
}

func TestBeautifySQL_UnparseableKeepsRaw(t *testing.T) {
	raw := "this is not valid clickhouse ddl"
	got, ok := BeautifySQL(raw)
	assert.False(t, ok)
	assert.Equal(t, raw, got)
}

func TestNormalizeQuery_UnparseableKeepsRaw(t *testing.T) {
	raw := "this is definitely not valid clickhouse sql"
	got, ok := normalizeQuery(raw)
	assert.False(t, ok)
	assert.Equal(t, raw, got, "an unparseable query is kept verbatim")
}

// TestParseFile_QueryForms_Agree is the anti-drift guarantee: the same logical
// query authored as a one-liner, a heredoc, or via file() all load to the same
// normalized query — so source formatting never shows as drift.
func TestParseFile_QueryForms_Agree(t *testing.T) {
	dir := t.TempDir()

	oneLiner := `database "posthog" {
  materialized_view "mv" {
    to_table = "posthog.dest"
    query    = "SELECT team_id, count() AS n FROM events GROUP BY team_id"
  }
}`
	heredoc := `database "posthog" {
  materialized_view "mv" {
    to_table = "posthog.dest"
    query    = <<-SQL
      SELECT team_id, count() AS n
      FROM events
      GROUP BY team_id
    SQL
  }
}`
	external := `database "posthog" {
  materialized_view "mv" {
    to_table = "posthog.dest"
    query    = file("mv.sql")
  }
}`

	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
		return p
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mv.sql"),
		[]byte("SELECT\n    team_id,\n    count() AS n\nFROM events\nGROUP BY team_id\n"), 0o600))

	queryOf := func(path string) string {
		s, err := ParseFile(path)
		require.NoError(t, err)
		require.Len(t, s.Databases, 1)
		require.Len(t, s.Databases[0].MaterializedViews, 1)
		return s.Databases[0].MaterializedViews[0].Query
	}

	q1 := queryOf(write("one.hcl", oneLiner))
	q2 := queryOf(write("heredoc.hcl", heredoc))
	q3 := queryOf(write("external.hcl", external))

	assert.Equal(t, q1, q2, "heredoc must normalize to the same query as the one-liner")
	assert.Equal(t, q1, q3, "file() must normalize to the same query as the one-liner")
	assert.Contains(t, q1, "\n", "the canonical form is multi-line (beautified)")
}

func TestFileFunc_ResolvesRelativeToHCL_AndErrors(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "q.sql"), []byte("SELECT 1"), 0o600))

	ok := filepath.Join(dir, "ok.hcl")
	require.NoError(t, os.WriteFile(ok, []byte(`database "d" {
  view "v" { query = file("q.sql") }
}`), 0o600))
	s, err := ParseFile(ok)
	require.NoError(t, err)
	assert.Equal(t, "SELECT 1", s.Databases[0].Views[0].Query)

	missing := filepath.Join(dir, "missing.hcl")
	require.NoError(t, os.WriteFile(missing, []byte(`database "d" {
  view "v" { query = file("nope.sql") }
}`), 0o600))
	_, err = ParseFile(missing)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nope.sql")
}

// TestNormalizeTTL_CanonicalizesIntervalAndKeepsMoveRule guards that an authored
// TTL converges to ClickHouse's stored form: INTERVAL literals become
// toInterval<Unit>(n) and the TO VOLUME move rule is preserved, so the authored
// clause matches its live-introspected counterpart and does not diff as drift.
func TestNormalizeTTL_CanonicalizesIntervalAndKeepsMoveRule(t *testing.T) {
	got, ok := normalizeTTL("ts + INTERVAL 7 DAY TO VOLUME 'cold', ts + INTERVAL 90 DAY")
	require.True(t, ok)
	assert.Equal(t, "ts + toIntervalDay(7) TO VOLUME 'cold', ts + toIntervalDay(90)", got)

	again, ok := normalizeTTL(got)
	require.True(t, ok, "normalizeTTL must be idempotent")
	assert.Equal(t, got, again)
}

func TestNormalizeTTL_PreservesIntervalTextInsideQuotedValues(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "where literal",
			in:   "ts + INTERVAL 7 DAY DELETE WHERE reason = 'INTERVAL 90 DAY'",
			want: "ts + toIntervalDay(7) DELETE WHERE reason = 'INTERVAL 90 DAY'",
		},
		{
			name: "volume literal",
			in:   "ts + INTERVAL 7 DAY TO VOLUME 'INTERVAL 90 DAY'",
			want: "ts + toIntervalDay(7) TO VOLUME 'INTERVAL 90 DAY'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := normalizeTTL(tt.in)
			require.True(t, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNormalizeTTL_WhereIntervalSemanticEquivalence(t *testing.T) {
	want := "deleted_at + toIntervalMonth(3) WHERE is_deleted = 1"

	stored, ok := normalizeTTL("deleted_at + toIntervalMonth(3) WHERE is_deleted = 1")
	require.True(t, ok)
	assert.Equal(t, want, stored, "the stored form must preserve the WHERE policy")

	authored, ok := normalizeTTL("deleted_at + INTERVAL 3 MONTH WHERE is_deleted = 1")
	require.True(t, ok)
	assert.Equal(t, want, authored, "canonicalizing INTERVAL must preserve the WHERE policy")

	assert.Equal(t, stored, authored)
}

// TestNormalizeType_CanonicalizesEnumSpacing covers the column-type
// canonicalizer. ClickHouse stores an Enum with spaces around '=', while the
// printer every introspected type is rendered through drops them, so an
// authored Enum8('a' = 1) must reduce to the same string as its introspected
// Enum8('a'=1) or the diff reports a perpetual no-op MODIFY COLUMN (issue #136).
func TestNormalizeType_CanonicalizesEnumSpacing(t *testing.T) {
	authored := "Enum8('a' = 1, 'b' = 2, 'tie' = 3)"
	introspected := "Enum8('a'=1, 'b'=2, 'tie'=3)"

	a, ok := normalizeType(authored)
	require.True(t, ok, "normalizeType(%q)", authored)
	i, ok := normalizeType(introspected)
	require.True(t, ok, "normalizeType(%q)", introspected)
	assert.Equal(t, i, a, "authored and introspected Enum types must canonicalize to the same string")

	again, ok := normalizeType(a)
	require.True(t, ok)
	assert.Equal(t, a, again, "normalizeType must be idempotent")

	// The '=' inside a nested Enum is canonicalized too.
	nested, ok := normalizeType("Array(Enum8('x' = 1, 'y' = 2))")
	require.True(t, ok)
	assert.Equal(t, "Array(Enum8('x'=1, 'y'=2))", nested)
}

// TestNormalizeType_LeavesNonEnumAndUnparseable verifies the canonicalizer is a
// no-op on types with no Enum '=' and keeps the raw text when it can't parse.
func TestNormalizeType_LeavesNonEnumAndUnparseable(t *testing.T) {
	for _, ty := range []string{
		"String",
		"LowCardinality(String)",
		"Nullable(DateTime64(3, 'UTC'))",
		"Decimal(10, 2)",
	} {
		got, ok := normalizeType(ty)
		require.True(t, ok, "normalizeType(%q)", ty)
		assert.Equal(t, ty, got, "non-enum type must be unchanged")
	}

	raw := "Enum8('a' = "
	got, ok := normalizeType(raw)
	assert.False(t, ok, "unparseable type reports ok=false")
	assert.Equal(t, raw, got, "unparseable type keeps raw text")
}

// TestNormalizeType_RejectsInputBeyondTheType pins the silent-truncation guard.
// The canonicalizer parses its input as the column type of a throwaway CREATE
// TABLE, and SQL does not stop at the type: `String DEFAULT 'x'` parses as a
// type *plus* a DEFAULT clause, and a stray `)` lets the input rewrite the rest
// of the statement. Returning just the type would silently drop the remainder —
// turning a malformed declaration into a plausible-looking canonical value and
// erasing a DEFAULT from every statement generated from it. Such input must be
// rejected so the raw text survives and the difference stays visible.
func TestNormalizeType_RejectsInputBeyondTheType(t *testing.T) {
	for _, ty := range []string{
		"String DEFAULT 'x'",        // belongs in the column's `default`
		"String CODEC(ZSTD)",        // belongs in `codec`
		"String COMMENT 'c'",        // belongs in `comment`
		"UInt64 MATERIALIZED a + b", // belongs in `materialized`
		"String, y UInt8",           // two columns, only the first survives
		"String) ENGINE = Log --",   // closes the column list and rewrites the engine
		"Int) ENGINE = MergeTree ORDER BY _x TTL _x + toIntervalDay(1) --", // and the TTL
	} {
		got, ok := normalizeType(ty)
		assert.False(t, ok, "normalizeType(%q) must reject input beyond the type", ty)
		assert.Equal(t, ty, got, "rejected type keeps raw text")
	}
}

// TestNormalizeExpr_RejectsInputBeyondTheExpression is the expression-side
// counterpart: an expression is parsed inside a throwaway SELECT, so trailing
// clauses would be silently dropped rather than kept as visible drift.
func TestNormalizeExpr_RejectsInputBeyondTheExpression(t *testing.T) {
	for _, expr := range []string{
		"x FROM t",
		"x WHERE y > 1",
		"1 AS z",
	} {
		got, ok := normalizeExpr(expr)
		assert.False(t, ok, "normalizeExpr(%q) must reject input beyond the expression", expr)
		assert.Equal(t, expr, got, "rejected expression keeps raw text")
	}

	// The guard must not disturb the paren stripping the expression
	// canonicalizer exists for.
	got, ok := normalizeExpr("(a + b)")
	require.True(t, ok)
	assert.Equal(t, "a + b", got)
}

// TestNormalizeTTL_RejectsInputBeyondTheClause is the TTL-side counterpart. The
// INTERVAL folding and move rules must keep working — the guard compares the
// clause's structure-preserving rendering, not its canonical one.
func TestNormalizeTTL_RejectsInputBeyondTheClause(t *testing.T) {
	trailing := "ts + INTERVAL 1 DAY SETTINGS merge_with_ttl_timeout = 3600"
	got, ok := normalizeTTL(trailing)
	assert.False(t, ok, "a SETTINGS tail must not be silently dropped")
	assert.Equal(t, trailing, got, "rejected TTL keeps raw text")

	moved, ok := normalizeTTL("ts + INTERVAL 7 DAY TO VOLUME 'cold', ts + INTERVAL 90 DAY")
	require.True(t, ok)
	assert.Equal(t, "ts + toIntervalDay(7) TO VOLUME 'cold', ts + toIntervalDay(90)", moved)
}
