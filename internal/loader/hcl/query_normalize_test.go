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
