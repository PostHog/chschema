package hcl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests lock the behaviour from #69: the file() function and query
// normalization (#68) must apply on the LoadLayers (-layer) path and through
// Diff, not only on the single-file ParseFile path. #68 only tested ParseFile
// directly, so a regression here would otherwise be silent.

func writeLayerFile(t *testing.T, dir, rel, content string) {
	t.Helper()
	p := filepath.Join(dir, rel)
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
}

// TestLoadLayers_ResolvesFileFunc: a view whose query is file("sql/v.sql")
// loads via LoadLayers (the -layer path) and resolves to the file's normalized
// SQL — not rejected with "Function calls not allowed".
func TestLoadLayers_ResolvesFileFunc(t *testing.T) {
	dir := t.TempDir()
	writeLayerFile(t, dir, "t.hcl", `database "posthog" {
  view "v" {
    query = file("sql/v.sql")
  }
}
`)
	writeLayerFile(t, dir, "sql/v.sql", "SELECT 1 AS a, 2 AS b\n")

	schema, err := LoadLayers([]string{dir})
	require.NoError(t, err)
	require.Len(t, schema.Databases, 1)
	require.Len(t, schema.Databases[0].Views, 1)

	want, ok := normalizeQuery("SELECT 1 AS a, 2 AS b")
	require.True(t, ok)
	assert.Equal(t, want, schema.Databases[0].Views[0].Query,
		"file() must resolve and the SQL be normalized on the LoadLayers path")
}

// TestLoadLayers_NormalizesQuery: a heredoc query and the equivalent one-liner,
// each loaded via LoadLayers, normalize to the same canonical form.
func TestLoadLayers_NormalizesQuery(t *testing.T) {
	heredocDir := t.TempDir()
	writeLayerFile(t, heredocDir, "t.hcl", `database "posthog" {
  view "v" {
    query = <<-SQL
      SELECT a, b
      FROM t
      WHERE x = 1
    SQL
  }
}
`)
	onelinerDir := t.TempDir()
	writeLayerFile(t, onelinerDir, "t.hcl", `database "posthog" {
  view "v" {
    query = "SELECT a, b FROM t WHERE x = 1"
  }
}
`)

	heredoc, err := LoadLayers([]string{heredocDir})
	require.NoError(t, err)
	oneliner, err := LoadLayers([]string{onelinerDir})
	require.NoError(t, err)

	hq := heredoc.Databases[0].Views[0].Query
	oq := oneliner.Databases[0].Views[0].Query
	assert.Equal(t, oq, hq, "heredoc and one-liner must normalize to the same query")
	assert.Contains(t, hq, "\n", "canonical form is multi-line (beautified)")
}

// TestDiff_HeredocVsOneliner_NoDifferences: diffing a heredoc-authored schema
// against the equivalent one-liner (both via LoadLayers) reports no drift.
func TestDiff_HeredocVsOneliner_NoDifferences(t *testing.T) {
	heredocDir := t.TempDir()
	writeLayerFile(t, heredocDir, "t.hcl", `database "posthog" {
  view "v" {
    query = <<-SQL
      SELECT a, b
      FROM t
      WHERE x = 1
    SQL
  }
}
`)
	onelinerDir := t.TempDir()
	writeLayerFile(t, onelinerDir, "t.hcl", `database "posthog" {
  view "v" {
    query = "SELECT a, b FROM t WHERE x = 1"
  }
}
`)

	left, err := LoadLayers([]string{heredocDir})
	require.NoError(t, err)
	right, err := LoadLayers([]string{onelinerDir})
	require.NoError(t, err)

	cs := Diff(left, right)
	assert.True(t, cs.IsEmpty(), "heredoc vs one-liner of the same query must not diff")
}
