package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sql2hclLeft = `database "posthog" {
  table "events" {
    engine "merge_tree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
}
`

// TestApplySQL2HCL_EndToEnd drives the testable core: load a left schema, apply
// a SQL ALTER from a file, and write the updated HCL to a file.
func TestApplySQL2HCL_EndToEnd(t *testing.T) {
	dir := t.TempDir()
	left := filepath.Join(dir, "schema.hcl")
	require.NoError(t, os.WriteFile(left, []byte(sql2hclLeft), 0o600))

	sqlFile := filepath.Join(dir, "change.sql")
	require.NoError(t, os.WriteFile(sqlFile, []byte("ALTER TABLE posthog.events ADD COLUMN ts DateTime;"), 0o600))

	out := filepath.Join(dir, "updated.hcl")
	applied, databases, err := applySQL2HCL(left, sqlFile, out, "", false)
	require.NoError(t, err)
	assert.Equal(t, 1, applied)
	assert.Equal(t, 1, databases)

	got, err := os.ReadFile(out)
	require.NoError(t, err)
	assert.Contains(t, string(got), `column "ts"`, "the added column should be present in the emitted HCL")
	assert.Contains(t, string(got), "DateTime")
}

func TestApplySQL2HCL_PropagatesErrors(t *testing.T) {
	dir := t.TempDir()
	left := filepath.Join(dir, "schema.hcl")
	require.NoError(t, os.WriteFile(left, []byte(sql2hclLeft), 0o600))

	// A data-mutation statement is rejected by ApplySQL (schema DDL only).
	sqlFile := filepath.Join(dir, "bad.sql")
	require.NoError(t, os.WriteFile(sqlFile, []byte("TRUNCATE TABLE posthog.events;"), 0o600))

	_, _, err := applySQL2HCL(left, sqlFile, filepath.Join(dir, "out.hcl"), "", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "apply SQL")
}

func TestLoadLeft_FileVsDirVsLayers(t *testing.T) {
	root := t.TempDir()

	// Single file.
	file := filepath.Join(root, "one.hcl")
	require.NoError(t, os.WriteFile(file, []byte(sql2hclLeft), 0o600))
	s, err := loadLeft(file)
	require.NoError(t, err)
	require.Len(t, s.Databases, 1)
	assert.Equal(t, "posthog", s.Databases[0].Name)

	// Single directory (loaded as one layer).
	layerA := filepath.Join(root, "a")
	require.NoError(t, os.MkdirAll(layerA, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(layerA, "schema.hcl"), []byte(sql2hclLeft), 0o600))
	s, err = loadLeft(layerA)
	require.NoError(t, err)
	require.Len(t, s.Databases, 1)

	// Comma-separated layer dirs (additive patch in a second layer).
	layerB := filepath.Join(root, "b")
	require.NoError(t, os.MkdirAll(layerB, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(layerB, "extra.hcl"), []byte(`database "other" {
  table "t" {
    engine "merge_tree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
}
`), 0o600))
	s, err = loadLeft(layerA + "," + layerB)
	require.NoError(t, err)
	assert.Len(t, s.Databases, 2, "both layer dirs contribute databases")
}

func TestReadSQL_FileAndStdin(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "in.sql")
	require.NoError(t, os.WriteFile(f, []byte("SELECT 1"), 0o600))

	got, err := readSQL(f)
	require.NoError(t, err)
	assert.Equal(t, "SELECT 1", got)

	// "" and "-" read stdin; swap os.Stdin to a temp file.
	stdinFile, err := os.CreateTemp(dir, "stdin")
	require.NoError(t, err)
	_, err = stdinFile.WriteString("DROP TABLE x")
	require.NoError(t, err)
	_, err = stdinFile.Seek(0, 0)
	require.NoError(t, err)
	orig := os.Stdin
	os.Stdin = stdinFile
	defer func() { os.Stdin = orig }()

	got, err = readSQL("-")
	require.NoError(t, err)
	assert.Equal(t, "DROP TABLE x", got)
}
