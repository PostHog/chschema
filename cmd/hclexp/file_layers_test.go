package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every command that takes a layer stack accepts a single .hcl file where a
// directory would go, so a one-table addition needs no directory of its own.
// These tests drive that through the CLI-facing entry points: -left on diff and
// sql2hcl, -layer on load/validate/web, and a manifest role's layers list.

// fileLayerStack writes a directory layer holding "events" plus a standalone
// persons.hcl file layer, and returns the dir and the file.
func fileLayerStack(t *testing.T) (dir, file string) {
	t.Helper()
	root := t.TempDir()

	dir = filepath.Join(root, "base")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "events.hcl"), []byte(`
database "posthog" {
  table "events" {
    order_by = ["timestamp"]
    column "timestamp" { type = "DateTime" }
    engine "merge_tree" {}
  }
}`), 0o600))

	file = filepath.Join(root, "persons.hcl")
	require.NoError(t, os.WriteFile(file, []byte(`
database "posthog" {
  table "persons" {
    order_by = ["id"]
    column "id" { type = "UUID" }
    engine "merge_tree" {}
  }
}`), 0o600))
	return dir, file
}

func tableNames(db hclload.DatabaseSpec) []string {
	names := make([]string, len(db.Tables))
	for i, t := range db.Tables {
		names[i] = t.Name
	}
	return names
}

func TestLoadSide_MixedDirAndFileStack(t *testing.T) {
	dir, file := fileLayerStack(t)

	schema, err := loadSide(dir + "," + file)
	require.NoError(t, err)
	require.Len(t, schema.Databases, 1)
	assert.Equal(t, []string{"events", "persons"}, tableNames(schema.Databases[0]))
}

func TestLoad_MixedDirAndFileStack(t *testing.T) {
	dir, file := fileLayerStack(t)

	schema, err := load("", dir+","+file)
	require.NoError(t, err)
	require.NoError(t, hclload.Resolve(schema))
	require.Len(t, schema.Databases, 1)
	assert.Equal(t, []string{"events", "persons"}, tableNames(schema.Databases[0]))
}

func TestLoadLeft_FileLayerInStack(t *testing.T) {
	dir, file := fileLayerStack(t)

	schema, err := loadLeft(dir + "," + file)
	require.NoError(t, err)
	require.Len(t, schema.Databases, 1)
	assert.Equal(t, []string{"events", "persons"}, tableNames(schema.Databases[0]))
}

// A manifest role's layers = [...] entries may name files, so a role can pull in
// one shared definition without wrapping it in a directory.
func TestComposeManifestRoles_FileLayerEntry(t *testing.T) {
	root := t.TempDir()
	writeLayer(t, root, "base/events.hcl", `
database "posthog" {
  table "events" {
    order_by = ["timestamp"]
    column "timestamp" { type = "DateTime" }
    engine "merge_tree" {}
  }
}`)
	writeLayer(t, root, "shared/persons.hcl", `
database "posthog" {
  table "persons" {
    order_by = ["id"]
    column "id" { type = "UUID" }
    engine "merge_tree" {}
  }
}`)
	manifest := writeTemp(t, "manifest.hcl", `
role "ops" {
  env "prod" { layers = ["base", "shared/persons.hcl"] }
}`)

	roles, err := parseManifest(manifest, "prod")
	require.NoError(t, err)
	composed, err := composeManifestRoles(roles, root)
	require.NoError(t, err)
	require.Len(t, composed, 1)

	require.Equal(t, []string{"base", "shared/persons.hcl"}, composed[0].Layers)
	require.Len(t, composed[0].Schema.Databases, 1)
	assert.Equal(t, []string{"events", "persons"}, tableNames(composed[0].Schema.Databases[0]))
}

// The web reload fingerprint lists exactly what the loader reads, so a stack
// mixing a dir and a file must fingerprint both — and keep noticing files added
// to or removed from the dir.
func TestWeb_SourceFilesWithFileLayer(t *testing.T) {
	dir, file := fileLayerStack(t)

	files, err := sourceFiles("", dir+","+file)
	require.NoError(t, err)
	assert.Equal(t, []string{filepath.Join(dir, "events.hcl"), file}, files)

	fp, err := sourceFingerprint("", dir+","+file)
	require.NoError(t, err)
	assert.Len(t, fp, 2)

	added := filepath.Join(dir, "zz_extra.hcl")
	require.NoError(t, os.WriteFile(added, []byte(`database "other" {}`), 0o600))
	grown, err := sourceFingerprint("", dir+","+file)
	require.NoError(t, err)
	assert.False(t, fingerprintEqual(fp, grown), "a file added to the dir layer changes the fingerprint")
	require.NoError(t, os.Remove(added))
}

// The file layer hot-reloads: editing it makes the next request past the
// throttle window serve the updated schema.
func TestWeb_ReloadOnFileLayerEdit(t *testing.T) {
	dir, file := fileLayerStack(t)
	layers := dir + "," + file

	schema, err := load("", layers)
	require.NoError(t, err)
	require.NoError(t, hclload.Resolve(schema))
	srv, err := newWebServer(schema)
	require.NoError(t, err)

	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	srv.now = func() time.Time { return clock }
	srv.enableReload("", layers, time.Second)

	_, body := getBody(t, srv, "/")
	assert.Contains(t, body, "persons")
	assert.NotContains(t, body, "persons_v2")

	require.NoError(t, os.WriteFile(file, []byte(`
database "posthog" {
  table "persons_v2" {
    order_by = ["id"]
    column "id" { type = "UUID" }
    engine "merge_tree" {}
  }
}`), 0o600))
	future := clock.Add(time.Hour)
	require.NoError(t, os.Chtimes(file, future, future))
	clock = clock.Add(2 * time.Second)

	_, body = getBody(t, srv, "/")
	assert.Contains(t, body, "persons_v2", "an edit to the file layer is picked up")
}
