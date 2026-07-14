package hcl

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A layer stack entry may name a single .hcl file instead of a directory, so a
// fine-grained addition (one node's extra table, one shared definition pulled
// into a role) does not need a directory of its own. The tests below cover the
// merge semantics a file layer inherits from a directory layer, plus the two
// ways naming a file can go wrong.

func TestLoadLayers_FileLayerAddsTable(t *testing.T) {
	schema, err := LoadLayers([]string{
		layerPath("file_layer", "base"),
		layerPath("file_layer", "persons.hcl"),
	})
	require.NoError(t, err)
	require.NoError(t, Resolve(schema))
	stripEngineBodies(schema.Databases)

	expected := []DatabaseSpec{
		{
			Name: "posthog",
			Tables: []TableSpec{
				{
					Name:    "events",
					OrderBy: []string{"timestamp", "team_id"},
					Columns: []ColumnSpec{
						{Name: "timestamp", Type: "DateTime"},
						{Name: "team_id", Type: "UInt64"},
					},
					Engine: &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
				},
				{
					Name:    "persons",
					OrderBy: []string{"id"},
					Columns: []ColumnSpec{{Name: "id", Type: "UUID"}},
					Engine:  &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
				},
			},
		},
	}
	assert.Equal(t, expected, schema.Databases, "the file layer's table follows the dir layer's, in stack order")
}

func TestLoadLayers_FileLayerPatchesDirLayerTable(t *testing.T) {
	schema, err := LoadLayers([]string{
		layerPath("file_layer", "base"),
		layerPath("file_layer", "events_patch.hcl"),
	})
	require.NoError(t, err)
	require.NoError(t, Resolve(schema))

	require.Len(t, schema.Databases, 1)
	require.Len(t, schema.Databases[0].Tables, 1)
	assert.Equal(t, []ColumnSpec{
		{Name: "timestamp", Type: "DateTime"},
		{Name: "team_id", Type: "UInt64"},
		{Name: "us_session_id", Type: "String"},
	}, schema.Databases[0].Tables[0].Columns)
}

func TestLoadLayers_FileLayerOverridesDirLayerTable(t *testing.T) {
	schema, err := LoadLayers([]string{
		layerPath("file_layer", "base"),
		layerPath("file_layer", "events_override.hcl"),
	})
	require.NoError(t, err)
	require.NoError(t, Resolve(schema))
	stripEngineBodies(schema.Databases)

	require.Len(t, schema.Databases, 1)
	require.Len(t, schema.Databases[0].Tables, 1)
	tbl := schema.Databases[0].Tables[0]
	assert.Equal(t, "events", tbl.Name)
	assert.Equal(t, []ColumnSpec{{Name: "dev_id", Type: "UInt32"}}, tbl.Columns)
	require.NotNil(t, tbl.Engine)
	assert.Equal(t, EngineLog{}, tbl.Engine.Decoded)
}

// A file layer without override collides with the dir layer's table exactly as
// a second directory layer would: file layers get no special merge treatment.
func TestLoadLayers_FileLayerCollisionWithoutOverrideErrors(t *testing.T) {
	dir := t.TempDir()
	writeLayerFile(t, dir, "events.hcl", `database "posthog" {
  table "events" {
    column "timestamp" { type = "DateTime" }
    engine "merge_tree" {}
    order_by = ["timestamp"]
  }
}
`)
	file := filepath.Join(t.TempDir(), "events_again.hcl")
	writeLayerFile(t, filepath.Dir(file), filepath.Base(file), `database "posthog" {
  table "events" {
    column "id" { type = "UInt64" }
    engine "log" {}
  }
}
`)

	_, err := LoadLayers([]string{dir, file})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "override")
}

func TestLoadLayers_FileLayerRejectsNonHCLFile(t *testing.T) {
	_, err := LoadLayers([]string{
		layerPath("file_layer", "base"),
		layerPath("file_layer", "seed.sql"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "seed.sql")
	assert.Contains(t, err.Error(), "not an .hcl file")
}

func TestLoadLayers_MissingEntryErrors(t *testing.T) {
	_, err := LoadLayers([]string{layerPath("file_layer", "nope.hcl")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nope.hcl", "the error names the offending stack entry")
}

// LayerFiles is exported so cmd/hclexp's web reload fingerprint lists exactly
// the files the loader reads; both layer shapes must round-trip through it.
func TestLayerFiles(t *testing.T) {
	dir := t.TempDir()
	writeLayerFile(t, dir, "b.hcl", "")
	writeLayerFile(t, dir, "a.hcl", "")
	writeLayerFile(t, dir, "notes.txt", "")

	files, err := LayerFiles(dir)
	require.NoError(t, err)
	assert.Equal(t, []string{filepath.Join(dir, "a.hcl"), filepath.Join(dir, "b.hcl")}, files,
		"a directory contributes its *.hcl files in lexical order, nothing else")

	files, err = LayerFiles(filepath.Join(dir, "a.hcl"))
	require.NoError(t, err)
	assert.Equal(t, []string{filepath.Join(dir, "a.hcl")}, files, "a file layer contributes itself")
}
