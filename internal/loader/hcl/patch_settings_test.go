package hcl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writePatchLayer writes content to root/rel, creating parent dirs, and
// returns the layer directory.
func writePatchLayer(t *testing.T, root, rel, content string) string {
	t.Helper()
	path := filepath.Join(root, rel)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return filepath.Dir(path)
}

// The issue #152 scenario: the table is declared once in the shared layer,
// and the env layer's whole delta is a one-key settings patch. The base
// setting is kept and the patched key added.
func TestPatchTable_SettingsMergeIntoTarget(t *testing.T) {
	root := t.TempDir()
	shared := writePatchLayer(t, root, "cloud/tables.hcl", `
database "posthog" {
  table "adhoc_events_deletion" {
    order_by = ["id"]
    settings = { index_granularity = "8192" }
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
  }
}`)
	env := writePatchLayer(t, root, "prod-us/patch.hcl", `
database "posthog" {
  patch_table "adhoc_events_deletion" {
    settings = { default_compression_codec = "lz4" }
  }
}`)

	s, err := LoadLayers([]string{shared, env})
	require.NoError(t, err)
	require.NoError(t, Resolve(s))

	tbl := s.Databases[0].Tables[0]
	assert.Equal(t, map[string]string{
		"index_granularity":         "8192",
		"default_compression_codec": "lz4",
	}, tbl.Settings, "the base setting is kept and the patch key added")
}

// A patched key that already exists on the target wins — env overlays retune
// base settings.
func TestPatchTable_SettingsPatchWinsOnCollision(t *testing.T) {
	db := DatabaseSpec{
		Name: "posthog",
		Tables: []TableSpec{{
			Name:     "t",
			Settings: map[string]string{"index_granularity": "8192", "ttl_only_drop_parts": "1"},
		}},
		Patches: []PatchTableSpec{{
			Name:     "t",
			Settings: map[string]string{"index_granularity": "4096"},
		}},
	}
	require.NoError(t, applyPatches(&db))
	assert.Equal(t, map[string]string{
		"index_granularity":   "4096",
		"ttl_only_drop_parts": "1",
	}, db.Tables[0].Settings)
}

// Patches accumulate in layer order; on the same key the later patch wins —
// the same precedence layers already have.
func TestPatchTable_SettingsLaterPatchWins(t *testing.T) {
	db := DatabaseSpec{
		Name:   "posthog",
		Tables: []TableSpec{{Name: "t"}},
		Patches: []PatchTableSpec{
			{Name: "t", Settings: map[string]string{"default_compression_codec": "zstd", "always_fetch_merged_part": "1"}},
			{Name: "t", Settings: map[string]string{"default_compression_codec": "lz4"}},
		},
	}
	require.NoError(t, applyPatches(&db))
	assert.Equal(t, map[string]string{
		"default_compression_codec": "lz4",
		"always_fetch_merged_part":  "1",
	}, db.Tables[0].Settings, "a settings-only patch also works on a target with no settings map")
}

// A single patch may carry both a column addition and settings; the column
// rule stays strictly additive.
func TestPatchTable_ColumnsAndSettingsTogether(t *testing.T) {
	db := DatabaseSpec{
		Name: "posthog",
		Tables: []TableSpec{{
			Name:    "t",
			Columns: []ColumnSpec{{Name: "id", Type: "UInt64"}},
		}},
		Patches: []PatchTableSpec{{
			Name:     "t",
			Columns:  []ColumnSpec{{Name: "team_id", Type: "UInt32"}},
			Settings: map[string]string{"default_compression_codec": "lz4"},
		}},
	}
	require.NoError(t, applyPatches(&db))
	require.Len(t, db.Tables[0].Columns, 2)
	assert.Equal(t, "team_id", db.Tables[0].Columns[1].Name)
	assert.Equal(t, map[string]string{"default_compression_codec": "lz4"}, db.Tables[0].Settings)

	dup := DatabaseSpec{
		Name:    "posthog",
		Tables:  []TableSpec{{Name: "t", Columns: []ColumnSpec{{Name: "id", Type: "UInt64"}}}},
		Patches: []PatchTableSpec{{Name: "t", Columns: []ColumnSpec{{Name: "id", Type: "UInt64"}}}},
	}
	require.ErrorContains(t, applyPatches(&dup), `column "id" already exists`)
}

// A settings patch flows through extend like any other pre-resolution
// mutation: patching the abstract base before resolution reaches children
// that don't set their own settings.
func TestPatchTable_SettingsOnAbstractBaseFlowToChild(t *testing.T) {
	root := t.TempDir()
	layer := writePatchLayer(t, root, "l/schema.hcl", `
database "posthog" {
  table "base" {
    abstract = true
    settings = { index_granularity = "8192" }
    column "id" { type = "UInt64" }
  }

  table "child" {
    extend   = "base"
    order_by = ["id"]
    engine "merge_tree" {}
  }

  patch_table "base" {
    settings = { default_compression_codec = "lz4" }
  }
}`)

	s, err := LoadLayers([]string{layer})
	require.NoError(t, err)
	require.NoError(t, Resolve(s))

	require.Len(t, s.Databases[0].Tables, 1, "the abstract base is dropped")
	assert.Equal(t, map[string]string{
		"index_granularity":         "8192",
		"default_compression_codec": "lz4",
	}, s.Databases[0].Tables[0].Settings)
}
