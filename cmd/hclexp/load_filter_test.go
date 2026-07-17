package main

import (
	"path/filepath"
	"sort"
	"testing"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// filterLayer writes a layer holding every object shape a split can lose —
// including the two-label raw "dictionary" block that hand-parsers silently
// dropped (issue #149) — plus a node{} block and a named collection.
func filterLayer(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeLayer(t, root, "layer/all.hcl", `
database "posthog" {
  table "events" {
    order_by = ["uuid"]
    column "uuid" { type = "UUID" }
    engine "merge_tree" {}
  }

  table "person" {
    order_by = ["id"]
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
  }

  materialized_view "events_mv" {
    to_table = "events"
    query    = "SELECT uuid FROM posthog.src"
    column "uuid" { type = "UUID" }
  }

  view "person_view" {
    query = "SELECT id FROM posthog.person"
  }

  dictionary "geo" {
    primary_key = ["id"]
    attribute "id" { type = "UInt64" }
    source "clickhouse" { table = "person" }
    layout "flat" {}
    lifetime { min = 0 }
  }

  raw "dictionary" "legacy_dict" {
    sql = "CREATE DICTIONARY posthog.legacy_dict (id UInt64) PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)"
  }
}

named_collection "kafka_creds" {
  param "user" { value = "u" }
}

node "host-1" {
  macros = { shard = "1" }
}
`)
	return filepath.Join(root, "layer")
}

// objectNames flattens every named object in a schema (all kinds), for
// asserting that a split loses nothing.
func objectNames(s *hclload.Schema) []string {
	var out []string
	for _, db := range s.Databases {
		for _, o := range db.Tables {
			out = append(out, o.Name)
		}
		for _, o := range db.MaterializedViews {
			out = append(out, o.Name)
		}
		for _, o := range db.Views {
			out = append(out, o.Name)
		}
		for _, o := range db.Dictionaries {
			out = append(out, o.Name)
		}
		for _, o := range db.Raws {
			out = append(out, o.Name)
		}
	}
	for _, nc := range s.NamedCollections {
		out = append(out, nc.Name)
	}
	sort.Strings(out)
	return out
}

// loadFiltered loads + resolves the layer and applies the filters, the same
// pipeline runLoad uses.
func loadFiltered(t *testing.T, layer string, filters loadFilters) *hclload.Schema {
	t.Helper()
	schema, err := load("", layer)
	require.NoError(t, err)
	require.NoError(t, hclload.Resolve(schema))
	filters.apply(schema)
	return schema
}

// The issue's layer-surgery invariant: -only L and -exclude-objects L are
// exact complements — together they hold every object once, the raw
// "dictionary" shape included, and both halves keep the database{} wrapper
// and the node{} block.
func TestLoadFilters_SplitIsLossless(t *testing.T) {
	layer := filterLayer(t)
	full := loadFiltered(t, layer, loadFilters{})
	all := objectNames(full)
	require.Len(t, all, 7)

	shared := []string{"events", "events_mv", "legacy_dict"}
	onlyHalf := loadFiltered(t, layer, loadFilters{onlyGlobs: shared})
	exclHalf := loadFiltered(t, layer, loadFilters{excludeGlobs: shared})

	assert.Equal(t, []string{"events", "events_mv", "legacy_dict"}, objectNames(onlyHalf))
	assert.Equal(t, []string{"geo", "kafka_creds", "person", "person_view"}, objectNames(exclHalf))

	union := append(objectNames(onlyHalf), objectNames(exclHalf)...)
	sort.Strings(union)
	assert.Equal(t, all, union, "nothing is dropped between the two halves")

	require.Len(t, onlyHalf.Databases[0].Raws, 1, "the raw dictionary survives -only by name")
	for _, half := range []*hclload.Schema{onlyHalf, exclHalf} {
		require.Len(t, half.Databases, 1, "the database block survives filtering")
		require.Len(t, half.Nodes, 1, "the node block is never filtered")
		assert.Equal(t, "host-1", half.Nodes[0].Name)
	}
}

// -exclude consumes the same config file as diff/drift/plan: patterns plus
// object_types, composable with the ad-hoc globs.
func TestLoadFilters_ExcludeConfig(t *testing.T) {
	layer := filterLayer(t)
	cfg := writeTemp(t, "exclude.hcl", `
exclude {
  patterns     = ["person*"]
  object_types = ["named_collection"]
}`)
	m, err := hclload.LoadExcludeConfig(cfg)
	require.NoError(t, err)

	got := loadFiltered(t, layer, loadFilters{exclude: m, excludeGlobs: []string{"legacy_*"}})
	assert.Equal(t, []string{"events", "events_mv", "geo"}, objectNames(got))
}

// Qualified db.name globs work like exclude patterns everywhere else.
func TestLoadFilters_QualifiedGlobs(t *testing.T) {
	layer := filterLayer(t)
	got := loadFiltered(t, layer, loadFilters{onlyGlobs: []string{"posthog.events*"}})
	assert.Equal(t, []string{"events", "events_mv"}, objectNames(got))
}

// Manifest mode filters each composed role before it is written.
func TestLoadFilters_ManifestMode(t *testing.T) {
	root, manifest := twoRoleManifest(t)
	roles, err := parseManifest(manifest, "dev")
	require.NoError(t, err)
	composed, err := composeManifestRoles(roles, root)
	require.NoError(t, err)

	filters := loadFilters{onlyGlobs: []string{"sharded_*"}}
	for i := range composed {
		filters.apply(composed[i].Schema)
	}

	byRole := map[string][]string{}
	for _, c := range composed {
		byRole[c.Role] = objectNames(c.Schema)
	}
	assert.Empty(t, byRole["data"], "the data role's proxy does not match -only")
	assert.Equal(t, []string{"sharded_web_stats"}, byRole["aux"])
	for _, c := range composed {
		require.Len(t, c.Schema.Databases, 1, "database blocks survive even when emptied")
	}
}
