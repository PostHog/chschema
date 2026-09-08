package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// locateTree writes a small manifest + layer tree + dump dir:
//
//	shared/base.hcl      abstract events_base, plain person
//	ingestion/events.hcl events extending events_base
//	aux/dup.hcl          a second plain person (the accidental duplicate)
//	dumps/node1.hcl      a node dump declaring posthog.events
//	dumps/node2.hcl      a dump with no node{} block declaring only_live
//
// manifest.hcl deploys (ingestion, prod-us), (ingestion, prod-eu) on
// shared+ingestion and (aux, prod-us) on shared+aux.
func locateTree(t *testing.T) (root string) {
	t.Helper()
	root = t.TempDir()
	mustWrite := func(rel, content string) {
		t.Helper()
		path := filepath.Join(root, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}

	mustWrite("manifest.hcl", `
role "ingestion" {
  env "prod-us" { layers = ["shared", "ingestion"] }
  env "prod-eu" { layers = ["shared", "ingestion"] }
}
role "aux" {
  env "prod-us" { layers = ["shared", "aux"] }
}
`)
	mustWrite("shared/base.hcl", `
database "posthog" {
  table "events_base" {
    abstract = true
    column "uuid" { type = "UUID" }
  }
  table "person" {
    engine "merge_tree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
}
`)
	mustWrite("ingestion/events.hcl", `
database "posthog" {
  table "events" {
    extend   = "events_base"
    engine "merge_tree" {}
    order_by = ["uuid"]
  }
}
`)
	mustWrite("aux/dup.hcl", `
database "posthog" {
  table "person" {
    engine "merge_tree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
}
`)
	mustWrite("dumps/node1.hcl", `
node "node1" {}
database "posthog" {
  table "events" {
    engine "merge_tree" {}
    order_by = ["uuid"]
    column "uuid" { type = "UUID" }
  }
}
`)
	mustWrite("dumps/node2.hcl", `
database "posthog" {
  table "only_live" {
    engine "merge_tree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
}
`)
	return root
}

func TestLocateFlagsError(t *testing.T) {
	one := []string{"events"}
	several := []string{"events", "person_*"}

	assert.NoError(t, locateFlagsError("m.hcl", "", "", "text", false, one, nil, nil))
	assert.NoError(t, locateFlagsError("", "", "dumps", "json", false, one, nil, nil))
	assert.NoError(t, locateFlagsError("", "a,b", "", "text", false, one, nil, nil), "-layer alone is a source")
	assert.NoError(t, locateFlagsError("m.hcl", "", "", "text", false, several, nil, nil), "several patterns")
	assert.NoError(t, locateFlagsError("m.hcl", "", "", "text", true, nil, nil, nil))
	assert.NoError(t, locateFlagsError("", "a", "", "text", true, nil, nil, nil), "-duplicates audits -layer too")
	assert.NoError(t, locateFlagsError("", "", "dumps", "json", false, nil, []string{"events"}, []string{"id"}), "column mode")

	assert.Error(t, locateFlagsError("", "", "", "text", false, one, nil, nil), "needs a source")
	assert.Error(t, locateFlagsError("m.hcl", "", "", "yaml", false, one, nil, nil), "bad format")
	assert.Error(t, locateFlagsError("m.hcl", "", "", "text", false, nil, nil, nil), "missing name")
	assert.Error(t, locateFlagsError("m.hcl", "", "", "text", false, []string{"a", "[bad"}, nil, nil), "invalid glob among several")
	assert.Error(t, locateFlagsError("", "", "dumps", "text", true, nil, nil, nil), "-duplicates without authored layers")
	assert.Error(t, locateFlagsError("m.hcl", "", "dumps", "text", true, nil, nil, nil), "-duplicates with -dump")
	assert.Error(t, locateFlagsError("m.hcl", "", "", "text", true, one, nil, nil), "-duplicates with name")
	assert.Error(t, locateFlagsError("m.hcl", "", "", "text", false, one, []string{"events"}, []string{"id"}), "columns with positional name")
	assert.Error(t, locateFlagsError("m.hcl", "", "", "text", false, nil, nil, []string{"id"}), "columns require tables")
	assert.Error(t, locateFlagsError("m.hcl", "", "", "text", false, nil, []string{"events"}, nil), "tables require columns")
	assert.Error(t, locateFlagsError("m.hcl", "", "", "text", true, nil, []string{"events"}, []string{"id"}), "duplicates with columns")
	assert.Error(t, locateFlagsError("m.hcl", "", "", "text", false, nil, []string{"[bad"}, []string{"id"}), "bad table glob")
	assert.Error(t, locateFlagsError("m.hcl", "", "", "text", false, nil, []string{"events"}, []string{"[bad"}), "bad column glob")
}

func TestParseManifestAllEnvs(t *testing.T) {
	root := locateTree(t)

	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	want := []locateStack{
		{Role: "ingestion", Env: "prod-us", Layers: []string{"shared", "ingestion"}},
		{Role: "ingestion", Env: "prod-eu", Layers: []string{"shared", "ingestion"}},
		{Role: "aux", Env: "prod-us", Layers: []string{"shared", "aux"}},
	}
	assert.Equal(t, want, stacks)
}

func TestBuildLocateDocFindsDeclarationsAndPlacements(t *testing.T) {
	root := locateTree(t)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	doc, unmatched, err := buildLocateDoc(stacks, root, nil, filepath.Join(root, "dumps"), []string{"events"}, false)
	require.NoError(t, err)
	assert.Empty(t, unmatched)

	require.Len(t, doc.Objects, 1)
	obj := doc.Objects[0]
	assert.Equal(t, "posthog", obj.Database)
	assert.Equal(t, "events", obj.Name)
	assert.Equal(t, []string{"table"}, obj.Types)

	require.Len(t, obj.Declarations, 1)
	d := obj.Declarations[0]
	assert.Equal(t, filepath.Join(root, "ingestion", "events.hcl"), d.File)
	assert.Equal(t, 3, d.Line)
	assert.Equal(t, filepath.Join(root, "ingestion"), d.Layer)
	assert.Equal(t, "events_base", d.Extends)
	assert.Equal(t, []locatePlacement{
		{Role: "ingestion", Env: "prod-us"},
		{Role: "ingestion", Env: "prod-eu"},
	}, d.Placements)

	assert.Equal(t, []locateDump{
		{File: filepath.Join(root, "dumps", "node1.hcl"), Line: 4, Node: "node1", Type: "table"},
	}, obj.Dumps, "the dump site is attributed to its node{} block")
}

func TestBuildLocateDocGlobAndSharedLayerPlacements(t *testing.T) {
	root := locateTree(t)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	doc, unmatched, err := buildLocateDoc(stacks, root, nil, "", []string{"person"}, false)
	require.NoError(t, err)
	assert.Empty(t, unmatched)

	require.Len(t, doc.Objects, 1)
	obj := doc.Objects[0]
	require.Len(t, obj.Declarations, 2)

	shared, aux := obj.Declarations[0], obj.Declarations[1]
	assert.Equal(t, filepath.Join(root, "shared", "base.hcl"), shared.File)
	assert.Equal(t, []locatePlacement{
		{Role: "ingestion", Env: "prod-us"},
		{Role: "ingestion", Env: "prod-eu"},
		{Role: "aux", Env: "prod-us"},
	}, shared.Placements)
	assert.Equal(t, filepath.Join(root, "aux", "dup.hcl"), aux.File)
	assert.Equal(t, []locatePlacement{{Role: "aux", Env: "prod-us"}}, aux.Placements)
}

func TestBuildLocateDocNoMatch(t *testing.T) {
	root := locateTree(t)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	doc, unmatched, err := buildLocateDoc(stacks, root, nil, "", []string{"nosuchobject"}, false)
	require.NoError(t, err)
	assert.Empty(t, doc.Objects)
	assert.Equal(t, []string{"nosuchobject"}, unmatched)
}

// With several patterns, each is an independent existence check: matched
// ones return their objects, and every pattern that found nothing is
// reported (the CLI exits 1 on any).
func TestBuildLocateDocMultiplePatterns(t *testing.T) {
	root := locateTree(t)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	doc, unmatched, err := buildLocateDoc(stacks, root, nil, filepath.Join(root, "dumps"),
		[]string{"person", "only_*", "nosuch*"}, false)
	require.NoError(t, err)

	names := make([]string, len(doc.Objects))
	for i, o := range doc.Objects {
		names[i] = o.Name
	}
	assert.Equal(t, []string{"only_live", "person"}, names,
		"only_live matches via the dump side only")
	assert.Equal(t, []string{"nosuch*"}, unmatched)
	assert.Equal(t, []string{"person", "only_*", "nosuch*"}, doc.Patterns)
}

func TestBuildLocateColumnDocSearchesResolvedManifestAndDumpModels(t *testing.T) {
	root := locateTree(t)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	// The aux stack deliberately redeclares person for the object-duplicate
	// tests. The two ingestion stacks are valid resolved models.
	doc, err := buildLocateColumnDoc(stacks[:2], root, nil, filepath.Join(root, "dumps"),
		[]string{"events"}, []string{"uuid"})
	require.NoError(t, err)
	assert.Equal(t, []string{"events"}, doc.TablePatterns)
	assert.Equal(t, []string{"uuid"}, doc.ColumnPatterns)
	require.Len(t, doc.Columns, 1)

	events := doc.Columns[0]
	assert.Equal(t, "posthog", events.Database)
	assert.Equal(t, "events", events.Table)
	assert.Equal(t, "uuid", events.Name)
	assert.Equal(t, []locateModelRef{
		{Source: "manifest", Role: "ingestion", Env: "prod-us", Layers: []string{"shared", "ingestion"}},
		{Source: "manifest", Role: "ingestion", Env: "prod-eu", Layers: []string{"shared", "ingestion"}},
		{Source: "dump", File: filepath.Join(root, "dumps", "node1.hcl"), Node: "node1"},
	}, events.Models, "uuid is inherited into both composed manifest models and loaded from the dump model")
}

func TestBuildLocateColumnDocSearchesResolvedLayerStack(t *testing.T) {
	root := t.TempDir()
	base := writeLocateLayer(t, root, "base.hcl", `
database "posthog" {
  table "events_base" {
    abstract = true
    column "uuid" { type = "UUID" }
  }
  table "events" {
    extend = "events_base"
    engine "log" {}
  }
}`)
	patch := writeLocateLayer(t, root, "patch.hcl", `
database "posthog" {
  patch_table "events" {
    column "person_properties" { type = "String" }
  }
}`)

	doc, err := buildLocateColumnDoc(nil, "", []string{base, patch}, "",
		[]string{"events"}, []string{"uuid", "person_properties"})
	require.NoError(t, err)
	require.Len(t, doc.Columns, 2)
	assert.Equal(t, "person_properties", doc.Columns[0].Name)
	assert.Equal(t, "uuid", doc.Columns[1].Name)
	for _, column := range doc.Columns {
		assert.Equal(t, []locateModelRef{{Source: "layer", Layers: []string{base, patch}}}, column.Models)
	}
}

func TestBuildLocateColumnDocNoMatchIsEmpty(t *testing.T) {
	root := locateTree(t)
	doc, err := buildLocateColumnDoc(nil, root, nil, filepath.Join(root, "dumps"),
		[]string{"flag_evaluations"}, []string{"person_properties"})
	require.NoError(t, err)
	assert.Empty(t, doc.Columns)

	body, err := json.Marshal(doc)
	require.NoError(t, err)
	assert.JSONEq(t, `{
  "table_patterns": ["flag_evaluations"],
  "column_patterns": ["person_properties"],
  "columns": []
}`, string(body))
}

// A dump file without a node{} block falls back to the filename stem, the
// same identity drift uses.
func TestBuildLocateDocDumpNodeFallback(t *testing.T) {
	root := locateTree(t)

	doc, unmatched, err := buildLocateDoc(nil, root, nil, filepath.Join(root, "dumps"), []string{"only_live"}, false)
	require.NoError(t, err)
	assert.Empty(t, unmatched)

	require.Len(t, doc.Objects, 1)
	assert.Equal(t, []locateDump{
		{File: filepath.Join(root, "dumps", "node2.hcl"), Line: 3, Node: "node2", Type: "table"},
	}, doc.Objects[0].Dumps)
}

// Ad-hoc -layer entries are searched without a manifest (no placements) and
// dedupe against the manifest's layers when combined.
func TestBuildLocateDocExtraLayers(t *testing.T) {
	root := locateTree(t)

	// Alone: no manifest, no placements.
	doc, unmatched, err := buildLocateDoc(nil, "", []string{filepath.Join(root, "aux")}, "", []string{"person"}, false)
	require.NoError(t, err)
	assert.Empty(t, unmatched)
	require.Len(t, doc.Objects, 1)
	require.Len(t, doc.Objects[0].Declarations, 1)
	d := doc.Objects[0].Declarations[0]
	assert.Equal(t, filepath.Join(root, "aux", "dup.hcl"), d.File)
	assert.Equal(t, filepath.Join(root, "aux"), d.Layer)
	assert.Empty(t, d.Placements)

	// Combined with the manifest: an already-scanned layer adds no second
	// declaration site.
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)
	doc, unmatched, err = buildLocateDoc(stacks, root, []string{filepath.Join(root, "shared")}, "", []string{"person"}, false)
	require.NoError(t, err)
	assert.Empty(t, unmatched)
	require.Len(t, doc.Objects, 1)
	assert.Len(t, doc.Objects[0].Declarations, 2, "shared + aux, not shared twice")
}

// -duplicates audits ad-hoc -layer entries too: the same once-only rule
// works before any manifest exists.
func TestBuildLocateDocDuplicatesFromExtraLayers(t *testing.T) {
	root := locateTree(t)
	shared := filepath.Join(root, "shared")
	aux := filepath.Join(root, "aux")

	doc, _, err := buildLocateDoc(nil, "", []string{
		shared,
		aux,
	}, "", nil, true)
	require.NoError(t, err)

	assert.Empty(t, doc.Duplicates)
	assert.Empty(t, doc.Variants)
	require.Len(t, doc.Collisions, 1)
	assert.Equal(t, "person", doc.Collisions[0].Name)
	assert.Equal(t, []locateCompositionRef{{Source: "layer", Layers: []string{shared, aux}}}, doc.Collisions[0].CollisionIn)
}

// An extended object cross-links its children even when the pattern matches
// only the parent — the reverse edges come from every authored declaration,
// not just the matching ones.
func TestBuildLocateDocExtendedBy(t *testing.T) {
	root := locateTree(t)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	doc, unmatched, err := buildLocateDoc(stacks, root, nil, "", []string{"events_base"}, false)
	require.NoError(t, err)
	assert.Empty(t, unmatched)

	require.Len(t, doc.Objects, 1)
	assert.Equal(t, []string{"posthog.events"}, doc.Objects[0].ExtendedBy)
}

func TestBuildLocateDocDuplicates(t *testing.T) {
	root := locateTree(t)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	doc, _, err := buildLocateDoc(stacks, root, nil, "", nil, true)
	require.NoError(t, err)

	// person is declared plainly in shared and aux; events_base (abstract) +
	// events (extend child) must not be flagged.
	assert.Empty(t, doc.Duplicates)
	assert.Empty(t, doc.Variants)
	require.Len(t, doc.Collisions, 1)
	collision := doc.Collisions[0]
	assert.Equal(t, "posthog", collision.Database)
	assert.Equal(t, "person", collision.Name)
	require.Len(t, collision.Declarations, 2)
	assert.Equal(t, filepath.Join(root, "aux", "dup.hcl"), collision.Declarations[0].File)
	assert.Equal(t, filepath.Join(root, "shared", "base.hcl"), collision.Declarations[1].File)
	assert.Equal(t, []locateCompositionRef{{
		Source: "manifest", Role: "aux", Env: "prod-us", Layers: []string{"shared", "aux"},
	}}, collision.CollisionIn)
	require.Len(t, collision.ResolvedVariants, 1)
	assert.Len(t, collision.ResolvedVariants[0].Models, 2, "the two valid ingestion compositions still resolve")
	assert.Len(t, collision.ResolvedVariants[0].Definitions, 1, "both valid compositions reuse the shared declaration")
}

func TestBuildLocateDocClassifiesDifferentResolvedObjectsAsVariants(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "manifest.hcl"), `
role "data" {
  env "dev"   { layers = ["dev"] }
  env "local" { layers = ["local"] }
}
`)
	definition := func(kind string) string {
		return `database "posthog" {
  table "events" {
    column "source" {
      type    = "String"
      default = "'` + kind + `'"
    }
    engine "log" {}
  }
}
`
	}
	writeFileT(t, filepath.Join(root, "dev", "events.hcl"), definition("dev"))
	writeFileT(t, filepath.Join(root, "local", "events.hcl"), definition("local"))
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	doc, _, err := buildLocateDoc(stacks, root, nil, "", nil, true)
	require.NoError(t, err)
	assert.Empty(t, doc.Duplicates)
	require.Len(t, doc.Variants, 1)
	variant := doc.Variants[0]
	assert.Equal(t, "events", variant.Name)
	assert.Empty(t, variant.CollisionIn)
	require.Len(t, variant.Declarations, 2)
	require.Len(t, variant.ResolvedVariants, 2)
	assert.Len(t, variant.ResolvedVariants[0].Definitions, 1)
	assert.Len(t, variant.ResolvedVariants[1].Definitions, 1)

	var text bytes.Buffer
	renderDuplicatesText(&text, doc)
	assert.Contains(t, text.String(), "distinct variants table posthog.events (2 sites)")
}

func TestBuildLocateDocSemanticDuplicatesCoverEveryManagedObjectKind(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "manifest.hcl"), `
role "data" {
  env "left"  { layers = ["left"] }
  env "right" { layers = ["right"] }
}
`)
	writeFileT(t, filepath.Join(root, "left", "schema.hcl"), `
named_collection "warehouse" {
  param "host" { value = "warehouse.internal" }
}
database "analytics" {
  table "events" {
    column "id" { type = "UInt64" }
    engine "log" {}
  }
  materialized_view "events_mv" {
    to_table = "analytics.events"
    query    = "SELECT id FROM analytics.events"
    column "id" { type = "UInt64" }
  }
  view "environment" {
    query = "SELECT 'production' AS name"
  }
  dictionary "labels" {
    primary_key = ["id"]
    attribute "id"    { type = "UInt64" }
    attribute "label" { type = "String" }
    source "null" {}
    layout "flat" {}
  }
  raw "view" "legacy_environment" {
    sql = "CREATE VIEW analytics.legacy_environment AS SELECT 'production'"
  }
}
`)
	writeFileT(t, filepath.Join(root, "right", "schema.hcl"), `
named_collection "warehouse" {
  override = true
  param "host" { value = "warehouse.internal" }
}
database "analytics" {
  table "events" {
    override = true
    column "id" { type = "UInt64" }
    engine "log" {}
  }
  materialized_view "events_mv" {
    override = true
    to_table = "analytics.events"
    query    = "SELECT id FROM analytics.events"
    column "id" { type = "UInt64" }
  }
  view "environment" {
    override = true
    query    = "SELECT 'production' AS name"
  }
  dictionary "labels" {
    override    = true
    primary_key = ["id"]
    attribute "id"    { type = "UInt64" }
    attribute "label" { type = "String" }
    source "null" {}
    layout "flat" {}
  }
  raw "view" "legacy_environment" {
    override = true
    sql      = "CREATE VIEW analytics.legacy_environment AS SELECT 'production'"
  }
}
`)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	var status bytes.Buffer
	doc, _, err := buildLocateDocWithProgress(stacks, root, nil, "", nil, true, &status)
	require.NoError(t, err)
	assert.Contains(t, status.String(), "locate: loading 2 composition models in parallel (2 workers)")
	assert.Contains(t, status.String(), "locate: loaded 2 composition models")
	assert.Empty(t, doc.Variants)
	require.Len(t, doc.Duplicates, 6)
	for _, duplicate := range doc.Duplicates {
		require.Len(t, duplicate.ResolvedVariants, 1, duplicate.Name)
		assert.Len(t, duplicate.ResolvedVariants[0].Models, 2, duplicate.Name)
		assert.Len(t, duplicate.ResolvedVariants[0].Definitions, 2, duplicate.Name)
	}
}

func TestBuildLocateDocComparesAbstractDefinitionsBeforeTheyAreDropped(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "manifest.hcl"), `
role "data" {
  env "left"  { layers = ["left"] }
  env "right" { layers = ["right"] }
}
`)
	definition := `database "analytics" {
  table "event_base" {
    abstract = true
    column "id" { type = "UInt64" }
  }
}
`
	writeFileT(t, filepath.Join(root, "left", "base.hcl"), definition)
	writeFileT(t, filepath.Join(root, "right", "base.hcl"), definition)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	doc, _, err := buildLocateDoc(stacks, root, nil, "", nil, true)
	require.NoError(t, err)
	require.Len(t, doc.Duplicates, 1)
	assert.Equal(t, "event_base", doc.Duplicates[0].Name)
	require.Len(t, doc.Duplicates[0].ResolvedVariants, 1)
	assert.Len(t, doc.Duplicates[0].ResolvedVariants[0].Definitions, 2)
}

func TestBuildLocateDocDoesNotClassifyAReplacedDefinitionFromOneModel(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "base", "events.hcl"), `
database "analytics" {
  table "events" {
    column "id" { type = "UInt64" }
    engine "log" {}
  }
}
`)
	writeFileT(t, filepath.Join(root, "env", "events.hcl"), `
database "analytics" {
  table "events" {
    override = true
    column "id" { type = "String" }
    engine "log" {}
  }
}
`)

	doc, _, err := buildLocateDoc(nil, "", []string{
		filepath.Join(root, "base"), filepath.Join(root, "env"),
	}, "", nil, true)
	require.NoError(t, err)
	assert.Empty(t, doc.Duplicates)
	assert.Empty(t, doc.Variants)
	assert.Empty(t, doc.Collisions)
}

func TestRenderLocateText(t *testing.T) {
	root := locateTree(t)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)
	doc, _, err := buildLocateDoc(stacks, root, nil, filepath.Join(root, "dumps"), []string{"events*"}, false)
	require.NoError(t, err)

	var buf bytes.Buffer
	renderLocateText(&buf, doc)
	out := buf.String()
	assert.Contains(t, out, "table posthog.events")
	assert.Contains(t, out, "events.hcl:3")
	assert.Contains(t, out, "extends events_base")
	assert.Contains(t, out, "(ingestion, prod-us)")
	assert.Contains(t, out, "(node node1)")
	assert.Contains(t, out, "extended by: posthog.events")

	var dupBuf bytes.Buffer
	dupDoc, _, err := buildLocateDoc(stacks, root, nil, "", nil, true)
	require.NoError(t, err)
	renderDuplicatesText(&dupBuf, dupDoc)
	assert.Contains(t, dupBuf.String(), "declaration collision table posthog.person")
	assert.Contains(t, dupBuf.String(), "base.hcl:7")
}

func TestRenderLocateColumnText(t *testing.T) {
	doc := locateColumnDoc{Columns: []locateColumn{{
		Database: "posthog",
		Table:    "flag_evaluations",
		Name:     "person_properties",
		Models: []locateModelRef{
			{Source: "manifest", Role: "ingestion", Env: "prod-us", Layers: []string{"shared", "prod-us"}},
			{Source: "layer", Layers: []string{"shared", "local"}},
			{Source: "dump", File: "prod-us/node1.hcl", Node: "node1"},
		},
	}}}

	var buf bytes.Buffer
	renderLocateColumnText(&buf, doc)
	out := buf.String()
	assert.Contains(t, out, "column posthog.flag_evaluations.person_properties")
	assert.Contains(t, out, "manifest: (ingestion, prod-us)  layers: shared,prod-us")
	assert.Contains(t, out, "layers: shared,local")
	assert.Contains(t, out, "dump: prod-us/node1.hcl  (node node1)")
}

func TestLoadLocateModelTasksLoadsThirtyNodesInParallel(t *testing.T) {
	tasks := make([]locateModelTask, 30)
	for i := range tasks {
		tasks[i].Ref = locateModelRef{Source: "dump", File: fmt.Sprintf("node-%02d.hcl", i)}
	}
	started := make(chan struct{}, len(tasks))
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	type result struct {
		models []locateModel
		err    error
	}
	done := make(chan result, 1)
	go func() {
		models, err := loadLocateModelTasks(tasks, locateLoadParallelism, func(task locateModelTask) (locateModel, error) {
			started <- struct{}{}
			<-release
			return locateModel{Ref: task.Ref, Schema: &hclload.Schema{}}, nil
		})
		done <- result{models: models, err: err}
	}()

	for range tasks {
		select {
		case <-started:
		case <-time.After(time.Second):
			require.FailNow(t, "all 30 loaders did not start concurrently")
		}
	}
	close(release)
	released = true
	got := <-done
	require.NoError(t, got.err)
	require.Len(t, got.models, 30)
	for i := range got.models {
		assert.Equal(t, tasks[i].Ref, got.models[i].Ref, "parallel results preserve deterministic task order")
	}
}

func TestLocateColumnsCLIProcess(t *testing.T) {
	if os.Getenv("HCLEXP_LOCATE_COLUMNS_HELPER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			runLocate(os.Args[i+1:])
			os.Exit(0)
			return
		}
	}
	t.Fatal("missing locate CLI arguments")
}

func TestLocateColumnsCLIEndToEnd(t *testing.T) {
	root := t.TempDir()
	dumps := filepath.Join(root, "prod-us")
	require.NoError(t, os.MkdirAll(dumps, 0o755))
	write := func(name, body string) {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(dumps, name), []byte(body), 0o600))
	}
	write("node-a.hcl", `
node "node-a" {}
database "posthog" {
  table "flag_evaluations" {
    column "person_properties" { type = "String" }
    column "group0_properties" { type = "String" }
    engine "log" {}
  }
  table "sharded_flag_evaluations" {
    column "group4_properties" { type = "String" }
    engine "log" {}
  }
  table "not_selected" {
    column "person_properties" { type = "String" }
    engine "log" {}
  }
}
`)
	write("node-b.hcl", `
node "node-b" {}
database "posthog" {
  table "flag_evaluations" {
    column "person_properties" { type = "String" }
    engine "log" {}
  }
  table "writable_flag_evaluations" {
    column "group2_properties" { type = "String" }
    engine "log" {}
  }
  table "kafka_flag_evaluations" {
    column "group3_properties" { type = "String" }
    column "event" { type = "String" }
    engine "log" {}
  }
}
`)

	tables := "flag_evaluations,sharded_flag_evaluations,writable_flag_evaluations,kafka_flag_evaluations"
	columns := "person_properties,group0_properties,group1_properties,group2_properties,group3_properties,group4_properties"
	output, err := runLocateColumnsCLI(t, "-dump", dumps, "-tables", tables, "-columns", columns, "-format", "json")
	require.NoError(t, err, string(output))
	var doc locateColumnDoc
	require.NoError(t, json.Unmarshal(output, &doc), string(output))
	require.Len(t, doc.Columns, 5)
	assert.Equal(t, "flag_evaluations", doc.Columns[0].Table)
	assert.Equal(t, "group0_properties", doc.Columns[0].Name)
	assert.Equal(t, "person_properties", doc.Columns[1].Name)
	assert.Equal(t, []string{"node-a", "node-b"}, []string{doc.Columns[1].Models[0].Node, doc.Columns[1].Models[1].Node})
	assert.Equal(t, "kafka_flag_evaluations", doc.Columns[2].Table)
	assert.Equal(t, "sharded_flag_evaluations", doc.Columns[3].Table)
	assert.Equal(t, "writable_flag_evaluations", doc.Columns[4].Table)

	empty, err := runLocateColumnsCLI(t, "-dump", dumps, "-tables", tables, "-columns", "removed_column", "-format", "json")
	require.NoError(t, err, string(empty))
	var emptyDoc locateColumnDoc
	require.NoError(t, json.Unmarshal(empty, &emptyDoc), string(empty))
	assert.Empty(t, emptyDoc.Columns)
	assert.Contains(t, string(empty), `"columns": []`)

	broken := filepath.Join(root, "broken")
	require.NoError(t, os.MkdirAll(broken, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(broken, "bad.hcl"), []byte(`database "posthog" {`), 0o600))
	failed, err := runLocateColumnsCLI(t, "-dump", broken, "-tables", tables, "-columns", columns, "-format", "json")
	require.Error(t, err, string(failed))
	exitErr, ok := err.(*exec.ExitError)
	require.True(t, ok)
	assert.Equal(t, 1, exitErr.ExitCode(), string(failed))
}

func TestLocateColumnsCLIThirtyNodesEndToEnd(t *testing.T) {
	dumps := t.TempDir()
	for i := range 30 {
		node := fmt.Sprintf("node-%02d", i)
		body := fmt.Sprintf(`
node %q {}
database "posthog" {
  table "events" {
    column "uuid" { type = "UUID" }
    engine "log" {}
  }
}
`, node)
		require.NoError(t, os.WriteFile(filepath.Join(dumps, node+".hcl"), []byte(body), 0o600))
	}

	output, err := runLocateColumnsCLI(t, "-dump", dumps, "-tables", "events", "-columns", "uuid", "-format", "json")
	require.NoError(t, err, string(output))
	var doc locateColumnDoc
	require.NoError(t, json.Unmarshal(output, &doc), string(output))
	require.Len(t, doc.Columns, 1)
	require.Len(t, doc.Columns[0].Models, 30)
	for i, model := range doc.Columns[0].Models {
		expectedNode := fmt.Sprintf("node-%02d", i)
		assert.Equal(t, "dump", model.Source)
		assert.Equal(t, expectedNode, model.Node)
		assert.Equal(t, filepath.Join(dumps, expectedNode+".hcl"), model.File)
	}
}

func TestLocateDuplicatesSemanticClassificationEndToEnd(t *testing.T) {
	root := t.TempDir()
	manifest := filepath.Join(root, "manifest.hcl")
	writeFileT(t, filepath.Join(root, "dev", "events.hcl"), `
database "posthog" {
  table "events" {
    column "id" { type = "UInt64" }
    engine "log" {}
  }
}
`)
	writeFileT(t, filepath.Join(root, "local", "events.hcl"), `
database "posthog" {
  table "events" {
    override = true
    column "id" { type = "String" }
    engine "log" {}
  }
}
`)
	writeFileT(t, manifest, `
role "data" {
  env "dev"   { layers = ["dev"] }
  env "local" { layers = ["local"] }
}
`)

	output, err := runLocateColumnsCLI(t, "-manifest", manifest, "-layer-root", root, "-duplicates", "-format", "json")
	require.NoError(t, err, string(output))
	var disjoint locateDoc
	require.NoError(t, json.Unmarshal(output, &disjoint), string(output))
	assert.Empty(t, disjoint.Duplicates)
	require.Len(t, disjoint.Variants, 1)
	assert.Equal(t, "events", disjoint.Variants[0].Name)
	assert.Len(t, disjoint.Variants[0].ResolvedVariants, 2)

	// An override in a mutually exclusive layer must not hide an identical
	// full definition. Once both resolved objects are equal, the command
	// classifies them as a duplicate even though they never co-compose.
	writeFileT(t, filepath.Join(root, "local", "events.hcl"), `
database "posthog" {
  table "events" {
    override = true
    column "id" { type = "UInt64" }
    engine "log" {}
  }
}
`)
	output, err = runLocateColumnsCLI(t, "-manifest", manifest, "-layer-root", root, "-duplicates", "-format", "json")
	require.Error(t, err, string(output))
	exitErr, ok := err.(*exec.ExitError)
	require.True(t, ok)
	assert.Equal(t, 1, exitErr.ExitCode())
	var composed locateDoc
	require.NoError(t, json.Unmarshal(output, &composed), string(output))
	require.Len(t, composed.Duplicates, 1)
	assert.Empty(t, composed.Variants)
	assert.Empty(t, composed.Duplicates[0].CollisionIn)
	require.Len(t, composed.Duplicates[0].ResolvedVariants, 1)
	assert.Len(t, composed.Duplicates[0].ResolvedVariants[0].Models, 2)
	assert.Len(t, composed.Duplicates[0].ResolvedVariants[0].Definitions, 2)

	writeFileT(t, filepath.Join(root, "local", "events.hcl"), `
database "posthog" {
  table "events" {
    column "id" { type = "String" }
    engine "log" {}
  }
}
`)
	writeFileT(t, manifest, `
role "data" {
  env "dev" { layers = ["dev", "local"] }
}
`)
	output, err = runLocateColumnsCLI(t, "-manifest", manifest, "-layer-root", root, "-duplicates", "-format", "json")
	require.Error(t, err, string(output))
	exitErr, ok = err.(*exec.ExitError)
	require.True(t, ok)
	assert.Equal(t, 1, exitErr.ExitCode())
	var collision locateDoc
	require.NoError(t, json.Unmarshal(output, &collision), string(output))
	assert.Empty(t, collision.Duplicates)
	assert.Empty(t, collision.Variants)
	require.Len(t, collision.Collisions, 1)
	assert.Equal(t, []locateCompositionRef{{
		Source: "manifest", Role: "data", Env: "dev", Layers: []string{"dev", "local"},
	}}, collision.Collisions[0].CollisionIn)
}

func runLocateColumnsCLI(t *testing.T, args ...string) ([]byte, error) {
	t.Helper()
	commandArgs := append([]string{"-test.run=^TestLocateColumnsCLIProcess$", "--"}, args...)
	cmd := exec.Command(os.Args[0], commandArgs...)
	cmd.Env = append(os.Environ(), "HCLEXP_LOCATE_COLUMNS_HELPER=1")
	return cmd.CombinedOutput()
}

func writeLocateLayer(t *testing.T, root, name, body string) string {
	t.Helper()
	path := filepath.Join(root, name)
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}
