package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

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
    engine "MergeTree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
}
`)
	mustWrite("ingestion/events.hcl", `
database "posthog" {
  table "events" {
    extend   = "events_base"
    engine "MergeTree" {}
    order_by = ["uuid"]
  }
}
`)
	mustWrite("aux/dup.hcl", `
database "posthog" {
  table "person" {
    engine "MergeTree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
}
`)
	mustWrite("dumps/node1.hcl", `
node "node1" {}
database "posthog" {
  table "events" {
    engine "MergeTree" {}
    order_by = ["uuid"]
    column "uuid" { type = "UUID" }
  }
}
`)
	mustWrite("dumps/node2.hcl", `
database "posthog" {
  table "only_live" {
    engine "MergeTree" {}
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

	assert.NoError(t, locateFlagsError("m.hcl", "", "", "text", false, one))
	assert.NoError(t, locateFlagsError("", "", "dumps", "json", false, one))
	assert.NoError(t, locateFlagsError("", "a,b", "", "text", false, one), "-layer alone is a source")
	assert.NoError(t, locateFlagsError("m.hcl", "", "", "text", false, several), "several patterns")
	assert.NoError(t, locateFlagsError("m.hcl", "", "", "text", true, nil))
	assert.NoError(t, locateFlagsError("", "a", "", "text", true, nil), "-duplicates audits -layer too")

	assert.Error(t, locateFlagsError("", "", "", "text", false, one), "needs a source")
	assert.Error(t, locateFlagsError("m.hcl", "", "", "yaml", false, one), "bad format")
	assert.Error(t, locateFlagsError("m.hcl", "", "", "text", false, nil), "missing name")
	assert.Error(t, locateFlagsError("m.hcl", "", "", "text", false, []string{"a", "[bad"}), "invalid glob among several")
	assert.Error(t, locateFlagsError("", "", "dumps", "text", true, nil), "-duplicates without authored layers")
	assert.Error(t, locateFlagsError("m.hcl", "", "dumps", "text", true, nil), "-duplicates with -dump")
	assert.Error(t, locateFlagsError("m.hcl", "", "", "text", true, one), "-duplicates with name")
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

	doc, _, err := buildLocateDoc(nil, "", []string{
		filepath.Join(root, "shared"),
		filepath.Join(root, "aux"),
	}, "", nil, true)
	require.NoError(t, err)

	require.Len(t, doc.Duplicates, 1)
	assert.Equal(t, "person", doc.Duplicates[0].Name)
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
	require.Len(t, doc.Duplicates, 1)
	dup := doc.Duplicates[0]
	assert.Equal(t, "posthog", dup.Database)
	assert.Equal(t, "person", dup.Name)
	require.Len(t, dup.Declarations, 2)
	assert.Equal(t, filepath.Join(root, "aux", "dup.hcl"), dup.Declarations[0].File)
	assert.Equal(t, filepath.Join(root, "shared", "base.hcl"), dup.Declarations[1].File)
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
	assert.Contains(t, dupBuf.String(), "duplicate table posthog.person")
	assert.Contains(t, dupBuf.String(), "base.hcl:7")
}
