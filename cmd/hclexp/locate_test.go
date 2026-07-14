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
	return root
}

func TestLocateFlagsError(t *testing.T) {
	assert.NoError(t, locateFlagsError("m.hcl", "", "text", false, 1, "events"))
	assert.NoError(t, locateFlagsError("", "dumps", "json", false, 1, "events"))
	assert.NoError(t, locateFlagsError("m.hcl", "", "text", true, 0, ""))

	assert.Error(t, locateFlagsError("", "", "text", false, 1, "events"), "needs a source")
	assert.Error(t, locateFlagsError("m.hcl", "", "yaml", false, 1, "events"), "bad format")
	assert.Error(t, locateFlagsError("m.hcl", "", "text", false, 0, ""), "missing name")
	assert.Error(t, locateFlagsError("m.hcl", "", "text", false, 2, "a"), "too many args")
	assert.Error(t, locateFlagsError("m.hcl", "", "text", false, 1, "[bad"), "invalid glob")
	assert.Error(t, locateFlagsError("", "dumps", "text", true, 0, ""), "-duplicates without manifest")
	assert.Error(t, locateFlagsError("m.hcl", "dumps", "text", true, 0, ""), "-duplicates with -dump")
	assert.Error(t, locateFlagsError("m.hcl", "", "text", true, 1, "x"), "-duplicates with name")
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

	doc, err := buildLocateDoc(stacks, root, filepath.Join(root, "dumps"), "events", false)
	require.NoError(t, err)

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
		{File: filepath.Join(root, "dumps", "node1.hcl"), Line: 4, Type: "table"},
	}, obj.Dumps)
}

func TestBuildLocateDocGlobAndSharedLayerPlacements(t *testing.T) {
	root := locateTree(t)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	doc, err := buildLocateDoc(stacks, root, "", "person", false)
	require.NoError(t, err)

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

	doc, err := buildLocateDoc(stacks, root, "", "nosuchobject", false)
	require.NoError(t, err)
	assert.Empty(t, doc.Objects)
}

func TestBuildLocateDocDuplicates(t *testing.T) {
	root := locateTree(t)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	doc, err := buildLocateDoc(stacks, root, "", "", true)
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
	doc, err := buildLocateDoc(stacks, root, filepath.Join(root, "dumps"), "events", false)
	require.NoError(t, err)

	var buf bytes.Buffer
	renderLocateText(&buf, doc)
	out := buf.String()
	assert.Contains(t, out, "table posthog.events")
	assert.Contains(t, out, "events.hcl:3")
	assert.Contains(t, out, "extends events_base")
	assert.Contains(t, out, "(ingestion, prod-us)")
	assert.Contains(t, out, "dump: ")

	var dupBuf bytes.Buffer
	dupDoc, err := buildLocateDoc(stacks, root, "", "", true)
	require.NoError(t, err)
	renderDuplicatesText(&dupBuf, dupDoc)
	assert.Contains(t, dupBuf.String(), "duplicate table posthog.person")
	assert.Contains(t, dupBuf.String(), "base.hcl:7")
}
