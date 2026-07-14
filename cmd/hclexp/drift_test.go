package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseNodeIdentity(t *testing.T) {
	cases := []struct {
		name                         string
		wantShard, wantRep, wantRole string
	}{
		{"prod-eu-fra-ch-10a-ingestion-events", "10", "a", "ingestion-events"},
		{"prod-us-iad-ch-1d-ops", "1", "d", "ops"},
		{"prod-eu-fra-ch-1f", "1", "f", ""},
		{"prod-eu-fra-ch-8h-offline", "8", "h", "offline"},
		{"not-a-node", "", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			shard, rep, role := parseNodeIdentity(c.name)
			assert.Equal(t, c.wantShard, shard)
			assert.Equal(t, c.wantRep, rep)
			assert.Equal(t, c.wantRole, role)
		})
	}
}

func TestGroupKey(t *testing.T) {
	n := driftNode{
		Macros:  map[string]string{"hostClusterRole": "data", "hostClusterType": "online"},
		Role:    "ingestion-events",
		Shard:   "3",
		Replica: "a",
	}
	assert.Equal(t, "data", groupKey(n, []string{"hostClusterRole"}))
	assert.Equal(t, "data/online", groupKey(n, []string{"hostClusterRole", "hostClusterType"}))
	assert.Equal(t, "ingestion-events", groupKey(n, []string{"role"}))
	assert.Equal(t, "3/a", groupKey(n, []string{"shard", "replica"}))
	assert.Equal(t, "(none)", groupKey(n, []string{"missingMacro"}))
}

func TestLoadDriftNodes_Glob(t *testing.T) {
	dir := filepath.Join("testdata", "drift")

	all, err := loadDriftNodes(dir, "*")
	require.NoError(t, err)
	assert.Len(t, all, 4)

	role2, err := loadDriftNodes(dir, "*-role2*")
	require.NoError(t, err)
	require.Len(t, role2, 1)
	assert.Equal(t, "prod-xx-aaa-ch-1a-role2", role2[0].Name)

	none, err := loadDriftNodes(dir, "*nomatch*")
	require.NoError(t, err)
	assert.Empty(t, none)

	// Comma-separated globs union their matches.
	union, err := loadDriftNodes(dir, "*-role2*,*-3a-*")
	require.NoError(t, err)
	names := make([]string, len(union))
	for i, n := range union {
		names[i] = n.Name
	}
	assert.ElementsMatch(t, []string{"prod-xx-aaa-ch-1a-role2", "prod-xx-aaa-ch-3a-role1"}, names)

	_, err = loadDriftNodes(dir, "*-role2*,[")
	assert.Error(t, err, "an invalid pattern in the list should error")
}

func TestNormalizeZKPaths(t *testing.T) {
	schemaWithZK := func(path string) *hclload.Schema {
		return &hclload.Schema{Databases: []hclload.DatabaseSpec{{
			Name: "posthog",
			Tables: []hclload.TableSpec{{
				Name:    "t",
				Columns: []hclload.ColumnSpec{{Name: "id", Type: "Int64"}},
				OrderBy: []string{"id"},
				Engine: &hclload.EngineSpec{
					Kind: "replicated_merge_tree",
					Decoded: hclload.EngineReplicatedMergeTree{
						ZooPath:     path,
						ReplicaName: "{replica}",
					},
				},
			}},
		}}}
	}
	uuidA := "/clickhouse/tables/3e6d8f0a-13bc-433c-9173-9ec7d4deb230_noshard/posthog.t"
	uuidB := "/clickhouse/tables/7695bea2-a760-4ff1-9f2e-d33cb4de53dd_noshard/posthog.t"

	// keep: UUID-only difference shows as drift.
	assert.False(t, hclload.Diff(schemaWithZK(uuidA), schemaWithZK(uuidB)).IsEmpty(),
		"raw paths with different UUIDs should differ")

	// mask-uuid: the same logical table compares equal.
	na, nb := schemaWithZK(uuidA), schemaWithZK(uuidB)
	normalizeZKPaths(na, "mask-uuid")
	normalizeZKPaths(nb, "mask-uuid")
	assert.True(t, hclload.Diff(na, nb).IsEmpty(),
		"mask-uuid should collapse UUID-only path differences")

	// mask-uuid: a genuine path difference still drifts.
	ga := schemaWithZK("/clickhouse/tables/{shard}/posthog.t")
	gb := schemaWithZK("/clickhouse/tables/{shard}/other.t")
	normalizeZKPaths(ga, "mask-uuid")
	normalizeZKPaths(gb, "mask-uuid")
	assert.False(t, hclload.Diff(ga, gb).IsEmpty(),
		"non-UUID path differences must still register as drift")

	// ignore: any path difference is erased.
	ia, ib := schemaWithZK(uuidA), schemaWithZK("/clickhouse/tables/{shard}/other.t")
	normalizeZKPaths(ia, "ignore")
	normalizeZKPaths(ib, "ignore")
	assert.True(t, hclload.Diff(ia, ib).IsEmpty(), "ignore should erase all zoo_path differences")
}

func TestLoadAndGroupDriftNodes(t *testing.T) {
	nodes, err := loadDriftNodes(filepath.Join("testdata", "drift"), "*")
	require.NoError(t, err)
	require.Len(t, nodes, 4)

	groups, order := groupNodes(nodes, []string{"hostClusterRole"})
	assert.Equal(t, []string{"role1", "role2"}, order)
	require.Len(t, groups["role1"], 3)
	require.Len(t, groups["role2"], 1)

	// Within role1, shard 3 adds a table; shard 2 matches the reference.
	byName := map[string]driftNode{}
	for _, n := range groups["role1"] {
		byName[n.Name] = n
	}
	ref := byName["prod-xx-aaa-ch-1a-role1"]
	same := byName["prod-xx-aaa-ch-2a-role1"]
	drifted := byName["prod-xx-aaa-ch-3a-role1"]

	assert.True(t, hclload.Diff(ref.Schema, same.Schema).IsEmpty(), "shard 2 should match reference")
	assert.False(t, hclload.Diff(ref.Schema, drifted.Schema).IsEmpty(), "shard 3 should drift")

	// Identity is parsed from the node name.
	assert.Equal(t, "3", drifted.Shard)
	assert.Equal(t, "role1", drifted.Role)
}

// writeDriftNode writes one per-node dump file into dir.
func writeDriftNode(t *testing.T, dir, name, hcl string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+".hcl"), []byte(hcl), 0o644))
}

const driftBaseHCL = `
database "d" {
  table "events" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}
`

// An MV present only on the drifter surfaces as status "added" with a CREATE
// operation — pinning the descriptive reference -> drifter direction.
func TestBuildDriftDoc_Direction(t *testing.T) {
	dir := t.TempDir()
	writeDriftNode(t, dir, "node-a", driftBaseHCL)
	writeDriftNode(t, dir, "node-b", driftBaseHCL+`
database "d" {
  materialized_view "mv_events" {
    to_table = "events"
    query    = "SELECT id FROM d.events"
    column "id" { type = "UInt64" }
  }
}
`)
	nodes, err := loadDriftNodes(dir, "*")
	require.NoError(t, err)
	doc := buildDriftDoc(nodes, []string{"role"})

	require.Len(t, doc.Groups, 1)
	g := doc.Groups[0]
	assert.Equal(t, "node-a", g.Reference)
	assert.Equal(t, 2, g.Nodes)
	require.Len(t, g.Drifters, 1)
	d := g.Drifters[0]
	assert.Equal(t, "node-b", d.Node)
	require.Len(t, d.Objects, 1)
	assert.Equal(t, hclload.StatusAdded, d.Objects[0].Status)
	assert.Equal(t, hclload.KindMaterializedView, d.Objects[0].ObjectType)
	require.NotEmpty(t, d.Objects[0].Operations)
	assert.Equal(t, hclload.OpCreate, d.Objects[0].Operations[0].Kind)
	assert.Equal(t, hclload.CompareSummary{MVsAdded: 1}, d.Summary)
	assert.Equal(t, "+1 mv", d.Summary.OneLiner())

	assert.Equal(t, hclload.DriftRunSummary{
		Nodes: 2, Groups: 1, GroupsWithDrift: 1, DriftingNodes: 1,
	}, doc.Summary)
}

// A node drifting ONLY in a raw block yields non-zero raw counts and a
// non-"changed" one-liner — regression for the summarizeChanges gap.
func TestBuildDriftDoc_RawOnlyDrift(t *testing.T) {
	dir := t.TempDir()
	writeDriftNode(t, dir, "node-a", driftBaseHCL+`
database "d" {
  raw "table" "legacy" {
    sql = "CREATE TABLE d.legacy (id UInt64) ENGINE = MergeTree ORDER BY id"
  }
}
`)
	writeDriftNode(t, dir, "node-b", driftBaseHCL+`
database "d" {
  raw "table" "legacy" {
    sql = "CREATE TABLE d.legacy (id UInt32) ENGINE = MergeTree ORDER BY id"
  }
}
`)
	nodes, err := loadDriftNodes(dir, "*")
	require.NoError(t, err)
	doc := buildDriftDoc(nodes, []string{"role"})

	require.Len(t, doc.Groups, 1)
	require.Len(t, doc.Groups[0].Drifters, 1)
	d := doc.Groups[0].Drifters[0]
	assert.Equal(t, hclload.CompareSummary{RawsAltered: 1}, d.Summary)
	assert.Equal(t, "~1 raw", d.Summary.OneLiner())
	assert.Equal(t, "table", d.Objects[0].RawKind)
}

// A node drifting ONLY in a named collection: same regression, other kind.
func TestBuildDriftDoc_NamedCollectionOnlyDrift(t *testing.T) {
	dir := t.TempDir()
	writeDriftNode(t, dir, "node-a", driftBaseHCL+`
named_collection "s3_creds" {
  param "url" { value = "https://a" }
}
`)
	writeDriftNode(t, dir, "node-b", driftBaseHCL+`
named_collection "s3_creds" {
  param "url" { value = "https://b" }
}
`)
	nodes, err := loadDriftNodes(dir, "*")
	require.NoError(t, err)
	doc := buildDriftDoc(nodes, []string{"role"})

	require.Len(t, doc.Groups, 1)
	require.Len(t, doc.Groups[0].Drifters, 1)
	d := doc.Groups[0].Drifters[0]
	assert.Equal(t, hclload.CompareSummary{NamedCollectionsChanged: 1}, d.Summary)
	assert.Equal(t, "~1 named_collection", d.Summary.OneLiner())
	require.Len(t, d.Objects, 1)
	assert.Equal(t, hclload.StatusAltered, d.Objects[0].Status)
	assert.Equal(t, []hclload.FieldChange{
		{Field: "param:url", Change: "modify", New: "https://b"},
	}, d.Objects[0].Changes)
}

// No drift: groups populated, drifters empty, zero summary counts.
func TestBuildDriftDoc_Clean(t *testing.T) {
	dir := t.TempDir()
	writeDriftNode(t, dir, "node-a", driftBaseHCL)
	writeDriftNode(t, dir, "node-b", driftBaseHCL)
	nodes, err := loadDriftNodes(dir, "*")
	require.NoError(t, err)
	doc := buildDriftDoc(nodes, []string{"role"})
	require.Len(t, doc.Groups, 1)
	assert.Empty(t, doc.Groups[0].Drifters)
	assert.Equal(t, hclload.DriftRunSummary{Nodes: 2, Groups: 1}, doc.Summary)
}

// The text report derives its one-liner from the same CompareSummary the JSON
// carries, so a raw-only drifter can no longer print the bare "changed".
func TestRenderDriftText(t *testing.T) {
	doc := hclload.DriftJSON{
		Groups: []hclload.DriftGroup{{
			Key: "ops", Reference: "node-a", Nodes: 2,
			Drifters: []hclload.DriftNode{{
				Node:    "node-b",
				Summary: hclload.CompareSummary{RawsAltered: 1},
				Objects: []hclload.ObjectComparison{{
					Database: "d", Object: "legacy", ObjectType: hclload.KindRaw,
					RawKind: "table", Status: hclload.StatusAltered,
				}},
			}},
		}},
		Summary: hclload.DriftRunSummary{Nodes: 2, Groups: 1, GroupsWithDrift: 1, DriftingNodes: 1},
	}

	var buf bytes.Buffer
	renderDriftText(&buf, doc, true)
	assert.Equal(t, `group "ops" — 2 nodes, reference node-a — 1 drifting
  ✗ node-b: ~1 raw
      database "d"
        ~ raw table legacy

summary: 2 nodes, 1 groups, 1 groups with drift, 1 drifting nodes
`, buf.String())
}
