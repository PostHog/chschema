package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func topologyNode(name, cluster, body string) string {
	return fmt.Sprintf(`
node %q {
  macros = {
    cluster = %q
  }
}
%s
`, name, cluster, body)
}

func storageTable(name string) string {
	return fmt.Sprintf(`
database "posthog" {
  table %q {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}
`, name)
}

func distributedTables(specs ...[3]string) string {
	var buf bytes.Buffer
	buf.WriteString("\ndatabase \"posthog\" {\n")
	for _, spec := range specs {
		fmt.Fprintf(&buf, `
  table %q {
    column "id" { type = "UInt64" }
    engine "distributed" {
      cluster_name    = %q
      remote_database = "posthog"
      remote_table    = %q
    }
  }
`, spec[0], spec[1], spec[2])
	}
	buf.WriteString("}\n")
	return buf.String()
}

func clusterByName(t *testing.T, doc validateDumpDoc, name string) validateDumpCluster {
	t.Helper()
	for _, cluster := range doc.Clusters {
		if cluster.Name == name {
			return cluster
		}
	}
	t.Fatalf("cluster %q not found in %#v", name, doc.Clusters)
	return validateDumpCluster{}
}

func nodeByName(t *testing.T, doc validateDumpDoc, name string) validateDumpNode {
	t.Helper()
	for _, node := range doc.Nodes {
		if node.Node == name {
			return node
		}
	}
	t.Fatalf("node %q not found in %#v", name, doc.Nodes)
	return validateDumpNode{}
}

func TestValidateTopologyDump_UnionsClusterMembersAndInfersAliases(t *testing.T) {
	dir := t.TempDir()
	writeDriftNode(t, dir, "data-a", topologyNode("data-a", "posthog", storageTable("sharded_events")))
	writeDriftNode(t, dir, "data-b", topologyNode("data-b", "posthog", storageTable("sharded_sessions")))
	writeDriftNode(t, dir, "aux-a", topologyNode("aux-a", "aux", distributedTables(
		[3]string{"events", "posthog", "sharded_events"},
		[3]string{"sessions", "posthog", "sharded_sessions"},
		[3]string{"events_writable", "posthog_writable", "sharded_events"},
		[3]string{"ghost_one", "ghost", "missing_one"},
	)))
	writeDriftNode(t, dir, "aux-b", topologyNode("aux-b", "aux", distributedTables(
		[3]string{"ghost_two", "ghost", "missing_two"},
	)))

	doc, err := validateTopologyDump(dir, validateDumpOptions{Glob: "*"})
	require.NoError(t, err)

	posthog := clusterByName(t, doc, "posthog")
	assert.Equal(t, "schema", posthog.Kind)
	assert.Equal(t, "dump", posthog.Source)
	assert.Equal(t, []string{"data-a", "data-b"}, posthog.Members)
	assert.Equal(t, 2, posthog.Objects, "heterogeneous members contribute a union of object names")

	alias := clusterByName(t, doc, "posthog_writable")
	assert.Equal(t, "alias", alias.Kind)
	assert.Equal(t, "inferred", alias.Source)
	assert.Equal(t, "posthog", alias.Base)

	require.Len(t, doc.UnmappedClusters, 1, "one cluster-level finding, not one error per proxy")
	assert.Equal(t, "ghost", doc.UnmappedClusters[0].Name)
	assert.Equal(t, []string{"aux-a", "aux-b"}, doc.UnmappedClusters[0].ReferencedBy)
	assert.Equal(t, []string{"ghost"}, nodeByName(t, doc, "aux-a").UnmappedClusters)
	assert.Equal(t, []string{"ghost"}, nodeByName(t, doc, "aux-b").UnmappedClusters)
	assert.Empty(t, nodeByName(t, doc, "aux-a").Errors,
		"unioned posthog targets and its inferred alias resolve; duplicate ghost errors are suppressed")
	assert.Equal(t, validateDumpSummary{
		Nodes: 4, Clusters: 3, NodesFailed: 2, UnmappedClusters: 1, Errors: 1,
	}, doc.Summary)

	var text bytes.Buffer
	renderValidateDumpText(&text, doc)
	assert.Contains(t, text.String(), "posthog: 2 nodes, 2 objects (data-a, data-b)")
	assert.Contains(t, text.String(), "posthog_writable: alias of posthog (inferred)")
	assert.Equal(t, 1, bytes.Count(text.Bytes(), []byte(`cluster "ghost"`)))

	raw, err := json.Marshal(doc)
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))
	assert.Contains(t, decoded, "clusters")
	assert.Contains(t, decoded, "nodes")
	assert.Contains(t, decoded, "summary")
}

func TestValidateTopologyDump_ExplicitClusterOverridesDerived(t *testing.T) {
	dir := t.TempDir()
	writeDriftNode(t, dir, "data-a", topologyNode("data-a", "posthog", storageTable("sharded_events")))
	writeDriftNode(t, dir, "aux-a", topologyNode("aux-a", "aux", distributedTables(
		[3]string{"events", "posthog", "not_in_derived_schema"},
		[3]string{"events_writable", "posthog_writable", "also_missing"},
	)))

	doc, err := validateTopologyDump(dir, validateDumpOptions{
		Glob: "*",
		ClusterEntries: []clusterEntry{
			{name: "posthog", stack: absentStack},
		},
	})
	require.NoError(t, err)

	posthog := clusterByName(t, doc, "posthog")
	assert.Equal(t, "absent", posthog.Kind)
	assert.Equal(t, "explicit", posthog.Source)
	assert.Empty(t, posthog.Members, "the explicit mapping replaces the dump-derived union")
	alias := clusterByName(t, doc, "posthog_writable")
	assert.Equal(t, "posthog", alias.Base, "alias inference follows the explicit base mapping")
	assert.Empty(t, doc.UnmappedClusters)
	assert.Zero(t, doc.Summary.Errors)
}

func TestValidateTopologyDump_ReportsCrossClusterColumnFindings(t *testing.T) {
	dir := t.TempDir()
	writeDriftNode(t, dir, "data-a", topologyNode("data-a", "posthog", storageTable("sharded_events")))
	writeDriftNode(t, dir, "sessions-a", topologyNode("sessions-a", "sessions", `
database "posthog" {
  table "events" {
    column "id"      { type = "UInt64" }
    column "mat_bad" { type = "String" }
    engine "distributed" {
      cluster_name    = "posthog"
      remote_database = "posthog"
      remote_table    = "sharded_events"
    }
  }
}
`))

	doc, err := validateTopologyDump(dir, validateDumpOptions{Glob: "*"})
	require.NoError(t, err)
	failures := nodeByName(t, doc, "sessions-a").Errors
	require.Len(t, failures, 1)
	assert.Equal(t, hclload.KindDistributedColumn, failures[0].Kind)
	assert.Equal(t, "posthog", failures[0].Cluster)
	assert.Equal(t, "posthog.mat_bad", failures[0].Missing)
	assert.Contains(t, failures[0].Reason, `column "mat_bad" is not present`)
}

func TestValidateTopologyDump_StrictClustersAppliesToExplicitAbsent(t *testing.T) {
	dir := t.TempDir()
	writeDriftNode(t, dir, "aux-a", topologyNode("aux-a", "aux", distributedTables(
		[3]string{"events", "posthog", "sharded_events"},
	)))

	doc, err := validateTopologyDump(dir, validateDumpOptions{
		Glob:     "*",
		Validate: hclload.ValidateOptions{StrictClusters: true},
		ClusterEntries: []clusterEntry{
			{name: "posthog", stack: absentStack},
		},
	})
	require.NoError(t, err)
	require.Len(t, doc.Nodes, 1)
	require.Len(t, doc.Nodes[0].Errors, 1)
	assert.Equal(t, "posthog", doc.Nodes[0].Errors[0].Cluster)
	assert.Contains(t, doc.Nodes[0].Errors[0].Reason, "-strict-clusters")
	assert.Equal(t, 1, doc.Summary.Errors)
}

func TestValidateTopologyDump_SkipSuppressesUnmappedCluster(t *testing.T) {
	dir := t.TempDir()
	writeDriftNode(t, dir, "aux-a", topologyNode("aux-a", "aux", distributedTables(
		[3]string{"ignored_proxy", "ghost", "missing"},
	)))

	doc, err := validateTopologyDump(dir, validateDumpOptions{
		Glob: "*",
		Skip: hclload.ParseSkipSet("ignored_proxy"),
	})
	require.NoError(t, err)
	assert.Empty(t, doc.UnmappedClusters)
	assert.Empty(t, doc.Nodes[0].UnmappedClusters)
	assert.Zero(t, doc.Summary.Errors)
}

func TestValidateTopologyDump_ExcludeAppliesBeforeClusterDiscovery(t *testing.T) {
	dir := t.TempDir()
	writeDriftNode(t, dir, "aux-a", topologyNode("aux-a", "aux", distributedTables(
		[3]string{"ignored_proxy", "ghost", "missing"},
	)))

	doc, err := validateTopologyDump(dir, validateDumpOptions{
		Glob:    "*",
		Exclude: hclload.NewExcludeMatcher("ignored_proxy"),
	})
	require.NoError(t, err)
	assert.Empty(t, doc.UnmappedClusters)
	assert.Zero(t, doc.Summary.Errors)
	assert.Equal(t, 0, clusterByName(t, doc, "aux").Objects)
}

func TestInferredDumpClusterBase_KnownSuffixes(t *testing.T) {
	mappings := map[string]validateDumpCluster{
		"posthog": {Name: "posthog", Kind: "schema", Source: "dump"},
	}
	for _, alias := range []string{
		"posthog_writable",
		"posthog_single_shard",
		"posthog_primary_replica",
	} {
		assert.Equal(t, "posthog", inferredDumpClusterBase(alias, mappings), alias)
	}
	assert.Empty(t, inferredDumpClusterBase("posthog_unknown_variant", mappings))
}

func TestValidateTopologyDump_GlobAndUnclusteredNode(t *testing.T) {
	dir := t.TempDir()
	writeDriftNode(t, dir, "selected", topologyNode("selected", "posthog", storageTable("events")))
	writeDriftNode(t, dir, "ignored", topologyNode("ignored", "other", storageTable("other_events")))
	writeDriftNode(t, dir, "legacy", storageTable("legacy_events"))

	doc, err := validateTopologyDump(dir, validateDumpOptions{Glob: "selected.hcl,legacy.hcl"})
	require.NoError(t, err)
	assert.Equal(t, 2, doc.Summary.Nodes)
	assert.Equal(t, []string{"legacy"}, doc.UnclusteredNodes)
	assert.Equal(t, []string{"selected"}, clusterByName(t, doc, "posthog").Members)
	assert.Equal(t, []string{"legacy", "selected"}, []string{doc.Nodes[0].Node, doc.Nodes[1].Node})
}
