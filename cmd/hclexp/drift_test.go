package main

import (
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

func TestSummarizeChanges(t *testing.T) {
	cs := hclload.ChangeSet{
		Databases: []hclload.DatabaseChange{{
			Database:             "posthog",
			AddTables:            []hclload.TableSpec{{Name: "a"}},
			AlterTables:          []hclload.TableDiff{{Table: "b"}, {Table: "c"}},
			AddMaterializedViews: []hclload.MaterializedViewSpec{{Name: "mv"}},
		}},
	}
	assert.Equal(t, "+1 table, ~2 table, +1 mv", summarizeChanges(cs))
	assert.Equal(t, "changed", summarizeChanges(hclload.ChangeSet{}))
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
