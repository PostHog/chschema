package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tableLayer(table string) string {
	return `database "posthog" {
  table "` + table + `" {
    engine "merge_tree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
}
`
}

// manifestFixture lays out a manifest with two roles in one env, plus a second
// env for one role, and returns the root dir.
func manifestFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "layers/ops/s.hcl"), tableLayer("ops_table"))
	writeFileT(t, filepath.Join(root, "layers/data/s.hcl"), tableLayer("data_table"))
	writeFileT(t, filepath.Join(root, "manifest.hcl"), `role "ops" {
  env "prod-us" { layers = ["layers/ops"] }
  env "prod-eu" { layers = ["layers/ops"] }
}
role "data" {
  env "prod-us" { layers = ["layers/data"] }
}
`)
	return root
}

func getMulti(t *testing.T, ms *multiServer, target string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	ms.handler().ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func TestManifestCompositions(t *testing.T) {
	root := manifestFixture(t)
	mf := filepath.Join(root, "manifest.hcl")

	all, err := manifestCompositions(mf, "")
	require.NoError(t, err)
	require.Len(t, all, 3) // ops/prod-us, ops/prod-eu, data/prod-us

	us, err := manifestCompositions(mf, "prod-us")
	require.NoError(t, err)
	require.Len(t, us, 2)
	for _, c := range us {
		assert.Equal(t, "prod-us", c.Env)
	}

	_, err = manifestCompositions(mf, "nope")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `no compositions for env "nope"`)
}

func TestWebManifest_BrowseSchemas(t *testing.T) {
	root := manifestFixture(t)
	comps, err := manifestCompositions(filepath.Join(root, "manifest.hcl"), "")
	require.NoError(t, err)
	ms, err := buildMultiServer(comps, root, 0)
	require.NoError(t, err)

	// Top-level list shows every env/role and links to each base path.
	code, body := getMulti(t, ms, "/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "prod-us")
	assert.Contains(t, body, "prod-eu")
	assert.Contains(t, body, `href="/s/prod-us/ops/"`)
	assert.Contains(t, body, `href="/s/prod-us/data/"`)

	// Each schema browses its own objects under its prefix.
	code, body = getMulti(t, ms, "/s/prod-us/ops/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "ops_table")
	assert.NotContains(t, body, "data_table")
	assert.Contains(t, body, "prod-us / ops", "nav shows the schema label")
	// Object links are prefixed with the schema base path.
	assert.Contains(t, body, `href="/s/prod-us/ops/db/posthog/table/ops_table"`)

	code, body = getMulti(t, ms, "/s/prod-us/data/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "data_table")
	assert.NotContains(t, body, "ops_table")

	// Object detail page resolves through the prefix.
	code, body = getMulti(t, ms, "/s/prod-us/ops/db/posthog/table/ops_table?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "ops_table")

	// Unknown schema -> 404.
	code, _ = getMulti(t, ms, "/s/prod-us/nope/")
	assert.Equal(t, http.StatusNotFound, code)
}

func TestWebManifest_LookupAcrossAndWithinSchemas(t *testing.T) {
	root := manifestFixture(t)
	comps, err := manifestCompositions(filepath.Join(root, "manifest.hcl"), "")
	require.NoError(t, err)
	ms, err := buildMultiServer(comps, root, 0)
	require.NoError(t, err)

	code, body := getMulti(t, ms, "/lookup?q=ops_table")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `href="/s/prod-us/ops/db/posthog/table/ops_table"`)
	assert.Contains(t, body, `href="/s/prod-eu/ops/db/posthog/table/ops_table"`)
	assert.Contains(t, body, "prod-us / ops")
	assert.Contains(t, body, "prod-eu / ops")
	assert.NotContains(t, body, "prod-us / data")

	code, body = getMulti(t, ms, "/s/prod-us/ops/lookup?q=ops")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `href="/s/prod-us/ops/db/posthog/table/ops_table"`)
	assert.NotContains(t, body, "prod-eu / ops")
}

func TestWebDump_BrowseAndLookupNodes(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "node-a.hcl"), `node "node-a" {
  macros = { cluster = "cluster-a", hostClusterRole = "data" }
}
`+tableLayer("events_a"))
	writeFileT(t, filepath.Join(root, "node-b.hcl"), `node "node-b" {
  macros = { cluster = "cluster-b", hostClusterRole = "data" }
}
`+tableLayer("events_b"))

	ms, err := buildDumpMultiServer(root, "*", 0)
	require.NoError(t, err)
	require.Len(t, ms.servers, 2)

	code, body := getMulti(t, ms, "/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "Dump nodes")
	assert.Contains(t, body, "cluster-a")
	assert.Contains(t, body, `href="/n/node-a/"`)
	assert.Contains(t, body, `href="/n/node-b/"`)

	code, body = getMulti(t, ms, "/n/node-a/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "events_a")
	assert.NotContains(t, body, "events_b")
	assert.Contains(t, body, "cluster-a / node-a")

	code, body = getMulti(t, ms, "/lookup?q=events")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `href="/n/node-a/db/posthog/table/events_a"`)
	assert.Contains(t, body, `href="/n/node-b/db/posthog/table/events_b"`)
	assert.Contains(t, body, "cluster-a / node-a")
	assert.Contains(t, body, "cluster-b / node-b")

	filtered, err := buildDumpMultiServer(root, "node-a.hcl", 0)
	require.NoError(t, err)
	require.Len(t, filtered.servers, 1, "-glob filters dump files before mounting")
}

func replicatedTableDump(node, cluster, uuid, columnType string) string {
	return `node "` + node + `" {
  macros = { cluster = "` + cluster + `" }
}
database "posthog" {
  table "events" {
    engine "replicated_merge_tree" {
      zoo_path     = "/clickhouse/tables/` + uuid + `/events"
      replica_name = "{replica}"
    }
    order_by = ["id"]
    column "id" { type = "` + columnType + `" }
  }
}
`
}

func TestWebDump_TablePresenceAndSchemaMarkers(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "node-a.hcl"), replicatedTableDump(
		"node-a", "cluster-a", "11111111-1111-1111-1111-111111111111", "UInt64"))
	writeFileT(t, filepath.Join(root, "node-b.hcl"), replicatedTableDump(
		"node-b", "cluster-a", "22222222-2222-2222-2222-222222222222", "UInt64"))
	writeFileT(t, filepath.Join(root, "node-c.hcl"), replicatedTableDump(
		"node-c", "cluster-b", "33333333-3333-3333-3333-333333333333", "String"))

	ms, err := buildDumpMultiServer(root, "*", time.Second)
	require.NoError(t, err)
	code, body := getMulti(t, ms, "/n/node-a/db/posthog/table/events?view=html")
	require.Equal(t, http.StatusOK, code)

	assert.Contains(t, body, "Across dumped nodes")
	assert.Contains(t, body, "Present on 3 dumped nodes")
	assert.Contains(t, body, `href="/n/node-a/db/posthog/table/events"`)
	assert.Contains(t, body, `href="/n/node-b/db/posthog/table/events"`)
	assert.Contains(t, body, `href="/n/node-c/db/posthog/table/events"`)
	assert.Contains(t, body, `schema-marker schema-current">current`)
	assert.Contains(t, body, `schema-marker schema-same">same`,
		"different replicated table UUIDs are masked like drift")
	assert.Contains(t, body, `schema-marker schema-different">different`,
		"a real column type change is marked different")
	assert.Contains(t, body, "1 differs")

	// Context remains visible on the source views, not only the overview.
	code, body = getMulti(t, ms, "/n/node-a/db/posthog/table/events?view=hcl")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "Across dumped nodes")

	// Visiting one node refreshes every peer before comparing. A change in
	// node-b therefore updates node-a's marker without first opening node-b.
	nodeBFile := filepath.Join(root, "node-b.hcl")
	require.NoError(t, os.WriteFile(nodeBFile, []byte(replicatedTableDump(
		"node-b", "cluster-a", "22222222-2222-2222-2222-222222222222", "String")), 0o600))
	nodeB := ms.servers[nodeBasePath("node-b")]
	future := nodeB.lastCheck.Add(time.Hour)
	nodeB.now = func() time.Time { return future }
	require.NoError(t, os.Chtimes(nodeBFile, future, future))

	code, body = getMulti(t, ms, "/n/node-a/db/posthog/table/events?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "2 differ")
	assert.NotContains(t, body, `schema-marker schema-same">same`)
}

func TestWeb_TablePresenceIsDumpOnly(t *testing.T) {
	srv, err := newWebServer(webTestSchema())
	require.NoError(t, err)
	code, body := getBody(t, srv, "/db/posthog/table/events")
	require.Equal(t, http.StatusOK, code)
	assert.NotContains(t, body, "Across dumped nodes")
}

func TestWebDump_RejectsDuplicateNodeNames(t *testing.T) {
	root := t.TempDir()
	for _, file := range []string{"a.hcl", "b.hcl"} {
		writeFileT(t, filepath.Join(root, file), `node "same-node" {}
`+tableLayer(strings.TrimSuffix(file, ".hcl")))
	}
	_, err := buildDumpMultiServer(root, "*", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `duplicate node name "same-node"`)
}

// TestWebManifest_Example builds the committed examples/manifest fleet through
// the web-manifest path, guarding the example against rot.
func TestWebManifest_Example(t *testing.T) {
	root := filepath.Join("..", "..", "examples", "manifest")
	comps, err := manifestCompositions(filepath.Join(root, "manifest.hcl"), "")
	require.NoError(t, err)
	require.Len(t, comps, 6, "2 roles x 3 envs")

	ms, err := buildMultiServer(comps, root, 0)
	require.NoError(t, err)
	require.Len(t, ms.servers, 6)

	// ops role composes the ops layer (system_metrics); the env patch adds region.
	code, body := getMulti(t, ms, "/s/prod-us/ops/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "system_metrics")
	code, body = getMulti(t, ms, "/s/prod-us/ops/db/posthog/table/events?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "region", "prod-us patch_table adds the region column")

	// data role omits the ops layer; dev adds debug_events.
	code, body = getMulti(t, ms, "/s/dev/data/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "debug_events")
	assert.NotContains(t, body, "system_metrics")
}

func TestWebManifest_EnvFilter(t *testing.T) {
	root := manifestFixture(t)
	comps, err := manifestCompositions(filepath.Join(root, "manifest.hcl"), "prod-eu")
	require.NoError(t, err)
	ms, err := buildMultiServer(comps, root, 0)
	require.NoError(t, err)

	// Only prod-eu/ops is mounted.
	code, body := getMulti(t, ms, "/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "prod-eu")
	assert.NotContains(t, body, "prod-us")

	code, _ = getMulti(t, ms, "/s/prod-eu/ops/")
	assert.Equal(t, http.StatusOK, code)
	code, _ = getMulti(t, ms, "/s/prod-us/ops/")
	assert.Equal(t, http.StatusNotFound, code)
}
