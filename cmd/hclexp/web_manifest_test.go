package main

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

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
