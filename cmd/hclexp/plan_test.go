package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFileT(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

const sampleManifest = `
role "ops" {
  env "local"   { layers = ["base", "env/local"] }
  env "prod-us" { layers = ["base", "prod", "env/prod-us"] }
}
role "data" {
  # data is only deployed in prod-us
  env "prod-us" { layers = ["base", "env/prod-us"] }
}
`

func TestParseManifest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.hcl")
	writeFileT(t, path, sampleManifest)

	// prod-us: both roles resolve.
	roles, err := parseManifest(path, "prod-us")
	require.NoError(t, err)
	require.Len(t, roles, 2)
	assert.Equal(t, "ops", roles[0].Role)
	assert.Equal(t, []string{"base", "prod", "env/prod-us"}, roles[0].Layers)
	assert.Equal(t, "data", roles[1].Role)
	assert.Equal(t, []string{"base", "env/prod-us"}, roles[1].Layers)

	// local: only ops is deployed; data is skipped.
	roles, err = parseManifest(path, "local")
	require.NoError(t, err)
	require.Len(t, roles, 1)
	assert.Equal(t, "ops", roles[0].Role)
	assert.Equal(t, []string{"base", "env/local"}, roles[0].Layers)
}

func TestParseManifest_Errors(t *testing.T) {
	dir := t.TempDir()

	noEnv := filepath.Join(dir, "noenv.hcl")
	writeFileT(t, noEnv, sampleManifest)
	_, err := parseManifest(noEnv, "prod-eu")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `no roles deployed in env "prod-eu"`)

	dup := filepath.Join(dir, "dup.hcl")
	writeFileT(t, dup, `role "ops" {
  env "prod-us" { layers = ["a"] }
}
role "ops" {
  env "prod-us" { layers = ["b"] }
}`)
	_, err = parseManifest(dup, "prod-us")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate role")

	dupEnv := filepath.Join(dir, "dupenv.hcl")
	writeFileT(t, dupEnv, `role "ops" {
  env "prod-us" { layers = ["a"] }
  env "prod-us" { layers = ["b"] }
}`)
	_, err = parseManifest(dupEnv, "prod-us")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate env")

	empty := filepath.Join(dir, "empty.hcl")
	writeFileT(t, empty, "# nothing here\n")
	_, err = parseManifest(empty, "prod-us")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no roles")
}

// TestCurrentByRole verifies dump nodes are keyed by their hostClusterRole macro
// and that replicas of a role collapse to one representative (lexically first).
func TestCurrentByRole(t *testing.T) {
	dir := t.TempDir()
	node := func(name, role, replica, table string) string {
		return `node "` + name + `" {
  macros = { cluster = "ops", hostClusterRole = "` + role + `", shard = "1", replica = "` + replica + `" }
}
database "posthog" {
  table "` + table + `" {
    engine "merge_tree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
}
`
	}
	// Two ops replicas (1c lexically first) + one data node.
	writeFileT(t, filepath.Join(dir, "prod-us-iad-ch-1d-ops.hcl"), node("prod-us-iad-ch-1d-ops", "ops", "d", "from_1d"))
	writeFileT(t, filepath.Join(dir, "prod-us-iad-ch-1c-ops.hcl"), node("prod-us-iad-ch-1c-ops", "ops", "c", "from_1c"))
	writeFileT(t, filepath.Join(dir, "prod-us-iad-ch-1a-data.hcl"), node("prod-us-iad-ch-1a-data", "data", "a", "data_tbl"))

	byRole, err := currentByRole(dir)
	require.NoError(t, err)
	require.Contains(t, byRole, "ops")
	require.Contains(t, byRole, "data")
	assert.Len(t, byRole, 2, "two ops replicas collapse to one role entry")

	// The lexically-first ops node (1c) is the representative.
	require.Len(t, byRole["ops"].Databases, 1)
	require.Len(t, byRole["ops"].Databases[0].Tables, 1)
	assert.Equal(t, "from_1c", byRole["ops"].Databases[0].Tables[0].Name)
}
