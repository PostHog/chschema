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

func TestParseManifest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.txt")
	writeFileT(t, path, `# comment line
ops    layers/ops
data   layers/base  layers/data

# trailing comment
`)
	roles, err := parseManifest(path)
	require.NoError(t, err)
	require.Len(t, roles, 2)
	assert.Equal(t, "ops", roles[0].Role)
	assert.Equal(t, []string{"layers/ops"}, roles[0].Layers)
	assert.Equal(t, "data", roles[1].Role)
	assert.Equal(t, []string{"layers/base", "layers/data"}, roles[1].Layers)
}

func TestParseManifest_Errors(t *testing.T) {
	dir := t.TempDir()

	roleOnly := filepath.Join(dir, "roleonly.txt")
	writeFileT(t, roleOnly, "ops\n")
	_, err := parseManifest(roleOnly)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "want '<role>")

	dup := filepath.Join(dir, "dup.txt")
	writeFileT(t, dup, "ops layers/a\nops layers/b\n")
	_, err = parseManifest(dup)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate role")

	empty := filepath.Join(dir, "empty.txt")
	writeFileT(t, empty, "# nothing here\n")
	_, err = parseManifest(empty)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
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
