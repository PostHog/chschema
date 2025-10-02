package loader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaLoader_Load(t *testing.T) {
	// Create a temporary directory for test fixtures
	tempDir, err := os.MkdirTemp("", "chschema_test")
	require.NoError(t, err, "Failed to create temp dir")
	defer os.RemoveAll(tempDir)

	// Create test directory structure
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "tables"), 0755), "Failed to create tables dir")
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "clusters"), 0755), "Failed to create clusters dir")

	// Create test table YAML file
	tableYAML := `name: users
database: myapp
columns:
  - name: id
    type: UInt64
  - name: name
    type: String
  - name: email
    type: String
orderBy:
  - id
`
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "tables", "users.yaml"), []byte(tableYAML), 0644), "Failed to write table YAML")

	// Create test cluster YAML file
	clusterYAML := `name: production
nodes:
  - host: localhost
    port: 9000
    shard: 1
    replica: 1
`
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "clusters", "production.yaml"), []byte(clusterYAML), 0644), "Failed to write cluster YAML")

	// Test the loader
	loader := NewSchemaLoader(tempDir)
	state, err := loader.Load()
	require.NoError(t, err, "Failed to load schema")

	// Verify loaded tables
	require.Equal(t, 1, len(state.Tables), "Expected 1 table")

	table := state.Tables[0]
	require.Equal(t, "users", table.Name, "Expected table name 'users'")
	require.NotNil(t, table.Database, "Expected database to be set")
	require.Equal(t, "myapp", *table.Database, "Expected database 'myapp'")
	require.Equal(t, 3, len(table.Columns), "Expected 3 columns")
	require.Equal(t, 1, len(table.OrderBy), "Expected 1 orderBy field")
	require.Equal(t, "id", table.OrderBy[0], "Expected orderBy ['id']")

	// Verify loaded clusters
	require.Equal(t, 1, len(state.Clusters), "Expected 1 cluster")

	cluster := state.Clusters[0]
	require.Equal(t, "production", cluster.Name, "Expected cluster name 'production'")
	require.Equal(t, 1, len(cluster.Nodes), "Expected 1 node")
	require.Equal(t, "localhost", cluster.Nodes[0].Host, "Expected host 'localhost'")
	require.Equal(t, int32(9000), cluster.Nodes[0].Port, "Expected port 9000")
}

func TestSchemaLoader_Load_EmptyDirectory(t *testing.T) {
	// Create a temporary empty directory
	tempDir, err := os.MkdirTemp("", "chschema_test_empty")
	require.NoError(t, err, "Failed to create temp dir")
	defer os.RemoveAll(tempDir)

	// Test loading from empty directory
	loader := NewSchemaLoader(tempDir)
	state, err := loader.Load()
	require.NoError(t, err, "Failed to load from empty directory")

	// Verify empty state
	require.Equal(t, 0, len(state.Tables), "Expected 0 tables")
	require.Equal(t, 0, len(state.Clusters), "Expected 0 clusters")
}

func TestSchemaLoader_Load_InvalidYAML(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "chschema_test_invalid")
	require.NoError(t, err, "Failed to create temp dir")
	defer os.RemoveAll(tempDir)

	// Create tables directory
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "tables"), 0755), "Failed to create tables dir")

	// Create invalid YAML file
	invalidYAML := `name: users
invalid: [unclosed bracket
columns:
  - name: id
`
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "tables", "invalid.yaml"), []byte(invalidYAML), 0644), "Failed to write invalid YAML")

	// Test the loader - should fail
	loader := NewSchemaLoader(tempDir)
	_, err = loader.Load()
	require.Error(t, err, "Expected error when loading invalid YAML")
}

func TestSchemaLoader_Load_NonExistentDirectory(t *testing.T) {
	// Test loading from non-existent directory
	loader := NewSchemaLoader("/non/existent/path")
	state, err := loader.Load()
	require.NoError(t, err, "Failed to load from non-existent directory")

	// Should return empty state without error
	require.Equal(t, 0, len(state.Tables), "Expected 0 tables")
	require.Equal(t, 0, len(state.Clusters), "Expected 0 clusters")
}
