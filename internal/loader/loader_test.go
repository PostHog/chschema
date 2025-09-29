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
	if len(state.Tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(state.Tables))
	}

	if table, exists := state.Tables["users"]; !exists {
		t.Error("Expected 'users' table to be loaded")
	} else {
		if table.Name != "users" {
			t.Errorf("Expected table name 'users', got '%s'", table.Name)
		}
		if table.Database == nil || *table.Database != "myapp" {
			t.Errorf("Expected database 'myapp', got %v", table.Database)
		}
		if len(table.Columns) != 3 {
			t.Errorf("Expected 3 columns, got %d", len(table.Columns))
		}
		if len(table.OrderBy) != 1 || table.OrderBy[0] != "id" {
			t.Errorf("Expected orderBy ['id'], got %v", table.OrderBy)
		}
	}

	// Verify loaded clusters
	if len(state.Clusters) != 1 {
		t.Errorf("Expected 1 cluster, got %d", len(state.Clusters))
	}

	if cluster, exists := state.Clusters["production"]; !exists {
		t.Error("Expected 'production' cluster to be loaded")
	} else {
		if cluster.Name != "production" {
			t.Errorf("Expected cluster name 'production', got '%s'", cluster.Name)
		}
		if len(cluster.Nodes) != 1 {
			t.Errorf("Expected 1 node, got %d", len(cluster.Nodes))
		}
		if cluster.Nodes[0].Host != "localhost" {
			t.Errorf("Expected host 'localhost', got '%s'", cluster.Nodes[0].Host)
		}
		if cluster.Nodes[0].Port != 9000 {
			t.Errorf("Expected port 9000, got %d", cluster.Nodes[0].Port)
		}
	}
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
