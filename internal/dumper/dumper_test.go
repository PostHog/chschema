package dumper

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/posthog/chschema/gen/chschema_v1"
	"github.com/posthog/chschema/internal/loader"
	"github.com/stretchr/testify/require"
)

func TestDumper_RoundTrip(t *testing.T) {
	originalTable := &chschema_v1.Table{
		Name:     "test_table",
		Database: stringPtr("default"),
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "name", Type: "String"},
			{Name: "created_at", Type: "DateTime", DefaultExpression: stringPtr("now()")},
		},
		Engine: &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_ReplicatedMergeTree{
				ReplicatedMergeTree: &chschema_v1.ReplicatedMergeTree{
					ZooPath:     "/clickhouse/tables/{shard}/test_table",
					ReplicaName: "{replica}",
				},
			},
		},
		OrderBy: []string{"id"},
		Settings: map[string]string{
			"index_granularity": "8192",
		},
		Indexes: []*chschema_v1.Index{
			{
				Name:        "idx_name",
				Type:        "minmax",
				Expression:  "name",
				Granularity: 4,
			},
		},
	}

	engine := &chschema_v1.Engine{}
	originalTable.Engine = engine

	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "dumper_test")
	require.NoError(t, err, "Failed to create temp dir")
	defer os.RemoveAll(tempDir)

	// Create tables subdirectory
	tablesDir := filepath.Join(tempDir, "tables")
	err = os.MkdirAll(tablesDir, 0755)
	require.NoError(t, err, "Failed to create tables directory")

	// Dump table to YAML
	yamlFile := filepath.Join(tablesDir, "test_table.yaml")
	err = WriteYAMLFile(yamlFile, originalTable, true)
	require.NoError(t, err, "Failed to write YAML file")

	// Load table back using loader
	schemaLoader := loader.NewSchemaLoader(tempDir)
	loadedSchema, err := schemaLoader.Load()
	require.NoError(t, err, "Failed to load dumped schema")

	// Get the loaded table
	require.Equal(t, 1, len(loadedSchema.Tables), "Should have exactly one table")
	loadedTable := loadedSchema.Tables[0]
	require.Equal(t, "test_table", loadedTable.Name, "Table name should match")

	// Compare using EqualValues
	require.EqualValues(t, originalTable, loadedTable, "Dumped and loaded table should be identical")
}

func stringPtr(s string) *string {
	return &s
}
