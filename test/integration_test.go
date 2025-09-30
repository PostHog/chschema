package test

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/posthog/chschema/internal/diff"
	"github.com/posthog/chschema/internal/loader"
	"github.com/posthog/chschema/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

var (
	updateSnapshots = flag.Bool("update-snapshots", false, "update SQL snapshots")
	emptyState      = loader.NewDesiredState()
)

// AssertSQLDiff is a generic function that tests SQL generation by comparing desired and current states
// and validating the generated SQL against a snapshot file.
func AssertSQLDiff(t *testing.T, current, wanted *loader.DesiredState, snapshotPath string) {
	// Generate diff plan
	differ := diff.NewDiffer()
	plan, err := differ.Plan(wanted, current)
	require.NoError(t, err, "Failed to create diff plan")

	// Generate SQL
	sqlGen := sqlgen.NewSQLGenerator()
	sqlStatements, err := sqlGen.GenerateSQL(plan.Actions)
	require.NoError(t, err, "Failed to generate SQL")

	// Combine all SQL statements into a single string
	combinedSQL := strings.Join(sqlStatements, ";\n")

	// Compare with snapshot
	expectedSQL := readOrCreateSnapshot(t, snapshotPath, combinedSQL)

	require.Equal(t, normalizeSQL(expectedSQL), normalizeSQL(combinedSQL),
		"Generated SQL should match snapshot. Use -update-snapshots to update.")
}

func TestIntegration_EventsTable_CreateSQL(t *testing.T) {
	// Load events.yaml file
	schemaDir := filepath.Join("..", "schema")
	schemaLoader := loader.NewSchemaLoader(schemaDir)
	fullSchema, err := schemaLoader.Load()
	require.NoError(t, err, "Failed to load schema")

	// Verify events table was loaded
	eventsTable, exists := fullSchema.Tables["events"]
	require.True(t, exists, "events table should exist in loaded schema")
	require.NotNil(t, eventsTable, "events table should not be nil")

	// Prepare test states
	wanted := loader.NewDesiredState() // Database with events table
	wanted.Tables["events"] = eventsTable

	// Run the generic test
	snapshotPath := filepath.Join("testdata", "snapshots", "events_create.sql")
	AssertSQLDiff(t, emptyState, wanted, snapshotPath)
}

func TestIntegration_UsersTable_CreateSQL(t *testing.T) {
	// Load schema
	schemaDir := filepath.Join("..", "schema")
	schemaLoader := loader.NewSchemaLoader(schemaDir)
	fullSchema, err := schemaLoader.Load()
	require.NoError(t, err, "Failed to load schema")

	// Get users table
	usersTable, exists := fullSchema.Tables["users"]
	require.True(t, exists, "users table should exist in loaded schema")

	// Prepare test states
	wanted := loader.NewDesiredState() // Database with users table
	wanted.Tables["users"] = usersTable

	// Run the generic test
	snapshotPath := filepath.Join("testdata", "snapshots", "users_create.sql")
	AssertSQLDiff(t, emptyState, wanted, snapshotPath)
}

func TestIntegration_BothTables_CreateSQL(t *testing.T) {
	// Load schema
	schemaDir := filepath.Join("..", "schema")
	schemaLoader := loader.NewSchemaLoader(schemaDir)
	fullSchema, err := schemaLoader.Load()
	require.NoError(t, err, "Failed to load schema")

	// Prepare test states
	wanted := loader.NewDesiredState() // Database with both tables
	wanted.Tables["events"] = fullSchema.Tables["events"]
	wanted.Tables["users"] = fullSchema.Tables["users"]

	// Run the generic test
	snapshotPath := filepath.Join("testdata", "snapshots", "both_tables_create.sql")
	AssertSQLDiff(t, emptyState, wanted, snapshotPath)
}

func TestIntegration_DropTable_SQL(t *testing.T) {
	// Load schema
	schemaDir := filepath.Join("..", "schema")
	schemaLoader := loader.NewSchemaLoader(schemaDir)
	fullSchema, err := schemaLoader.Load()
	require.NoError(t, err, "Failed to load schema")

	// Prepare test states - reverse scenario (drop table)
	current := loader.NewDesiredState() // Database with events table
	current.Tables["events"] = fullSchema.Tables["events"]

	// Run the generic test
	snapshotPath := filepath.Join("testdata", "snapshots", "events_drop.sql")
	AssertSQLDiff(t, current, emptyState, snapshotPath)
}

// readOrCreateSnapshot reads a snapshot file or creates it if it doesn't exist (when -update-snapshots is used)
func readOrCreateSnapshot(t *testing.T, snapshotPath, actual string) string {
	if *updateSnapshots {
		// Create snapshot directory if it doesn't exist
		dir := filepath.Dir(snapshotPath)
		err := os.MkdirAll(dir, 0755)
		require.NoError(t, err, "Failed to create snapshot directory")

		// Write the snapshot
		err = os.WriteFile(snapshotPath, []byte(actual), 0644)
		require.NoError(t, err, "Failed to write snapshot file")

		t.Logf("Updated snapshot: %s", snapshotPath)
		return actual
	}

	// Read existing snapshot
	expected, err := os.ReadFile(snapshotPath)
	if os.IsNotExist(err) {
		t.Fatalf("Snapshot file %s does not exist. Run with -update-snapshots to create it.", snapshotPath)
	}
	require.NoError(t, err, "Failed to read snapshot file")

	return string(expected)
}

// normalizeSQL normalizes SQL for comparison by removing extra whitespace
func normalizeSQL(sql string) string {
	// Remove leading/trailing whitespace and normalize internal whitespace
	lines := strings.Split(sql, "\n")
	var normalized []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}

	return strings.Join(normalized, "\n")
}
