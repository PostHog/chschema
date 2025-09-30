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

var updateSnapshots = flag.Bool("update-snapshots", false, "update SQL snapshots")

func TestIntegration_EventsTable_CreateSQL(t *testing.T) {
	// Load events.yaml file
	schemaDir := filepath.Join("..", "schema")
	schemaLoader := loader.NewSchemaLoader(schemaDir)
	desired, err := schemaLoader.Load()
	require.NoError(t, err, "Failed to load schema")

	// Verify events table was loaded
	eventsTable, exists := desired.Tables["events"]
	require.True(t, exists, "events table should exist in loaded schema")
	require.NotNil(t, eventsTable, "events table should not be nil")

	// Create empty current state (simulating new database)
	current := loader.NewDesiredState()

	// Create a state with only the events table for this test
	eventsOnlyState := loader.NewDesiredState()
	eventsOnlyState.Tables["events"] = eventsTable

	// Generate diff plan (comparing events-only state vs empty state)
	differ := diff.NewDiffer()
	plan, err := differ.Plan(eventsOnlyState, current)
	require.NoError(t, err, "Failed to create diff plan")

	// Should have exactly one CREATE_TABLE action for events
	require.Len(t, plan.Actions, 1, "Expected exactly 1 action for events table")
	require.Equal(t, diff.ActionCreateTable, plan.Actions[0].Type, "Expected CREATE_TABLE action")

	// Generate SQL
	sqlGen := sqlgen.NewSQLGenerator()
	sqlStatements, err := sqlGen.GenerateSQL(plan.Actions)
	require.NoError(t, err, "Failed to generate SQL")
	require.Len(t, sqlStatements, 1, "Expected exactly 1 SQL statement")

	// Compare with snapshot
	snapshotPath := filepath.Join("testdata", "snapshots", "events_create.sql")
	expectedSQL := readOrCreateSnapshot(t, snapshotPath, sqlStatements[0])

	require.Equal(t, normalizeSQL(expectedSQL), normalizeSQL(sqlStatements[0]),
		"Generated SQL should match snapshot. Use -update-snapshots to update.")
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
