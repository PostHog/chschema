package diff

import (
	"testing"

	"github.com/posthog/chschema/gen/chschema_v1"
	"github.com/posthog/chschema/internal/loader"
	"github.com/stretchr/testify/require"
)

func TestDiffer_Plan_EmptyStates(t *testing.T) {
	differ := NewDiffer()
	desired := loader.NewDesiredState()
	current := loader.NewDesiredState()

	plan, err := differ.Plan(desired, current)
	require.NoError(t, err, "Failed to create plan")

	require.Empty(t, plan.Actions, "Expected 0 actions for empty states")
}

func TestDiffer_Plan_CreateTable(t *testing.T) {
	differ := NewDiffer()

	// Desired state has a table
	desired := loader.NewDesiredState()
	database := "myapp"
	usersTable := &chschema_v1.Table{
		Name:     "users",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "name", Type: "String"},
		},
	}
	desired.Tables = append(desired.Tables, usersTable)

	// Current state is empty
	current := loader.NewDesiredState()

	plan, err := differ.Plan(desired, current)
	require.NoError(t, err, "Failed to create plan")

	require.Len(t, plan.Actions, 1, "Expected 1 action")

	action := plan.Actions[0]
	require.Equal(t, ActionCreateTable, action.Type, "Expected ActionCreateTable")
	require.Equal(t, usersTable, action.Payload, "Expected payload to be the table object")

	expectedReason := "Table users is defined in schema but does not exist in the database."
	require.Equal(t, expectedReason, action.Reason, "Expected correct reason")
}

func TestDiffer_Plan_DropTable(t *testing.T) {
	differ := NewDiffer()

	// Desired state is empty
	desired := loader.NewDesiredState()

	// Current state has a table
	current := loader.NewDesiredState()
	database := "myapp"
	current.Tables = append(current.Tables, &chschema_v1.Table{
		Name:     "old_table",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
		},
	})

	plan, err := differ.Plan(desired, current)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	if len(plan.Actions) != 1 {
		t.Fatalf("Expected 1 action, got %d", len(plan.Actions))
	}

	action := plan.Actions[0]
	if action.Type != ActionDropTable {
		t.Errorf("Expected ActionDropTable, got %v", action.Type)
	}

	if action.Payload != "old_table" {
		t.Errorf("Expected payload to be 'old_table', got %v", action.Payload)
	}

	expectedReason := "Table old_table exists in the database but is not defined in the schema."
	if action.Reason != expectedReason {
		t.Errorf("Expected reason '%s', got '%s'", expectedReason, action.Reason)
	}
}

func TestDiffer_Plan_AddColumn(t *testing.T) {
	differ := NewDiffer()
	database := "myapp"

	// Desired state has table with 2 columns
	desired := loader.NewDesiredState()
	desired.Tables = append(desired.Tables, &chschema_v1.Table{
		Name:     "users",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "email", Type: "String"}, // New column
		},
	})

	// Current state has table with 1 column
	current := loader.NewDesiredState()
	current.Tables = append(current.Tables, &chschema_v1.Table{
		Name:     "users",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
		},
	})

	plan, err := differ.Plan(desired, current)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	if len(plan.Actions) != 1 {
		t.Fatalf("Expected 1 action, got %d", len(plan.Actions))
	}

	action := plan.Actions[0]
	if action.Type != ActionAddColumn {
		t.Errorf("Expected ActionAddColumn, got %v", action.Type)
	}

	expectedReason := "Column users.email is defined in schema but does not exist in the table."
	if action.Reason != expectedReason {
		t.Errorf("Expected reason '%s', got '%s'", expectedReason, action.Reason)
	}

	// Verify payload structure
	payload, ok := action.Payload.(map[string]interface{})
	if !ok {
		t.Fatal("Expected payload to be a map")
	}
	if payload["table"] != "users" {
		t.Errorf("Expected table name 'users', got %v", payload["table"])
	}
	column, ok := payload["column"].(*chschema_v1.Column)
	if !ok {
		t.Fatal("Expected column to be a Column object")
	}
	if column.Name != "email" {
		t.Errorf("Expected column name 'email', got '%s'", column.Name)
	}
}

func TestDiffer_Plan_DropColumn(t *testing.T) {
	differ := NewDiffer()
	database := "myapp"

	// Desired state has table with 1 column
	desired := loader.NewDesiredState()
	desired.Tables = append(desired.Tables, &chschema_v1.Table{
		Name:     "users",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
		},
	})

	// Current state has table with 2 columns
	current := loader.NewDesiredState()
	current.Tables = append(current.Tables, &chschema_v1.Table{
		Name:     "users",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "old_email", Type: "String"}, // Column to be dropped
		},
	})

	plan, err := differ.Plan(desired, current)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	if len(plan.Actions) != 1 {
		t.Fatalf("Expected 1 action, got %d", len(plan.Actions))
	}

	action := plan.Actions[0]
	if action.Type != ActionDropColumn {
		t.Errorf("Expected ActionDropColumn, got %v", action.Type)
	}

	expectedReason := "Column users.old_email exists in the table but is not defined in the schema."
	if action.Reason != expectedReason {
		t.Errorf("Expected reason '%s', got '%s'", expectedReason, action.Reason)
	}

	// Verify payload structure
	payload, ok := action.Payload.(map[string]interface{})
	if !ok {
		t.Fatal("Expected payload to be a map")
	}
	if payload["table"] != "users" {
		t.Errorf("Expected table name 'users', got %v", payload["table"])
	}
	if payload["column_name"] != "old_email" {
		t.Errorf("Expected column name 'old_email', got %v", payload["column_name"])
	}
}

func TestDiffer_Plan_ComplexScenario(t *testing.T) {
	differ := NewDiffer()
	database := "myapp"

	// Desired state
	desired := loader.NewDesiredState()
	desired.Tables = append(desired.Tables, &chschema_v1.Table{
		Name:     "users",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "email", Type: "String"}, // New column
		},
	})
	desired.Tables = append(desired.Tables, &chschema_v1.Table{ // New table
		Name:     "products",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "name", Type: "String"},
		},
	})

	// Current state
	current := loader.NewDesiredState()
	current.Tables = append(current.Tables, &chschema_v1.Table{
		Name:     "users",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "old_name", Type: "String"}, // Column to be dropped
		},
	})
	current.Tables = append(current.Tables, &chschema_v1.Table{ // Table to be dropped
		Name:     "legacy_table",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
		},
	})

	plan, err := differ.Plan(desired, current)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	// Should have 4 actions: CREATE_TABLE, DROP_TABLE, ADD_COLUMN, DROP_COLUMN
	if len(plan.Actions) != 4 {
		t.Fatalf("Expected 4 actions, got %d", len(plan.Actions))
	}

	// Count action types
	actionCounts := make(map[ActionType]int)
	for _, action := range plan.Actions {
		actionCounts[action.Type]++
	}

	if actionCounts[ActionCreateTable] != 1 {
		t.Errorf("Expected 1 CREATE_TABLE action, got %d", actionCounts[ActionCreateTable])
	}
	if actionCounts[ActionDropTable] != 1 {
		t.Errorf("Expected 1 DROP_TABLE action, got %d", actionCounts[ActionDropTable])
	}
	if actionCounts[ActionAddColumn] != 1 {
		t.Errorf("Expected 1 ADD_COLUMN action, got %d", actionCounts[ActionAddColumn])
	}
	if actionCounts[ActionDropColumn] != 1 {
		t.Errorf("Expected 1 DROP_COLUMN action, got %d", actionCounts[ActionDropColumn])
	}
}

func TestDiffer_Plan_NoChanges(t *testing.T) {
	differ := NewDiffer()
	database := "myapp"

	// Identical states
	desired := loader.NewDesiredState()
	desired.Tables = append(desired.Tables, &chschema_v1.Table{
		Name:     "users",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "name", Type: "String"},
		},
	})

	current := loader.NewDesiredState()
	current.Tables = append(current.Tables, &chschema_v1.Table{
		Name:     "users",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "name", Type: "String"},
		},
	})

	plan, err := differ.Plan(desired, current)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	if len(plan.Actions) != 0 {
		t.Errorf("Expected 0 actions for identical states, got %d", len(plan.Actions))
	}
}

func Test_FindColumnByName(t *testing.T) {
	columns := []*chschema_v1.Column{
		{Name: "id", Type: "UInt64"},
		{Name: "name", Type: "String"},
		{Name: "email", Type: "String"},
	}

	// Test existing column
	if chschema_v1.FindColumnByName(columns, "name") == nil {
		t.Error("Expected 'name' to exist in columns")
	}

	// Test non-existing column
	if chschema_v1.FindColumnByName(columns, "phone") != nil {
		t.Error("Expected 'phone' to not exist in columns")
	}

	// Test empty columns
	if chschema_v1.FindColumnByName([]*chschema_v1.Column{}, "any") != nil {
		t.Error("Expected no columns to exist in empty slice")
	}

	// Test case sensitivity
	if chschema_v1.FindColumnByName(columns, "NAME") != nil {
		t.Error("Expected case-sensitive comparison to fail")
	}
}
