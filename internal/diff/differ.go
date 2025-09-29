package diff

import (
	"fmt"
	"github.com/posthog/chschema/gen/chschema_v1"
	"github.com/posthog/chschema/internal/loader"
)

// ActionType defines the type of DDL action to be taken.
type ActionType string

const (
	ActionCreateTable ActionType = "CREATE_TABLE"
	ActionDropTable   ActionType = "DROP_TABLE"
	ActionAddColumn   ActionType = "ADD_COLUMN"
	ActionDropColumn  ActionType = "DROP_COLUMN"
)

// Action represents a single DDL operation to be performed.
type Action struct {
	Type    ActionType
	Payload interface{}
	Reason  string
}

// Plan is an ordered list of actions to be executed.
type Plan struct {
	Actions []Action
}

// Differ compares the desired and current states to produce a plan.
type Differ struct{}

func NewDiffer() *Differ {
	return &Differ{}
}

// Plan generates a list of actions required to migrate the current state to the desired state.
func (d *Differ) Plan(desired, current *loader.DesiredState) (*Plan, error) {
	plan := &Plan{}
	d.compareTables(plan, desired, current)
	return plan, nil
}

func (d *Differ) compareTables(plan *Plan, desired, current *loader.DesiredState) {
	// Check for tables to create
	for name, desiredTable := range desired.Tables {
		if _, exists := current.Tables[name]; !exists {
			plan.Actions = append(plan.Actions, Action{
				Type:    ActionCreateTable,
				Payload: desiredTable,
				Reason:  fmt.Sprintf("Table %s is defined in schema but does not exist in the database.", name),
			})
		}
	}

	// Check for tables to drop
	for name := range current.Tables {
		if _, exists := desired.Tables[name]; !exists {
			plan.Actions = append(plan.Actions, Action{
				Type:    ActionDropTable,
				Payload: name, // Just need the name to drop
				Reason:  fmt.Sprintf("Table %s exists in the database but is not defined in the schema.", name),
			})
		}
	}

	// Check for tables to modify (columns)
	for name, desiredTable := range desired.Tables {
		if currentTable, exists := current.Tables[name]; exists {
			d.compareColumns(plan, desiredTable, currentTable)
		}
	}
}

func (d *Differ) compareColumns(plan *Plan, desiredTable, currentTable *chschema_v1.Table) {
	currentColumns := make(map[string]*chschema_v1.Column)
	for _, col := range currentTable.Columns {
		currentColumns[col.Name] = col
	}

	// Check for columns to add
	for _, desiredColumn := range desiredTable.Columns {
		if _, exists := currentColumns[desiredColumn.Name]; !exists {
			plan.Actions = append(plan.Actions, Action{
				Type:    ActionAddColumn,
				Payload: map[string]interface{}{"table": desiredTable.Name, "column": desiredColumn},
				Reason:  fmt.Sprintf("Column %s.%s is defined in schema but does not exist in the table.", desiredTable.Name, desiredColumn.Name),
			})
		}
	}

	// Check for columns to drop
	for _, currentColumn := range currentTable.Columns {
		if !columnExistsIn(currentColumn.Name, desiredTable.Columns) {
			plan.Actions = append(plan.Actions, Action{
				Type:    ActionDropColumn,
				Payload: map[string]interface{}{"table": desiredTable.Name, "column_name": currentColumn.Name},
				Reason:  fmt.Sprintf("Column %s.%s exists in the table but is not defined in the schema.", desiredTable.Name, currentColumn.Name),
			})
		}
	}
}

func columnExistsIn(name string, columns []*chschema_v1.Column) bool {
	for _, col := range columns {
		if col.Name == name {
			return true
		}
	}
	return false
}
