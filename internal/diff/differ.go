package diff

import (
	"fmt"
	"sort"

	"github.com/posthog/chschema/gen/chschema_v1"
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
func (d *Differ) Plan(desired, current *chschema_v1.NodeSchemaState) (*Plan, error) {
	plan := &Plan{}
	d.compareTables(plan, desired, current)
	return plan, nil
}

func listToMap[T any](list []T, getKey func(T) string) map[string]T {
	result := make(map[string]T)
	for _, v := range list {
		result[getKey(v)] = v
	}
	return result
}

// tablesToMap converts a slice of tables to a map keyed by table name
func tablesToMap(tables []*chschema_v1.Table) map[string]*chschema_v1.Table {
	return listToMap(tables, func(table *chschema_v1.Table) string {
		return table.Name
	})
}

func (d *Differ) compareTables(plan *Plan, desired, current *chschema_v1.NodeSchemaState) {
	// Convert slices to maps for easy lookup
	desiredMap := tablesToMap(desired.Tables)
	currentMap := tablesToMap(current.Tables)

	// Get sorted table names for deterministic ordering
	desiredTableNames := make([]string, 0, len(desiredMap))
	for name := range desiredMap {
		desiredTableNames = append(desiredTableNames, name)
	}
	sort.Strings(desiredTableNames)

	currentTableNames := make([]string, 0, len(currentMap))
	for name := range currentMap {
		currentTableNames = append(currentTableNames, name)
	}
	sort.Strings(currentTableNames)

	// Check for tables to create (in sorted order)
	for _, name := range desiredTableNames {
		if _, exists := currentMap[name]; !exists {
			plan.Actions = append(plan.Actions, Action{
				Type:    ActionCreateTable,
				Payload: desiredMap[name],
				Reason:  fmt.Sprintf("Table %s is defined in schema but does not exist in the database.", name),
			})
		}
	}

	// Check for tables to drop (in sorted order)
	for _, name := range currentTableNames {
		if _, exists := desiredMap[name]; !exists {
			plan.Actions = append(plan.Actions, Action{
				Type:    ActionDropTable,
				Payload: name, // Just need the name to drop
				Reason:  fmt.Sprintf("Table %s exists in the database but is not defined in the schema.", name),
			})
		}
	}

	// Check for tables to modify (columns) (in sorted order)
	for _, name := range desiredTableNames {
		if currentTable, exists := currentMap[name]; exists {
			d.compareColumns(plan, desiredMap[name], currentTable)
		}
	}
}

func (d *Differ) compareColumns(plan *Plan, desiredTable, currentTable *chschema_v1.Table) {
	// Check for columns to add
	for _, desiredColumn := range desiredTable.Columns {
		if chschema_v1.FindColumnByName(currentTable.Columns, desiredColumn.Name) == nil {
			plan.Actions = append(plan.Actions, Action{
				Type:    ActionAddColumn,
				Payload: map[string]interface{}{"table": desiredTable.Name, "column": desiredColumn},
				Reason:  fmt.Sprintf("Column %s.%s is defined in schema but does not exist in the table.", desiredTable.Name, desiredColumn.Name),
			})
		}
	}

	// Check for columns to drop
	for _, currentColumn := range currentTable.Columns {
		if chschema_v1.FindColumnByName(desiredTable.Columns, currentColumn.Name) == nil {
			plan.Actions = append(plan.Actions, Action{
				Type:    ActionDropColumn,
				Payload: map[string]interface{}{"table": desiredTable.Name, "column_name": currentColumn.Name},
				Reason:  fmt.Sprintf("Column %s.%s exists in the table but is not defined in the schema.", desiredTable.Name, currentColumn.Name),
			})
		}
	}
}
