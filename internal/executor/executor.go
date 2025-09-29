package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/posthog/chschema/gen/chschema_v1"
	"github.com/posthog/chschema/internal/diff"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Executor is responsible for applying a plan to the database.
type Executor struct {
	conn clickhouse.Conn
}

// NewExecutor creates a new executor with a given ClickHouse connection.
func NewExecutor(conn clickhouse.Conn) *Executor {
	return &Executor{conn: conn}
}

// Execute applies the actions in the plan to the database.
func (e *Executor) Execute(ctx context.Context, plan *diff.Plan) error {
	if len(plan.Actions) == 0 {
		fmt.Println("No actions to execute.")
		return nil
	}

	fmt.Println("Executing plan...")

	for _, action := range plan.Actions {
		ddl, err := e.generateDDL(action)
		if err != nil {
			return fmt.Errorf("failed to generate DDL for action %s: %w", action.Type, err)
		}

		if ddl == "" {
			fmt.Printf("Skipping action %s: No DDL generated.\n", action.Type)
			continue
		}

		fmt.Printf("Executing: %s\n", ddl)
		if err := e.conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("failed to execute DDL: %w", err)
		}
	}

	fmt.Println("Plan executed successfully.")
	return nil
}

// generateDDL creates the appropriate DDL statement for a given action.
func (e *Executor) generateDDL(action diff.Action) (string, error) {
	switch action.Type {
	case diff.ActionCreateTable:
		table, ok := action.Payload.(*chschema_v1.Table)
		if !ok {
			return "", fmt.Errorf("invalid payload for CREATE_TABLE")
		}
		return generateCreateTableDDL(table), nil

	case diff.ActionDropTable:
		tableName, ok := action.Payload.(string)
		if !ok {
			return "", fmt.Errorf("invalid payload for DROP_TABLE")
		}
		return fmt.Sprintf("DROP TABLE %s", tableName), nil

	case diff.ActionAddColumn:
		payload, ok := action.Payload.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("invalid payload for ADD_COLUMN")
		}
		tableName := payload["table"].(string)
		column := payload["column"].(*chschema_v1.Column)
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, column.Name, column.Type), nil

	case diff.ActionDropColumn:
		payload, ok := action.Payload.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("invalid payload for DROP_COLUMN")
		}
		tableName := payload["table"].(string)
		columnName := payload["column_name"].(string)
		return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tableName, columnName), nil

	default:
		return "", fmt.Errorf("unsupported action type: %s", action.Type)
	}
}

func generateCreateTableDDL(table *chschema_v1.Table) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s.%s (\n", table.Database, table.Name))

	// Columns
	for i, col := range table.Columns {
		sb.WriteString(fmt.Sprintf("  %s %s", col.Name, col.Type))
		if col.DefaultExpression != nil && *col.DefaultExpression != "" {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", col.DefaultExpression))
		}
		if i < len(table.Columns)-1 {
			sb.WriteString(",\n")
		}
	}
	sb.WriteString("\n)")

	// Engine
	sb.WriteString(fmt.Sprintf(" ENGINE = %s", generateEngineString(table.Engine)))

	// Order By
	if len(table.OrderBy) > 0 {
		sb.WriteString(fmt.Sprintf(" ORDER BY (%s)", strings.Join(table.OrderBy, ", ")))
	}

	return sb.String()
}

func generateEngineString(engine *chschema_v1.Engine) string {
	if t := engine.GetReplicatedMergeTree(); t != nil {
		return fmt.Sprintf("ReplicatedMergeTree('%s', '%s')", t.ZooPath, t.ReplicaName)
	}
	// Add other engines here
	return "MergeTree()"
}
