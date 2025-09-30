package sqlgen

import (
	"fmt"
	"strings"

	"github.com/posthog/chschema/gen/chschema_v1"
	"github.com/posthog/chschema/internal/diff"
)

// SQLGenerator is responsible for generating DDL SQL statements from actions.
type SQLGenerator struct{}

// NewSQLGenerator creates a new SQL generator.
func NewSQLGenerator() *SQLGenerator {
	return &SQLGenerator{}
}

// GenerateSQL creates SQL DDL statements for all actions in a plan.
func (g *SQLGenerator) GenerateSQL(actions []diff.Action) ([]string, error) {
	var statements []string

	for _, action := range actions {
		sql, err := g.GenerateActionSQL(action)
		if err != nil {
			return nil, fmt.Errorf("failed to generate SQL for action %s: %w", action.Type, err)
		}

		if sql != "" {
			statements = append(statements, sql)
		}
	}

	return statements, nil
}

// GenerateActionSQL creates the appropriate DDL statement for a single action.
func (g *SQLGenerator) GenerateActionSQL(action diff.Action) (string, error) {
	switch action.Type {
	case diff.ActionCreateTable:
		table, ok := action.Payload.(*chschema_v1.Table)
		if !ok {
			return "", fmt.Errorf("invalid payload for CREATE_TABLE")
		}
		return g.GenerateCreateTable(table), nil

	case diff.ActionDropTable:
		tableName, ok := action.Payload.(string)
		if !ok {
			return "", fmt.Errorf("invalid payload for DROP_TABLE")
		}
		return g.GenerateDropTable(tableName), nil

	case diff.ActionAddColumn:
		payload, ok := action.Payload.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("invalid payload for ADD_COLUMN")
		}
		tableName := payload["table"].(string)
		column := payload["column"].(*chschema_v1.Column)
		return g.GenerateAddColumn(tableName, column), nil

	case diff.ActionDropColumn:
		payload, ok := action.Payload.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("invalid payload for DROP_COLUMN")
		}
		tableName := payload["table"].(string)
		columnName := payload["column_name"].(string)
		return g.GenerateDropColumn(tableName, columnName), nil

	default:
		return "", fmt.Errorf("unsupported action type: %s", action.Type)
	}
}

// GenerateCreateTable generates a CREATE TABLE statement.
func (g *SQLGenerator) GenerateCreateTable(table *chschema_v1.Table) string {
	var sb strings.Builder

	database := "default"
	if table.Database != nil {
		database = *table.Database
	}

	sb.WriteString(fmt.Sprintf("CREATE TABLE %s.%s (\n", database, table.Name))

	// Columns
	for i, col := range table.Columns {
		sb.WriteString(fmt.Sprintf("  %s %s", col.Name, col.Type))
		if col.DefaultExpression != nil && *col.DefaultExpression != "" {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", *col.DefaultExpression))
		}
		if i < len(table.Columns)-1 {
			sb.WriteString(",\n")
		}
	}
	sb.WriteString("\n)")

	// Engine
	sb.WriteString(fmt.Sprintf(" ENGINE = %s\n", g.generateEngineString(table.Engine)))

	// Order By
	if len(table.OrderBy) > 0 {
		sb.WriteString(fmt.Sprintf(" ORDER BY (%s)\n", strings.Join(table.OrderBy, ", ")))
	}

	// Partition By
	if table.PartitionBy != nil && *table.PartitionBy != "" {
		sb.WriteString(fmt.Sprintf(" PARTITION BY %s\n", *table.PartitionBy))
	}

	// TTL
	if table.Ttl != nil && *table.Ttl != "" {
		sb.WriteString(fmt.Sprintf(" TTL %s\n", *table.Ttl))
	}

	// Settings
	if len(table.Settings) > 0 {
		var settings []string
		for key, value := range table.Settings {
			settings = append(settings, fmt.Sprintf("%s = %s", key, value))
		}
		sb.WriteString(fmt.Sprintf(" SETTINGS %s", strings.Join(settings, ", ")))
	}

	return sb.String()
}

// GenerateDropTable generates a DROP TABLE statement.
func (g *SQLGenerator) GenerateDropTable(tableName string) string {
	return fmt.Sprintf("DROP TABLE %s", tableName)
}

// GenerateAddColumn generates an ALTER TABLE ADD COLUMN statement.
func (g *SQLGenerator) GenerateAddColumn(tableName string, column *chschema_v1.Column) string {
	sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, column.Name, column.Type)
	if column.DefaultExpression != nil && *column.DefaultExpression != "" {
		sql += fmt.Sprintf(" DEFAULT %s", *column.DefaultExpression)
	}
	return sql
}

// GenerateDropColumn generates an ALTER TABLE DROP COLUMN statement.
func (g *SQLGenerator) GenerateDropColumn(tableName, columnName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tableName, columnName)
}

// generateEngineString creates the engine specification string.
func (g *SQLGenerator) generateEngineString(engine *chschema_v1.Engine) string {
	if engine == nil {
		return "MergeTree()"
	}

	if t := engine.GetReplicatedMergeTree(); t != nil {
		return fmt.Sprintf("ReplicatedMergeTree('%s', '%s')", t.ZooPath, t.ReplicaName)
	}

	if engine.GetMergeTree() != nil {
		return "MergeTree()"
	}

	// Default fallback
	return "MergeTree()"
}
