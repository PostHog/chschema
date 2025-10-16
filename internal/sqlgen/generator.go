package sqlgen

import (
	"fmt"
	"sort"
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

func GenerateCreateTable(table *chschema_v1.Table) string {
	var sb strings.Builder

	database := "default"
	if table.Database != nil {
		database = *table.Database
	}

	sb.WriteString(fmt.Sprintf("CREATE TABLE %s.%s (\n", database, table.Name))

	// Columns
	for i, col := range table.Columns {
		// TODO move the column generation to a separate function
		sb.WriteString(fmt.Sprintf("  `%s` %s", col.Name, col.Type))
		if col.DefaultExpression != nil && *col.DefaultExpression != "" {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", *col.DefaultExpression))
		}
		if col.Codec != nil && *col.Codec != "" {
			sb.WriteString(fmt.Sprintf(" %s", *col.Codec))
		}
		if col.Comment != nil && *col.Comment != "" {
			sb.WriteString(fmt.Sprintf(" COMMENT '%s'", *col.Comment))
		}
		if i < len(table.Columns)-1 || len(table.Indexes) > 0 {
			sb.WriteString(",\n")
		}
	}

	// Indexes
	for i, idx := range table.Indexes {
		sb.WriteString(fmt.Sprintf("  INDEX `%s` %s TYPE %s GRANULARITY %d",
			idx.Name, idx.Expression, idx.Type, idx.Granularity))
		if i < len(table.Indexes)-1 {
			sb.WriteString(",\n")
		}
	}

	sb.WriteString("\n)")

	// Engine
	sb.WriteString(fmt.Sprintf(" ENGINE = %s\n", GenerateEngineString(table.Engine)))

	// Partition By
	if table.PartitionBy != nil && *table.PartitionBy != "" {
		sb.WriteString(fmt.Sprintf(" PARTITION BY %s\n", *table.PartitionBy))
	}

	// Order By
	if l := len(table.OrderBy); l == 1 {
		sb.WriteString(fmt.Sprintf(" ORDER BY %s\n", table.OrderBy[0]))
	} else if l > 1 {
		sb.WriteString(fmt.Sprintf(" ORDER BY (%s)\n", strings.Join(table.OrderBy, ", ")))
	}

	// TTL
	if table.Ttl != nil && *table.Ttl != "" {
		sb.WriteString(fmt.Sprintf(" TTL %s\n", *table.Ttl))
	}

	// Settings
	if len(table.Settings) > 0 {
		// Sort setting keys for deterministic output
		keys := make([]string, 0, len(table.Settings))
		for key := range table.Settings {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		var settings []string
		for _, key := range keys {
			settings = append(settings, fmt.Sprintf("%s = %s", key, table.Settings[key]))
		}
		sb.WriteString(fmt.Sprintf(" SETTINGS %s", strings.Join(settings, ", ")))
	}

	return sb.String()
}

// GenerateCreateTable generates a CREATE TABLE statement.
func (g *SQLGenerator) GenerateCreateTable(table *chschema_v1.Table) string {
	return GenerateCreateTable(table)
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
	if column.Codec != nil && *column.Codec != "" {
		sql += fmt.Sprintf(" %s", *column.Codec)
	}

	if column.Comment != nil && *column.Comment != "" {
		sql += fmt.Sprintf(" COMMENT '%s'", *column.Comment)
	}
	return sql
}

// GenerateDropColumn generates an ALTER TABLE DROP COLUMN statement.
func (g *SQLGenerator) GenerateDropColumn(tableName, columnName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tableName, columnName)
}

func GenerateEngineString(engine *chschema_v1.Engine) string {
	if engine == nil {
		return "MergeTree()"
	}

	// MergeTree family
	if engine.GetMergeTree() != nil {
		return "MergeTree()"
	}

	if t := engine.GetReplicatedMergeTree(); t != nil {
		return fmt.Sprintf("ReplicatedMergeTree('%s', '%s')", t.ZooPath, t.ReplicaName)
	}

	if t := engine.GetReplacingMergeTree(); t != nil {
		if t.VersionColumn != nil {
			return fmt.Sprintf("ReplacingMergeTree(%s)", *t.VersionColumn)
		}
		return "ReplacingMergeTree()"
	}

	if t := engine.GetReplicatedReplacingMergeTree(); t != nil {
		if t.VersionColumn != nil {
			return fmt.Sprintf("ReplicatedReplacingMergeTree('%s', '%s', %s)", t.ZooPath, t.ReplicaName, *t.VersionColumn)
		}
		return fmt.Sprintf("ReplicatedReplacingMergeTree('%s', '%s')", t.ZooPath, t.ReplicaName)
	}

	if t := engine.GetSummingMergeTree(); t != nil {
		if len(t.SumColumns) > 0 {
			return fmt.Sprintf("SummingMergeTree((%s))", strings.Join(t.SumColumns, ", "))
		}
		return "SummingMergeTree()"
	}

	if t := engine.GetCollapsingMergeTree(); t != nil {
		return fmt.Sprintf("CollapsingMergeTree(%s)", t.SignColumn)
	}

	if t := engine.GetReplicatedCollapsingMergeTree(); t != nil {
		return fmt.Sprintf("ReplicatedCollapsingMergeTree('%s', '%s', %s)", t.ZooPath, t.ReplicaName, t.SignColumn)
	}

	if engine.GetAggregatingMergeTree() != nil {
		return "AggregatingMergeTree()"
	}

	if t := engine.GetReplicatedAggregatingMergeTree(); t != nil {
		return fmt.Sprintf("ReplicatedAggregatingMergeTree('%s', '%s')", t.ZooPath, t.ReplicaName)
	}

	// Distributed
	if t := engine.GetDistributed(); t != nil {
		if t.ShardingKey != nil {
			return fmt.Sprintf("Distributed(%s, %s, %s, %s)", t.ClusterName, t.RemoteDatabase, t.RemoteTable, *t.ShardingKey)
		}
		return fmt.Sprintf("Distributed(%s, %s, %s)", t.ClusterName, t.RemoteDatabase, t.RemoteTable)
	}

	// Log
	if engine.GetLog() != nil {
		return "Log()"
	}

	// Kafka
	if t := engine.GetKafka(); t != nil {
		brokerList := strings.Join(t.BrokerList, ",")
		return fmt.Sprintf("Kafka('%s', '%s', '%s', '%s')", brokerList, t.Topic, t.ConsumerGroup, t.Format)
	}

	// Default fallback
	return "MergeTree()"
}

// generateEngineString creates the engine specification string.
func (g *SQLGenerator) generateEngineString(engine *chschema_v1.Engine) string {
	return GenerateEngineString(engine)
}
