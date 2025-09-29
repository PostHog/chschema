package introspection

import (
	"context"
	"fmt"
	"strings"

	"github.com/posthog/chschema/gen/chschema_v1"
	"github.com/posthog/chschema/internal/loader"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Introspector is responsible for querying a ClickHouse cluster to determine its current state.
type Introspector struct {
	conn clickhouse.Conn
}

// NewIntrospector creates a new Introspector with a given ClickHouse connection.
func NewIntrospector(conn clickhouse.Conn) *Introspector {
	return &Introspector{conn: conn}
}

// GetCurrentState queries the system tables to build a model of the current schema.
func (i *Introspector) GetCurrentState(ctx context.Context) (*loader.DesiredState, error) {
	state := loader.NewDesiredState()

	// 1. Introspect Tables and Columns
	if err := i.introspectTables(ctx, state); err != nil {
		return nil, err
	}

	return state, nil
}

func (i *Introspector) introspectTables(ctx context.Context, state *loader.DesiredState) error {
	rows, err := i.conn.Query(ctx, `
		SELECT
			database,
			name,
			engine,
			sorting_key,
			partition_key,
			primary_key,
			total_rows,
			total_bytes
		FROM system.tables
		WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
		ORDER BY database, name
	`)
	if err != nil {
		return fmt.Errorf("failed to query system.tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var db, name, engine, sortingKey, partitionKey, primaryKey string
		var totalRows, totalBytes uint64
		if err := rows.Scan(&db, &name, &engine, &sortingKey, &partitionKey, &primaryKey, &totalRows, &totalBytes); err != nil {
			return fmt.Errorf("failed to scan table row: %w", err)
		}

		table := &chschema_v1.Table{
			Name:     name,
			Database: &db,
		}

		// Parse and set engine information
		if err := i.parseTableEngine(table, engine, sortingKey, partitionKey, primaryKey); err != nil {
			return fmt.Errorf("failed to parse engine for table %s: %w", name, err)
		}

		// Introspect columns
		if err := i.introspectColumns(ctx, table); err != nil {
			return err
		}

		// Get table settings
		if err := i.introspectTableSettings(ctx, table); err != nil {
			return err
		}

		state.Tables[table.Name] = table
	}

	return nil
}

func (i *Introspector) introspectColumns(ctx context.Context, table *chschema_v1.Table) error {
	rows, err := i.conn.Query(ctx, `
		SELECT name, type, default_expression
		FROM system.columns
		WHERE database = ? AND table = ?
	`, table.Database, table.Name)
	if err != nil {
		return fmt.Errorf("failed to query system.columns for table %s: %w", table.Name, err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, colType, defaultExpr string
		if err := rows.Scan(&name, &colType, &defaultExpr); err != nil {
			return fmt.Errorf("failed to scan column row: %w", err)
		}

		column := &chschema_v1.Column{
			Name:              name,
			Type:              colType,
			DefaultExpression: &defaultExpr,
		}
		table.Columns = append(table.Columns, column)
	}

	return nil
}

// parseTableEngine parses engine information and sets table properties
func (i *Introspector) parseTableEngine(table *chschema_v1.Table, engine, sortingKey, partitionKey, primaryKey string) error {
	// Set ORDER BY clause
	if sortingKey != "" {
		table.OrderBy = strings.Split(sortingKey, ", ")
	}

	// Set PARTITION BY clause
	if partitionKey != "" {
		table.PartitionBy = &partitionKey
	}

	// For now, we'll store the engine as a string
	// In a full implementation, you'd parse this into proper engine types
	// table.Engine = &chschema_v1.Engine{Type: engine}

	return nil
}

// introspectTableSettings queries table-specific settings
func (i *Introspector) introspectTableSettings(ctx context.Context, table *chschema_v1.Table) error {
	// Query table settings from system.table_settings or other system tables
	// For now, this is a placeholder - in a full implementation you would
	// query specific settings like index_granularity, etc.

	// Example query (commented out as it might not exist in all ClickHouse versions):
	// rows, err := i.conn.Query(ctx, `
	//     SELECT name, value
	//     FROM system.settings
	//     WHERE name LIKE '%granularity%'
	// `)

	return nil
}
