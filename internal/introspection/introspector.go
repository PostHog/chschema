package introspection

import (
	"context"
	"fmt"

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
		SELECT database, name, engine, sorting_key, partition_key
		FROM system.tables
		WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
	`)
	if err != nil {
		return fmt.Errorf("failed to query system.tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var db, name, engine, sortingKey, partitionKey string
		if err := rows.Scan(&db, &name, &engine, &sortingKey, &partitionKey); err != nil {
			return fmt.Errorf("failed to scan table row: %w", err)
		}

		table := &chschema_v1.Table{
			Name:        name,
			Database:    &db,
			OrderBy:     []string{sortingKey}, // Simplified
			PartitionBy: &partitionKey,
		}

		if err := i.introspectColumns(ctx, table); err != nil {
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
