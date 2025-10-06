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
	conn      clickhouse.Conn
	Databases []string
	Tables    []string

	// Statistics
	DumpedEngines  map[string]int // engine type -> count
	SkippedEngines map[string]int // engine type -> count
}

// NewIntrospector creates a new Introspector with a given ClickHouse connection.
func NewIntrospector(conn clickhouse.Conn) *Introspector {
	return &Introspector{
		conn:           conn,
		DumpedEngines:  make(map[string]int),
		SkippedEngines: make(map[string]int),
	}
}

// GetCurrentState queries the system tables to build a model of the current schema.
func (i *Introspector) GetCurrentState(ctx context.Context) (*chschema_v1.NodeSchemaState, error) {
	state := loader.NewDesiredState()

	// 0. First, get all tables to track what's available
	if err := i.introspectAllTables(ctx); err != nil {
		return nil, err
	}

	// 1. Introspect Tables and Columns
	if err := i.introspectTables(ctx, state); err != nil {
		return nil, err
	}

	// 2. Introspect Materialized Views
	if err := i.introspectMaterializedViews(ctx, state); err != nil {
		return nil, err
	}

	// 3. Introspect Views
	if err := i.introspectViews(ctx, state); err != nil {
		return nil, err
	}

	return state, nil
}

// introspectAllTables queries all tables to build statistics
func (i *Introspector) introspectAllTables(ctx context.Context) error {
	var (
		predicate string
		args      []interface{}
	)
	if len(i.Databases) > 0 {
		predicate = " AND database IN $1"
		args = append(args, i.Databases)
	}
	if len(i.Tables) > 0 {
		predicate += fmt.Sprintf(" AND name IN $%d", len(args)+1)
		args = append(args, i.Tables)
	}

	query := `
		SELECT name, engine
		FROM system.tables
		WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')` +
		predicate +
		`
		ORDER BY engine, name
	`

	rows, err := i.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to query all tables: %w", err)
	}
	defer rows.Close()

	// Build map of engine -> table names
	engineTables := make(map[string][]string)
	for rows.Next() {
		var name, engine string
		if err := rows.Scan(&name, &engine); err != nil {
			return fmt.Errorf("failed to scan table row: %w", err)
		}
		engineTables[engine] = append(engineTables[engine], name)
	}

	// Initialize SkippedEngines with all engines
	for engine, tables := range engineTables {
		i.SkippedEngines[engine] = len(tables)
	}

	return nil
}

func (i *Introspector) introspectTables(ctx context.Context, state *chschema_v1.NodeSchemaState) error {
	var (
		predicate string
		args      []interface{}
	)
	if len(i.Databases) > 0 {
		predicate = " AND database IN $1"
		args = append(args, i.Databases)
	}
	if len(i.Tables) > 0 {
		predicate += fmt.Sprintf(" AND name IN $%d", len(args)+1)
		args = append(args, i.Tables)
	}

	query_sql := `
		SELECT
			database,
			name,
			engine,
			engine_full,
			sorting_key,
			partition_key,
			primary_key,
			total_rows,
			total_bytes
		FROM system.tables
		WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
		  AND engine IN (
			'MergeTree',
			'ReplicatedMergeTree',
			'ReplacingMergeTree',
			'ReplicatedReplacingMergeTree',
			'SummingMergeTree',
			'CollapsingMergeTree',
			'ReplicatedCollapsingMergeTree',
			'AggregatingMergeTree',
			'ReplicatedAggregatingMergeTree',
			'Distributed',
			'Log'
		  )` +
		predicate +
		`
		ORDER BY database, name
	`

	rows, err := i.conn.Query(ctx, query_sql, args...)
	if err != nil {
		return fmt.Errorf("failed to query system.tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var db, name, engine, engineFull, sortingKey, partitionKey, primaryKey string
		var totalRows, totalBytes uint64
		if err := rows.Scan(&db, &name, &engine, &engineFull, &sortingKey, &partitionKey, &primaryKey, &totalRows, &totalBytes); err != nil {
			return fmt.Errorf("failed to scan table row: %w", err)
		}

		table := &chschema_v1.Table{
			Name:     name,
			Database: &db,
		}

		// Parse and set engine information
		if err := i.parseTableEngine(table, engine, engineFull, sortingKey, partitionKey, primaryKey); err != nil {
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

		state.Tables = append(state.Tables, table)

		// Track dumped engine and remove from skipped
		i.DumpedEngines[engine]++
		if i.SkippedEngines[engine] > 0 {
			i.SkippedEngines[engine]--
		}
	}

	return nil
}

func (i *Introspector) introspectColumns(ctx context.Context, table *chschema_v1.Table) error {
	rows, err := i.conn.Query(ctx, `
		SELECT name, type, default_expression, compression_codec, comment
		FROM system.columns
		WHERE database = ? AND table = ?
	`, table.Database, table.Name)
	if err != nil {
		return fmt.Errorf("failed to query system.columns for table %s: %w", table.Name, err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, colType, defaultExprVal, codecVal, commentVal string
		if err := rows.Scan(&name, &colType, &defaultExprVal, &codecVal, &commentVal); err != nil {
			return fmt.Errorf("failed to scan column row: %w", err)
		}
		var defaultExpr *string
		if defaultExprVal != "" {
			defaultExpr = &defaultExprVal
		}

		var codec *string
		if codecVal != "" {
			codec = &codecVal
		}

		var comment *string
		if commentVal != "" {
			comment = &commentVal
		}

		column := &chschema_v1.Column{
			Name:              name,
			Type:              colType,
			DefaultExpression: defaultExpr,
			Codec:             codec,
			Comment:           comment,
		}
		table.Columns = append(table.Columns, column)
	}

	return nil
}

// parseTableEngine parses engine information and sets table properties
func (i *Introspector) parseTableEngine(table *chschema_v1.Table, engine, engineFull, sortingKey, partitionKey, primaryKey string) error {
	// Set ORDER BY clause
	if sortingKey != "" {
		table.OrderBy = strings.Split(sortingKey, ", ")
	}

	// Set PARTITION BY clause
	if partitionKey != "" {
		table.PartitionBy = &partitionKey
	}

	// Parse engine using the engine parser
	parsedEngine, err := ParseEngine(engine, engineFull)
	if err != nil {
		return fmt.Errorf("failed to parse engine: %w", err)
	}
	table.Engine = parsedEngine

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

// introspectMaterializedViews queries materialized views from system.tables
func (i *Introspector) introspectMaterializedViews(ctx context.Context, state *chschema_v1.NodeSchemaState) error {
	var (
		predicate string
		args      []interface{}
	)
	if len(i.Databases) > 0 {
		predicate = " AND database IN $1"
		args = append(args, i.Databases)
	}
	if len(i.Tables) > 0 {
		predicate += fmt.Sprintf(" AND name IN $%d", len(args)+1)
		args = append(args, i.Tables)
	}

	query := `
		SELECT
			database,
			name,
			engine_full,
			as_select
		FROM system.tables
		WHERE engine = 'MaterializedView'
		  AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')` +
		predicate +
		`
		ORDER BY database, name
	`

	rows, err := i.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to query materialized views: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var db, name, engineFull, selectQuery string
		if err := rows.Scan(&db, &name, &engineFull, &selectQuery); err != nil {
			return fmt.Errorf("failed to scan materialized view row: %w", err)
		}

		// Parse destination table from engine_full
		// Format: "MaterializedView" or sometimes includes destination info
		destinationTable := i.parseDestinationTable(engineFull)

		mv := &chschema_v1.MaterializedView{
			Name:             name,
			Database:         &db,
			DestinationTable: destinationTable,
			SelectQuery:      selectQuery,
		}

		state.MaterializedViews = append(state.MaterializedViews, mv)

		// Track dumped materialized views and remove from skipped
		i.DumpedEngines["MaterializedView"]++
		if i.SkippedEngines["MaterializedView"] > 0 {
			i.SkippedEngines["MaterializedView"]--
		}
	}

	return nil
}

// parseDestinationTable extracts the destination table from materialized view engine_full
func (i *Introspector) parseDestinationTable(engineFull string) string {
	// For most materialized views, the destination is implicit (.inner table)
	// This is a placeholder - may need enhancement based on actual engine_full format
	return ""
}

// introspectViews queries regular views from system.tables
func (i *Introspector) introspectViews(ctx context.Context, state *chschema_v1.NodeSchemaState) error {
	var (
		predicate string
		args      []interface{}
	)
	if len(i.Databases) > 0 {
		predicate = " AND database IN $1"
		args = append(args, i.Databases)
	}
	if len(i.Tables) > 0 {
		predicate += fmt.Sprintf(" AND name IN $%d", len(args)+1)
		args = append(args, i.Tables)
	}

	query := `
		SELECT
			database,
			name,
			as_select
		FROM system.tables
		WHERE engine = 'View'
		  AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')` +
		predicate +
		`
		ORDER BY database, name
	`

	rows, err := i.conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to query views: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var db, name, selectQuery string
		if err := rows.Scan(&db, &name, &selectQuery); err != nil {
			return fmt.Errorf("failed to scan view row: %w", err)
		}

		view := &chschema_v1.View{
			Name:        name,
			Database:    &db,
			SelectQuery: selectQuery,
		}

		state.Views = append(state.Views, view)

		// Track dumped views and remove from skipped
		i.DumpedEngines["View"]++
		if i.SkippedEngines["View"] > 0 {
			i.SkippedEngines["View"]--
		}
	}

	return nil
}
