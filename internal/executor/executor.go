package executor

import (
	"context"
	"fmt"

	"github.com/posthog/chschema/internal/diff"
	"github.com/posthog/chschema/internal/sqlgen"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Executor is responsible for applying a plan to the database.
type Executor struct {
	conn   clickhouse.Conn
	sqlGen *sqlgen.SQLGenerator
}

// NewExecutor creates a new executor with a given ClickHouse connection.
func NewExecutor(conn clickhouse.Conn) *Executor {
	return &Executor{
		conn:   conn,
		sqlGen: sqlgen.NewSQLGenerator(),
	}
}

// Execute applies the actions in the plan to the database.
func (e *Executor) Execute(ctx context.Context, plan *diff.Plan) error {
	if len(plan.Actions) == 0 {
		fmt.Println("No actions to execute.")
		return nil
	}

	fmt.Println("Executing plan...")

	for _, action := range plan.Actions {
		ddl, err := e.sqlGen.GenerateActionSQL(action)
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
