package executor

import (
	"context"
	"fmt"

	"github.com/posthog/chschema/internal/diff"
	"github.com/posthog/chschema/internal/sqlgen"
	"github.com/rs/zerolog/log"

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
		log.Debug().Msg("No actions to execute")
		return nil
	}

	log.Info().Int("action_count", len(plan.Actions)).Msg("Executing plan")

	for i, action := range plan.Actions {
		ddl, err := e.sqlGen.GenerateActionSQL(action)
		if err != nil {
			return fmt.Errorf("failed to generate DDL for action %s: %w", action.Type, err)
		}

		if ddl == "" {
			log.Warn().Str("action_type", string(action.Type)).Msg("Skipping action: no DDL generated")
			continue
		}

		log.Info().
			Str("action_type", string(action.Type)).
			Str("sql", ddl).
			Int("action_number", i+1).
			Msg("Executing DDL")

		if err := e.conn.Exec(ctx, ddl); err != nil {
			log.Error().Err(err).Str("sql", ddl).Msg("Failed to execute DDL")
			return fmt.Errorf("failed to execute DDL: %w", err)
		}
	}

	log.Info().Int("actions_executed", len(plan.Actions)).Msg("Plan executed successfully")
	return nil
}
