package sqlgen

import (
	"testing"

	"github.com/posthog/chschema/gen/chschema_v1"
	"github.com/posthog/chschema/internal/diff"
	"github.com/stretchr/testify/require"
)

func TestSQLGenerator_GenerateCreateTable(t *testing.T) {
	generator := NewSQLGenerator()
	database := "test_db"

	table := &chschema_v1.Table{
		Name:     "users",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "name", Type: "String"},
		},
		OrderBy: []string{"id"},
		Engine: &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_MergeTree{
				MergeTree: &chschema_v1.MergeTree{},
			},
		},
	}

	sql := generator.GenerateCreateTable(table)

	require.Contains(t, sql, "CREATE TABLE test_db.users")
	require.Contains(t, sql, "id UInt64")
	require.Contains(t, sql, "name String")
	require.Contains(t, sql, "ENGINE = MergeTree()")
	require.Contains(t, sql, "ORDER BY (id)")
}

func TestSQLGenerator_GenerateCreateTable_WithDefaults(t *testing.T) {
	generator := NewSQLGenerator()
	database := "test_db"
	defaultExpr := "now()"

	table := &chschema_v1.Table{
		Name:     "events",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UUID"},
			{Name: "created_at", Type: "DateTime", DefaultExpression: &defaultExpr},
		},
		OrderBy: []string{"id"},
		Engine: &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_MergeTree{
				MergeTree: &chschema_v1.MergeTree{},
			},
		},
	}

	sql := generator.GenerateCreateTable(table)

	require.Contains(t, sql, "id UUID")
	require.Contains(t, sql, "created_at DateTime DEFAULT now()")
}

func TestSQLGenerator_GenerateCreateTable_ReplicatedMergeTree(t *testing.T) {
	generator := NewSQLGenerator()
	database := "test_db"

	table := &chschema_v1.Table{
		Name:     "events",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UUID"},
		},
		OrderBy: []string{"id"},
		Engine: &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_ReplicatedMergeTree{
				ReplicatedMergeTree: &chschema_v1.ReplicatedMergeTree{
					ZooPath:     "/clickhouse/tables/events",
					ReplicaName: "replica-1",
				},
			},
		},
	}

	sql := generator.GenerateCreateTable(table)

	require.Contains(t, sql, "ENGINE = ReplicatedMergeTree('/clickhouse/tables/events', 'replica-1')")
}

func TestSQLGenerator_GenerateActionSQL_CreateTable(t *testing.T) {
	generator := NewSQLGenerator()
	database := "test_db"

	table := &chschema_v1.Table{
		Name:     "users",
		Database: &database,
		Columns: []*chschema_v1.Column{
			{Name: "id", Type: "UInt64"},
		},
		OrderBy: []string{"id"},
		Engine: &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_MergeTree{
				MergeTree: &chschema_v1.MergeTree{},
			},
		},
	}

	action := diff.Action{
		Type:    diff.ActionCreateTable,
		Payload: table,
		Reason:  "Test reason",
	}

	sql, err := generator.GenerateActionSQL(action)
	require.NoError(t, err)
	require.Contains(t, sql, "CREATE TABLE test_db.users")
}

func TestSQLGenerator_GenerateActionSQL_DropTable(t *testing.T) {
	generator := NewSQLGenerator()

	action := diff.Action{
		Type:    diff.ActionDropTable,
		Payload: "test_table",
		Reason:  "Test reason",
	}

	sql, err := generator.GenerateActionSQL(action)
	require.NoError(t, err)
	require.Equal(t, "DROP TABLE test_table", sql)
}

func TestSQLGenerator_GenerateActionSQL_AddColumn(t *testing.T) {
	generator := NewSQLGenerator()

	action := diff.Action{
		Type: diff.ActionAddColumn,
		Payload: map[string]interface{}{
			"table":  "users",
			"column": &chschema_v1.Column{Name: "email", Type: "String"},
		},
		Reason: "Test reason",
	}

	sql, err := generator.GenerateActionSQL(action)
	require.NoError(t, err)
	require.Equal(t, "ALTER TABLE users ADD COLUMN email String", sql)
}

func TestSQLGenerator_GenerateActionSQL_DropColumn(t *testing.T) {
	generator := NewSQLGenerator()

	action := diff.Action{
		Type: diff.ActionDropColumn,
		Payload: map[string]interface{}{
			"table":       "users",
			"column_name": "old_email",
		},
		Reason: "Test reason",
	}

	sql, err := generator.GenerateActionSQL(action)
	require.NoError(t, err)
	require.Equal(t, "ALTER TABLE users DROP COLUMN old_email", sql)
}
