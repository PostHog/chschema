package test

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/posthog/chschema/gen/chschema_v1"
	"github.com/posthog/chschema/internal/introspection"
	"github.com/posthog/chschema/internal/sqlgen"
	"github.com/posthog/chschema/internal/utils"
	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestLive_Introspection_Engine(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}

	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	testCases := []struct {
		Name  string
		SQL   string
		table *chschema_v1.Table
	}{
		{
			Name: "Complex",
			SQL: `
		CREATE TABLE ` + dbName + `.test_table (
			id UInt64,
			name String,
			age Nullable(UInt8),
			pineapple_on_pizza Bool DEFAULT TRUE,
			created_at DateTime DEFAULT now()
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMM(created_at)
		ORDER BY id
	`,
			table: &chschema_v1.Table{
				Name:     "test_table",
				Database: &dbName,
				Columns: []*chschema_v1.Column{
					{Name: "id", Type: "UInt64"},
					{Name: "name", Type: "String"},
					{Name: "age", Type: "Nullable(UInt8)"},
					{Name: "pineapple_on_pizza", Type: "Bool", DefaultExpression: utils.Ptr("true")},
					{Name: "created_at", Type: "DateTime", DefaultExpression: utils.Ptr("now()")},
				},
				Engine: &chschema_v1.Engine{
					EngineType: &chschema_v1.Engine_MergeTree{
						MergeTree: &chschema_v1.MergeTree{},
					},
				},
				PartitionBy: utils.Ptr("toYYYYMM(created_at)"),
				OrderBy:     []string{"id"},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			err := conn.Exec(ctx, testCase.SQL)
			require.NoError(t, err, "Failed to create test table")

			// Introspect the database
			intro := introspection.NewIntrospector(conn)
			state, err := intro.GetCurrentState(ctx)
			require.NoError(t, err, "Failed to introspect database")

			// Check that our table was found
			gotTable := chschema_v1.FindTableByName(state.Tables, "test_table")
			require.NotNil(t, gotTable, "test_table should be found")

			// Compare using protocmp for proper protobuf comparison
			diff := cmp.Diff(testCase.table, gotTable, protocmp.Transform())
			require.Empty(t, diff, "Table should match expected structure. Diff:\n%s", diff)

			createTableSQL := sqlgen.GenerateCreateTable(gotTable)
			require.Equal(t, simplify(testCase.SQL), simplify(createTableSQL))
		})
	}
}

var whitespaces = regexp.MustCompile(`[\t\n\s]+`)

func simplify(stmt string) string {
	return whitespaces.ReplaceAllString(
		strings.ToLower(
			strings.TrimSpace(stmt)), " ")
}
