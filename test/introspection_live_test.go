package test

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"unicode"

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
			props String CODEC(ZSTD(3)),
			props_json JSON,
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
					{Name: "props", Type: "String", Codec: utils.Ptr("CODEC(ZSTD(3))")},
					{Name: "props_json", Type: "JSON"},
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
		{
			Name: "ColumnComments",
			SQL: `
		CREATE TABLE ` + dbName + `.test_comments (
			user_id UInt64 COMMENT 'The unique identifier for the user',
			email String COMMENT 'User email address',
			status String
		) ENGINE = MergeTree()
		ORDER BY user_id
	`,
			table: &chschema_v1.Table{
				Name:     "test_comments",
				Database: &dbName,
				Columns: []*chschema_v1.Column{
					{Name: "user_id", Type: "UInt64", Comment: utils.Ptr("The unique identifier for the user")},
					{Name: "email", Type: "String", Comment: utils.Ptr("User email address")},
					{Name: "status", Type: "String"},
				},
				Engine: &chschema_v1.Engine{
					EngineType: &chschema_v1.Engine_MergeTree{
						MergeTree: &chschema_v1.MergeTree{},
					},
				},
				OrderBy: []string{"user_id"},
			},
		},
		{
			Name: "MinMaxIndex",
			SQL: `
		CREATE TABLE ` + dbName + `.test_minmax_index (
			id UInt64,
			timestamp DateTime,
			value Float64,
			INDEX idx_timestamp_minmax timestamp TYPE minmax GRANULARITY 4
		) ENGINE = MergeTree()
		ORDER BY id
	`,
			table: &chschema_v1.Table{
				Name:     "test_minmax_index",
				Database: &dbName,
				Columns: []*chschema_v1.Column{
					{Name: "id", Type: "UInt64"},
					{Name: "timestamp", Type: "DateTime"},
					{Name: "value", Type: "Float64"},
				},
				Engine: &chschema_v1.Engine{
					EngineType: &chschema_v1.Engine_MergeTree{
						MergeTree: &chschema_v1.MergeTree{},
					},
				},
				OrderBy: []string{"id"},
				Indexes: []*chschema_v1.Index{
					{Name: "idx_timestamp_minmax", Type: "minmax", Expression: "timestamp", Granularity: 4},
				},
			},
		},
		{
			Name: "SetIndex",
			SQL: `
		CREATE TABLE ` + dbName + `.test_set_index (
			id UInt64,
			category String,
			tags Array(String),
			INDEX idx_category_set category TYPE set(100) GRANULARITY 4
		) ENGINE = MergeTree()
		ORDER BY id
	`,
			table: &chschema_v1.Table{
				Name:     "test_set_index",
				Database: &dbName,
				Columns: []*chschema_v1.Column{
					{Name: "id", Type: "UInt64"},
					{Name: "category", Type: "String"},
					{Name: "tags", Type: "Array(String)"},
				},
				Engine: &chschema_v1.Engine{
					EngineType: &chschema_v1.Engine_MergeTree{
						MergeTree: &chschema_v1.MergeTree{},
					},
				},
				OrderBy: []string{"id"},
				Indexes: []*chschema_v1.Index{
					{Name: "idx_category_set", Type: "set(100)", Expression: "category", Granularity: 4},
				},
			},
		},
		{
			Name: "BloomFilterIndex",
			SQL: `
		CREATE TABLE ` + dbName + `.test_bloom_index (
			id UInt64,
			email String,
			content String,
			INDEX idx_email_bloom email TYPE bloom_filter() GRANULARITY 1
		) ENGINE = MergeTree()
		ORDER BY id
	`,
			table: &chschema_v1.Table{
				Name:     "test_bloom_index",
				Database: &dbName,
				Columns: []*chschema_v1.Column{
					{Name: "id", Type: "UInt64"},
					{Name: "email", Type: "String"},
					{Name: "content", Type: "String"},
				},
				Engine: &chschema_v1.Engine{
					EngineType: &chschema_v1.Engine_MergeTree{
						MergeTree: &chschema_v1.MergeTree{},
					},
				},
				OrderBy: []string{"id"},
				Indexes: []*chschema_v1.Index{
					{Name: "idx_email_bloom", Type: "bloom_filter()", Expression: "email", Granularity: 1},
				},
			},
		},
		{
			Name: "TokenBfIndex",
			SQL: `
		CREATE TABLE ` + dbName + `.test_tokenbf_index (
			id UInt64,
			description String,
			INDEX idx_description_tokenbf description TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 2
		) ENGINE = MergeTree()
		ORDER BY id
	`,
			table: &chschema_v1.Table{
				Name:     "test_tokenbf_index",
				Database: &dbName,
				Columns: []*chschema_v1.Column{
					{Name: "id", Type: "UInt64"},
					{Name: "description", Type: "String"},
				},
				Engine: &chschema_v1.Engine{
					EngineType: &chschema_v1.Engine_MergeTree{
						MergeTree: &chschema_v1.MergeTree{},
					},
				},
				OrderBy: []string{"id"},
				Indexes: []*chschema_v1.Index{
					{Name: "idx_description_tokenbf", Type: "tokenbf_v1(32768, 3, 0)", Expression: "description", Granularity: 2},
				},
			},
		},
		{
			Name: "NgramBfIndex",
			SQL: `
		CREATE TABLE ` + dbName + `.test_ngrambf_index (
			id UInt64,
			search_text String,
			INDEX idx_search_ngrambf search_text TYPE ngrambf_v1(4, 32768, 3, 0) GRANULARITY 1
		) ENGINE = MergeTree()
		ORDER BY id
	`,
			table: &chschema_v1.Table{
				Name:     "test_ngrambf_index",
				Database: &dbName,
				Columns: []*chschema_v1.Column{
					{Name: "id", Type: "UInt64"},
					{Name: "search_text", Type: "String"},
				},
				Engine: &chschema_v1.Engine{
					EngineType: &chschema_v1.Engine_MergeTree{
						MergeTree: &chschema_v1.MergeTree{},
					},
				},
				OrderBy: []string{"id"},
				Indexes: []*chschema_v1.Index{
					{Name: "idx_search_ngrambf", Type: "ngrambf_v1(4, 32768, 3, 0)", Expression: "search_text", Granularity: 1},
				},
			},
		},
		{
			Name: "MultipleIndexes",
			SQL: `
		CREATE TABLE ` + dbName + `.test_multiple_indexes (
			id UInt64,
			timestamp DateTime,
			category String,
			email String,
			INDEX idx_category_set category TYPE set(50) GRANULARITY 4,
			INDEX idx_email_bloom email TYPE bloom_filter() GRANULARITY 1,
			INDEX idx_timestamp_minmax timestamp TYPE minmax GRANULARITY 4
		) ENGINE = MergeTree()
		ORDER BY id
	`,
			table: &chschema_v1.Table{
				Name:     "test_multiple_indexes",
				Database: &dbName,
				Columns: []*chschema_v1.Column{
					{Name: "id", Type: "UInt64"},
					{Name: "timestamp", Type: "DateTime"},
					{Name: "category", Type: "String"},
					{Name: "email", Type: "String"},
				},
				Engine: &chschema_v1.Engine{
					EngineType: &chschema_v1.Engine_MergeTree{
						MergeTree: &chschema_v1.MergeTree{},
					},
				},
				OrderBy: []string{"id"},
				Indexes: []*chschema_v1.Index{
					// Indexes are returned alphabetically by name from system.data_skipping_indices
					{Name: "idx_category_set", Type: "set(50)", Expression: "category", Granularity: 4},
					{Name: "idx_email_bloom", Type: "bloom_filter()", Expression: "email", Granularity: 1},
					{Name: "idx_timestamp_minmax", Type: "minmax", Expression: "timestamp", Granularity: 4},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			err := conn.Exec(ctx, testCase.SQL)
			require.NoError(t, err, "Failed to create test table")

			// Introspect the database
			intro := introspection.NewIntrospector(conn)
			intro.Databases = []string{dbName}
			state, err := intro.GetCurrentState(ctx)
			require.NoError(t, err, "Failed to introspect database")

			// Check that our table was found
			gotTable := chschema_v1.FindTableByName(state.Tables, testCase.table.Name)
			require.NotNil(t, gotTable, "%s should be found", testCase.table.Name)

			// Compare using protocmp for proper protobuf comparison
			diff := cmp.Diff(testCase.table, gotTable, protocmp.Transform())
			require.Empty(t, diff, "Table should match expected structure. Diff:\n%s", diff)

			createTableSQL := sqlgen.GenerateCreateTable(gotTable)
			require.Equal(t, simplify(testCase.SQL), simplify(createTableSQL))
		})
	}
}

func TestLive_Introspection_AllStatements(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}

	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	type testCase struct {
		Name string
		Path string
	}

	testGroups := make(map[string][]testCase)
	// Walk through the testdata directory to find all SQL files
	err := filepath.Walk("../test/testdata/posthog-create-statements", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".sql") {
			groupName := filepath.Base(filepath.Dir(path))
			testName := strings.TrimSuffix(info.Name(), ".sql")

			testGroups[groupName] = append(testGroups[groupName], testCase{
				Name: testName,
				Path: path,
			})
		}
		return nil
	})
	require.NoError(t, err, "Failed to walk testdata directory")

	for groupName, testsInGroup := range testGroups {
		groupName := groupName
		testsInGroup := testsInGroup
		t.Run(groupName, func(t *testing.T) {
			t.Parallel()
			for _, tc := range testsInGroup {
				tc := tc // capture range variable
				t.Run(tc.Name, func(t *testing.T) {
					// NOTE: We can't run these in parallel as they all use the same database connection
					// and create tables, which can cause race conditions.

					// Read the original CREATE statement
					sqlBytes, err := os.ReadFile(tc.Path)
					require.NoError(t, err)
					originalSQL := string(sqlBytes)

					// Some files might have multiple statements or comments, we only want the CREATE statement.
					// This is a simple heuristic, might need adjustment.
					if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(originalSQL)), "CREATE") {
						t.Skip("Skipping file without a CREATE statement at the beginning")
					}

					// Replace the database name to use our temporary test database
					// Also replace hidden passwords in dictionary sources
					statement := cleanupSQL(t, dbName, originalSQL)
					statement = strings.ReplaceAll(statement, "PASSWORD '[HIDDEN]'", "PASSWORD ''")

					// Execute the statement to create the object
					err = conn.Exec(ctx, statement)
					require.NoError(t, err, "Failed to execute CREATE statement from file: %s", tc.Path)

					// Introspect the database
					intro := introspection.NewIntrospector(conn)
					intro.Databases = []string{dbName}
					state, err := intro.GetCurrentState(ctx)
					require.NoError(t, err, "Failed to introspect database")

					// Extract table/view/dictionary name from the SQL
					objectName := getObjectName(t, statement)

					// Find the introspected object
					foundObject := chschema_v1.FindTableByName(state.Tables, objectName)
					require.NotNil(t, foundObject, "Object '%s' should be found after introspection", objectName)

					// Generate the CREATE statement from the introspected object
					generatedSQL := sqlgen.GenerateCreateTable(foundObject)

					// Compare the simplified versions of the original and generated statements
					require.Equal(t, simplify(statement), simplify(generatedSQL), "Generated SQL does not match original for %s", objectName)
				})
			}
		})
	}
}

var whitespaces = regexp.MustCompile(`[\t\n\s]+`)

func simplify(stmt string) string {
	// Remove backticks
	stmt = strings.ReplaceAll(stmt, "`", "")
	return whitespaces.ReplaceAllString(
		strings.ToLower(
			strings.TrimSpace(stmt)), " ")
}

// getObjectName is a helper to extract the object name from a CREATE statement.
// e.g., "CREATE TABLE default.my_table" -> "my_table"
func getObjectName(t *testing.T, sql string) string {
	t.Helper()
	// A simple regex to find the object name. It looks for CREATE [type] [db.]name
	re := regexp.MustCompile(`(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?(?:VIEW|TABLE|DICTIONARY)\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[a-zA-Z0-9_]+\.)?([a-zA-Z0-9_]+)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) > 1 {
		// Remove backticks if present
		return strings.Trim(matches[1], "`")
	}

	// Fallback for more complex names or formats
	fields := strings.FieldsFunc(sql, func(r rune) bool {
		return unicode.IsSpace(r) || r == '('
	})
	if len(fields) > 2 {
		parts := strings.Split(fields[2], ".")
		return strings.Trim(parts[len(parts)-1], "`")
	}
	t.Fatalf("Could not determine object name from SQL: %s", sql)
	return ""
}
