package test

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/posthog/chschema/gen/chschema_v1"
	"github.com/posthog/chschema/internal/diff"
	"github.com/posthog/chschema/internal/dumper"
	"github.com/posthog/chschema/internal/executor"
	"github.com/posthog/chschema/internal/introspection"
	"github.com/posthog/chschema/internal/loader"
	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/require"
)

func select1(t *testing.T, conn driver.Conn) {
	var one uint8
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT 1").Scan(&one))
	require.EqualValues(t, 1, one)
}

func TestLive_BasicConnectivity(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}
	conn := testhelpers.RequireClickHouse(t)

	// Test basic ping
	err := testhelpers.PingClickHouse(conn)
	require.NoError(t, err, "ClickHouse ping should succeed")

	select1(t, conn)
}

func TestLive_DatabaseCreation(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}
	conn := testhelpers.RequireClickHouse(t)

	dbName := testhelpers.CreateTestDatabase(t, conn)
	require.NotEmpty(t, dbName, "Test database name should not be empty")

	query := "SELECT count() FROM system.databases WHERE name = $1"
	var count uint64
	require.NoError(t, conn.QueryRow(context.Background(), query, dbName).Scan(&count), "Database existence query should succeed")
	require.Equal(t, uint64(1), count, "Database should exist")

	select1(t, conn)
}

func TestLive_EndToEnd_SchemaApply(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}

	// 1. Get connection and create test database
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)

	// 2. Load desired schema from YAML files
	schemaDir := filepath.Join("testdata", "live")
	schemaLoader := loader.NewSchemaLoader(schemaDir)
	desiredState, err := schemaLoader.Load()
	require.NoError(t, err, "Failed to load desired schema")

	// Update table database to use test database
	for _, table := range desiredState.Tables {
		table.Database = &dbName
	}

	// 3. Get current state (should be empty - no tables in new database)
	ctx := context.Background()
	introspector := introspection.NewIntrospector(conn)
	introspector.Databases = []string{dbName}
	currentState, err := introspector.GetCurrentState(ctx)
	require.NoError(t, err, "Failed to introspect current state")

	// 4. Compute diff between the empty state and the desired state
	differ := diff.NewDiffer()
	plan, err := differ.Plan(desiredState, currentState)
	require.NoError(t, err, "Failed to create diff plan")
	require.NotEmpty(t, plan.Actions, "Plan should have actions to create tables")

	// 5. Execute plan against ClickHouse
	exec := executor.NewExecutor(conn)
	err = exec.Execute(ctx, plan)
	require.NoError(t, err, "Failed to execute plan")

	// 7. Introspect the database again to get actual state
	actualState, err := introspector.GetCurrentState(ctx)
	require.NoError(t, err, "Failed to introspect actual state after apply")

	// 8. Compare actual state with desired state
	// The tables should match
	require.Equal(t, len(desiredState.Tables), len(actualState.Tables),
		"Number of tables should match")

	for _, desiredTable := range desiredState.Tables {
		tableName := desiredTable.Name
		actualTable := chschema_v1.FindTableByName(actualState.Tables, tableName)
		require.NotNil(t, actualTable, "Table %s should exist in actual state", tableName)

		// Compare basic table properties
		require.Equal(t, desiredTable.Name, actualTable.Name, "Table names should match")
		require.Equal(t, *desiredTable.Database, *actualTable.Database, "Database names should match")

		// Compare columns
		require.Equal(t, len(desiredTable.Columns), len(actualTable.Columns),
			"Number of columns should match for table %s", tableName)

		for i, desiredCol := range desiredTable.Columns {
			actualCol := actualTable.Columns[i]
			require.Equal(t, desiredCol.Name, actualCol.Name,
				"Column %d name should match", i)
			require.Equal(t, desiredCol.Type, actualCol.Type,
				"Column %d type should match", i)
		}

		// Compare ORDER BY
		require.Equal(t, desiredTable.OrderBy, actualTable.OrderBy,
			"ORDER BY should match for table %s", tableName)

		// Compare engine
		require.NotNil(t, actualTable.Engine, "Engine should be set for table %s", tableName)
		// TODO use proper protocmp for proper protobuf comparison
		testhelpers.EqualProto(t, desiredTable.Engine, actualTable.Engine)

		// TODO: Enable settings comparison once settings introspection is implemented
		// Currently settings introspection is placeholder (see introspector.go:149)
		if len(actualTable.Settings) > 0 {
			require.Equal(t, desiredTable.Settings, actualTable.Settings,
				"Settings should match for table %s", tableName)
		}
	}
}

// createStubsForFixture pre-creates stub source/destination tables for a
// CREATE MATERIALIZED VIEW / VIEW / DICTIONARY statement so the CREATE is
// executable in isolation. ClickHouse rejects an MV CREATE when its
// destination or any SELECT-FROM source table is missing; this helper
// derives the dependency set via the hcl package and synthesizes minimal
// Null-engine stubs.
//
// Stub schema: the column list declared on the CREATE statement itself —
// destination column list for MVs, attribute list for dictionaries,
// declared columns for views — applied to every referenced table. This is
// only correct for fixtures where source and destination share a schema
// (the common PostHog kafka_* → sharded_* shape), which is the case for
// every fixture in test/testdata/posthog-create-statements/.
//
// Engine = Null: never accepts INSERTs (it discards them silently), so
// these stubs add no storage or replication overhead. They're sufficient
// for CREATE-time validation and for the introspector to see the object.
//
// Stubs are created with IF NOT EXISTS so multiple fixtures in the same
// group sharing a source table don't collide.
//
// Stubs live in a sibling `<dbName>_stubs` database, not in `dbName`
// itself. This keeps the per-test database holding only real fixture
// objects, so a later subtest that CREATEs (for real) a table whose
// name happens to match a stub source — e.g. `exchange_rate` is both a
// fixture and referenced by `exchange_rate_mv` — doesn't collide.
//
// To make MV/View bodies actually find the stubs, we also rewrite each
// referenced `<dbName>.<refName>` in the SQL to point at the stubs
// database. The returned string replaces the input SQL when the caller
// runs `conn.Exec`.
func createStubsForFixture(t *testing.T, conn driver.Conn, dbName, createSQL string) string {
	t.Helper()
	refs, err := hclload.ExtractReferencedTables(createSQL)
	if err != nil {
		t.Logf("createStubsForFixture: extract refs failed: %v (continuing without stubs)", err)
		return createSQL
	}
	if len(refs) == 0 {
		return createSQL
	}
	stubsDB := dbName + "_stubs"
	// Drop and recreate the stubs DB so each fixture sees stubs whose
	// columns match its own declared schema. Without this, the first
	// fixture to reference `kafka_person` would lock in its column
	// list and a later MV with a different column list would fail
	// the CREATE with "no matching columns in target table".
	if err := conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+stubsDB+" SYNC"); err != nil {
		t.Logf("createStubsForFixture: drop stubs db failed: %v", err)
	}
	if err := conn.Exec(context.Background(), "CREATE DATABASE "+stubsDB); err != nil {
		t.Logf("createStubsForFixture: create stubs db failed: %v", err)
		return createSQL
	}
	cols, err := hclload.ExtractDeclaredColumns(createSQL)
	if err != nil {
		t.Logf("createStubsForFixture: extract declared columns failed: %v", err)
	}
	colList := "_dummy String"
	if len(cols) > 0 {
		parts := make([]string, len(cols))
		for i, c := range cols {
			parts[i] = fmt.Sprintf("`%s` %s", c.Name, c.Type)
		}
		colList = strings.Join(parts, ", ")
	}
	for _, r := range refs {
		stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s) ENGINE = Null", stubsDB, r.Name, colList)
		if err := conn.Exec(context.Background(), stmt); err != nil {
			t.Logf("createStubsForFixture: stub create failed for %s.%s: %v\n%s", stubsDB, r.Name, err, stmt)
		}
		// Repoint references in the fixture SQL from the test DB to
		// the stubs DB. cleanupSQL already rewrote `default.X` to
		// `dbName.X`, so we only need to handle the post-cleanup
		// shape (bare and backtick-quoted forms). Match on a word
		// boundary after the name to avoid clobbering `dbName.groups`
		// inside `dbName.groups_mv`.
		bareRe := regexp.MustCompile(`\b` + regexp.QuoteMeta(dbName+"."+r.Name) + `\b`)
		createSQL = bareRe.ReplaceAllString(createSQL, stubsDB+"."+r.Name)
		quoted := "`" + dbName + "`.`" + r.Name + "`"
		createSQL = strings.ReplaceAll(createSQL, quoted, "`"+stubsDB+"`.`"+r.Name+"`")
	}
	return createSQL
}

func cleanupSQL(t *testing.T, dbName, createSQL string) string {
	// Rewrite every reference to the fixture's `default` database to the
	// isolated per-test database. This covers:
	//   - the object being created: `CREATE TABLE/MATERIALIZED VIEW/DICTIONARY/VIEW default.X`
	//   - destination tables on MVs: `TO default.X`
	//   - source tables in SELECT bodies: `FROM default.X`, `JOIN default.X`
	//   - backtick-quoted forms inside dictionary QUERY clauses: `` `default`.`X` ``
	//
	// We do the simplest thing that works: a global replace of every
	// `default.` occurrence. The fixtures only ever reference the `default`
	// database, so this is safe in this test corpus.
	createSQL = strings.ReplaceAll(createSQL, "`default`.", "`"+dbName+"`.")
	createSQL = strings.ReplaceAll(createSQL, "default.", dbName+".")
	// Make every ReplicatedMergeTree ZooKeeper path unique to this test
	// database. The fixture paths come in several shapes
	// (/clickhouse/tables/..., /clickhouse/prod/tables/...), and a fixed
	// path would collide in ZooKeeper across repeated test runs.
	zkPath := regexp.MustCompile(`'/clickhouse/`)
	createSQL = zkPath.ReplaceAllString(createSQL, "'/clickhouse/"+dbName+"/")
	return createSQL
}

func TestCleanupSQL(t *testing.T) {
	createSQL := "CREATE TABLE default.query_log_archive (`hostname` LowCardinality(String),`user` LowCardinality(String),`query_id` String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/noshard/posthog.query_log_archive_new', '{replica}-{shard}') PARTITION BY toYYYYMM(event_date) PRIMARY KEY (team_id, event_date, event_time, query_id) ORDER BY (team_id, event_date, event_time, query_id) SETTINGS index_granularity = 8192"
	got := cleanupSQL(t, "my_test_database", createSQL)
	want := "CREATE TABLE my_test_database.query_log_archive (`hostname` LowCardinality(String),`user` LowCardinality(String),`query_id` String) ENGINE = ReplicatedMergeTree('/clickhouse/my_test_database/tables/noshard/posthog.query_log_archive_new', '{replica}-{shard}') PARTITION BY toYYYYMM(event_date) PRIMARY KEY (team_id, event_date, event_time, query_id) ORDER BY (team_id, event_date, event_time, query_id) SETTINGS index_granularity = 8192"
	require.Equal(t, want, got)

	createSQL = "CREATE TABLE default.sharded_events (`uuid` UUID, `event` String, `properties` String CODEC(ZSTD(3)), `timestamp` DateTime64(6, 'UTC'), `team_id` Int64) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/posthog.events', '{replica}', _timestamp) PARTITION BY toYYYYMM(timestamp) ORDER BY (team_id, toDate(timestamp), event, cityHash64(distinct_id), cityHash64(uuid)) SAMPLE BY cityHash64(distinct_id) SETTINGS index_granularity = 8192"
	got = cleanupSQL(t, "my_test_database", createSQL)
	want = "CREATE TABLE my_test_database.sharded_events (`uuid` UUID, `event` String, `properties` String CODEC(ZSTD(3)), `timestamp` DateTime64(6, 'UTC'), `team_id` Int64) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/my_test_database/tables/{shard}/posthog.events', '{replica}', _timestamp) PARTITION BY toYYYYMM(timestamp) ORDER BY (team_id, toDate(timestamp), event, cityHash64(distinct_id), cityHash64(uuid)) SAMPLE BY cityHash64(distinct_id) SETTINGS index_granularity = 8192"
	require.Equal(t, want, got)
}

// 1. Create a table from SQL dump
// 2. Dump table from ClickHouse
// 3. Create a table from a Dump
// 4. Dump table created from ClickHouse
// 5. Compare the dumps
func TestEnd2End(t *testing.T) {
	if !*clickhouse {
		t.SkipNow()
	}

	// 1. Get connection and create test database
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)

	testCases := []struct {
		Name   string
		Engine string
		Skip   bool
	}{
		{
			Name:   "query_log_archive",
			Engine: "ReplicatedMergeTree",
		},
		{
			Name:   "sharded_events",
			Engine: "ReplicatedReplacingMergeTree",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			if testCase.Skip {
				t.SkipNow()
			}

			tableName := testCase.Name
			sqlPath := filepath.Join("testdata/posthog-create-statements", testCase.Engine, tableName+".sql")
			createSQLRaw, err := os.ReadFile(sqlPath)
			require.NoError(t, err, "Failed to read file %s", tableName)
			createSQL := cleanupSQL(t, dbName, string(createSQLRaw))

			defer func(t *testing.T) {
				dropSQL := fmt.Sprintf("DROP TABLE %s.%s", dbName, tableName)
				if err := conn.Exec(context.Background(), dropSQL); err != nil {
					t.Logf("Failed to drop table: %s", dropSQL)
				}
			}(t)

			require.NoError(t, conn.Exec(context.Background(), createSQL))

			// introspect
			intro := introspection.NewIntrospector(conn)
			intro.Databases = []string{dbName}
			intro.Tables = []string{tableName}
			state, err := intro.GetCurrentState(context.Background())
			require.NoError(t, err, "Failed to introspect current state")
			require.Len(t, state.Tables, 1)

			tempDir := t.TempDir()

			require.NoError(t, dumper.WriteYAMLFile(path.Join(tempDir, tableName+".yaml"), state.Tables[0], false))
		})
	}
}
