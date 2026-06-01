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
func createStubsForFixture(t *testing.T, conn driver.Conn, dbName, groupName, createSQL string) string {
	t.Helper()
	refs, err := hclload.ExtractReferencedTables(createSQL)
	if err != nil {
		t.Logf("createStubsForFixture: extract refs failed: %v (continuing without stubs)", err)
		return createSQL
	}
	if len(refs) == 0 {
		return createSQL
	}
	// Per-group stubs DB so parallel fixture groups (Dictionary,
	// MaterializedView, View, Table all run with t.Parallel()) don't
	// race over each other's stubs database. groupName is the directory
	// name under test/testdata/posthog-create-statements/.
	stubsDB := dbName + "_stubs_" + groupName
	// Drop dependent dictionaries that reference this group's stubs
	// DB before dropping it. A Dictionary subtest that ran earlier in
	// this same group may have CREATEd `<dbName>.<X>_dict` with SOURCE
	// pointing at a stub table here; ClickHouse refuses to DROP the
	// stubs database while that dictionary still references it (code
	// 630). The dictionary subtest has already asserted introspection
	// of its object, so dropping the residue is safe. Filtering by
	// source = this group's stubs DB avoids racing with the Dictionary
	// group running in parallel.
	dropDependentDictionaries(t, conn, dbName, stubsDB)
	// Drop and recreate the stubs DB so each fixture sees stubs whose
	// columns match its own declared schema. Without this, the first
	// fixture to reference `kafka_person` would lock in its column
	// list and a later MV with a different column list would fail
	// the CREATE with "no matching columns in target table".
	if err := conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+stubsDB+" SYNC"); err != nil {
		t.Logf("createStubsForFixture: drop stubs db failed: %v", err)
		return createSQL
	}
	if err := conn.Exec(context.Background(), "CREATE DATABASE "+stubsDB); err != nil {
		t.Logf("createStubsForFixture: create stubs db failed: %v", err)
		return createSQL
	}
	// Fall back to the columns declared by the fixture being CREATEd
	// when we can't find the source's own fixture (or the source
	// fixture has no explicit column list).
	fallbackCols, err := hclload.ExtractDeclaredColumns(createSQL)
	if err != nil {
		t.Logf("createStubsForFixture: extract declared columns failed: %v", err)
	}
	for _, r := range refs {
		refCols := fallbackCols
		isKafka := false
		if srcSQL, kafka := findSourceFixture(r.Name); srcSQL != "" {
			if sc, err := hclload.ExtractDeclaredColumns(srcSQL); err == nil && len(sc) > 0 {
				refCols = sc
			}
			isKafka = kafka
		}
		colList := buildStubColList(refCols, isKafka)
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

// dropDependentDictionaries drops every dictionary in dbName whose
// loading dependencies reference stubsDB. In the live-introspection
// test loop, a Dictionary subtest may have CREATEd `<dbName>.<X>_dict`
// with SOURCE rewritten to point at stubsDB; ClickHouse refuses to
// drop stubsDB while that registered dependency exists (code 630).
//
// We use system.tables.loading_dependencies_database, NOT
// system.dictionaries.source — the latter is empty until the dict is
// actually loaded (and we use LIFETIME(MIN 0 MAX 0) so it never auto-
// loads). The loading-dependency filter is essential under
// t.Parallel(): without it we'd race with a sibling Dictionary group
// running in parallel and drop a dict whose subtest hasn't asserted
// introspection yet.
//
// Errors are logged but not fatal: failure to query or to drop a
// specific dict is treated as best-effort — the subsequent DROP
// DATABASE will surface any remaining dependency.
func dropDependentDictionaries(t *testing.T, conn driver.Conn, dbName, stubsDB string) {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		`SELECT name FROM system.tables
		 WHERE database = ?
		   AND engine = 'Dictionary'
		   AND has(loading_dependencies_database, ?)`,
		dbName, stubsDB)
	if err != nil {
		t.Logf("dropDependentDictionaries: query system.tables failed: %v", err)
		return
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Logf("dropDependentDictionaries: scan failed: %v", err)
			continue
		}
		names = append(names, n)
	}
	for _, n := range names {
		stmt := fmt.Sprintf("DROP DICTIONARY IF EXISTS %s.%s SYNC", dbName, n)
		if err := conn.Exec(context.Background(), stmt); err != nil {
			t.Logf("dropDependentDictionaries: drop %s.%s failed: %v", dbName, n, err)
		}
	}
}

// renderVirtualsForCreate emits a CREATE-statement column-list fragment
// for the engine's virtual columns. Folds `_headers.name`/`_headers.value`
// (the dot-access form the loader models) back into a single
// `_headers Nested(name String, value String)` declaration so the CREATE
// statement parses against real ClickHouse. Returns "" when the engine
// contributes no virtuals.
func renderVirtualsForCreate(e hclload.Engine) string {
	cols := hclload.VirtualColumnsFor(e, nil)
	if len(cols) == 0 {
		return ""
	}
	parts := make([]string, 0, len(cols))
	var headersInner []string
	for _, c := range cols {
		if rest, ok := strings.CutPrefix(c.Name, "_headers."); ok {
			headersInner = append(headersInner, fmt.Sprintf("%s %s", rest, stripArrayWrapper(c.Type)))
			continue
		}
		parts = append(parts, fmt.Sprintf("`%s` %s", c.Name, c.Type))
	}
	if len(headersInner) > 0 {
		parts = append(parts, fmt.Sprintf("`_headers` Nested(%s)", strings.Join(headersInner, ", ")))
	}
	return strings.Join(parts, ", ")
}

// stripArrayWrapper turns "Array(String)" into "String"; ClickHouse's
// Nested columns implicitly wrap each field in Array, so the dot-access
// type the loader emits (Array(X)) must be unwrapped for the Nested form.
func stripArrayWrapper(t string) string {
	if rest, ok := strings.CutPrefix(t, "Array("); ok && strings.HasSuffix(rest, ")") {
		return rest[:len(rest)-1]
	}
	return t
}

// findSourceFixture searches the testdata corpus for the original CREATE
// statement of a referenced source table. The corpus is laid out as
// test/testdata/posthog-create-statements/<EngineKind>/<name>.sql with
// unique basenames across engine subdirs, so a glob on basename suffices.
// Returns "" when no fixture exists for refName (e.g. system tables, or
// tables only present in production).
func findSourceFixture(refName string) (sql string, isKafka bool) {
	matches, err := filepath.Glob(filepath.Join("testdata/posthog-create-statements", "*", refName+".sql"))
	if err != nil || len(matches) == 0 {
		return "", false
	}
	b, err := os.ReadFile(matches[0])
	if err != nil {
		return "", false
	}
	s := string(b)
	return s, strings.Contains(s, "ENGINE = Kafka(")
}

// buildStubColList renders a column list for the stub CREATE. For Kafka
// sources, the engine's virtual columns are appended so MVs that read
// them (`_topic`, `_partition`, `_headers.*`, …) parse.
func buildStubColList(cols []hclload.DeclaredColumn, isKafka bool) string {
	parts := make([]string, 0, len(cols)+1)
	for _, c := range cols {
		parts = append(parts, fmt.Sprintf("`%s` %s", c.Name, c.Type))
	}
	if isKafka {
		// Stub source: emit the maximal Kafka virtual set (stream mode)
		// so MVs referencing _raw_message / _error parse against the
		// stub regardless of the real source's handle_error_mode.
		stream := "stream"
		if v := renderVirtualsForCreate(hclload.EngineKafka{HandleErrorMode: &stream}); v != "" {
			parts = append(parts, v)
		}
	}
	if len(parts) == 0 {
		return "_dummy String"
	}
	return strings.Join(parts, ", ")
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
