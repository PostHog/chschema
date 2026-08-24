package hcl

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCHLive_Introspect drives every non-skipLive createTableCase through
// the full round-trip:
//
//	HCL → resolve → SQL → exec on CH → introspect → compare.
//
// The introspected TableSpec is expected to match the HCL-resolved one for
// every field the live ClickHouse can faithfully echo back. Fields not yet
// covered by introspection (constraints, cluster) are nulled out on the
// expected side before comparison.
func TestCHLive_Introspect(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	for _, tc := range createTableCases {
		tc := tc
		if tc.skipLive || tc.skipIntrospect {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() {
				_ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.t", dbName))
			})

			src := strings.Replace(tc.hcl, `database "db"`, fmt.Sprintf(`database %q`, dbName), 1)

			// 1. Parse + resolve to get the expected, post-resolution table.
			parsed := mustParseResolve(t, src)
			require.Len(t, parsed.Tables, 1)
			expected := parsed.Tables[0]

			// 2. Create the table in CH via generated DDL.
			sql := parseAndGenerate(t, src)
			require.NoError(t, conn.Exec(ctx, sql), "CREATE TABLE rejected:\n%s", sql)

			// 3. Introspect back.
			db, err := Introspect(ctx, conn, dbName, false)
			require.NoError(t, err)
			var got *TableSpec
			for i := range db.Tables {
				if db.Tables[i].Name == "t" {
					got = &db.Tables[i]
					break
				}
			}
			require.NotNil(t, got, "introspected schema has no table %q", "t")

			// 4. Normalize both sides for comparison.
			normalizeForCompare(&expected)
			normalizeForCompare(got)
			alignEphemeralDefaults(&expected, got)
			alignCodecDefaults(&expected, got)
			alignColumnTTLs(&expected, got)
			alignSettings(&expected, got)
			assert.Equal(t, expected, *got)
		})
	}
}

// TestCHLive_IntrospectMaterializedView creates a destination table, a source
// table, and a TO-form materialized view on a live ClickHouse instance, then
// introspects the database and asserts the MV round-trips. It also exercises
// the original bug: introspecting a database that contains a materialized
// view must not fail.
func TestCHLive_IntrospectMaterializedView(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.metrics (`team_id` Int64, `cnt` UInt64) ENGINE = MergeTree ORDER BY team_id", dbName)))
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.events (team_id Int64) ENGINE = MergeTree ORDER BY team_id", dbName)))
	// Use a TO-form MV with an explicit column list so that mv.Columns round-trips.
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE MATERIALIZED VIEW %s.metrics_mv TO %s.metrics (`team_id` Int64, `cnt` UInt64) "+
			"AS SELECT team_id, count() AS cnt FROM %s.events GROUP BY team_id",
		dbName, dbName, dbName)))

	db, err := Introspect(ctx, conn, dbName, false)
	require.NoError(t, err, "introspecting a database with a materialized view must not fail")

	require.Len(t, db.MaterializedViews, 1)
	mv := db.MaterializedViews[0]
	assert.Equal(t, "metrics_mv", mv.Name)
	assert.Equal(t, dbName+".metrics", mv.ToTable)
	assert.Contains(t, mv.Query, "team_id")
	assert.Contains(t, mv.Query, "events")
	// Assert that explicit MV destination columns round-trip as name+type only.
	assert.Equal(t, []ColumnSpec{
		{Name: "team_id", Type: "Int64"},
		{Name: "cnt", Type: "UInt64"},
	}, mv.Columns)

	// The destination and source tables still introspect alongside the MV.
	var tableNames []string
	for _, tbl := range db.Tables {
		tableNames = append(tableNames, tbl.Name)
	}
	assert.ElementsMatch(t, []string{"metrics", "events"}, tableNames)
}

// TestCHLive_AlterMaterializedViewAdditiveProjection exercises issue #197
// end-to-end against ClickHouse: introspect the current source/destination/MV,
// diff to an additive destination + MV projection, execute the generated
// statements, verify routed data, and require a converged second diff.
func TestCHLive_AlterMaterializedViewAdditiveProjection(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.source (id UInt64, value String) ENGINE = MergeTree ORDER BY id", dbName)))
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.destination (id UInt64) ENGINE = MergeTree ORDER BY id", dbName)))
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE MATERIALIZED VIEW %s.events_mv TO %s.destination (id UInt64) "+
			"AS SELECT id FROM %s.source", dbName, dbName, dbName)))

	currentDB, err := Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)

	// Clone the introspected state and express the exact desired change: add a
	// nullable destination column and the matching MV output/query projection.
	desiredDB := *currentDB
	desiredDB.Tables = append([]TableSpec(nil), currentDB.Tables...)
	for i := range desiredDB.Tables {
		desiredDB.Tables[i].Columns = append([]ColumnSpec(nil), desiredDB.Tables[i].Columns...)
		if desiredDB.Tables[i].Name == "destination" {
			desiredDB.Tables[i].Columns = append(desiredDB.Tables[i].Columns,
				ColumnSpec{Name: "value", Type: "Nullable(String)"})
		}
	}
	desiredDB.MaterializedViews = append([]MaterializedViewSpec(nil), currentDB.MaterializedViews...)
	for i := range desiredDB.MaterializedViews {
		desiredDB.MaterializedViews[i].Columns = append(
			[]ColumnSpec(nil), desiredDB.MaterializedViews[i].Columns...)
		if desiredDB.MaterializedViews[i].Name == "events_mv" {
			desiredDB.MaterializedViews[i].Columns = append(desiredDB.MaterializedViews[i].Columns,
				ColumnSpec{Name: "value", Type: "Nullable(String)"})
			query, ok := normalizeQuery(fmt.Sprintf(
				"SELECT id, nullIf(value, '') AS value FROM %s.source", dbName))
			require.True(t, ok)
			desiredDB.MaterializedViews[i].Query = query
		}
	}

	current := &Schema{Databases: []DatabaseSpec{*currentDB}}
	desired := &Schema{Databases: []DatabaseSpec{desiredDB}}
	generated := GenerateSQL(Diff(current, desired))
	require.Empty(t, generated.Unsafe)
	require.Equal(t, []string{
		fmt.Sprintf("ALTER TABLE %s.destination ADD COLUMN value Nullable(String)", dbName),
		fmt.Sprintf("ALTER TABLE %s.events_mv MODIFY QUERY %s",
			dbName, desiredDB.MaterializedViews[0].Query),
	}, generated.Statements)

	for _, statement := range generated.Statements {
		require.NoError(t, conn.Exec(ctx, statement), "ClickHouse rejected generated statement:\n%s", statement)
	}
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.source VALUES (1, 'kept'), (2, '')", dbName)))

	rows, err := conn.Query(ctx, fmt.Sprintf(
		"SELECT id, value FROM %s.destination ORDER BY id", dbName))
	require.NoError(t, err)
	defer rows.Close()
	var gotIDs []uint64
	var gotValues []*string
	for rows.Next() {
		var id uint64
		var value *string
		require.NoError(t, rows.Scan(&id, &value))
		gotIDs = append(gotIDs, id)
		gotValues = append(gotValues, value)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []uint64{1, 2}, gotIDs)
	require.Len(t, gotValues, 2)
	require.NotNil(t, gotValues[0])
	assert.Equal(t, "kept", *gotValues[0])
	assert.Nil(t, gotValues[1])

	afterDB, err := Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)
	secondDiff := Diff(&Schema{Databases: []DatabaseSpec{*afterDB}}, desired)
	require.True(t, secondDiff.IsEmpty(),
		"generated migration did not converge; residual SQL: %#v; unsafe: %#v",
		GenerateSQL(secondDiff).Statements, GenerateSQL(secondDiff).Unsafe)

	// Starting from the converged live schema, verify that removing an output
	// or changing an existing output definition does not inherit the additive
	// exception. Both require recreation and must emit no MODIFY QUERY.
	for _, tc := range []struct {
		name   string
		mutate func(*MaterializedViewSpec)
	}{
		{
			name: "drop output",
			mutate: func(mv *MaterializedViewSpec) {
				mv.Columns = mv.Columns[:1]
				query, ok := normalizeQuery(fmt.Sprintf("SELECT id FROM %s.source", dbName))
				require.True(t, ok)
				mv.Query = query
			},
		},
		{
			name: "change output type",
			mutate: func(mv *MaterializedViewSpec) {
				for i := range mv.Columns {
					if mv.Columns[i].Name == "value" {
						mv.Columns[i].Type = "String"
					}
				}
				query, ok := normalizeQuery(fmt.Sprintf("SELECT id, value FROM %s.source", dbName))
				require.True(t, ok)
				mv.Query = query
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unsafeDB := *afterDB
			unsafeDB.MaterializedViews = append(
				[]MaterializedViewSpec(nil), afterDB.MaterializedViews...)
			require.Len(t, unsafeDB.MaterializedViews, 1)
			unsafeDB.MaterializedViews[0].Columns = append(
				[]ColumnSpec(nil), unsafeDB.MaterializedViews[0].Columns...)
			tc.mutate(&unsafeDB.MaterializedViews[0])

			change := Diff(
				&Schema{Databases: []DatabaseSpec{*afterDB}},
				&Schema{Databases: []DatabaseSpec{unsafeDB}},
			)
			require.Len(t, change.Databases, 1)
			require.Len(t, change.Databases[0].AlterMaterializedViews, 1)
			mvChange := change.Databases[0].AlterMaterializedViews[0]
			assert.True(t, mvChange.ColumnsChanged)
			assert.True(t, mvChange.Recreate)
			assert.Nil(t, mvChange.QueryChange)

			got := GenerateSQL(change)
			assert.Empty(t, got.Statements)
			require.Len(t, got.Unsafe, 1)
			assert.Equal(t, dbName, got.Unsafe[0].Database)
			assert.Equal(t, "events_mv", got.Unsafe[0].Table)
			assert.Contains(t, got.Unsafe[0].Reason, "incompatible column list")
		})
	}
}

// mustParseResolve parses HCL source from a literal string by writing it to
// a temp file, then runs Resolve, returning the single DatabaseSpec.
func mustParseResolve(t *testing.T, src string) *DatabaseSpec {
	t.Helper()
	schema, err := parseSource(t, src)
	require.NoError(t, err)
	require.NoError(t, Resolve(schema))
	require.Len(t, schema.Databases, 1)
	return &schema.Databases[0]
}

func parseSource(t *testing.T, src string) (*Schema, error) {
	t.Helper()
	tmp := t.TempDir() + "/spec.hcl"
	require.NoError(t, os.WriteFile(tmp, []byte(src), 0o644))
	return ParseFile(tmp)
}

// alignColumnTTLs copies the introspected per-column TTL onto expected when
// expected has a TTL set. ClickHouse rewrites interval expressions (e.g.
// `INTERVAL 1 MONTH` → `toIntervalMonth(1)`), so the textual form differs
// from the HCL fixture even though the meaning is identical. Presence is
// what the introspection round-trip asserts.
func alignColumnTTLs(expected, got *TableSpec) {
	byName := make(map[string]*ColumnSpec, len(got.Columns))
	for i := range got.Columns {
		byName[got.Columns[i].Name] = &got.Columns[i]
	}
	for i := range expected.Columns {
		ec := &expected.Columns[i]
		if ec.TTL == nil {
			continue
		}
		if gc, ok := byName[ec.Name]; ok && gc.TTL != nil {
			v := *gc.TTL
			ec.TTL = &v
		}
	}
	// Table-level TTL: same interval-canonicalization issue.
	if expected.TTL != nil && got.TTL != nil {
		v := *got.TTL
		expected.TTL = &v
	}
}

// alignSettings filters the introspected Settings map down to the keys
// expected actually declared. ClickHouse always emits server defaults like
// `index_granularity = 8192` in engine_full even when the user didn't write
// them; this filter keeps the assertion focused on what the HCL specifies.
func alignSettings(expected, got *TableSpec) {
	if got.Settings == nil {
		return
	}
	if len(expected.Settings) == 0 {
		got.Settings = nil
		return
	}
	filtered := make(map[string]string, len(expected.Settings))
	for k, v := range got.Settings {
		if _, ok := expected.Settings[k]; ok {
			filtered[k] = v
		}
	}
	got.Settings = filtered
}

// alignCodecDefaults copies the introspected codec onto the expected side
// whenever the expected has a codec set. ClickHouse fills in default args
// (e.g. ZSTD → ZSTD(1), Delta → Delta(4)) so the introspected text differs
// from the HCL fixture's short form. The test still asserts a codec is
// present and that it parses back; the exact textual form is validated by
// the static TestCH_Column_Codec* tests.
func alignCodecDefaults(expected, got *TableSpec) {
	byName := make(map[string]*ColumnSpec, len(got.Columns))
	for i := range got.Columns {
		byName[got.Columns[i].Name] = &got.Columns[i]
	}
	for i := range expected.Columns {
		ec := &expected.Columns[i]
		if ec.Codec == nil {
			continue
		}
		if gc, ok := byName[ec.Name]; ok && gc.Codec != nil {
			v := *gc.Codec
			ec.Codec = &v
		}
	}
}

// alignEphemeralDefaults handles bare-EPHEMERAL columns: ClickHouse expands
// `EPHEMERAL` with no expression into `defaultValueOfTypeName('Type')`. We
// preserve the introspected form by copying it onto the expected side.
func alignEphemeralDefaults(expected, got *TableSpec) {
	byName := make(map[string]*ColumnSpec, len(got.Columns))
	for i := range got.Columns {
		byName[got.Columns[i].Name] = &got.Columns[i]
	}
	for i := range expected.Columns {
		ec := &expected.Columns[i]
		if ec.Ephemeral == nil || *ec.Ephemeral != "" {
			continue
		}
		if gc, ok := byName[ec.Name]; ok && gc.Ephemeral != nil {
			v := *gc.Ephemeral
			ec.Ephemeral = &v
		}
	}
}

// normalizeForCompare collapses HCL spellings that ClickHouse stores in a
// single canonical form (so the introspector echoes one shape), and nulls
// out fields the introspection layer doesn't (yet) recover from CH.
func normalizeForCompare(t *TableSpec) {
	t.Constraints = nil // not introspected (no system table)
	t.Cluster = nil     // not introspected (cluster topology lives in server config)
	for i := range t.Columns {
		c := &t.Columns[i]
		// nullable = true is stored by CH inside the type itself; canonicalize.
		if c.Nullable && !strings.HasPrefix(c.Type, "Nullable(") {
			c.Type = "Nullable(" + c.Type + ")"
			c.Nullable = false
		}
	}
	if t.Engine != nil {
		t.Engine.Body = nil
	}
}

// TestCHLive_IntrospectDictionary creates a source table and a TO-form
// CLICKHOUSE-source HASHED-layout dictionary against a real ClickHouse
// instance, introspects the database, and asserts the dictionary
// round-trips.
func TestCHLive_IntrospectDictionary(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	runSQL := func(sql string) {
		require.NoError(t, conn.Exec(ctx, sql), "rejected by ClickHouse:\n%s", sql)
	}

	runSQL(fmt.Sprintf(
		"CREATE TABLE %s.src (`k` UInt64, `v` String) ENGINE = MergeTree ORDER BY k",
		dbName))
	runSQL(fmt.Sprintf(
		"INSERT INTO %s.src VALUES (1, 'one'), (2, 'two')", dbName))
	runSQL(fmt.Sprintf(
		"CREATE DICTIONARY %s.kv_dict (`k` UInt64, `v` String) "+
			"PRIMARY KEY k "+
			"SOURCE(CLICKHOUSE(QUERY 'SELECT k, v FROM %s.src' USER 'default')) "+
			"LIFETIME(0) "+
			"LAYOUT(HASHED())",
		dbName, dbName))

	db, err := Introspect(ctx, conn, dbName, false)
	require.NoError(t, err)

	var got *DictionarySpec
	for i := range db.Dictionaries {
		if db.Dictionaries[i].Name == "kv_dict" {
			got = &db.Dictionaries[i]
			break
		}
	}
	require.NotNil(t, got, "introspected schema has no dictionary kv_dict")

	assert.Equal(t, []string{"k"}, got.PrimaryKey)
	assert.Equal(t, []DictionaryAttribute{
		{Name: "k", Type: "UInt64"},
		{Name: "v", Type: "String"},
	}, got.Attributes)
	require.NotNil(t, got.Source)
	assert.Equal(t, "clickhouse", got.Source.Kind)
	require.IsType(t, SourceClickHouse{}, got.Source.Decoded)
	chs := got.Source.Decoded.(SourceClickHouse)
	require.NotNil(t, chs.Query)
	assert.Contains(t, *chs.Query, "src")
	require.NotNil(t, got.Layout)
	assert.Equal(t, "hashed", got.Layout.Kind)
	assert.IsType(t, LayoutHashed{}, got.Layout.Decoded)

	// The src table still introspects fine alongside the dictionary.
	assert.Len(t, db.Tables, 1)
}
