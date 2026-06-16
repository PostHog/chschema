package hcl

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// baseSchema returns a schema with one database "db" holding a single table
// "events" used as the left side for ALTER/DROP/RENAME tests.
func baseSchema() *Schema {
	return &Schema{
		Databases: []DatabaseSpec{{
			Name: "db",
			Tables: []TableSpec{{
				Name: "events",
				Columns: []ColumnSpec{
					{Name: "id", Type: "UInt64"},
					{Name: "ts", Type: "DateTime"},
				},
				OrderBy:  []string{"id"},
				Settings: map[string]string{"index_granularity": "8192"},
				Engine:   &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
			}},
		}},
	}
}

func colNames(t TableSpec) []string {
	out := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		out[i] = c.Name
	}
	return out
}

func TestApplySQL_CreateTableAddsAndReplaces(t *testing.T) {
	s := &Schema{}
	n, err := ApplySQL(s, `CREATE TABLE db.events (id UInt64) ENGINE = MergeTree ORDER BY id`, "", false)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.Len(t, s.Databases, 1)
	require.Len(t, s.Databases[0].Tables, 1)
	assert.Equal(t, "events", s.Databases[0].Tables[0].Name)
	assert.Equal(t, []string{"id"}, colNames(s.Databases[0].Tables[0]))

	// A second CREATE with the same name replaces the object, not appends.
	_, err = ApplySQL(s, `CREATE TABLE db.events (id UInt64, name String) ENGINE = MergeTree ORDER BY id`, "", false)
	require.NoError(t, err)
	require.Len(t, s.Databases[0].Tables, 1)
	assert.Equal(t, []string{"id", "name"}, colNames(s.Databases[0].Tables[0]))
}

func TestApplySQL_UnqualifiedUsesDefaultDatabase(t *testing.T) {
	s := &Schema{}
	_, err := ApplySQL(s, `CREATE TABLE events (id UInt64) ENGINE = MergeTree ORDER BY id`, "analytics", false)
	require.NoError(t, err)
	require.Len(t, s.Databases, 1)
	assert.Equal(t, "analytics", s.Databases[0].Name)
}

func TestApplySQL_UnqualifiedNoDefaultMultiDBErrors(t *testing.T) {
	s := &Schema{Databases: []DatabaseSpec{{Name: "a"}, {Name: "b"}}}
	_, err := ApplySQL(s, `CREATE TABLE events (id UInt64) ENGINE = MergeTree ORDER BY id`, "", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unqualified")
}

func TestApplySQL_AlterAddColumn(t *testing.T) {
	s := baseSchema()
	_, err := ApplySQL(s, `ALTER TABLE db.events ADD COLUMN name String`, "", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "ts", "name"}, colNames(s.Databases[0].Tables[0]))
}

func TestApplySQL_AlterAddColumnAfter(t *testing.T) {
	s := baseSchema()
	_, err := ApplySQL(s, `ALTER TABLE db.events ADD COLUMN name String AFTER id`, "", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name", "ts"}, colNames(s.Databases[0].Tables[0]))
}

func TestApplySQL_AlterAddColumnDuplicateErrors(t *testing.T) {
	s := baseSchema()
	_, err := ApplySQL(s, `ALTER TABLE db.events ADD COLUMN id UInt64`, "", false)
	require.Error(t, err)

	// IF NOT EXISTS makes the duplicate a no-op.
	s = baseSchema()
	_, err = ApplySQL(s, `ALTER TABLE db.events ADD COLUMN IF NOT EXISTS id UInt64`, "", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "ts"}, colNames(s.Databases[0].Tables[0]))
}

func TestApplySQL_AlterModifyColumn(t *testing.T) {
	s := baseSchema()
	_, err := ApplySQL(s, `ALTER TABLE db.events MODIFY COLUMN ts DateTime64(3)`, "", false)
	require.NoError(t, err)
	cols := s.Databases[0].Tables[0].Columns
	assert.Equal(t, "DateTime64(3)", cols[1].Type)
}

func TestApplySQL_AlterDropColumn(t *testing.T) {
	s := baseSchema()
	_, err := ApplySQL(s, `ALTER TABLE db.events DROP COLUMN ts`, "", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"id"}, colNames(s.Databases[0].Tables[0]))

	_, err = ApplySQL(s, `ALTER TABLE db.events DROP COLUMN nope`, "", false)
	require.Error(t, err)
}

func TestApplySQL_AlterRenameColumnRecordsRenamedFrom(t *testing.T) {
	s := baseSchema()
	_, err := ApplySQL(s, `ALTER TABLE db.events RENAME COLUMN ts TO event_time`, "", false)
	require.NoError(t, err)
	cols := s.Databases[0].Tables[0].Columns
	assert.Equal(t, "event_time", cols[1].Name)
	require.NotNil(t, cols[1].RenamedFrom)
	assert.Equal(t, "ts", *cols[1].RenamedFrom)
}

func TestApplySQL_AlterIndex(t *testing.T) {
	s := baseSchema()
	_, err := ApplySQL(s, `ALTER TABLE db.events ADD INDEX idx_ts ts TYPE minmax GRANULARITY 4`, "", false)
	require.NoError(t, err)
	require.Len(t, s.Databases[0].Tables[0].Indexes, 1)
	assert.Equal(t, "idx_ts", s.Databases[0].Tables[0].Indexes[0].Name)

	_, err = ApplySQL(s, `ALTER TABLE db.events DROP INDEX idx_ts`, "", false)
	require.NoError(t, err)
	assert.Empty(t, s.Databases[0].Tables[0].Indexes)
}

func TestApplySQL_AlterSettings(t *testing.T) {
	s := baseSchema()
	_, err := ApplySQL(s, `ALTER TABLE db.events MODIFY SETTING index_granularity = 4096`, "", false)
	require.NoError(t, err)
	assert.Equal(t, "4096", s.Databases[0].Tables[0].Settings["index_granularity"])

	_, err = ApplySQL(s, `ALTER TABLE db.events RESET SETTING index_granularity`, "", false)
	require.NoError(t, err)
	_, ok := s.Databases[0].Tables[0].Settings["index_granularity"]
	assert.False(t, ok)
}

func TestApplySQL_AlterTTL(t *testing.T) {
	s := baseSchema()
	_, err := ApplySQL(s, `ALTER TABLE db.events MODIFY TTL ts + INTERVAL 30 DAY`, "", false)
	require.NoError(t, err)
	require.NotNil(t, s.Databases[0].Tables[0].TTL)

	_, err = ApplySQL(s, `ALTER TABLE db.events REMOVE TTL`, "", false)
	require.NoError(t, err)
	assert.Nil(t, s.Databases[0].Tables[0].TTL)
}

func TestApplySQL_AlterMaterializedViewModifyQuery(t *testing.T) {
	s := &Schema{Databases: []DatabaseSpec{{
		Name: "db",
		MaterializedViews: []MaterializedViewSpec{{
			Name:    "mv",
			ToTable: "db.dest",
			Query:   "SELECT id FROM db.events",
		}},
	}}}
	_, err := ApplySQL(s, `ALTER TABLE db.mv MODIFY QUERY SELECT id, ts FROM db.events`, "", false)
	require.NoError(t, err)
	assert.Contains(t, s.Databases[0].MaterializedViews[0].Query, "ts")
}

func TestApplySQL_AlterMissingTableErrors(t *testing.T) {
	s := baseSchema()
	_, err := ApplySQL(s, `ALTER TABLE db.missing ADD COLUMN x String`, "", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no such table")
}

func TestApplySQL_DropTable(t *testing.T) {
	s := baseSchema()
	_, err := ApplySQL(s, `DROP TABLE db.events`, "", false)
	require.NoError(t, err)
	assert.Empty(t, s.Databases[0].Tables)

	// Dropping a missing table errors unless IF EXISTS.
	_, err = ApplySQL(s, `DROP TABLE db.events`, "", false)
	require.Error(t, err)
	_, err = ApplySQL(s, `DROP TABLE IF EXISTS db.events`, "", false)
	require.NoError(t, err)
}

func TestApplySQL_RenameTable(t *testing.T) {
	s := baseSchema()
	_, err := ApplySQL(s, `RENAME TABLE db.events TO db.events_v2`, "", false)
	require.NoError(t, err)
	require.Len(t, s.Databases[0].Tables, 1)
	assert.Equal(t, "events_v2", s.Databases[0].Tables[0].Name)
}

func TestApplySQL_MultipleStatementsInOrder(t *testing.T) {
	s := baseSchema()
	sql := `
		ALTER TABLE db.events ADD COLUMN name String;
		ALTER TABLE db.events ADD COLUMN city String AFTER name;
		ALTER TABLE db.events DROP COLUMN ts;
	`
	n, err := ApplySQL(s, sql, "", false)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, []string{"id", "name", "city"}, colNames(s.Databases[0].Tables[0]))
}

func TestApplySQL_RejectsDataOperations(t *testing.T) {
	for _, sql := range []string{
		`TRUNCATE TABLE db.events`,
		`ALTER TABLE db.events DELETE WHERE id = 1`,
		`ALTER TABLE db.events DROP PARTITION '2024-01-01'`,
	} {
		s := baseSchema()
		_, err := ApplySQL(s, sql, "", false)
		require.Error(t, err, "expected rejection for %q", sql)
	}
}

func TestApplySQL_AllowRawCapturesUnexpressibleCreate(t *testing.T) {
	// An inner-engine (non-TO-form) materialized view is parseable but not
	// expressible in the typed model. Strict mode fails; allow-raw captures it.
	sql := `CREATE MATERIALIZED VIEW db.mv ENGINE = MergeTree ORDER BY id AS SELECT id FROM db.events`

	s := &Schema{}
	_, err := ApplySQL(s, sql, "", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "-allow-raw")

	s = &Schema{}
	_, err = ApplySQL(s, sql, "", true)
	require.NoError(t, err)
	require.Len(t, s.Databases, 1)
	require.Len(t, s.Databases[0].Raws, 1)
	assert.Equal(t, "materialized_view", s.Databases[0].Raws[0].Kind)
	assert.Equal(t, "mv", s.Databases[0].Raws[0].Name)
}

func TestApplySQL_ParseErrorAborts(t *testing.T) {
	s := baseSchema()
	_, err := ApplySQL(s, `this is not valid sql`, "", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse SQL")
}

// TestApplySQL_RoundTripThroughHCL exercises the real sql2hcl path: parse HCL
// from disk, resolve, apply SQL edits, emit HCL, and re-parse the emitted HCL —
// confirming the edit survives a full load/edit/emit/reload round-trip.
func TestApplySQL_RoundTripThroughHCL(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "schema.hcl")
	require.NoError(t, os.WriteFile(src, []byte(`
database "db" {
  table "events" {
    order_by = ["id"]
    column "id" { type = "UInt64" }
    column "ts" { type = "DateTime" }
    engine "merge_tree" {}
  }
}
`), 0o644))

	schema, err := ParseFile(src)
	require.NoError(t, err)
	require.NoError(t, Resolve(schema))

	_, err = ApplySQL(schema, `
		ALTER TABLE db.events ADD COLUMN name String AFTER id;
		CREATE TABLE db.users (uid UInt64) ENGINE = MergeTree ORDER BY uid;
	`, "", false)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, Write(&buf, schema))

	out := filepath.Join(dir, "out.hcl")
	require.NoError(t, os.WriteFile(out, buf.Bytes(), 0o644))
	reparsed, err := ParseFile(out)
	require.NoError(t, err)

	require.Len(t, reparsed.Databases, 1)
	db := reparsed.Databases[0]
	require.Len(t, db.Tables, 2)

	events := db.Tables[0]
	require.Equal(t, "events", events.Name)
	assert.Equal(t, []string{"id", "name", "ts"}, colNames(events))
	assert.Equal(t, "users", db.Tables[1].Name)
}
