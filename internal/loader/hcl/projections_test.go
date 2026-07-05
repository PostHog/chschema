package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Projection queries must normalize to the beautified canonical form on
// load, exactly like view/MV queries, so formatting differences never
// surface as drift.
func TestLoad_ProjectionBlocks_NormalizeQuery(t *testing.T) {
	db := mustParseResolve(t, `
database "db" {
  table "events" {
    column "id" { type = "UInt64" }
    column "user_id" { type = "UInt64" }
    projection "by_user" {
      query = "select * order by user_id"
    }
    projection "daily" {
      query    = "SELECT user_id, count() GROUP BY user_id"
      settings = { index_granularity = "4096" }
    }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`)
	require.Len(t, db.Tables, 1)
	ps := db.Tables[0].Projections
	require.Len(t, ps, 2)
	assert.Equal(t, "by_user", ps[0].Name)
	assert.Contains(t, ps[0].Query, "SELECT *")
	assert.Contains(t, ps[0].Query, "ORDER BY user_id")
	assert.Equal(t, map[string]string{"index_granularity": "4096"}, ps[1].Settings)
}

func TestDiff_Projections_AddDropModify(t *testing.T) {
	base := func(ps ...ProjectionSpec) *Schema {
		return &Schema{Databases: []DatabaseSpec{{
			Name: "db",
			Tables: []TableSpec{{
				Name:        "t",
				OrderBy:     []string{"id"},
				Columns:     []ColumnSpec{{Name: "id", Type: "UInt64"}},
				Projections: ps,
				Engine:      &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
			}},
		}}}
	}
	pA := ProjectionSpec{Name: "p", Query: "SELECT id ORDER BY id"}
	pB := ProjectionSpec{Name: "p", Query: "SELECT id, count() GROUP BY id"}

	d := Diff(base(), base(pA))
	require.Len(t, d.Databases, 1)
	require.Len(t, d.Databases[0].AlterTables, 1)
	assert.Equal(t, []ProjectionSpec{pA}, d.Databases[0].AlterTables[0].AddProjections)
	assert.Empty(t, d.Databases[0].AlterTables[0].DropProjections)

	d = Diff(base(pA), base())
	require.Len(t, d.Databases, 1)
	require.Len(t, d.Databases[0].AlterTables, 1)
	assert.Equal(t, []string{"p"}, d.Databases[0].AlterTables[0].DropProjections)

	// Changed query ⇒ drop+add (no ALTER MODIFY PROJECTION exists).
	d = Diff(base(pA), base(pB))
	require.Len(t, d.Databases, 1)
	require.Len(t, d.Databases[0].AlterTables, 1)
	assert.Equal(t, []string{"p"}, d.Databases[0].AlterTables[0].DropProjections)
	assert.Equal(t, []ProjectionSpec{pB}, d.Databases[0].AlterTables[0].AddProjections)

	// Identical projections ⇒ no diff at all.
	d = Diff(base(pA), base(pA))
	assert.Empty(t, d.Databases)
}

func TestSQLGen_ProjectionClause(t *testing.T) {
	assert.Equal(t,
		"by_region (SELECT id, region ORDER BY region)",
		projectionClause(ProjectionSpec{Name: "by_region", Query: "SELECT id, region ORDER BY region"}))
	assert.Equal(t,
		"p (SELECT x ORDER BY x) WITH SETTINGS (index_granularity = 4096)",
		projectionClause(ProjectionSpec{
			Name:     "p",
			Query:    "SELECT x ORDER BY x",
			Settings: map[string]string{"index_granularity": "4096"},
		}))
}

func TestSQLGen_MaterializeProjectionSQL(t *testing.T) {
	assert.Equal(t, "ALTER TABLE db.t MATERIALIZE PROJECTION p",
		materializeProjectionSQL("db", "t", "p"))
}

func TestSQLGen_ProjectionOps(t *testing.T) {
	td := TableDiff{
		Table:           "events",
		DropProjections: []string{"old_p"},
		AddProjections:  []ProjectionSpec{{Name: "by_user", Query: "SELECT * ORDER BY user_id"}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AlterTables: []TableDiff{td}},
	}})
	expected := []string{
		"ALTER TABLE posthog.events DROP PROJECTION old_p, ADD PROJECTION by_user (SELECT * ORDER BY user_id)",
		"ALTER TABLE posthog.events MATERIALIZE PROJECTION by_user",
	}
	assert.Equal(t, expected, out.Statements)
	assert.False(t, out.Ops[0].Manual)
	assert.True(t, out.Ops[1].Manual, "MATERIALIZE PROJECTION must be operator-run, never auto-executed")
}

// A brand-new table's projections are built as data arrives — only ADD
// PROJECTION on an existing table emits the manual MATERIALIZE companion.
func TestSQLGen_CreateTableWithProjection(t *testing.T) {
	tbl := TableSpec{
		Name:        "fresh",
		Columns:     []ColumnSpec{{Name: "id", Type: "UInt64"}},
		Projections: []ProjectionSpec{{Name: "p", Query: "SELECT id ORDER BY id"}},
		Engine:      &EngineSpec{Decoded: EngineMergeTree{}},
		OrderBy:     []string{"id"},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{Database: "posthog", AddTables: []TableSpec{tbl}}}})
	require.Len(t, out.Statements, 1)
	assert.Contains(t, out.Statements[0], "PROJECTION p (SELECT id ORDER BY id)")
	for _, op := range out.Ops {
		assert.False(t, op.Manual)
		assert.NotContains(t, op.SQL, "MATERIALIZE PROJECTION")
	}
}

func TestApplySQL_AddDropProjection(t *testing.T) {
	s := baseSchema()
	_, err := ApplySQL(s, `ALTER TABLE db.events ADD PROJECTION by_user (SELECT * ORDER BY user_id)`, "", false)
	require.NoError(t, err)
	require.Len(t, s.Databases[0].Tables[0].Projections, 1)
	p := s.Databases[0].Tables[0].Projections[0]
	assert.Equal(t, "by_user", p.Name)
	assert.Contains(t, p.Query, "ORDER BY user_id")

	// Duplicate without IF NOT EXISTS errors; with it, no-op.
	_, err = ApplySQL(s, `ALTER TABLE db.events ADD PROJECTION by_user (SELECT * ORDER BY user_id)`, "", false)
	require.Error(t, err)
	_, err = ApplySQL(s, `ALTER TABLE db.events ADD PROJECTION IF NOT EXISTS by_user (SELECT * ORDER BY user_id)`, "", false)
	require.NoError(t, err)
	require.Len(t, s.Databases[0].Tables[0].Projections, 1)

	_, err = ApplySQL(s, `ALTER TABLE db.events DROP PROJECTION by_user`, "", false)
	require.NoError(t, err)
	assert.Empty(t, s.Databases[0].Tables[0].Projections)

	// MATERIALIZE PROJECTION is a data op, not schema DDL — rejected.
	_, err = ApplySQL(s, `ALTER TABLE db.events MATERIALIZE PROJECTION by_user`, "", false)
	require.Error(t, err)
}

// The live probe that exposed #87's silent drop, as a regression test:
// both projection forms must be captured, not skipped.
func TestIntrospect_Projections(t *testing.T) {
	rows := &fakeRows{rows: []fakeRow{{
		name: "t",
		sql: "CREATE TABLE db.t (`id` UInt64, `user_id` UInt64, `ts` DateTime, " +
			"PROJECTION by_user (SELECT * ORDER BY user_id), " +
			"PROJECTION daily_agg (SELECT user_id, toDate(ts) AS d, count() GROUP BY user_id, d)) " +
			"ENGINE = MergeTree ORDER BY (id, ts)",
	}}}
	db := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRows(db, "db", rows))
	require.Len(t, db.Tables, 1)
	ps := db.Tables[0].Projections
	require.Len(t, ps, 2)
	assert.Equal(t, "by_user", ps[0].Name)
	assert.Contains(t, ps[0].Query, "SELECT *")
	assert.Contains(t, ps[0].Query, "ORDER BY user_id")
	assert.NotContains(t, ps[0].Query, "(SELECT", "outer parens must be stripped")
	assert.Equal(t, "daily_agg", ps[1].Name)
	assert.Contains(t, ps[1].Query, "GROUP BY")
}
