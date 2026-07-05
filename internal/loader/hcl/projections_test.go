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
