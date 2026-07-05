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
