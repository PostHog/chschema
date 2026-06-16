package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderObjectSQL_Table(t *testing.T) {
	db := mkDB("posthog",
		mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UInt64"}),
	)
	db.Tables[0].OrderBy = []string{"id"}

	sql, err := RenderObjectSQL("posthog", KindTable, "events", &db)
	require.NoError(t, err)
	assert.Contains(t, sql, "CREATE TABLE posthog.events")
	assert.Contains(t, sql, "ORDER BY (id)")
}

func TestRenderObjectHCL_Table(t *testing.T) {
	db := mkDB("posthog",
		mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UInt64"}),
	)
	db.Tables[0].OrderBy = []string{"id"}

	out, err := RenderObjectHCL("posthog", KindTable, "events", &db)
	require.NoError(t, err)
	assert.Contains(t, out, `table "events"`)
	assert.Contains(t, out, `column "id"`)
}

func TestRenderObjectSQL_MaterializedView(t *testing.T) {
	db := DatabaseSpec{
		Name: "posthog",
		MaterializedViews: []MaterializedViewSpec{
			mkMV("metrics_mv", "posthog.metrics", "SELECT a FROM posthog.src"),
		},
	}
	sql, err := RenderObjectSQL("posthog", KindMaterializedView, "metrics_mv", &db)
	require.NoError(t, err)
	assert.Contains(t, sql, "CREATE MATERIALIZED VIEW posthog.metrics_mv")
	assert.Contains(t, sql, "TO posthog.metrics")
}

func TestRenderObject_UnknownNameAndKind(t *testing.T) {
	db := mkDB("posthog", mkTable("events", EngineMergeTree{}))

	_, err := RenderObjectSQL("posthog", KindTable, "missing", &db)
	require.Error(t, err)

	_, err = RenderObjectHCL("posthog", "bogus", "events", &db)
	require.Error(t, err)
}
