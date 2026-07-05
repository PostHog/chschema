package main

import (
	"os"
	"path/filepath"
	"testing"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func renderPlanToString(t *testing.T, plan hclload.PlanResult) string {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "plan.txt"))
	require.NoError(t, err)
	renderPlanText(f, plan)
	require.NoError(t, f.Close())
	out, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	return string(out)
}

func TestPlanRenderEmpty(t *testing.T) {
	assert.Equal(t, "no changes\n", renderPlanToString(t, hclload.PlanResult{}))
}

// TestPlanRenderUnsafeOnly: unsafe changes that produce no statement (engine or
// ORDER BY swap needing a full recreate) still print their warnings before the
// "no changes" summary.
func TestPlanRenderUnsafeOnly(t *testing.T) {
	plan := hclload.PlanResult{
		Unsafe: []hclload.JSONUnsafe{
			{Database: "posthog", Object: "events", Reason: "engine changed: MergeTree -> ReplacingMergeTree"},
			{Database: "reports", Object: "daily", Reason: "ORDER BY changed"},
		},
	}
	want := `-- UNSAFE: posthog.events: engine changed: MergeTree -> ReplacingMergeTree
-- UNSAFE: reports.daily: ORDER BY changed
no changes
`
	assert.Equal(t, want, renderPlanToString(t, plan))
}

func TestPlanRenderOperations(t *testing.T) {
	plan := hclload.PlanResult{
		Operations: []hclload.PlanOperation{
			{Order: 0, Kind: hclload.OpCreate, ObjectType: hclload.KindTable, Database: "posthog", Object: "sharded_events", Roles: []string{"data"}},
			{Order: 1, Kind: hclload.OpCreate, ObjectType: hclload.KindMaterializedView, Database: "posthog", Object: "events_mv", Roles: []string{"data", "ops"}},
			{Order: 2, Kind: hclload.OpAlter, ObjectType: hclload.KindTable, Database: "posthog", Object: "events", Roles: []string{"ops"}, Manual: true},
			{Order: 3, Kind: hclload.OpAlter, ObjectType: hclload.KindTable, Database: "posthog", Object: "sessions", Roles: []string{"data", "ops"}, Unsafe: true, Manual: true},
			{Order: 4, Kind: hclload.OpDrop, ObjectType: hclload.KindView, Database: "reports", Object: "daily", Roles: []string{"ops"}, Unsafe: true},
		},
		Unsafe: []hclload.JSONUnsafe{
			{Database: "posthog", Object: "sessions", Reason: "engine changed: MergeTree -> ReplacingMergeTree"},
			{Database: "reports", Object: "daily", Reason: "view definition changed"},
		},
	}
	want := `-- UNSAFE: posthog.sessions: engine changed: MergeTree -> ReplacingMergeTree
-- UNSAFE: reports.daily: view definition changed
  0  CREATE  table              posthog.sharded_events  [data]
  1  CREATE  materialized_view  posthog.events_mv  [data,ops]
  2  ALTER   table              posthog.events  [ops] (MANUAL)
  3  ALTER   table              posthog.sessions  [data,ops] (UNSAFE) (MANUAL)
  4  DROP    view               reports.daily  [ops] (UNSAFE)
`
	assert.Equal(t, want, renderPlanToString(t, plan))
}
