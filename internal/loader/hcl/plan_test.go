package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// distTable builds a Distributed proxy forwarding to remoteTable on the ops cluster.
func distTable(name, remoteTable string, cols ...ColumnSpec) TableSpec {
	return TableSpec{
		Name:    name,
		Columns: cols,
		Engine: &EngineSpec{
			Kind: "distributed",
			Decoded: EngineDistributed{
				ClusterName:    "ops",
				RemoteDatabase: "posthog",
				RemoteTable:    remoteTable,
			},
		},
	}
}

// TestBuildPlan_CrossRoleAlterOrdering encodes the regression a consumer-side
// name sort can't catch: adding a column to the OPS storage table must be
// ordered before the per-role Distributed write proxies, which must precede the
// MV — even though the object names sort the other way
// (ops_query_log_archive_mv < sharded_query_log_archive < writable_query_log_archive).
func TestBuildPlan_CrossRoleAlterOrdering(t *testing.T) {
	id := ColumnSpec{Name: "id", Type: "UInt64"}
	team := ColumnSpec{Name: "team_id", Type: "Int64"}

	mkDB := func(withTeam bool, includeStorage bool) DatabaseSpec {
		cols := []ColumnSpec{id}
		query := "SELECT id FROM system.query_log"
		if withTeam {
			cols = []ColumnSpec{id, team}
			query = "SELECT id, team_id FROM system.query_log"
		}
		db := DatabaseSpec{Name: "posthog"}
		if includeStorage {
			db.Tables = append(db.Tables, mkTable("sharded_query_log_archive", EngineReplicatedMergeTree{}, cols...))
		}
		db.Tables = append(db.Tables, distTable("writable_query_log_archive", "sharded_query_log_archive", cols...))
		db.MaterializedViews = []MaterializedViewSpec{
			{Name: "ops_query_log_archive_mv", ToTable: "posthog.writable_query_log_archive", Query: query},
		}
		return db
	}

	roleDiff := func(role string, includeStorage bool) RoleDiff {
		return RoleDiff{
			Role:    role,
			Desired: &Schema{Databases: []DatabaseSpec{mkDB(true, includeStorage)}},
			Current: &Schema{Databases: []DatabaseSpec{mkDB(false, includeStorage)}},
		}
	}

	// ops hosts the physical storage; data only has the proxies + MV.
	plan := BuildPlan([]RoleDiff{
		roleDiff("ops", true),
		roleDiff("data", false),
	})

	order := make(map[string]PlanOperation, len(plan.Operations))
	for _, op := range plan.Operations {
		order[op.Object] = op
		assert.Equal(t, OpAlter, op.Kind, "every op here is an ALTER (%s)", op.Object)
	}

	require.Contains(t, order, "sharded_query_log_archive")
	require.Contains(t, order, "writable_query_log_archive")
	require.Contains(t, order, "ops_query_log_archive_mv")

	sharded := order["sharded_query_log_archive"]
	writable := order["writable_query_log_archive"]
	mv := order["ops_query_log_archive_mv"]

	// Dependency order: storage -> write proxy -> MV.
	assert.Less(t, sharded.Order, writable.Order, "storage must ALTER before the write proxy")
	assert.Less(t, writable.Order, mv.Order, "the write proxy must ALTER before the MV")

	// The MV sorts first by name but must come LAST by dependency.
	assert.Equal(t, len(plan.Operations)-1, mv.Order, "MV (name-sorts-first) must be ordered last")

	// Identical statements across roles dedupe to one op with the role union.
	assert.ElementsMatch(t, []string{"ops", "data"}, writable.Roles, "proxy ALTER exists on both roles")
	assert.ElementsMatch(t, []string{"ops", "data"}, mv.Roles, "MV MODIFY QUERY exists on both roles")
	assert.Equal(t, []string{"ops"}, sharded.Roles, "storage lives only on ops")

	// Engine enrichment carries through (#64 shape) for the storage table.
	assert.Equal(t, "ReplicatedMergeTree", sharded.Engine)
	assert.True(t, sharded.Replicated)
}
