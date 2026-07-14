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

// TestMergeDesiredSchemas_IncludesRaws locks the cross-role analogue of issue
// #80: mergeDesiredSchemas must carry raw{} blocks into the union (previously
// omitted, so they never entered the keyspace and dropped out of the plan).
// Dedup is by (database, kind, name) — a raw's identity includes its kind, so a
// same-named raw of a different kind is a distinct object (mirrors indexRaws).
func TestMergeDesiredSchemas_IncludesRaws(t *testing.T) {
	raw := func(kind, name string) RawSpec {
		return RawSpec{Kind: kind, Name: name, SQL: "CREATE " + kind + " posthog." + name}
	}
	merged := mergeDesiredSchemas([]RoleDiff{
		{
			Role: "ops",
			Desired: &Schema{Databases: []DatabaseSpec{{
				Name: "posthog",
				Raws: []RawSpec{raw("dictionary", "dict_a")},
			}}},
		},
		{
			Role: "data",
			Desired: &Schema{Databases: []DatabaseSpec{{
				Name: "posthog",
				Raws: []RawSpec{
					raw("dictionary", "dict_a"), // dupe across roles -> deduped
					raw("dictionary", "dict_b"), // new -> kept
					raw("view", "dict_a"),       // same name, other kind -> kept
				},
			}}},
		},
	})

	require.Len(t, merged.Databases, 1)
	var got []string
	for _, r := range merged.Databases[0].Raws {
		got = append(got, r.Kind+"/"+r.Name)
	}
	assert.ElementsMatch(t, []string{"dictionary/dict_a", "dictionary/dict_b", "view/dict_a"}, got,
		"raws from every role must enter the union, deduped by (kind,name)")
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

// The Manual flag (operator-run statements like MATERIALIZE INDEX) must survive
// the per-role merge into plan operations.
func TestBuildPlan_ManualMaterializeIndexPropagates(t *testing.T) {
	id := ColumnSpec{Name: "id", Type: "UInt64"}
	current := mkTable("events", EngineMergeTree{}, id)
	current.OrderBy = []string{"id"}
	desired := mkTable("events", EngineMergeTree{}, id)
	desired.OrderBy = []string{"id"}
	desired.Indexes = []IndexSpec{{Name: "idx_id", Expr: "id", Type: "minmax", Granularity: 1}}

	plan := BuildPlan([]RoleDiff{{
		Role:    "data",
		Desired: &Schema{Databases: []DatabaseSpec{mkDB("posthog", desired)}},
		Current: &Schema{Databases: []DatabaseSpec{mkDB("posthog", current)}},
	}})

	require.Len(t, plan.Operations, 2)
	byManual := map[bool]PlanOperation{}
	for _, op := range plan.Operations {
		byManual[op.Manual] = op
	}
	assert.Contains(t, byManual[false].SQL, "ADD INDEX idx_id")
	assert.Equal(t, "ALTER TABLE posthog.events MATERIALIZE INDEX idx_id", byManual[true].SQL)
}

// Each role gets its own (non-deduped) object comparison list; a shared
// object drifting on two roles appears under both, and nested op orders
// reference the merged global list.
func TestBuildPlanRoleComparisons(t *testing.T) {
	idCol := ColumnSpec{Name: "id", Type: "UInt64"}
	shared := mkTable("shared_t", EngineMergeTree{}, idCol)
	shared.OrderBy = []string{"id"}

	empty := &Schema{}
	desired := &Schema{Databases: []DatabaseSpec{mkDB("d", shared)}}

	plan := BuildPlan([]RoleDiff{
		{Role: "ops", Desired: desired, Current: empty},
		{Role: "logs", Desired: desired, Current: empty},
	})

	require.Len(t, plan.Roles, 2)
	for i, role := range []string{"ops", "logs"} {
		rc := plan.Roles[i]
		assert.Equal(t, role, rc.Role)
		assert.Equal(t, CompareSummary{TablesAdded: 1}, rc.Summary)
		require.Len(t, rc.Objects, 1)
		assert.Equal(t, StatusAdded, rc.Objects[0].Status)
		require.Len(t, rc.Objects[0].Operations, 1)
	}

	// The identical CREATE dedupes to ONE global operation; both roles' nested
	// op carries that operation's global order.
	require.Len(t, plan.Operations, 1)
	assert.Equal(t, []string{"ops", "logs"}, plan.Operations[0].Roles)
	assert.Equal(t, plan.Operations[0].Order, plan.Roles[0].Objects[0].Operations[0].Order)
	assert.Equal(t, plan.Operations[0].Order, plan.Roles[1].Objects[0].Operations[0].Order)
}
