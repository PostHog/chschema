package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Introspection must capture the optional 5th (policy_name) Distributed
// parameter (#109): dropping it is symmetric between a golden and a fresh
// dump, so diff cannot see the loss and generated SQL would recreate the
// table without its storage policy.
func TestIntrospect_Distributed_PolicyName(t *testing.T) {
	rows := &fakeRows{rows: []fakeRow{{
		name: "events_dist",
		sql: "CREATE TABLE posthog.events_dist (`id` UInt64) " +
			"ENGINE = Distributed('posthog', 'posthog', 'events', sipHash64(id), 'tiered')",
	}}}
	db := &DatabaseSpec{Name: "posthog"}
	require.NoError(t, processIntrospectRows(db, "posthog", rows))
	require.Len(t, db.Tables, 1)
	assert.Equal(t, EngineDistributed{
		ClusterName:    "posthog",
		RemoteDatabase: "posthog",
		RemoteTable:    "events",
		ShardingKey:    strPtr("sipHash64(id)"),
		PolicyName:     strPtr("tiered"),
	}, db.Tables[0].Engine.Decoded)
}

// The Distributed signature is positional: policy_name can only be sent
// when sharding_key occupies the 4th slot.
func TestValidateDistributedEngines_PolicyRequiresShardingKey(t *testing.T) {
	s := &Schema{Databases: []DatabaseSpec{{
		Name: "db",
		Tables: []TableSpec{{
			Name: "t",
			Engine: &EngineSpec{Decoded: EngineDistributed{
				ClusterName:    "c",
				RemoteDatabase: "db",
				RemoteTable:    "r",
				PolicyName:     strPtr("tiered"),
			}},
		}},
	}}}
	err := validateDistributedEngines(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "policy_name requires sharding_key")
}

func TestSQLGen_Engine_DistributedPolicyName(t *testing.T) {
	clause, extra := engineSQL(EngineDistributed{
		ClusterName:    "posthog",
		RemoteDatabase: "default",
		RemoteTable:    "events",
		ShardingKey:    strPtr("sipHash64(id)"),
		PolicyName:     strPtr("tiered"),
	})
	assert.Equal(t, "Distributed('posthog', 'default', 'events', sipHash64(id), 'tiered')", clause)
	assert.Nil(t, extra)
}

// A parameter beyond policy_name must abort introspection loudly (#109).
func TestIntrospect_Distributed_TooManyParamsErrors(t *testing.T) {
	rows := &fakeRows{rows: []fakeRow{{
		name: "t",
		sql:  "CREATE TABLE db.t (`id` UInt64) ENGINE = Distributed('c', 'db', 'r', rand(), 'p', extra)",
	}}}
	err := processIntrospectRows(&DatabaseSpec{Name: "db"}, "db", rows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at most")
}
