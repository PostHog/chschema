package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Introspection must capture the optional is_deleted engine parameter (#108):
// dropping it is symmetric between a golden and a fresh dump, so diff cannot
// see the loss and the golden's generated SQL would recreate the table
// without soft-delete semantics.
func TestIntrospect_ReplacingMergeTree_IsDeleted(t *testing.T) {
	rows := &fakeRows{rows: []fakeRow{
		{
			name: "adhoc_events_deletion",
			sql: "CREATE TABLE posthog.adhoc_events_deletion (`id` UInt64, `ts` DateTime, `is_deleted` UInt8) " +
				"ENGINE = ReplacingMergeTree(ts, is_deleted) ORDER BY id",
		},
		{
			name: "pg_embeddings",
			sql: "CREATE TABLE posthog.pg_embeddings (`id` UInt64, `timestamp` DateTime, `is_deleted` UInt8) " +
				"ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/noshard/posthog.pg_embeddings', '{replica}-{shard}', timestamp, is_deleted) ORDER BY id",
		},
	}}
	db := &DatabaseSpec{Name: "posthog"}
	require.NoError(t, processIntrospectRows(db, "posthog", rows))
	require.Len(t, db.Tables, 2)

	assert.Equal(t, EngineReplacingMergeTree{
		VersionColumn:   strPtr("ts"),
		IsDeletedColumn: strPtr("is_deleted"),
	}, db.Tables[0].Engine.Decoded)
	assert.Equal(t, EngineReplicatedReplacingMergeTree{
		ZooPath:         "/clickhouse/tables/noshard/posthog.pg_embeddings",
		ReplicaName:     "{replica}-{shard}",
		VersionColumn:   strPtr("timestamp"),
		IsDeletedColumn: strPtr("is_deleted"),
	}, db.Tables[1].Engine.Decoded)
}

// A parameter beyond (ver, is_deleted) must abort introspection loudly, not
// silently drop — a dropped parameter is exactly the false-fidelity failure
// mode of #108.
func TestIntrospect_ReplacingMergeTree_TooManyParamsErrors(t *testing.T) {
	rows := &fakeRows{rows: []fakeRow{{
		name: "t",
		sql:  "CREATE TABLE db.t (`id` UInt64) ENGINE = ReplacingMergeTree(a, b, c) ORDER BY id",
	}}}
	err := processIntrospectRows(&DatabaseSpec{Name: "db"}, "db", rows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at most")

	rows = &fakeRows{rows: []fakeRow{{
		name: "t",
		sql:  "CREATE TABLE db.t (`id` UInt64) ENGINE = ReplicatedReplacingMergeTree('/p', '{replica}', a, b, c) ORDER BY id",
	}}}
	err = processIntrospectRows(&DatabaseSpec{Name: "db"}, "db", rows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at most")
}

func TestSQLGen_Engine_ReplacingIsDeleted(t *testing.T) {
	clause, extra := engineSQL(EngineReplacingMergeTree{
		VersionColumn:   strPtr("ver"),
		IsDeletedColumn: strPtr("is_deleted"),
	})
	assert.Equal(t, "ReplacingMergeTree(ver, is_deleted)", clause)
	assert.Nil(t, extra)

	clause, extra = engineSQL(EngineReplicatedReplacingMergeTree{
		ZooPath:         "/p",
		ReplicaName:     "{replica}",
		VersionColumn:   strPtr("ver"),
		IsDeletedColumn: strPtr("is_deleted"),
	})
	assert.Equal(t, "ReplicatedReplacingMergeTree('/p', '{replica}', ver, is_deleted)", clause)
	assert.Nil(t, extra)
}

// ClickHouse only accepts is_deleted together with ver; the resolver must
// reject is_deleted_column without version_column.
func TestValidateReplacingEngines_IsDeletedRequiresVersion(t *testing.T) {
	for _, engine := range []Engine{
		EngineReplacingMergeTree{IsDeletedColumn: strPtr("is_deleted")},
		EngineReplicatedReplacingMergeTree{ZooPath: "/p", ReplicaName: "{replica}", IsDeletedColumn: strPtr("is_deleted")},
	} {
		s := &Schema{Databases: []DatabaseSpec{{
			Name: "db",
			Tables: []TableSpec{{
				Name:   "t",
				Engine: &EngineSpec{Decoded: engine},
			}},
		}}}
		err := validateReplacingEngines(s)
		require.Error(t, err, "engine %s", engine.Kind())
		assert.Contains(t, err.Error(), "is_deleted_column requires version_column")
	}
}
