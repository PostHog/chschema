package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mkDistTable builds a Distributed-engine table forwarding to remoteDB.remoteTable.
func mkDistTable(name, remoteDB, remoteTable string) TableSpec {
	return mkTable(name, EngineDistributed{
		ClusterName:    "posthog",
		RemoteDatabase: remoteDB,
		RemoteTable:    remoteTable,
	})
}

// mkDBMixed builds a database holding both tables and materialized views.
func mkDBMixed(name string, tables []TableSpec, mvs []MaterializedViewSpec) DatabaseSpec {
	return DatabaseSpec{Name: name, Tables: tables, MaterializedViews: mvs}
}

func TestSplitQualified(t *testing.T) {
	assert.Equal(t, ObjectRef{Database: "db", Name: "tbl"}, splitQualified("db.tbl", "fallback"))
	assert.Equal(t, ObjectRef{Database: "fallback", Name: "tbl"}, splitQualified("tbl", "fallback"))
	assert.Equal(t, ObjectRef{Database: "db", Name: "tbl"}, splitQualified("`db`.`tbl`", "fallback"))
}

func TestParseSkipSet(t *testing.T) {
	none := ParseSkipSet("")
	assert.False(t, none.Skips(ObjectRef{Database: "posthog", Name: "mv"}))

	named := ParseSkipSet("events_mv, posthog.app_dist")
	assert.True(t, named.Skips(ObjectRef{Database: "posthog", Name: "events_mv"}))
	assert.True(t, named.Skips(ObjectRef{Database: "posthog", Name: "app_dist"}))
	assert.False(t, named.Skips(ObjectRef{Database: "other", Name: "events_mv2"}))

	all := ParseSkipSet("*")
	assert.True(t, all.Skips(ObjectRef{Database: "anything", Name: "goes"}))
}

func TestExtractSourceTables(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  []ObjectRef
	}{
		{
			name:  "qualified from",
			query: "SELECT id FROM posthog.src",
			want:  []ObjectRef{{Database: "posthog", Name: "src"}},
		},
		{
			name:  "bare from and join",
			query: "SELECT a.x FROM src a JOIN other b ON a.id = b.id",
			want:  []ObjectRef{{Name: "src"}, {Name: "other"}},
		},
		{
			name:  "subquery source",
			query: "SELECT * FROM (SELECT y FROM nested_tbl) z",
			want:  []ObjectRef{{Name: "nested_tbl"}},
		},
		{
			name:  "cte name filtered out",
			query: "WITH t AS (SELECT 1) SELECT * FROM t JOIN real_table r ON 1 = 1",
			want:  []ObjectRef{{Name: "real_table"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := extractSourceTables(tc.query)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestExtractSourceTables_ParseError(t *testing.T) {
	_, err := extractSourceTables("this is not sql")
	assert.Error(t, err)
}

func TestValidate_ValidSchema(t *testing.T) {
	dbs := []DatabaseSpec{
		mkDBMixed("posthog",
			[]TableSpec{
				mkTable("events_local", EngineMergeTree{}),
				mkTable("metrics", EngineMergeTree{}),
				mkDistTable("events_dist", "posthog", "events_local"),
			},
			[]MaterializedViewSpec{
				mkMV("metrics_mv", "posthog.metrics", "SELECT team_id FROM posthog.events_local"),
			},
		),
	}
	assert.Empty(t, Validate(dbs, ParseSkipSet("")))
}

func TestValidate_MissingMVSource(t *testing.T) {
	dbs := []DatabaseSpec{
		mkDBMixed("posthog",
			[]TableSpec{mkTable("metrics", EngineMergeTree{})},
			[]MaterializedViewSpec{
				mkMV("metrics_mv", "posthog.metrics", "SELECT team_id FROM posthog.events_local"),
			},
		),
	}
	errs := Validate(dbs, ParseSkipSet(""))
	require.Len(t, errs, 1)
	assert.Equal(t, ObjectRef{Database: "posthog", Name: "metrics_mv"}, errs[0].Object)
	assert.Equal(t, ObjectRef{Database: "posthog", Name: "events_local"}, errs[0].Missing)
	assert.Equal(t, DepMVSource, errs[0].Kind)
}

func TestValidate_MissingMVDest(t *testing.T) {
	dbs := []DatabaseSpec{
		mkDBMixed("posthog",
			[]TableSpec{mkTable("events_local", EngineMergeTree{})},
			[]MaterializedViewSpec{
				mkMV("metrics_mv", "posthog.metrics", "SELECT team_id FROM posthog.events_local"),
			},
		),
	}
	errs := Validate(dbs, ParseSkipSet(""))
	require.Len(t, errs, 1)
	assert.Equal(t, ObjectRef{Database: "posthog", Name: "metrics"}, errs[0].Missing)
	assert.Equal(t, DepMVDest, errs[0].Kind)
}

func TestValidate_MissingDistributedRemote(t *testing.T) {
	dbs := []DatabaseSpec{
		mkDBMixed("posthog",
			[]TableSpec{mkDistTable("events_dist", "posthog", "events_local")},
			nil,
		),
	}
	errs := Validate(dbs, ParseSkipSet(""))
	require.Len(t, errs, 1)
	assert.Equal(t, ObjectRef{Database: "posthog", Name: "events_dist"}, errs[0].Object)
	assert.Equal(t, ObjectRef{Database: "posthog", Name: "events_local"}, errs[0].Missing)
	assert.Equal(t, DepDistributedRemote, errs[0].Kind)
}

func TestValidate_UnloadedDatabase(t *testing.T) {
	dbs := []DatabaseSpec{
		mkDBMixed("posthog",
			[]TableSpec{mkDistTable("events_dist", "warehouse", "events_local")},
			nil,
		),
	}
	errs := Validate(dbs, ParseSkipSet(""))
	require.Len(t, errs, 1)
	assert.Equal(t, ObjectRef{Database: "warehouse", Name: "events_local"}, errs[0].Missing)
	assert.Contains(t, errs[0].Reason, "is not loaded")
}

func TestValidate_SkipByName(t *testing.T) {
	dbs := []DatabaseSpec{
		mkDBMixed("posthog",
			[]TableSpec{mkDistTable("events_dist", "posthog", "events_local")},
			[]MaterializedViewSpec{
				mkMV("metrics_mv", "posthog.metrics", "SELECT x FROM posthog.src"),
			},
		),
	}
	// Without skipping: events_dist (1 missing remote) + metrics_mv (missing dest + source).
	assert.Len(t, Validate(dbs, ParseSkipSet("")), 3)

	// Skipping the MV by name leaves only the Distributed table's error.
	errs := Validate(dbs, ParseSkipSet("metrics_mv"))
	require.Len(t, errs, 1)
	assert.Equal(t, "events_dist", errs[0].Object.Name)
}

func TestValidate_SkipAll(t *testing.T) {
	dbs := []DatabaseSpec{
		mkDBMixed("posthog",
			[]TableSpec{mkDistTable("events_dist", "posthog", "missing")},
			[]MaterializedViewSpec{
				mkMV("metrics_mv", "posthog.gone", "SELECT x FROM posthog.absent"),
			},
		),
	}
	assert.Empty(t, Validate(dbs, ParseSkipSet("*")))
}

func TestCollectDependencies_DefaultsSourceDatabase(t *testing.T) {
	dbs := []DatabaseSpec{
		mkDBMixed("posthog", nil, []MaterializedViewSpec{
			// Unqualified source table should default to the MV's database.
			mkMV("mv", "metrics", "SELECT x FROM events_local"),
		}),
	}
	deps, err := CollectDependencies(dbs)
	require.NoError(t, err)
	require.Len(t, deps, 2)

	byKind := map[string]Dependency{}
	for _, d := range deps {
		byKind[d.Kind] = d
	}
	assert.Equal(t, ObjectRef{Database: "posthog", Name: "metrics"}, byKind[DepMVDest].To)
	assert.Equal(t, ObjectRef{Database: "posthog", Name: "events_local"}, byKind[DepMVSource].To)
}
