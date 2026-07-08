package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate_RawObjectSatisfiesReference(t *testing.T) {
	dbs := []DatabaseSpec{{
		Name: "db",
		// A Distributed table forwarding to a raw-captured local table.
		Tables: []TableSpec{mkDistTable("dist", "db", "raw_local")},
		Raws: []RawSpec{
			{Kind: "table", Name: "raw_local", SQL: "CREATE TABLE db.raw_local (a UInt64) ENGINE = MergeTree ORDER BY a\n"},
		},
	}}
	errs := Validate(dbs, ParseSkipSet(""), ClusterSet{})
	require.Empty(t, errs, "a declared raw block satisfies a Distributed remote_table reference")
}

// mkDistTable builds a Distributed-engine table forwarding to remoteDB.remoteTable.
func mkDistTable(name, remoteDB, remoteTable string) TableSpec {
	return mkDistTableOn(name, "posthog", remoteDB, remoteTable)
}

// mkDistTableOn builds a Distributed-engine table on an explicit cluster.
func mkDistTableOn(name, cluster, remoteDB, remoteTable string) TableSpec {
	return mkTable(name, EngineDistributed{
		ClusterName:    cluster,
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
	assert.Empty(t, Validate(dbs, ParseSkipSet(""), ClusterSet{}))
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
	errs := Validate(dbs, ParseSkipSet(""), ClusterSet{})
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
	errs := Validate(dbs, ParseSkipSet(""), ClusterSet{})
	require.Len(t, errs, 1)
	assert.Equal(t, ObjectRef{Database: "posthog", Name: "metrics"}, errs[0].Missing)
	assert.Equal(t, DepMVDest, errs[0].Kind)
}

func TestValidate_ViewSourceTableMustExist(t *testing.T) {
	dbs := []DatabaseSpec{{
		Name:   "posthog",
		Tables: []TableSpec{mkTable("events_local", EngineMergeTree{})},
		Views: []ViewSpec{
			{Name: "v", Query: "SELECT team_id FROM posthog.nonexistent"},
		},
	}}
	errs := Validate(dbs, ParseSkipSet(""), ClusterSet{})
	require.Len(t, errs, 1)
	assert.Equal(t, ObjectRef{Database: "posthog", Name: "v"}, errs[0].Object)
	assert.Equal(t, ObjectRef{Database: "posthog", Name: "nonexistent"}, errs[0].Missing)
	assert.Equal(t, DepViewSource, errs[0].Kind)
}

func TestValidate_ViewSourceTableInSameSchema(t *testing.T) {
	dbs := []DatabaseSpec{{
		Name:   "posthog",
		Tables: []TableSpec{mkTable("events_local", EngineMergeTree{})},
		Views: []ViewSpec{
			{Name: "v", Query: "SELECT team_id FROM posthog.events_local"},
		},
	}}
	assert.Empty(t, Validate(dbs, ParseSkipSet(""), ClusterSet{}))
}

func TestValidate_ViewSkipByName(t *testing.T) {
	dbs := []DatabaseSpec{{
		Name: "posthog",
		Views: []ViewSpec{
			{Name: "v", Query: "SELECT 1 FROM posthog.missing"},
		},
	}}
	assert.Empty(t, Validate(dbs, ParseSkipSet("v"), ClusterSet{}))
}

func TestValidate_ViewCTENameNotASource(t *testing.T) {
	dbs := []DatabaseSpec{{
		Name:   "posthog",
		Tables: []TableSpec{mkTable("events_local", EngineMergeTree{})},
		Views: []ViewSpec{
			{Name: "v", Query: "WITH stats AS (SELECT team_id FROM posthog.events_local) SELECT * FROM stats"},
		},
	}}
	assert.Empty(t, Validate(dbs, ParseSkipSet(""), ClusterSet{}))
}

func TestValidate_MissingDistributedRemote(t *testing.T) {
	dbs := []DatabaseSpec{
		mkDBMixed("posthog",
			[]TableSpec{mkDistTable("events_dist", "posthog", "events_local")},
			nil,
		),
	}
	errs := Validate(dbs, ParseSkipSet(""), ClusterSet{})
	require.Len(t, errs, 1)
	assert.Equal(t, ObjectRef{Database: "posthog", Name: "events_dist"}, errs[0].Object)
	assert.Equal(t, ObjectRef{Database: "posthog", Name: "events_local"}, errs[0].Missing)
	assert.Equal(t, DepDistributedRemote, errs[0].Kind)
}

func TestValidate_UnloadedDatabase(t *testing.T) {
	// A materialized view writing into a database that isn't loaded cannot be
	// checked, so the reference itself is an error. Distributed remotes route
	// through the cluster-aware algorithm instead (see the DistributedRemote
	// tests), so this exercises the non-Distributed "database not loaded" path.
	dbs := []DatabaseSpec{
		mkDBMixed("posthog",
			[]TableSpec{mkTable("events_local", EngineMergeTree{})},
			[]MaterializedViewSpec{mkMV("events_mv", "warehouse.events_dest", "SELECT x FROM posthog.events_local")},
		),
	}
	errs := Validate(dbs, ParseSkipSet(""), ClusterSet{})
	require.Len(t, errs, 1)
	assert.Equal(t, ObjectRef{Database: "warehouse", Name: "events_dest"}, errs[0].Missing)
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
	assert.Len(t, Validate(dbs, ParseSkipSet(""), ClusterSet{}), 3)

	// Skipping the MV by name leaves only the Distributed table's error.
	errs := Validate(dbs, ParseSkipSet("metrics_mv"), ClusterSet{})
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
	assert.Empty(t, Validate(dbs, ParseSkipSet("*"), ClusterSet{}))
}

// auxCluster is a small external "aux" composition: it declares
// posthog.sharded_web_stats_preaggregated, the storage table an off-node
// Distributed proxy forwards to.
func auxCluster() ClusterSet {
	cs := NewClusterSet()
	cs.Add("aux", []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{mkTable("sharded_web_stats_preaggregated", EngineMergeTree{})},
		nil,
	)})
	return cs
}

// A cross-cluster proxy resolves against its mapped cluster's composition.
func TestValidate_DistributedRemote_ResolvesViaMappedCluster(t *testing.T) {
	dbs := []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{mkDistTableOn("web_stats_preaggregated", "aux", "posthog", "sharded_web_stats_preaggregated")},
		nil,
	)}
	assert.Empty(t, Validate(dbs, ParseSkipSet(""), auxCluster()),
		"remote declared in the mapped aux cluster resolves")
}

// A remote missing from its mapped cluster is real cross-cluster drift.
func TestValidate_DistributedRemote_MissingInMappedCluster(t *testing.T) {
	dbs := []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{mkDistTableOn("web_stats_preaggregated", "aux", "posthog", "sharded_gone")},
		nil,
	)}
	errs := Validate(dbs, ParseSkipSet(""), auxCluster())
	require.Len(t, errs, 1)
	assert.Equal(t, ObjectRef{Database: "posthog", Name: "sharded_gone"}, errs[0].Missing)
	assert.Equal(t, DepDistributedRemote, errs[0].Kind)
	assert.Contains(t, errs[0].Reason, `cluster "aux"`)
}

// An off-node remote with no cluster mapping errors — the anti-staleness guard.
func TestValidate_DistributedRemote_UnknownClusterNoMapping(t *testing.T) {
	dbs := []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{mkDistTableOn("web_stats_preaggregated", "aux", "posthog", "sharded_web_stats_preaggregated")},
		nil,
	)}
	errs := Validate(dbs, ParseSkipSet(""), ClusterSet{})
	require.Len(t, errs, 1)
	assert.Equal(t, DepDistributedRemote, errs[0].Kind)
	assert.Contains(t, errs[0].Reason, "no -cluster mapping")
}

// An alias cluster resolves against its base cluster's composition.
func TestValidate_DistributedRemote_AliasResolvesViaBase(t *testing.T) {
	cs := auxCluster() // maps "aux"
	cs.AddAlias("aux_writable", "aux")
	dbs := []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{mkDistTableOn("web_stats_writable", "aux_writable", "posthog", "sharded_web_stats_preaggregated")},
		nil,
	)}
	assert.Empty(t, Validate(dbs, ParseSkipSet(""), cs),
		"alias resolves against the base cluster's composition")
}

// An alias whose base is @absent inherits the base's satisfied status.
func TestValidate_DistributedRemote_AliasToAbsentBase(t *testing.T) {
	cs := NewClusterSet()
	cs.AddAbsent("batch_exports")
	cs.AddAlias("batch_exports_writable", "batch_exports")
	dbs := []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{mkDistTableOn("exports_proxy", "batch_exports_writable", "posthog", "sharded_exports")},
		nil,
	)}
	assert.Empty(t, Validate(dbs, ParseSkipSet(""), cs))
}

// An alias whose base has no mapping errors, naming both the alias and base.
func TestValidate_DistributedRemote_AliasToUnmappedBase(t *testing.T) {
	cs := NewClusterSet()
	cs.AddAlias("aux_writable", "aux")
	dbs := []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{mkDistTableOn("web_stats_writable", "aux_writable", "posthog", "sharded_web_stats_preaggregated")},
		nil,
	)}
	errs := Validate(dbs, ParseSkipSet(""), cs)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Reason, `"aux_writable" (alias of "aux")`)
	assert.Contains(t, errs[0].Reason, "no -cluster mapping")
}

// A materialized view whose source table lives on a mapped sibling cluster
// resolves against that cluster instead of erroring (co-located composition).
func TestValidate_MVSource_ResolvesViaMappedCluster(t *testing.T) {
	dbs := []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{mkTable("web_stats_mv_target", EngineMergeTree{})},
		[]MaterializedViewSpec{mkMV("web_stats_mv", "posthog.web_stats_mv_target",
			"SELECT day FROM posthog.sharded_web_stats_preaggregated")},
	)}
	assert.Empty(t, Validate(dbs, ParseSkipSet(""), auxCluster()),
		"MV source declared in a mapped cluster resolves")
}

// A plain view's source table resolves the same way.
func TestValidate_ViewSource_ResolvesViaMappedCluster(t *testing.T) {
	dbs := []DatabaseSpec{{
		Name:  "posthog",
		Views: []ViewSpec{{Name: "web_stats_view", Query: "SELECT day FROM posthog.sharded_web_stats_preaggregated"}},
	}}
	assert.Empty(t, Validate(dbs, ParseSkipSet(""), auxCluster()),
		"view source declared in a mapped cluster resolves")
}

// A source declared on neither the node nor any mapped cluster is a real error.
func TestValidate_MVSource_MissingEverywhere(t *testing.T) {
	dbs := []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{mkTable("web_stats_mv_target", EngineMergeTree{})},
		[]MaterializedViewSpec{mkMV("web_stats_mv", "posthog.web_stats_mv_target",
			"SELECT day FROM posthog.nonexistent_src")},
	)}
	errs := Validate(dbs, ParseSkipSet(""), auxCluster())
	require.Len(t, errs, 1)
	assert.Equal(t, ObjectRef{Database: "posthog", Name: "nonexistent_src"}, errs[0].Missing)
	assert.Equal(t, DepMVSource, errs[0].Kind)
}

// With no -cluster mapping, a cross-cluster source errors (backward compatible;
// documents the migration path for callers).
func TestValidate_MVSource_NoMappingErrors(t *testing.T) {
	dbs := []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{mkTable("web_stats_mv_target", EngineMergeTree{})},
		[]MaterializedViewSpec{mkMV("web_stats_mv", "posthog.web_stats_mv_target",
			"SELECT day FROM posthog.sharded_web_stats_preaggregated")},
	)}
	assert.Len(t, Validate(dbs, ParseSkipSet(""), ClusterSet{}), 1,
		"no mapping -> cross-cluster source is unresolved")
}

// An @absent cluster contributes no composition, so it never satisfies an
// MV/View source (unlike a named Distributed remote). The source must really
// exist somewhere.
func TestValidate_MVSource_AbsentClusterDoesNotSatisfy(t *testing.T) {
	cs := NewClusterSet()
	cs.AddAbsent("aux")
	dbs := []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{mkTable("web_stats_mv_target", EngineMergeTree{})},
		[]MaterializedViewSpec{mkMV("web_stats_mv", "posthog.web_stats_mv_target",
			"SELECT day FROM posthog.sharded_web_stats_preaggregated")},
	)}
	assert.Len(t, Validate(dbs, ParseSkipSet(""), cs), 1,
		"@absent does not satisfy an MV source")
}

// The fallback is scoped to SELECT sources: a materialized view's destination
// (a write target, which must be local) is not resolved against mapped clusters.
func TestValidate_MVDest_NotResolvedCrossCluster(t *testing.T) {
	cs := NewClusterSet()
	cs.Add("aux", []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{mkTable("remote_dest", EngineMergeTree{})}, nil)})
	dbs := []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{mkTable("local_src", EngineMergeTree{})},
		[]MaterializedViewSpec{mkMV("mv", "posthog.remote_dest", "SELECT x FROM posthog.local_src")},
	)}
	errs := Validate(dbs, ParseSkipSet(""), cs)
	require.Len(t, errs, 1)
	assert.Equal(t, ObjectRef{Database: "posthog", Name: "remote_dest"}, errs[0].Missing)
	assert.Equal(t, DepMVDest, errs[0].Kind)
}

// A cluster marked @absent has no local composition; references into it are
// structurally unresolvable and count as satisfied.
func TestValidate_DistributedRemote_AbsentCluster(t *testing.T) {
	cs := NewClusterSet()
	cs.AddAbsent("batch_exports")
	dbs := []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{mkDistTableOn("exports_proxy", "batch_exports", "posthog", "sharded_exports")},
		nil,
	)}
	assert.Empty(t, Validate(dbs, ParseSkipSet(""), cs))
}

// References into the built-in system database are satisfied for every
// dependency kind, not just Distributed remotes: a view or MV reading a
// system.* table needs no skip entry.
func TestValidate_SystemDatabaseSourceSatisfied(t *testing.T) {
	dbs := []DatabaseSpec{{
		Name:  "posthog",
		Views: []ViewSpec{mkView("custom_metrics", "SELECT count() FROM system.parts")},
		MaterializedViews: []MaterializedViewSpec{
			mkMV("ops_query_log_archive_mv", "posthog.query_log_archive", "SELECT event_time FROM system.query_log"),
		},
		Tables: []TableSpec{mkTable("query_log_archive", EngineMergeTree{})},
	}}
	assert.Empty(t, Validate(dbs, ParseSkipSet(""), ClusterSet{}),
		"view/MV sources into system.* are satisfied without a skip or mapping")
}

// A system.* remote is a built-in and needs no cluster mapping.
func TestValidate_DistributedRemote_SystemDB(t *testing.T) {
	dbs := []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{mkDistTableOn("query_log_proxy", "aux", "system", "query_log")},
		nil,
	)}
	assert.Empty(t, Validate(dbs, ParseSkipSet(""), ClusterSet{}),
		"system.* remote is satisfied without any mapping")
}

// A remote declared on the node's own schema resolves locally regardless of
// the (unmapped) cluster name it carries.
func TestValidate_DistributedRemote_LocalWins(t *testing.T) {
	dbs := []DatabaseSpec{mkDBMixed("posthog",
		[]TableSpec{
			mkDistTableOn("events", "some_cluster", "posthog", "sharded_events"),
			mkTable("sharded_events", EngineMergeTree{}),
		},
		nil,
	)}
	assert.Empty(t, Validate(dbs, ParseSkipSet(""), ClusterSet{}),
		"a locally declared remote resolves even with an unmapped cluster name")
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

func TestExtractReferencedTables(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want []ObjectRef
	}{
		{
			name: "create table — no refs",
			sql:  `CREATE TABLE default.events (id UInt64) ENGINE = MergeTree ORDER BY id`,
		},
		{
			name: "MV: TO dest + SELECT FROM src",
			sql:  `CREATE MATERIALIZED VIEW default.mv TO default.dest (` + "`id`" + ` UInt64) AS SELECT id FROM default.src`,
			want: []ObjectRef{{Database: "default", Name: "dest"}, {Database: "default", Name: "src"}},
		},
		{
			name: "view: SELECT FROM",
			sql:  `CREATE VIEW default.v AS SELECT * FROM default.events`,
			want: []ObjectRef{{Database: "default", Name: "events"}},
		},
		{
			name: "dictionary: SOURCE(CLICKHOUSE(TABLE 'x'))",
			sql:  `CREATE DICTIONARY default.d (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src_tbl')) LIFETIME(0) LAYOUT(HASHED())`,
			want: []ObjectRef{{Name: "src_tbl"}},
		},
		{
			name: "dictionary: SOURCE(CLICKHOUSE(QUERY 'SELECT ... FROM y'))",
			sql:  `CREATE DICTIONARY default.d (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(QUERY 'SELECT k, v FROM default.src')) LIFETIME(0) LAYOUT(HASHED())`,
			want: []ObjectRef{{Database: "default", Name: "src"}},
		},
		{
			name: "MV with CTE: CTE name filtered out",
			sql:  `CREATE MATERIALIZED VIEW default.mv TO default.dest AS WITH cte AS (SELECT * FROM default.real_src) SELECT * FROM cte`,
			want: []ObjectRef{{Database: "default", Name: "dest"}, {Database: "default", Name: "real_src"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ExtractReferencedTables(tc.sql)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestExtractDeclaredColumns(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want []DeclaredColumn
	}{
		{
			name: "create table",
			sql:  "CREATE TABLE default.t (`a` UInt64, `b` String) ENGINE = MergeTree ORDER BY a",
			want: []DeclaredColumn{{Name: "a", Type: "UInt64"}, {Name: "b", Type: "String"}},
		},
		{
			name: "MV with TO dest (col list)",
			sql:  "CREATE MATERIALIZED VIEW default.mv TO default.dest (`id` UInt64, `payload` String) AS SELECT id, payload FROM default.src",
			want: []DeclaredColumn{{Name: "id", Type: "UInt64"}, {Name: "payload", Type: "String"}},
		},
		{
			name: "dict attributes",
			sql:  "CREATE DICTIONARY default.d (`k` UInt64, `v` String) PRIMARY KEY k SOURCE(NULL()) LIFETIME(0) LAYOUT(HASHED())",
			want: []DeclaredColumn{{Name: "k", Type: "UInt64"}, {Name: "v", Type: "String"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ExtractDeclaredColumns(tc.sql)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// --- MV column validation (heuristic) ----------------------------------

// mvKafkaFixture builds a single-DB schema with a Kafka source table and
// an MV writing to a (non-existent in v1 fixtures) destination — the
// destination's existence is checked by the dependency pass, not the
// column pass, so we declare a stub destination too.
func mvKafkaFixture(query string, sourceCols ...ColumnSpec) []DatabaseSpec {
	dest := TableSpec{
		Name:    "events_local",
		Columns: sourceCols,
		Engine:  &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
	}
	src := TableSpec{
		Name:    "events_kafka",
		Columns: sourceCols,
		Engine: &EngineSpec{Kind: "kafka", Decoded: EngineKafka{
			BrokerList: ptr("k:9092"), TopicList: ptr("ev"),
			GroupName: ptr("g"), Format: ptr("JSONEachRow"),
		}},
	}
	mv := MaterializedViewSpec{
		Name:    "events_mv",
		ToTable: "events_local",
		Query:   query,
	}
	return []DatabaseSpec{{
		Name:              "default",
		Tables:            []TableSpec{src, dest},
		MaterializedViews: []MaterializedViewSpec{mv},
	}}
}

func TestValidate_MVColumn_VirtualRefAccepted(t *testing.T) {
	dbs := mvKafkaFixture(
		"SELECT _offset, team_id FROM events_kafka",
		ColumnSpec{Name: "team_id", Type: "UInt32"},
	)
	errs := Validate(dbs, SkipSet{}, ClusterSet{})
	for _, e := range errs {
		assert.NotEqual(t, KindMVColumn, e.Kind, "real Kafka virtual must not be flagged: %s", e.Reason)
	}
}

func TestValidate_MVColumn_BogusVirtualRefFlagged(t *testing.T) {
	dbs := mvKafkaFixture(
		"SELECT _offsett, team_id FROM events_kafka",
		ColumnSpec{Name: "team_id", Type: "UInt32"},
	)
	errs := Validate(dbs, SkipSet{}, ClusterSet{})
	var mvErr *ValidationError
	for i := range errs {
		if errs[i].Kind == KindMVColumn {
			mvErr = &errs[i]
		}
	}
	require.NotNil(t, mvErr, "typo'd virtual should be flagged")
	assert.Contains(t, mvErr.Reason, "_offsett")
	assert.Equal(t, "events_mv", mvErr.Object.Name)
}

func TestValidate_MVColumn_HeadersDottedRefAccepted(t *testing.T) {
	dbs := mvKafkaFixture(
		"SELECT _headers.name, team_id FROM events_kafka",
		ColumnSpec{Name: "team_id", Type: "UInt32"},
	)
	errs := Validate(dbs, SkipSet{}, ClusterSet{})
	for _, e := range errs {
		assert.NotEqual(t, KindMVColumn, e.Kind, "_headers.name is a real Kafka virtual: %s", e.Reason)
	}
}

func TestValidate_MVColumn_DeclaredColumnNotFlagged(t *testing.T) {
	// _meta is declared on the source — should pass even though it starts
	// with `_` and is not a Kafka virtual.
	dbs := mvKafkaFixture(
		"SELECT _meta, team_id FROM events_kafka",
		ColumnSpec{Name: "team_id", Type: "UInt32"},
		ColumnSpec{Name: "_meta", Type: "String"},
	)
	errs := Validate(dbs, SkipSet{}, ClusterSet{})
	for _, e := range errs {
		assert.NotEqual(t, KindMVColumn, e.Kind, "declared column must not be flagged: %s", e.Reason)
	}
}

func TestValidate_MVColumn_SelectStarSkipped(t *testing.T) {
	dbs := mvKafkaFixture(
		"SELECT * FROM events_kafka",
		ColumnSpec{Name: "team_id", Type: "UInt32"},
	)
	errs := Validate(dbs, SkipSet{}, ClusterSet{})
	for _, e := range errs {
		assert.NotEqual(t, KindMVColumn, e.Kind, "SELECT * must skip the column check: %s", e.Reason)
	}
}

func TestValidate_MVColumn_JoinSkipped(t *testing.T) {
	// JOIN with second source ⇒ attribution ambiguous ⇒ bail.
	dbs := mvKafkaFixture(
		"SELECT _offsett, team_id FROM events_kafka JOIN events_local USING team_id",
		ColumnSpec{Name: "team_id", Type: "UInt32"},
	)
	errs := Validate(dbs, SkipSet{}, ClusterSet{})
	for _, e := range errs {
		assert.NotEqual(t, KindMVColumn, e.Kind, "JOIN must skip the column check: %s", e.Reason)
	}
}

func TestValidate_MVColumn_AliasNotFlagged(t *testing.T) {
	// An aliased projection like `countState() AS _agg_count` introduces
	// the name `_agg_count` — it's an output binding, not a source ref.
	dbs := mvKafkaFixture(
		"SELECT countState() AS _agg_count, team_id FROM events_kafka GROUP BY team_id",
		ColumnSpec{Name: "team_id", Type: "UInt32"},
	)
	errs := Validate(dbs, SkipSet{}, ClusterSet{})
	for _, e := range errs {
		assert.NotEqual(t, KindMVColumn, e.Kind, "alias name must not be flagged as a source ref: %s", e.Reason)
	}
}

func TestValidate_MVColumn_SkipSetWorks(t *testing.T) {
	dbs := mvKafkaFixture(
		"SELECT _offsett, team_id FROM events_kafka",
		ColumnSpec{Name: "team_id", Type: "UInt32"},
	)
	skip := ParseSkipSet("events_mv")
	errs := Validate(dbs, skip, ClusterSet{})
	for _, e := range errs {
		assert.NotEqual(t, KindMVColumn, e.Kind, "skip should suppress mv_column: %s", e.Reason)
	}
}

func TestValidate_TimeSeries_ExternalTargetMustExist(t *testing.T) {
	tgt := "db.samples_does_not_exist"
	dbs := []DatabaseSpec{{Name: "db",
		Tables: []TableSpec{{Name: "m",
			Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
			Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
				Samples: &TimeSeriesTarget{Target: &tgt},
			}},
		}},
	}}
	errs := Validate(dbs, SkipSet{}, ClusterSet{})
	var ferr *ValidationError
	for i := range errs {
		if errs[i].Kind == DepTimeSeriesTarget {
			ferr = &errs[i]
			break
		}
	}
	require.NotNil(t, ferr, "external TimeSeries target should be flagged")
	assert.Equal(t, "db.samples_does_not_exist", ferr.Missing.String())
}

func TestValidate_TimeSeries_InnerTargetNoDependency(t *testing.T) {
	dbs := []DatabaseSpec{{Name: "db",
		Tables: []TableSpec{{Name: "m",
			Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
			Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
				Samples: &TimeSeriesTarget{Inner: &TimeSeriesInnerTable{
					Columns: []ColumnSpec{{Name: "id", Type: "UUID"}},
					Engine:  &EngineSpec{Decoded: EngineMergeTree{}},
				}},
			}},
		}},
	}}
	errs := Validate(dbs, SkipSet{}, ClusterSet{})
	for _, e := range errs {
		assert.NotEqual(t, DepTimeSeriesTarget, e.Kind, "inner-form target should create no dep")
	}
}

func TestValidate_TimeSeries_AllTargetsExist_OK(t *testing.T) {
	tgtData := "db.m_data"
	tgtTags := "db.m_tags"
	tgtMetrics := "db.m_metrics"
	dbs := []DatabaseSpec{{Name: "db",
		Tables: []TableSpec{
			{Name: "m",
				Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
				Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
					Samples: &TimeSeriesTarget{Target: &tgtData},
					Tags:    &TimeSeriesTarget{Target: &tgtTags},
					Metrics: &TimeSeriesTarget{Target: &tgtMetrics},
				}},
			},
			{Name: "m_data", Engine: &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}}},
			{Name: "m_tags", Engine: &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}}},
			{Name: "m_metrics", Engine: &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}}},
		},
	}}
	errs := Validate(dbs, SkipSet{}, ClusterSet{})
	for _, e := range errs {
		assert.NotEqual(t, DepTimeSeriesTarget, e.Kind, "all targets exist; no errors expected: %s", e.Reason)
	}
}

func TestValidate_Buffer_DestMustExist(t *testing.T) {
	dbs := []DatabaseSpec{{Name: "db",
		Tables: []TableSpec{{Name: "buf",
			Columns: []ColumnSpec{{Name: "id", Type: "UUID"}},
			Engine: &EngineSpec{Kind: "buffer", Decoded: EngineBuffer{
				Database: "", Table: "missing_dest",
				NumLayers: 1, MinTime: 1, MaxTime: 10,
				MinRows: 1, MaxRows: 10, MinBytes: 1, MaxBytes: 10,
			}},
		}},
	}}
	errs := Validate(dbs, SkipSet{}, ClusterSet{})
	var ferr *ValidationError
	for i := range errs {
		if errs[i].Kind == DepBufferDestination {
			ferr = &errs[i]
		}
	}
	require.NotNil(t, ferr, "Buffer should require its destination table")
	assert.Equal(t, "db.missing_dest", ferr.Missing.String())
}

func TestValidate_Buffer_DestExists_OK(t *testing.T) {
	dbs := []DatabaseSpec{{Name: "db",
		Tables: []TableSpec{
			{Name: "buf",
				Columns: []ColumnSpec{{Name: "id", Type: "UUID"}},
				Engine: &EngineSpec{Kind: "buffer", Decoded: EngineBuffer{
					Database: "", Table: "dest",
					NumLayers: 1, MinTime: 1, MaxTime: 10,
					MinRows: 1, MaxRows: 10, MinBytes: 1, MaxBytes: 10,
				}},
			},
			{Name: "dest",
				Engine: &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
			},
		},
	}}
	errs := Validate(dbs, SkipSet{}, ClusterSet{})
	for _, e := range errs {
		assert.NotEqual(t, DepBufferDestination, e.Kind, "dest exists; no error: %s", e.Reason)
	}
}
