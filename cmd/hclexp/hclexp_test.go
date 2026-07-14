package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/posthog/chschema/config"
	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/require"
)

func TestUsage(t *testing.T) {
	var buf bytes.Buffer
	usage(&buf)
	out := buf.String()
	for _, cmd := range []string{"introspect", "diff", "validate", "drift", "load", "version"} {
		require.Contains(t, out, cmd)
	}
}

func TestApplyTLSFlags(t *testing.T) {
	t.Run("both off leaves cfg untouched", func(t *testing.T) {
		cfg := config.ClickHouseConfig{}
		require.NoError(t, applyTLSFlags(&cfg, false, false))
		require.False(t, cfg.Secure)
		require.False(t, cfg.TLSSkipVerify)
	})

	t.Run("secure on, skip-verify off", func(t *testing.T) {
		cfg := config.ClickHouseConfig{}
		require.NoError(t, applyTLSFlags(&cfg, true, false))
		require.True(t, cfg.Secure)
		require.False(t, cfg.TLSSkipVerify)
	})

	t.Run("secure on, skip-verify on", func(t *testing.T) {
		cfg := config.ClickHouseConfig{}
		require.NoError(t, applyTLSFlags(&cfg, true, true))
		require.True(t, cfg.Secure)
		require.True(t, cfg.TLSSkipVerify)
	})

	t.Run("skip-verify without secure is rejected", func(t *testing.T) {
		cfg := config.ClickHouseConfig{}
		err := applyTLSFlags(&cfg, false, true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "-tls-skip-verify requires -secure")
	})
}

func TestShortHost(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"a.b.c", "a"},
		{"chi-clickhouse-0-0.svc.cluster.local", "chi-clickhouse-0-0"},
		{"host", "host"},
		{"", ""},
		{".leading", ""},
		{"trailing.", "trailing"},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			require.Equal(t, c.want, shortHost(c.in))
		})
	}
}

func TestParseClickHouseURI(t *testing.T) {
	cfg, dbs, err := parseClickHouseURI("clickhouse://ro:secret@ch.example.com:9100/posthog,system")
	require.NoError(t, err)
	require.Equal(t, "ch.example.com", cfg.Host)
	require.Equal(t, 9100, cfg.Port)
	require.Equal(t, "ro", cfg.User)
	require.Equal(t, "secret", cfg.Password)
	require.Equal(t, []string{"posthog", "system"}, dbs)
	require.Equal(t, "posthog", cfg.Database)

	// Missing pieces fall back to the environment-driven defaults.
	def := config.GetDefaultConfig()
	cfg, dbs, err = parseClickHouseURI("clickhouse://localhost/mydb")
	require.NoError(t, err)
	require.Equal(t, "localhost", cfg.Host)
	require.Equal(t, def.Port, cfg.Port)
	require.Equal(t, def.User, cfg.User)
	require.Equal(t, []string{"mydb"}, dbs)

	_, _, err = parseClickHouseURI("clickhouse://localhost:notaport/db")
	require.Error(t, err)
}

func TestParseClickHouseURI_TLSQueryParams(t *testing.T) {
	t.Run("plaintext stays plaintext", func(t *testing.T) {
		cfg, _, err := parseClickHouseURI("clickhouse://u:p@h:9000/db")
		require.NoError(t, err)
		require.False(t, cfg.Secure)
		require.False(t, cfg.TLSSkipVerify)
	})

	t.Run("?secure=true enables TLS", func(t *testing.T) {
		cfg, _, err := parseClickHouseURI("clickhouse://u:p@h:9440/db?secure=true")
		require.NoError(t, err)
		require.True(t, cfg.Secure)
		require.False(t, cfg.TLSSkipVerify)
	})

	t.Run("?secure=true&skip-verify=true sets both", func(t *testing.T) {
		cfg, _, err := parseClickHouseURI("clickhouse://u:p@h:9440/db?secure=true&skip-verify=true")
		require.NoError(t, err)
		require.True(t, cfg.Secure)
		require.True(t, cfg.TLSSkipVerify)
	})

	t.Run("?skip-verify=true without secure is rejected", func(t *testing.T) {
		_, _, err := parseClickHouseURI("clickhouse://u:p@h:9440/db?skip-verify=true")
		require.Error(t, err)
		require.Contains(t, err.Error(), "skip-verify requires secure=true")
	})
}

func TestLoadSide_HCLFile(t *testing.T) {
	path := writeTemp(t, "schema.hcl", `
database "posthog" {
  table "events" {
    order_by = ["timestamp"]
    column "timestamp" { type = "DateTime" }
    engine "merge_tree" {}
  }
}`)

	schema, err := loadSide(path)
	require.NoError(t, err)
	require.Len(t, schema.Databases, 1)
	require.Equal(t, "posthog", schema.Databases[0].Name)
	require.Len(t, schema.Databases[0].Tables, 1)
	require.Equal(t, "events", schema.Databases[0].Tables[0].Name)
}

func TestRunDiff_RenderChangeSet(t *testing.T) {
	left := writeTemp(t, "left.hcl", `
database "posthog" {
  table "events" {
    order_by = ["timestamp"]
    column "timestamp" { type = "DateTime" }
    column "team_id"   { type = "UInt32" }
    engine "merge_tree" {}
  }
  table "old_table" {
    column "id" { type = "UUID" }
    engine "merge_tree" {}
  }
}`)
	right := writeTemp(t, "right.hcl", `
database "posthog" {
  table "events" {
    order_by = ["timestamp"]
    column "timestamp" { type = "DateTime" }
    column "team_id"   { type = "UInt64" }
    column "event"     { type = "String" }
    engine "merge_tree" {}
    settings = { index_granularity = "8192" }
  }
  table "new_table" {
    column "id" { type = "UUID" }
    engine "merge_tree" {}
  }
}`)

	leftDBs, err := loadSide(left)
	require.NoError(t, err)
	rightDBs, err := loadSide(right)
	require.NoError(t, err)

	cs := hclload.Diff(leftDBs, rightDBs)
	require.False(t, cs.IsEmpty())

	var buf bytes.Buffer
	renderComparisons(&buf, cs, leftDBs, rightDBs)

	want := `database "posthog"
  + table new_table
  - table old_table
  ~ table events
      + column event = String
      ~ column team_id: UInt32 -> UInt64
      + setting index_granularity = 8192
`
	require.Equal(t, want, buf.String())

	// Identical sides produce an empty change set.
	require.True(t, hclload.Diff(leftDBs, leftDBs).IsEmpty())
}

func TestRunDiff_SelfDiffEmpty(t *testing.T) {
	path := writeTemp(t, "schema.hcl", `
database "posthog" {
  table "events" {
    order_by     = ["timestamp", "team_id"]
    partition_by = "toYYYYMM(timestamp)"
    settings = { index_granularity = "8192" }
    column "timestamp" { type = "DateTime" }
    column "team_id"   { type = "UInt64" }
    column "event"     { type = "String" }
    index "idx_team" {
      expr        = "team_id"
      type        = "minmax"
      granularity = 4
    }
    engine "replicated_merge_tree" {
      zoo_path     = "/clickhouse/tables/{shard}/events"
      replica_name = "{replica}"
    }
  }
}`)

	left, err := loadSide(path)
	require.NoError(t, err)
	right, err := loadSide(path)
	require.NoError(t, err)

	cs := hclload.Diff(left, right)
	require.True(t, cs.IsEmpty(), "diffing a file with itself must produce no changes")

	var buf bytes.Buffer
	renderComparisons(&buf, cs, left, right)
	require.Empty(t, buf.String())
}

func TestRenderChangeSet_MaterializedViews(t *testing.T) {
	cs := hclload.ChangeSet{
		Databases: []hclload.DatabaseChange{
			{
				Database: "posthog",
				AddMaterializedViews: []hclload.MaterializedViewSpec{
					{Name: "mv_events", ToTable: "events_agg"},
				},
				DropMaterializedViews: []string{"mv_old"},
				AlterMaterializedViews: []hclload.MaterializedViewDiff{
					{Name: "mv_rebuild", Recreate: true, ColumnsChanged: true,
						ToTableChange: &hclload.StringChange{Old: ptrStr("t_old"), New: ptrStr("t_new")}},
					{Name: "mv_query_update", QueryChange: &hclload.StringChange{Old: ptrStr("SELECT 1"), New: ptrStr("SELECT 2")}},
				},
			},
		},
	}

	var buf bytes.Buffer
	renderComparisons(&buf, cs, nil, nil)

	want := `database "posthog"
  + materialized_view mv_events
  - materialized_view mv_old
  ~ materialized_view mv_rebuild (UNSAFE: materialized view to_table or column list change requires recreating the view)
      ~ to_table: t_old -> t_new
      ~ columns changed
  ~ materialized_view mv_query_update
      ~ query changed
`
	require.Equal(t, want, buf.String())
}

func TestRenderChangeSet_Views(t *testing.T) {
	cs := hclload.ChangeSet{
		Databases: []hclload.DatabaseChange{
			{
				Database:  "posthog",
				AddViews:  []hclload.ViewSpec{{Name: "new_v", Query: "SELECT 1"}},
				DropViews: []string{"old_v"},
				AlterViews: []hclload.ViewDiff{
					{Name: "modified_q", QueryChange: &hclload.StringChange{Old: ptrStr("SELECT 1"), New: ptrStr("SELECT 2")}},
					{Name: "recreated", Recreate: true, RecreateChanged: []string{"sql_security", "cluster"}},
				},
			},
		},
	}

	var buf bytes.Buffer
	renderComparisons(&buf, cs, nil, nil)

	want := `database "posthog"
  + view new_v
  - view old_v
  ~ view modified_q
      ~ query changed
  ~ view recreated (UNSAFE: view column_aliases / sql_security / definer / cluster change requires recreating the view)
      ~ sql_security changed
      ~ cluster changed
`
	require.Equal(t, want, buf.String())
}

// TestWriteIntrospected_DirectoryLayout locks the shape the consuming
// chart in posthog/charts relies on: when -out is a directory, hclexp
// writes one <db>.hcl per database under it. The aws-cli sidecar then
// uploads that tree to S3.
func TestWriteIntrospected_DirectoryLayout(t *testing.T) {
	dir := t.TempDir()
	schema := &hclload.Schema{
		Databases: []hclload.DatabaseSpec{
			{Name: "posthog"},
			{Name: "system"},
		},
	}
	require.NoError(t, writeIntrospected(dir, schema))

	for _, name := range []string{"posthog", "system"} {
		p := filepath.Join(dir, name+".hcl")
		info, err := os.Stat(p)
		require.NoError(t, err, "expected %s", p)
		require.False(t, info.IsDir())
	}
}

func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func ptrStr(s string) *string { return &s }

// renderComparisons is the text path `hclexp diff` takes: a ChangeSet is
// projected onto per-object comparisons (with the generated DDL attached, so
// unsafe flags come from sqlgen) and rendered. Tests that build a ChangeSet by
// hand pass nil schemas — engine enrichment simply stays empty.
func renderComparisons(w io.Writer, cs hclload.ChangeSet, left, right *hclload.Schema) {
	gen := hclload.GenerateSQL(cs)
	hclload.RenderObjectComparisons(w, hclload.BuildObjectComparisons(cs, gen, left, right))
}

func TestRenderChangeSet_Dictionaries(t *testing.T) {
	rebuilt := hclload.DictionarySpec{
		Name:       "rebuild_dict",
		PrimaryKey: []string{"k"},
		Attributes: []hclload.DictionaryAttribute{{Name: "k", Type: "UInt64"}},
		Source:     &hclload.DictionarySourceSpec{Kind: "null", Decoded: hclload.SourceNull{}},
		Layout:     &hclload.DictionaryLayoutSpec{Kind: "hashed", Decoded: hclload.LayoutHashed{}},
	}
	cs := hclload.ChangeSet{Databases: []hclload.DatabaseChange{{
		Database:         "posthog",
		AddDictionaries:  []hclload.DictionarySpec{{Name: "new_dict"}},
		DropDictionaries: []string{"old_dict"},
		AlterDictionaries: []hclload.DictionaryDiff{{
			Name:    "rebuild_dict",
			Changed: []string{"layout", "source"},
			New:     rebuilt,
		}},
	}}}

	var buf bytes.Buffer
	renderComparisons(&buf, cs, nil, nil)

	// No UNSAFE annotation: a dictionary is reconciled by CREATE OR REPLACE,
	// which loses nothing (it reloads from its source). Before #140 this line
	// carried "(UNSAFE: dictionary change requires CREATE OR REPLACE ...)" —
	// and no such statement was ever emitted.
	want := `database "posthog"
  + dictionary new_dict
  - dictionary old_dict
  ~ dictionary rebuild_dict
      ~ layout changed
      ~ source changed
`
	require.Equal(t, want, buf.String())
}

func TestRenderChangeSet_Raws(t *testing.T) {
	cs := hclload.ChangeSet{Databases: []hclload.DatabaseChange{{
		Database: "posthog",
		AddRaws:  []hclload.RawSpec{{Kind: "dictionary", Name: "new_raw"}},
		DropRaws: []hclload.RawSpec{{Kind: "view", Name: "old_raw"}},
		AlterRaws: []hclload.RawChange{
			{Kind: "dictionary", Name: "safe_raw"},
			{Kind: "table", Name: "risky_raw"},
		},
	}}}

	var buf bytes.Buffer
	renderComparisons(&buf, cs, nil, nil)

	want := `database "posthog"
  + raw dictionary new_raw
  - raw view old_raw
  ~ raw dictionary safe_raw
      ~ sql changed
  ~ raw table risky_raw (UNSAFE: raw table change requires DROP + CREATE, which destroys the table's data)
      ~ sql changed
`
	require.Equal(t, want, buf.String())
}

func TestRenderChangeSet_NamedCollections(t *testing.T) {
	cs := hclload.ChangeSet{
		NamedCollections: []hclload.NamedCollectionChange{
			{Name: "new_nc", Add: &hclload.NamedCollectionSpec{Name: "new_nc"}},
			{Name: "old_nc", Drop: true},
			{Name: "prod_nc", SetParams: []hclload.NamedCollectionParam{
				{Key: "kafka_topic_list", Value: "new_topic"},
				{Key: "kafka_new_setting", Value: "added"},
			}, DeleteParams: []string{"kafka_unused"}},
		},
	}

	var buf bytes.Buffer
	renderComparisons(&buf, cs, nil, nil)

	want := `named_collections
  + named_collection new_nc
  - named_collection old_nc
  ~ named_collection prod_nc
      ~ param kafka_topic_list = new_topic
      ~ param kafka_new_setting = added
      - param kafka_unused
`
	require.Equal(t, want, buf.String())
}

func TestClusterFlag_Set(t *testing.T) {
	var c clusterFlag
	require.NoError(t, c.Set("aux=dir1:dir2"))
	require.NoError(t, c.Set("events_recent=@absent"))
	require.Equal(t, []clusterEntry{
		{name: "aux", stack: "dir1:dir2"},
		{name: "events_recent", stack: "@absent"},
	}, c.entries)
	require.Equal(t, "aux=dir1:dir2,events_recent=@absent", c.String())

	require.Error(t, c.Set("nostack"), "missing '=' is rejected")
	require.Error(t, c.Set("=stack"), "empty name is rejected")
}

// buildClusterSet loads a mapped stack into a resolvable ClusterSet and marks
// @absent clusters; a proxy resolves against the mapped cluster, is satisfied
// for an absent cluster, and errors for a table missing from the mapped stack.
func TestBuildClusterSet_ResolvesAndAbsent(t *testing.T) {
	auxPath := writeTemp(t, "aux.hcl", `
database "posthog" {
  table "sharded_web_stats_preaggregated" {
    order_by = ["day"]
    column "day" { type = "Date" }
    engine "merge_tree" {}
  }
}`)

	cs, err := buildClusterSet([]clusterEntry{
		{name: "aux", stack: filepath.Dir(auxPath)},
		{name: "batch_exports", stack: absentStack},
	})
	require.NoError(t, err)

	proxy := func(cluster, remoteTable string) []hclload.DatabaseSpec {
		return []hclload.DatabaseSpec{{
			Name: "posthog",
			Tables: []hclload.TableSpec{{
				Name: "proxy",
				Engine: &hclload.EngineSpec{
					Kind: "distributed",
					Decoded: hclload.EngineDistributed{
						ClusterName:    cluster,
						RemoteDatabase: "posthog",
						RemoteTable:    remoteTable,
					},
				},
			}},
		}}
	}

	require.Empty(t, hclload.Validate(proxy("aux", "sharded_web_stats_preaggregated"), hclload.ParseSkipSet(""), cs),
		"remote in the mapped aux stack resolves")
	require.Empty(t, hclload.Validate(proxy("batch_exports", "anything"), hclload.ParseSkipSet(""), cs),
		"@absent cluster is satisfied")
	require.Len(t, hclload.Validate(proxy("aux", "missing"), hclload.ParseSkipSet(""), cs), 1,
		"remote missing from the mapped aux stack errors")
}

func TestBuildClusterSet_LoadError(t *testing.T) {
	_, err := buildClusterSet([]clusterEntry{{name: "aux", stack: "/no/such/layer/dir"}})
	require.Error(t, err)
}

// An @alias=BASE mapping resolves a proxy on the alias cluster against BASE's
// composition, without loading a second stack.
func TestBuildClusterSet_Alias(t *testing.T) {
	auxPath := writeTemp(t, "aux.hcl", `
database "posthog" {
  table "sharded_web_stats_preaggregated" {
    order_by = ["day"]
    column "day" { type = "Date" }
    engine "merge_tree" {}
  }
}`)

	cs, err := buildClusterSet([]clusterEntry{
		{name: "aux", stack: filepath.Dir(auxPath)},
		{name: "aux_writable", stack: aliasPrefix + "aux"},
	})
	require.NoError(t, err)

	proxy := []hclload.DatabaseSpec{{
		Name: "posthog",
		Tables: []hclload.TableSpec{{
			Name: "proxy",
			Engine: &hclload.EngineSpec{
				Kind: "distributed",
				Decoded: hclload.EngineDistributed{
					ClusterName:    "aux_writable",
					RemoteDatabase: "posthog",
					RemoteTable:    "sharded_web_stats_preaggregated",
				},
			},
		}},
	}}
	require.Empty(t, hclload.Validate(proxy, hclload.ParseSkipSet(""), cs),
		"proxy on the alias cluster resolves against the base composition")
}

func TestBuildClusterSet_AliasMissingBaseName(t *testing.T) {
	_, err := buildClusterSet([]clusterEntry{{name: "aux_writable", stack: aliasPrefix}})
	require.Error(t, err)
}

// parseManifestClusters decodes cluster blocks (roles + aliases) and rejects
// duplicates / empty role lists.
func TestParseManifestClusters(t *testing.T) {
	path := writeTemp(t, "roles.hcl", `
role "data" {
  env "prod-us" { layers = ["layers/base"] }
}
role "ingestion-events" {
  env "prod-us" { layers = ["layers/ingestion"] }
}

cluster "posthog" {
  roles   = ["data", "ingestion-events"]
  aliases = ["posthog_writable", "posthog_single_shard"]
}`)
	clusters, err := parseManifestClusters(path)
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	require.Equal(t, "posthog", clusters[0].Name)
	require.Equal(t, []string{"data", "ingestion-events"}, clusters[0].Roles)
	require.Equal(t, []string{"posthog_writable", "posthog_single_shard"}, clusters[0].Aliases)

	dup := writeTemp(t, "dup.hcl", `
cluster "posthog" {
  roles = ["data"]
}
cluster "posthog" {
  roles = ["aux"]
}`)
	_, err = parseManifestClusters(dup)
	require.Error(t, err)

	empty := writeTemp(t, "empty.hcl", `
cluster "posthog" {
  roles = []
}`)
	_, err = parseManifestClusters(empty)
	require.Error(t, err)

	// A cluster referencing a role with no role block is rejected.
	unknown := writeTemp(t, "unknown.hcl", `
role "data" {
  env "prod-us" { layers = ["layers/data"] }
}
cluster "posthog" {
  roles = ["data", "ghost"]
}`)
	_, err = parseManifestClusters(unknown)
	require.ErrorContains(t, err, "ghost")
}

func TestParseManifestClusters_Absent(t *testing.T) {
	ok := writeTemp(t, "ok.hcl", `
cluster "events_recent" {
  absent  = true
  aliases = ["events_recent_writable"]
}`)
	clusters, err := parseManifestClusters(ok)
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	require.True(t, clusters[0].Absent)
	require.Empty(t, clusters[0].Roles)

	both := writeTemp(t, "both.hcl", `
role "data" {
  env "prod-us" { layers = ["layers/data"] }
}
cluster "x" {
  roles  = ["data"]
  absent = true
}`)
	_, err = parseManifestClusters(both)
	require.ErrorContains(t, err, "mutually exclusive")

	neither := writeTemp(t, "neither.hcl", `
cluster "x" {
}`)
	_, err = parseManifestClusters(neither)
	require.ErrorContains(t, err, "roles or absent")
}

// A manifest cluster declared absent = true resolves proxies (and its aliases)
// into it as satisfied, with no composing role.
func TestBuildManifestClusters_ExplicitAbsent(t *testing.T) {
	root := t.TempDir()
	writeLayer(t, root, "layers/data/data.hcl", `
database "posthog" {
  table "t" {
    order_by = ["id"]
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
  }
}`)
	manifest := writeTemp(t, "roles.hcl", `
role "data" {
  env "prod-us" { layers = ["layers/data"] }
}
cluster "events_recent" {
  absent  = true
  aliases = ["events_recent_writable"]
}`)
	cs := hclload.NewClusterSet()
	require.NoError(t, buildManifestClusters(&cs, manifest, "prod-us", root))

	proxy := func(cluster string) []hclload.DatabaseSpec {
		return []hclload.DatabaseSpec{{
			Name: "posthog",
			Tables: []hclload.TableSpec{{
				Name: "p",
				Engine: &hclload.EngineSpec{Kind: "distributed", Decoded: hclload.EngineDistributed{
					ClusterName: cluster, RemoteDatabase: "posthog", RemoteTable: "sharded_x",
				}},
			}},
		}}
	}
	require.Empty(t, hclload.Validate(proxy("events_recent"), hclload.ParseSkipSet(""), cs),
		"proxy into an explicitly-absent manifest cluster is satisfied")
	require.Empty(t, hclload.Validate(proxy("events_recent_writable"), hclload.ParseSkipSet(""), cs),
		"an alias of an absent cluster is also satisfied")
}

// A cluster's schema is the UNION of its member roles' compositions: a proxy
// on cluster "posthog" resolves against a table declared in EITHER role.
func TestBuildManifestClusters_UnionOfRoles(t *testing.T) {
	root := t.TempDir()
	// role "data" declares sharded_events; role "ingestion" declares kafka_events.
	writeLayer(t, root, "layers/data/data.hcl", `
database "posthog" {
  table "sharded_events" {
    order_by = ["id"]
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
  }
}`)
	writeLayer(t, root, "layers/ingestion/ing.hcl", `
database "posthog" {
  table "kafka_events" {
    order_by = ["id"]
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
  }
}`)
	manifest := writeTemp(t, "roles.hcl", `
role "data" {
  env "prod-us" { layers = ["layers/data"] }
}
role "ingestion" {
  env "prod-us" { layers = ["layers/ingestion"] }
}

cluster "posthog" {
  roles   = ["data", "ingestion"]
  aliases = ["posthog_writable"]
}`)
	cs := hclload.NewClusterSet()
	require.NoError(t, buildManifestClusters(&cs, manifest, "prod-us", root))

	proxy := func(cluster, remote string) []hclload.DatabaseSpec {
		return []hclload.DatabaseSpec{{
			Name: "posthog",
			Tables: []hclload.TableSpec{{
				Name: "p",
				Engine: &hclload.EngineSpec{Kind: "distributed", Decoded: hclload.EngineDistributed{
					ClusterName: cluster, RemoteDatabase: "posthog", RemoteTable: remote,
				}},
			}},
		}}
	}
	// Both a data-role table and an ingestion-role table resolve on "posthog".
	require.Empty(t, hclload.Validate(proxy("posthog", "sharded_events"), hclload.ParseSkipSet(""), cs),
		"remote from the data role resolves against the unioned posthog cluster")
	require.Empty(t, hclload.Validate(proxy("posthog", "kafka_events"), hclload.ParseSkipSet(""), cs),
		"remote from the ingestion role resolves against the unioned posthog cluster")
	// The alias resolves via the same union.
	require.Empty(t, hclload.Validate(proxy("posthog_writable", "kafka_events"), hclload.ParseSkipSet(""), cs),
		"alias resolves via the unioned base cluster")
	// A table on neither role errors.
	require.Len(t, hclload.Validate(proxy("posthog", "nonexistent"), hclload.ParseSkipSet(""), cs), 1)
}

// End-to-end through the binary path: -manifest resolves a cross-cluster proxy
// with no -cluster flags; without it the proxy errors.
func TestValidate_ManifestClusters_EndToEnd(t *testing.T) {
	root := t.TempDir()
	writeLayer(t, root, "layers/aux/aux.hcl", `
database "posthog" {
  table "sharded_web_stats" {
    order_by = ["day"]
    column "day" { type = "Date" }
    engine "merge_tree" {}
  }
}`)
	manifest := writeTemp(t, "roles.hcl", `
role "aux" {
  env "prod-us" { layers = ["layers/aux"] }
}
cluster "aux" {
  roles   = ["aux"]
  aliases = ["aux_writable"]
}`)

	cs := hclload.NewClusterSet()
	require.NoError(t, buildManifestClusters(&cs, manifest, "prod-us", root))

	proxy := func(cluster string) []hclload.DatabaseSpec {
		return []hclload.DatabaseSpec{{
			Name: "posthog",
			Tables: []hclload.TableSpec{{
				Name: "web_stats",
				Engine: &hclload.EngineSpec{Kind: "distributed", Decoded: hclload.EngineDistributed{
					ClusterName: cluster, RemoteDatabase: "posthog", RemoteTable: "sharded_web_stats",
				}},
			}},
		}}
	}
	require.Empty(t, hclload.Validate(proxy("aux"), hclload.ParseSkipSet(""), cs))
	require.Empty(t, hclload.Validate(proxy("aux_writable"), hclload.ParseSkipSet(""), cs),
		"manifest alias resolves via its base cluster")
}

// validateManifest validates every role's composition, resolving cross-cluster
// references against the manifest-derived cluster set. Clean roles pass; a role
// with a broken reference reports it, while its cross-cluster proxy resolves.
func TestValidateManifest_AllRoles(t *testing.T) {
	root := t.TempDir()
	// aux role: a clean storage table.
	writeLayer(t, root, "layers/aux/aux.hcl", `
database "posthog" {
  table "sharded_web_stats" {
    order_by = ["day"]
    column "day" { type = "Date" }
    engine "merge_tree" {}
  }
}`)
	// data role: a Distributed proxy into aux (resolves cross-cluster) plus a
	// view referencing a table that exists nowhere (an error).
	writeLayer(t, root, "layers/data/data.hcl", `
database "posthog" {
  table "web_stats" {
    engine "distributed" {
      cluster_name    = "aux"
      remote_database = "posthog"
      remote_table    = "sharded_web_stats"
    }
    column "day" { type = "Date" }
  }
  view "broken" {
    query = "SELECT day FROM posthog.nonexistent"
  }
}`)
	manifest := writeTemp(t, "roles.hcl", `
role "data" {
  env "prod-us" { layers = ["layers/data"] }
}
role "aux" {
  env "prod-us" { layers = ["layers/aux"] }
}
cluster "aux"     { roles = ["aux"] }
cluster "posthog" { roles = ["data"] }`)

	results, err := validateManifest(manifest, "prod-us", root, "", hclload.ParseSkipSet(""), hclload.ValidateOptions{}, nil)
	require.NoError(t, err)
	require.Len(t, results, 2, "every deployed role is validated")

	byRole := map[string][]hclload.ValidationError{}
	for _, r := range results {
		byRole[r.Role] = r.Errs
	}
	require.Empty(t, byRole["aux"], "aux role is clean")
	require.Len(t, byRole["data"], 1, "data role has exactly the broken view (its proxy resolved cross-cluster)")
	require.Equal(t, "broken", byRole["data"][0].Object.Name)
	require.Equal(t, "nonexistent", byRole["data"][0].Missing.Name)
}

// A fully consistent manifest yields no errors on any role.
func TestValidateManifest_AllClean(t *testing.T) {
	root := t.TempDir()
	writeLayer(t, root, "layers/aux/aux.hcl", `
database "posthog" {
  table "sharded_web_stats" {
    order_by = ["day"]
    column "day" { type = "Date" }
    engine "merge_tree" {}
  }
}`)
	writeLayer(t, root, "layers/data/data.hcl", `
database "posthog" {
  table "web_stats" {
    engine "distributed" {
      cluster_name    = "aux"
      remote_database = "posthog"
      remote_table    = "sharded_web_stats"
    }
    column "day" { type = "Date" }
  }
}`)
	manifest := writeTemp(t, "roles.hcl", `
role "data" {
  env "prod-us" { layers = ["layers/data"] }
}
role "aux" {
  env "prod-us" { layers = ["layers/aux"] }
}
cluster "aux" { roles = ["aux"] }`)

	results, err := validateManifest(manifest, "prod-us", root, "", hclload.ParseSkipSet(""), hclload.ValidateOptions{}, nil)
	require.NoError(t, err)
	require.Len(t, results, 2)
	for _, r := range results {
		require.Empty(t, r.Errs, "role %s should be clean", r.Role)
	}
}

// -cluster NAME=@absent narrowly satisfies references into that one missing
// cluster: a proxy into it passes, while every OTHER problem (a remote missing
// from a mapped cluster, a broken view) is still reported. @absent is not a
// blanket skip.
func TestValidateManifest_AbsentSkipsOnlyMissingCluster(t *testing.T) {
	root := t.TempDir()
	// aux role: declares sharded_web_stats but NOT sharded_gone.
	writeLayer(t, root, "layers/aux/aux.hcl", `
database "posthog" {
  table "sharded_web_stats" {
    order_by = ["day"]
    column "day" { type = "Date" }
    engine "merge_tree" {}
  }
}`)
	// data role: four objects — one resolves via aux, one via the @absent
	// cluster, one is a real cross-cluster miss, one is a broken view.
	writeLayer(t, root, "layers/data/data.hcl", `
database "posthog" {
  table "web_stats" {
    engine "distributed" {
      cluster_name    = "aux"
      remote_database = "posthog"
      remote_table    = "sharded_web_stats"
    }
    column "day" { type = "Date" }
  }
  table "events_recent" {
    engine "distributed" {
      cluster_name    = "events_recent"
      remote_database = "posthog"
      remote_table    = "sharded_events_recent"
    }
    column "id" { type = "UInt64" }
  }
  table "web_stats_gone" {
    engine "distributed" {
      cluster_name    = "aux"
      remote_database = "posthog"
      remote_table    = "sharded_gone"
    }
    column "id" { type = "UInt64" }
  }
  view "broken" {
    query = "SELECT day FROM posthog.nonexistent"
  }
}`)
	manifest := writeTemp(t, "roles.hcl", `
role "data" {
  env "prod-us" { layers = ["layers/data"] }
}
role "aux" {
  env "prod-us" { layers = ["layers/aux"] }
}
cluster "aux"     { roles = ["aux"] }
cluster "posthog" { roles = ["data"] }`)

	// events_recent has no composing role; declare it @absent via a flag.
	flags := []clusterEntry{{name: "events_recent", stack: absentStack}}
	results, err := validateManifest(manifest, "prod-us", root, "", hclload.ParseSkipSet(""), hclload.ValidateOptions{}, flags)
	require.NoError(t, err)

	byRole := map[string][]hclload.ValidationError{}
	for _, r := range results {
		byRole[r.Role] = r.Errs
	}
	require.Empty(t, byRole["aux"], "aux role is clean")

	missing := map[string]bool{}
	for _, e := range byRole["data"] {
		missing[e.Missing.Name] = true
	}
	// The two real problems are found...
	require.True(t, missing["sharded_gone"], "a remote missing from the mapped aux cluster is still reported")
	require.True(t, missing["nonexistent"], "the broken view source is still reported")
	// ...but the @absent cluster's proxy (and the valid aux proxy) are not.
	require.False(t, missing["sharded_events_recent"], "the @absent cluster's proxy is satisfied, not flagged")
	require.False(t, missing["sharded_web_stats"], "the valid aux proxy resolves")
	require.Len(t, byRole["data"], 2, "exactly the two real problems, nothing more")
}

// #127: a cluster whose member roles have no composition in the selected env
// should resolve @absent (proxies satisfied), not register empty-but-mapped
// (proxies falsely erroring "not declared in that cluster's schema").
func TestValidateManifest_UncomposedClusterIsAbsent(t *testing.T) {
	root := t.TempDir()
	// data role composes the posthog cluster, but only for env "local".
	writeLayer(t, root, "layers/data/data.hcl", `
database "posthog" {
  table "sharded_events" {
    order_by = ["id"]
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
  }
}`)
	// ingestion role is deployed in prod-us and proxies into the posthog cluster.
	writeLayer(t, root, "layers/ingestion/ing.hcl", `
database "posthog" {
  table "events" {
    engine "distributed" {
      cluster_name    = "posthog"
      remote_database = "posthog"
      remote_table    = "sharded_events"
    }
    column "id" { type = "UInt64" }
  }
}`)
	manifest := writeTemp(t, "roles.hcl", `
role "data" {
  env "local" { layers = ["layers/data"] }
}
role "ingestion" {
  env "prod-us" { layers = ["layers/ingestion"] }
}
cluster "posthog" { roles = ["data"] }`)

	// -env prod-us: data has no prod-us composition, so posthog is uncomposed.
	results, err := validateManifest(manifest, "prod-us", root, "", hclload.ParseSkipSet(""), hclload.ValidateOptions{}, nil)
	require.NoError(t, err)
	byRole := map[string][]hclload.ValidationError{}
	for _, r := range results {
		byRole[r.Role] = r.Errs
	}
	require.Empty(t, byRole["ingestion"],
		"proxy into an uncomposed cluster should be satisfied (@absent), not errored")
}

// The same cluster, in an env where its member role IS composed, validates the
// proxy against its real schema.
func TestValidateManifest_ComposedClusterValidates(t *testing.T) {
	root := t.TempDir()
	writeLayer(t, root, "layers/data/data.hcl", `
database "posthog" {
  table "sharded_events" {
    order_by = ["id"]
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
  }
  table "events" {
    engine "distributed" {
      cluster_name    = "posthog"
      remote_database = "posthog"
      remote_table    = "sharded_events"
    }
    column "id" { type = "UInt64" }
  }
}`)
	manifest := writeTemp(t, "roles.hcl", `
role "data" {
  env "local" { layers = ["layers/data"] }
}
cluster "posthog" { roles = ["data"] }`)

	results, err := validateManifest(manifest, "local", root, "", hclload.ParseSkipSet(""), hclload.ValidateOptions{}, nil)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Empty(t, results[0].Errs, "composed cluster: the proxy resolves against its real schema")
}

// #127 corollary: -cluster X=@absent must override a manifest-declared X.
func TestValidateManifest_AbsentFlagOverridesManifestCluster(t *testing.T) {
	root := t.TempDir()
	// posthog is composed (data deployed in prod-us) but declares only 'other',
	// so a proxy to sharded_events would error unless posthog is forced @absent.
	writeLayer(t, root, "layers/data/data.hcl", `
database "posthog" {
  table "other" {
    order_by = ["id"]
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
  }
}`)
	writeLayer(t, root, "layers/ingestion/ing.hcl", `
database "posthog" {
  table "events" {
    engine "distributed" {
      cluster_name    = "posthog"
      remote_database = "posthog"
      remote_table    = "sharded_events"
    }
    column "id" { type = "UInt64" }
  }
}`)
	manifest := writeTemp(t, "roles.hcl", `
role "data" {
  env "prod-us" { layers = ["layers/data"] }
}
role "ingestion" {
  env "prod-us" { layers = ["layers/ingestion"] }
}
cluster "posthog" { roles = ["data"] }`)

	flags := []clusterEntry{{name: "posthog", stack: absentStack}}
	results, err := validateManifest(manifest, "prod-us", root, "", hclload.ParseSkipSet(""), hclload.ValidateOptions{}, flags)
	require.NoError(t, err)
	byRole := map[string][]hclload.ValidationError{}
	for _, r := range results {
		byRole[r.Role] = r.Errs
	}
	require.Empty(t, byRole["ingestion"],
		"-cluster posthog=@absent should override the manifest-declared posthog and satisfy the proxy")
}

// writeLayer writes content to root/rel, creating parent dirs.
func writeLayer(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, rel)
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
}

// End-to-end through real HCL parsing: a Distributed proxy that declares a
// column its remote lacks is flagged, and -strict-proxy-columns flags a
// remote-only column too.
func TestValidate_ProxyColumns_EndToEnd(t *testing.T) {
	path := writeTemp(t, "node.hcl", `
database "posthog" {
  table "sharded_events" {
    order_by = ["id"]
    column "id" { type = "UInt64" }
    column "team_id" { type = "UInt32" }
    engine "merge_tree" {}
  }
  table "events" {
    engine "distributed" {
      cluster_name    = "posthog"
      remote_database = "posthog"
      remote_table    = "sharded_events"
    }
    column "id" { type = "UInt64" }
    column "proxy_only" { type = "String" }
  }
}`)
	schema, err := hclload.ParseFile(path)
	require.NoError(t, err)
	require.NoError(t, hclload.Resolve(schema))

	// Default (subset): the proxy-only column is flagged; the remote-only
	// team_id is allowed.
	errs := hclload.Validate(schema.Databases, hclload.ParseSkipSet(""), hclload.ClusterSet{})
	require.Len(t, errs, 1)
	require.Equal(t, "proxy_only", errs[0].Missing.Name)

	// Strict: the remote-only team_id is also flagged.
	strict := hclload.ValidateOpts(schema.Databases, hclload.ParseSkipSet(""), hclload.ClusterSet{},
		hclload.ValidateOptions{StrictProxyColumns: true})
	require.Len(t, strict, 2)
}

// End-to-end through real HCL parsing: a view whose source table lives in a
// mapped cluster resolves; without the mapping it errors.
func TestValidate_ViewSourceCrossCluster_EndToEnd(t *testing.T) {
	nodePath := writeTemp(t, "node.hcl", `
database "posthog" {
  view "web_stats_view" {
    query = "SELECT day FROM posthog.sharded_web_stats_preaggregated"
  }
}`)
	auxPath := filepath.Join(t.TempDir(), "aux.hcl")
	require.NoError(t, os.WriteFile(auxPath, []byte(`
database "posthog" {
  table "sharded_web_stats_preaggregated" {
    order_by = ["day"]
    column "day" { type = "Date" }
    engine "merge_tree" {}
  }
}`), 0o600))

	node, err := hclload.ParseFile(nodePath)
	require.NoError(t, err)
	require.NoError(t, hclload.Resolve(node))

	withAux, err := buildClusterSet([]clusterEntry{{name: "aux", stack: filepath.Dir(auxPath)}})
	require.NoError(t, err)
	require.Empty(t, hclload.Validate(node.Databases, hclload.ParseSkipSet(""), withAux),
		"view source resolves against the mapped aux cluster")

	require.Len(t, hclload.Validate(node.Databases, hclload.ParseSkipSet(""), hclload.ClusterSet{}), 1,
		"without the aux mapping the cross-cluster view source is unresolved")
}

// A diff whose only difference is an excluded object must be empty after
// FilterSchema — the CLI-level contract behind `-exclude`.
func TestDiffWithExcludeFilteredSchemas(t *testing.T) {
	dir := t.TempDir()
	leftPath := filepath.Join(dir, "left.hcl")
	rightPath := filepath.Join(dir, "right.hcl")
	require.NoError(t, os.WriteFile(leftPath, []byte(`
database "d" {
  table "events" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}
`), 0o644))
	require.NoError(t, os.WriteFile(rightPath, []byte(`
database "d" {
  table "events" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
  table "tmp_scratch" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}
`), 0o644))

	left, err := loadSide(leftPath)
	require.NoError(t, err)
	right, err := loadSide(rightPath)
	require.NoError(t, err)

	// Without the filter the scratch table is a real difference.
	require.False(t, hclload.Diff(left, right).IsEmpty())

	m := hclload.NewExcludeMatcherWithTypes(nil, "tmp_*")
	hclload.FilterSchema(left, m)
	hclload.FilterSchema(right, m)
	require.True(t, hclload.Diff(left, right).IsEmpty())
}
