package main

import (
	"bytes"
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
	renderChangeSet(&buf, cs)

	want := `database "posthog"
  + table new_table
  - table old_table
  ~ table events
      + column event String
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
	renderChangeSet(&buf, cs)
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
					{Name: "mv_rebuild", Recreate: true},
					{Name: "mv_query_update", QueryChange: &hclload.StringChange{Old: ptrStr("SELECT 1"), New: ptrStr("SELECT 2")}},
				},
			},
		},
	}

	var buf bytes.Buffer
	renderChangeSet(&buf, cs)

	want := `database "posthog"
  + materialized_view mv_events -> events_agg
  - materialized_view mv_old
  ~ materialized_view mv_rebuild
      ! requires recreation (to_table or columns changed)
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
					{Name: "recreated", Recreate: true},
				},
			},
		},
	}

	var buf bytes.Buffer
	renderChangeSet(&buf, cs)

	want := `database "posthog"
  + view new_v
  - view old_v
  ~ view modified_q
      ~ query changed
  ~ view recreated
      ! requires recreation (column_aliases / sql_security / definer / cluster changed)
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

func TestRenderChangeSet_Dictionaries(t *testing.T) {
	cs := hclload.ChangeSet{Databases: []hclload.DatabaseChange{{
		Database:          "posthog",
		AddDictionaries:   []hclload.DictionarySpec{{Name: "new_dict"}},
		DropDictionaries:  []string{"old_dict"},
		AlterDictionaries: []hclload.DictionaryDiff{{Name: "rebuild_dict", Changed: []string{"layout", "source"}}},
	}}}

	var buf bytes.Buffer
	renderChangeSet(&buf, cs)

	want := `database "posthog"
  + dictionary new_dict
  - dictionary old_dict
  ~ dictionary rebuild_dict (changed: layout, source)
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
	renderChangeSet(&buf, cs)

	want := `database "posthog"
  + raw dictionary new_raw
  - raw view old_raw
  ~ raw dictionary safe_raw (recreate)
  ~ raw table risky_raw (recreate) ! UNSAFE: destroys data
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
	renderChangeSet(&buf, cs)

	want := `named_collections
  + named_collection new_nc
  - named_collection old_nc
  ~ named_collection prod_nc
      ~ param kafka_topic_list (set)
      ~ param kafka_new_setting (set)
      - param kafka_unused
`
	require.Equal(t, want, buf.String())
}
