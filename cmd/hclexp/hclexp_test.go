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

func TestLoadSide_HCLFile(t *testing.T) {
	path := writeTemp(t, "schema.hcl", `
database "posthog" {
  table "events" {
    order_by = ["timestamp"]
    column "timestamp" { type = "DateTime" }
    engine "merge_tree" {}
  }
}`)

	dbs, err := loadSide(path)
	require.NoError(t, err)
	require.Len(t, dbs, 1)
	require.Equal(t, "posthog", dbs[0].Name)
	require.Len(t, dbs[0].Tables, 1)
	require.Equal(t, "events", dbs[0].Tables[0].Name)
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

func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func ptrStr(s string) *string { return &s }
