package main

import (
	"os"
	"path/filepath"
	"testing"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decomposeTable(columns ...hclload.ColumnSpec) *hclload.Schema {
	return &hclload.Schema{Databases: []hclload.DatabaseSpec{{
		Name: "analytics",
		Tables: []hclload.TableSpec{{
			Name: "events", Columns: columns, OrderBy: []string{"id"},
			Engine: &hclload.EngineSpec{Kind: "merge_tree", Decoded: hclload.EngineMergeTree{}},
		}},
	}}}
}

func TestBuildDecomposition_AdditiveTablePatchIsAnchoredAndRoundTrips(t *testing.T) {
	base := decomposeTable(
		hclload.ColumnSpec{Name: "id", Type: "UInt64"},
		hclload.ColumnSpec{Name: "created_at", Type: "DateTime"},
	)
	target := decomposeTable(
		hclload.ColumnSpec{Name: "runtime_first", Type: "UInt8"},
		hclload.ColumnSpec{Name: "id", Type: "UInt64"},
		hclload.ColumnSpec{Name: "runtime_middle", Type: "String"},
		hclload.ColumnSpec{Name: "created_at", Type: "DateTime"},
		hclload.ColumnSpec{Name: "runtime_tail", Type: "UInt32"},
	)
	snapshots := []decomposeSnapshot{
		{Env: "prod-eu", Role: "events", Schema: base},
		{Env: "prod-us", Role: "events", Schema: target},
	}

	generated, err := buildDecomposition(snapshots, []string{"prod-eu", "prod-us"}, decomposeAssignment{
		Version: 1, BaselineEnv: "prod-eu", Objects: map[string]decomposeObjectAssignment{},
	})
	require.NoError(t, err)
	patch := string(generated.Files[envLayerPath("prod-us", "events")])
	assert.Contains(t, patch, `column "runtime_first"`)
	assert.Contains(t, patch, "first = true")
	assert.Contains(t, patch, `column "runtime_middle"`)
	assert.Contains(t, patch, `after = "id"`)
	assert.Contains(t, patch, `column "runtime_tail"`)
	assert.NotContains(t, patch[stringsIndex(t, patch, `column "runtime_tail"`):], "after =",
		"a true tail addition must remain an append")
}

func stringsIndex(t *testing.T, value, needle string) int {
	t.Helper()
	index := -1
	for i := 0; i+len(needle) <= len(value); i++ {
		if value[i:i+len(needle)] == needle {
			index = i
			break
		}
	}
	require.NotEqual(t, -1, index)
	return index
}

func TestBuildDecomposition_ExplicitSharedReorderFailsPrecisely(t *testing.T) {
	from := decomposeTable(
		hclload.ColumnSpec{Name: "id", Type: "UInt64"},
		hclload.ColumnSpec{Name: "created_at", Type: "DateTime"},
	)
	to := decomposeTable(
		hclload.ColumnSpec{Name: "created_at", Type: "DateTime"},
		hclload.ColumnSpec{Name: "id", Type: "UInt64"},
	)
	key := "events/analytics/table/events"
	_, err := buildDecomposition([]decomposeSnapshot{
		{Env: "prod-eu", Role: "events", Schema: from},
		{Env: "prod-us", Role: "events", Schema: to},
	}, []string{"prod-eu", "prod-us"}, decomposeAssignment{
		Version: 1,
		Objects: map[string]decomposeObjectAssignment{key: {Mode: "shared"}},
	})
	require.ErrorContains(t, err, key)
	require.ErrorContains(t, err, "existing-column reorder")
	require.ErrorContains(t, err, "[id created_at] -> [created_at id]")
}

func TestBuildDecomposition_PositionsAddedIndexes(t *testing.T) {
	from := decomposeTable(hclload.ColumnSpec{Name: "id", Type: "UInt64"})
	to := decomposeTable(hclload.ColumnSpec{Name: "id", Type: "UInt64"})
	from.Databases[0].Tables[0].Indexes = []hclload.IndexSpec{
		{Name: "existing_a", Expr: "id", Type: "minmax"},
		{Name: "existing_b", Expr: "id", Type: "minmax"},
	}
	to.Databases[0].Tables[0].Indexes = []hclload.IndexSpec{
		{Name: "new_first", Expr: "id", Type: "minmax"},
		{Name: "existing_a", Expr: "id", Type: "minmax"},
		{Name: "new_middle", Expr: "id", Type: "minmax"},
		{Name: "existing_b", Expr: "id", Type: "minmax"},
	}
	generated, err := buildDecomposition([]decomposeSnapshot{
		{Env: "prod-eu", Role: "ops", Schema: from},
		{Env: "prod-us", Role: "ops", Schema: to},
	}, []string{"prod-eu", "prod-us"}, decomposeAssignment{Version: 1, Objects: map[string]decomposeObjectAssignment{}})
	require.NoError(t, err)
	patch := string(generated.Files[envLayerPath("prod-us", "ops")])
	assert.Contains(t, patch, `index "new_first"`)
	assert.Contains(t, patch, "first = true")
	assert.Contains(t, patch, `index "new_middle"`)
	assert.Contains(t, patch, `after = "existing_a"`)
}

func TestBuildDecomposition_AssignmentExcludeIsPartOfRoundTripScope(t *testing.T) {
	schema := decomposeTable(hclload.ColumnSpec{Name: "id", Type: "UInt64"})
	generated, err := buildDecomposition([]decomposeSnapshot{
		{Env: "prod", Role: "events", Schema: schema},
	}, []string{"prod"}, decomposeAssignment{
		Version: 1,
		Objects: map[string]decomposeObjectAssignment{
			"events/analytics/table/events": {Mode: "exclude"},
		},
	})
	require.NoError(t, err)
	assert.NotContains(t, generated.Files, sharedLayerPath("events"))
	assert.Equal(t, "", string(generated.Files["goldens/prod/events.hcl"]))
}

func TestBuildDecomposition_IncludesClusterScopedNamedCollections(t *testing.T) {
	schema := &hclload.Schema{NamedCollections: []hclload.NamedCollectionSpec{{
		Name: "warehouse", Params: []hclload.NamedCollectionParam{{Key: "host", Value: "db.internal"}},
	}}}
	generated, err := buildDecomposition([]decomposeSnapshot{
		{Env: "prod-eu", Role: "ops", Schema: schema},
		{Env: "prod-us", Role: "ops", Schema: schema},
	}, []string{"prod-eu", "prod-us"}, decomposeAssignment{Version: 1, Objects: map[string]decomposeObjectAssignment{}})
	require.NoError(t, err)
	shared := string(generated.Files[sharedLayerPath("ops")])
	assert.Contains(t, shared, `named_collection "warehouse"`)
	assert.Contains(t, shared, `param "host"`)
}

func TestWriteDecomposition_IsIdempotentAndOnlyRemovesTrackedFiles(t *testing.T) {
	out := t.TempDir()
	userFile := filepath.Join(out, "keep.txt")
	require.NoError(t, os.WriteFile(userFile, []byte("mine"), 0o644))
	require.NoError(t, writeDecomposition(out, map[string][]byte{
		"layers/shared/events/tables.hcl": []byte("one\n"),
		"manifest.hcl":                    []byte("manifest one\n"),
	}))
	require.NoError(t, writeDecomposition(out, map[string][]byte{
		"manifest.hcl": []byte("manifest two\n"),
	}))
	_, err := os.Stat(filepath.Join(out, "layers/shared/events/tables.hcl"))
	assert.ErrorIs(t, err, os.ErrNotExist)
	body, err := os.ReadFile(userFile)
	require.NoError(t, err)
	assert.Equal(t, "mine", string(body))
	body, err = os.ReadFile(filepath.Join(out, "manifest.hcl"))
	require.NoError(t, err)
	assert.Equal(t, "manifest two\n", string(body))
}

func TestLoadDecomposeSnapshots_ReportsReplicaDriftAndUsesFineFilenameRole(t *testing.T) {
	root := t.TempDir()
	envDir := filepath.Join(root, "prod")
	require.NoError(t, os.MkdirAll(envDir, 0o755))
	dump := func(node, table string) string {
		return `node "` + node + `" {
  macros = { hostClusterRole = "ingestion", shard = "1", replica = "a" }
}
database "analytics" {
  table "` + table + `" {
    order_by = ["id"]
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
  }
}`
	}
	require.NoError(t, os.WriteFile(filepath.Join(envDir, "prod-us-iad-ch-1a-ingestion-events.hcl"),
		[]byte(dump("prod-us-iad-ch-1a-ingestion-events", "events_a")), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(envDir, "prod-us-iad-ch-1b-ingestion-events.hcl"),
		[]byte(dump("prod-us-iad-ch-1b-ingestion-events", "events_b")), 0o644))

	snapshots, envs, drift, err := loadDecomposeSnapshots(root, []string{"prod"}, "*", "keep", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"prod"}, envs)
	require.Len(t, snapshots, 1)
	assert.Equal(t, "ingestion-events", snapshots[0].Role,
		"the deployment-role suffix must refine the coarse hostClusterRole macro")
	require.Len(t, drift, 1)
	assert.Equal(t, "ingestion-events", drift[0].Role)
	assert.Contains(t, drift[0].Summary, "table")
}
