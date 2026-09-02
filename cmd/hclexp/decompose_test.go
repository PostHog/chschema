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

func TestTablePatch_IdenticalTableReturnsEmptyPatch(t *testing.T) {
	schema := decomposeTable(hclload.ColumnSpec{Name: "id", Type: "UInt64"})
	patch, err := tablePatch(schema, schema, decomposeObject{
		Role: "logs", Database: "analytics", Kind: hclload.KindTable, Name: "events",
	})
	require.NoError(t, err)
	assert.True(t, patchTableEmpty(patch))
}

func TestDecompose_ThreeEnvironmentsWithOneDivergence_EndToEnd(t *testing.T) {
	dumpRoot := t.TempDir()
	writeDump := func(env string, divergent bool) {
		t.Helper()
		settings := ""
		if divergent {
			settings = `
    settings = { ttl_only_drop_parts = "1" }`
		}
		body := `node "` + env + `-logs" {
  macros = { hostClusterRole = "logs" }
}
database "analytics" {
  table "log_attributes2" {
    order_by = ["id"]
    column "id" { type = "UInt64" }
    engine "merge_tree" {}` + settings + `
  }
}`
		dir := filepath.Join(dumpRoot, env)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, env+"-logs.hcl"), []byte(body), 0o600))
	}
	writeDump("dev", false)
	writeDump("prod-eu", true)
	writeDump("prod-us", false)

	snapshots, envs, drift, err := loadDecomposeSnapshots(
		dumpRoot, []string{"dev", "prod-eu", "prod-us"}, "*-logs.hcl", "keep", nil,
	)
	require.NoError(t, err)
	require.Empty(t, drift)

	generated, err := buildDecomposition(snapshots, envs, decomposeAssignment{
		Version: 1, BaselineEnv: "dev", Objects: map[string]decomposeObjectAssignment{},
	})
	require.NoError(t, err)
	out := t.TempDir()
	require.NoError(t, writeDecomposition(out, generated.Files))

	shared, err := os.ReadFile(filepath.Join(out, sharedLayerPath("logs")))
	require.NoError(t, err)
	assert.Contains(t, string(shared), `table "log_attributes2"`)

	prodEU, err := os.ReadFile(filepath.Join(out, envLayerPath("prod-eu", "logs")))
	require.NoError(t, err)
	assert.Contains(t, string(prodEU), `patch_table "log_attributes2"`)
	assert.Contains(t, string(prodEU), `ttl_only_drop_parts = "1"`)

	for _, env := range []string{"dev", "prod-us"} {
		_, err := os.Stat(filepath.Join(out, envLayerPath(env, "logs")))
		assert.ErrorIs(t, err, os.ErrNotExist, "%s matches the baseline and must not get an env layer", env)
	}
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

func TestBuildDecomposition_PatchesEveryPatchableObjectKind(t *testing.T) {
	t.Run("materialized view", func(t *testing.T) {
		from := &hclload.Schema{Databases: []hclload.DatabaseSpec{{Name: "analytics", MaterializedViews: []hclload.MaterializedViewSpec{{
			Name: "events_mv", ToTable: "events", Query: "SELECT 1",
			Columns: []hclload.ColumnSpec{{Name: "id", Type: "UInt64"}, {Name: "created_at", Type: "DateTime"}},
		}}}}}
		to := &hclload.Schema{Databases: []hclload.DatabaseSpec{{Name: "analytics", MaterializedViews: []hclload.MaterializedViewSpec{{
			Name: "events_mv", ToTable: "events", Query: "SELECT 2",
			Columns: []hclload.ColumnSpec{{Name: "runtime", Type: "String"}, {Name: "id", Type: "UInt64"}, {Name: "created_at", Type: "DateTime"}},
		}}}}}
		generated, err := buildDecomposition([]decomposeSnapshot{
			{Env: "eu", Role: "events", Schema: from}, {Env: "us", Role: "events", Schema: to},
		}, []string{"eu", "us"}, decomposeAssignment{Version: 1, Objects: map[string]decomposeObjectAssignment{}})
		require.NoError(t, err)
		patch := string(generated.Files[envLayerPath("us", "events")])
		assert.Contains(t, patch, `patch_materialized_view "events_mv"`)
		assert.Contains(t, patch, `column "runtime"`)
		assert.Contains(t, patch, "first = true")
	})

	t.Run("view", func(t *testing.T) {
		commentA, commentB := "old", "new"
		from := &hclload.Schema{Databases: []hclload.DatabaseSpec{{Name: "analytics", Views: []hclload.ViewSpec{{Name: "events", Query: "SELECT 1", Comment: &commentA}}}}}
		to := &hclload.Schema{Databases: []hclload.DatabaseSpec{{Name: "analytics", Views: []hclload.ViewSpec{{Name: "events", Query: "SELECT 2", Comment: &commentB}}}}}
		generated, err := buildDecomposition([]decomposeSnapshot{
			{Env: "eu", Role: "ops", Schema: from}, {Env: "us", Role: "ops", Schema: to},
		}, []string{"eu", "us"}, decomposeAssignment{Version: 1, Objects: map[string]decomposeObjectAssignment{}})
		require.NoError(t, err)
		patch := string(generated.Files[envLayerPath("us", "ops")])
		assert.Contains(t, patch, `patch_view "events"`)
		assert.Contains(t, patch, `query   = "SELECT 2"`)
		assert.Contains(t, patch, `comment = "new"`)
	})

	t.Run("dictionary", func(t *testing.T) {
		lifetimeA, lifetimeB := int64(10), int64(20)
		base := hclload.DictionarySpec{
			Name: "countries", PrimaryKey: []string{"id"},
			Attributes: []hclload.DictionaryAttribute{{Name: "id", Type: "UInt64"}},
			Source:     &hclload.DictionarySourceSpec{Kind: "null", Decoded: hclload.SourceNull{}},
			Layout:     &hclload.DictionaryLayoutSpec{Kind: "direct", Decoded: hclload.LayoutDirect{}},
			Lifetime:   &hclload.DictionaryLifetime{Min: &lifetimeA}, Settings: map[string]string{"max_threads_for_updates": "1"},
		}
		target := base
		target.Lifetime = &hclload.DictionaryLifetime{Min: &lifetimeB}
		target.Settings = map[string]string{"max_threads_for_updates": "2"}
		from := &hclload.Schema{Databases: []hclload.DatabaseSpec{{Name: "analytics", Dictionaries: []hclload.DictionarySpec{base}}}}
		to := &hclload.Schema{Databases: []hclload.DatabaseSpec{{Name: "analytics", Dictionaries: []hclload.DictionarySpec{target}}}}
		generated, err := buildDecomposition([]decomposeSnapshot{
			{Env: "eu", Role: "ops", Schema: from}, {Env: "us", Role: "ops", Schema: to},
		}, []string{"eu", "us"}, decomposeAssignment{Version: 1, Objects: map[string]decomposeObjectAssignment{}})
		require.NoError(t, err)
		patch := string(generated.Files[envLayerPath("us", "ops")])
		assert.Contains(t, patch, `patch_dictionary "countries"`)
		assert.Contains(t, patch, "min = 20")
		assert.Contains(t, patch, `max_threads_for_updates = "2"`)
	})

	t.Run("named collection override", func(t *testing.T) {
		from := &hclload.Schema{NamedCollections: []hclload.NamedCollectionSpec{{Name: "warehouse", Params: []hclload.NamedCollectionParam{{Key: "host", Value: "eu.internal"}}}}}
		to := &hclload.Schema{NamedCollections: []hclload.NamedCollectionSpec{{Name: "warehouse", Params: []hclload.NamedCollectionParam{{Key: "host", Value: "us.internal"}}}}}
		generated, err := buildDecomposition([]decomposeSnapshot{
			{Env: "eu", Role: "ops", Schema: from}, {Env: "us", Role: "ops", Schema: to},
		}, []string{"eu", "us"}, decomposeAssignment{Version: 1, Objects: map[string]decomposeObjectAssignment{}})
		require.NoError(t, err)
		patch := string(generated.Files[envLayerPath("us", "ops")])
		assert.Contains(t, patch, `named_collection "warehouse"`)
		assert.Contains(t, patch, "override = true")
		assert.Contains(t, patch, `value = "us.internal"`)
	})
}

func TestBuildDecomposition_RecreateOnlyRawUsesEnvironmentDeclarations(t *testing.T) {
	from := &hclload.Schema{Databases: []hclload.DatabaseSpec{{Name: "analytics", Raws: []hclload.RawSpec{{
		Kind: hclload.KindView, Name: "unsupported", SQL: "CREATE VIEW analytics.unsupported AS SELECT 1\n",
	}}}}}
	to := &hclload.Schema{Databases: []hclload.DatabaseSpec{{Name: "analytics", Raws: []hclload.RawSpec{{
		Kind: hclload.KindView, Name: "unsupported", SQL: "CREATE VIEW analytics.unsupported AS SELECT 2\n",
	}}}}}
	generated, err := buildDecomposition([]decomposeSnapshot{
		{Env: "eu", Role: "ops", Schema: from}, {Env: "us", Role: "ops", Schema: to},
	}, []string{"eu", "us"}, decomposeAssignment{Version: 1, Objects: map[string]decomposeObjectAssignment{}})
	require.NoError(t, err)
	assert.NotContains(t, generated.Files, sharedLayerPath("ops"))
	assert.Contains(t, string(generated.Files[envLayerPath("eu", "ops")]), "SELECT 1")
	assert.Contains(t, string(generated.Files[envLayerPath("us", "ops")]), "SELECT 2")
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
