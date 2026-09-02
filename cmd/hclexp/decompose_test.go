package main

import (
	"os"
	"os/exec"
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

func TestDecompose_EngineOnlyDivergence_EndToEnd(t *testing.T) {
	dumpRoot := t.TempDir()
	writeDump := func(env, zooPath string) {
		t.Helper()
		body := `node "` + env + `-ops" {
  macros = { hostClusterRole = "ops" }
}
database "posthog" {
  table "sharded_tophog" {
    order_by = ["id"]
    column "id" { type = "UInt64" }
    engine "replicated_merge_tree" {
      zoo_path     = "` + zooPath + `"
      replica_name = "{replica}"
    }
  }
}`
		dir := filepath.Join(dumpRoot, env)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, env+"-ops.hcl"), []byte(body), 0o600))
	}
	basePath := "/clickhouse/tables/ops/{shard}/posthog.tophog"
	writeDump("dev", basePath)
	writeDump("prod-eu", basePath)
	writeDump("prod-us", "/clickhouse/tables/ops/{shard}/posthog.tophog_new")

	snapshots, envs, drift, err := loadDecomposeSnapshots(
		dumpRoot, []string{"dev", "prod-eu", "prod-us"}, "*-ops.hcl", "mask-uuid", nil,
	)
	require.NoError(t, err)
	require.Empty(t, drift)

	key := "ops/posthog/table/sharded_tophog"
	for _, tc := range []struct {
		name    string
		objects map[string]decomposeObjectAssignment
	}{
		{name: "auto", objects: map[string]decomposeObjectAssignment{}},
		{name: "forced shared", objects: map[string]decomposeObjectAssignment{key: {Mode: "shared"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			generated, err := buildDecomposition(snapshots, envs, decomposeAssignment{
				Version: 1, BaselineEnv: "dev", Objects: tc.objects,
			})
			require.NoError(t, err)

			shared := string(generated.Files[sharedLayerPath("ops")])
			assert.Contains(t, shared, `table "sharded_tophog"`)
			assert.Contains(t, shared, `zoo_path     = "`+basePath+`"`)

			prodUS := string(generated.Files[envLayerPath("prod-us", "ops")])
			assert.Contains(t, prodUS, `patch_table "sharded_tophog"`)
			assert.Contains(t, prodUS, `engine "replicated_merge_tree"`)
			assert.Contains(t, prodUS, `zoo_path     = "/clickhouse/tables/ops/{shard}/posthog.tophog_new"`)
			assert.NotContains(t, prodUS, `column "id"`, "an engine-only patch must not duplicate the table body")

			for _, env := range []string{"dev", "prod-eu"} {
				assert.NotContains(t, generated.Files, envLayerPath(env, "ops"),
					"%s matches the baseline and must not get an env layer", env)
			}
		})
	}
}

func TestDecompose_EnvironmentGroup_EndToEnd(t *testing.T) {
	dumpRoot := t.TempDir()
	writeDump := func(env string, includeProdTables bool) {
		t.Helper()
		prodTables := ""
		if includeProdTables {
			prodTables = `
  table "events_main" {
    order_by = ["id"]
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
  }
  table "query_team_daily_stats" {
    order_by = ["team_id"]
    column "team_id" { type = "UInt64" }
    engine "merge_tree" {}
  }`
		}
		body := `node "` + env + `-ops" {
  macros = { hostClusterRole = "ops" }
}
database "posthog" {
  table "all_environments" {
    order_by = ["id"]
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
  }` + prodTables + `
}`
		dir := filepath.Join(dumpRoot, env)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, env+"-ops.hcl"), []byte(body), 0o600))
	}
	writeDump("dev", false)
	writeDump("prod-eu", true)
	writeDump("prod-us", true)

	assignmentPath := filepath.Join(t.TempDir(), "assignment.json")
	require.NoError(t, os.WriteFile(assignmentPath, []byte(`{
  "version": 1,
  "objects": {
    "ops/posthog/table/events_main": {
      "mode": "group",
      "envs": ["prod-us", "prod-eu"]
    },
    "ops/posthog/table/query_team_daily_stats": {
      "mode": "group",
      "envs": ["prod-eu", "prod-us"]
    }
  }
}`), 0o600))
	out := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestDecomposeCLIProcess$", "--",
		"-dump-root", dumpRoot,
		"-env", "dev,prod-eu,prod-us",
		"-glob", "*-ops.hcl",
		"-zk-paths", "keep",
		"-assignment", assignmentPath,
		"-out", out,
	)
	cmd.Env = append(os.Environ(), "HCLEXP_DECOMPOSE_HELPER=1")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	assert.Contains(t, string(output), "decomposed 3 environments")
	assert.Contains(t, string(output), "round-trip verified")

	sharedBody, err := os.ReadFile(filepath.Join(out, sharedLayerPath("ops")))
	require.NoError(t, err)
	shared := string(sharedBody)
	assert.Contains(t, shared, `table "all_environments"`)
	assert.NotContains(t, shared, `table "events_main"`)

	groupPath := groupLayerPath("prod", "ops")
	groupBody, err := os.ReadFile(filepath.Join(out, groupPath))
	require.NoError(t, err)
	group := string(groupBody)
	assert.Contains(t, group, `table "events_main"`)
	assert.Contains(t, group, `table "query_team_daily_stats"`)
	for _, env := range []string{"dev", "prod-eu", "prod-us"} {
		_, err := os.Stat(filepath.Join(out, envLayerPath(env, "ops")))
		assert.ErrorIs(t, err, os.ErrNotExist, "grouped declarations must not be duplicated into %s", env)
	}

	manifestBody, err := os.ReadFile(filepath.Join(out, "manifest.hcl"))
	require.NoError(t, err)
	manifest := string(manifestBody)
	assert.Contains(t, manifest, `env "dev" { layers = ["layers/shared/ops"] }`)
	assert.Contains(t, manifest, `env "prod-eu" { layers = ["layers/shared/ops", "layers/group/prod/ops"] }`)
	assert.Contains(t, manifest, `env "prod-us" { layers = ["layers/shared/ops", "layers/group/prod/ops"] }`)
}

func TestDecomposeCLIProcess(t *testing.T) {
	if os.Getenv("HCLEXP_DECOMPOSE_HELPER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			runDecompose(os.Args[i+1:])
			return
		}
	}
	t.Fatal("missing decompose CLI arguments")
}

func TestBuildDecomposition_GroupAssignmentFailsClosed(t *testing.T) {
	base := decomposeTable(hclload.ColumnSpec{Name: "id", Type: "UInt64"})
	different := decomposeTable(
		hclload.ColumnSpec{Name: "id", Type: "UInt64"},
		hclload.ColumnSpec{Name: "extra", Type: "String"},
	)
	key := "ops/analytics/table/events"

	t.Run("presence differs from members", func(t *testing.T) {
		_, err := buildDecomposition([]decomposeSnapshot{
			{Env: "dev", Role: "ops", Schema: base},
			{Env: "prod-eu", Role: "ops", Schema: base},
			{Env: "prod-us", Role: "ops", Schema: base},
		}, []string{"dev", "prod-eu", "prod-us"}, decomposeAssignment{
			Version: 1, Objects: map[string]decomposeObjectAssignment{
				key: {Mode: "group", Envs: []string{"prod-eu", "prod-us"}},
			},
		})
		require.ErrorContains(t, err, `requests group "prod"`)
		require.ErrorContains(t, err, "object is present in [dev, prod-eu, prod-us]")
	})

	t.Run("members differ", func(t *testing.T) {
		_, err := buildDecomposition([]decomposeSnapshot{
			{Env: "dev", Role: "ops", Schema: &hclload.Schema{}},
			{Env: "prod-eu", Role: "ops", Schema: base},
			{Env: "prod-us", Role: "ops", Schema: different},
		}, []string{"dev", "prod-eu", "prod-us"}, decomposeAssignment{
			Version: 1, Objects: map[string]decomposeObjectAssignment{
				key: {Mode: "group", Envs: []string{"prod-eu", "prod-us"}},
			},
		})
		require.ErrorContains(t, err, `requests group "prod"`)
		require.ErrorContains(t, err, "object differs between member environments")
	})

	t.Run("unknown member", func(t *testing.T) {
		_, err := buildDecomposition([]decomposeSnapshot{
			{Env: "prod-eu", Role: "ops", Schema: base},
			{Env: "prod-us", Role: "ops", Schema: base},
		}, []string{"prod-eu", "prod-us"}, decomposeAssignment{
			Version: 1, Objects: map[string]decomposeObjectAssignment{
				key: {Mode: "group", Envs: []string{"prod-eu", "prod-ap"}},
			},
		})
		require.ErrorContains(t, err, `references unknown environment "prod-ap"`)
	})
}

func TestResolveDecomposeGroups_NamingAndValidation(t *testing.T) {
	byObject, groups, err := resolveDecomposeGroups(map[string]decomposeObjectAssignment{
		"explicit": {Mode: "group", Envs: []string{"prod-us", "prod-eu"}, Name: "production"},
	}, []string{"prod-eu", "prod-us"})
	require.NoError(t, err)
	assert.Equal(t, "production", byObject["explicit"].Name)
	assert.Equal(t, []decomposeGroup{{Name: "production", Envs: []string{"prod-eu", "prod-us"}}}, groups)

	_, _, err = resolveDecomposeGroups(map[string]decomposeObjectAssignment{
		"a": {Mode: "group", Envs: []string{"prod-eu", "prod-us"}},
		"b": {Mode: "group", Envs: []string{"prod-ap", "prod-ca"}},
	}, []string{"prod-ap", "prod-ca", "prod-eu", "prod-us"})
	require.ErrorContains(t, err, `group name "prod" maps to conflicting environment sets`)

	for _, tc := range []struct {
		name       string
		assignment decomposeObjectAssignment
		want       string
	}{
		{name: "one member", assignment: decomposeObjectAssignment{Mode: "group", Envs: []string{"prod-eu"}}, want: "requires at least two environments"},
		{name: "duplicate member", assignment: decomposeObjectAssignment{Mode: "group", Envs: []string{"prod-eu", "prod-eu"}}, want: `contains environment "prod-eu" more than once`},
		{name: "unsafe name", assignment: decomposeObjectAssignment{Mode: "group", Envs: []string{"prod-eu", "prod-us"}, Name: "../prod"}, want: "group name"},
		{name: "group fields on another mode", assignment: decomposeObjectAssignment{Mode: "auto", Envs: []string{"prod-eu", "prod-us"}}, want: "sets group envs/name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := resolveDecomposeGroups(map[string]decomposeObjectAssignment{"invalid": tc.assignment}, []string{"prod-eu", "prod-us"})
			require.ErrorContains(t, err, tc.want)
		})
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
