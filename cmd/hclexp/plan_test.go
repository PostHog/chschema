package main

import (
	"os"
	"path/filepath"
	"testing"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFileT(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

const sampleManifest = `
role "ops" {
  env "local"   { layers = ["base", "env/local"] }
  env "prod-us" { layers = ["base", "prod", "env/prod-us"] }
}
role "data" {
  # data is only deployed in prod-us
  env "prod-us" { layers = ["base", "env/prod-us"] }
}
`

func TestParseManifest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.hcl")
	writeFileT(t, path, sampleManifest)

	// prod-us: both roles resolve.
	roles, err := parseManifest(path, "prod-us")
	require.NoError(t, err)
	require.Len(t, roles, 2)
	assert.Equal(t, "ops", roles[0].Role)
	assert.Equal(t, []string{"base", "prod", "env/prod-us"}, roles[0].Layers)
	assert.Equal(t, "data", roles[1].Role)
	assert.Equal(t, []string{"base", "env/prod-us"}, roles[1].Layers)

	// local: only ops is deployed; data is skipped.
	roles, err = parseManifest(path, "local")
	require.NoError(t, err)
	require.Len(t, roles, 1)
	assert.Equal(t, "ops", roles[0].Role)
	assert.Equal(t, []string{"base", "env/local"}, roles[0].Layers)
}

func TestParseManifest_Errors(t *testing.T) {
	dir := t.TempDir()

	noEnv := filepath.Join(dir, "noenv.hcl")
	writeFileT(t, noEnv, sampleManifest)
	_, err := parseManifest(noEnv, "prod-eu")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `no roles deployed in env "prod-eu"`)

	dup := filepath.Join(dir, "dup.hcl")
	writeFileT(t, dup, `role "ops" {
  env "prod-us" { layers = ["a"] }
}
role "ops" {
  env "prod-us" { layers = ["b"] }
}`)
	_, err = parseManifest(dup, "prod-us")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate role")

	dupEnv := filepath.Join(dir, "dupenv.hcl")
	writeFileT(t, dupEnv, `role "ops" {
  env "prod-us" { layers = ["a"] }
  env "prod-us" { layers = ["b"] }
}`)
	_, err = parseManifest(dupEnv, "prod-us")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate env")

	empty := filepath.Join(dir, "empty.hcl")
	writeFileT(t, empty, "# nothing here\n")
	_, err = parseManifest(empty, "prod-us")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no roles")
}

// TestCurrentByRole verifies dump nodes are keyed by their hostClusterRole macro
// and that replicas of a role collapse to one representative (lexically first).
func TestCurrentByRole(t *testing.T) {
	dir := t.TempDir()
	node := func(name, role, replica, table string) string {
		return `node "` + name + `" {
  macros = { cluster = "ops", hostClusterRole = "` + role + `", shard = "1", replica = "` + replica + `" }
}
database "posthog" {
  table "` + table + `" {
    engine "merge_tree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
}
`
	}
	// Two ops replicas (1c lexically first) + one data node.
	writeFileT(t, filepath.Join(dir, "prod-us-iad-ch-1d-ops.hcl"), node("prod-us-iad-ch-1d-ops", "ops", "d", "from_1d"))
	writeFileT(t, filepath.Join(dir, "prod-us-iad-ch-1c-ops.hcl"), node("prod-us-iad-ch-1c-ops", "ops", "c", "from_1c"))
	writeFileT(t, filepath.Join(dir, "prod-us-iad-ch-1a-data.hcl"), node("prod-us-iad-ch-1a-data", "data", "a", "data_tbl"))

	byRole, err := currentByRole(dir)
	require.NoError(t, err)
	require.Contains(t, byRole, "ops")
	require.Contains(t, byRole, "data")
	assert.Len(t, byRole, 2, "two ops replicas collapse to one role entry")

	// The lexically-first ops node (1c) is the representative.
	require.Len(t, byRole["ops"].Databases, 1)
	require.Len(t, byRole["ops"].Databases[0].Tables, 1)
	assert.Equal(t, "from_1c", byRole["ops"].Databases[0].Tables[0].Name)
}

func TestRoleDiffsFromDump_DesiredScopeAddressesIssue75(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "desired", "tables.hcl"), `
database "posthog" {
  table "managed" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`)
	desiredRoles := []manifestRole{{Role: "ops", Layers: []string{"desired"}}}
	managed := hclload.TableSpec{
		Name: "managed", Columns: []hclload.ColumnSpec{{Name: "id", Type: "UInt64"}},
		OrderBy: []string{"id"}, Engine: &hclload.EngineSpec{Kind: "merge_tree", Decoded: hclload.EngineMergeTree{}},
	}
	unmanaged := managed
	unmanaged.Name = "unmanaged_adhoc"
	current := map[string]*hclload.Schema{"ops": {
		Databases: []hclload.DatabaseSpec{{Name: "posthog", Tables: []hclload.TableSpec{managed, unmanaged}}},
	}}

	scoped, err := roleDiffsFromDump(desiredRoles, root, current, nil, "desired")
	require.NoError(t, err)
	scopedPlan := hclload.BuildPlan(scoped)
	require.Empty(t, scopedPlan.Operations)
	require.Len(t, scopedPlan.Roles, 1)
	require.Empty(t, scopedPlan.Roles[0].Objects)

	exact, err := roleDiffsFromDump(desiredRoles, root, current, nil, "all")
	require.NoError(t, err)
	exactPlan := hclload.BuildPlan(exact)
	require.Len(t, exactPlan.Operations, 1)
	assert.Equal(t, hclload.OpDrop, exactPlan.Operations[0].Kind)
	assert.Equal(t, "unmanaged_adhoc", exactPlan.Operations[0].Object)
}

func TestRoleDiffsFromDump_DesiredScopeRemovesEveryLiveOnlyObjectKind(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "desired", "tables.hcl"), `
database "posthog" {
  table "managed" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`)
	managed := hclload.TableSpec{
		Name: "managed", Columns: []hclload.ColumnSpec{{Name: "id", Type: "UInt64"}},
		OrderBy: []string{"id"}, Engine: &hclload.EngineSpec{Kind: "merge_tree", Decoded: hclload.EngineMergeTree{}},
	}
	current := map[string]*hclload.Schema{"ops": {
		Databases: []hclload.DatabaseSpec{{
			Name:              "posthog",
			Tables:            []hclload.TableSpec{managed, {Name: "unmanaged_table"}},
			MaterializedViews: []hclload.MaterializedViewSpec{{Name: "unmanaged_mv"}},
			Views:             []hclload.ViewSpec{{Name: "unmanaged_view"}},
			Dictionaries:      []hclload.DictionarySpec{{Name: "unmanaged_dict"}},
			Raws: []hclload.RawSpec{{
				Kind: hclload.KindView, Name: "unmanaged_raw", SQL: "CREATE VIEW posthog.unmanaged_raw AS SELECT 1\n",
			}},
		}},
		NamedCollections: []hclload.NamedCollectionSpec{{Name: "unmanaged_nc"}},
	}}

	roles, err := roleDiffsFromDump(
		[]manifestRole{{Role: "ops", Layers: []string{"desired"}}}, root, current, nil, "desired",
	)
	require.NoError(t, err)
	plan := hclload.BuildPlan(roles)
	assert.Empty(t, plan.Operations)
	require.Len(t, plan.Roles, 1)
	assert.Empty(t, plan.Roles[0].Objects)
}

func TestRoleDiffsFromDump_DesiredScopeKeepsManagedCreatesAndAlters(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "desired", "tables.hcl"), `
database "posthog" {
  table "managed" {
    column "id" { type = "UInt64" }
    column "team_id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
  table "missing_live" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`)
	current := map[string]*hclload.Schema{"ops": {
		Databases: []hclload.DatabaseSpec{{Name: "posthog", Tables: []hclload.TableSpec{{
			Name: "managed", Columns: []hclload.ColumnSpec{{Name: "id", Type: "UInt64"}},
			OrderBy: []string{"id"}, Engine: &hclload.EngineSpec{Kind: "merge_tree", Decoded: hclload.EngineMergeTree{}},
		}}}},
	}}

	roles, err := roleDiffsFromDump(
		[]manifestRole{{Role: "ops", Layers: []string{"desired"}}}, root, current, nil, "desired",
	)
	require.NoError(t, err)
	plan := hclload.BuildPlan(roles)
	require.Len(t, plan.Operations, 2)
	kinds := map[string]string{}
	for _, op := range plan.Operations {
		kinds[op.Object] = op.Kind
	}
	assert.Equal(t, hclload.OpAlter, kinds["managed"])
	assert.Equal(t, hclload.OpCreate, kinds["missing_live"])
}

func TestRoleDiffsFromDump_RoleAbsentPlansCreates(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "desired", "tables.hcl"), `
database "posthog" {
  table "managed" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`)

	roles, err := roleDiffsFromDump(
		[]manifestRole{{Role: "ops", Layers: []string{"desired"}}}, root, nil, nil, "desired",
	)
	require.NoError(t, err)
	plan := hclload.BuildPlan(roles)
	require.Len(t, plan.Operations, 1)
	assert.Equal(t, hclload.OpCreate, plan.Operations[0].Kind)
}

func TestRoleDiffsFromManifest_ExactPreviousToProposed(t *testing.T) {
	previousRoot := filepath.Join(t.TempDir(), "previous")
	proposedRoot := filepath.Join(t.TempDir(), "proposed")
	writeFileT(t, filepath.Join(previousRoot, "ops", "tables.hcl"), `
database "posthog" {
  table "managed" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
  table "old_table" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`)
	writeFileT(t, filepath.Join(proposedRoot, "ops", "tables.hcl"), `
database "posthog" {
  table "managed" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
  table "new_table" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`)

	roles, err := roleDiffsFromManifest(
		[]manifestRole{{Role: "ops", Layers: []string{"ops"}}}, proposedRoot,
		[]manifestRole{{Role: "ops", Layers: []string{"ops"}}}, previousRoot, nil,
	)
	require.NoError(t, err)
	plan := hclload.BuildPlan(roles)
	require.Len(t, plan.Operations, 2)
	byObject := make(map[string]hclload.PlanOperation, len(plan.Operations))
	for _, op := range plan.Operations {
		byObject[op.Object] = op
	}
	assert.Equal(t, hclload.OpCreate, byObject["new_table"].Kind)
	assert.Equal(t, hclload.OpDrop, byObject["old_table"].Kind)
	assert.Equal(t, []string{"ops"}, byObject["old_table"].Roles)
}

func TestRoleDiffsFromManifest_RoleSetRules(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "ops", "tables.hcl"), `
database "posthog" {
  table "managed" {
    column "id" { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`)

	t.Run("proposed-only role creates", func(t *testing.T) {
		roles, err := roleDiffsFromManifest(
			[]manifestRole{{Role: "ops", Layers: []string{"ops"}}}, root, nil, root, nil,
		)
		require.NoError(t, err)
		plan := hclload.BuildPlan(roles)
		require.Len(t, plan.Operations, 1)
		assert.Equal(t, hclload.OpCreate, plan.Operations[0].Kind)
	})

	t.Run("previous-only role is explicit error", func(t *testing.T) {
		_, err := roleDiffsFromManifest(
			nil, root, []manifestRole{{Role: "ops", Layers: []string{"ops"}}}, root, nil,
		)
		require.ErrorContains(t, err, `previous-only role "ops"`)
	})
}

func TestParseManifestOptional_AllowsNewEnvironment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.hcl")
	writeFileT(t, path, sampleManifest)
	roles, err := parseManifestOptional(path, "new-env")
	require.NoError(t, err)
	assert.Empty(t, roles)
}
