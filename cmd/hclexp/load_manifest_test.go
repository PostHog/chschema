package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/require"
)

// twoRoleManifest writes a data/aux layer tree plus the manifest describing it,
// mirroring the shape TestValidateManifest_AllRoles uses: aux owns the storage
// table, data owns a Distributed proxy into it. The "dev" env deploys both
// roles; "staging" deploys only aux.
func twoRoleManifest(t *testing.T) (root, manifest string) {
	t.Helper()
	root = t.TempDir()
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
	manifest = writeTemp(t, "manifest.hcl", `
role "data" {
  env "dev" { layers = ["layers/data"] }
}
role "aux" {
  env "dev"     { layers = ["layers/aux"] }
  env "staging" { layers = ["layers/aux"] }
}
cluster "aux"     { roles = ["aux"] }
cluster "posthog" { roles = ["data"] }`)
	return root, manifest
}

// composeManifestRoles preserves manifest order and reports both the declared
// layer stack and the stack resolved under -layer-root.
func TestComposeManifestRoles(t *testing.T) {
	root, manifest := twoRoleManifest(t)

	roles, err := parseManifest(manifest, "dev")
	require.NoError(t, err)
	composed, err := composeManifestRoles(roles, root)
	require.NoError(t, err)

	require.Len(t, composed, 2)
	require.Equal(t, "data", composed[0].Role, "manifest order is preserved")
	require.Equal(t, "aux", composed[1].Role)

	require.Equal(t, []string{"layers/data"}, composed[0].Layers)
	require.Equal(t, []string{filepath.Join(root, "layers/data")}, composed[0].Resolved)

	require.Len(t, composed[1].Schema.Databases, 1)
	require.Equal(t, "posthog", composed[1].Schema.Databases[0].Name)
	require.Len(t, composed[1].Schema.Databases[0].Tables, 1)
	require.Equal(t, "sharded_web_stats", composed[1].Schema.Databases[0].Tables[0].Name)
}

// A later layer in a role's stack overrides an earlier one, so composition
// through the manifest honors the same precedence as -layer.
func TestComposeManifestRoles_LayerPrecedence(t *testing.T) {
	root := t.TempDir()
	writeLayer(t, root, "base/base.hcl", `
database "posthog" {
  table "events" {
    order_by = ["day"]
    column "day" { type = "Date" }
    engine "merge_tree" {}
  }
}`)
	writeLayer(t, root, "prod/prod.hcl", `
database "posthog" {
  patch_table "events" {
    column "team_id" { type = "UInt32" }
  }
}`)
	manifest := writeTemp(t, "manifest.hcl", `
role "ops" {
  env "prod" { layers = ["base", "prod"] }
}`)

	roles, err := parseManifest(manifest, "prod")
	require.NoError(t, err)
	composed, err := composeManifestRoles(roles, root)
	require.NoError(t, err)
	require.Len(t, composed, 1)

	cols := composed[0].Schema.Databases[0].Tables[0].Columns
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	require.Equal(t, []string{"day", "team_id"}, names, "the later layer's patch_table column is applied")
}

func TestFilterManifestRoles(t *testing.T) {
	roles := []manifestRole{
		{Role: "data", Layers: []string{"layers/data"}},
		{Role: "aux", Layers: []string{"layers/aux"}},
	}

	t.Run("empty keeps every role", func(t *testing.T) {
		got, err := filterManifestRoles(roles, "", "dev")
		require.NoError(t, err)
		require.Equal(t, roles, got)
	})

	t.Run("selects one", func(t *testing.T) {
		got, err := filterManifestRoles(roles, "aux", "dev")
		require.NoError(t, err)
		require.Equal(t, []manifestRole{{Role: "aux", Layers: []string{"layers/aux"}}}, got)
	})

	t.Run("unknown role names the deployed ones", func(t *testing.T) {
		_, err := filterManifestRoles(roles, "nope", "dev")
		require.ErrorContains(t, err, `unknown role "nope" in env "dev"`)
		require.ErrorContains(t, err, "aux, data")
		require.ErrorIs(t, err, errUnknownRole,
			"an unknown role is a usage error, so load and validate both exit 2")
	})
}

// -role composes a single node straight from the manifest: exactly that role's
// schema, written where -out points.
func TestRunLoadManifest_SingleRoleToFile(t *testing.T) {
	root, manifest := twoRoleManifest(t)
	out := filepath.Join(t.TempDir(), "aux.hcl")

	roles, err := parseManifest(manifest, "dev")
	require.NoError(t, err)
	roles, err = filterManifestRoles(roles, "aux", "dev")
	require.NoError(t, err)
	composed, err := composeManifestRoles(roles, root)
	require.NoError(t, err)
	require.NoError(t, checkOutTarget(out, len(composed)))
	require.NoError(t, writeComposedRoles(out, "dev", composed))

	body, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Contains(t, string(body), `table "sharded_web_stats"`)
	require.NotContains(t, string(body), `table "web_stats"`, "the data role's proxy is not composed")
}

// Without -role every deployed role is composed into one <env>-<role>.hcl each.
// A role with no env block for -env produces no file.
func TestRunLoadManifest_AllRolesToDir(t *testing.T) {
	root, manifest := twoRoleManifest(t)
	out := t.TempDir()

	roles, err := parseManifest(manifest, "dev")
	require.NoError(t, err)
	composed, err := composeManifestRoles(roles, root)
	require.NoError(t, err)
	require.NoError(t, checkOutTarget(out, len(composed)))
	require.NoError(t, writeComposedRoles(out, "dev", composed))

	for _, name := range []string{"dev-data.hcl", "dev-aux.hcl"} {
		_, err := os.Stat(filepath.Join(out, name))
		require.NoError(t, err, "expected %s", name)
	}

	// staging deploys only aux, so data contributes no file.
	stagingOut := t.TempDir()
	stagingRoles, err := parseManifest(manifest, "staging")
	require.NoError(t, err)
	stagingComposed, err := composeManifestRoles(stagingRoles, root)
	require.NoError(t, err)
	require.NoError(t, writeComposedRoles(stagingOut, "staging", stagingComposed))

	entries, err := os.ReadDir(stagingOut)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "staging-aux.hcl", entries[0].Name())
}

// Composing several roles onto stdout would repeat their database blocks, so it
// is refused rather than emitting an unloadable schema.
func TestCheckOutTarget(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "f.hcl")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))

	require.NoError(t, checkOutTarget("", 1), "one role may go to stdout")
	require.NoError(t, checkOutTarget("-", 1))
	require.NoError(t, checkOutTarget(file, 1))
	require.NoError(t, checkOutTarget(dir, 2))

	require.ErrorContains(t, checkOutTarget("", 2), "-out (a directory) is required")
	require.ErrorContains(t, checkOutTarget("-", 2), "-out (a directory) is required")
	require.ErrorContains(t, checkOutTarget(file, 2), "must be an existing directory")
}

// The JSON document is what callers parse instead of the manifest, so its shape
// is pinned whole.
func TestWriteLoadJSON(t *testing.T) {
	root, manifest := twoRoleManifest(t)
	roles, err := parseManifest(manifest, "dev")
	require.NoError(t, err)
	composed, err := composeManifestRoles(roles, root)
	require.NoError(t, err)

	out := filepath.Join(t.TempDir(), "stacks.json")
	require.NoError(t, writeLoadJSON(out, "dev", composed))

	body, err := os.ReadFile(out)
	require.NoError(t, err)
	var got loadJSON
	require.NoError(t, json.Unmarshal(body, &got))

	require.Equal(t, loadJSON{
		Env: "dev",
		Roles: []loadRoleJSON{
			{
				Role:           "data",
				Layers:         []string{"layers/data"},
				ResolvedLayers: []string{filepath.Join(root, "layers/data")},
			},
			{
				Role:           "aux",
				Layers:         []string{"layers/aux"},
				ResolvedLayers: []string{filepath.Join(root, "layers/aux")},
			},
		},
	}, got)
}

// -role filters the JSON document too.
func TestWriteLoadJSON_RoleFilter(t *testing.T) {
	root, manifest := twoRoleManifest(t)
	roles, err := parseManifest(manifest, "dev")
	require.NoError(t, err)
	roles, err = filterManifestRoles(roles, "aux", "dev")
	require.NoError(t, err)
	composed, err := composeManifestRoles(roles, root)
	require.NoError(t, err)

	out := filepath.Join(t.TempDir(), "stacks.json")
	require.NoError(t, writeLoadJSON(out, "dev", composed))
	body, err := os.ReadFile(out)
	require.NoError(t, err)

	var got loadJSON
	require.NoError(t, json.Unmarshal(body, &got))
	require.Len(t, got.Roles, 1)
	require.Equal(t, "aux", got.Roles[0].Role)
}

func TestLoadFlagsError(t *testing.T) {
	tests := []struct {
		name                                string
		manifest, env, role, format, layers string
		configSet                           bool
		wantErr                             string
	}{
		{name: "layer mode", format: "hcl"},
		{name: "layer mode with layers", format: "hcl", layers: "a,b"},
		{name: "manifest mode", manifest: "m.hcl", env: "dev", format: "hcl"},
		{name: "manifest mode with role", manifest: "m.hcl", env: "dev", role: "ops", format: "hcl"},
		{name: "manifest json", manifest: "m.hcl", env: "dev", format: "json"},

		{name: "manifest without env", manifest: "m.hcl", format: "hcl",
			wantErr: "-manifest and -env must be used together"},
		{name: "env without manifest", env: "dev", format: "hcl",
			wantErr: "-manifest and -env must be used together"},
		{name: "bad format", format: "yaml",
			wantErr: `invalid -format "yaml"`},
		{name: "role without manifest", role: "ops", format: "hcl",
			wantErr: "-role requires -manifest"},
		{name: "json without manifest", format: "json",
			wantErr: "-format json requires -manifest"},
		{name: "manifest and layer", manifest: "m.hcl", env: "dev", format: "hcl", layers: "a",
			wantErr: "-manifest is mutually exclusive with -layer and -config"},
		{name: "manifest and config", manifest: "m.hcl", env: "dev", format: "hcl", configSet: true,
			wantErr: "-manifest is mutually exclusive with -layer and -config"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := loadFlagsError(tc.manifest, tc.env, tc.role, tc.format, tc.layers, tc.configSet)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// A composed role written out as canonical HCL reloads to the same schema the
// layer stack composes to — the round-trip gen-golden.sh depends on.
func TestRunLoadManifest_RoundTrip(t *testing.T) {
	root, manifest := twoRoleManifest(t)
	out := filepath.Join(t.TempDir(), "aux.hcl")

	roles, err := parseManifest(manifest, "dev")
	require.NoError(t, err)
	roles, err = filterManifestRoles(roles, "aux", "dev")
	require.NoError(t, err)
	composed, err := composeManifestRoles(roles, root)
	require.NoError(t, err)
	require.NoError(t, writeComposedRoles(out, "dev", composed))

	reloaded, err := loadSide(out)
	require.NoError(t, err)
	require.True(t, hclload.Diff(composed[0].Schema, reloaded).IsEmpty(),
		"the written HCL reloads to the composed schema")
}

// validate -manifest -role narrows which roles are checked, but the cluster set
// still comes from the whole manifest — so the data role's Distributed proxy
// into the aux role's storage table still resolves.
func TestValidateManifest_RoleFilter(t *testing.T) {
	root, manifest := twoRoleManifest(t)

	results, err := validateManifest(manifest, "dev", root, "data", hclload.ParseSkipSet(""), hclload.ValidateOptions{}, nil)
	require.NoError(t, err)
	require.Len(t, results, 1, "only the selected role is validated")
	require.Equal(t, "data", results[0].Role)
	require.Empty(t, results[0].Errs, "its cross-role proxy resolves against the whole manifest's clusters")

	_, err = validateManifest(manifest, "dev", root, "nope", hclload.ParseSkipSet(""), hclload.ValidateOptions{}, nil)
	require.ErrorIs(t, err, errUnknownRole)
}
