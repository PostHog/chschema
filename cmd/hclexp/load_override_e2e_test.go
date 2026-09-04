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

func TestLoadOverrideCLIProcess(t *testing.T) {
	if os.Getenv("HCLEXP_LOAD_OVERRIDE_HELPER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			runLoad(os.Args[i+1:])
			return
		}
	}
	t.Fatal("missing load CLI arguments")
}

func TestLoadCLI_OverrideEveryManagedObjectKindEndToEnd(t *testing.T) {
	root := t.TempDir()
	base := writeLoadOverrideLayer(t, root, "base.hcl", `
named_collection "warehouse" {
  param "host" { value = "production.internal" }
}

database "analytics" {
  table "events" {
    column "id" { type = "UInt64" }
    engine "log" {}
  }

  materialized_view "events_mv" {
    to_table = "analytics.events"
    query    = "SELECT id FROM production_source"
    column "id" { type = "UInt64" }
  }

  view "environment" {
    query = "SELECT 'production' AS name"
  }

  dictionary "labels" {
    primary_key = ["id"]
    attribute "id"    { type = "UInt64" }
    attribute "label" { type = "String" }
    source "null" {}
    layout "flat" {}
  }

  raw "view" "legacy_environment" {
    sql = "CREATE VIEW analytics.legacy_environment AS SELECT 'production'"
  }
}`)
	override := writeLoadOverrideLayer(t, root, "override.hcl", `
named_collection "warehouse" {
  override = true
  param "host" { value = "development.internal" }
}

database "analytics" {
  table "events" {
    override = true
    column "dev_id" { type = "UInt32" }
    engine "log" {}
  }

  materialized_view "events_mv" {
    override = true
    to_table = "analytics.events"
    query    = "SELECT dev_id FROM development_source"
    column "dev_id" { type = "UInt32" }
  }

  view "environment" {
    override = true
    query    = "SELECT 'development' AS name"
  }

  dictionary "labels" {
    override    = true
    primary_key = ["key"]
    attribute "key"   { type = "String" }
    attribute "label" { type = "String" }
    source "null" {}
    layout "flat" {}
  }

  raw "view" "legacy_environment" {
    override = true
    sql      = "CREATE VIEW analytics.legacy_environment AS SELECT 'development'"
  }
}`)
	out := filepath.Join(root, "resolved.hcl")

	cmd := exec.Command(os.Args[0], "-test.run=^TestLoadOverrideCLIProcess$", "--",
		"-layer", base+","+override,
		"-out", out,
	)
	cmd.Env = append(os.Environ(), "HCLEXP_LOAD_OVERRIDE_HELPER=1")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	schema, err := hclload.ParseFile(out)
	require.NoError(t, err)
	require.NoError(t, hclload.Resolve(schema))
	require.Len(t, schema.NamedCollections, 1)
	require.Len(t, schema.Databases, 1)
	db := schema.Databases[0]
	require.Len(t, db.Tables, 1)
	require.Len(t, db.MaterializedViews, 1)
	require.Len(t, db.Views, 1)
	require.Len(t, db.Dictionaries, 1)
	require.Len(t, db.Raws, 1)
	assert.Equal(t, "development.internal", schema.NamedCollections[0].Params[0].Value)
	assert.Equal(t, "dev_id", db.Tables[0].Columns[0].Name)
	assert.Contains(t, db.MaterializedViews[0].Query, "development_source")
	assert.Contains(t, db.Views[0].Query, "development")
	assert.Equal(t, []string{"key"}, db.Dictionaries[0].PrimaryKey)
	assert.Contains(t, db.Raws[0].SQL, "development")

	resolved, err := os.ReadFile(out)
	require.NoError(t, err)
	assert.NotContains(t, string(resolved), "override = true", "layer control metadata must not leak into resolved output")
}

func writeLoadOverrideLayer(t *testing.T, root, name, body string) string {
	t.Helper()
	path := filepath.Join(root, name)
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}
