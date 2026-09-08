package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanColumnOrderCLIProcess(t *testing.T) {
	if os.Getenv("HCLEXP_PLAN_COLUMN_ORDER_HELPER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			runPlan(os.Args[i+1:])
			os.Exit(0)
			return
		}
	}
	t.Fatal("missing plan CLI arguments")
}

func TestPlanIgnoreColumnOrderEndToEnd(t *testing.T) {
	root := t.TempDir()
	desiredRoot := filepath.Join(root, "desired")
	dumpRoot := filepath.Join(root, "dump")
	manifest := `role "ops" {
  env "prod" { layers = ["schema.hcl"] }
}
`
	writeFileT(t, filepath.Join(desiredRoot, "manifest.hcl"), manifest)
	writeFileT(t, filepath.Join(desiredRoot, "schema.hcl"), planColumnOrderHCL("b", "a"))
	writeFileT(t, filepath.Join(dumpRoot, "prod-us-iad-ch-1a-ops.hcl"), `node "prod-us-iad-ch-1a-ops" {
  macros = { cluster = "ops", hostClusterRole = "ops", shard = "1", replica = "a" }
}
`+planColumnOrderHCL("a", "b"))

	args := []string{
		"-manifest", filepath.Join(desiredRoot, "manifest.hcl"),
		"-layer-root", desiredRoot,
		"-env", "prod",
		"-dump", dumpRoot,
	}
	output, err := runPlanColumnOrderCLI(root, args...)
	require.NoError(t, err, string(output))
	var orderSensitive hclload.PlanResult
	require.NoError(t, json.Unmarshal(output, &orderSensitive), string(output))
	require.Len(t, orderSensitive.Unsafe, 2)
	require.Len(t, orderSensitive.Roles, 1)
	require.Len(t, orderSensitive.Roles[0].Objects, 2)
	for _, object := range orderSensitive.Roles[0].Objects {
		assert.Equal(t, []hclload.FieldChange{{
			Field: "column_order", Change: "modify", Old: "a, b", New: "b, a",
		}}, object.Changes)
	}

	ignored, err := runPlanColumnOrderCLI(root, append(args, "-ignore-column-order")...)
	require.NoError(t, err, string(ignored))
	var clean hclload.PlanResult
	require.NoError(t, json.Unmarshal(ignored, &clean), string(ignored))
	assert.Empty(t, clean.Operations)
	assert.Empty(t, clean.Unsafe)
	require.Len(t, clean.Roles, 1)
	assert.Empty(t, clean.Roles[0].Objects)

	text, err := runPlanColumnOrderCLI(root, append(args, "-ignore-column-order", "-format", "text")...)
	require.NoError(t, err, string(text))
	assert.Equal(t, "no changes\n", string(text))
}

func planColumnOrderHCL(first, second string) string {
	return `database "posthog" {
  table "events" {
    column "` + first + `" { type = "UInt64" }
    column "` + second + `" { type = "UInt64" }
    engine "log" {}
  }
  materialized_view "events_mv" {
    to_table = "events"
    query    = "SELECT a, b FROM source"
    column "` + first + `" { type = "UInt64" }
    column "` + second + `" { type = "UInt64" }
  }
}
`
}

func runPlanColumnOrderCLI(workdir string, args ...string) ([]byte, error) {
	commandArgs := append([]string{"-test.run=^TestPlanColumnOrderCLIProcess$", "--"}, args...)
	cmd := exec.Command(os.Args[0], commandArgs...)
	cmd.Dir = workdir
	cmd.Env = hclexpConfigTestEnv(filepath.Join(workdir, "test-home"), "HCLEXP_PLAN_COLUMN_ORDER_HELPER=1")
	return cmd.Output()
}
