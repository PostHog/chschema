package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiffColumnOrderCLIProcess(t *testing.T) {
	if os.Getenv("HCLEXP_DIFF_COLUMN_ORDER_HELPER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			runDiff(os.Args[i+1:])
			os.Exit(0)
			return
		}
	}
	t.Fatal("missing diff CLI arguments")
}

func TestDiffMaterializedViewColumnOrderEndToEnd(t *testing.T) {
	root := t.TempDir()
	left := filepath.Join(root, "left.hcl")
	right := filepath.Join(root, "right.hcl")
	require.NoError(t, os.WriteFile(left, []byte(materializedViewOrderHCL("a", "b")), 0o600))
	require.NoError(t, os.WriteFile(right, []byte(materializedViewOrderHCL("b", "a")), 0o600))

	text, err := runDiffColumnOrderCLI(left, right)
	require.NoError(t, err, string(text))
	assert.Contains(t, string(text), "UNSAFE: materialized view column order changed; recreating the view is required")
	assert.Contains(t, string(text), "~ column_order: a, b -> b, a")
	assert.NotContains(t, string(text), "~ columns changed")

	structured, err := runDiffColumnOrderCLI(left, right, "-format", "json")
	require.NoError(t, err, string(structured))
	var doc hclload.DiffJSON
	require.NoError(t, json.Unmarshal(structured, &doc), string(structured))
	require.Len(t, doc.Objects, 1)
	assert.Equal(t, []hclload.FieldChange{{
		Field: "column_order", Change: "modify", Old: "a, b", New: "b, a",
	}}, doc.Objects[0].Changes)
	assert.True(t, doc.Objects[0].Unsafe)
	assert.Equal(t, "materialized view column order changed; recreating the view is required", doc.Objects[0].UnsafeReason)
	assert.Empty(t, doc.Operations)

	ignored, err := runDiffColumnOrderCLI(left, right, "-ignore-column-order")
	require.NoError(t, err, string(ignored))
	assert.Equal(t, "no differences\n", string(ignored))
}

func materializedViewOrderHCL(first, second string) string {
	return fmt.Sprintf(`database "posthog" {
  table "destination" {
    column "a" { type = "UInt8" }
    column "b" { type = "UInt8" }
    engine "log" {}
  }
  materialized_view "events_mv" {
    to_table = "destination"
    query    = "SELECT a, b FROM source"
    column %q { type = "UInt8" }
    column %q { type = "UInt8" }
  }
}
`, first, second)
}

func runDiffColumnOrderCLI(left, right string, extra ...string) ([]byte, error) {
	args := []string{"-test.run=^TestDiffColumnOrderCLIProcess$", "--", "-left", left, "-right", right}
	args = append(args, extra...)
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "HCLEXP_DIFF_COLUMN_ORDER_HELPER=1")
	return cmd.Output()
}
