package main

import (
	"io"
	"os"
	"testing"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStdoutTarget(t *testing.T) {
	assert.True(t, stdoutTarget(""))
	assert.True(t, stdoutTarget("-"))
	assert.False(t, stdoutTarget("schema.hcl"))
	assert.False(t, stdoutTarget("./out"))
}

// TestWriteIntrospected_DashIsStdout: -out "-" writes to stdout and does not
// create a file literally named "-".
func TestWriteIntrospected_DashIsStdout(t *testing.T) {
	dir := t.TempDir()
	cwd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	defer func() { _ = os.Chdir(cwd) }()

	r, w, err := os.Pipe()
	require.NoError(t, err)
	orig := os.Stdout
	os.Stdout = w
	werr := writeIntrospected("-", &hclload.Schema{Databases: []hclload.DatabaseSpec{{Name: "posthog"}}})
	require.NoError(t, w.Close())
	os.Stdout = orig
	require.NoError(t, werr)

	out, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Contains(t, string(out), `database "posthog"`)

	_, statErr := os.Stat("-")
	assert.True(t, os.IsNotExist(statErr), `must not create a file named "-"`)
}
