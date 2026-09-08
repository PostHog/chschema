package main

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveHCLExpConfigPrecedence(t *testing.T) {
	home := filepath.Join(t.TempDir(), "home")
	workdir := filepath.Join(t.TempDir(), "project")
	writeFileT(t, filepath.Join(home, hclexpConfigFilename), `
global { ignore_column_order = true }
plan   { ignore_column_order = false }
`)
	writeFileT(t, filepath.Join(workdir, hclexpConfigFilename), `
global { ignore_column_order = false }
diff   { ignore_column_order = true }
web    { ignore_column_order = true }
`)

	tests := []struct {
		command string
		want    bool
	}{
		{command: "diff", want: true},
		{command: "drift", want: false},
		{command: "plan", want: false},
		{command: "web", want: true},
	}
	for _, tc := range tests {
		t.Run(tc.command, func(t *testing.T) {
			resolved, err := resolveHCLExpConfig(home, workdir, tc.command)
			require.NoError(t, err)
			require.NotNil(t, resolved.IgnoreColumnOrder)
			assert.Equal(t, tc.want, *resolved.IgnoreColumnOrder)
			assert.Equal(t, []string{
				filepath.Join(home, hclexpConfigFilename),
				filepath.Join(workdir, hclexpConfigFilename),
			}, resolved.Files)
		})
	}
}

func TestResolveHCLExpConfigMissingAndDeduplicated(t *testing.T) {
	dir := t.TempDir()
	resolved, err := resolveHCLExpConfig(dir, dir, "diff")
	require.NoError(t, err)
	assert.Nil(t, resolved.IgnoreColumnOrder)
	assert.Empty(t, resolved.Files)

	writeFileT(t, filepath.Join(dir, hclexpConfigFilename), `global { ignore_column_order = true }`)
	resolved, err = resolveHCLExpConfig(dir, dir, "diff")
	require.NoError(t, err)
	require.NotNil(t, resolved.IgnoreColumnOrder)
	assert.True(t, *resolved.IgnoreColumnOrder)
	assert.Equal(t, []string{filepath.Join(dir, hclexpConfigFilename)}, resolved.Files)
}

func TestResolveHCLExpConfigRejectsInvalidAndUnknownSettings(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "invalid syntax", body: `global { ignore_column_order = }`, want: "Invalid expression"},
		{name: "unknown setting", body: `global { ignore_column_orders = true }`, want: "Unsupported argument"},
		{name: "unknown command", body: `validate { ignore_column_order = true }`, want: "Unsupported block type"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, hclexpConfigFilename)
			writeFileT(t, path, tc.body)
			_, err := resolveHCLExpConfig("", dir, "diff")
			require.Error(t, err)
			assert.Contains(t, err.Error(), path)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestEffectiveIgnoreColumnOrderCLIWinsBothWays(t *testing.T) {
	configuredTrue := true
	configuredFalse := false
	assert.False(t, effectiveIgnoreColumnOrder(hclexpConfigResolution{IgnoreColumnOrder: &configuredTrue}, true, false))
	assert.True(t, effectiveIgnoreColumnOrder(hclexpConfigResolution{IgnoreColumnOrder: &configuredFalse}, true, true))
	assert.True(t, effectiveIgnoreColumnOrder(hclexpConfigResolution{IgnoreColumnOrder: &configuredTrue}, false, false))
	assert.False(t, effectiveIgnoreColumnOrder(hclexpConfigResolution{}, false, false))
}

func TestRenderHCLExpConfigInfo(t *testing.T) {
	var out bytes.Buffer
	files := []string{"/home/alice/.hclexp.config", "/work/project/.hclexp.config"}
	renderHCLExpConfigInfoIfTerminal(&out, files, false)
	assert.Empty(t, out.String())
	renderHCLExpConfigInfoIfTerminal(&out, files, true)
	assert.Equal(t, "hclexp: loaded config /home/alice/.hclexp.config\nhclexp: loaded config /work/project/.hclexp.config\n", out.String())
}
