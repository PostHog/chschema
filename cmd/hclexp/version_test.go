package main

import (
	"bytes"
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func goLine() string {
	return fmt.Sprintf("  go:     %s %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
}

func buildInfo(settings map[string]string) *debug.BuildInfo {
	bi := &debug.BuildInfo{}
	for k, v := range settings {
		bi.Settings = append(bi.Settings, debug.BuildSetting{Key: k, Value: v})
	}
	return bi
}

func TestFormatVersion_StampedValuesWin(t *testing.T) {
	bi := buildInfo(map[string]string{
		"vcs.revision": "ffffffffffffffffffffffffffffffffffffffff",
		"vcs.time":     "2026-01-01T00:00:00Z",
		"vcs.modified": "false",
	})
	got := formatVersion("v1.2.3", "deff440c221aa", "2026-07-05T10:00:00Z", bi)
	want := "hclexp v1.2.3\n" +
		"  commit: deff440c221aa\n" +
		"  built:  2026-07-05T10:00:00Z\n" +
		goLine()
	assert.Equal(t, want, got)
}

func TestFormatVersion_FallbackToBuildInfo(t *testing.T) {
	bi := buildInfo(map[string]string{
		"vcs.revision": "deff440c221aabbccddeeff00112233445566aabb",
		"vcs.time":     "2026-07-01T08:30:00Z",
		"vcs.modified": "true",
	})
	got := formatVersion("", "", "", bi)
	want := "hclexp deff440c221a-dirty\n" +
		"  commit: deff440c221aabbccddeeff00112233445566aabb (modified)\n" +
		"  built:  2026-07-01T08:30:00Z (commit time)\n" +
		goLine()
	assert.Equal(t, want, got)
}

func TestFormatVersion_StampedDirtyNotDoubled(t *testing.T) {
	// justfile stamps a describe string already ending in -dirty while
	// build info also reports vcs.modified; the version line must not
	// become "…-dirty-dirty".
	bi := buildInfo(map[string]string{
		"vcs.revision": "deff440c221aabbccddeeff00112233445566aabb",
		"vcs.modified": "true",
	})
	got := formatVersion("deff440-dirty", "", "", bi)
	assert.True(t, strings.HasPrefix(got, "hclexp deff440-dirty\n"))
	assert.NotContains(t, got, "dirty-dirty")
	assert.Contains(t, got, "(modified)")
}

func TestFormatVersion_NoInfoAtAll(t *testing.T) {
	want := "hclexp dev\n" + goLine()
	assert.Equal(t, want, formatVersion("", "", "", nil))
	// -buildvcs=false: build info present but without vcs settings.
	assert.Equal(t, want, formatVersion("", "", "", &debug.BuildInfo{}))
}

func TestRunVersion_WritesReport(t *testing.T) {
	var buf bytes.Buffer
	runVersion(&buf)
	assert.True(t, strings.HasPrefix(buf.String(), "hclexp "))
	assert.Contains(t, buf.String(), "  go:     ")
}
