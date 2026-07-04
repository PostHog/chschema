package main

import (
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
	"strings"
)

// Set at build time via
// -ldflags "-X main.version=… -X main.commit=… -X main.buildTime=…"
// (justfile build recipe, Dockerfile via publish.yml build-args).
// Empty values fall back to the VCS metadata `go build` embeds in the
// binary, so unstamped dev builds still identify themselves.
var (
	version   string // git describe --tags --always --dirty
	commit    string // full git SHA
	buildTime string // RFC3339 UTC
)

// runVersion implements `hclexp version` (and -version/--version).
func runVersion(w io.Writer) {
	bi, _ := debug.ReadBuildInfo()
	fmt.Fprint(w, formatVersion(version, commit, buildTime, bi))
}

// formatVersion renders the version report. Stamped (ldflags) values
// win; empty ones degrade to build-info VCS settings, then to "dev".
// Never returns an empty string.
func formatVersion(version, commit, buildTime string, bi *debug.BuildInfo) string {
	var revision, commitTime string
	dirty := false
	if bi != nil {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				revision = s.Value
			case "vcs.time":
				commitTime = s.Value
			case "vcs.modified":
				dirty = s.Value == "true"
			}
		}
	}

	if commit == "" {
		commit = revision
	}

	built := buildTime
	builtNote := ""
	if built == "" && commitTime != "" {
		built = commitTime
		// vcs.time is when the commit was made, not when the binary
		// was built — label it so the report stays honest.
		builtNote = " (commit time)"
	}

	if version == "" {
		if revision != "" {
			version = revision
			if len(version) > 12 {
				version = version[:12]
			}
			if dirty {
				version += "-dirty"
			}
		} else {
			version = "dev"
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "hclexp %s\n", version)
	if commit != "" {
		modified := ""
		if dirty {
			modified = " (modified)"
		}
		fmt.Fprintf(&b, "  commit: %s%s\n", commit, modified)
	}
	if built != "" {
		fmt.Fprintf(&b, "  built:  %s%s\n", built, builtNote)
	}
	fmt.Fprintf(&b, "  go:     %s %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
	return b.String()
}
