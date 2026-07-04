# `hclexp version` — build/version identification

Status: design approved 2026-07-05; implementation in progress.

## Goal

`hclexp version` (plus `-version` / `--version`) prints which build of the
binary is running: version string, commit SHA, build time, and Go runtime
info. Every build path — `just build`, plain `go build`, the published
Docker images — prints something truthful; no path prints empty values.

## Constraints discovered

- The repo has **no git tags**, so there is no semantic version to print
  today. The version string is `git describe --tags --always --dirty`:
  a short SHA (e.g. `deff440-dirty`) now, and automatically `v0.3.0` /
  `v0.3.0-4-gdeff440` once tags exist. No bump chore, no policy needed.
- `.dockerignore` excludes `.git`, so the published images cannot
  self-discover VCS info. Docker builds get the values via
  `--build-arg` → `-ldflags -X` injection from `publish.yml`.
- A plain `go build` of a git checkout embeds `vcs.revision`, `vcs.time`
  and `vcs.modified` in the binary (`debug.ReadBuildInfo`). That is the
  fallback when ldflags were not set, so dev builds still identify
  themselves.

## Design

### Approach

Hybrid stamping (the standard Go CLI pattern):

1. Package-level vars in `cmd/hclexp`, set via
   `-ldflags "-X main.version=… -X main.commit=… -X main.buildTime=…"`
   by the builds that know the values (justfile, Dockerfile+publish.yml).
2. `debug.ReadBuildInfo()` fallback for unstamped builds
   (`go build`, `go run`, `go install …@sha`).

Rejected alternatives: pure ldflags (plain `go build` would print "dev"
despite the info being embedded); pure ReadBuildInfo (published Docker
images would print "unknown" because `.git` is excluded from the build
context, and shipping `.git` into the context is a bad cache/size trade).

### CLI surface

- `version` subcommand in `main()`'s dispatch switch.
- `-version` / `--version` aliases, matched **before** the
  `strings.HasPrefix(os.Args[1], "-")` fallback that routes flags into
  `runLoad` (which would otherwise die on the unknown flag).
- One new line in `usage()`.

Output of a stamped build:

```
hclexp deff440-dirty
  commit: deff440c221…full-sha (modified)
  built:  2026-07-05T09:12:44Z
  go:     go1.26.0 darwin/arm64
```

Fallback build (no ldflags): version falls back to the short
`vcs.revision` SHA (first 12 chars, Go pseudo-version convention) plus
`-dirty` when `vcs.modified`; the `built:` line
shows `vcs.time` labeled `(commit time)` because build time is unknowable
there; `dev` when no VCS info exists at all (e.g. `-buildvcs=false`).
No `-json` output (YAGNI).

### Code

- `cmd/hclexp/version.go` — follows the one-file-per-command pattern:
  - `var version, commit, buildTime string` (ldflags targets).
  - A pure formatting function taking those values plus a
    `*debug.BuildInfo`, so tests are deterministic.
  - `runVersion` writes the formatted string to stdout.
  - Dirty flag is not double-appended when the stamped `version` already
    carries `-dirty` from git describe.
- `cmd/hclexp/version_test.go` — stamped-values-win, fallback path,
  dirty handling, `dev` degradation.

### Build stamping

- **justfile** `build` recipe (ldflags computed in-recipe so other
  recipes don't run git):

  ```
  go build -ldflags "-X main.version=$(git describe --tags --always --dirty) -X main.commit=$(git rev-parse HEAD) -X main.buildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" -o hclexp ./cmd/hclexp
  ```

- **Dockerfile** build stage: `ARG VERSION`, `ARG COMMIT`,
  `ARG BUILD_TIME` (empty defaults), folded into the existing
  `-ldflags="-s -w …"`. A bare `docker build .` passes empty values,
  which degrade through the fallback chain to a clean `hclexp dev`
  report instead of printing `unknown` placeholders.
- **publish.yml**: checkout gains `fetch-depth: 0` (git describe needs
  tag history; repo is small), one step computes `version` and
  `build_time` outputs, both `build-push-action` steps get
  `build-args:` `VERSION`, `COMMIT=${{ github.sha }}`, `BUILD_TIME`.
- **ci.yml** untouched — it ships no artifacts.

### Error handling

Stamping can never fail the binary or the build: absent values degrade
through the fallback chain (`ldflags → build info → "dev"`), never to
empty output or a crash.

### Out of scope

- `-json` output.
- Surfacing the version in the `web` UI or in introspect dump headers.
- A release-tagging policy (the design auto-upgrades when tags appear).

## Implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `hclexp version` / `-version` / `--version` prints version,
commit, build time, and Go runtime info, truthfully on every build path.

**Architecture:** Package-main vars stamped via `-ldflags -X` by the
builds that know the values (justfile, Dockerfile via publish.yml
build-args); a pure `formatVersion` function degrades missing values
through `debug.ReadBuildInfo()` VCS settings down to `"dev"`.

**Tech Stack:** Go 1.26 stdlib (`runtime/debug`), testify, just,
Docker buildx, GitHub Actions.

### Global constraints

- `go.mod`: never change the module name (`github.com/posthog/chschema`)
  or the go version.
- Tests use testify (`assert`/`require`), matching `cmd/hclexp` style.
- ldflags var names are exactly `main.version`, `main.commit`,
  `main.buildTime` — the contract between Tasks 1–4.
- No comments that restate the code; comment only non-obvious intent.
- Code must be `gofmt -s` clean; CI also runs `go vet` + golangci-lint.
- Unit tests run with `go test ./cmd/... ./internal/... -v`; snapshot
  suite with `go test ./test -v`.

---

### Task 1: `version` subcommand — formatting, fallback, dispatch

**Files:**
- Create: `cmd/hclexp/version.go`
- Create: `cmd/hclexp/version_test.go`
- Modify: `cmd/hclexp/hclexp.go` (dispatch switch in `main()`, `usage()`)
- Modify: `cmd/hclexp/hclexp_test.go` (`TestUsage` command list)

**Interfaces:**
- Consumes: nothing new.
- Produces: `formatVersion(version, commit, buildTime string, bi *debug.BuildInfo) string`;
  `runVersion(w io.Writer)`; ldflags targets `main.version`,
  `main.commit`, `main.buildTime` (Tasks 2–4 stamp exactly these).

- [ ] **Step 1: Write the failing tests** — `cmd/hclexp/version_test.go`:

```go
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
```

- [ ] **Step 2: Run tests, verify they fail**

Run: `go test ./cmd/hclexp -run 'TestFormatVersion|TestRunVersion' -v`
Expected: compile error — `undefined: formatVersion`, `undefined: runVersion`.

- [ ] **Step 3: Implement** — `cmd/hclexp/version.go`:

```go
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
```

- [ ] **Step 4: Run tests, verify they pass**

Run: `go test ./cmd/hclexp -run 'TestFormatVersion|TestRunVersion' -v`
Expected: all 5 PASS.

- [ ] **Step 5: Wire dispatch and usage** — in `cmd/hclexp/hclexp.go`,
add to the `switch os.Args[1]` (after the `github-token` case; must be
inside the switch so `-version`/`--version` never reach the
`strings.HasPrefix(os.Args[1], "-")` fallback that routes flags into
`runLoad`):

```go
	case "version", "-version", "--version":
		runVersion(os.Stdout)
		return
```

In `usage()`, before the `help` line:

```
  version      print the hclexp build version, commit and build time
```

In `TestUsage` (`cmd/hclexp/hclexp_test.go`), extend the command list:

```go
	for _, cmd := range []string{"introspect", "diff", "validate", "drift", "load", "version"} {
```

- [ ] **Step 6: Run the package tests**

Run: `go test ./cmd/... -v`
Expected: PASS (including `TestUsage`).

- [ ] **Step 7: Commit**

```bash
git add cmd/hclexp/version.go cmd/hclexp/version_test.go cmd/hclexp/hclexp.go cmd/hclexp/hclexp_test.go
git commit -m "cli: add hclexp version subcommand

Prints version, commit, build time and Go runtime info. Values are
stamped via -ldflags -X main.{version,commit,buildTime}; unstamped
builds fall back to debug.ReadBuildInfo VCS settings, then to \"dev\".
-version/--version are aliases handled in the dispatch switch so they
don't fall through to the default load path.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: justfile — stamp local builds

**Files:**
- Modify: `justfile` (`build` recipe only)

**Interfaces:**
- Consumes: ldflags targets `main.version`, `main.commit`,
  `main.buildTime` from Task 1.
- Produces: `just build` emits a stamped `./hclexp`.

- [ ] **Step 1: Edit the `build` recipe** (ldflags computed in-recipe so
other recipes never run git):

```make
# Build all packages and the hclexp binary, matching CI's build job
build:
    go build ./...
    go build -ldflags "-X main.version=$(git describe --tags --always --dirty) -X main.commit=$(git rev-parse HEAD) -X main.buildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" -o hclexp ./cmd/hclexp
```

- [ ] **Step 2: Verify the stamped binary**

Run: `just build && ./hclexp version && ./hclexp -version && ./hclexp --version`
Expected: three identical reports; first line `hclexp <short-sha>` (or
`<short-sha>-dirty`), `commit:` full SHA, `built:` current UTC time
(no `(commit time)` note), `go:` line. No git tags exist yet, so
`git describe --tags --always --dirty` yields the short SHA form.

- [ ] **Step 3: Commit**

```bash
git add justfile
git commit -m "build: stamp version metadata in just build

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Dockerfile — build args → ldflags

**Files:**
- Modify: `Dockerfile` (build stage only)

**Interfaces:**
- Consumes: ldflags targets from Task 1.
- Produces: build args `VERSION`, `COMMIT`, `BUILD_TIME` (Task 4 passes
  exactly these names).

- [ ] **Step 1: Add ARGs and extend ldflags.** ARGs go after `COPY . .`
(an ARG change then only invalidates the final RUN layer, keeping the
dep and source layers cached). Replace the build stage's final RUN:

```dockerfile
COPY . .

# Version metadata injected by publish.yml; empty defaults let a bare
# `docker build .` degrade to the binary's "dev" report.
ARG VERSION
ARG COMMIT
ARG BUILD_TIME

# Static binary (no CGO) so it runs on distroless without a libc.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.buildTime=${BUILD_TIME}" \
    -o /out/hclexp ./cmd/hclexp
```

- [ ] **Step 2: Verify the ldflags string compiles** (no Docker daemon
required — same flags, local toolchain):

Run: `go build -trimpath -ldflags="-s -w -X main.version=vtest -X main.commit=ctest -X main.buildTime=ttest" -o /tmp/hclexp-ldflags-check ./cmd/hclexp && /tmp/hclexp-ldflags-check version && rm /tmp/hclexp-ldflags-check`
Expected: report shows `hclexp vtest`, `commit: ctest`, `built:  ttest`.

- [ ] **Step 3 (if a Docker daemon is available): smoke-build the build
stage**

Run: `docker build --target build --build-arg VERSION=vsmoke --build-arg COMMIT=csmoke --build-arg BUILD_TIME=tsmoke .`
Expected: builds successfully. If no daemon is available, note that in
the task report — publish.yml exercises this path on push.

- [ ] **Step 4: Commit**

```bash
git add Dockerfile
git commit -m "build: accept version metadata build args in Dockerfile

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: publish.yml — compute and pass build args

**Files:**
- Modify: `.github/workflows/publish.yml`

**Interfaces:**
- Consumes: build args `VERSION`, `COMMIT`, `BUILD_TIME` from Task 3.
- Produces: published images that report real version metadata.

- [ ] **Step 1: Give checkout tag history** — the `Checkout` step gains:

```yaml
    - name: Checkout
      uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd # v6.0.2
      with:
        fetch-depth: 0
```

(`git describe --tags` needs tag history; actions/checkout defaults to
a depth-1, tagless clone. The repo is small.)

- [ ] **Step 2: Compute version metadata** — insert after `Checkout`,
before the QEMU step:

```yaml
    - name: Version metadata
      id: ver
      run: |
        echo "version=$(git describe --tags --always --dirty)" >> "$GITHUB_OUTPUT"
        echo "build_time=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$GITHUB_OUTPUT"
```

- [ ] **Step 3: Pass build-args to BOTH image builds** — add to the
`with:` block of *both* `docker/build-push-action` steps (main image and
ops image):

```yaml
        build-args: |
          VERSION=${{ steps.ver.outputs.version }}
          COMMIT=${{ github.sha }}
          BUILD_TIME=${{ steps.ver.outputs.build_time }}
```

- [ ] **Step 4: Validate the workflow file**

Run: `actionlint .github/workflows/publish.yml` if actionlint is
installed; otherwise `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/publish.yml'))"`.
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/publish.yml
git commit -m "ci: stamp published images with version metadata

fetch-depth: 0 so git describe sees tags; VERSION/COMMIT/BUILD_TIME
build-args flow into both the distroless and ops image builds.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Final verification sweep

**Files:** none (verification only; update this plan doc if anything
shifted).

- [ ] **Step 1: Full test suites**

Run: `go test ./cmd/... ./internal/... -v` and `go test ./test -v`
Expected: PASS.

- [ ] **Step 2: Lint gate**

Run: `test -z "$(gofmt -s -l .)" && go vet ./...`
Expected: no output, exit 0.

- [ ] **Step 3: End-to-end binary check**

Run: `just build && ./hclexp version`
Expected: stamped report as in Task 2; also `./hclexp help | grep -F "version"`
lists the subcommand.

Docs note: CLAUDE.md has no per-subcommand list (usage() is the
authoritative command reference), so no CLAUDE.md change is needed;
the earlier design sketch's "CLAUDE.md mention" is dropped as YAGNI.
