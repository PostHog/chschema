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

- **Dockerfile** build stage: `ARG VERSION=dev`, `ARG COMMIT=unknown`,
  `ARG BUILD_TIME=unknown`, folded into the existing
  `-ldflags="-s -w …"`. Defaults keep a bare `docker build .` working.
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

1. **`cmd/hclexp/version.go` + `version_test.go`** (TDD: tests first)
   - Pure `formatVersion` function; table-driven tests for: all three
     vars stamped; stamped version already `-dirty`; empty vars with
     BuildInfo carrying `vcs.*` settings; empty vars + nil/empty
     BuildInfo → `dev`.
   - `runVersion` + dispatch wiring in `main()` + `usage()` line.
   - CLI-level test alongside the existing `hclexp_test.go` patterns.
2. **justfile** — stamp ldflags in the `build` recipe; verify
   `just build && ./hclexp version` prints a SHA-based version.
3. **Dockerfile** — ARGs + ldflags; verify defaults compile (build args
   optional).
4. **publish.yml** — fetch-depth, version step, build-args on both
   image builds.
5. **Docs** — CLAUDE.md command list mention; this plan updated if the
   design shifts during implementation.

Verification: `go test ./cmd/... ./internal/... ./test`, `just build`,
`./hclexp version`, `./hclexp -version`, `./hclexp --version`, and a
local `docker build` smoke check of the build stage.
