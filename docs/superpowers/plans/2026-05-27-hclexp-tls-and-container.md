# hclexp TLS Support & Container Image — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add TLS connection support to `hclexp`, package the binary as a minimal container image, and publish that image to PostHog's ECR on every `main` push and Git tag.

**Architecture:** Three deliverables, two PRs.
- **PR 1 (TLS):** Plumb `Secure`/`TLSSkipVerify` through `config.ClickHouseConfig` (env + flag + URL query). Wire them into `clickhouse.Options.TLS` using `crypto/tls`. Zero behavior change when both are false.
- **PR 2 (image + publish):** Multi-stage `Dockerfile` (`golang:1.26-alpine` → `gcr.io/distroless/static-debian12:nonroot`), `.dockerignore`, CI smoke-test job, and a separate `publish.yml` workflow that builds linux/{amd64,arm64} and pushes to ECR `795637471508.dkr.ecr.us-east-1.amazonaws.com/posthog/chschema` via OIDC.

**Tech Stack:** Go 1.26, `clickhouse-go/v2`, `crypto/tls` (stdlib), Docker BuildKit, GitHub Actions, AWS OIDC.

**Spec:** [`docs/superpowers/specs/2026-05-27-hclexp-tls-and-container.md`](../specs/2026-05-27-hclexp-tls-and-container.md)

**File Map:**
- Modify: `config/clickhouse.go` (new fields, env reads, TLS in `NewConnection`)
- Create: `config/clickhouse_test.go` (unit coverage for the new wiring)
- Modify: `cmd/hclexp/hclexp.go` (CLI flags on `runIntrospect`; URL query parsing in `parseClickHouseURI`)
- Modify: `cmd/hclexp/hclexp_test.go` (URL query param coverage)
- Modify: `docs/README.hcl.md` (TLS docs)
- Create: `Dockerfile`
- Create: `.dockerignore`
- Modify: `.github/workflows/ci.yml` (docker smoke job)
- Create: `.github/workflows/publish.yml`
- Modify: `docker-compose.yml` (optional second TLS listener — kept off the CI path)
- Modify: `justfile` (optional `test-live-tls` recipe)

---

## PR 1 — TLS support

### Task 1: Extend `ClickHouseConfig` + env parsing

**Files:**
- Modify: `config/clickhouse.go`
- Create: `config/clickhouse_test.go`

- [ ] **Step 1: Write the failing test for config + env parsing**

Create `config/clickhouse_test.go`:

```go
package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetDefaultConfig_TLSEnvVars(t *testing.T) {
	t.Run("defaults are false when env unset", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_SECURE", "")
		t.Setenv("CLICKHOUSE_TLS_SKIP_VERIFY", "")
		cfg := GetDefaultConfig()
		require.False(t, cfg.Secure)
		require.False(t, cfg.TLSSkipVerify)
	})

	t.Run("CLICKHOUSE_SECURE=true enables TLS", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_SECURE", "true")
		cfg := GetDefaultConfig()
		require.True(t, cfg.Secure)
	})

	t.Run("CLICKHOUSE_SECURE=1 enables TLS", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_SECURE", "1")
		cfg := GetDefaultConfig()
		require.True(t, cfg.Secure)
	})

	t.Run("CLICKHOUSE_TLS_SKIP_VERIFY=true sets the skip flag", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_TLS_SKIP_VERIFY", "true")
		cfg := GetDefaultConfig()
		require.True(t, cfg.TLSSkipVerify)
	})

	t.Run("garbage values fall back to false", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_SECURE", "banana")
		cfg := GetDefaultConfig()
		require.False(t, cfg.Secure)
	})
}

// guard against accidental import shadowing
var _ = os.Getenv
```

- [ ] **Step 2: Run the test to verify it fails**

```
go test ./config -run TestGetDefaultConfig_TLSEnvVars -v
```

Expected: FAIL — `cfg.Secure undefined`, `cfg.TLSSkipVerify undefined`.

- [ ] **Step 3: Implement the new fields + env parser**

Edit `config/clickhouse.go`:

```go
package config

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ClickHouseConfig holds the connection configuration for ClickHouse.
type ClickHouseConfig struct {
	Host          string
	Port          int
	Database      string
	User          string
	Password      string
	Secure        bool // connect over native TLS (port 9440 by convention)
	TLSSkipVerify bool // skip server certificate verification (internal-CA clusters)
}

// GetDefaultConfig returns default configuration based on environment.
func GetDefaultConfig() ClickHouseConfig {
	return ClickHouseConfig{
		Host:          getEnvOrDefault("CLICKHOUSE_HOST", "localhost"),
		Port:          getEnvIntOrDefault("CLICKHOUSE_PORT", 9000),
		Database:      getEnvOrDefault("CLICKHOUSE_DB", "migration_test"),
		User:          getEnvOrDefault("CLICKHOUSE_USER", "user1"),
		Password:      getEnvOrDefault("CLICKHOUSE_PASSWORD", "pass1"),
		Secure:        getEnvBoolOrDefault("CLICKHOUSE_SECURE", false),
		TLSSkipVerify: getEnvBoolOrDefault("CLICKHOUSE_TLS_SKIP_VERIFY", false),
	}
}

func getEnvBoolOrDefault(key string, defaultValue bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return defaultValue
	}
}
```

(Keep the existing `getEnvOrDefault` and `getEnvIntOrDefault` helpers below.)

- [ ] **Step 4: Run the test to verify it passes**

```
go test ./config -run TestGetDefaultConfig_TLSEnvVars -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```
git add config/clickhouse.go config/clickhouse_test.go
git commit -m "feat(config): add Secure/TLSSkipVerify with env-var parsing

CLICKHOUSE_SECURE and CLICKHOUSE_TLS_SKIP_VERIFY drive the new
fields. Defaults stay false so behavior is unchanged for existing
invocations.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Wire TLS into `NewConnection`

**Files:**
- Modify: `config/clickhouse.go`
- Modify: `config/clickhouse_test.go`

- [ ] **Step 1: Write the failing test for the options builder**

Add to `config/clickhouse_test.go`:

```go
func TestBuildOptions_TLS(t *testing.T) {
	t.Run("plaintext leaves TLS nil", func(t *testing.T) {
		opts := buildOptions(ClickHouseConfig{Host: "h", Port: 9000, User: "u", Database: "d"})
		require.Nil(t, opts.TLS)
	})

	t.Run("Secure=true sets verifying TLS config", func(t *testing.T) {
		opts := buildOptions(ClickHouseConfig{Host: "h", Port: 9440, User: "u", Database: "d", Secure: true})
		require.NotNil(t, opts.TLS)
		require.False(t, opts.TLS.InsecureSkipVerify)
	})

	t.Run("Secure+TLSSkipVerify sets InsecureSkipVerify", func(t *testing.T) {
		opts := buildOptions(ClickHouseConfig{Host: "h", Port: 9440, User: "u", Database: "d", Secure: true, TLSSkipVerify: true})
		require.NotNil(t, opts.TLS)
		require.True(t, opts.TLS.InsecureSkipVerify)
	})

	t.Run("TLSSkipVerify alone is ignored without Secure", func(t *testing.T) {
		// NewConnection rejects this combo at the call site; the builder
		// itself just respects Secure as the gate.
		opts := buildOptions(ClickHouseConfig{Host: "h", Port: 9000, User: "u", Database: "d", TLSSkipVerify: true})
		require.Nil(t, opts.TLS)
	})
}
```

- [ ] **Step 2: Run the test to verify it fails**

```
go test ./config -run TestBuildOptions_TLS -v
```

Expected: FAIL — `buildOptions` undefined.

- [ ] **Step 3: Extract `buildOptions` and wire TLS in**

Replace `NewConnection` in `config/clickhouse.go`:

```go
// buildOptions translates a ClickHouseConfig into clickhouse-go options.
// Extracted so it can be unit-tested without dialing.
func buildOptions(cfg ClickHouseConfig) *clickhouse.Options {
	opts := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
	}
	if cfg.Secure {
		opts.TLS = &tls.Config{
			InsecureSkipVerify: cfg.TLSSkipVerify, //nolint:gosec // user-opted-in via -tls-skip-verify
		}
	}
	return opts
}

// NewConnection creates a new ClickHouse connection from the config.
func NewConnection(cfg ClickHouseConfig) (driver.Conn, error) {
	conn, err := clickhouse.Open(buildOptions(cfg))
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}
	return conn, nil
}
```

- [ ] **Step 4: Run the test to verify it passes**

```
go test ./config -v
```

Expected: PASS for all `TestBuildOptions_TLS` subtests + `TestGetDefaultConfig_TLSEnvVars` still passes.

- [ ] **Step 5: Run the broader unit suite to confirm no regression**

```
go test ./internal/... ./config -v 2>&1 | tail -20
```

Expected: green — no live-connection tests run.

- [ ] **Step 6: Commit**

```
git add config/clickhouse.go config/clickhouse_test.go
git commit -m "feat(config): set TLS options on the clickhouse-go client when Secure

Extract buildOptions so the TLS branch can be unit-tested without
dialing. Plaintext behavior is preserved: options.TLS stays nil
unless cfg.Secure is true.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: CLI flags on `runIntrospect`

**Files:**
- Modify: `cmd/hclexp/hclexp.go`
- Modify: `cmd/hclexp/hclexp_test.go`

- [ ] **Step 1: Write the failing test for flag-driven config**

Add to `cmd/hclexp/hclexp_test.go`:

```go
func TestApplyTLSFlags(t *testing.T) {
	t.Run("both off leaves cfg untouched", func(t *testing.T) {
		cfg := config.ClickHouseConfig{}
		require.NoError(t, applyTLSFlags(&cfg, false, false))
		require.False(t, cfg.Secure)
		require.False(t, cfg.TLSSkipVerify)
	})

	t.Run("secure on, skip-verify off", func(t *testing.T) {
		cfg := config.ClickHouseConfig{}
		require.NoError(t, applyTLSFlags(&cfg, true, false))
		require.True(t, cfg.Secure)
		require.False(t, cfg.TLSSkipVerify)
	})

	t.Run("secure on, skip-verify on", func(t *testing.T) {
		cfg := config.ClickHouseConfig{}
		require.NoError(t, applyTLSFlags(&cfg, true, true))
		require.True(t, cfg.Secure)
		require.True(t, cfg.TLSSkipVerify)
	})

	t.Run("skip-verify without secure is rejected", func(t *testing.T) {
		cfg := config.ClickHouseConfig{}
		err := applyTLSFlags(&cfg, false, true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "-tls-skip-verify requires -secure")
	})
}
```

- [ ] **Step 2: Run the test to verify it fails**

```
go test ./cmd/hclexp -run TestApplyTLSFlags -v
```

Expected: FAIL — `applyTLSFlags` undefined.

- [ ] **Step 3: Add the helper and wire the flags into `runIntrospect`**

In `cmd/hclexp/hclexp.go`, add the helper near `parseClickHouseURI`:

```go
// applyTLSFlags merges the two TLS toggles into cfg, validating that
// -tls-skip-verify is only set together with -secure.
func applyTLSFlags(cfg *config.ClickHouseConfig, secure, skipVerify bool) error {
	if skipVerify && !secure {
		return fmt.Errorf("-tls-skip-verify requires -secure")
	}
	cfg.Secure = secure
	cfg.TLSSkipVerify = skipVerify
	return nil
}
```

Update `runIntrospect` to declare the flags and apply them. Inside the
`runIntrospect` function, immediately after the existing flag
declarations and before `_ = fs.Parse(args)`, add:

```go
	secure := fs.Bool("secure", cfg.Secure, "connect to ClickHouse over TLS")
	skipVerify := fs.Bool("tls-skip-verify", cfg.TLSSkipVerify, "skip TLS certificate verification (requires -secure)")
```

Then, right after the existing `cfg.Host, cfg.Port, cfg.User, cfg.Password = *host, *port, *user, *password` line, add:

```go
	if err := applyTLSFlags(&cfg, *secure, *skipVerify); err != nil {
		slog.Error("invalid TLS flag combination", "err", err)
		os.Exit(2)
	}
```

- [ ] **Step 4: Run the test to verify it passes**

```
go test ./cmd/hclexp -run TestApplyTLSFlags -v
```

Expected: PASS.

- [ ] **Step 5: Smoke-test the new help output**

```
go build -o /tmp/hclexp ./cmd/hclexp && /tmp/hclexp introspect -h 2>&1 | grep -E -- "-secure|-tls-skip-verify"
```

Expected: both flags shown in usage.

- [ ] **Step 6: Commit**

```
git add cmd/hclexp/hclexp.go cmd/hclexp/hclexp_test.go
git commit -m "feat(hclexp): -secure and -tls-skip-verify flags on introspect

Defaults pick up CLICKHOUSE_SECURE and CLICKHOUSE_TLS_SKIP_VERIFY via
config.GetDefaultConfig. -tls-skip-verify without -secure is rejected
to prevent silent misconfiguration.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: TLS query params on the `clickhouse://` URL

**Files:**
- Modify: `cmd/hclexp/hclexp.go`
- Modify: `cmd/hclexp/hclexp_test.go`

- [ ] **Step 1: Write the failing test**

Add to `cmd/hclexp/hclexp_test.go`:

```go
func TestParseClickHouseURI_TLSQueryParams(t *testing.T) {
	t.Run("plaintext stays plaintext", func(t *testing.T) {
		cfg, _, err := parseClickHouseURI("clickhouse://u:p@h:9000/db")
		require.NoError(t, err)
		require.False(t, cfg.Secure)
		require.False(t, cfg.TLSSkipVerify)
	})

	t.Run("?secure=true enables TLS", func(t *testing.T) {
		cfg, _, err := parseClickHouseURI("clickhouse://u:p@h:9440/db?secure=true")
		require.NoError(t, err)
		require.True(t, cfg.Secure)
		require.False(t, cfg.TLSSkipVerify)
	})

	t.Run("?secure=true&skip-verify=true sets both", func(t *testing.T) {
		cfg, _, err := parseClickHouseURI("clickhouse://u:p@h:9440/db?secure=true&skip-verify=true")
		require.NoError(t, err)
		require.True(t, cfg.Secure)
		require.True(t, cfg.TLSSkipVerify)
	})

	t.Run("?skip-verify=true without secure is rejected", func(t *testing.T) {
		_, _, err := parseClickHouseURI("clickhouse://u:p@h:9440/db?skip-verify=true")
		require.Error(t, err)
		require.Contains(t, err.Error(), "skip-verify requires secure=true")
	})
}
```

- [ ] **Step 2: Run the test to verify it fails**

```
go test ./cmd/hclexp -run TestParseClickHouseURI_TLSQueryParams -v
```

Expected: FAIL — TLS fields aren't read from the URL yet.

- [ ] **Step 3: Read the query params in `parseClickHouseURI`**

In `cmd/hclexp/hclexp.go`, inside `parseClickHouseURI`, just before the final `return cfg, databases, nil`:

```go
	q := u.Query()
	secure := parseBoolQuery(q.Get("secure"))
	skipVerify := parseBoolQuery(q.Get("skip-verify"))
	if skipVerify && !secure {
		return config.ClickHouseConfig{}, nil, fmt.Errorf("skip-verify requires secure=true in clickhouse:// URL")
	}
	cfg.Secure = secure
	cfg.TLSSkipVerify = skipVerify
```

Add this helper next to `parseClickHouseURI`:

```go
// parseBoolQuery interprets a URL query value as a boolean. Empty/unknown
// values map to false; "1"/"true"/"yes"/"on" (case-insensitive) map to true.
func parseBoolQuery(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
```

- [ ] **Step 4: Run the test to verify it passes**

```
go test ./cmd/hclexp -v
```

Expected: all `parseClickHouseURI` tests green (including the existing `TestParseClickHouseURI`).

- [ ] **Step 5: Commit**

```
git add cmd/hclexp/hclexp.go cmd/hclexp/hclexp_test.go
git commit -m "feat(hclexp): accept ?secure and ?skip-verify on clickhouse:// URLs

Lets hclexp diff -left/-right reach a TLS-only cluster without
introducing a new scheme. ?skip-verify=true without ?secure=true is
rejected to mirror the CLI flag validation.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Document TLS support in the README

**Files:**
- Modify: `docs/README.hcl.md`

- [ ] **Step 1: Locate the CLI flag/usage section**

```
grep -n "^##\|introspect\|-host\|-port" docs/README.hcl.md | head -40
```

Identify the section that lists `hclexp introspect` flags. If there is
no explicit flag list, add a new top-level section titled `## TLS /
secure connections` at the end of the file.

- [ ] **Step 2: Add the TLS documentation block**

Append (or insert into the appropriate section) the following content
to `docs/README.hcl.md`:

````markdown
## TLS / secure connections

`hclexp` connects to ClickHouse in plaintext by default. To reach a
TLS-only cluster (e.g. production on port 9440), enable TLS via flags,
environment variables, or query parameters on the `clickhouse://` URL
used by `hclexp diff`.

| Form                          | Enable TLS              | Skip cert verification         |
| ----------------------------- | ----------------------- | ------------------------------ |
| `hclexp introspect` flag      | `-secure`               | `-tls-skip-verify`             |
| Environment variable          | `CLICKHOUSE_SECURE=true`| `CLICKHOUSE_TLS_SKIP_VERIFY=true` |
| `clickhouse://` URL query     | `?secure=true`          | `?skip-verify=true`            |

`-tls-skip-verify` / `?skip-verify=true` is only valid together with
`-secure` / `?secure=true`; passing it alone is rejected. Skip
verification is intended for internal-CA clusters — for public CAs the
default verification path uses the system trust store.

### Examples

Introspect a TLS cluster with a private CA:

```sh
hclexp introspect \
  -host ch.prod.internal -port 9440 -user readonly \
  -secure -tls-skip-verify \
  -database posthog,system \
  -out ./dump
```

Diff a local schema tree against a TLS cluster:

```sh
hclexp diff \
  -left ./schema \
  -right 'clickhouse://ro:secret@ch.prod.internal:9440/posthog?secure=true&skip-verify=true'
```
````

- [ ] **Step 3: Commit**

```
git add docs/README.hcl.md
git commit -m "docs: TLS / secure connections section for hclexp

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: PR 1 verification + push

- [ ] **Step 1: Run the full unit suite**

```
go test ./internal/... ./config ./cmd/... 2>&1 | tail -20
```

Expected: green across the board.

- [ ] **Step 2: Confirm the new flags appear in help**

```
go build -o /tmp/hclexp ./cmd/hclexp && /tmp/hclexp introspect -h
```

Expected: usage text mentions `-secure` and `-tls-skip-verify`.

- [ ] **Step 3: Push the branch**

```
git push -u origin HEAD
```

- [ ] **Step 4: STOP — pause for human PR creation/review of PR 1**

PR 1 is now ready. Do not start PR 2 work until the user gives the
go-ahead (PR 1 may merge first, or PR 2 may proceed on a stacked
branch — the user decides).

---

## PR 2 — Container image + publish workflow

> Resume only after PR 1 is merged or the user explicitly OKs stacked
> work.

### Task 7: `.dockerignore`

**Files:**
- Create: `.dockerignore`

- [ ] **Step 1: Write the file**

Create `.dockerignore`:

```
# VCS + tooling
.git
.gitignore
.github
.claude
.worktrees

# Docs, plans, drafts — not needed in the image build context
docs
*.md

# Build artifacts (locally built binaries, dump output)
chschema
hclexp
logs
*.sql

# Tests' snapshot fixtures (large, irrelevant to the image)
test/snapshots

# Image build inputs themselves
Dockerfile
.dockerignore
```

- [ ] **Step 2: Verify**

```
test -f .dockerignore && head -5 .dockerignore
```

Expected: prints the first lines.

- [ ] **Step 3: Commit**

```
git add .dockerignore
git commit -m "chore: add .dockerignore for hclexp container build

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: `Dockerfile`

**Files:**
- Create: `Dockerfile`

- [ ] **Step 1: Write the Dockerfile**

Create `Dockerfile`:

```dockerfile
# syntax=docker/dockerfile:1.7

# ---- Build stage ----
FROM golang:1.26-alpine AS build
WORKDIR /src

# Cache deps independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binary (no CGO) so it runs on distroless without a libc.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/hclexp ./cmd/hclexp

# ---- Runtime stage ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/hclexp /hclexp
USER nonroot:nonroot
ENTRYPOINT ["/hclexp"]
```

- [ ] **Step 2: Build the image locally**

```
docker build -t hclexp:dev .
```

Expected: image built successfully (final size visible in output).

- [ ] **Step 3: Smoke-test the image**

```
docker run --rm hclexp:dev -help 2>&1 | head -20
```

Expected: hclexp usage text, exit 0.

Also verify subcommand help works (the entrypoint passes args through):

```
docker run --rm hclexp:dev introspect -h 2>&1 | head -5
```

Expected: introspect usage shown.

- [ ] **Step 4: Verify the runtime image is minimal**

```
docker image ls hclexp:dev --format '{{.Size}}'
```

Expected: under ~30 MB (distroless static + a stripped Go binary).

- [ ] **Step 5: Commit**

```
git add Dockerfile
git commit -m "feat: Dockerfile for the hclexp binary

Multi-stage build: golang:1.26-alpine compiles a static binary,
gcr.io/distroless/static-debian12:nonroot runs it as UID 65532. No
shell, no AWS CLI; the consuming chart pairs this image with
amazon/aws-cli via a shared emptyDir volume.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 9: CI smoke-test job for the image

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add a new `docker` job**

Append the following job to `.github/workflows/ci.yml` (after the
existing `build` job):

```yaml
  docker:
    runs-on: depot-ubuntu-24.04
    needs: [build]
    steps:
    - name: Checkout code
      uses: actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd # v5.0.1

    - name: Set up Buildx
      uses: docker/setup-buildx-action@988b5a0280414f521da01fcc63a27aeeb4b104db # v3.6.1

    - name: Build hclexp image (amd64)
      uses: docker/build-push-action@5176d81f87c23d6fc96624dfdbcd9f3830bbe445 # v6.5.0
      with:
        context: .
        load: true
        tags: hclexp:ci
        platforms: linux/amd64
        cache-from: type=gha
        cache-to: type=gha,mode=max

    - name: Smoke-test -help
      run: docker run --rm hclexp:ci -help

    - name: Smoke-test introspect -h
      run: docker run --rm hclexp:ci introspect -h
```

Note: action SHAs above are commonly-used v3/v6 pins — verify against
[github.com/docker/setup-buildx-action](https://github.com/docker/setup-buildx-action/releases)
and [docker/build-push-action](https://github.com/docker/build-push-action/releases)
at implementation time. If a newer pin is preferred, swap the SHA but
keep `# v<x.y>` comments consistent with the existing file style.

- [ ] **Step 2: Verify the workflow YAML is syntactically valid**

```
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))" && echo OK
```

Expected: prints `OK`.

- [ ] **Step 3: Commit**

```
git add .github/workflows/ci.yml
git commit -m "ci: docker build + smoke-test the hclexp image

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: Lock the `-out` directory contract

**Files:**
- Modify: `cmd/hclexp/hclexp_test.go` (or extend the closest existing test file)

- [ ] **Step 1: Find the existing introspect/writeIntrospected coverage**

```
grep -rn "writeIntrospected\|-out" cmd/hclexp/ test/ 2>&1 | head -20
```

If a test already covers the `-out <dir>` per-DB layout, skip this
task and note it in the PR description. Otherwise continue.

- [ ] **Step 2: Write a failing test that asserts the per-DB layout**

Add to `cmd/hclexp/hclexp_test.go`:

```go
func TestWriteIntrospected_DirectoryLayout(t *testing.T) {
	dir := t.TempDir()
	schema := &hclload.Schema{
		Databases: []hclload.Database{
			{Name: "posthog"},
			{Name: "system"},
		},
	}
	require.NoError(t, writeIntrospected(dir, schema))

	for _, name := range []string{"posthog", "system"} {
		p := filepath.Join(dir, name+".hcl")
		info, err := os.Stat(p)
		require.NoError(t, err, "expected %s", p)
		require.False(t, info.IsDir())
	}
}
```

- [ ] **Step 3: Run it**

```
go test ./cmd/hclexp -run TestWriteIntrospected_DirectoryLayout -v
```

Expected: PASS if the existing implementation already emits
`<dir>/<db>.hcl`. If it FAILS, the contract is already broken — STOP
and surface it to the user before continuing; this is exactly the
regression the test is meant to catch.

- [ ] **Step 4: Commit**

```
git add cmd/hclexp/hclexp_test.go
git commit -m "test(hclexp): lock '-out <dir>' per-database file layout

The chart's shared-volume integration with the aws-cli sidecar relies
on hclexp writing one <db>.hcl per database under the output
directory. Pin that shape.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 11: Publish workflow

**Files:**
- Create: `.github/workflows/publish.yml`

- [ ] **Step 1: Ask the user for the publisher IAM role ARN**

This is the OIDC role this workflow assumes to push to
`795637471508.dkr.ecr.us-east-1.amazonaws.com/posthog/chschema`. If
the user does not have it yet, use the placeholder
`arn:aws:iam::795637471508:role/github-actions-publisher-chschema` and
call it out in the PR description; the user can update it before
merging.

- [ ] **Step 2: Write `publish.yml`**

Create `.github/workflows/publish.yml`:

```yaml
name: Publish

on:
  push:
    branches: [main]
    tags: ['v*']

permissions:
  contents: read
  id-token: write

jobs:
  publish:
    runs-on: depot-ubuntu-24.04
    steps:
    - name: Checkout
      uses: actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd # v5.0.1

    - name: Configure AWS credentials (OIDC)
      uses: aws-actions/configure-aws-credentials@e3dd6a429d7300a6a4c196c26e071d42e0343502 # v4.0.2
      with:
        role-to-assume: arn:aws:iam::795637471508:role/github-actions-publisher-chschema
        aws-region: us-east-1

    - name: Login to Amazon ECR
      uses: aws-actions/amazon-ecr-login@062b18b96a7aff071d4dc91bc00c4c1a7945b076 # v2.0.1

    - name: Set up QEMU (for arm64 cross-build)
      uses: docker/setup-qemu-action@49b3bc8e6bdd4a60e6116a5414239cba5943d3cf # v3.2.0

    - name: Set up Buildx
      uses: docker/setup-buildx-action@988b5a0280414f521da01fcc63a27aeeb4b104db # v3.6.1

    - name: Image metadata (tags + labels)
      id: meta
      uses: docker/metadata-action@8e5442c4ef9f78752691e2d8f8d19755c6f78c81 # v5.5.1
      with:
        images: 795637471508.dkr.ecr.us-east-1.amazonaws.com/posthog/chschema
        tags: |
          type=sha,format=short,prefix=sha-
          type=raw,value=latest,enable={{is_default_branch}}
          type=semver,pattern={{version}}
          type=semver,pattern={{major}}.{{minor}}
          type=semver,pattern={{major}}

    - name: Build and push (linux/amd64,linux/arm64)
      uses: docker/build-push-action@5176d81f87c23d6fc96624dfdbcd9f3830bbe445 # v6.5.0
      with:
        context: .
        platforms: linux/amd64,linux/arm64
        push: true
        tags: ${{ steps.meta.outputs.tags }}
        labels: ${{ steps.meta.outputs.labels }}
        cache-from: type=gha
        cache-to: type=gha,mode=max
```

Action SHAs are commonly-used release pins; verify each against its
GitHub Releases page at implementation time and update if a newer
stable release exists. Keep the `# v<x.y>` comments alongside.

- [ ] **Step 3: Validate the YAML**

```
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/publish.yml'))" && echo OK
```

Expected: `OK`.

- [ ] **Step 4: Commit**

```
git add .github/workflows/publish.yml
git commit -m "ci: publish hclexp image to ECR on main + tags

OIDC-authenticated push to
795637471508.dkr.ecr.us-east-1.amazonaws.com/posthog/chschema.
Builds linux/amd64 and linux/arm64. Tags: sha-<short>+latest on main,
semver on v* tags.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 12: PR 2 verification + push

- [ ] **Step 1: Run the full test suite once more**

```
go test ./internal/... ./config ./cmd/... ./test/... 2>&1 | tail -10
```

Expected: green.

- [ ] **Step 2: Confirm the new files exist and the workflow files parse**

```
ls Dockerfile .dockerignore .github/workflows/publish.yml
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml')); yaml.safe_load(open('.github/workflows/publish.yml'))" && echo "yaml OK"
```

Expected: files listed, `yaml OK` printed.

- [ ] **Step 3: Local end-to-end docker smoke-test**

```
docker build -t hclexp:final . && docker run --rm hclexp:final -help | head -5
```

Expected: usage shown.

- [ ] **Step 4: Push the branch**

```
git push -u origin HEAD
```

- [ ] **Step 5: STOP — pause for PR creation/review**

PR 2 is ready. Call out the two open items in the PR description:
1. Publisher IAM role ARN — placeholder used; user to confirm/update.
2. After the first image is published, comment back to
   `posthog/charts` with the image tag and the final flag names
   (`-secure` / `-tls-skip-verify`).

---

## Optional follow-up (NOT part of the two PRs above)

These items are useful but explicitly deferred so the PRs ship faster.
Do not add them inline unless the user asks.

- TLS-enabled second listener in `docker-compose.yml` (port 9440, self-signed cert) + `just test-live-tls` recipe.
- Promoting the TLS live test into CI once it is locally green.
- A multi-arch local-build justfile target (`just docker-build`) for developer convenience.
