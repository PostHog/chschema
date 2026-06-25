# Add a `dump-cluster` subcommand to hclexp

## Context
`hclexp` (in `cmd/hclexp/`) already has subcommands `introspect`, `diff`, `validate`, `drift`, `load` (see the `switch` in `cmd/hclexp/hclexp.go:main`). It connects to ClickHouse over the **native protocol** via `config.NewConnection(cfg)` (returns `driver.Conn` from clickhouse-go v2) and dumps schemas as HCL via the `internal/loader/hcl` package (`hclload`).

## Why
A k8s Job (PostHog/charts) currently enumerates a cluster's nodes with `curl` against ClickHouse's **HTTP interface (8443)**, then runs `hclexp introspect` per node. The HTTP enumeration breaks in our environment: `curl` honors `HTTPS_PROXY` and routes the request through the Smokescreen egress proxy, which rejects the internal private-range IP with `407 ... Deny: Private Range`. The native protocol (9440) never goes through that proxy. Moving enumeration into `hclexp` eliminates the proxy problem and deletes the brittle shell.

## Goal
Add `hclexp dump-cluster`: connect natively to one entry host, enumerate every node of a named cluster from `system.clusters`, introspect each node, and write one `<out-dir>/<short-host>.hcl` per node.

## Requirements

### CLI
New subcommand `dump-cluster` (add a `case` in `main` and a line in `usage`). Flags, mirroring `runIntrospect`'s defaults from `config.GetDefaultConfig()`:
- `-host` (entry host), `-port` (default 9440 via cfg), `-user`, `-password`
- `-database` (comma-separated, same semantics as `introspect`)
- `-secure`, `-tls-skip-verify` (reuse `applyTLSFlags`)
- `-cluster` (required) — the `system.clusters` cluster name to enumerate
- `-out-dir` (required) — directory to write `<short-host>.hcl` files into

### Behavior
1. Connect to the entry host (`config.NewConnection`).
2. Enumerate nodes:
   ```go
   var hosts []string
   err := conn.Select(ctx, &hosts,
     "SELECT DISTINCT host_name FROM system.clusters WHERE cluster = ? ORDER BY host_name", cluster)
   ```
   - On connect/query error → log via `slog.Error` and `os.Exit(1)`.
   - Empty result → `slog.Warn("no hosts in cluster", ...)` and return (exit 0); nothing to dump.
3. Create `-out-dir` if missing, and **remove existing `*.hcl` in it first** so decommissioned nodes disappear from the dump (the chart relies on this today).
4. For each host: open a fresh native connection (`cfg.Host = host`), run the **same introspection pipeline as `runIntrospect`** — `hclload.Introspect` per database, `hclload.IntrospectNamedCollections`, `hclload.IntrospectNode` (pass node name `""` so it uses the server's `hostName()`), assemble one `hclload.Schema`, and write it to `filepath.Join(outDir, short+".hcl")` where `short` is the first DNS label of `host` (everything before the first `.`). Write the whole schema (all databases + named collections + the node block) into that single per-host file via the existing `writeFile` helper.
   - **Per-node failures are non-fatal**: log a warning and continue to the next host. Track a failure count.
5. Exit 0 if enumeration succeeded (even if some nodes failed); the count of failed nodes goes in a final log line.

### Refactor (avoid duplication)
Factor the per-node introspection body out of `runIntrospect` into a shared helper, e.g.:
```go
func introspectSchema(ctx context.Context, conn driver.Conn, databases []string) (*hclload.Schema, error)
```
that does the `Introspect` loop + named collections + node, returning the assembled `*hclload.Schema`. Have both `runIntrospect` and `runDumpCluster` call it. Keep `runIntrospect`'s existing behavior (including its redacted-named-collection warnings) intact.

### Conventions
Match the surrounding code: `flag.NewFlagSet("hclexp dump-cluster", flag.ExitOnError)`, `slog` for logging, `splitList` for the database list, `os.Exit(1)`/`os.Exit(2)` like the other `runX` funcs.

## Tests
- Add a unit test (in `cmd/hclexp/hclexp_test.go` style) for the pure `short host` derivation (`"a.b.c" -> "a"`, `"host" -> "host"`, edge cases). If you extract it into a small `func shortHost(string) string`, test that directly.
- Live cluster behavior depends on a real ClickHouse with a configured cluster; follow the existing `*_live_test.go` gating rather than adding a unit test that dials. A live test is optional/nice-to-have.

## Verify
```
go build ./...
go vet ./...
go test ./...
```

## Downstream contract (for reference — do NOT change anything outside this repo)
The chart will invoke it as:
```
hclexp dump-cluster \
  -host "$CLICKHOUSE_MIGRATIONS_HOST" -port 9440 \
  -user "$CH_DUMP_USER" -password "$CH_DUMP_PASSWORD" \
  -secure -tls-skip-verify \
  -cluster "$MIGRATIONS_CLUSTER" -database "$CLICKHOUSE_DATABASE" \
  -out-dir "/tmp/dump/$ENVIRONMENT"
```
Keep flag names stable to match that. Then a new `chschema-ops` image gets cut and the chart swaps its curl-enumeration + per-host loop for this single command (separate PR in PostHog/charts; git push stays in the chart).