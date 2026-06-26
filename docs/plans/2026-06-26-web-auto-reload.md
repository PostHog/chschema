# `hclexp web`: auto-reload on source change

## Problem

`hclexp web` loaded and resolved the schema once at startup and served it
forever. Editing the HCL meant restarting the server — painful while authoring.

## Approach

On each request, re-stat the source files at most once per interval; if any
file's mod time changed (or a file was added/removed), reload + re-resolve and
swap in fresh derived state. Throttled and non-blocking.

### Mechanics (`cmd/hclexp/web.go`)

- `webServer` now separates **static** state (templates) from **schema-derived**
  state (`schema`, `deps`, `kindIndex`, `flows`, `flowAnchors`, `problems`,
  `globalProblems`), guarded by a `sync.RWMutex`. The build logic moved into
  `rebuildState(schema)` (reused by construction and reload).
- `enableReload(config, layers, interval)` arms reload (called from `runWeb`
  when `-reload-interval > 0`); `newWebServer(schema)` keeps its signature with
  reload **off** by default, so all existing tests and callers are unchanged.
- `maybeReload()` runs at the top of each handler:
  - `interval <= 0` → no-op (reload disabled).
  - `reloadMu.TryLock()` — **never blocks a request**; if another goroutine is
    already checking, this request proceeds with current state.
  - Throttle on `lastCheck` (interval); compute a fingerprint
    (`path -> modtime` over the re-globbed source files); if unchanged, return.
  - On change: `load` + `Resolve`; on success take the write lock and
    `rebuildState`; on failure log and **keep the last good schema** (retried
    next interval).
- Handlers (`handleIndex`, `handleObject`, `handleFlows`) take `mu.RLock` for
  their duration, so a reader sees a fully-old or fully-new state — never a torn
  swap. `now func() time.Time` is injectable for deterministic tests.

### Flag

`-reload-interval` (duration, default `2s`; `0` disables). Source = the
`-config` file, or every `*.hcl` in the `-layer` dirs (re-globbed each check so
added/removed files register).

## Scope

Tracks the HCL **source** files. `file()`-referenced `.sql` files are not yet
watched (the loader doesn't report which files it read) — a possible follow-up.

## Verification

- `cmd/hclexp/web_test.go`:
  - `TestWeb_Reload` — injected clock: an edit is picked up only after the
    interval (and not before).
  - `TestWeb_ReloadKeepsSchemaOnBadEdit` — a broken edit keeps serving the last
    good schema (HTTP 200).
  - `TestWeb_ReloadConcurrent` — concurrent readers during live reloads; clean
    under `-race`, every response 200.
- Existing `TestWeb_*` unchanged (reload off by default). Full suite +
  golangci-lint + vet + gofmt green. Live smoke: edited the source under a
  running server and the served page updated after the interval.
