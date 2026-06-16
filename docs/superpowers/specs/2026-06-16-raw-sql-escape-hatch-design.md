# Raw SQL escape hatch — design

## Context

`hclexp introspect` reconstructs a declarative HCL schema by reading each
object's `create_table_query` from `system.tables` and parsing it with the
ClickHouse SQL parser (`chparser`). When the parser cannot handle a DDL
construct, or when the parsed object uses an engine / form the HCL model does
not express (e.g. inner-engine MVs, exotic engines, future ClickHouse syntax),
introspection **aborts the entire database dump** with a hard error
(`introspect.go:69` and the per-builder error paths). A single unusual object
takes down the whole dump.

This is brittle in production: the `dump-cluster` k8s job feeds drift
detection, and one weird object on one node should not break that node's dump.
At the same time, silently papering over a *parser bug* (like the recent
`LAYOUT(IP_TRIE)` gap) would hide problems we want to fix upstream.

This design adds a **raw SQL escape hatch**: an object that cannot be parsed or
expressed in HCL can be stored verbatim as a `raw` block, round-tripped, and
re-applied. The behavior is **opt-in** — strict-by-default keeps parser gaps
loud — so robustness is a deliberate choice (a flag), not a silent default.

## Goal

Survive + round-trip. When enabled, introspection never crashes on an
unparseable/unsupported object; it captures the original CREATE DDL so dumps
stay complete and re-appliable. Raw objects are opaque: diffed by literal SQL
text, recreated (DROP+CREATE) on change.

Non-goals: dependency analysis of raw SQL, structural diffing of raw SQL,
in-place ALTER of raw objects.

## HCL shape

A generic, type-agnostic block scoped to a `database`, mirroring Terraform's
`resource "<type>" "<name>"` two-label addressing:

```hcl
database "posthog" {
  raw "dictionary" "city_postal_ip_trie" {
    sql = <<-SQL
      CREATE DICTIONARY posthog.city_postal_ip_trie (
        `prefix` String,
        `city_name` String DEFAULT '',
        `postal_code` String DEFAULT ''
      )
      PRIMARY KEY prefix
      SOURCE(CLICKHOUSE(USER 'admin' PASSWORD '[HIDDEN]' QUERY '...'))
      LIFETIME(MIN 0 MAX 3600)
      LAYOUT(IP_TRIE)
    SQL
  }
}
```

- First label = `kind` ∈ {`table`, `materialized_view`, `view`, `dictionary`}.
  Drives the `DROP` form on a recreate.
- Second label = object `name`.
- `sql` = the original CREATE DDL, emitted verbatim on apply.

## Components

### 1. Type model (`internal/loader/hcl/types.go`)

```go
type RawSpec struct {
    Kind string `hcl:"kind,label"`  // table | materialized_view | view | dictionary
    Name string `hcl:"name,label"`
    SQL  string `hcl:"sql"`
}
```

`DatabaseSpec` gains: `Raws []RawSpec `hcl:"raw,block"``.

Allowed kinds are a small package-level set; anything else is a decode error.

### 2. Introspection capture (`internal/loader/hcl/introspect.go`)

- Extend the query to carry the object kind:
  `SELECT name, create_table_query, engine FROM system.tables ...`.
  Map `engine` → kind: `Dictionary`→`dictionary`, `View`→`view`,
  `MaterializedView`→`materialized_view`, everything else→`table`.
  (`engine` is populated even when `create_table_query` will not parse.)
- `Introspect` / `processIntrospectRows` take an `allowRaw bool`.
- In the per-row flow, wrap parse + build. On `parseCreateStatement` failure,
  an unsupported-engine/form `build*` error, or the `default` unsupported
  statement type:
  - **`allowRaw == false` (default, strict):** return the existing error, but
    extended to name the flag, e.g.
    `parse create_table_query for posthog.x: <err> (re-run with -allow-raw to capture this object as a raw SQL block instead of failing)`.
  - **`allowRaw == true`:** append `RawSpec{Kind, Name, SQL: createSQL}` to
    `db.Raws`, `slog.Warn("captured object as raw SQL", "object", name, "reason", err)`,
    and continue to the next row.
- The `rowScanner`/`fakeRows` test helper is updated to scan the extra column.

### 3. Decode (`internal/loader/hcl/parser.go`)

gohcl auto-decodes the two-label `raw` block via the struct tags — no new
post-decode phase. Add a `kind` validation pass alongside the existing engine
decode loop: reject unknown kinds with `database %q raw %q: unknown kind %q`.

### 4. Encode (`internal/loader/hcl/dump.go`)

In `writeDatabase`, after the existing object types, emit each `RawSpec`
(sorted by name) as a `raw "<kind>" "<name>"` block with `sql` rendered as a
**heredoc** (`<<-SQL ... SQL`) for readability. Heredoc emission is built with
`hclwrite` tokens; if that proves fiddly, fall back to a quoted `cty.StringVal`
(correct round-trip, less pretty). A small helper
`setHeredocAttribute(body, name, tag, content)` keeps it contained.

### 5. Diff (`internal/loader/hcl/diff.go`)

- `DatabaseChange` gains `AddRaws []RawSpec`, `DropRaws []RawSpec`,
  `AlterRaws []RawChange` where
  `RawChange struct { Kind, Name, OldSQL, NewSQL string }`.
- Raw objects are matched by `(kind, name)`. `sql` is compared by **exact
  string equality** — no whitespace normalization, because whitespace inside
  SQL string literals (e.g. a dictionary's `QUERY '...'`) is significant.
- A new-vs-old DB follows the existing add/drop-everything pattern; the
  same-name path diffs raw maps and emits `AlterRaws` on a `sql` mismatch.
- `sortDatabaseChange` sorts the three new slices by name for deterministic
  output.

### 6. SQL generation (`internal/loader/hcl/sqlgen.go`)

- **Add:** emit the stored `sql` verbatim, in its own phase placed after table
  creates (raw objects are usually leaf dictionaries/views; their dependencies
  cannot be analyzed, so no finer ordering is attempted).
- **Drop:** `DROP <form> IF EXISTS db.name` where `<form>` is kind-mapped —
  `dictionary`→`DICTIONARY`, `view`→`VIEW`, `table`/`materialized_view`→`TABLE`.
- **Change (`AlterRaws`):** DROP + CREATE (the new `sql`). Flag the DROP+CREATE
  `-- UNSAFE` **only when `kind == "table"`** (real on-disk data loss). A
  helper `rawRecreateIsUnsafe(kind) bool` returns true only for `table`;
  `materialized_view`, `view`, `dictionary` recreate without the flag.

### 7. Validation (`internal/loader/hcl/validate.go`)

Raw objects are opaque, so they contribute **no outgoing** dependency checks.
But a declared `raw` block **registers its name** in the known-objects set, so
a real materialized view's `to_table` or a Distributed table's `remote_table`
that points at a raw object resolves cleanly instead of reporting a missing
dependency.

### 8. CLI plumbing (`cmd/hclexp/hclexp.go`)

- Add `-allow-raw` (bool, default false) to both `runIntrospect` and
  `runDumpCluster`.
- Thread it through the shared `introspectSchema(ctx, conn, databases, nodeName, allowRaw)`
  helper into `hclload.Introspect`.
- `dump-cluster` already treats per-node failures as non-fatal; with
  `-allow-raw` a node containing an unparseable object now produces a complete
  dump (raw blocks) instead of being skipped.

## Documentation

`docs/README.hcl.md`: new "Raw SQL fallback" subsection documenting the `raw`
block, the `-allow-raw` flag, the opaque/recreate-only semantics, and that a
`table`-kind change is flagged `-- UNSAFE`.

Update the supported-features list in `CLAUDE.md` to mention the escape hatch.

## Testing

- **Round-trip:** an introspect row whose `create_table_query` does not parse,
  with `allowRaw=true`, produces a `RawSpec`; `Write` emits a `raw` block;
  re-parsing the emitted HCL yields the same `RawSpec`. Use a real
  `LAYOUT(...)`-style or deliberately-unparseable DDL fixture.
- **Strict default:** the same row with `allowRaw=false` returns an error whose
  message contains `-allow-raw`.
- **Kind mapping:** `engine` values map to the right kind.
- **Decode validation:** an unknown `kind` label is a decode error.
- **Diff:** add / drop / change of a raw object yields the right
  `AddRaws`/`DropRaws`/`AlterRaws`.
- **SQLgen:** add emits the `sql` verbatim; drop emits the kind-mapped `DROP`;
  a `table`-kind change is `-- UNSAFE`, a `dictionary`/`view` change is not.
- **Validation:** a real MV `to_table` pointing at a declared raw object passes.

## Decisions (resolved)

1. **Goal:** survive + round-trip (opaque, recreate-on-change).
2. **HCL shape:** two-label `raw "<kind>" "<name>" { sql = ... }`.
3. **UNSAFE flagging:** only `table` recreate is flagged `-- UNSAFE`.
4. **Default posture:** strict by default; `-allow-raw` opts into capture; the
   strict error names the flag.
