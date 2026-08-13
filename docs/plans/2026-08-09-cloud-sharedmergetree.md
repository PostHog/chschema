# ClickHouse Cloud — reading the `Shared*MergeTree` engines

Status: implemented as the mergeable successor to #176.
First step of running `hclexp` against ClickHouse Cloud.

## Problem

ClickHouse Cloud takes input DDL from users and scripts like, e.g.

```sql
create table t ( ... )
engine = ReplacingMergeTree(version)
```

, but interprets it as, and reports `show create table t` as, e.g.

```sql
create table t ( ... )
engine = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)
```

. This applies for MergeTree => SharedMergeTree, ReplacingMergeTree => SharedReplacingMergeTree, etc.

Users don't generally care that the actual implementation is SharedReplacingMergeTree.
However, before this work, chschema obviously does - it treats it as a new unknown engine type (which it technically is).

chschema should "just work" when used with Cloud, and subject to this transform.

This means, for example:

- ddl in HCL should be able to be written as
  `engine "replacing_merge_tree" { version_column = "version" }`
- introspect should write HCL the same way

## Research

ClickHouse Cloud rewrites MergeTree-family DDL on the way in. The server
setting `cloud_mode_engine` controls it:

| value | behaviour (from `system.settings.description`) |
|---|---|
| 0 | allow everything |
| 1 | rewrite DDLs to use `*ReplicatedMergeTree` |
| 2 | rewrite DDLs to use `SharedMergeTree` |
| 3 | as 2, except when an explicit remote disk is specified |
| 4 | as 3, plus `Alias` instead of `Distributed` |

An internal Cloud service reports `cloud_mode_engine = 2`, so every
MergeTree-family table comes back from `system.tables` as a `Shared*`
engine:

| engine | tables |
|---|---|
| `SharedMergeTree` | 291 |
| `SharedReplacingMergeTree` | 49 |
| `SharedAggregatingMergeTree` | 1 |

`engineFromAST` has no case for these names, so it falls through to
`unsupported engine: SharedMergeTree` and introspection aborts before
anything else can be checked.

## What the server actually returns

`Shared<X>MergeTree` takes the `Replicated<X>MergeTree` argument list: a
`(zoo_path, replica_name)` pair, then the same trailing arguments as
plain `<X>MergeTree`.

```
SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)
SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', ver, is_deleted)
SharedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
```

The decisive detail: across all 341 tables the pair is always the same
two literals. Cloud generates them; nobody authored them, and there is
no per-table replication path to manage on Cloud. They carry no
information a schema should record.

## Design — collapse on introspect, gated on the server setting

Read `Shared<X>MergeTree(cloud_path, cloud_replica, rest…)` as the plain
`<X>MergeTree(rest…)`. The HCL vocabulary does not change, `sqlgen` keeps
emitting `MergeTree()` / `ReplacingMergeTree(ver)`, and Cloud rewrites
that back to the Shared form on apply. The round trip is stable:

```
server   SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)
  ↓ engineFromAST
HCL      engine "replacing_merge_tree" { version_column = "version" }
  ↓ sqlgen
DDL      ENGINE = ReplacingMergeTree(version)
  ↓ Cloud rewrite (cloud_mode_engine = 2)
server   SharedReplacingMergeTree(…)          ← no drift
```

Two constraints keep this from becoming a guess.

**Gated on the server, not assumed.** The collapse happens only when the
connected server reports a `cloud_mode_engine` that rewrites DDL to
`SharedMergeTree`: 2, 3 or 4. Everywhere else a `Shared*` engine is still
`unsupported engine`.

What the gate protects is the **write** direction, not the read. Reading a
`Shared*` engine is unambiguous, since it exists only on Cloud, and the
argument guard below already rejects a pair Cloud did not generate. The
collapse is only safe if the server turns the plain engine `sqlgen` emits
back into a Shared one:

| mode | our `MergeTree()` becomes | collapse |
|---|---|---|
| 0 | a plain `MergeTree` | unsafe — changes the engine |
| 1 | `ReplicatedMergeTree` | unsafe — changes the engine |
| 2 | `SharedMergeTree` | safe |
| 3 | `SharedMergeTree` (we never emit an explicit disk) | safe |
| 4 | `SharedMergeTree` | safe |

Mode 4 also swaps `Distributed` for `Alias`, which `hclexp` does not model.
That is a separate problem: including 4 here means a mode-4 service gets its
MergeTree tables read correctly and fails only on the Distributed ones, rather
than failing on everything.

**A surprise aborts.** Modes must be in the known set above. The `(zoo_path, replica_name)` pair must be
exactly `('/clickhouse/tables/{uuid}/{shard}', '{replica}')`. Anything
else errors rather than being dropped, matching how #108 and #109 treat
unexpected engine parameters: a silently dropped argument round-trips as
a false "no drift".

### Rejected alternatives

- **First-class `shared_*` engine kinds.** Lossless, but it puts
  Cloud-only names in the HCL vocabulary, forces a per-env engine patch
  on every table for a schema that also targets self-hosted, and makes
  authors write the generated `zoo_path` as noise. Emitting
  `SharedMergeTree` buys nothing either — Cloud rewrites the DDL
  regardless of which name we send.
- **Equivalence in the diff** (`MergeTree` ≡ `SharedMergeTree`). Pushes
  fuzzy comparison out of `diff.go` into `compare.go` and both
  renderers, which today are a straight structural comparison of typed
  values. Dumps would still carry Cloud-specific engines.

## Implementation

- `internal/loader/hcl/introspect.go`
  - `IntrospectOptions{AllowRaw, Exclude, CollapseSharedMergeTree}` plus
    `IntrospectWithOptions`. `Introspect` and `IntrospectWithExclude`
    delegate, keeping their signatures.
  - `DetectCloudEngineRewrite(ctx, conn)` reads `cloud_mode_engine` from
    `system.settings` and passes it to `classifyCloudModeEngine`, which
    reports both whether the server rewrites (`2`/`3`/`4`) and whether the
    value is recognised at all. An absent setting → false, no error
    (self-hosted). An unrecognised value → false plus a `slog.Warn` naming
    the value: `0` and `1` are the server deliberately not rewriting, but a
    value we have never seen means ClickHouse has added a behaviour this
    build does not model, and the unsupported-engine error that follows on a
    Cloud service does not report what the server said. CLI callers treat a
    probe error as a warning and keep the collapse off, so a restricted
    self-hosted user is not newly required to read `system.settings`; safety
    remains fail-closed for any `Shared*` table.
  - `collapseSharedEngine(name, params)` rewrites the name and drops the
    generated pair, or errors on an unexpected pair.
  - `engineFromAST` switches on the possibly-rewritten name. Its
    fallback error explains the `cloud_mode_engine` gate when the name
    starts with `Shared`.
  - Options thread through `processIntrospectRowsOpt` →
    `introspectOneObject` → `upsertObjectFromStmt` →
    `buildTableFromCreateTable` → `engineFromAST`, and to the
    `time_series` inner-engine call.
- `cmd/hclexp/hclexp.go` — `introspectSchema` and `loadFromClickHouse`
  probe once per connection and pass the result into each per-database
  introspect. That covers `introspect`, `dump-cluster`, and the live
  side of `diff`, `plan`, `drift` and `load`.

## Verification against a live Cloud service

All 341 Shared* tables replayed through `processIntrospectRowsOpt`. The one
table that originally aborted has a column `DEFAULT` using ClickHouse's `<=>`
operator. Parser panic containment was split into chschema #184, and native
`<=>` support plus the upstream nil-dereference fix is tracked in
clickhouse-sql-parser #21 / #23. With that parser revision, all 341 tables
introspect cleanly.

Collapsed engine kinds match the server's own counts exactly: 291
`merge_tree`, 49 `replacing_merge_tree`, 1 `aggregating_merge_tree`. Every
collapsed engine re-emits a non-empty clause through `sqlgen`.

## Consequences and known gaps

- **Declare the plain family for Cloud.** A table declared
  `replicated_merge_tree` emits `ReplicatedMergeTree('/path', …)`, which
  Cloud rewrites to `SharedMergeTree` with its own path; the next
  introspect collapses that to `merge_tree` and the diff reports a
  permanent engine change. The signal is legible, and the fix is to
  declare `merge_tree`. Documented, not worked around.
- **`sql2hcl` stays strict.** It parses DDL from a file with no server
  to ask, so Cloud DDL pasted into it still errors. A follow-up could
  add an explicit opt-in flag.
- Other Cloud-isms seen on that service, out of scope here and each a likely
  next step: the `Shared` database engine; `KeeperMap` / `S3` / `S3Queue`
  tables (10 / 1 / 1); and one inner-engine materialized view out of 149
  (the other 148 are TO-form, which is already supported).

## Tests

- `collapseSharedEngine` over each family, with and without trailing
  arguments, and the `Replacing` two-argument form.
- A non-default `zoo_path`/`replica_name` errors.
- With the gate off, `SharedMergeTree` still reports `unsupported
  engine`, and the message names `cloud_mode_engine`.
- `DetectCloudEngineRewrite`: `2`/`3`/`4` → true; `0`/`1` → false; an
  unrecognised `5` → false; no row → false.
- Round trip: Cloud `create_table_query` → `TableSpec` → `sqlgen` emits
  the plain engine clause.
