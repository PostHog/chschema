# Cross-cluster Distributed proxy validation (Issue A + B)

Status: design draft 2026-07-07.
Issues: A = cluster-aware validate (#118); B = proxy column consistency (#119).
Origin: PostHog/posthog `check.sh` carries per-role `skip-validation` lists —
23 entries on the `data` role alone — that are all cross-cluster `Distributed`
proxies (21 → `aux`, 1 → `ai_events`, 1 → `system`). The skip means these
proxies are **completely unvalidated**: nothing checks the remote exists or that
the proxy's columns match it. The list also grows +1 per new queryable satellite
table. This adds real validation and lets the skip lists shrink to (near) empty.

## Problem

`Validate` (validate.go) resolves every dependency against **one** loaded
schema. For a `Distributed` table it emits
`Dependency{To: {RemoteDatabase, RemoteTable}, Kind: DepDistributedRemote}` and
**ignores `EngineDistributed.ClusterName`** (CollectDependencies, ~line 140).
A proxy on the `data` node points at `posthog.sharded_web_stats_preaggregated`,
whose storage table lives in the **aux** cluster's composition — not loaded when
validating `data`. Result: `loadedDBs["posthog"]` is true but the table isn't in
`declared`, so Validate reports *"references … which is not declared in the
schema"*. The only escape today is `-skip-validation`, which disables **all**
dependency checks for that object — remote existence and column agreement alike.

Two consequences:
1. A drifted proxy (column present on the proxy, absent/renamed/retyped on the
   aux storage table) passes CI and fails only at query time.
2. The skip list is hand-maintained and unbounded (greptile P2 on posthog#68185).

## Key facts (code-grounded)

- `EngineDistributed{ClusterName, RemoteDatabase, RemoteTable, ShardingKey, PolicyName}`
  (engines.go:88). `ClusterName` is param 0 of `Distributed(...)` and is always
  captured, by HCL load **and** by introspect (introspect.go:645, :1010).
- The remote **database** is almost always `posthog` for every cluster — ClickHouse
  uses one database name across clusters. The **cluster** is the discriminator,
  not the database. So resolution must key on `ClusterName`, then look up
  `(RemoteDatabase, RemoteTable)` inside that cluster's schema.
- `Validate(dbs, skip)` builds `declared` + `loadedDBs` from the single schema
  (validate.go:448). `Dependency` has no cluster field yet.
- Column model: `TableSpec.Columns []ColumnSpec{Name, Type, Nullable, …}`;
  `columnsEqual` (diff.go:201) is the modifier-aware comparison; `ColumnsProvidedBy`
  (virtual_columns.go:71) yields declared+virtual columns via a `TableResolver`.
- Loading: `LoadLayers([]string) → *Schema`, then `Resolve(*Schema)`. Each external
  cluster stack resolves independently into its own `*Schema`.

## Issue A — cluster-aware validate

### CLI

Repeatable flag on `validate` (and, later, wherever validation runs):

```
hclexp validate -layer <node-stack> \
  -cluster aux=<dir>:<dir>:...  \
  -cluster ai_events=<dir>:...  \
  -cluster events_recent=@absent
```

`-cluster NAME=STACK`, repeatable. `STACK` uses the OS list separator (`:`) between
dirs so it never clashes with the comma that separates `-layer` dirs. The sentinel
`STACK == "@absent"` declares a cluster that has **no** composition in this
env/repo (e.g. `batch_exports` has no local node) — references into it are
structurally unresolvable here and count as satisfied. chschema stays generic and
stateless; the caller (posthog `check.sh`) translates its own `clusters` manifest
into these flags.

### Loading

At startup, load+`Resolve` each `-cluster NAME=STACK` into its own schema and build
a `ClusterSet`:

```go
type ClusterSet struct {
    declared map[string]map[ObjectRef]bool // name → declared objects (nil ⇒ absent)
    resolver map[string]TableResolver      // name → resolver (for Issue B)
    absent   map[string]bool               // name → true
}
```

### Resolution algorithm

Carry the cluster on the dependency: add `Cluster string` to `Dependency`,
populated for `DepDistributedRemote` from `eng.ClusterName`. In `Validate`, resolve
a Distributed remote `R = {RemoteDatabase, RemoteTable}` on cluster `C`:

1. **Built-in DB.** `R.Database == "system"` → satisfied. (Folds in the
   `custom_metrics*`, `distributed_system_processes`, `ops_query_log_archive_mv`
   `system.*` readers — they leave the skip list entirely, independent of A.)
2. **Local.** `R` declared in the node's own schema → satisfied (today's path;
   covers own-cluster proxies like `posthog.sharded_events`, and cluster aliases
   whose storage is on the same node).
3. **Mapped external.** `C` in `ClusterSet` and not absent → resolve `R` against
   that cluster's `declared[C]`. Missing → **error** (real cross-cluster drift).
4. **Absent.** `C` marked `@absent` → satisfied.
5. **Unknown.** `R` not local, `C` neither mapped nor absent → **error**:
   *"remote `posthog.sharded_x` on cluster `aux` is not declared locally and
   cluster `aux` has no `-cluster` mapping (add it or mark @absent)"*. This is the
   anti-staleness guarantee — a new cross-cluster proxy can't be silently accepted.

Backward compatibility: with **no** `-cluster` flags, steps 1–2 preserve today's
behavior for local/`system` refs; cross-cluster proxies hit step 5 (error) unless
still `-skip`ped — so existing callers that pass `-skip-validation` are unchanged,
and callers opt into real checking by supplying mappings and dropping the skips.

### Files

- `engines.go` / `validate.go`: `Dependency.Cluster`; populate in
  `CollectDependencies`.
- `validate.go`: `Validate(dbs, skip, clusters ClusterSet)`; the 5-step routing;
  `system`-DB rule.
- `cmd/hclexp/hclexp.go`: repeatable `-cluster` flag (custom `flag.Value`), load
  each stack, build `ClusterSet`, thread into `Validate`. Update `runValidate`
  doc + `validate` help line.

## Issue B — proxy column consistency

Once a remote resolves (step 2 local or step 3 external), assert the proxy's
columns agree with the remote's storage columns. This is the "properly created"
guarantee.

### Rule

- **Subset + type-match (default).** Every column on the `Distributed` table must
  exist on the remote with an equal type. Compare on **type + nullability only** —
  *not* full `columnsEqual` — because a proxy legitimately omits the storage
  table's `CODEC`/`DEFAULT`/`TTL`/`comment`. A missing or retyped column is an
  error (`Kind: KindDistributedColumn`).
- **Strict (opt-in, `-strict-proxy-columns`).** Also require the reverse (remote ⊆
  proxy) so the proxy is an exact mirror. Propose default = subset, strict = flag,
  until a legitimate subset proxy is found in the fleet (then keep default subset).
- Compare against the remote's **declared** columns (not `ColumnsProvidedBy`):
  virtuals like `_timestamp` are readable-through but are not Distributed columns.
- Cross-cluster: look the remote up via `ClusterSet.resolver[C]`; local via the
  node's own resolver. Only runs when the remote resolved, so it is naturally
  gated by Issue A and never fires for `@absent`/`system`.

### Optional extension (own issue or B follow-up)

Verify `ShardingKey` identifiers reference columns that exist on the remote
(cheap once the remote is resolvable). Flag `-check-sharding-key`. Deferred.

### Files

- `validate.go`: `validateDistributedColumns(dep, proxySpec, remoteSpec)` →
  `[]ValidationError`; new `KindDistributedColumn`; a type-only column comparator
  (reuse the type-normalization behind `columnsEqual`, dropping the modifier
  fields). Wire into `Validate`'s Distributed loop after resolution.
- `cmd/hclexp/hclexp.go`: `-strict-proxy-columns` flag.

## Test plan (inline TDD, per repo convention)

Issue A (`validate_test.go`, table-driven like `TestValidate_*`):
- external remote resolves via `-cluster aux=…` → no error.
- external remote missing in the aux stack → error (step 3).
- unknown cluster, no mapping, not skipped → error (step 5).
- `@absent` cluster → satisfied.
- `system.*` remote → satisfied with no mapping (step 1).
- no `-cluster`, no `-skip` → cross-cluster proxy errors (documents the migration
  path); with `-skip` → unchanged (regression guard).
- CLI parse test for repeatable `-cluster NAME=a:b` and `=@absent`.

Issue B:
- proxy column absent on remote → error; present+type-match → ok.
- proxy column retyped vs remote → error; proxy omits remote's CODEC/DEFAULT → ok
  (type-only comparison).
- strict mode: remote has an extra column the proxy lacks → error only under
  `-strict-proxy-columns`.
- cross-cluster column check resolves the remote through `ClusterSet.resolver`.

Snapshot/live: no new snapshots needed; a live check is out of scope (validate is
offline). Keep the 73% coverage floor.

## Downstream (PostHog/posthog `check.sh`)

After A ships in a pinned build: a new `clusters` manifest (`<env> <cluster> <role|@absent>`)
+ the existing `nodes` stacks generate the `-cluster` flags per env; `skip_for()`
collapses to the `system.*` set (or empty, once step 1 lands). After B: the fleet's
proxy/remote column drift surfaces as CI failures, fixed as normal HCL edits.
Mirror in `posthog-cloud-infra` for the prod `data` goldens.

## Open questions

- Default subset vs. strict exact-mirror for Issue B (proposed: subset default).
- Alias clusters (`posthog_writable`, `posthog_primary_replica`, single-shard
  writer endpoints) — map each alias to the same stack as its base, or teach
  `ClusterSet` an alias table? Proposed: caller maps each name explicitly (simplest,
  keeps chschema dumb).
- Whether `-cluster` should also gate MV/View cross-cluster sources (rare today);
  the `DepDistributedRemote` case is the immediate need — MV/View can reuse the
  same `ClusterSet` routing later without CLI changes.
