# #65: multi-node diff with cross-role dependency ordering

> Supersedes the original issue's "opaque (env, role) manifest" proposal. The
> premises are re-grounded against the real composition repo
> (`posthog/clickhouse/hcl`) and the production dumps (`clickhouse-schema/`).

## What the original issue got wrong

The issue assumed consumers pass an **external manifest** of `<env> <role>
<layer dirs…>` with the first two tokens **opaque**. Two problems, found by
reading the real repos:

1. **The composition already exists and isn't opaque.** Authoring lives in
   `posthog/clickhouse/hcl/ops`, composed by layer stacks
   (`base` + `prod` + `env/<env>`), encoded in `check.sh`/`diff.sh`'s
   `stack_for()`. Each stack resolves to **one node-role's** desired schema and
   is diffed against a vendored **golden** dump. This path works today
   (`hclexp diff -left <stack> -right golden/<env>-ops.hcl`).

2. **Opaque keys can't resolve cross-cluster edges.** The motivating object set
   is a fan-in: a role's `writable_query_log_archive` (Distributed,
   `cluster_name = "ops"`) writes into `query_log_archive_buffer` →
   `sharded_query_log_archive` (ReplicatedMergeTree — the *only* physical
   storage), which live **on the OPS nodes, in a different node file**.
   Resolving that edge needs `cluster_name → physical nodes`, which comes from
   each dump node's `node { macros = { cluster = "ops", … } }` block — not from
   an opaque manifest token.

So the metadata is: **layered composition for the desired side** (already in
`stack_for`) + **node-role / cluster macros from the dump for the topology
side**. The manifest, reconsidered, is just `role → ordered layer dirs` where
`role` is *meaningful* and matches the dump's `hostClusterRole`.

## What's actually missing (the real gap)

`base/ops.hcl` holds **all five** query_log_archive objects in one layer
(`query_log_archive`, `query_log_archive_buffer`, `sharded_query_log_archive`,
`writable_query_log_archive`, `ops_query_log_archive_mv`), so the
storage→buffer→distributed→MV order is **already produced** by the existing
single-`ChangeSet` topo-sort (`createDependencyEdges`/`topoSortNodes`).

The gap is **cross-role**. Per the repo README, *every node role across the
fleet* gets the three companion objects (`query_log_archive`,
`writable_query_log_archive`, `ops_query_log_archive_mv`) that reference the OPS
storage. Adding a column then requires a **global** order that no per-role diff
can see:

```
1. ops   : sharded_query_log_archive   ALTER ADD COLUMN   (physical storage — first)
2. ops   : query_log_archive_buffer    ALTER ADD COLUMN   (Buffer mirrors structure)
3. *all* : writable_query_log_archive   ALTER ADD COLUMN   (Distributed write proxies)
   *all* : query_log_archive            ALTER ADD COLUMN   (Distributed read proxies)
4. *all* : ops_query_log_archive_mv     MODIFY QUERY        (SELECT must match new width)
```

The role-X→OPS edge crosses node-file boundaries; only a run that loads the
whole topology and resolves `cluster_name="ops"` via node macros can order it.

## Two modes (both served by one new path)

### Mode A — node-role-matched parity (the case you described)

> "left is a one node schema; on the right we pass a full cluster topology, so
> the diff should only compare the proper node type identified by node role."

- **left** = a composed single-role schema (e.g. OPS env stack).
- **right** = a topology dump with many `node` blocks of different roles
  (`clickhouse-schema/<env>/*.hcl`, or a future multi-node golden).
- Match left's role against **only** the right nodes whose
  `node.macros.hostClusterRole` equals it, diff those, ignore the rest (so a
  data node is never reported as "missing all the ops tables").

### Mode B — fleet plan with cross-role ordering

- **left** = several role compositions (one stack per role).
- **right** = the full topology dump.
- Per-role diff (Mode A matching) → union/dedupe identical objects across roles
  → build one global dependency graph with cross-cluster edges → topo-sort
  **CREATE + ALTER + DROP** once → emit globally-ordered ops.

## Command (implemented)

```
hclexp plan -manifest <role→stack> -dump <topology-dir> [-layer-root <dir>] -format json|text
```

- `-manifest`: lines of `<role> <layer> [<layer>…]`. `role` is **not opaque** —
  it must equal the dump's `hostClusterRole` so matching + edge resolution work.
  (Equivalently, `stack_for` keyed by role.) Blank/`#` lines ignored; duplicate
  roles rejected. The composed stack is each role's **desired** schema (target).
- `-dump`: directory of per-node **current**-state HCL files, each carrying a
  `node { macros }` block. Nodes are matched to manifest roles by their
  `hostClusterRole` macro; replicas/shards collapse to the lexically-first
  representative per role. A role absent from the dump means every object is a
  CREATE.
- `-layer-root` (default `.`): prefix the manifest's layer paths resolve under,
  so a consumer can point at a committed snapshot (`git archive` to a tmp dir)
  or the working tree. Keeps git out of `hclexp`.
- `-format json|text`: JSON reuses the `#64` operation shape, extended with a
  `roles` array and a top-level `unsafe` list; `order` is the global rank.

**Direction.** The plan emits the migration that brings each node (Current) to
the authored schema (Desired) — internally `Diff(Current, Desired)` — so the
target is the manifest composition. (`RoleDiff{Role, Desired, Current}`.)

### Algorithm (reuses existing pieces)

1. **Index the topology**: load every `-right-dir` node file; read its `node`
   block → `(hostClusterRole, cluster, shard, replica)`. Build `cluster → nodes`
   and `role → nodes`.
2. **Per-role diff**: for each manifest role, resolve its layer stack
   (`loadSide`, existing) and `Diff` it against the **representative** right node
   of that role (one per role+shard; replicas collapse via `{replica}` macro,
   like `drift`). Yields a per-role `ChangeSet`.
3. **Union + dedupe**: project every role's ops into one set keyed by
   `(database, object, sql)`. Identical statements across roles collapse to one
   op carrying `roles: […]` / `nodes: […]` provenance.
4. **Global graph**: build `createDependencyEdges` over the unioned schema, plus
   **cross-cluster edges** resolved through `cluster → nodes`: a Distributed/
   Buffer object's `(cluster_name, remote_table)` links to that object on the
   nodes hosting `cluster_name`. Run `topoSortNodes` **once**.
5. **Order all kinds**: CREATE and widening ALTER (ADD COLUMN, MODIFY widen)
   flow **with** dependency direction (storage→proxies→MV); DROP and narrowing
   changes flow **reverse** (MV→proxies→storage), consistent with today's
   DROP-in-reverse rule. `order` = global rank.
6. **Emit** the `#64` JSON, `nodes`/`roles` populated.

### Policy stays in the consumer

"The data role is the sharded cluster", which envs exist, etc., stay out of
`hclexp`. The tool exposes `replicated` + `roles`/`cluster` provenance; the
consumer derives policy from that.

## Acceptance criteria

- `hclexp plan … -format json` emits one merged, globally-ordered `operations`
  list with per-op `roles`/`nodes` provenance.
- **Role matching** (Mode A): left OPS role vs a topology dump containing ops +
  data + ingestion nodes diffs **only** the ops node; non-ops roles are ignored,
  not reported as drift.
- **Dedupe**: identical statements across roles collapse to one op with the
  union of roles.
- **Cross-role ALTER order** (the regression a name sort can't catch): adding a
  column to `sharded_query_log_archive` (ops) emits its ALTER at a **lower
  `order`** than every role's `writable_query_log_archive` ALTER, which in turn
  precedes the `ops_query_log_archive_mv` MODIFY QUERY — even though the names
  sort the other way.
- `go test ./...` fixture encodes the above with a minimal 2-role topology
  (ops storage + one consumer role) and a cross-cluster edge.

## Decisions (locked)

1. **Left role identity = the manifest's `role` key.** A composed stack has no
   `node` block; its role is the manifest key, which must equal the dump's
   `hostClusterRole` so matching + cross-cluster edge resolution work.
2. **New `plan` subcommand** subsuming both modes; `diff` stays untouched.
3. **Collapse replicas by the `{replica}` macro** (reuse `drift`'s grouping): a
   role's nodes are grouped by `(role, shard)` and one representative per group
   is diffed, so an N-replica role yields one diff, not N.

## Dependencies

Reuses the operation JSON shape from #64 (merged), `Diff`/`loadSide`,
`createDependencyEdges`/`topoSortNodes`, and the `node`-macro grouping already
in `drift`.
