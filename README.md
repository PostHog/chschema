# chschema — Declarative ClickHouse Schema Management

A declarative tool for managing ClickHouse schemas. Schemas are written in
HCL, layered for multi-environment setups, resolved into a flat desired
state, and round-tripped against a live cluster.

## What hclexp does

`hclexp` is a multi-command CLI. The core modes:

1. **Introspect** — connect to a live ClickHouse instance and dump its
   databases as HCL (to stdout, a file, or a directory). Round-trips
   tables, materialized views, plain views, dictionaries, and named
   collections.
2. **Load & resolve** — read an HCL schema (a single file or a stack of
   layer directories), apply the deterministic
   [parent-first composition model](#core-composition-model-parent-first-resolution),
   and emit the resolved, flat schema as canonical HCL.
3. **Validate** — check that every cross-object reference (MV sources +
   destination, view sources, Distributed `remote_*`) in a resolved
   schema is satisfied, without connecting to a cluster.
4. **Diff** — compare two schemas (HCL sources or live clusters, in any
   combination) and report the changes — or the migration DDL — between
   them; `-format json` emits a dependency-ordered, machine-readable plan.

Additional commands:

- **plan** — diff every node role in a desired `-manifest` against either a
  live `-dump` topology or a previous `-from-manifest` composition, emitting a
  single globally-ordered, cross-role operation list (storage before its
  Distributed/Buffer proxies before the MV). See
  **[Cross-role planning](#cross-role-planning)** and the runnable
  **[`examples/manifest/`](examples/manifest/)**.
- **drift** — detect cross-node schema drift across per-node HCL dumps.
- **locate** — find every declaration site of an object across manifest
  layers and per-node dumps; `-duplicates` audits the once-only rule.
- **dump-cluster** — enumerate a cluster's nodes and dump one `<host>.hcl`
  per node. **dump-sql** — dump a database's CREATE statements as replayable
  DDL.
- **sql2hcl** — apply ClickHouse DDL edits to an HCL schema and emit
  updated HCL.
- **web** — serve a read-only web UI to browse a resolved schema.
- **github-token** — mint a short-lived GitHub App installation token.

Run `hclexp <command> -h` for command-specific flags.

Connections can be plaintext (default) or TLS (see
**[TLS / secure connections](#tls--secure-connections)**). `hclexp` also
ships as a minimal container image (see **[Container image](#container-image)**)
for use as a deployment-time schema-dump hook.

## Build

```bash
go build -o hclexp ./cmd/hclexp
```

ClickHouse connection defaults come from environment variables and can be
overridden by flags:

| Variable                      | Default          |
|-------------------------------|------------------|
| `CLICKHOUSE_HOST`             | `localhost`      |
| `CLICKHOUSE_PORT`             | `9000`           |
| `CLICKHOUSE_DB`               | `migration_test` |
| `CLICKHOUSE_USER`             | `user1`          |
| `CLICKHOUSE_PASSWORD`         | `pass1`          |
| `CLICKHOUSE_SECURE`           | `false`          |
| `CLICKHOUSE_TLS_SKIP_VERIFY`  | `false`          |

For TLS-only clusters (typically port `9440`), set `CLICKHOUSE_SECURE=true`
— or pass `-secure` on the CLI, or `?secure=true` on the diff URL form.
See **[TLS / secure connections](#tls--secure-connections)** below.

## Introspect a live database

```bash
# Dump a database to stdout as HCL
hclexp introspect -database posthog

# Dump several databases, one <db>.hcl file per database, into a directory
hclexp introspect -database posthog,system -out ./schema/

# Dump to a single file
hclexp introspect -database posthog -out posthog.hcl

# Override connection details
hclexp introspect -host ch.example.com -port 9000 -user ro -password secret \
  -database posthog -out ./schema/

# TLS-only cluster on port 9440, internal CA → skip cert verification
hclexp introspect -host ch.prod.internal -port 9440 -user readonly \
  -secure -tls-skip-verify -database posthog -out ./dump/
```

**Flags:**

- `-database` — comma-separated list of databases to introspect (required)
- `-node` — name for the emitted `node {}` block; defaults to the server's
  `hostName()`
- `-host`, `-port`, `-user`, `-password` — connection overrides
- `-secure` — connect over TLS (matches `CLICKHOUSE_SECURE`)
- `-tls-skip-verify` — skip server-cert verification (requires `-secure`;
  matches `CLICKHOUSE_TLS_SKIP_VERIFY`)
- `-out` — output target:
  - omitted → write HCL to stdout
  - a directory → write one `<database>.hcl` per database
  - any other path → write all databases to that single file
- `-allow-raw` — capture objects whose `CREATE` DDL can't be parsed or
  expressed as a `raw {}` block instead of failing (see below)
- `-show-secrets` — capture real secret values (dictionary source passwords,
  named-collection params) instead of the redacted `[HIDDEN]`. Off by default;
  requires the server's `display_secrets_in_show_and_select = 1` and the
  `displaySecretsInShowAndSelect` grant. **Writes real secrets to the output —
  handle with care.** See [docs/secrets.md](docs/secrets.md).

Introspection reads each object's `create_table_query` and parses it with
the ClickHouse SQL parser, so columns (types, defaults, codecs, comments,
`MATERIALIZED`/`ALIAS`/`EPHEMERAL`), indexes, constraints, engine +
parameters, `ORDER BY`, `PARTITION BY`, `SAMPLE BY`, `PRIMARY KEY`, `TTL`,
and `SETTINGS` all come back populated. Materialized views (TO-form),
plain views (with `column_aliases`, `sql_security`, `definer`, comment),
dictionaries (every supported source + layout kind), and named
collections are dumped in the same pass.

Each dump also gets a top-level `node {}` block recording the source
node's name and ClickHouse macros (`shard`, `replica`, `hostClusterRole`,
`hostClusterType`, …) read from `system.macros`. It's metadata only —
`hclexp diff` ignores it — and exists so `hclexp drift` (below) can group
nodes by their authoritative identity.

Introspection is **strict by default**: an object whose DDL the parser can't
handle, or that uses an engine/form the HCL model can't express, aborts the
dump with an error. Pass `-allow-raw` to capture such objects verbatim as
`raw "<kind>" "<name>" { sql = ... }` escape-hatch blocks (with a warning)
and continue, so one unusual object never breaks the whole dump.
`hclexp dump-cluster` takes the same flag. Raw blocks are opaque — diffed as
text and recreated (`DROP` + `CREATE`) on change, with a `table`-kind change
flagged `-- UNSAFE`. See [`docs/README.hcl.md`](docs/README.hcl.md#raw) for
the full reference.

## Dump a whole cluster

`hclexp dump-cluster` enumerates every node of a named cluster from an
entry host's `system.clusters` and introspects **each node natively**,
writing one `<short-host>.hcl` per node — the per-node dumps that `drift`,
`plan`, and `locate -dump` consume.

```bash
hclexp dump-cluster -host entry.prod.internal -cluster posthog \
  -database posthog,system -out-dir ./prod/eu -allow-raw -exclude exclude.hcl
```

- `-cluster` — the `system.clusters` name to enumerate (required)
- `-out-dir` — output directory (required). Existing `*.hcl` files in it are
  removed first, so decommissioned nodes disappear from the dump.
- `-database`, `-allow-raw`, `-exclude`, and the connection/TLS flags work
  exactly as in `introspect`, applied on every node.
- Per-node failures are non-fatal: the run logs the node, continues, and
  reports the failure count at the end — one unreachable replica doesn't
  lose the fleet dump.

Enumeration and introspection use the native protocol from your machine to
each node; the entry host only supplies the node list.

## Load & resolve an HCL schema

```bash
# Load a single HCL file, resolve it, print a summary
hclexp -config ./schema/posthog.hcl

# Load a layer stack (applied left to right); an entry may be a dir or an .hcl file
hclexp -layer ./schema/base,./schema/env_us,./schema/nodes/ingest.hcl

# Write the resolved schema out as canonical HCL
hclexp -config ./schema/posthog.hcl -out ./resolved.hcl
```

**Flags:**

- `-config` — path to a single HCL file (default `./cmd/hclexp/node.conf`)
- `-layer` — comma-separated layer stack, loaded in order; each entry is a
  directory (every `*.hcl` in it) or a single `.hcl` file
  (mutually exclusive with `-config`)
- `-out` — if set, write the resolved schema as canonical HCL to this path
- `-exclude` — HCL exclude config (`patterns` + `object_types`, the same file
  `diff`/`drift`/`plan` consume); matching objects are dropped from the
  emitted schema
- `-exclude-objects` — comma-separated name globs (bare or `db.name`) dropped
  from the emitted schema
- `-only` — comma-separated name globs; keep **only** the matching objects

`-only`/`-exclude-objects` make layer factoring two `load` calls instead of a
hand-written HCL parser (which silently loses shapes like the two-label
`raw "dictionary" "x" {}`):

```bash
# the shared layer: only the objects identical everywhere
hclexp load -layer overrides/data/dev -only "$LIST" -out overrides/data/cloud/tables.hcl

# each env layer: everything except those
hclexp load -layer overrides/data/dev -exclude-objects "$LIST" -out overrides/data/dev/tables.hcl
```

An object survives iff it matches `-only` (when given) and neither exclusion.
Filtering removes objects only: the `database {}` wrapper survives even when
emptied and `node {}` blocks are untouched, so the two halves of a split are
exact complements.

### Compose from a manifest

With `-manifest`/`-env` the layer stack comes from the same role manifest
`validate` and `plan` consume, so callers never rebuild it by hand
(mutually exclusive with `-layer`/`-config`):

```bash
# One role to stdout (or -out FILE)
hclexp load -manifest manifest.hcl -env prod-us -layer-root ./schema -role ops

# Every role deployed in the env, one file per role into -out (a directory)
hclexp load -manifest manifest.hcl -env prod-us -layer-root ./schema -out ./golden

# Per-env tree instead of the flat default: golden/prod-us/ops.hcl
hclexp load -manifest manifest.hcl -env prod-us -layer-root ./schema \
  -out ./golden -out-name '{env}/{role}'

# The resolved layer stacks themselves (no composition; works before the
# layer dirs exist)
hclexp load -manifest manifest.hcl -env prod-us -format json
```

- `-role` — compose only this role (default: every role deployed in `-env`)
- `-layer-root` — root directory the manifest's layer paths resolve under
- `-out-name` — file name template for roles written into the `-out`
  directory (default `{env}-{role}`, i.e. the flat `<env>-<role>.hcl`
  layout). `{env}` and `{role}` expand, `.hcl` is appended, and template
  subdirectories are created, so `'{env}/{role}'` writes
  `golden/<env>/<role>.hcl` directly. Unknown placeholders, paths escaping
  `-out`, and two roles rendering to the same path are errors.
- `-format json` — emit each role's declared and resolved layer stack
  instead of composing

## Diff two schemas

`hclexp diff` reports the changes needed to turn a **left** schema into a
**right** schema. Either side can be an HCL source *or* a live ClickHouse
instance, so you can diff config-vs-config, config-vs-cluster, or
cluster-vs-cluster.

```bash
# Local HCL file vs. a live cluster
hclexp diff -left ./schema/posthog.hcl \
            -right clickhouse://user:pass@ch.example.com:9000/posthog

# Layered config vs. a single resolved file
hclexp diff -left ./schema/base,./schema/env_us -right ./resolved.hcl

# Two clusters
hclexp diff -left  clickhouse://localhost:9000/posthog \
            -right clickhouse://staging:9000/posthog

# Emit migration DDL (left -> right) instead of a summary
hclexp diff -left ./schema/posthog.hcl \
            -right clickhouse://localhost:9000/posthog -sql

# Diff against a TLS-only cluster with an internal CA
hclexp diff -left ./schema/posthog.hcl \
            -right 'clickhouse://ro:secret@ch.prod.internal:9440/posthog?secure=true&skip-verify=true'
```

**Side specs** (`-left` / `-right`): each is one of

- a comma-separated layer stack, loaded + resolved in order; each entry is a
  directory or a single `.hcl` file (so one `.hcl` path is the common case)
- `clickhouse://[user[:password]@]host:port/db1[,db2][?secure=true[&skip-verify=true]]`
  — introspected live; missing connection pieces fall back to the
  `CLICKHOUSE_*` defaults. The optional `secure` / `skip-verify` query
  params switch on TLS (see below).

**Flags:**

- `-left`, `-right` — the two schemas to compare (both required)
- `-scope all|left|right` — object ownership for the comparison. `all` is the
  default exact diff. `left` ignores right-only objects; `right` ignores
  left-only objects. Scoping removes whole unmanaged objects only: once an
  object is present on the scope side, all of its columns, engine, TTL,
  settings, and other fields still compare exactly.
- `-ignore-column-order` — compare table and materialized-view columns by name
  and definition while ignoring declaration order. By default physical column
  order is significant because it affects `SELECT *`, positional inserts, and
  dump convergence.
- `-sql` — emit the migration DDL (`CREATE` / `ALTER` / `DROP`) that turns
  the left side into the right side, instead of the change summary.
  Changes ClickHouse can't apply in place (engine swap, `ORDER BY`,
  `PARTITION BY`, `SAMPLE BY`) are flagged with `-- UNSAFE` comments.
  Heavy operator-run statements (currently `MATERIALIZE INDEX`, generated
  alongside every `ADD INDEX` on an existing table) are printed commented
  out as `-- MANUAL:` lines — run them deliberately, never as part of an
  automated apply. In `-format json` output the same statements carry
  `"manual": true`.

Comparison is semantic rather than rendered-text based: supported SQL
expressions are canonicalized and equivalent table TTL forms such as
`toIntervalMonth(3)` and `INTERVAL 3 MONTH` compare equal without losing a TTL
`WHERE` condition. When adding a column before an existing target column,
generated SQL includes `FIRST` or `AFTER <column>` so applying the migration
converges to the right-hand declaration order. Reordering columns that already
exist is reported for explicit review; hclexp does not auto-generate those
`MODIFY COLUMN ... FIRST/AFTER` statements.

This supports a reference schema that owns only part of a live node:

```bash
# Managed drift: live-only objects are outside the reference's ownership set.
hclexp diff -left ./reference -right ./clickhouse-schema/node-01.hcl \
  -scope left -format json

# Correct production back to the reference without dropping live-only objects.
hclexp diff -left ./clickhouse-schema/node-01.hcl -right ./reference \
  -scope right -sql
```

Use an unscoped exact diff for migrations between repository revisions:
`hclexp diff -left ./reference -right ./proposed -format json`. Every managed
drift must first be corrected in production or accepted into the reference;
the reviewed migration should never be adapted to an unresolved live state.

The default output is an indented `+`/`-`/`~` summary:

```
database "posthog"
  + table new_table
  - table old_table
  ~ table events
      + column event String
      ~ column team_id: UInt32 -> UInt64
      + setting index_granularity = 8192
```

## Validate dependencies

`hclexp validate` checks that every cross-object reference in a resolved
schema can actually be satisfied — without connecting to ClickHouse.

It's the static guard the diff/apply path relies on:

- A **`materialized_view`** reads from source tables (named in its `query`)
  and writes into `to_table`. Both must be declared somewhere in the
  loaded schema — the source may also live on a mapped sibling cluster
  (see below), the destination must be local.
- A **`distributed`-engine table** forwards to the table named by its
  `cluster_name` / `remote_database` / `remote_table`. The remote must be
  declared on the node itself, on a mapped cluster (see below), or in the
  built-in `system` database. Once the remote resolves, the proxy's columns
  are checked against it (see [Distributed proxy columns](#distributed-proxy-columns)).

Missing references — or references into a database that wasn't loaded —
fail with a non-zero exit code. The MV `query` is parsed to discover its
source tables; `WITH ... AS` CTE names are not treated as table references.
References into the built-in `system` database are always satisfied.

```sh
# Validate a single-file schema
hclexp validate -config ./schema/posthog.hcl

# Validate a layer stack
hclexp validate -layer ./schema/base,./schema/env_us

# Skip dependency checks for specific objects, or all of them
hclexp validate -config ./schema/posthog.hcl -skip-validation=events_mv,events_dist
hclexp validate -config ./schema/posthog.hcl -skip-validation='*'
```

### Cross-cluster references

A `Distributed` proxy routinely forwards to a storage table that lives on
**another cluster's** composition — the remote database is `posthog` on
every cluster, so `cluster_name` is the only discriminator. Map each such
cluster to the layer stack that composes it with the repeatable
`-cluster NAME=STACK` flag, and the proxy's remote is resolved against that
cluster's schema. `STACK` is a layer stack (directories or `.hcl` files) joined
by the OS list separator (`:`), so it never clashes with the comma that
separates `-layer` entries. Two sentinel forms stand in for a stack:

- `NAME=@absent` — a cluster with no composition in this env; references into
  it are structurally unresolvable and count as satisfied.
- `NAME=@alias=BASE` — a ClickHouse `remote_servers` alias (e.g.
  `posthog_writable`, `batch_exports_primary_replica`) that shares `BASE`'s
  composition; the remote resolves against `BASE` (which may itself be mapped
  or `@absent`). This avoids re-listing the same stack under every alias.

```sh
hclexp validate -layer ./nodes/data \
  -cluster aux=./nodes/aux \
  -cluster ai_events=./base:./nodes/ai_events \
  -cluster aux_writable=@alias=aux \
  -cluster events_recent=@absent
```

The same mappings also resolve **materialized-view and plain-view sources**:
a `SELECT ... FROM posthog.foo` carries no cluster name, so a source missing
from the node is satisfied when `foo` is declared in *any* mapped cluster —
the co-located composition the server sees at runtime. `@absent` clusters
contribute no schema and never satisfy a view source (the table must really
exist somewhere); only `Distributed` remotes, which name their cluster
explicitly, are satisfied by `@absent`.

With **no** `-cluster` mapping an off-node remote is an error — a new
cross-cluster proxy can't be silently accepted: map its cluster, mark it
`@absent`, or `-skip-validation` the proxy. This lets a caller replace a
hand-maintained skip list with a generated cluster mapping that actually
checks the remotes exist.

#### Deriving clusters from per-node dumps

Use `-dump` when the input is the directory produced by `dump-cluster`, rather
than a declared layer composition or role manifest:

```sh
# Validate every node and print the derived cluster map plus per-node findings.
hclexp validate -dump ./clickhouse-schema/prod-eu

# Select part of the topology, apply the normal validation controls, and emit
# stable structured output for CI.
hclexp validate -dump ./clickhouse-schema/prod-eu \
  -glob '*[fg].hcl,*-offline.hcl' \
  -exclude ./exclude.hcl \
  -skip-validation legacy_proxy \
  -strict-proxy-columns \
  -format json
```

A dump directory is **not** a layer stack. Each `.hcl` file is loaded and
resolved independently, so the same table declared by 58 peer nodes is not a
redefinition conflict. Validation then builds a topology index:

1. Nodes are grouped by their `node.macros.cluster` value.
2. Each cluster exposes the union of object names declared by all of its
   members. This is a reference-resolution union, not a merged HCL schema;
   heterogeneous members can therefore contribute different objects.
3. Referenced remote-server variants ending in `_writable`, `_single_shard`,
   or `_primary_replica` are inferred as aliases when their base cluster exists
   in the dump.
4. Explicit `-cluster` flags are applied last and override the derived mapping,
   including `@alias=BASE` and `@absent`.

The text output prints this derived map before the per-node results. JSON emits
the same assumptions under `clusters`, including every mapping's source
(`dump`, `inferred`, or `explicit`), member nodes, and alias base. It also emits
per-node findings and an aggregate `summary` suitable for CI.

A `cluster_name` that has neither a member node, an inferred base, nor an
explicit mapping is an error. It is reported once under `unmapped_clusters`
with all referencing nodes, rather than repeated once for every Distributed
table. Nodes without `macros.cluster` are still validated, but are listed under
`unclustered_nodes` and contribute to no derived cluster. `-glob`,
`-skip-validation`, `-strict-proxy-columns`, `-strict-clusters`, and `-exclude`
all apply to dump mode.

#### Deriving clusters from the role manifest

Instead of listing every cluster as a flag, `-manifest`/`-env` derive the
mappings from the same role manifest `plan`/`web` consume. A ClickHouse cluster
is composed of nodes from one or more roles, so a `cluster` block lists its
member roles and its schema is their **union** for the selected env:

```hcl
role "data"             { env "prod-us" { layers = ["layers/base", "layers/env/prod-us"] } }
role "ingestion-events" { env "prod-us" { layers = ["layers/base", "layers/ingestion", "layers/env/prod-us"] } }

cluster "posthog" {
  roles   = ["data", "ingestion-events"]
  aliases = ["posthog_writable", "posthog_single_shard"]
}

# A cluster with no composition in the manifest (modeled elsewhere) is declared
# absent instead of with roles; proxies into it resolve as satisfied.
cluster "events_recent" {
  absent = true
}
```

```sh
hclexp validate -layer ./nodes/data -manifest roles.hcl -env prod-us -layer-root .
```

Each cluster resolves against the union of its member roles' compositions;
aliases resolve to their base. A cluster whose member roles aren't deployed in
the env (or that sets `absent = true`) is treated absent. Explicit `-cluster`
flags are applied last, so they override or extend the manifest (e.g.
`-cluster events_recent=@absent`). A cluster that references an undeclared role,
or sets both `roles` and `absent`, is rejected.

Pass **`-strict-clusters`** to forbid absence entirely: every Distributed remote
must resolve against a real composition, so a remote on an `@absent` cluster
becomes an error. Use it as the CI gate once the whole fleet is composed, so a
stale `@absent` (a cluster that has since been mapped) cannot silently pass.

#### Validating a whole environment

With `-manifest`/`-env` and **no** `-layer`/`-config`, validate runs in
manifest-driven mode: it validates **every role** in the manifest for that env,
each against the cluster set derived from the whole manifest. One command checks
the entire environment instead of a shell loop over nodes; failures are prefixed
with the role they came from.

```sh
hclexp validate -manifest roles.hcl -env prod-us -layer-root .
# validation error: [role data] posthog.web_stats: Distributed table column ...
```

### Distributed proxy columns

Once a `Distributed` remote resolves to an inspectable table (locally or via a
`-cluster` mapping), the proxy's columns are checked against the remote storage
table. By default the proxy's columns must be a **subset** of the remote's:
every forwarded proxy column must exist on the remote with the same type and
nullability. The comparison is type-only — a proxy legitimately drops the
remote's `CODEC` / `DEFAULT` / `TTL` / `COMMENT`. `ALIAS` and `EPHEMERAL`
columns are computed or insert-only, not forwarded, and are ignored on both
sides.

A proxy column absent from the remote, or with a differing type, is an error —
this catches drift where the storage table renamed, retyped, or dropped a
column the proxy still exposes. Pass `-strict-proxy-columns` to also require the
reverse (the remote's columns must all exist on the proxy), i.e. an exact
mirror.

```sh
# default: proxy columns must be a subset of the remote's, types must match
hclexp validate -layer ./nodes/data -cluster aux=./nodes/aux

# strict: proxy and remote must have exactly the same forwarded columns
hclexp validate -layer ./nodes/data -cluster aux=./nodes/aux -strict-proxy-columns
```

The comparison engine (`diff`, `plan`, `drift`) applies the same subset idea
to one narrow case: a Distributed proxy whose `remote_database` is `system`.
System tables are server-defined and gain columns with version bumps, so a
live proxy created from a fuller set routinely carries columns the layer
intentionally omits — column *presence* differences on such proxies are not
reported (in either direction), while columns declared on both sides still
compare fully. Non-system proxies keep exact column semantics.

`hclexp diff -sql` applies the same dependency knowledge to DDL ordering:
within the generated migration, a table is created before any
Distributed/MV/Dictionary that depends on it, and dropped after.

## Detect cross-node drift

`hclexp drift` compares the per-node HCL dumps in a directory and reports
where nodes that should share a schema don't. It's built for fleet dumps
like `prod/eu/*.hcl` — one file per node, each carrying a `node {}` block
with that node's macros (see [Introspect](#introspect-a-live-database)).

```sh
# Compare every node, grouped by the hostClusterRole macro
hclexp drift -dir prod/eu

# Compare just one pool via a filename glob
hclexp drift -dir prod/eu -glob '*ingestion-small*'

# Group by the deployment role parsed from the node name; show full diffs
hclexp drift -dir prod/eu -group-by role -details
```

Within each group every node is diffed — via the same engine as `hclexp
diff` — against the lexically-first **reference** node, and a one-line
change summary is printed per drifting node. The command exits non-zero
when any drift is found, so it doubles as a CI guard.

**Flags:**

- `-dir` — directory of per-node `.hcl` dumps to compare (required)
- `-glob` — comma-separated filename globs selecting dumps within `-dir`
  (default `*`); a file is included if it matches **any** pattern. Use it
  to hand-pick exactly the nodes you want compared, e.g.
  `'*ingestion-small*'` for one pool, or `'*-ch-*[fg].hcl,*-offline.hcl'`
  to pull all DATA nodes (online `f`/`g` replicas plus offline `h`)
  together into one comparison.
- `-group-by` — comma-separated keys to group nodes by (default
  `hostClusterRole`). Each key is looked up first in the node's macros,
  then as one of the pseudo-keys `role` / `shard` / `replica` parsed from
  the node name (`prod-<region>-<az>-ch-<shard><replica>[-<role>]`).
  Examples: `-group-by role`, `-group-by hostClusterRole,hostClusterType`.
- `-zk-paths` — how to treat ReplicatedMergeTree `zoo_path` before diffing
  (default `mask-uuid`):
  - `mask-uuid` — replace the literal table UUID with the `{uuid}` macro.
    ClickHouse expands `{uuid}` to the table's own UUID at `CREATE` time
    (while keeping `{shard}`/`{replica}` as macros), so the same table on
    different shards otherwise looks like drift. Masking compares the
    *intended* path; genuine differences (e.g. a different database in the
    path) still drift.
  - `keep` — compare paths verbatim (no normalization).
  - `ignore` — blank `zoo_path`/`replica_name` entirely.
- `-details` — print the full change set of each drifting node against its
  group reference, instead of just the one-line summary
- `-ignore-column-order` — ignore table and materialized-view column order for
  the whole drift run; column names, types, and modifiers still compare

Example output:

```
group "ingestion" — 22 nodes, reference prod-eu-fra-ch-10a-ingestion-events — 6 drifting
  ✗ prod-eu-fra-ch-1a-ingestion-medium: +16 table, -8 table, +8 mv, -4 mv
  ✗ prod-eu-fra-ch-1a-ingestion-small: +25 table, -8 table, +10 mv, -4 mv
group "ops" — 2 nodes, reference prod-eu-fra-ch-1a-ops — OK (all identical)

summary: 58 nodes, 8 groups, 2 groups with drift, 28 drifting nodes
```

> **Tip:** `hostClusterRole` is coarse — it can lump distinct pools
> together (e.g. `ingestion` covers ingestion-events / -medium / -small,
> and `data` covers online and offline nodes). `-group-by role` uses the
> finer deployment role from the node name and usually isolates genuine
> drift.

## Decompose node dumps into layers

`hclexp decompose` turns a dump-repository layout (one directory per
environment, one HCL file per node) into deterministic shared and
environment-specific layers:

```bash
hclexp decompose \
  -dump-root ../clickhouse-schema \
  -env dev,prod-eu,prod-us \
  -out ./generated-schema
```

The command first verifies that selected replicas for each environment and
`hostClusterRole` agree. It places objects identical in every environment
under `layers/shared/<role>`, puts partial-presence objects in environment
layers, and emits exact `patch_table` deltas where differing table shapes can
be represented without loss. Added columns carry `first` / `after` anchors
computed from the target physical order. Unsupported shared splits fall back
to complete per-environment declarations in automatic mode.

Every object kind hclexp can load is included: tables, materialized views,
views, dictionaries, raw escape-hatch objects, and cluster-scoped named
collections. Decompose uses `patch_materialized_view`, `patch_view`,
`patch_dictionary`, and named-collection overrides when their vocabulary can
represent the delta exactly. Recreate-only or otherwise unrepresentable
changes use complete environment declarations in `auto` mode; an explicit
`shared` assignment fails instead of weakening the requested layout.

Before writing, every generated stack is loaded and composed again and
compared to its normalized input dump with column order enabled. A failed
round trip writes nothing. The output also includes canonical goldens and a
generated-file ledger; later runs remove only stale files recorded in that
ledger and never delete untracked files in the output directory.

Use `-list` to inspect the cross-environment inventory as JSON. Persistent
human decisions live in an optional, versioned JSON assignment file:

```json
{
  "version": 1,
  "baseline_env": "prod-eu",
  "objects": {
    "ops/posthog/table/events_recent": { "mode": "environment" },
    "ops/posthog/table/temporary_table": { "mode": "exclude" }
  }
}
```

Object modes are `auto`, `shared`, `environment`, and `exclude`. `shared` is a
strict assertion: if an object is absent or its delta cannot be expressed by
the patch vocabulary (for example, existing columns appear in a new order),
decomposition fails with the object and conflicting columns instead of
silently reordering them. Assignment keys are validated so stale or mistyped
decisions cannot be ignored. `-exclude` applies the standard HCL exclude rules
before inventory and decomposition. ReplicatedMergeTree UUIDs are masked by
default; use `-zk-paths keep|mask-uuid|ignore` to select another policy.

## Cross-role planning

`hclexp plan` diffs **every role in a manifest** in one run and emits a single
globally-ordered, cross-role operation list —
storage tables before the Distributed/Buffer proxies that front them,
proxies before the MVs that read them — with `roles` provenance on every
operation. It has two deliberately separate current-state modes.

For managed convergence against a live topology, use `-dump -scope desired`:

```bash
hclexp plan -manifest manifest.hcl -env prod-us -layer-root ./schema \
  -dump ./prod/us -scope desired -format json \
  | jq '.operations[] | {order, kind, object_type, object, roles}'
```

Desired state is each role's composed layer stack from the manifest;
current state is the matching node in the dump (nodes matched by their
`hostClusterRole` macro, replicas collapsed to one representative per
role; a role absent from the dump plans as all-CREATE). `desired` ignores
live-only objects but still creates missing managed objects and reconciles all
fields inside managed objects. The default `-scope all` is exact and may emit
DROPs for live-only objects. Desired scope cannot represent an intentional
deletion from the reference.

For a deterministic migration, compare the previous and proposed manifest
compositions exactly. Resolve the deployed revision into files (for example,
with a temporary Git worktree) and pass it as `-from-manifest`:

```bash
git worktree add /tmp/chschema-reference "$DEPLOYED_SHA"
hclexp plan \
  -from-manifest /tmp/chschema-reference/schema/manifest.hcl \
  -from-layer-root /tmp/chschema-reference/schema \
  -manifest schema/manifest.hcl -layer-root schema -env prod-us \
  -format json
```

`-from-manifest` and `-dump` are mutually exclusive. A proposed-only role
plans as all-CREATE; a previous-only role is rejected because decommissioning
a role must be explicit. `-format text` prints the same ordered list
human-readably. Require the reference-scoped live drift check to be empty
immediately before applying this reference-to-proposed plan.

See **[Cross-role planning](docs/README.hcl.md#cross-role-planning--hclexp-plan)**
in the reference for the manifest format, and
**[`examples/manifest/`](examples/manifest/)** for a runnable
two-role × three-environment example.

## Locate declarations

`hclexp locate` answers "where does a name live?" across a manifest's
whole layer tree: every declaration site of each matching object
(tables, materialized views, views, dictionaries, named collections,
raw blocks), with its inheritance markers and the `(role, env)` stacks
whose layer lists include each declaring layer. It is query-only:
nothing is resolved or diffed. See
**[Locating declarations](docs/README.hcl.md#locating-declarations--hclexp-locate)**
for the full reference.

```sh
# Every declaration site of an object, plus where those layers deploy
hclexp locate -manifest manifest.hcl -layer-root ./schema posthog.events

# Globs match the bare name and the db.name qualified form; several
# patterns are independent existence checks (exit 1 if any finds nothing)
hclexp locate -manifest manifest.hcl -layer-root ./schema 'events*' person

# Also report which per-node dump files declare it
hclexp locate -manifest manifest.hcl -layer-root ./schema -dump prod/eu events

# Ad-hoc layer dirs or files, before a manifest exists (no placement info)
hclexp locate -layer roles/shared,roles/ops/prod events

# CI guard: every object defined at more than one site, even
# across layers that never co-compose into one stack
hclexp locate -manifest manifest.hcl -layer-root ./schema -duplicates
```

Example output:

```
table posthog.events
  roles/shared/events.hcl:7  extends events_base
      (ops, prod-us), (ops, prod-eu), (ingest, prod-us)
  roles/ops/prod/events.hcl:2  [override]
      (ops, prod-us), (ops, prod-eu)
  roles/ops/prod/events.hcl:9  [patch_table]
      (ops, prod-us), (ops, prod-eu)
  dump: dumps/node1.hcl:2  (node prod-ch-1a)
```

Each site is marked when it is `[abstract]`, an `[override]`
redeclaration, an object-specific `[patch_*]`, an `extends <parent>` child,
or a `[raw <kind>]` block. An object extended by others lists its children
(`extended by: ...`), and each dump site names its node (from the dump's
`node {}` block, else the filename).

**Flags:**

- `-manifest` — the same role manifest `plan`/`validate`/`load` consume.
  Unlike those commands there is no `-env`: locate scans the union of
  every layer named by any `(role, env)` stack and derives placement for
  all of them.
- `-layer-root` — root directory the manifest's layer paths resolve
  under (default `.`)
- `-layer` — comma-separated ad-hoc layer dirs or `.hcl` files to search
  too (or instead of a manifest); resolved as given, no placement info,
  deduped against the manifest's layers
- `-dump` — directory of per-node `.hcl` dumps (as written by
  `introspect`/`dump-cluster`); reports which node files also declare
  each matching object
- `-format` — `text` (default) or `json` (a `{"patterns": [...],
  "objects": [...]}` / `{"duplicates": [...]}` document with per-site
  file/line/layer/markers/placements)
- `-duplicates` — takes no name argument and requires `-manifest` or
  `-layer` (mutually exclusive with `-dump`); lists every object defined
  at more than one site and exits non-zero when any is found. Patch sites,
  `override = true` redeclarations, and declarations carrying `extend` are
  refinements rather than definitions and do not count. Abstract declarations
  do count: copying the same abstract schema is still duplication. Reported
  groups retain every site, including refinements.

Exit codes: `0` on success, `1` when any pattern matches nothing (a
scriptable existence check) or `-duplicates` finds any, `2` on usage
errors.

## Browse the schema in a web UI

`hclexp web` serves a read-only UI for resolved schemas — databases,
objects, their columns/engine/settings, dependency cross-links, and data
flows — with no cluster connection. The header lookup finds objects by a
case-insensitive partial name, qualified name, database, or kind.

```bash
# One schema, from a config file or a layer stack
hclexp web -layer ./schema/base,./schema/envs/us -addr :8080

# Every (env, role) a manifest composes: a schema list at /, each
# composition under /s/<env>/<role>/
hclexp web -manifest manifest.hcl -layer-root ./schema

# Every per-node schema in a dump directory: a node list at /, each dump
# kept independent under /n/<node>/
hclexp web -dump ../clickhouse-schema -glob '*.hcl'
```

On the manifest or dump list page, lookup searches every schema and includes
the owning composition/node in each result. After opening one schema, the same
lookup box searches only that schema. Dump nodes are grouped by their `cluster`
macro, falling back to `hostClusterRole`; two files declaring the same node name
are rejected rather than silently combined. `-dump` is mutually exclusive with
the single-schema and manifest inputs, and `-glob` accepts the same
comma-separated filename patterns as `drift`.

The dump-node list links to **Review object differences**, a fleet-wide
inventory of every qualified object found in the loaded dumps. It uses the same
semantic diff as the CLI, summarizes uniform and differing schemas, groups
nodes by semantic schema variant, and links each non-baseline variant to the
full comparison view. Filters narrow the inventory by status or by database,
kind, object, cluster, and node. Object presence follows an all-or-none rule per
cluster. It is valid for an object to be absent from every loaded node of a
cluster, because different clusters can legitimately contain different object
sets. If the object exists on any node of a cluster, however, the review expects
it on every loaded node of that cluster and reports the missing nodes as
**inconsistent presence**. Presence inconsistencies are kept separate from
semantic schema differences.

In dump mode, every object page (table, materialized view, view, dictionary, or
raw object) also shows all cluster/node dumps containing the same qualified
object, ordered by cluster and then node. The current node is the comparison
baseline; peers are marked **same** or **different** by semantic comparison of
their resolved HCL. Click **different** to open a unified HCL diff with the
baseline and peer node identified explicitly. The comparison can swap its two
sides and lists every dumped node sharing either semantic schema variant. For
tables, comparison uses the same default as `drift`: literal UUIDs expanded into
ReplicatedMergeTree ZooKeeper paths are masked, while real column, engine, key,
TTL, setting, index, projection, constraint, and comment differences remain
visible as schema drift.

The **Ignore column order** control applies to the fleet review, individual
object markers, comparison groups, and patch generation for the entire browser
session. `hclexp web -dump ... -ignore-column-order` makes that the initial
session default; the control can still change it without restarting the server.

**Patch to uniform** previews the pretty-printed SQL needed to change the
right-hand node's object to match the left-hand baseline, using the same
migration planner as `hclexp diff -sql`. It never executes SQL. The preview
**must be reviewed** against the live cluster before use; unsafe or
unexpressible changes are called out explicitly and require manual
reconciliation.

Materialized-view `to_table` values and Distributed `remote_table` values are
links when their targets resolve. In dump mode the browser derives the same
cluster unions and well-known aliases (`_writable`, `_single_shard`, and
`_primary_replica`) as `validate -dump`, so cross-cluster destinations and MV
read sources link to the matching node and resolve consistently in validation,
dependency lists, and data flows. MV write destinations remain local, matching
ClickHouse execution semantics.

Navigation links preserve object context throughout the UI: node names open the
node schema, database names jump to stable database anchors, and object names
open the exact object view. The cross-node comparison matrix exposes all three
targets separately instead of making the node label stand in for the object.

The server auto-reloads on source edits: each request re-stats the source
files at most once per `-reload-interval` (default `2s`; `0` disables) and
reloads when a mod time changes — a broken edit keeps serving the last good
schema. Existing dump files are reloaded the same way; restart the server to
discover added or removed dump files. In manifest mode `-env` filters to one
environment. Try it against [`examples/manifest/`](examples/manifest/).

## Verify round-trip fidelity

`hclexp dump-sql` captures a database's `CREATE` statements (the `SHOW CREATE`
equivalent) as a replayable SQL file, and a gated test recreates that schema
through the full hclexp round-trip and asserts every `CREATE` is byte-identical —
so you can **dump production and verify it locally**.

```bash
# Capture a production schema
hclexp dump-sql -host prod-ch -database posthog -user … -password … -out prod.sql

# Replay it on a local ClickHouse and verify hclexp round-trips it exactly
docker compose up -d
ROUNDTRIP_FIXTURE=$PWD/prod.sql go test ./test -run TestLive_RoundTripFidelity -clickhouse
```

See [docs/roundtrip-fidelity.md](docs/roundtrip-fidelity.md) for the full
workflow and limitations.

## Apply SQL edits to the HCL schema

You already know ClickHouse DDL; `hclexp sql2hcl` applies it to an HCL
schema and emits the updated HCL, so a change you'd naturally write as SQL
never has to be hand-translated into blocks:

```bash
printf 'ALTER TABLE posthog.events ADD COLUMN plan LowCardinality(String);\n' | \
  hclexp sql2hcl -left ./schema -database posthog -in - -out ./resolved.hcl
```

Supported: `CREATE TABLE/MATERIALIZED VIEW/VIEW/DICTIONARY` (add or replace
by name), `ALTER TABLE` add/drop/modify/rename column, add/drop index,
TTL and settings changes, `MODIFY QUERY`, `DROP …`, and `RENAME TABLE`.
Data/partition operations (`TRUNCATE`, `ALTER … DELETE`, `MATERIALIZE …`)
are rejected — this is schema editing, not execution. Output is the
resolved (flat) schema; pair it with `hclexp diff -sql` to preview the
migration the edit implies. See
**[SQL → HCL edits](docs/README.hcl.md#sql--hcl-edits--hclexp-sql2hcl)**
for the full statement list and flags.

## Mint a GitHub App token

`hclexp github-token` exchanges a GitHub App's credentials for a short-lived
(~1 h) **installation access token** and prints **only the token** to stdout, so
a workload can authenticate to GitHub as an App instead of a long-lived PAT. The
ops image already ships `hclexp`, so the JWT signing + token exchange happen in
Go — no `openssl`/shell needed in the (distroless) image.

```bash
# Private key from env (never touches disk); token captured by the caller
export GITHUB_APP_PRIVATE_KEY="$(cat app.private-key.pem)"
TOKEN=$(hclexp github-token -app-id 123456 -installation-id 7654321)

# …then push over HTTPS as x-access-token:$TOKEN
```

**Flags:**

- `-app-id` — GitHub App ID (required)
- `-installation-id` — the App's installation ID on the target org/repo (required)
- `-private-key-file` — path to the App private key PEM; defaults to the
  `GITHUB_APP_PRIVATE_KEY` env var (PKCS#1 or PKCS#8)
- `-repo owner/name` — optional: scope the token to a single repository and
  print the resolved `repository_selection` / `permissions` to stderr

It builds an RS256 JWT (`iss` = App ID, `exp` ≤ 10 min) signed with the private
key, `POST`s it to `/app/installations/{id}/access_tokens`, and prints the
resulting token. Diagnostics go to stderr and a failure exits non-zero, so
`TOKEN=$(hclexp github-token …)` is safe. `HTTPS_PROXY`/`HTTP_PROXY` are honoured
(api.github.com egresses through Smokescreen in our clusters).

## TLS / secure connections

`hclexp` connects in plaintext by default. To reach a TLS-only cluster
(typically port `9440`), enable TLS via any of three equivalent forms:

| Form                          | Enable TLS                | Skip cert verification              |
| ----------------------------- | ------------------------- | ----------------------------------- |
| `hclexp introspect` flag      | `-secure`                 | `-tls-skip-verify`                  |
| Environment variable          | `CLICKHOUSE_SECURE=true`  | `CLICKHOUSE_TLS_SKIP_VERIFY=true`   |
| `clickhouse://` URL query     | `?secure=true`            | `?skip-verify=true`                 |

- Defaults are `false` — existing invocations behave identically.
- `-tls-skip-verify` / `?skip-verify=true` is only valid together with
  `-secure` / `?secure=true`; passing it alone is rejected to prevent
  silent misconfiguration.
- For public-CA certs the default verification path uses the system
  trust store; `-tls-skip-verify` is for internal/self-signed CAs.

```sh
# Introspect a TLS cluster with a private CA
hclexp introspect \
  -host ch.prod.internal -port 9440 -user readonly \
  -secure -tls-skip-verify \
  -database posthog,system \
  -out ./dump

# Diff local HCL against a TLS cluster
hclexp diff \
  -left ./schema \
  -right 'clickhouse://ro:secret@ch.prod.internal:9440/posthog?secure=true&skip-verify=true'
```

## Container images

Two multi-arch (`linux/amd64`, `linux/arm64`) images are built and pushed
on every `main` push and Git tag. Both bundle the same static `hclexp`
binary and share one tag stream each:

- `main` push → `main` + `sha-<short>` + `latest`
- `vX.Y.Z` tag → `X.Y.Z`, `X.Y`, `X`

| Image | Base | Contents | Use |
| ----- | ---- | -------- | --- |
| [`ghcr.io/posthog/chschema`](https://github.com/PostHog/chschema/pkgs/container/chschema) | distroless | `hclexp` only — no shell | default; minimal runtime for diff/introspect |
| [`ghcr.io/posthog/chschema-ops`](https://github.com/PostHog/chschema/pkgs/container/chschema-ops) | `alpine:3.20` | `hclexp` + `sh`, `git`, `curl`, `ca-certificates` | deploy-time schema-dump hook (needs a shell to git-commit dumps) |

Both packages are published **public** (the repo is public), so an EKS
cluster can pull them without credentials. If a package is ever flipped to
private, consumers must attach an `imagePullSecret` holding a GHCR token
with `read:packages`:

```sh
kubectl create secret docker-registry ghcr-pull \
  --docker-server=ghcr.io --docker-username=<gh-user> \
  --docker-password=<PAT-with-read:packages>
# then reference it via the workload's imagePullSecrets / serviceAccount
```

### `chschema` (distroless, default)

Minimal — distroless, no shell, no AWS CLI, no extras.

```sh
# Print usage
docker run --rm ghcr.io/posthog/chschema:latest -help

# Introspect into a host directory
docker run --rm \
  -e CLICKHOUSE_HOST=ch.prod.internal -e CLICKHOUSE_PORT=9440 \
  -e CLICKHOUSE_USER=readonly -e CLICKHOUSE_PASSWORD=secret \
  -e CLICKHOUSE_SECURE=true -e CLICKHOUSE_TLS_SKIP_VERIFY=true \
  -v "$PWD/dump:/dump" \
  ghcr.io/posthog/chschema:latest \
  introspect -database posthog,system -out /dump
```

It can be paired with `amazon/aws-cli` (or any other uploader) via a shared
`emptyDir` volume — `hclexp` writes HCL to the volume, the sidecar ships it.

### `chschema-ops` (shell-capable)

Same binary plus `sh`, `git`, and `curl`. It exists for the deploy-time
**schema-dump hook**: a Kubernetes Job in
[`posthog/charts`](https://github.com/PostHog/charts) (the
`clickhouse-schema-dump` PostSync hook) runs `hclexp introspect` against
each ClickHouse node and then `git`-commits the dumped HCL — which needs a
shell, git, and curl that the distroless image deliberately omits. The
`ENTRYPOINT` is still `hclexp`; override it (e.g. `--entrypoint sh`) to run
the surrounding dump-and-commit script.

```sh
docker run --rm --entrypoint sh ghcr.io/posthog/chschema-ops:latest -c \
  'hclexp introspect -database posthog -out /work && cd /work && git ...'
```

### Building locally

```sh
# Default (distroless)
docker build -t hclexp:dev .
docker run --rm hclexp:dev -help

# Ops image
docker build --target ops -t chschema-ops:dev .
docker run --rm --entrypoint sh chschema-ops:dev -c 'hclexp -h; git --version'
```

## HCL schema format

A schema is one or more `database` blocks, each containing `table` blocks.

```hcl
database "posthog" {
  table "events" {
    order_by     = ["timestamp", "team_id"]
    partition_by = "toYYYYMM(timestamp)"
    sample_by    = "team_id"
    ttl          = "timestamp + INTERVAL 2 YEARS"
    settings = {
      index_granularity = "8192"
    }

    column "timestamp" { type = "DateTime" }
    column "team_id"   { type = "UInt64" }
    column "event"     { type = "String" }

    column "payload" {
      type    = "String"
      codec   = "ZSTD(3)"
      comment = "raw event body"
    }

    index "idx_team" {
      expr        = "team_id"
      type        = "minmax"
      granularity = 4
    }

    constraint "team_positive" {
      check = "team_id > 0"
    }

    engine "replicated_merge_tree" {
      zoo_path     = "/clickhouse/tables/{shard}/events"
      replica_name = "{replica}"
    }
  }
}
```

### Column attributes

`type` is required; everything else is optional:

- `nullable` — wrap `type` in `Nullable(...)`
- `default`, `materialized`, `ephemeral`, `alias` — mutually exclusive
  default-value expressions (`DEFAULT` / `MATERIALIZED` / `EPHEMERAL` /
  `ALIAS`)
- `codec` — compression codec, e.g. `"ZSTD(3)"`
- `ttl` — per-column TTL expression
- `comment` — column comment
- `renamed_from` — previous column name; the diff engine emits
  `RENAME COLUMN` instead of drop + add

### Engine blocks

The engine block label is the engine kind. Supported kinds and their
attributes:

| Kind                                | Attributes |
|-------------------------------------|------------|
| `merge_tree`                        | — |
| `replicated_merge_tree`             | `zoo_path`, `replica_name` |
| `replacing_merge_tree`              | `version_column`, `is_deleted_column` |
| `replicated_replacing_merge_tree`   | `zoo_path`, `replica_name`, `version_column`, `is_deleted_column` |
| `summing_merge_tree`                | `sum_columns` |
| `replicated_summing_merge_tree`     | `zoo_path`, `replica_name`, `sum_columns` |
| `collapsing_merge_tree`             | `sign_column` |
| `replicated_collapsing_merge_tree`  | `zoo_path`, `replica_name`, `sign_column` |
| `aggregating_merge_tree`            | — |
| `replicated_aggregating_merge_tree` | `zoo_path`, `replica_name` |
| `distributed`                       | `cluster_name`, `remote_database`, `remote_table`, `sharding_key` |
| `log`                               | — |
| `kafka`                             | `broker_list`, `topic`, `consumer_group`, `format` |

### Materialized views

A `materialized_view` block declares a **TO-form** materialized view — it
continuously transforms inserts and writes the result into a separate
destination table.

```hcl
database "posthog" {
  materialized_view "app_metrics_mv" {
    to_table = "posthog.sharded_app_metrics"
    query    = "SELECT team_id, category FROM posthog.kafka_app_metrics"

    column "team_id"  { type = "Int64" }
    column "category" { type = "LowCardinality(String)" }
  }
}
```

| Attribute  | Required | Meaning |
|------------|----------|---------|
| `to_table` | yes      | destination table the MV writes into (`TO <db.>table`) |
| `query`    | yes      | the `AS SELECT ...` body |
| `column`   | yes      | the destination column list (name + type) |
| `cluster`  | no       | `ON CLUSTER` target |
| `comment`  | no       | view comment |

`hclexp diff` reports a changed `query` as an in-place `ALTER TABLE ...
MODIFY QUERY`; a changed `to_table` or column list is flagged unsafe
because it needs the view dropped and recreated.

**Not supported.** These fail introspection with a clear error rather than
being silently mishandled:

- inner-engine materialized views (`CREATE MATERIALIZED VIEW ... ENGINE = ...`)
- refreshable materialized views (`REFRESH EVERY|AFTER ...`)
- window views

### Views

A `view` block declares a ClickHouse **plain** (non-materialized) view — a
saved `SELECT` evaluated on every read of the view.

```hcl
database "posthog" {
  view "team_event_counts" {
    query = "SELECT team_id, count() AS n FROM posthog.events GROUP BY team_id"

    column_aliases = ["team_id", "n"]

    sql_security = "definer"
    definer      = "alice"

    cluster = "posthog"
    comment = "team-level event counter"
  }
}
```

| Attribute        | Required | Meaning |
|------------------|----------|---------|
| `query`          | yes      | the `AS SELECT ...` body (verbatim text) |
| `column_aliases` | no       | `CREATE VIEW v (a, b, ...) AS ...` |
| `sql_security`   | no       | `SQL SECURITY` clause: `definer`, `invoker`, or `none` (canonical lowercase; case-insensitive on parse) |
| `definer`        | no       | `DEFINER = <user>` or `DEFINER = current_user`; only valid alongside `sql_security = "definer"` |
| `cluster`        | no       | `ON CLUSTER` target |
| `comment`        | no       | view comment |

`hclexp diff` reports a body change as in-place `ALTER TABLE ... MODIFY
QUERY`; a comment-only change becomes `ALTER TABLE ... MODIFY COMMENT`;
any change to `column_aliases` / `sql_security` / `definer` / `cluster`
requires drop-and-recreate and is flagged unsafe.

**Not supported.** Live views, refreshable materialized views, and window
views fail introspection with a clear error.

### Dictionaries

A `dictionary` block declares a ClickHouse dictionary — a key/value
lookup loaded from an external source (another ClickHouse table, a
relational database, an HTTP endpoint, a file, etc.) and queried at
runtime via `dictGet*` functions. Credentials do not belong in HCL; the
recommended form refers to a named collection provisioned on the target
cluster:

```hcl
named_collection "exchange_rate_source" {
  external = true
  comment  = "credentials are provisioned by the cluster deployment"
}

database "posthog" {
  dictionary "exchange_rate_dict" {
    primary_key = ["currency"]
    lifetime { min = 3000  max = 3600 }
    range    { min = "start_date"  max = "end_date" }

    attribute "currency"   { type = "String" }
    attribute "start_date" { type = "Date" }
    attribute "end_date"   { type = "Nullable(Date)" }
    attribute "rate"       { type = "Decimal64(10)" }

    source "clickhouse" {
      collection = "exchange_rate_source"
      query      = "SELECT currency, start_date, end_date, rate FROM default.exchange_rate"
    }
    layout "complex_key_range_hashed" {
      range_lookup_strategy = "max"
    }
  }
}
```

| Block / attribute | Required | Meaning |
|-------------------|----------|---------|
| `primary_key`     | yes      | single or composite key column names |
| `attribute`       | yes      | one per column (`type` + optional `default` / `expression` / `hierarchical` / `injective` / `is_object_id`) |
| `source`          | yes      | exactly one — see supported kinds below |
| `layout`          | yes      | exactly one — see supported kinds below |
| `lifetime`        | no       | `{ min = <s>  max = <s> }` (range form) or just `{ min = <s> }` (simple `LIFETIME(n)`) |
| `range`           | no       | `{ min = "<col>"  max = "<col>" }` — only with `range_hashed` / `complex_key_range_hashed` layouts |
| `settings`        | no       | dictionary-level SETTINGS map |
| `cluster`         | no       | `ON CLUSTER` target |
| `comment`         | no       | dictionary comment |

**Supported source kinds:** `clickhouse`, `mysql`, `postgresql`, `http`, `file`, `executable`, `null`.
**Supported layout kinds:** `flat`, `hashed`, `sparse_hashed`, `complex_key_hashed`, `complex_key_sparse_hashed`, `range_hashed`, `complex_key_range_hashed`, `cache`, `ip_trie`, `direct`.

`clickhouse`, `mysql`, `postgresql`, and `http` sources accept an optional
`collection` attribute. It renders as ClickHouse's native `NAME` argument and
may be combined with non-secret overrides such as `table` or `query`:

```sql
SOURCE(CLICKHOUSE(NAME exchange_rate_source QUERY 'SELECT ...'))
```

Kinds outside these lists error during introspection with `unsupported dictionary source/layout kind: <name>`. Adding a new kind is a small change — one typed struct + one switch case in `dictionary_sources.go` / `dictionary_layouts.go`.

**Diff & apply.** ClickHouse has no useful in-place `ALTER DICTIONARY`. `hclexp diff` reports any non-empty change with `~ dictionary <name> (changed: ...)`; `-sql` emits a `CREATE OR REPLACE DICTIONARY` statement, which is the idiomatic ClickHouse update path and is treated as safe when every required runtime value is resolvable.

#### Runtime dictionary credentials

The credential boundary is deliberately cluster-side. `hclexp plan` is an
ordered statement producer, not a secret provider, and its JSON is commonly
stored as a CI artifact. Neither HCL nor `operation.sql` should contain a
dictionary password.

The execution contract is:

1. Before schema operations run, the deployment system creates the named
   collection on every target node, or makes it available through ClickHouse's
   Keeper-backed named-collection storage. It obtains the password from the
   environment's secret manager.
2. HCL declares that collection with `external = true`. This is a reference and
   ownership marker: hclexp validates the name but emits no CREATE/ALTER/DROP for
   the collection and never compares its values.
3. The dictionary source sets `collection = "..."`. hclexp emits
   `SOURCE(<KIND>(NAME <collection> ...))` with only non-secret overrides.
4. The executor sends that DDL unchanged. ClickHouse resolves `NAME` against
   the collection already present on that node, so the password enters the
   dictionary source only inside ClickHouse.

For example, the cluster bootstrap—not a committed migration—may execute a
secret-injected statement like:

```sql
CREATE NAMED COLLECTION exchange_rate_source AS
  host = 'localhost', user = 'dict_reader', password = '<from secret manager>';
```

The reviewed plan still contains only:

```sql
CREATE OR REPLACE DICTIONARY posthog.exchange_rate_dict (...)
SOURCE(CLICKHOUSE(NAME exchange_rate_source QUERY 'SELECT ...'))
LAYOUT(...);
```

Important operational details:

- The collection is a precondition. Because `external = true` emits no DDL, a
  missing collection makes ClickHouse reject the dictionary statement rather
  than silently creating a passwordless source.
- Per-node execution means the collection must be available on every node that
  receives the dictionary DDL. Keeper-backed storage is the convenient shared
  option; server XML/config or per-node bootstrap also works.
- The execution user needs `NAMED COLLECTION ON <name>` access in addition to
  its dictionary DDL privileges.
- Source arguments after `NAME` are named-collection overrides. ClickHouse
  allows them by default; if `allow_named_collection_override_by_default` is
  disabled, declare those keys overridable in the cluster-side collection or
  put them in the collection itself.
- `clickhouse`, `mysql`, `postgresql`, and `http` dictionary sources support
  this form. `file`, `executable`, and `null` have no connection password.

An inline live dictionary may introspect as `password = "[HIDDEN]"`. If HCL
merely omits that password, hclexp reports an unsafe-only difference and emits
no replacement DDL—the safe fallback from #178. To migrate it, provision the
external collection and add `collection = "..."`; the resulting source change
is executable because ClickHouse, rather than hclexp, can resolve the secret.

### Raw escape hatch

When an object's `CREATE` DDL can't be parsed, or uses an engine/form the HCL
model doesn't express, capture it verbatim in a `raw` block. The two labels
mirror Terraform's `resource "<type>" "<name>"` — first the `kind`
(`table`, `materialized_view`, `view`, or `dictionary`), then the name:

```hcl
database "posthog" {
  raw "dictionary" "city_postal_ip_trie" {
    sql = <<SQL
CREATE DICTIONARY posthog.city_postal_ip_trie (`prefix` String)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(USER 'reader' QUERY 'SELECT prefix FROM s3(...)'))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(IP_TRIE)
SQL
  }
}
```

Raw objects are opaque: `hclexp diff` compares the stored `sql` as text and
renders `+ / - / ~ raw <kind> <name>`; `-sql` emits the `sql` verbatim to
create and a `DROP` + `CREATE` to change. Recreating a view/dictionary/MV is
lossless; a **`table`-kind change is flagged `-- UNSAFE`** and its
destructive DDL is not auto-generated. A declared raw block also satisfies
dependency references to it (an MV `to_table`, a Distributed `remote_table`).
`hclexp introspect`/`dump-cluster` only emit raw blocks under `-allow-raw`;
otherwise an object they can't express is a hard error. Full reference:
[`docs/README.hcl.md#raw`](docs/README.hcl.md#raw).

### Named collections

A `named_collection` block declares a ClickHouse named collection —
cluster-scoped key/value bags that other objects (most notably Kafka
tables) can reference by name. Named collections sit **at the top level**
of the HCL document, next to `database` blocks rather than inside one —
they're cluster-scoped, not database-scoped.

```hcl
named_collection "my_kafka" {
  cluster = "posthog"

  param "kafka_broker_list" { value = "kafka:9092" }
  param "kafka_topic_list"  { value = "events" }
  param "kafka_group_name"  { value = "ch_events" }
  param "kafka_format"      { value = "JSONEachRow" }
}

database "posthog" {
  table "events_kafka" {
    column "team_id" { type = "Int64" }
    column "payload" { type = "String" }
    engine "kafka" { collection = "my_kafka" }
  }
}
```

| Block / attribute | Required | Meaning |
|-------------------|----------|---------|
| `external`        | no       | `true` marks an NC managed outside hclexp (e.g. server XML config); hclexp emits no DDL for it but lets Kafka and dictionary-source references resolve. |
| `cluster`         | no       | `ON CLUSTER` target. Changing it forces a DROP+CREATE recreate. |
| `comment`         | no       | NC comment. |
| `param`           | yes (unless `external = true`) | one per key, with required `value` and optional `overridable` boolean. |

**Diff & apply.** `hclexp diff` uses `ALTER NAMED COLLECTION ... SET / DELETE` for surgical param changes and a `DROP+CREATE` pair (emitted adjacently) when `cluster` changes. A collection marked `external = true` on either side is ignored: external is an ownership boundary, not a property introspection can recover from the live object.

**Production secret pattern.** Secret-bearing collections should be external:
the cluster deployment obtains their values from its secret manager, while HCL
commits only their names and references. Managed named collections remain useful
for non-secret settings.

**Externally-managed NCs (PostHog-style XML config).** When a collection is defined in the ClickHouse server's XML config rather than via DDL, declare it in HCL with `external = true`:

```hcl
named_collection "kafka_main" {
  external = true
  comment  = "managed in /etc/clickhouse-server/config.d/kafka.xml"
}
```

hclexp emits no DDL for external collections, but their declaration makes
Kafka engine and dictionary source `collection = "..."` references resolvable
and validatable at parse time.

**Privilege & redaction caveat.** ClickHouse redacts named-collection values to `[HIDDEN]` for users without `SHOW_NAMED_COLLECTIONS_SECRETS`. In production, declare secret-bearing collections external so comparison never needs their values. Use `-show-secrets` only for controlled debugging or export workflows; it writes plaintext secrets to the output.

### Kafka engine with named collections

`engine "kafka" { ... }` accepts either a `collection` reference or a complete inline set of `kafka_*` settings — never both. The inline form is the canonical preferred shape, modeling every documented `kafka_*` setting as a typed HCL attribute (numbers, booleans, strings) with an `extra` escape map for settings ClickHouse adds in versions hclexp doesn't yet model:

```hcl
engine "kafka" {
  // option A: named collection reference (no other field allowed)
  collection = "my_kafka"
}

engine "kafka" {
  // option B: full inline (canonical Kafka() + SETTINGS form)
  broker_list          = "kafka:9092"
  topic_list           = "events"
  group_name           = "ch_events"
  format               = "JSONEachRow"
  num_consumers        = 4
  max_block_size       = 1048576
  commit_on_select     = false
  skip_broken_messages = 100
  handle_error_mode    = "stream"
  sasl_mechanism       = "PLAIN"
  sasl_username        = "ch"
  sasl_password        = "[set via override layer]"
  extra = {
    kafka_some_future_setting = "passthrough"
  }
}
```

Field names drop the `kafka_` prefix (implicit inside `engine "kafka"`). The `extra` map carries any setting that doesn't have a typed field; its keys must include the `kafka_` prefix.

## Layering & inheritance

Layers let a base schema be specialized per environment. `-layer a,b,c`
loads every `.hcl` file under each directory in order; later layers merge
on top of earlier ones.

### Core composition model: parent-first resolution

This is the central mechanic of hclexp. Authored HCL may be spread across
layers, inheritance parents, child-local column specializations, and
environment patches, but the desired schema is always reduced to one flat
table by the same rule:

Before resolving inheritance, hclexp merges the selected layers in their
declared order. A later `override = true` replaces the earlier declaration;
patch blocks are collected rather than applied during that merge. The resolver
then evaluates the table graph below.

```text
resolved(table) =
  apply that table's patch_table blocks, in layer order, to
    merge the table's declaration with its fully resolved parent
```

More explicitly, resolving a table performs these steps:

1. **Resolve its parent recursively.** If the table has `extend`, the parent is
   completed first—including every `patch_table` addressed to that parent.
   Resolution is dependency-driven, so file/declaration order does not change
   the result.
2. **Build the child from the resolved parent and its own declaration.** Parent
   columns come first. Child-local `patch_column` blocks partially specialize
   inherited columns in place; ordinary child `column` blocks then append.
   Parent and child indexes are combined similarly. Inheritable scalar fields
   such as `engine`, `order_by`, and `settings` flow from the parent only when
   the child does not declare its own value.
3. **Apply `patch_table` blocks addressed to this table.** Patches run in layer
   order, and each patch sees the result of the previous one. At this point the
   complete inherited shape exists, so `modify_column`/`drop_columns` can target
   inherited columns and positioned column/index additions can use inherited
   names in `after`.
4. **Publish the completed table to its descendants.** A child extending this
   table inherits the patched result. A sibling does not: it resolves through
   its own parent path.
5. **Discard resolution-only syntax.** Once the graph is resolved,
   `extend`, `abstract`, `patch_column`, and patch blocks are consumed;
   abstract tables are removed, and the emitted desired schema is flat.

There is no abstract-versus-concrete exception. Abstract tables participate in
the same algorithm and are removed only after their descendants have inherited
their resolved shapes. Likewise, a concrete table may be extended, and its
descendants inherit its completed patched shape.

The important propagation rule is therefore:

```text
patch parent  → parent and every descendant
patch child   → child and that child's descendants
sibling       → unaffected
```

For example, consider this inheritance graph:

```text
_event_base (abstract)
├── events
└── sharded_events
    └── regional_events
```

It can be authored and specialized across layers like this:

```hcl
# base layer
database "posthog" {
  table "_event_base" {
    abstract = true
    column "uuid"       { type = "UUID" }
    column "event"      { type = "String" }
    column "properties" { type = "String" }
  }

  table "events" {
    extend   = "_event_base"
    order_by = ["uuid"]
    engine "merge_tree" {}
  }

  table "sharded_events" {
    extend   = "_event_base"
    order_by = ["uuid"]
    patch_column "uuid" { codec = "ZSTD(1)" }
    engine "merge_tree" {}
  }

  table "regional_events" {
    extend = "sharded_events"
  }
}

# environment layer
database "posthog" {
  patch_table "sharded_events" {
    modify_column "properties" {
      type         = "String"
      materialized = "lower(event)"
    }
    column "region" {
      type  = "LowCardinality(String)"
      after = "event"
    }
  }
}
```

The resolved output has these effective shapes:

| Table | Effective columns |
|-------|-------------------|
| `events` | `uuid`, `event`, `properties` |
| `sharded_events` | `uuid CODEC(ZSTD(1))`, `event`, `region`, `properties MATERIALIZED lower(event)` |
| `regional_events` | the completed `sharded_events` shape, including `region` and the materialized `properties` |
| `_event_base` | not emitted (`abstract = true`) |

This ordering is also why `patch_column` and `modify_column` are different:
`patch_column` is a partial, child-local overlay applied while inheritance is
being merged; `modify_column` is a full column replacement from a cross-layer
`patch_table`, applied after the target's inherited shape exists.

The resolver enforces several safety invariants around this model:

- Inheritance copies the resolved parent shape into the child; specializing or
  patching a child never mutates its parent or siblings.
- A normal child `column`/`index` block is an addition. Reusing an inherited
  name is an error; use `patch_column` for a partial inherited-column overlay or
  `patch_table.modify_column` for a full cross-layer replacement.
- Unknown parents, inheritance cycles, unknown patch targets, missing
  modify/drop targets, and invalid `after` references fail resolution rather
  than silently producing a different shape.
- Patches keep layer order. A later patch observes and may deliberately build
  on earlier patches for the same table.
- Downstream diffing and SQL generation see only the final flat tables—not
  inheritance or patch control syntax.

### Inheritance and patching vocabulary

**Table inheritance** within a database:

- `abstract = true` — a template table that is not emitted itself
- `extend = "other_table"` — declare a **new** table inheriting another's
  shape: inherited columns may be specialized with partial `patch_column`
  blocks, then ordinary columns and indexes append (collisions error); `engine`,
  `order_by`, `partition_by`, `sample_by`, `ttl`, and `settings` are
  inherited only if the child doesn't set its own — a child that does set
  one **replaces it wholesale** (a one-key `settings` map loses every
  inherited key). `primary_key`, `comment`, `cluster`, constraints, and
  projections never flow through `extend`.
- `override = true` — required for a later layer to replace a table or
  materialized view that an earlier layer already defined

Rule of thumb: `extend` is for *different tables sharing a shape*
(`events_local` / `events_distributed`); `patch_table` (below) is for *the
same table adjusted by one layer* — it adds no declaration and its
`settings` **merge** instead of replacing. The full decision guide is in
[`docs/README.hcl.md`](docs/README.hcl.md#comparison-extend-vs-patch_table-vs-override).

```hcl
database "posthog" {
  table "_event_base" {
    abstract = true
    column "timestamp" { type = "DateTime" }
    column "team_id"   { type = "UInt64" }
  }

  table "events_local" {
    extend   = "_event_base"
    order_by = ["timestamp", "team_id"]
    patch_column "timestamp" { codec = "Delta(4), ZSTD(1)" }
    column "event" { type = "String" }
    engine "merge_tree" {}
  }
}
```

`patch_column` is child-local inheritance specialization: it targets a column
from `extend`, preserves every omitted field and the inherited position, and
is consumed during resolution. This is useful when the storage table needs a
`CODEC`, TTL, comment, default, type, or nullability variation that a sibling
Distributed table should not inherit. It is distinct from `patch_table`, which
modifies the same named table across layers. An ordinary `column` block still
means “add” and still errors if its name collides with an inherited column.

**Patching** — a `patch_table` block modifies a table declared elsewhere,
so the table itself stays declared once and an environment layer carries
just its delta: columns (add — appended or positioned with
`after`/`first` — plus `modify_column` / `drop_columns`), indexes
(add — likewise positioned — / `drop_indexes`),
projections (add),
`order_by`/`partition_by`/`sample_by`/`ttl`
(replace when set), the `engine` block (wholesale replace — e.g. a
Distributed target that moves with the env's topology), and `settings`
(merged, patch wins per key):

```hcl
# env_us/events_patch.hcl
database "posthog" {
  patch_table "events_local" {
    column "us_session_id" { type = "String" }
    settings = { default_compression_codec = "lz4" }
  }
}
```

Tables resolve parent-first, then apply their own patches. A patch can therefore
modify/drop inherited columns or position additions after inherited
columns/indexes. Any descendant inherits that completed result, while siblings
remain on their own inheritance path. Abstract and concrete tables follow the
same rule.

`patch_materialized_view` does the same for MV queries and output columns;
its column operations run after `extend`, so inherited columns are patchable.
`patch_view` and `patch_dictionary` cover views (`query`, `comment`) and
dictionaries (`source`/`layout`/`lifetime` replace, `settings` merge). See the
[`patch_table` reference](docs/README.hcl.md#patch_table).

After resolution, `extend` / `abstract` / patch blocks are all consumed
and every table is flat with its effective columns, engine, and settings.

## Development

```bash
# Build
go build -o hclexp ./cmd/hclexp

# Unit + snapshot tests
just test

# Live ClickHouse integration tests (needs: docker compose up -d)
just test-live

# Install the repo's pre-commit hook (gofmt + go vet) — one-time per checkout
just setup-hooks
```

The pre-commit hook lives at `.githooks/pre-commit` and is opt-in:
`just setup-hooks` sets `core.hooksPath` to `.githooks`. It rejects a
commit whose staged Go files aren't gofmt'd or whose module fails
`go vet`, and prints the exact command to fix.

See `CLAUDE.md` for repository conventions and `justfile` for the full
list of recipes.

## License

MIT — see [LICENSE](LICENSE).

`chschema` links against [HCL](https://github.com/hashicorp/hcl) (MPL-2.0)
and [clickhouse-go](https://github.com/ClickHouse/clickhouse-go) (Apache-2.0);
their licenses apply to those components.
