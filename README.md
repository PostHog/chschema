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
   layer directories), apply inheritance/patching, and emit the resolved,
   flat schema as canonical HCL.
3. **Validate** — check that every cross-object reference (MV sources +
   destination, view sources, Distributed `remote_*`) in a resolved
   schema is satisfied, without connecting to a cluster.
4. **Diff** — compare two schemas (HCL sources or live clusters, in any
   combination) and report the changes — or the migration DDL — between
   them; `-format json` emits a dependency-ordered, machine-readable plan.

Additional commands:

- **plan** — diff every node role in a `-manifest` against a `-dump`
  topology in one run, emitting a single globally-ordered, cross-role
  operation list (storage before its Distributed/Buffer proxies before the
  MV). See **[Cross-role planning](docs/README.hcl.md#cross-role-planning--hclexp-plan)**.
- **drift** — detect cross-node schema drift across per-node HCL dumps.
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
- `-sql` — emit the migration DDL (`CREATE` / `ALTER` / `DROP`) that turns
  the left side into the right side, instead of the change summary.
  Changes ClickHouse can't apply in place (engine swap, `ORDER BY`,
  `PARTITION BY`, `SAMPLE BY`) are flagged with `-- UNSAFE` comments.
  Heavy operator-run statements (currently `MATERIALIZE INDEX`, generated
  alongside every `ADD INDEX` on an existing table) are printed commented
  out as `-- MANUAL:` lines — run them deliberately, never as part of an
  automated apply. In `-format json` output the same statements carry
  `"manual": true`.

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

# CI guard: every object declared at more than one plain site, even
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
redeclaration, a `[patch_table]`, an `extends <parent>` child, or a
`[raw <kind>]` block. An object extended by others lists its children
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
  `-layer` (mutually exclusive with `-dump`); lists every object declared
  at more than one *plain* site and exits non-zero when any is found.
  `override = true` redeclarations, additive `patch_table` blocks, and
  `abstract` declarations (dropped at resolve) are exempt — so what
  remains is exactly the accidental-duplication class that `load` only
  catches when the two layers co-occur in one stack.

Exit codes: `0` on success, `1` when any pattern matches nothing (a
scriptable existence check) or `-duplicates` finds any, `2` on usage
errors.

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
runtime via `dictGet*` functions.

```hcl
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
      query    = "SELECT currency, start_date, end_date, rate FROM default.exchange_rate"
      user     = "default"
      password = "[HIDDEN]"
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

Kinds outside these lists error during introspection with `unsupported dictionary source/layout kind: <name>`. Adding a new kind is a small change — one typed struct + one switch case in `dictionary_sources.go` / `dictionary_layouts.go`.

**Diff & apply.** ClickHouse has no useful in-place `ALTER DICTIONARY`. `hclexp diff` reports any non-empty change with `~ dictionary <name> (changed: ...)`; `-sql` emits a `CREATE OR REPLACE DICTIONARY` statement, which is the idiomatic ClickHouse update path and is treated as safe.

**`PASSWORD '[HIDDEN]'` caveat.** ClickHouse's `system.tables.create_table_query` redacts secrets, so an introspected dictionary's `password` is the literal string `[HIDDEN]`. Applying a dumped dictionary verbatim will leave it unable to load data from its source. Edit dumped HCL to restore real credentials (or wire secrets through some out-of-band mechanism) before deploying.

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
| `external`        | no       | `true` marks an NC managed outside hclexp (e.g. server XML config); hclexp emits no DDL for it but lets Kafka references resolve. |
| `cluster`         | no       | `ON CLUSTER` target. Changing it forces a DROP+CREATE recreate. |
| `comment`         | no       | NC comment. |
| `param`           | yes (unless `external = true`) | one per key, with required `value` and optional `overridable` boolean. |

**Diff & apply.** `hclexp diff` uses `ALTER NAMED COLLECTION ... SET / DELETE` for surgical param changes and a `DROP+CREATE` pair (emitted adjacently) when `cluster` changes. External↔managed transitions are flagged as unsupported migrations.

**Production secret pattern.** The natural way to keep secret NC values out of VCS is the layered HCL pattern:

```bash
hclexp -layer schema/base,schema/prod-secrets ...
```

The base layer commits the NC declaration with placeholder values; the override layer (gitignored or vault-sourced) declares the same NC with `override = true` and the real values. Layer merging applies the override.

**Externally-managed NCs (PostHog-style XML config).** When a collection is defined in the ClickHouse server's XML config rather than via DDL, declare it in HCL with `external = true`:

```hcl
named_collection "kafka_main" {
  external = true
  comment  = "managed in /etc/clickhouse-server/config.d/kafka.xml"
}
```

hclexp emits no DDL for external collections, but their declaration makes engine `collection = "..."` references resolvable and validatable at parse time.

**Privilege & redaction caveat.** ClickHouse redacts named-collection values to `[HIDDEN]` for users without `SHOW_NAMED_COLLECTIONS_SECRETS`. The introspection in this package relies on the cluster exposing real values. In production with restricted users, introspected NCs come back with `[HIDDEN]` placeholders — use the override-layer pattern (or external NCs) to keep the real values out of VCS.

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

**Table inheritance** within a database:

- `abstract = true` — a template table that is not emitted itself
- `extend = "other_table"` — declare a **new** table inheriting another's
  shape: columns and indexes append (collisions error); `engine`,
  `order_by`, `partition_by`, `sample_by`, `ttl`, and `settings` are
  inherited only if the child doesn't set its own — a child that does set
  one **replaces it wholesale** (a one-key `settings` map loses every
  inherited key). `primary_key`, `comment`, `cluster`, constraints, and
  projections never flow through `extend`.
- `override = true` — required for a later layer to replace a table that
  an earlier layer already defined

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
    column "event" { type = "String" }
    engine "merge_tree" {}
  }
}
```

**Patching** — a `patch_table` block modifies a table declared elsewhere,
so the table itself stays declared once. Columns are strictly additive
(a duplicate errors); `settings` merges into the target's map with the
patch winning on key collision, so an environment layer can retune one
setting without redeclaring the table:

```hcl
# env_us/events_patch.hcl
database "posthog" {
  patch_table "events_local" {
    column "us_session_id" { type = "String" }
    settings = { default_compression_codec = "lz4" }
  }
}
```

After resolution, `extend` / `abstract` / `patch_table` are all consumed
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

TBD

