# chschema — Declarative ClickHouse Schema Management

A declarative tool for managing ClickHouse schemas. Schemas are written in
HCL, layered for multi-environment setups, resolved into a flat desired
state, and round-tripped against a live cluster.

## What hclexp does

`hclexp` has four modes:

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
   them.

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

## Load & resolve an HCL schema

```bash
# Load a single HCL file, resolve it, print a summary
hclexp -config ./schema/posthog.hcl

# Load a stack of layer directories (applied left to right)
hclexp -layer ./schema/base,./schema/env_us

# Write the resolved schema out as canonical HCL
hclexp -config ./schema/posthog.hcl -out ./resolved.hcl
```

**Flags:**

- `-config` — path to a single HCL file (default `./cmd/hclexp/node.conf`)
- `-layer` — comma-separated list of layer directories, loaded in order
  (mutually exclusive with `-config`)
- `-out` — if set, write the resolved schema as canonical HCL to this path

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

- a single `.hcl` file
- comma-separated layer directories (loaded + resolved in order)
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
  loaded schema.
- A **`distributed`-engine table** forwards to the table named by
  `remote_database` / `remote_table`, which must also be declared.

Missing references — or references into a database that wasn't loaded —
fail with a non-zero exit code. The MV `query` is parsed to discover its
source tables; `WITH ... AS` CTE names are not treated as table references.

```sh
# Validate a single-file schema
hclexp validate -config ./schema/posthog.hcl

# Validate a layer stack
hclexp validate -layer ./schema/base,./schema/env_us

# Skip dependency checks for specific objects, or all of them
hclexp validate -config ./schema/posthog.hcl -skip-validation=events_mv,events_dist
hclexp validate -config ./schema/posthog.hcl -skip-validation='*'
```

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
- `-glob` — filename glob selecting dumps within `-dir` (default `*`),
  e.g. `'*ingestion-small*'`
- `-group-by` — comma-separated keys to group nodes by (default
  `hostClusterRole`). Each key is looked up first in the node's macros,
  then as one of the pseudo-keys `role` / `shard` / `replica` parsed from
  the node name (`prod-<region>-<az>-ch-<shard><replica>[-<role>]`).
  Examples: `-group-by role`, `-group-by hostClusterRole,hostClusterType`.
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

## Container image

`hclexp` ships as a minimal multi-arch container image — distroless, no
shell, no AWS CLI, no extras. It's built and pushed on every `main` push
and Git tag.

- Registry: [`ghcr.io/posthog/chschema`](https://github.com/PostHog/chschema/pkgs/container/chschema)
  (GitHub Container Registry; the chschema repo is public, so the image
  is publicly pullable without authentication)
- Architectures: `linux/amd64`, `linux/arm64`
- Tags:
  - `main` push → `main` + `sha-<short>` + `latest`
  - `vX.Y.Z` tag → `X.Y.Z`, `X.Y`, `X`

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

The image is intended to be paired with `amazon/aws-cli` (or any other
uploader) via a shared `emptyDir` volume in the consuming workload. The
deployment pattern that drives the design lives in
[`posthog/charts`](https://github.com/PostHog/charts) — `hclexp` writes
HCL to the shared volume, the sidecar pushes it to S3.

To build the image locally:

```sh
docker build -t hclexp:dev .
docker run --rm hclexp:dev -help
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
- `extend = "other_table"` — inherit columns/engine/settings from another
  table, then add or override
- `override = true` — required for a later layer to replace a table that
  an earlier layer already defined

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

**Patching** — a `patch_table` block is a strictly additive cross-layer
modification. Only column additions are allowed; it is the safe way for an
environment layer to add columns to a base table:

```hcl
# env_us/events_patch.hcl
database "posthog" {
  patch_table "events_local" {
    column "us_session_id" { type = "String" }
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

