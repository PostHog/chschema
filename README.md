# chschema — Declarative ClickHouse Schema Management

A declarative tool for managing ClickHouse schemas. Schemas are written in
HCL, layered for multi-environment setups, resolved into a flat desired
state, and round-tripped against a live cluster.

> **Project direction:** `hclexp` is the future of this project. The HCL
> loader/resolver and `hclexp` CLI are where active development happens.
> The legacy `chschema` binary, its protobuf contracts, and the YAML
> schema format are **deprecated and will be removed**. New work should
> target `hclexp`. (`hclexp` itself may be renamed once it stabilizes.)

## What hclexp does

`hclexp` has three modes:

1. **Introspect** — connect to a live ClickHouse instance and dump its
   tables as HCL (to stdout, a file, or a directory).
2. **Load & resolve** — read an HCL schema (a single file or a stack of
   layer directories), apply inheritance/patching, and emit the resolved,
   flat schema as canonical HCL.
3. **Diff** — compare two schemas (HCL sources or live clusters, in any
   combination) and report the changes — or the migration DDL — between
   them.

## Build

```bash
go build -o hclexp ./cmd/hclexp
```

ClickHouse connection defaults come from environment variables and can be
overridden by flags:

| Variable             | Default          |
|----------------------|------------------|
| `CLICKHOUSE_HOST`    | `localhost`      |
| `CLICKHOUSE_PORT`    | `9000`           |
| `CLICKHOUSE_DB`      | `migration_test` |
| `CLICKHOUSE_USER`    | `user1`          |
| `CLICKHOUSE_PASSWORD`| `pass1`          |

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
```

**Flags:**

- `-database` — comma-separated list of databases to introspect (required)
- `-host`, `-port`, `-user`, `-password` — connection overrides
- `-out` — output target:
  - omitted → write HCL to stdout
  - a directory → write one `<database>.hcl` per database
  - any other path → write all databases to that single file

Introspection reads each table's `create_table_query` and parses it with
the ClickHouse SQL parser, so columns (types, defaults, codecs, comments,
`MATERIALIZED`/`ALIAS`), indexes, constraints, engine + parameters,
`ORDER BY`, `PARTITION BY`, `SAMPLE BY`, `PRIMARY KEY`, `TTL`, and
`SETTINGS` all come back populated.

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
```

**Side specs** (`-left` / `-right`): each is one of

- a single `.hcl` file
- comma-separated layer directories (loaded + resolved in order)
- `clickhouse://[user[:password]@]host:port/db1[,db2]` — introspected
  live; missing connection pieces fall back to the `CLICKHOUSE_*` defaults

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
| `collapsing_merge_tree`             | `sign_column` |
| `replicated_collapsing_merge_tree`  | `zoo_path`, `replica_name`, `sign_column` |
| `aggregating_merge_tree`            | — |
| `replicated_aggregating_merge_tree` | `zoo_path`, `replica_name` |
| `distributed`                       | `cluster_name`, `remote_database`, `remote_table`, `sharding_key` |
| `log`                               | — |
| `kafka`                             | `broker_list`, `topic`, `consumer_group`, `format` |

### Materialized views

A `materialized_view` block sits alongside `table` blocks in a `database`.
Only the **`TO <table>` form** is supported — the view reads from the source
referenced in `query` and writes rows into an existing destination table.

```hcl
database "posthog" {
  materialized_view "app_metrics_mv" {
    to_table = "default.sharded_app_metrics"
    query    = "SELECT team_id, category FROM default.kafka_app_metrics"
    cluster  = "posthog"           # optional, ON CLUSTER
    comment  = "rolls metrics up"  # optional

    column "team_id"  { type = "Int64" }
    column "category" { type = "LowCardinality(String)" }
  }
}
```

- `to_table` (required) — destination table, optionally `db.`-qualified
- `query` (required) — the `AS SELECT ...` body
- `column` blocks — explicit column list (optional; `type` only)
- A changed `query` diffs to `ALTER TABLE ... MODIFY QUERY`; a changed
  `to_table` or column list is flagged **unsafe** (requires recreating the
  view).
- Materialized views do **not** participate in table inheritance
  (`extend` / `abstract` / `patch_table`).

**Out of scope** — these are rejected with a clear error during
introspection rather than silently mishandled:

- inner-engine materialized views (`CREATE MATERIALIZED VIEW ... ENGINE = ...`)
- refreshable materialized views (`REFRESH EVERY|AFTER ...`)
- window views
- plain (non-materialized) views — skipped during introspection

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
```

See `CLAUDE.md` for repository conventions and `justfile` for the full
list of recipes.

## Legacy: chschema

The original `chschema` binary (YAML schema files, protobuf contracts,
`dump` / `--dry-run` / `--auto-approve`) still builds but is
**deprecated**. It will be removed in favor of `hclexp`. Do not build new
functionality on top of the `proto/`, `gen/`, or `cmd/chschema/` paths.

## License

TBD
