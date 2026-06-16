# HCL schema reference

This document describes the HCL language used to declare ClickHouse schemas in
this project. It is a reference, not a tutorial — see [FAQ.md](./FAQ.md) for
worked examples.

## File and layer model

A schema is the result of merging an **ordered list of layer directories**.
The loader walks each directory in order, reads every `*.hcl` file
(lexically by filename), and merges them into one combined schema. The
loader has no built-in notion of "base," "env," or "node" — layers are
generic. A typical convention:

```
schema/
  base/        # always loaded
  envs/us/     # loaded only for the US deployment
  nodes/ingestion/
```

is just three layer directories passed to the loader in that order.

## Top-level blocks

Most files declare one or more `database` blocks. Within a database, the
allowed children are `table`, `patch_table`, `materialized_view`, `view`,
`dictionary`, and `raw` (the escape hatch; see [`raw`](#raw)).

```hcl
database "posthog" {
  table "events"        { ... }
  patch_table "events"  { ... }
}
```

Two other blocks live at the top level, as siblings of `database`:
`named_collection` (cluster-scoped config bags) and `node` (per-node
identity captured by `hclexp introspect`; see [`node`](#node)).

## `table`

```hcl
table "events" {
  extend   = "_event_base"   # optional; inherit from another table
  abstract = false           # optional; abstract tables are never emitted
  override = false           # optional; cross-layer full replacement

  order_by     = ["timestamp", "team_id"]
  partition_by = "toYYYYMM(timestamp)"
  sample_by    = "team_id"
  ttl          = "timestamp + INTERVAL 2 YEARS"
  settings     = { ttl_only_drop_parts = "1" }

  column "timestamp" { type = "DateTime" }
  column "team_id"   { type = "UInt64" }

  index "idx_team" {
    expr        = "team_id"
    type        = "minmax"
    granularity = 4
  }

  engine "replicated_merge_tree" {
    zoo_path     = "/clickhouse/tables/{shard}/events"
    replica_name = "{replica}"
  }
}
```

All non-block attributes are optional. `column` and `index` are repeatable
blocks. `engine` is a single labeled block — see *Engine kinds* below.

### Control attributes

- `extend = "other_table"` — single-inheritance from another table in the same
  database. See *Inheritance*.
- `abstract = true` — declares this table as inheritable-only; it is not
  emitted as a real ClickHouse table.
- `override = true` — declares that this block replaces an earlier-layer
  `table` of the same name. Without it, a cross-layer name collision is an
  error.

## `column`

```hcl
column "name" {
  type = "DateTime"
}
```

`type` is required. (Defaults, codecs, and nullability live in future work.)

## `index`

```hcl
index "idx_team" {
  expr        = "team_id"
  type        = "minmax"
  granularity = 4         # optional
}
```

## `engine`

The engine is a labeled block; the label is the engine kind. The body's
attributes depend on the kind.

| Kind                                  | Required attributes                                | Optional               |
| ------------------------------------- | -------------------------------------------------- | ---------------------- |
| `merge_tree`                          | —                                                  | —                      |
| `replicated_merge_tree`               | `zoo_path`, `replica_name`                         | —                      |
| `replacing_merge_tree`                | —                                                  | `version_column`       |
| `replicated_replacing_merge_tree`     | `zoo_path`, `replica_name`                         | `version_column`       |
| `summing_merge_tree`                  | —                                                  | `sum_columns = [...]`  |
| `replicated_summing_merge_tree`       | `zoo_path`, `replica_name`                         | `sum_columns = [...]`  |
| `collapsing_merge_tree`               | `sign_column`                                      | —                      |
| `replicated_collapsing_merge_tree`    | `zoo_path`, `replica_name`, `sign_column`          | —                      |
| `aggregating_merge_tree`              | —                                                  | —                      |
| `replicated_aggregating_merge_tree`   | `zoo_path`, `replica_name`                         | —                      |
| `distributed`                         | `cluster_name`, `remote_database`, `remote_table`  | `sharding_key`         |
| `log`                                 | —                                                  | —                      |
| `kafka`                               | `broker_list = [...]`, `topic`, `consumer_group`, `format` | —              |
| `time_series` (experimental)          | —                                                  | `settings`, `tags_to_columns`, nested `samples`/`tags`/`metrics` blocks |
| `join`                                | `strictness` (`ANY`/`ALL`/`SEMI`/`ANTI`), `type` (`LEFT`/`INNER`/`RIGHT`/`FULL`), `keys = [...]` | — |
| `null`                                | —                                                  | —                      |
| `memory`                              | —                                                  | —                      |
| `merge`                               | `db_regex`, `table_regex`                          | —                      |
| `buffer`                              | `database`, `table`, `num_layers`, `min_time`, `max_time`, `min_rows`, `max_rows`, `min_bytes`, `max_bytes` | `flush_time`, `flush_rows`, `flush_bytes` |

Dictionary layouts supported via `layout "<kind>"` inside a `dictionary` block: `flat`, `hashed`, `sparse_hashed`, `complex_key_hashed` (optional `preallocate`), `complex_key_sparse_hashed`, `range_hashed` / `complex_key_range_hashed` (optional `range_lookup_strategy`), `cache` (required `size_in_cells`), `complex_key_cache` (required `size_in_cells`), `hashed_array` / `complex_key_hashed_array` (optional `shards`), `direct`, `complex_key_direct`, `ip_trie` (optional `access_to_key_from_attributes`).

Unknown kinds and missing required attributes are rejected at parse time
with file/line positions.

### `time_series` engine

Models ClickHouse's [TimeSeries](https://clickhouse.com/docs/en/engines/table-engines/special/time_series) engine for Prometheus-style metrics. Three sibling target tables (samples/tags/metrics) are declared via nested sub-blocks; each takes either an external `target = "db.table"` reference or an inline `inner {}` block with column list + nested engine.

```hcl
table "prom_metrics" {
  column "metric_name" { type = "LowCardinality(String)" }
  # ... outer columns ...

  engine "time_series" {
    settings = {
      id_generator = "sipHash64(metric_name, all_tags)"
    }
    tags_to_columns = {
      instance = "instance"
      job      = "job"
    }
    samples { target = "default.prom_metrics_data" }
    tags    { target = "default.prom_metrics_tags" }
    metrics { target = "default.prom_metrics_metrics" }
  }
}
```

- Each target sub-block is optional. Omitted = CH default inner targets (CH auto-generates them).
- Exactly one of `target` or `inner` per sub-block.
- Inner-form engines restricted to MergeTree-family kinds.
- ALTER-able settings: `id_generator`, `filter_by_min_time_and_max_time`. Every other setting and every target change requires recreating the table (flagged `-- UNSAFE`).
- HCL authors always write `samples`; the `DATA` alias CH supports is preserved on dump-side only via a `KeywordHint` round-trip.

Inner form for a single target:

```hcl
samples {
  inner {
    column "id"        { type = "UUID" }
    column "timestamp" { type = "DateTime64(3)" }
    column "value"     { type = "Float64" }
    engine "merge_tree" {}
    order_by = ["id", "timestamp"]
  }
}
```

## `patch_table`

Strictly additive cross-layer modification of an existing table.

```hcl
database "posthog" {
  patch_table "events" {
    column "us_session_id" { type = "String" }
  }
}
```

- The target table must exist somewhere in the merged config.
- Only `column` blocks are accepted. `engine`, `order_by`, `settings`, `index`,
  etc. are rejected at parse time.
- Adding a column that already exists on the target is an error.
- `patch_table` lives at any layer.

## Inheritance — `extend = "Y"`

A `table` can declare `extend = "Y"`, where `Y` is another table (or an
abstract table) in the same database.

The child inherits:

- All `column` blocks (appended; collisions with child's own columns error).
- All `index` blocks (appended; collisions error).
- `engine`, `order_by`, `partition_by`, `sample_by`, `ttl`, `settings`
  — if the child does **not** set its own; otherwise the child's value
  replaces the inherited one.

Chains are allowed (`A extends B extends C`); cycles error.

### Abstract bases

`abstract = true` marks a table as inheritance-only. Abstract tables are
removed from the schema after resolution, so they never become real
ClickHouse tables. Use them to factor shared columns:

```hcl
table "_event_base" {
  abstract = true
  column "timestamp" { type = "DateTime" }
  column "team_id"   { type = "UInt64" }
}

table "events_local"       { extend = "_event_base"; engine "merge_tree" {}; ... }
table "events_distributed" { extend = "_event_base"; engine "distributed" { ... } }
```

### Inheritance on `materialized_view`

A `materialized_view` may also `extend` an `abstract = true` table. The MV
inherits the parent's `column` list (and `cluster` / `comment` if it
hasn't set its own). Engine, `order_by`, etc. are table-only and not
inherited. Extending a concrete (non-abstract) table or another MV is an
error.

When to reach for it: an MV that **declares its own output shape** — most
commonly an aggregating MV whose `AggregateFunction(...)` columns must
agree with the destination table. The destination and the MV share one
base, the source table does not.

```hcl
table "team_metrics_base" {
  abstract = true
  column "hour"         { type = "DateTime" }
  column "team_id"      { type = "UInt32" }
  column "events"       { type = "AggregateFunction(count, UInt64)" }
}

table "team_metrics" {
  extend = "team_metrics_base"
  engine "aggregating_merge_tree" {}
  order_by = ["hour", "team_id"]
}

materialized_view "team_metrics_mv" {
  extend   = "team_metrics_base"
  to_table = "team_metrics"
  query    = "SELECT toStartOfHour(timestamp) AS hour, team_id, countState() AS events FROM events_kafka GROUP BY hour, team_id"
}
```

For a passthrough MV that just mirrors columns from source to destination,
omit both the column list and `extend` — ClickHouse derives the MV's
schema from the `SELECT`. See the FAQ for the full worked passthrough vs.
aggregating comparison.

An `abstract = true` materialized_view is accepted for symmetry (it is
dropped after resolution, like an abstract table), but has no common use
case — MVs are usually concrete glue.

## Resolution pipeline

When the loader processes a layered set, this pipeline runs:

1. **Parse** every `.hcl` file in every layer (ordered).
2. **Merge** databases by name. Tables collide on name unless the later one
   sets `override = true`. `patch_table` blocks accumulate.
3. **Apply `patch_table`s** — additively, in layer order, against their targets.
4. **Resolve `extend` chains** — DFS with cycle detection; children see the
   post-patch parent.
5. **Drop abstract tables** from the emit set.
6. **Validate** — every remaining (non-abstract) table must have an engine.

## Comparison: `extend` vs `patch_table` vs `override`

| Need                                              | Use            |
| ------------------------------------------------- | -------------- |
| Two tables share most columns; different engines  | `extend` + `abstract` parent |
| Add a column to the same table in one environment | `patch_table`  |
| Replace a table entirely in one environment       | `override = true` |

## Dependency validation — `hclexp validate`

Some objects can only be created after the objects they reference exist:

- A **`materialized_view`** reads from a source table (named in its `query`)
  and writes into its `to_table` destination. Both must exist first.
- A **`view`** (plain, non-materialized) reads from the source tables named
  in its `query`. All of them must exist first.
- A **`distributed`-engine table** forwards to the table named by
  `remote_database` / `remote_table`, which must exist first.

`hclexp validate -config <file>` (or `-layer <dirs>`) resolves the schema and
checks every such dependency. A missing reference — or a reference into a
database that wasn't loaded — fails with a non-zero exit code. The MV `query`
is parsed to discover its source tables; `WITH ... AS` CTE names are not
treated as table references.

To bypass the check for specific objects, pass `-skip-validation` a
comma-separated list of the **dependent** object names (the MV or Distributed
table), or `*` to skip everything:

```sh
hclexp validate -config schema.hcl -skip-validation=events_mv,events_dist
hclexp validate -config schema.hcl -skip-validation='*'
```

`hclexp diff -sql` applies the same dependency knowledge to ordering: within
the generated DDL, a table is created before any Distributed table that
forwards to it, and dropped after it.

## Cross-node drift — `hclexp drift`

`hclexp drift -dir <dir>` compares the per-node dumps in a directory and
reports where nodes that should share a schema diverge. Each dump is one
node (carrying its [`node`](#node) block); nodes are grouped, then every
node in a group is diffed — using the same engine as `hclexp diff` —
against the group's lexically-first **reference** node.

```sh
hclexp drift -dir prod/eu                              # group by hostClusterRole macro
hclexp drift -dir prod/eu -glob '*ingestion-small*'    # one pool only
hclexp drift -dir prod/eu -group-by role -details      # finer grouping + full diffs
```

| Flag | Default | Meaning |
|------|---------|---------|
| `-dir`      | —                 | directory of per-node `.hcl` dumps (required) |
| `-glob`     | `*`               | comma-separated filename globs selecting dumps within `-dir` (a file matching any pattern is included); e.g. `*-ch-*[fg].hcl,*-offline.hcl` to compare all DATA nodes together |
| `-group-by` | `hostClusterRole` | comma-separated grouping keys: macro names, or the pseudo-keys `role`/`shard`/`replica` parsed from the node name |
| `-zk-paths` | `mask-uuid`       | ReplicatedMergeTree `zoo_path` handling: `mask-uuid` (replace the literal table UUID with `{uuid}`), `keep` (verbatim), or `ignore` (blank path + replica) |
| `-details`  | off               | print each drifting node's full change set, not just a one-line summary |

ClickHouse expands the `{uuid}` macro to the table's literal UUID at
`CREATE` time (while keeping `{shard}`/`{replica}` as macros), so the same
table on different shards gets a different `zoo_path` — pure noise for
drift. The default `-zk-paths=mask-uuid` collapses that back to `{uuid}`
so only genuine path differences register.

A drifting node is printed as `✗ <name>: <summary>` (e.g. `+16 table, -8
table, +8 mv`); a fully consistent group prints `OK (all identical)`. The
command exits non-zero when any drift is found, so it works as a CI guard.

`hostClusterRole` is coarse and can merge distinct pools (`ingestion`
spans ingestion-events / -medium / -small; `data` spans online and offline
nodes). `-group-by role` uses the deployment role from the node name and
usually isolates genuine drift.

## `view`

A `view` block declares a ClickHouse **plain** (non-materialized) view — a
saved `SELECT` that's evaluated on every read of the view.

```hcl
database "posthog" {
  view "team_event_counts" {
    query = "SELECT team_id, count() AS n FROM posthog.events GROUP BY team_id"

    column_aliases = ["team_id", "n"]   // optional CREATE VIEW v (a, b) AS ...

    sql_security = "definer"            // optional: definer | invoker | none
    definer      = "alice"              // required iff sql_security == "definer" with a named user

    cluster = "posthog"                 // optional ON CLUSTER
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

## `raw`

The escape hatch for objects whose `CREATE` DDL the parser cannot handle, or
that use an engine/form this schema language does not express. The whole
statement is stored verbatim and round-tripped unchanged. The two labels
mirror Terraform's `resource "<type>" "<name>"`: the first is the object
`kind`, the second its name.

```hcl
database "posthog" {
  raw "dictionary" "city_postal_ip_trie" {
    sql = <<SQL
CREATE DICTIONARY posthog.city_postal_ip_trie (`prefix` String, `city_name` String DEFAULT '')
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(USER 'reader' QUERY 'SELECT prefix, city_name FROM s3(...)'))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(IP_TRIE)
SQL
  }
}
```

| Field  | Meaning |
|--------|---------|
| `kind` (1st label) | `table`, `materialized_view`, `view`, or `dictionary`. Drives the `DROP` form on a recreate. |
| `name` (2nd label) | The object name. |
| `sql`  | The original `CREATE` statement, emitted verbatim on apply. |

**Semantics.** Raw objects are opaque:

- **Diff** compares the stored `sql` as text (trailing newlines ignored;
  all other whitespace is significant). There is no structural diff.
- **Apply** emits the `sql` verbatim to create. A changed `sql` is a
  **recreate** (`DROP` + `CREATE`). Recreating a `view`, `dictionary`, or
  `materialized_view` is lossless; recreating a **`table` is flagged
  `-- UNSAFE`** (it destroys on-disk data) and the destructive DDL is *not*
  auto-generated — you must apply it by hand.
- **Validation** runs no outgoing dependency checks on raw objects (their SQL
  is opaque), but a declared `raw` block *does* satisfy references to it — a
  real materialized view's `to_table` or a Distributed table's `remote_table`
  pointing at a raw object resolves cleanly.

**Capturing.** `hclexp introspect` is **strict by default**: an object it
cannot parse or express aborts the dump with an error that names the flag.
Pass `-allow-raw` to capture such objects as `raw` blocks (with a warning)
and continue. `hclexp dump-cluster` takes the same flag. The diff live side
stays strict regardless — materialize raw blocks into HCL with
`introspect -allow-raw` first.

## `node`

A `node` block is metadata, not a managed object. `hclexp introspect`
emits one at the top of each per-node dump to record that node's identity:
the hostname (the block label) and its ClickHouse macros, read from
`SELECT * FROM system.macros`.

```hcl
node "prod-eu-fra-ch-1d-ops" {
  macros = {
    hostClusterRole = "ops"
    hostClusterType = "online"
    replica         = "d"
    shard           = "1"
  }
}
```

| Attribute | Required | Meaning |
|-----------|----------|---------|
| label     | yes      | node hostname (defaults to the server's `hostName()` on introspect; override with `-node`) |
| `macros`  | no       | key/value bag from `system.macros` (`shard`, `replica`, `hostClusterRole`, `hostClusterType`, …) |

`node` blocks are ignored by `hclexp diff` — they're identity, not schema.
Their purpose is to let [`hclexp drift`](#cross-node-drift--hclexp-drift)
group nodes by their authoritative macros.

## Virtual columns

ClickHouse exposes implicit columns on tables of certain engines — names
like `_part` on MergeTree, `_offset`/`_timestamp` on Kafka, `_shard_num`
on Distributed. `hclexp` knows about them per engine:

| Engine                | Virtual columns                                                                                       |
| --------------------- | ----------------------------------------------------------------------------------------------------- |
| MergeTree family      | `_part`, `_part_index`, `_part_uuid`, `_partition_id`, `_partition_value`, `_sample_factor`, `_part_offset` |
| Kafka                 | `_topic`, `_key`, `_offset`, `_partition`, `_timestamp`, `_timestamp_ms`, `_headers.name`, `_headers.value` (+`_raw_message`, `_error` when `handle_error_mode = "stream"`) |
| Distributed           | `_shard_num`, **plus the virtuals of its remote table** (transitive; chains and cycles handled)        |
| Log, all others       | none                                                                                                  |

This awareness shows up in three places:

1. **MV source-column validation** (`hclexp validate`). When a materialized
   view's `query` has a single resolvable source (no JOIN/UNION/CTE/
   subquery/`SELECT *`), the validator walks identifier references whose
   names begin with `_` and flags any that aren't provided by the source —
   declared columns plus the engine's virtual set. A bare `_offsett` on a
   Kafka source is flagged; a legitimate `_offset`/`_timestamp` /
   `_headers.name` passes. Skip per-MV with `-skip-validation=<mv-name>`.

2. **Diff guard** (`hclexp diff`). A column name recognised as virtual on
   one side is suppressed from ADD/DROP DDL only when the other side has
   not declared it — preventing CH-illegal `DROP COLUMN _offset` from a
   stray introspector leak, while still surfacing `MODIFY COLUMN` when
   both sides explicitly declared the column (the `_key FixedString(8)`
   case from `kafka_map_virtual_columns_on_write`).

3. **Schema-as-source-of-truth**. A user can declare a column whose name
   matches a virtual (e.g. `column "_key" { type = "FixedString(8)" }` on
   a Kafka table) — the declared definition wins over the virtual at
   every consumer site.

Out of scope in v1: MergeTree's version-gated virtuals (`_block_number`,
`_block_offset`, `_row_exists`) — these depend on ClickHouse version /
table settings (lightweight deletes) and would risk false positives on
older deployments. Declare them explicitly in HCL when you need them.

## TLS / secure connections

`hclexp` connects to ClickHouse in plaintext by default. To reach a
TLS-only cluster (e.g. production on port 9440), enable TLS via flags,
environment variables, or query parameters on the `clickhouse://` URL
used by `hclexp diff`.

| Form                          | Enable TLS              | Skip cert verification              |
| ----------------------------- | ----------------------- | ----------------------------------- |
| `hclexp introspect` flag      | `-secure`               | `-tls-skip-verify`                  |
| Environment variable          | `CLICKHOUSE_SECURE=true`| `CLICKHOUSE_TLS_SKIP_VERIFY=true`   |
| `clickhouse://` URL query     | `?secure=true`          | `?skip-verify=true`                 |

`-tls-skip-verify` / `?skip-verify=true` is only valid together with
`-secure` / `?secure=true`; passing it alone is rejected. Skip
verification is intended for internal-CA clusters — for public CAs the
default verification path uses the system trust store.

### Examples

Introspect a TLS cluster with a private CA:

```sh
hclexp introspect \
  -host ch.prod.internal -port 9440 -user readonly \
  -secure -tls-skip-verify \
  -database posthog,system \
  -out ./dump
```

Diff a local schema tree against a TLS cluster:

```sh
hclexp diff \
  -left ./schema \
  -right 'clickhouse://ro:secret@ch.prod.internal:9440/posthog?secure=true&skip-verify=true'
```