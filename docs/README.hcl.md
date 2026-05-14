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

Every file declares one or more `database` blocks. Within a database, the
allowed children are `table`, `patch_table`, and (later) `materialized_view`,
`view`, `dictionary`.

```hcl
database "posthog" {
  table "events"        { ... }
  patch_table "events"  { ... }
}
```

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
| `collapsing_merge_tree`               | `sign_column`                                      | —                      |
| `replicated_collapsing_merge_tree`    | `zoo_path`, `replica_name`, `sign_column`          | —                      |
| `aggregating_merge_tree`              | —                                                  | —                      |
| `replicated_aggregating_merge_tree`   | `zoo_path`, `replica_name`                         | —                      |
| `distributed`                         | `cluster_name`, `remote_database`, `remote_table`  | `sharding_key`         |
| `log`                                 | —                                                  | —                      |
| `kafka`                               | `broker_list = [...]`, `topic`, `consumer_group`, `format` | —              |

Unknown kinds and missing required attributes are rejected at parse time
with file/line positions.

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