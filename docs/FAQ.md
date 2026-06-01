# FAQ

Worked answers to common modeling questions. See
[README.hcl.md](./README.hcl.md) for the language reference.

## How do I create two tables with the same columns?

Define the shared columns once on an `abstract` table, then let each real
table `extend` it.

```hcl
database "posthog" {
  table "_event_base" {
    abstract = true
    column "timestamp" { type = "DateTime" }
    column "team_id"   { type = "UInt64" }
    column "event"     { type = "String" }
  }

  table "events_local" {
    extend = "_event_base"
    engine "replicated_merge_tree" {
      zoo_path     = "/clickhouse/tables/{shard}/events_local"
      replica_name = "{replica}"
    }
    order_by = ["timestamp", "team_id"]
  }

  table "events_distributed" {
    extend = "_event_base"
    engine "distributed" {
      cluster_name    = "posthog"
      remote_database = "default"
      remote_table    = "events_local"
    }
  }
}
```

After resolution there are two emitted tables — `events_local` and
`events_distributed` — each with the three shared columns. `_event_base` is
dropped.

## How do I share columns across a Kafka source, MV, local table, and Distributed table?

The canonical ingest pipeline: Kafka feeds an MV, which writes to a local
replicated table, fronted by a Distributed table. The three **tables** share
the same columns — declare them once on an abstract base and let each table
`extend` it. The MV, since it's a 1:1 passthrough, doesn't need any column
list at all: ClickHouse derives the MV's schema from its `SELECT`.

```hcl
database "default" {
  cluster = "main"

  table "events_base" {
    abstract = true
    column "timestamp" { type = "DateTime64(3)" }
    column "team_id"   { type = "UInt32" }
    column "event"     { type = "String" }
  }

  table "events_kafka" {
    extend = "events_base"
    engine "kafka" {
      broker_list = "kafka:9092"
      topic_list  = "events"
      group_name  = "ch_events"
      format      = "JSONEachRow"
    }
  }

  table "events_local" {
    extend = "events_base"
    engine "replicated_merge_tree" {
      zoo_path     = "/clickhouse/tables/{shard}/events"
      replica_name = "{replica}"
    }
    order_by = ["team_id", "timestamp"]
  }

  table "events_distributed" {
    extend = "events_base"
    engine "distributed" {
      cluster_name    = "main"
      remote_database = "default"
      remote_table    = "events_local"
      sharding_key    = "cityHash64(team_id)"
    }
  }

  materialized_view "events_mv" {
    to_table = "events_local"
    query    = "SELECT timestamp, team_id, event FROM events_kafka"
  }
}
```

Adding a column to `events_base` adds it to all three tables. The MV picks
it up automatically the next time you edit the `SELECT` to project it.

## When does it make sense to `extend` on a `materialized_view`?

When the MV is *not* a passthrough — typically an aggregating MV whose
declared output column types must match its destination table. The
destination and the MV share the same `AggregateFunction(...)` columns, so
both should extend the same base. The Kafka source uses raw types and does
*not* extend this base.

```hcl
database "default" {
  cluster = "main"

  table "events_kafka" {
    column "timestamp" { type = "DateTime64(3)" }
    column "team_id"   { type = "UInt32" }
    column "user_id"   { type = "UInt64" }
    engine "kafka" { broker_list = "kafka:9092"; topic_list = "events"; group_name = "ch_metrics"; format = "JSONEachRow" }
  }

  # Shared shape: aggregation state columns. The destination table and the
  # MV both extend this — they MUST agree on the AggregateFunction types.
  table "team_metrics_base" {
    abstract = true
    column "hour"         { type = "DateTime" }
    column "team_id"      { type = "UInt32" }
    column "events"       { type = "AggregateFunction(count, UInt64)" }
    column "unique_users" { type = "AggregateFunction(uniq, UInt64)" }
  }

  table "team_metrics" {
    extend = "team_metrics_base"
    engine "aggregating_merge_tree" {}
    order_by = ["hour", "team_id"]
  }

  materialized_view "team_metrics_mv" {
    extend   = "team_metrics_base"
    to_table = "team_metrics"
    query    = <<-SQL
      SELECT
        toStartOfHour(timestamp)        AS hour,
        team_id,
        countState()                    AS events,
        uniqState(user_id)              AS unique_users
      FROM events_kafka
      GROUP BY hour, team_id
    SQL
  }
}
```

Why the column list pulls its weight here:

- Pins the `AggregateFunction(...)` types instead of relying on CH to infer
  them from the SELECT — an ambiguous `sumState(...)` or accidental type
  drift in the SELECT will fail at MV creation, not silently at query time.
- Documents the MV's shape at the call site.
- Catches drift between MV and destination when you change either one — the
  diff highlights the mismatch immediately.

Rule of thumb: passthrough MVs → no column list, no `extend`. Aggregating /
transforming MVs whose output shape matches a destination → declare a
shared base and have both `extend` it.

Note: any change to an MV's declared column list — including one that flows
in via `extend` — requires recreating the MV. `hclexp diff -sql` flags that
as `UNSAFE`. Plan column changes accordingly.

## How do I make a table that's *almost* identical to another, just with a different `order_by`?

`extend` from the table you want to clone, then override the differing
attribute.

```hcl
table "events_by_team" {
  extend   = "events_local"
  order_by = ["team_id", "timestamp"]
}
```

The child inherits every column, the engine, partition_by, ttl, settings, and
indexes. Setting `order_by` replaces the inherited value.

## How do I add an extra column to a table in only one environment?

Use `patch_table` in the environment layer. The base table stays a single
declaration; the patch is surgical.

```hcl
# base/events.hcl
database "posthog" {
  table "events" {
    column "timestamp" { type = "DateTime" }
    column "team_id"   { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["timestamp", "team_id"]
  }
}

# envs/us/events_patch.hcl
database "posthog" {
  patch_table "events" {
    column "us_session_id" { type = "String" }
  }
}
```

When the loader runs with `--layer base --layer envs/us`, the resolved
`events` table has three columns.

## How do I add the same extra column to a table in *several* environments?

If only US needs it, use `patch_table` in `envs/us/`. If US and EU both need
it, put a `patch_table` in each. If the column is present in every
environment, just add it to the base `table`. Don't try to be clever — the
local-knowledge wins over global indirection.

## How do I replace a table entirely in one environment?

Declare the table in the environment layer with `override = true`.

```hcl
# envs/dev/events.hcl
database "posthog" {
  table "events" {
    override = true
    column "id" { type = "UUID" }
    engine "log" {}
  }
}
```

Without `override = true`, the cross-layer collision is an error. With it,
the dev-layer definition wins.

## What's the difference between `extend` and `patch_table`?

| Aspect            | `table X { extend = "Y" }`                | `patch_table "Y" { ... }`               |
| ----------------- | ------------------------------------------ | ---------------------------------------- |
| Creates new table | Yes (`X` is a new, distinct table)        | No (modifies `Y` in place)               |
| Engine identity   | `X` has its own engine                     | `Y`'s engine is unchanged                |
| Can override anything? | Yes — engine, order_by, ttl, settings | No — additive columns only               |
| Where it lives    | Same layer, typical                        | Any layer (commonly higher overlays)     |
| Use case          | "Similar but different" tables             | Per-environment column extras            |

In short: `extend` creates a *new* table; `patch_table` modifies an *existing*
one.

## Can `patch_table` change the engine or `order_by`?

No. `patch_table` is strictly additive — only `column` blocks. If you need to
change those in one environment, use a full `table` declaration with
`override = true` in that environment's layer.

## What happens if my `extend` chain has a cycle?

You get an error pointing at the cycle. `A → B → A` and self-cycles
(`A → A`) are both detected.

## Can a table extend a table in a different database?

Not in v1. `extend = "name"` resolves only within the same `database` block.

## Why is my `engine` block being rejected?

A few common reasons:

1. **No kind label.** `engine { ... }` is invalid; you need
   `engine "merge_tree" { ... }`.
2. **Unknown kind.** The label must match one of the supported kinds (see the
   table in [README.hcl.md](./README.hcl.md)).
3. **Missing required attribute** for the kind. For example,
   `engine "replicated_merge_tree" { zoo_path = "..." }` without
   `replica_name` errors at parse time.
4. **Attribute belongs to a different kind.** `version_column` is only valid
   on `replacing_merge_tree` / `replicated_replacing_merge_tree`. Placing it
   on `merge_tree` is rejected.

## Why does my non-abstract table fail with "requires an engine"?

Every emitted table needs an engine, either declared on itself or inherited
through `extend`. If neither is true, the resolver fails. If you meant the
table to be a base only, set `abstract = true`.

## In what order are files within a layer read?

Lexically by filename (`events.hcl` before `users.hcl`). The order rarely
matters because every block in a single layer is merged before the next
layer is processed; cross-file ordering only affects diagnostic line
numbers, not semantics.

## Can I use variables, conditionals, or templates with parameters?

Not in v1. The supported reuse mechanisms are:

- `abstract` tables + `extend` for in-layer column reuse.
- `patch_table` for cross-layer additive changes.
- Layer composition for environment-specific differences.

If your case doesn't fit one of those, the answer for now is to repeat
yourself.

## How do I see what the resolved schema looks like?

The pipeline mutates the parsed `[]DatabaseSpec` in place. After
`Resolve(dbs)` returns, every remaining `TableSpec` has its inherited
fields filled in, engines decoded into their typed structs, abstracts
dropped, and `extend` cleared. That value is what the diff engine and SQL
generator consume.
## How do I model a Prometheus TimeSeries table?

Use the `time_series` engine. The common production shape is the
external-target form, where three target tables already exist (declared
elsewhere in the schema) and the TimeSeries wraps them:

```hcl
table "prom_metrics" {
  column "metric_name" { type = "LowCardinality(String)" }
  # ... outer columns ...

  engine "time_series" {
    samples { target = "default.prom_metrics_data" }
    tags    { target = "default.prom_metrics_tags" }
    metrics { target = "default.prom_metrics_metrics" }
  }
}
```

`hclexp validate` will refuse to resolve until each target table is
declared somewhere in the loaded schema (a new dependency kind
`ts_target`). If you don't manage them via hclexp, declare them with
`engine "merge_tree" {}` (or whichever they actually use) so the
dependency check passes.

Two settings are `ALTER`-able (`id_generator`,
`filter_by_min_time_and_max_time`); every other change is a recreate
and shows up as `-- UNSAFE` in the diff output. The `DATA` alias for
`SAMPLES` is preserved on the dump side via a hidden `KeywordHint` —
HCL authors always write `samples`.

See the `time_series` reference in `README.hcl.md` for the full
attribute list and the inner-target form.
