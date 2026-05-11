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