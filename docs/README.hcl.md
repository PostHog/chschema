# HCL schema reference

This document describes the HCL language used to declare ClickHouse schemas in
this project. It is a reference, not a tutorial — see [FAQ.md](./FAQ.md) for
worked examples.

## File and layer model

A schema is the result of merging an **ordered list of layers**. A layer is
either a **directory** — the loader reads every `*.hcl` file in it, lexically by
filename — or a **single `.hcl` file**, which is simply a layer of one file. The
loader walks the layers in order and merges them into one combined schema. It
has no built-in notion of "base," "env," or "node" — layers are generic. A
typical convention:

```
schema/
  base/        # always loaded
  envs/us/     # loaded only for the US deployment
  nodes/ingestion/
```

is just three layer directories passed to the loader in that order.

A stack may mix the two forms, so a fine-grained addition (one node's extra
table, one shared definition a role pulls in) needs no directory of its own:

```
hclexp validate -layer schema/base,schema/envs/us,schema/nodes/ingest/events.hcl
```

A file layer has the same merge semantics as a directory layer: `patch_table`,
`extend`, and `override = true` behave identically, and its declarations are
applied at its position in the stack. The file must have the `.hcl` extension —
naming anything else is an error, as is naming a path that does not exist.
Listing the same file both directly and through its parent directory declares it
twice, which is the usual duplicate-declaration error.

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

Adding an index to an existing table generates the `ALTER TABLE … ADD INDEX`
plus a companion `ALTER TABLE … MATERIALIZE INDEX` that rebuilds the index for
existing parts. The materialize is a heavy, unpredictable mutation, so it is
**never executed automatically**: `diff -sql` prints it commented out as a
`-- MANUAL:` line, and the JSON/plan output marks it `"manual": true` so
executors skip it. An operator runs it deliberately.

## `projection`

```hcl
projection "by_user" {
  query = "SELECT * ORDER BY user_id"      # one-liner, heredoc, or file()
}

projection "daily_agg" {
  query    = <<-SQL
    SELECT user_id, toDate(ts) AS d, count()
    GROUP BY user_id, d
  SQL
  settings = { index_granularity = "4096" }  # optional; ClickHouse ≥ 26.5
}
```

The `query` is the projection's SELECT (implicit `FROM` the parent table,
optional `GROUP BY` / `ORDER BY`). Like view and MV queries it normalizes to
the parser's beautified canonical form on load, introspect, and dump, so
formatting never diffs as drift. The optional `settings` map renders as the
`WITH SETTINGS (…)` clause (projection-level MergeTree settings; servers
older than 26.5 reject it).

Changing a projection has no in-place ALTER: the diff emits
`DROP PROJECTION` + `ADD PROJECTION`. Adding one to an existing table also
generates a companion `ALTER TABLE … MATERIALIZE PROJECTION` which — exactly
like `MATERIALIZE INDEX` above — is a manual, operator-run statement
(`-- MANUAL:` / `"manual": true`), never executed automatically.

The newer index-form projection (`PROJECTION p INDEX expr TYPE basic`) is not
supported; introspecting a table that uses it fails loudly at parse
(capture it with `-allow-raw` if needed).

## `engine`

The engine is a labeled block; the label is the engine kind. The body's
attributes depend on the kind.

| Kind                                  | Required attributes                                | Optional               |
| ------------------------------------- | -------------------------------------------------- | ---------------------- |
| `merge_tree`                          | —                                                  | —                      |
| `replicated_merge_tree`               | `zoo_path`, `replica_name`                         | —                      |
| `replacing_merge_tree`                | —                                                  | `version_column`, `is_deleted_column` |
| `replicated_replacing_merge_tree`     | `zoo_path`, `replica_name`                         | `version_column`, `is_deleted_column` |
| `summing_merge_tree`                  | —                                                  | `sum_columns = [...]`  |
| `replicated_summing_merge_tree`       | `zoo_path`, `replica_name`                         | `sum_columns = [...]`  |
| `collapsing_merge_tree`               | `sign_column`                                      | —                      |
| `replicated_collapsing_merge_tree`    | `zoo_path`, `replica_name`, `sign_column`          | —                      |
| `aggregating_merge_tree`              | —                                                  | —                      |
| `replicated_aggregating_merge_tree`   | `zoo_path`, `replica_name`                         | —                      |
| `distributed`                         | `cluster_name`, `remote_database`, `remote_table`  | `sharding_key`, `policy_name` (requires `sharding_key`) |
| `log`                                 | —                                                  | —                      |
| `kafka`                               | `broker_list = [...]`, `topic`, `consumer_group`, `format` | —              |
| `time_series` (experimental)          | —                                                  | `settings`, `tags_to_columns`, nested `samples`/`tags`/`metrics` blocks |
| `join`                                | `strictness` (`ANY`/`ALL`/`SEMI`/`ANTI`), `type` (`LEFT`/`INNER`/`RIGHT`/`FULL`), `keys = [...]` | — |
| `null`                                | —                                                  | —                      |
| `memory`                              | —                                                  | —                      |
| `merge`                               | `db_regex`, `table_regex`                          | —                      |
| `buffer`                              | `database`, `table`, `num_layers`, `min_time`, `max_time`, `min_rows`, `max_rows`, `min_bytes`, `max_bytes` | `flush_time`, `flush_rows`, `flush_bytes` |

`is_deleted_column` (ClickHouse's `is_deleted` ReplacingMergeTree parameter:
rows with a `1` in that column are delete markers) requires `version_column`,
matching ClickHouse's own rule that `is_deleted` can only be used with `ver`.

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

Cross-layer modification of an existing table: the table stays declared
once, and an env layer patches just its delta.

```hcl
database "posthog" {
  patch_table "events" {
    column "us_session_id" { type = "String" }
    settings = { default_compression_codec = "lz4" }
  }
}
```

- The target table must exist somewhere in the merged config.
- `column` blocks are strictly additive: adding a column that already exists
  on the target is an error.
- `settings` merges into the target's map, **patch wins** on key collision —
  an env overlay that retunes a base setting is the point. A table whose
  envs differ by one setting stays declared once; the env layer carries a
  one-line patch. Patches accumulate in layer order, so a later layer's
  patch wins over an earlier one.
- Anything else — `engine`, `order_by`, `index`, etc. — is rejected at
  parse time.
- `patch_table` lives at any layer.

## Inheritance — `extend = "Y"`

A `table` can declare `extend = "Y"`, where `Y` is another table (or an
abstract table) in the same database. The parent does **not** have to be
abstract: a concrete table can be extended, and then both are emitted.

The child inherits:

- All `column` blocks (appended; collisions with child's own columns error).
- All `index` blocks (appended; collisions error).
- `engine`, `order_by`, `partition_by`, `sample_by`, `ttl`, `settings`
  — if the child does **not** set its own; otherwise the child's value
  replaces the inherited one.

The child does **not** inherit:

- `primary_key`, `comment` — declare them on the child if it needs them.
- `constraint` and `projection` blocks.
- `cluster` — the table-level attribute never flows through `extend`;
  the database-level `cluster` default cascades into every emitted
  table separately, after resolution, which covers the common case.

> **Settings replace wholesale — they do not merge.** A child that sets
> `settings = { default_compression_codec = "lz4" }` loses every
> inherited key (`index_granularity`, …) and ends up with exactly that
> one setting. This is deliberate: an `extend` child is a *new table*
> and is authoritative about whatever it sets. If what you actually
> want is "the same table, with one setting adjusted", you want
> `patch_table` — its `settings` *merge* (see below).

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

### Long queries: heredoc and `file()`

A view/MV `query` can be written three ways — all equivalent:

```hcl
# one line
query = "SELECT a, b FROM t WHERE x = 1"

# heredoc (multi-line, readable)
query = <<-SQL
  SELECT a, b
  FROM t
  WHERE x = 1
SQL

# external .sql file, resolved relative to this HCL file
query = file("my_query.sql")
```

The loaded query is **normalized to a canonical (beautified) form** before any
comparison, so source formatting never shows as drift: a heredoc-formatted
query, a one-liner, and a `file()` reference to the same SQL all diff equal.
`introspect`/`dump` emit long queries as heredocs, so a captured schema is
readable out of the box. A query the SQL parser can't handle is kept verbatim
(with a warning) rather than blocking the load.

`file()` works for any string attribute (e.g. a long `default`/`materialized`
expression), not just `query`; the path resolves relative to the HCL file that
calls it.

An `abstract = true` materialized_view is accepted for symmetry (it is
dropped after resolution, like an abstract table), but has no common use
case — MVs are usually concrete glue.

## Resolution pipeline

When the loader processes a layered set, this pipeline runs:

1. **Parse** every `.hcl` file in every layer (ordered).
2. **Merge** databases by name. Tables collide on name unless the later one
   sets `override = true`. `patch_table` blocks accumulate.
3. **Apply `patch_table`s** — in layer order, against their targets: columns
   additively, settings patch-wins.
4. **Resolve `extend` chains** — DFS with cycle detection; children see the
   post-patch parent.
5. **Drop abstract tables** from the emit set.
6. **Validate** — every remaining (non-abstract) table must have an engine.

## Comparison: `extend` vs `patch_table` vs `override`

The three mechanisms answer different questions — the choice follows from
which question you are asking:

- **`extend`**: *"these are **different tables** that share a shape."*
  `events_local` and `events_distributed` both look like an abstract
  `_event_base`, but each is its own table with its own name and engine.
  `extend` always produces a **new declaration**.
- **`patch_table`**: *"this is the **same table**, and one layer wants to
  adjust it."* The table stays declared exactly **once**; the env layer
  contributes a modification, not a declaration.
- **`override = true`**: *"in this environment the table is genuinely
  different."* A full replacement declaration, sanctioned by the flag.

The declaration count is not cosmetic: `hclexp locate -duplicates` (the
once-only CI guard) treats `patch_table` and `override` sites as
legitimate, but an extend-child-per-environment pattern is N distinct
declarations — `extend` cannot express "the same table, varied per env",
and using it that way reintroduces the duplication the layer discipline
exists to prevent.

The other asymmetry worth internalizing: **`extend`'s `settings` replace
wholesale, `patch_table`'s `settings` merge (patch wins per key)**. An
extend child is a new table and owns whatever it sets; a patch adjusts an
existing table, which stays authoritative except where patched.

| Need                                              | Use            |
| ------------------------------------------------- | -------------- |
| Two tables share most columns; different engines  | `extend` + `abstract` parent |
| Add a column to the same table in one environment | `patch_table`  |
| Change one setting on the same table in one environment | `patch_table` with `settings` |
| A table differing per env by engine / `order_by` / dropped columns | `override = true` |
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
| `-format`   | `text`            | `text` (prose report) or `json` (see [Structured comparison output](#structured-comparison-output)) |
| `-exclude`  | —                 | exclude config; matching objects are dropped from every node before comparing |

ClickHouse expands the `{uuid}` macro to the table's literal UUID at
`CREATE` time (while keeping `{shard}`/`{replica}` as macros), so the same
table on different shards gets a different `zoo_path` — pure noise for
drift. The default `-zk-paths=mask-uuid` collapses that back to `{uuid}`
so only genuine path differences register.

A drifting node is printed as `✗ <name>: <summary>` (e.g. `+16 table, -8
table, +8 mv`); a fully consistent group prints `OK (all identical)`. The
command exits non-zero when any drift is found, so it works as a CI guard.
`-format json` emits the same comparison as structured data (see
[Structured comparison output](#structured-comparison-output)).

Direction is descriptive, not prescriptive: a drifter's changes describe how it
differs **from its reference**, and the reference is merely the group's
lexically-first member — not a source of truth. For desired state, diff against
the HCL with `plan`/`diff`.

`hostClusterRole` is coarse and can merge distinct pools (`ingestion`
spans ingestion-events / -medium / -small; `data` spans online and offline
nodes). `-group-by role` uses the deployment role from the node name and
usually isolates genuine drift.

## Excluding transient objects — `-exclude`

A live cluster carries transient tables and dictionaries you don't want in a
committed dump: ClickHouse's atomic-replace temporaries (`_tmp_replace_*`),
migration/DAG scratch tables (`tmp_*`), backups, staging, and backfills. Their
DDL also often can't be parsed, which would otherwise abort introspection.

`introspect`, `dump-cluster`, `diff`, `plan`, `drift`, and `load` all take
`-exclude <file>`, an HCL config:

```hcl
exclude {
  patterns     = ["_tmp_replace_*", "tmp_*", "*_backup", "*_backup_*", "*_staging", "*_backfill"]
  object_types = ["named_collection"]   # optional: drop a whole class, whatever its name
}
```

```bash
hclexp introspect   -database posthog -exclude exclude.hcl -out posthog.hcl
hclexp dump-cluster -cluster ops -out-dir ./prod -exclude exclude.hcl
hclexp diff  -left ./schema -right clickhouse://... -exclude exclude.hcl
hclexp drift -dir ./prod -exclude exclude.hcl
```

Patterns are globs (`*` `?` `[..]`), matched against both the bare object name
and the `<database>.<name>` qualified form (so `posthog.*_staging` scopes to one
database). `object_types` excludes a whole class regardless of name — valid
values are `table`, `materialized_view`, `view`, `dictionary`, `raw`, and
`named_collection` (useful when, say, named collections hold secrets managed out
of band).

On `introspect`/`dump-cluster` a matching object is **skipped before its DDL is
parsed**, so it neither appears in the dump nor breaks introspection. On the
comparison commands (`diff`, `plan`, `drift`) **both sides** are filtered before
the diff runs, so an excluded object appears in no output, no operation, and no
count — no post-filtering of the JSON needed. A starter config is at
[`examples/exclude.hcl`](../examples/exclude.hcl).

### Layer surgery — `load -only` / `-exclude-objects`

`load` additionally takes the filter as ad-hoc globs, in both directions, so
a layer can be split without leaving hclexp (and without hand-parsing HCL,
which silently loses shapes like the two-label `raw "dictionary" "x" {}`):

```bash
# the shared layer: only the objects identical everywhere
hclexp load -layer overrides/data/dev -only "$LIST" -out overrides/data/cloud/tables.hcl

# each env layer: everything except those
hclexp load -layer overrides/data/dev -exclude-objects "$LIST" -out overrides/data/dev/tables.hcl
```

`-only <glob,...>` keeps only the matching objects; `-exclude-objects
<glob,...>` drops them; both use the same bare-or-qualified glob matching as
the exclude config and compose with `-exclude <file>` (an object survives iff
it matches `-only`, when given, and neither exclusion). Filtering applies to
the emitted resolved schema — in manifest mode to every composed role — and
removes objects only: the `database {}` wrapper survives even when emptied
(both halves of a split still need it), and `node {}` blocks are untouched.

## SQL → HCL edits — `hclexp sql2hcl`

You already know ClickHouse SQL; `sql2hcl` lets you change a schema with the
DDL you'd naturally write instead of hand-editing HCL. It loads your existing
HCL (the **left side**), applies one or more SQL statements to it, and emits the
updated HCL.

```sh
# Edit from stdin, preview on stdout
echo 'ALTER TABLE db.events ADD COLUMN name String AFTER id;' \
  | hclexp sql2hcl -left ./schema

# Edit from a file, write the updated schema, then preview the migration
hclexp sql2hcl -left ./schema -in change.sql -out /tmp/updated.hcl
hclexp diff -left ./schema -right /tmp/updated.hcl -sql
```

| Flag | Default | Meaning |
|------|---------|---------|
| `-left`       | —      | HCL schema to modify: a comma-separated layer stack of directories or `.hcl` files (required) |
| `-in`         | stdin  | SQL file to apply (`-` also means stdin) |
| `-out`        | stdout | empty → stdout; a directory → one `<db>.hcl` per database; else a single file |
| `-database`   | —      | default database for unqualified object names (e.g. `CREATE TABLE foo`, not `db.foo`) |
| `-allow-raw`  | off    | capture a `CREATE` the model can't express as a [`raw`](#raw) block instead of failing |

**Supported statements** (declarative schema changes):

- `CREATE TABLE | MATERIALIZED VIEW | VIEW | DICTIONARY` — adds the object, or
  replaces an existing object of the same name.
- `ALTER TABLE … ADD/DROP/MODIFY/RENAME COLUMN`, `ADD/DROP INDEX`,
  `MODIFY/REMOVE TTL`, `MODIFY/RESET SETTING` — edits the matching `table` block.
  `RENAME COLUMN` records [`renamed_from`](#column) so a later diff emits
  `RENAME COLUMN` rather than drop + add.
- `ALTER TABLE <mv> MODIFY QUERY …` — replaces a materialized view's `query`.
- `DROP TABLE | VIEW | DICTIONARY | DATABASE` — removes the object (`IF EXISTS`
  makes a missing target a no-op).
- `RENAME TABLE a TO b` — renames, moving across databases when they differ.

**Scope boundary.** `sql2hcl` works on schema *state*, not data: `TRUNCATE`,
`ALTER … DELETE`, partition operations (attach/detach/drop/freeze/replace), and
`MATERIALIZE INDEX/PROJECTION` are rejected with a clear error. The output is the
resolved (flat) schema — `sql2hcl` does **not** rewrite your layered source files
in place. Review the emitted HCL (or the `diff -sql` it implies) and integrate.
The default database for an unqualified name comes from `-database`, or, when the
left side has exactly one database, that database.

## Cross-role planning — `hclexp plan`

A node's schema is composed along two axes — **environment** (dev/prod-us/…)
and **node role** (ops/data/logs/…). When an object on one role references an
object physically hosted on another — a per-role `writable_query_log_archive`
(Distributed) forwarding to the OPS `sharded_query_log_archive` — no
single-role diff sees both ends, so the migration order can't be reconstructed
by merging per-role diffs. `hclexp plan` diffs **every role in one run** and
emits a single, globally-ordered operation list with cross-role dependency
ordering.

```bash
hclexp plan -manifest roles.hcl -env prod-us -dump ./topology -format json
```

The **manifest** is HCL, role-first with one `env` block per environment a role
is deployed in (so all of a cluster's environments sit in one place):

```hcl
role "ops" {
  env "prod-us" { layers = ["base", "prod", "env/prod-us"] }
  env "prod-eu" { layers = ["base", "prod", "env/prod-eu"] }
}
role "data" {
  # not in every env; "shared/events.hcl" is a single-file layer
  env "prod-us" { layers = ["base", "env/prod-us", "shared/events.hcl"] }
}
```

- `-env` selects each role's matching `env` block; a role with no block for the
  env is not deployed there and is skipped. The composed stack is that role's
  **desired** schema. As everywhere a layer stack is accepted, a `layers` entry
  is a directory or a single `.hcl` file (see
  [File and layer model](#file-and-layer-model)).
- `-dump` is a directory of per-node current-state HCL (e.g. from
  [`dump-cluster`](#cross-node-drift--hclexp-drift)); nodes are matched to roles
  by their `hostClusterRole` macro, and replicas collapse to one representative
  per role. `role` must equal `hostClusterRole`.
- `-layer-root` prefixes the manifest's layer paths (point it at a committed
  snapshot or the working tree).
- Output: `-format json` (default) or `text`. CREATE and widening ALTERs flow in
  dependency order (a referenced object before its referrers — storage → proxies
  → MV); DROP runs in reverse. Identical statements across roles dedupe to one
  operation carrying the union of contributing `roles`. Alongside the merged
  `operations`, the JSON carries a `roles` list: each role's own
  [object comparisons](#structured-comparison-output) with derived counts,
  deliberately **not** deduped (triage is per role, execution is global).
- `-exclude` drops matching objects from both sides of every role's diff.

## Locating declarations — `hclexp locate`

`locate` answers two questions the layer tree makes hard to grep for:
*where is object X declared?* and *is X declared more than once?* It is
query-only — no diffing, no DDL.

```bash
# Every declaration site of a table, and which (role, env) stacks place it
hclexp locate -manifest manifest.hcl -layer-root ./schema events

# Globs work like exclude patterns: bare name or db.name qualified.
# Several patterns are independent existence checks (exit 1 if any misses).
hclexp locate -manifest manifest.hcl -layer-root ./schema 'posthog.person_*' events

# Also search per-node dumps (introspect / dump-cluster output)
hclexp locate -manifest manifest.hcl -layer-root ./schema -dump ./dumps events

# Ad-hoc layer dirs or .hcl files, no manifest required (no placements)
hclexp locate -layer ./schema/shared,./schema/ingestion events

# CI guard: any object declared at more than one plain site exits 1
hclexp locate -manifest manifest.hcl -layer-root ./schema -duplicates
```

For each matching object (tables, MVs, views, dictionaries, named
collections, raw blocks), `locate` lists every declaration site as
`file:line` plus its control flags (`[abstract]`, `[override]`,
`[patch_table]`, `extends <parent>`, `[raw <kind>]`), and derives the
**placements**: the (role, env) stacks whose manifest layer lists include
the declaring layer. Unlike `plan`/`load`, the manifest is read across
*all* envs. An object extended by others cross-links its children
(`extended by: ...`), computed over every authored declaration — the
children need not match the pattern themselves. `-layer` adds ad-hoc
layer dirs or files (searched after the manifest's layers, deduped
against them, no placements), so the same queries work before a manifest
exists. With `-dump DIR`, the per-node `.hcl` dump files declaring the
object are listed too, each attributed to its node (the dump's `node {}`
block, else the filename stem — the same identity `drift` uses).
`-format json` emits the same document structurally, with the queried
`patterns` echoed back.

Objects are grouped by `(database, name)` — the namespace ClickHouse
object types share — so a stray `view "events"` next to a `table
"events"` shows up as one entry with both types.

`-duplicates` (no name argument; requires `-manifest` or `-layer`) audits
the once-only discipline: `load`/compose only reject a redeclaration when
the two layers meet in one stack, so two layers that never co-compose can
silently hold divergent copies of the same object. A site is a *plain*
declaration unless it is a `patch_table` (additive), has `override =
true` (deliberate replacement), or is `abstract` (dropped at resolve);
any object with two or more plain sites is reported and the command
exits 1.

Exit codes: 0 found / no duplicates; 1 any pattern without a match,
duplicates found, or a load error; 2 usage.

## Structured comparison output

`diff -format json`, `plan`, and `drift -format json` all describe a comparison
with the same per-object model, so an agent or CI gate never has to parse DDL
strings to learn what changed.

```bash
hclexp diff -left ./schema -right clickhouse://... -format json | jq '.summary'
```

```json
{
  "objects": [
    {
      "database": "posthog", "object": "events", "object_type": "table",
      "status": "altered",
      "changes": [
        {"field": "column:event",   "change": "add",    "new": "String"},
        {"field": "column:team_id", "change": "modify", "old": "UInt32", "new": "UInt64"}
      ],
      "operations": [
        {"order": 1, "kind": "ALTER", "object_type": "table",
         "database": "posthog", "object": "events", "engine": "MergeTree",
         "sql": "ALTER TABLE posthog.events ADD COLUMN event String, MODIFY COLUMN team_id UInt64",
         "manual": false, "unsafe": false}
      ],
      "unsafe": false
    }
  ],
  "operations": [ "… the same ops, flat and dependency-ordered — the execution view" ],
  "summary": {"tables_added": 1, "tables_altered": 1, "…": 0}
}
```

`objects` is the per-object view and `operations` the flat execution view of the
**same** diff; an object's nested `operations` carry their index into the global
list as `order`, so the two can never disagree about sequencing. `summary` counts
are derived from `objects` (keys: `tables_added`/`_dropped`/`_altered`, same for
`mvs_`, `views_`, `dicts_`, `raws_`, plus `named_collections_changed`).

**`status` is right-relative:** `added` means the right side of the comparison
has the object and the left does not. `diff` and `plan` put the *desired* schema
on the right (so `added` will be CREATEd), while `drift` puts the *drifter* on
the right — its output describes how that node differs from its group reference,
and is **not** a fix script.

An object with an unsafe change carries `unsafe: true` and `unsafe_reason`, and
may have **no** operations at all: an in-place-impossible change (ORDER BY,
engine, a raw table recreate) is reported, never auto-emitted. Raw blocks also
carry `raw_kind` (`table`/`view`/`dictionary`/…) — only a raw *table* holds rows,
so its DROP+CREATE is the destructive one.

### `field` vocabulary

`changes` is only present on `altered` objects. Each entry has a `field`, a
`change` (`add` | `drop` | `modify` | `rename`), and the `old`/`new` values
(omitted when a side is unset).

| `field` | Applies to |
|---|---|
| `column:<name>` | table |
| `index:<name>`, `projection:<name>`, `constraint:<name>` | table |
| `setting:<name>` | table |
| `engine`, `order_by`, `primary_key`, `partition_by`, `sample_by`, `ttl` | table |
| `comment` | table, view, named collection |
| `query` | view, materialized view |
| `to_table`, `columns` | materialized view (either forces a recreate) |
| `column_aliases`, `sql_security`, `definer`, `cluster` | view (each forces a recreate) |
| `param:<name>`, `on_cluster` | named collection |
| `sql` | raw block |
| a dotted config path (`layout`, `source.clickhouse.table`, …) | dictionary |
| `source.<secret>` (`source.password`, `source.credentials_password`) | dictionary — a credential hclexp could not verify |

How values render: a column as a compact descriptor (`Nullable(String) MATERIALIZED
upper(s) CODEC(LZ4)`), an engine as its SQL clause, `order_by`/`primary_key`
comma-joined. A rename is reported on the **new** name (`column:<new>`, with `old`
= the previous name). Two cases carry no per-field values, because the diff holds
none: a dictionary reconciles via `CREATE OR REPLACE`, so it emits one `modify`
per changed config path; and a named-collection `param:` set is always `modify`
with `new` only (`ALTER … SET` overwrites, even for a param that is new). A param
that is redacted (`[HIDDEN]`) on one side while the other side holds a real value,
differs in flags, or lacks it entirely reports `[HIDDEN]` on **both** sides — hclexp
could not verify equality, and printing the known side would leak a real secret into
CI logs. Identically-redacted params (hidden on both sides, flags equal) are not a
difference at all, so identically-redacted dumps compare clean. A dictionary's
`source.<secret>` reports the same way, for the same reason.

### Secrets and the `[HIDDEN]` marker

ClickHouse replaces a secret with the literal `[HIDDEN]` when the introspecting
user lacks `displaySecretsInShowAndSelect` (or the server/query does not enable
`display_secrets_in_show_and_select`). This affects named-collection param
values and a dictionary's `SOURCE(...)` credentials.

hclexp keeps that marker rather than dropping it, so it round-trips through an
HCL dump: a dump with `password = "[HIDDEN]"` says *"this object has a secret I
cannot see"*, which a dump with no `password` at all cannot. Three rules follow,
and they are the same for both object kinds.

**It is never written back.** No generated statement may contain `[HIDDEN]` —
writing the literal would overwrite the real credential with a placeholder. Any
statement built from a spec carrying the marker is refused, with an unsafe
reason naming the offending fields.

**It compares as unknown, not as a value.** Both sides `[HIDDEN]` → equal, and
silently (the normal `drift` case, where every node was dumped by the same user;
two genuinely different hidden secrets do read as equal — no observer without
the grant can do better). One side `[HIDDEN]` and the other a real value → the
field is reported as unverifiable and excluded from the comparison, so an
authored secret is not mistaken for a change on every run. `[HIDDEN]` versus
*absent* is a real difference: present-vs-absent is visible even when the value
is not.

**You may write it deliberately.** `password = "[HIDDEN]"` in authored HCL
declares a secret managed outside hclexp. It compares clean against a redacting
cluster.

What a blocked emission costs you differs by kind, because the two reconcile
differently:

- A **dictionary** is rewritten whole (`CREATE OR REPLACE DICTIONARY` — how
  *every* dictionary change is applied), and that rewrite includes the source
  credential. So an unknown secret blocks **any** change to that dictionary; a
  whole-object rewrite cannot preserve a secret it does not know.
- A **named collection** has surgical DDL (`ALTER … SET` / `DELETE`), so a
  normal param change still applies while a secret stays unknown. Only the
  statements that write every param — `CREATE NAMED COLLECTION`, and the
  DROP+CREATE pair an `ON CLUSTER` change forces — are blocked. The recreate is
  blocked as a *pair*: dropping a collection hclexp cannot then recreate would
  destroy it.

If an object reports an unverifiable secret on every run, pick one:

1. Grant `displaySecretsInShowAndSelect` and set
   `display_secrets_in_show_and_select=1` for the introspecting user — the
   secret becomes visible, comparisons are exact, and rotating a credential
   through hclexp works. Note that dumps then contain real secrets in
   plaintext; treat the artifacts accordingly.
2. Declare it unmanaged: `password = "[HIDDEN]"` (above).
3. `-exclude` the object entirely — the bluntest option.

A passworded dictionary whose authored HCL simply *omits* the password is a
genuine difference, and the generated `CREATE OR REPLACE` will **remove** the
live password. Say what you mean: the real value, or the marker.

### `drift -format json`

Same objects, wrapped per node:

```json
{
  "groups": [{
    "key": "ops", "reference": "node-a", "nodes": 2,
    "drifters": [{
      "node": "node-b", "file": "prod/node-b.hcl", "macros": {"…": "…"},
      "objects": [ "… ObjectComparison, reference -> this node …" ],
      "summary": {"raws_altered": 1, "…": 0}
    }]
  }],
  "summary": {"nodes": 2, "groups": 1, "groups_with_drift": 1, "drifting_nodes": 1}
}
```

The text report's one-liner (`✗ node-b: ~1 raw`) is rendered from the same
`summary`, so every object kind counts — a node drifting only in a `raw` block or
a named collection is reported as such.

## Browsing a schema — `hclexp web`

`hclexp web -config <file>` (or `-layer <dirs>`) loads and resolves an HCL
schema and serves a read-only web UI (default `-addr :8080`) to browse
databases, objects, their columns/engine/settings, and dependency cross-links.
No cluster connection.

The server **auto-reloads** when you edit the source: on each request it
re-stats the source files at most once per `-reload-interval` (default `2s`;
set `0` to disable) and reloads the schema when a file's mod time changes — so
you can edit HCL and refresh the browser. A syntactically broken edit is logged
and the last good schema keeps serving.

To browse a whole fleet at once, pass a **`-manifest`** (the same role/env/layers
format as [`hclexp plan`](#cross-role-planning--hclexp-plan)):

```bash
hclexp web -manifest roles.hcl -layer-root . [-env prod-us]
```

Every `(env, role)` the manifest declares is composed and served in one process:
a schema list at `/`, each schema under `/s/<env>/<role>/`. `-env` filters to a
single environment; `-layer-root` prefixes the manifest's layer paths. Each
composed schema auto-reloads from its own layers like the single-schema mode.

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

## System-table proxies in the diff

A `Distributed` proxy whose `remote_database` is `system` compares columns
**subset-tolerantly** in the comparison engine (`diff`, `plan`, `drift`).
System tables are server-defined and gain columns with version bumps, so a
live proxy created from a fuller column set (e.g. `AS system.processes` on a
newer server) routinely carries columns the layer intentionally omits — that
is not drift, and reporting it would block convergence forever. Column
*presence* differences on such proxies are suppressed in either direction;
columns declared on **both** sides still compare fully, so a real type change
surfaces as `MODIFY COLUMN`, and an engine change still yields the full
column diff.

Non-system proxies keep exact column semantics: their remotes are
layer-managed, so an extra column there is genuine drift. (This mirrors
`validate`'s proxy-column check, which is subset-tolerant by default —
see the validate docs.)

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