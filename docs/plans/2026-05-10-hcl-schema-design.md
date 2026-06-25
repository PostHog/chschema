# HCL schema design

## Context

The current schema model uses Protobuf as both the on-disk representation
(via YAML) and the in-memory data model. YAML+protojson is awkward for humans,
has poor comment ergonomics, and offers no expression language for reuse.

This design moves the source-of-truth schema language to HCL. HCL is
comment-aware, supports labeled blocks naturally, and `gohcl` gives strong
static decoding (required attrs, type checks, unknown-attribute rejection).
The existing proto types remain useful as an internal data model for the diff
engine and executor; HCL is what users edit and review.

## Goals

- Define `table`, `materialized_view`, `view`, and `dictionary` blocks in HCL.
- Per-engine validation at parse time (kind-specific required fields, unknown
  attribute rejection).
- Multi-layer overlays without duplicating shared definitions, with no "env"
  concept baked into the data model.
- Catch broken materialized views before PR merge (static + live).

## Non-goals (deferred)

- Templates with parameters; templates referencing other templates.
- `extend_table` modifying engine, `order_by`, `partition_by`, `ttl`,
  `settings`, indexes, `sample_by`.
- Per-key force-merge in `settings`.
- Conditional blocks inside a single file (`var.env == "us" ? ...`).
- HCL expression-language features beyond plain decoding.
- Removing the YAML loader. Kept for round-trip compatibility during transition.

## Block grammar

### `database`

Top-level container, labeled by database name. Contains `table`,
`extend_table`, `materialized_view`, `view`, `dictionary`, and `template`
blocks.

```hcl
database posthog {
  table events { ... }
  extend_table events { ... }
  materialized_view events_daily { ... }
  template event_columns { ... }
}
```

### `table`

Labeled by table name. Full table definition.

```hcl
table events {
  use_template "event_columns" {}
  column ingestion_ts { type = "DateTime" }

  engine "replicated_merge_tree" {
    zoo_path     = "/clickhouse/tables/{shard}/events"
    replica_name = "{replica}"
  }

  order_by      = ["timestamp", "team_id"]
  partition_by  = "toYYYYMM(timestamp)"
  ttl           = "timestamp + INTERVAL 2 YEARS"
  settings      = { ttl_only_drop_parts = "1" }

  index idx_team {
    expr        = "team_id"
    type        = "minmax"
    granularity = 4
  }
}
```

Optional `override = true` declares this block replaces an earlier-layer
`table` of the same name (loud, rare).

### `column`

Labeled by column name. Required `type` attribute. Lives inside `table`,
`extend_table`, or `template`.

### `engine`

Labeled by engine kind; decoded in two passes — gohcl reads the label and the
remaining body, then dispatches to a kind-specific Go struct via switch.
Unknown kinds error out with a position pointing at the label.

Supported kinds (v1, parity with the existing proto support):

| Kind                                     | Required attrs                                                  | Optional attrs              |
| ---------------------------------------- | --------------------------------------------------------------- | --------------------------- |
| `merge_tree`                             | —                                                               | —                           |
| `replicated_merge_tree`                  | `zoo_path`, `replica_name`                                      | —                           |
| `replacing_merge_tree`                   | —                                                               | `version_column`            |
| `replicated_replacing_merge_tree`        | `zoo_path`, `replica_name`                                      | `version_column`            |
| `summing_merge_tree`                     | —                                                               | `sum_columns = [...]`       |
| `collapsing_merge_tree`                  | `sign_column`                                                   | —                           |
| `replicated_collapsing_merge_tree`       | `zoo_path`, `replica_name`, `sign_column`                       | —                           |
| `aggregating_merge_tree`                 | —                                                               | —                           |
| `replicated_aggregating_merge_tree`      | `zoo_path`, `replica_name`                                      | —                           |
| `distributed`                            | `cluster_name`, `remote_database`, `remote_table`               | `sharding_key`              |
| `log`                                    | —                                                               | —                           |
| `kafka`                                  | `broker_list = [...]`, `topic`, `consumer_group`, `format`      | —                           |

### `extend_table`

Labeled by target table name. Strict additive semantics in v1:

- **Allowed:** add `column` blocks. Error on a column name that already exists on the target.
- **Forbidden:** `engine`, `order_by`, `partition_by`, `ttl`, `settings`,
  `index`, `sample_by`. These aren't fields on the extension struct, so gohcl
  rejects them automatically with an HCL position diagnostic.
- The target table must exist somewhere in the merged config (any layer).

Allowed in any layer — the loader does not distinguish "base" from "overlay."

### `materialized_view`

```hcl
materialized_view events_daily {
  to_table = "events_daily_local"
  populate = false

  select = <<-SQL
    SELECT toDate(timestamp) AS day,
           team_id,
           countState() AS event_count
    FROM events
    GROUP BY day, team_id
  SQL
}
```

### `view`, `dictionary`

Mirror the existing proto shapes; HCL fields map 1:1. Detail to be filled in
during implementation, since these are less central to the multi-env story.

### `template`

Top-level under `database`, labeled by template name. Bundles **columns
only** in v1.

```hcl
template event_columns {
  column timestamp  { type = "DateTime" }
  column team_id    { type = "UInt64" }
  column event      { type = "String" }
  column properties { type = "String" }
}
```

`use_template "name" {}` inside a `table` splices the template's columns at
that point. Collisions across template applications, and between templates and
inline columns, are errors. Templates cannot `use_template` (no nesting).

## Loader and composition

The loader takes an **ordered list of layers**, each a directory of HCL files:

```bash
chschema validate \
  --layer schema/base \
  --layer schema/envs/us \
  --layer schema/nodes/ingestion
```

The loader has no built-in concept of "env" or "base"; `--env` and
`--node-type` may later exist as CLI sugar that expands to a `--layer` chain.

### Resolution pipeline

1. **Parse.** Walk every layer in order; parse all `.hcl` files with `hclparse`.
2. **Collect.** Build registries keyed by qualified name (`database.table`,
   `database.template`, etc.). Track which layer each declaration came from.
3. **Templates.** Resolve `use_template` references; expand columns into the
   owning `table` body. Detect collisions.
4. **Extensions.** Apply `extend_table` patches in layer order against the
   post-template column set.
5. **Override resolution.** A `table` with `override = true` replaces an
   earlier-layer `table` of the same name. Without `override`, duplicate
   `table` names across layers error.
6. **Validation.** Engine-specific checks, MV cross-references, dictionary
   checks (see below).
7. **Lower to proto.** Convert the resolved HCL data into existing
   `chschema_v1.*` messages so the diff engine and executor work unchanged.

## Materialized-view validation

Two layers, both runnable in CI before merge.

### Static (in Go, no DB)

After resolution:

- `to_table` resolves to a known table in the merged config.
- Source tables in `FROM ...` exist. SELECT parsed via a ClickHouse SQL parser
  library (chosen during implementation; fallback regex extraction acceptable
  for v1).
- Projection alias names match target column names 1:1.
- For `AggregatingMergeTree` / `ReplicatedAggregatingMergeTree` targets:
  target columns of type `AggregateFunction(...)` align with the `-State`
  aggregate functions in the SELECT.
- Target's `order_by` columns appear in the SELECT projection.

Diagnostics carry `hcl.Range` positions.

### Live (in CI)

`chschema validate --layer ... --live` boots ClickHouse via the existing
`docker-compose` setup, applies the resolved schema to an empty database, and
reports anything the server rejects. Strongest signal; catches what the static
layer can't see.

## Suggested file layout (convention only)

```
schema/
  base/
    tables/*.hcl
    materialized_views/*.hcl
    templates/*.hcl
  envs/
    us/{tables,materialized_views}/*.hcl
    eu/{tables,materialized_views}/*.hcl
  nodes/
    ingestion/{tables,materialized_views}/*.hcl
```

The loader takes an ordered list of layer directories; layout is not enforced.

## Critical files

To create:

- `internal/loader/hcl/types.go` — HCL struct definitions
  (`DatabaseSpec`, `TableSpec`, `ExtendTableSpec`, `TemplateSpec`,
  `EngineSpec`, `MaterializedViewSpec`, ...).
- `internal/loader/hcl/engines.go` — per-kind engine structs and the
  label-to-struct dispatch.
- `internal/loader/hcl/parser.go` — file walking + `gohcl.DecodeBody` + diagnostic plumbing.
- `internal/loader/hcl/resolve.go` — template expansion, `extend_table`
  application, layer composition.
- `internal/loader/hcl/lower.go` — resolved HCL → `chschema_v1.*` proto.
- `internal/validate/mv.go` — static MV validator.

To modify:

- `cmd/hclexp/hclexp.go` — replace the empty `EngineSpec struct{}` with the
  labeled-block design as a standalone tester for the parser.
- `cmd/chschema/main.go` (later) — wire `--layer` and a `validate` subcommand.

Untouched (per CLAUDE.md):

- `gen/` (generated proto code).
- Existing YAML loader (kept for round-trip compatibility during transition).

## Verification

- **Unit:** `go test ./internal/loader/hcl/... -v`
  - Per-engine kind decode round-trip.
  - `extend_table` accepts additions; rejects engine, `order_by`, etc., with
    HCL position diagnostics.
  - Template expansion happy path + collision detection.
  - Layer composition: later-layer `table` without `override` errors; with
    `override` replaces.
- **Integration:** `go test ./test -v` — end-to-end fixture: HCL files across
  layers → resolved → DDL.
- **MV validation:** dedicated tests for unknown `to_table`, missing source
  table, AggregateFunction mismatch.
- **Live:** `go test ./test -v -clickhouse` runs `validate --live` against
  docker-compose ClickHouse.
- **Manual sanity:** `chschema validate --layer cmd/hclexp/` against
  `node.conf`.

## Future work (decisions deferred)

- Templates with parameters; nested template expansion.
- `extend_table` modifying engine, `order_by`, `partition_by`, `ttl`,
  `settings`, indexes, `sample_by`.
- Per-key force-merge in `settings` (`force = true`).
- HCL expression language usage (variables, locals, functions).
- CLI ergonomics for layer selection (`--env`, `--node-type` flags).
- YAML loader deprecation timeline.
