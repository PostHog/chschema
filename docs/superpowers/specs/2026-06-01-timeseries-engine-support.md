# TimeSeries Engine Support in hclexp — Design Spec

**Date:** 2026-06-01
**Status:** Approved, ready for implementation
**Reference docs:** `clickhouse-docs/en/engines/table-engines/integrations/time-series.md` (full file; in particular the *Syntax*, *Target tables*, *Creation*, and *Settings* sections)

## Motivation

`hclexp introspect` against PostHog production fails the entire database dump on the first table whose engine the chparser cannot tokenize. The reported case:

```
parse create_table_query for posthog.prom_metrics:
  parser: line 0:431 <EOF> or ';' was expected, but got: "DATA"
CREATE TABLE posthog.prom_metrics (...) ENGINE = TimeSeries
  DATA posthog.prom_metrics_data
  TAGS posthog.prom_metrics_tags
  METRICS posthog.prom_metrics_metrics
```

`TimeSeries` is an experimental ClickHouse engine for storing Prometheus-style time series. It declares three sibling target tables (`samples`/`tags`/`metrics`) either as external table references or via inline `INNER COLUMNS` + `INNER ENGINE` clauses. The chparser fork has no handling for this grammar; the introspector has no handling for the engine kind. We need both.

The companion need ("be able to keep dumping when some engines aren't supported") is split off into a separate spec on skip mechanics.

## Surface (HCL)

A new engine kind `time_series` available inside `table` blocks, with three optional repeatable-style sub-blocks (one per target kind).

```hcl
table "prom_metrics" {
  # Outer columns (CH may auto-generate them; explicit declaration also allowed).
  column "id"          { type = "UUID" }
  column "timestamp"   { type = "DateTime64(3)" }
  column "value"       { type = "Float64" }
  column "metric_name" { type = "LowCardinality(String)" }
  # ... other CH-auto outer columns ...

  engine "time_series" {
    settings = {
      id_generator                    = "sipHash64(metric_name, all_tags)"
      store_min_time_and_max_time     = "1"
      filter_by_min_time_and_max_time = "1"
    }
    tags_to_columns = {
      instance = "instance"
      job      = "job"
    }

    samples {                              # AKA "data" (alias retained on dump-side for round-trip)
      target = "posthog.prom_metrics_data"
    }

    tags {
      inner {
        column "id"          { type = "UUID" }
        column "metric_name" { type = "LowCardinality(String)" }
        column "tags"        { type = "Map(LowCardinality(String), String)" }
        column "min_time"    { type = "SimpleAggregateFunction(min, Nullable(DateTime64(3)))" }
        column "max_time"    { type = "SimpleAggregateFunction(max, Nullable(DateTime64(3)))" }

        engine "aggregating_merge_tree" {}
        primary_key = ["metric_name"]
        order_by    = ["metric_name", "id"]
      }
    }

    metrics {
      target = "posthog.prom_metrics_metrics"
    }
  }
}
```

Field-by-field on `engine "time_series"`:

| HCL attribute      | Go type                | Mapped to                                                              |
| ------------------ | ---------------------- | ---------------------------------------------------------------------- |
| `settings`         | `map[string]string`    | `SETTINGS k = 'v', …` clause on the CREATE                             |
| `tags_to_columns`  | `map[string]string`    | `SETTINGS tags_to_columns = {'tag1': 'col1', …}` — promoted for clarity |
| `samples` block    | `*TimeSeriesTarget`    | `SAMPLES <ref>` or `SAMPLES INNER COLUMNS (…) [SAMPLES INNER ENGINE = …]` |
| `tags` block       | `*TimeSeriesTarget`    | `TAGS <ref>` or `TAGS INNER COLUMNS (…) [TAGS INNER ENGINE = …]`         |
| `metrics` block    | `*TimeSeriesTarget`    | `METRICS <ref>` or `METRICS INNER COLUMNS (…) [METRICS INNER ENGINE = …]` |

Each `TimeSeriesTarget` has:

| HCL attribute  | Go type                  | Mapped to                                |
| -------------- | ------------------------ | ---------------------------------------- |
| `target`       | `*string`                | external `<db>.<table>` reference        |
| `inner` block  | `*TimeSeriesInnerTable`  | inline `INNER COLUMNS` + `INNER ENGINE`  |

Each `TimeSeriesInnerTable` is a small slice of `TableSpec` covering only the fields CH emits inside an `INNER COLUMNS`/`INNER ENGINE` block:

| HCL attribute  | Go type             | Mapped to                          |
| -------------- | ------------------- | ---------------------------------- |
| `column`       | `[]ColumnSpec`      | inner-target column list           |
| `engine`       | `*EngineSpec`       | inner-target engine (MergeTree-family only) |
| `primary_key`  | `[]string`          | `PRIMARY KEY` on the inner target  |
| `order_by`     | `[]string`          | `ORDER BY`                         |
| `partition_by` | `*string`           | `PARTITION BY`                     |
| `settings`     | `map[string]string` | `SETTINGS` on the inner target     |

### Rules

- **Exactly one** of `target` or `inner` per sub-block. Both → resolve-time error. Neither → resolve-time error.
- All three sub-blocks are optional. `engine "time_series" {}` is the simplest valid form (CH auto-generates inner targets for the missing kinds, matching the docs' "Creation" section).
- The inner-target engine must be one of the MergeTree-family kinds the resolver already knows. Reject `kafka`/`distributed`/etc. with a clear error — CH itself rejects these and we surface it earlier.
- `tags_to_columns` is modelled as a separate top-level attribute rather than nested inside `settings`. Rationale: CH treats it as a *schema-shaping* setting (declaring tags-target columns), distinct from runtime tuning. Promoting it makes the HCL self-documenting; sqlgen folds it back into the `SETTINGS` clause at emit time.
- `DATA` (the documented alias for `SAMPLES`) is **dump-side only**: introspection captures the source keyword; sqlgen round-trips it. HCL authors always write the canonical `samples`.

### Out of scope (rejected during introspection with a clear error)

- Any future TimeSeries syntax extension (e.g., new settings, new clause kinds) not modelled here. Introspection fails with `unknown TimeSeries clause: %s` so the user is forced to upgrade hclexp rather than silently losing the clause.
- Validation that the external target's schema matches CH's required column shape (the docs constrain inner targets' columns, but enforcing that would duplicate CH's own CREATE-time check; we leave it to CH).

## Architecture

Work spans two repositories. The chparser change must land first because the engine implementation here consumes the new AST node.

### chparser fork (`../clickhouse-sql-parser`, repo `orian/clickhouse-sql-parser`)

1. **Lexer.** New keywords: `SAMPLES`, `DATA`, `TAGS`, `METRICS`, `INNER`. Tokenised contextually (engine-tail position only) to avoid colliding with user identifiers.
2. **AST.** New node `TimeSeriesTargetClause`:
   ```go
   type TimeSeriesTargetClause struct {
       Kind        string             // "samples" | "tags" | "metrics"
       Keyword     string             // verbatim source keyword: "SAMPLES" | "DATA" | "TAGS" | "METRICS"
       External    *TableIdentifier   // external form
       InnerCols   *TableSchemaClause // inner form
       InnerEngine *EngineExpr        // inner form
   }
   ```
   `CreateTable` gains `TimeSeriesTargets []*TimeSeriesTargetClause` (nil when engine != TimeSeries).
3. **Parser.** New production for the engine-tail clause list, parsed only when `Engine.Name == "TimeSeries"`. Each clause: `(SAMPLES | DATA | TAGS | METRICS) (table_identifier | INNER COLUMNS '(' column_list ')' [(SAMPLES|DATA|TAGS|METRICS) INNER ENGINE '=' engine_expr])`.
4. **PrintVisitor.** Round-trip rendering of the new clauses.
5. **Tests.** Parser-level fixtures: PostHog's prom_metrics, the doc's "fully generated" inner form, a minimal `ENGINE = TimeSeries`, the `tags_to_columns` form, the `id_generator` form.
6. **Issue + PR:** File issue at `github.com/orian/clickhouse-sql-parser` referencing this spec; PR with the changes above; upstream PR to `AfterShip/clickhouse-sql-parser` as a follow-up (out of scope here).

After the chparser change is merged, bump the replace directive in this repo's `go.mod`.

### chschema (this repo) — files touched

1. **`internal/loader/hcl/types.go`** — no change (TimeSeries is engine-internal; the outer table is a regular `TableSpec`).
2. **`internal/loader/hcl/engines.go`** — new `EngineTimeSeries`, `TimeSeriesTarget`, `TimeSeriesInnerTable` structs; new `DecodeEngine` case; `Virtuals()` returns nil (TimeSeries exposes no engine-virtual columns — its outer columns are real declared columns).
3. **`internal/loader/hcl/resolver.go`** — validate per-target both-or-neither rule; restrict inner engine to MergeTree-family kinds; resolve unqualified target names against the surrounding database.
4. **`internal/loader/hcl/introspect.go`** — when the chparser returns a `CreateTable` whose engine is `TimeSeries`, dispatch to a new `buildTimeSeriesFromCreateTable` that walks the `TimeSeriesTargetClause` list, populates `EngineTimeSeries`, and stamps `KeywordHint` from the source `Keyword`.
5. **`internal/loader/hcl/sqlgen.go`** — new `createTimeSeriesEngineSQL(e EngineTimeSeries) (string, map[string]string)` invoked from `engineSQL`; promotes `TagsToColumns` into the `SETTINGS` map before rendering; emits one tail-clause line per non-nil sub-block in `samples`/`tags`/`metrics` order; chooses `SAMPLES`/`DATA` based on `KeywordHint` (default `SAMPLES`).
6. **`internal/loader/hcl/diff.go`** — new `TimeSeriesDiff` (covering target swaps, inner-target column/engine/settings changes, tags_to_columns changes, and settings changes split into ALTER-able vs recreate buckets); folded into the existing `TableDiff` so the rest of the dispatch logic stays intact.
7. **`internal/loader/hcl/validate.go`** — new dependency kind `DepTimeSeriesTarget = "ts_target"`. `CollectDependencies` walks every table; for each `EngineTimeSeries` with an external-form target, emit a dependency on `db.table`. Inner-form targets create no dependency.
8. **`internal/loader/hcl/sqlgen.go`** (diff path) — ALTER emission: `id_generator` and `filter_by_min_time_and_max_time` changes → `ALTER TABLE … MODIFY SETTING k = v`. Every other change → mark `Recreate = true` + add `UnsafeChange`.
9. **`docs/README.hcl.md`** — new section under "Engine kinds" describing the `time_series` engine; cross-reference to the upstream TimeSeries docs.
10. **`docs/FAQ.md`** — short Q&A: "How do I model a Prometheus TimeSeries table?"

### Tests

- **Unit:** `engines_test.go` (decode), `introspect_test.go` (round-trip from hand-written CREATE strings), `sqlgen_test.go` (emit including keyword hint), `diff_test.go` (ALTER-able vs recreate buckets), `validate_test.go` (`ts_target` dependency), `resolver_test.go` (both-or-neither, inner-engine restriction). Add a top-level round-trip test asserting `parse → build → sqlgen → parse → build` is a fixed point.
- **Live:** extend `test/hcl_introspect_live_test.go` with two TimeSeries variants (external + inner forms). Apply-and-diff smoke test.
- **Fixtures:** three SQL files under `test/testdata/posthog-create-statements/TimeSeries/` (the production CREATE, the doc-default inner form, the minimal form). Picked up automatically by the existing engine-grouped loop.
- **chparser fork:** parser_test entries for each fixture asserting the AST shape.

## Implementation order

1. **chparser fork:** open issue, implement lexer + AST + parser + PrintVisitor + tests, open PR, merge.
2. **chschema:** bump go.mod replace to the new chparser SHA.
3. **chschema:** `EngineTimeSeries` struct + `DecodeEngine` case + `Virtuals()`.
4. **chschema:** resolver rules + tests.
5. **chschema:** introspect path + tests + fixtures.
6. **chschema:** sqlgen + tests.
7. **chschema:** diff path + tests.
8. **chschema:** validate path + tests.
9. **chschema:** live tests.
10. **chschema:** docs.

Each step ends green. Each PR-sized chunk: chparser change (1–2), then chschema-side work as either one feature PR (3–10) or split if review-time is tight.

## Out of scope

- Skip-tables / skip-engines flag on introspect (separate spec).
- TimeSeries dependency *type* validation (CH-style "external samples table needs columns id/timestamp/value"). CH itself rejects mismatches at CREATE.
- `ALTER ... MODIFY SETTING` for `tags_to_columns` and other non-ALTER-able settings. CH itself rejects them — sqlgen marks recreate + unsafe.
- `CREATE TABLE x AS y` semantics for TimeSeries (docs' "Creating a table AS existing table" section). The existing `hclexp` model doesn't model AS-based creation; nothing changes here.

## Future work

- Skip-tables / skip-engines on introspect (separate spec, drafted next).
- `ALTER … MODIFY SETTING id_generator` only matters once HCL adopters need it; reuse the same diff infrastructure if other engines also gain ALTER-able settings later.
- Upstream the chparser PR to `AfterShip/clickhouse-sql-parser` so the fork can eventually shrink.
