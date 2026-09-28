# Issue #250: a second SETTINGS clause drops the engine settings; move to ClickHouse 26.8

## Verified against live ClickHouse (26.3.12.3 and 26.8.11.7)

- `CREATE TABLE … ENGINE = Kafka SETTINGS kafka_… SETTINGS flatten_nested = 0` is
  valid. `formatQuery` keeps two separate clauses, and a third `SETTINGS` is a
  syntax error.
- The trailing clause is query-level. It is **not** stored in
  `create_table_query`, but it does take effect: with `flatten_nested = 0`, a
  `Nested(...)` column is kept rather than flattened to `n.a Array(...)`.
  Introspection never sees the second clause. Only `sql2hcl` input does.
- A single trailing `SETTINGS max_threads = 4` on MergeTree is accepted, but it
  is not stored as a storage setting.

## Root cause: parser (upstream) — fixed in clickhouse-sql-parser v1.1.x

`parseEngineExpr` assigned each engine clause without checking for an earlier
one, so a second `SETTINGS` overwrote the first (orian/clickhouse-sql-parser#35).
v1.1.0 keeps the trailing query-level clause separately in
`CreateTable.Settings` (#37) and rejects repeated engine clauses.

## Parser upgrade: v1.0.2 → v1.1.1

- v1.1.0 requires Go 1.27; v1.1.1 declares `go 1.27` (not `1.27.0`), so
  chschema's `go.mod` is `go 1.27` and the Dockerfile builder is
  `golang:1.27-alpine`.
- Compiled cleanly: chschema implements no `ASTVisitor` directly, and
  `TableIndex.Granularity` was already nil-checked. No `Pos()`/`End()` slicing.
- Behavioural consequences handled here:
  - Kafka double `SETTINGS` (#250) now parses correctly. The interim
    `errKafkaNoConfig` guard (and its raw-block exception in `sql2hcl`) is
    removed; the resolver still rejects a Kafka engine with no config. The test
    is now a round trip: all four `kafka_*` keys survive, the query-level
    `flatten_nested` is not part of the schema.
  - TimeSeries `RECENT SAMPLES` parses (#42), see below.
  - An engine-only target (`SAMPLES ENGINE = …`, no `INNER COLUMNS`) is now an
    inner table without columns instead of an error.

## Move the repo to ClickHouse 26.8 LTS (26.8.11.7)

`docker-compose.yml` server and keeper go from 26.3.12.3 to 26.8.11.7. Live
suite regressions on 26.8:

- **Single-column keys keep their parentheses.** 26.8 stores `ORDER BY (id)`
  verbatim, where 26.3 normalised it to `ORDER BY id`. hclexp introspects both
  to the same HCL (no drift), but it always emitted `(x)`, so the round-trip
  fidelity tests' byte comparison broke. Fix: emit a single-element
  `ORDER BY`/`PRIMARY KEY` bare.
- **TimeSeries tags table** must now have `min_time`/`max_time` columns, and
  AggregatingMergeTree rejects the non-key `tags` column unless
  `allow_dimensions_outside_sorting_key = 1` is set (the default inner tags
  table sets it). The external-targets live fixture is updated.
- **`DATA` keyword** is stored as `SAMPLES` by 26.8; the live test accepts both.
- **TimeSeries `RECENT SAMPLES` target** (new in 26.8, on by default via
  `recent_samples_ttl_seconds = 345600`):
  - model: `EngineTimeSeries.RecentSamples` (HCL `recent_samples {}`), and all
    target loops go through `EngineTimeSeries.targets()`;
  - inner tables gained `ttl`, and introspection now keeps inner `SETTINGS` and
    full inner columns (codec/default) — before, it kept name/type only and
    dropped the settings; sqlgen emits inner `TTL`/`SETTINGS`. Without the TTL
    a recreated recent-samples table would never expire its data;
  - inner `ttl` is canonicalized on both paths (like a table TTL);
  - sqlgen puts a TimeSeries table's `SETTINGS` *before* the target clauses. A
    trailing clause is query-level, and ClickHouse rejects
    `recent_samples_ttl_seconds` there;
  - the TimeSeries target diff used `reflect.DeepEqual`, which included the
    inner engine's HCL `Body`, so any loaded `inner {}` target always differed
    from its introspected twin. Targets now compare without the body.

## Status

- Done: parser v1.1.1 + Go 1.27, the #250 round trip, the 26.8 compose bump,
  bare single-column keys, the TimeSeries fixture, `recent_samples`, inner
  TTL/settings/full columns. Unit (`-race`) and live suites pass on 26.8.

## Follow-ups (not in this change)

- **Bare TimeSeries vs live never round-trips.** `engine "time_series" {}` diffed
  against the live table shows ClickHouse's synthesized outer columns, the four
  `.inner_id.*` tables and, on 26.8, the four fully spelled-out default targets
  (the 26.3 shorthand regex in `stripDefaultTimeSeriesTargetShorthand` does not
  match the 26.8 form). Needs a design: either nil-vs-inner tolerance in the
  diff or modelling the defaults.
- `sql2hcl` drops a query-level `SETTINGS flatten_nested = 0`; a `Nested(...)`
  column recreated without it is flattened by ClickHouse.
- TimeSeries inner `ORDER BY tuple(a, b)` is not flattened to `["a", "b"]`
  (pre-existing, cosmetic).
