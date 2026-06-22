# Fix: HCL dumper drops column modifiers and table fields (issue #45)

## Context

`hclexp introspect`/`dump` emitted every column with only its `type`, silently
dropping `ALIAS`, `MATERIALIZED`, `DEFAULT`, `EPHEMERAL`, `CODEC`, per-column
`TTL`, and `COMMENT` — even though the parser populates them. This hides drift
and, if a dump is treated as desired state, would recreate `ALIAS`/`MATERIALIZED`
columns as plain stored columns. The loss is in the dumper, not the parser.

## Audit ("verify similar issues are not present")

A field-by-field audit of every dumper emitter against its spec type (and the
empirical proof from an extended round-trip fixture) found the bug is **not
limited to columns**. `writeTable`/`writeMaterializedView`/the TimeSeries inner
emitter wrote only `type`, and additionally:

| Emitter | Dropped fields |
|---|---|
| column (table / MV / inner) | nullable, default, materialized, ephemeral, alias, codec, ttl, comment |
| `writeTable` | **primary_key, comment, cluster, constraints** |
| `writeDatabase` | database-level **cluster** |

`writeView`, `writeDictionary` (incl. attributes/source/layout), `writeEngine`,
`writeNamedCollection`, and `writeNode` were **complete** — no drops. The SQL
generator (`createTableSQL`/`columnDefSQL`) was also complete; the bug was
dumper-only.

The existing `roundTrip` test (parse→Write→reparse→deep-equal) is the right
property test but passed only because `dump_round_trip_full.hcl` lacked all these
fields.

## Fix (`internal/loader/hcl/dump.go`)

- New shared `writeColumn` helper emits the full `ColumnSpec`; used by table, MV,
  and TimeSeries-inner column loops.
- `writeTable` now emits `comment`, `cluster`, `primary_key`, and `constraint`
  blocks. `writeDatabase` emits the database `cluster`.

## Tests

- `dump_round_trip_full.hcl` extended to exercise every modifier + primary_key +
  comment + cluster + constraints (the regression guard; failed before the fix).
- `TestWrite_RoundTrip_IntrospectedColumnModifiers` — starts from the issue's
  CREATE TABLE, runs introspect-parse → dump → reparse, asserts equality
  (CI-runnable; closes the introspect→dump seam).
- `TestLive_CreateTableRoundTrip_ColumnModifiers` (gated) — the requested e2e:
  create a table with all modifiers, introspect → dump → reparse → GenerateSQL,
  recreate in place, and assert the stored `CREATE TABLE` is byte-identical.

## Note: column-modifier order is `DEFAULT … COMMENT … CODEC … TTL`

ClickHouse's column grammar fixes the modifier order as
`[DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS] [COMMENT] [CODEC] [TTL]`; `COMMENT`
after `TTL` is a syntax error in ClickHouse itself (verified in CI: `code 62`).
The chparser enforces the same order, and the SQL generator (`columnDefSQL`)
emits it correctly — no parser issue. An earlier draft of the live e2e test
hand-wrote the original `CREATE TABLE` with `COMMENT` after `TTL`; that was a
test bug, fixed to the valid order.
