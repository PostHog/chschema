# Fix #49: Diff drops dictionaries (and would skip dropping them) for a whole-database add/drop

## Root cause (traced live)

The round-trip fidelity test recreates a schema via `Diff(&Schema{}, introspected)`
— the database does not exist on the `from` side. Stage-by-stage tracing showed:

- introspect → `DatabaseSpec.Dictionaries` = 1 ✓
- dump → reparse → resolve → `Dictionaries` = 1 ✓
- **`Diff` → `AddDictionaries` = 0 ✗**

`Diff` (`internal/loader/hcl/diff.go`) has a shortcut for a database present on
only one side. The new-database case copied `Tables`, `MaterializedViews`,
`Views`, and `Raws` into the change set but **omitted `Dictionaries`**; the
dropped-database case symmetrically omitted `DropDictionaries`. So a brand-new
database's dictionaries were never created (and a fully-removed database's
dictionaries never dropped). Databases present on *both* sides were fine —
`diffDatabase` handles dictionaries — which is why `diff -left <empty-but-declared-db>`
worked while `Diff(empty-schema, …)` did not.

## Fix

Add `AddDictionaries` to the new-database branch and `DropDictionaries` to the
dropped-database branch, mirroring the existing table/view/MV/raw handling.

## A note on dictionary source order (not a regression)

ClickHouse echoes the written order of `SOURCE(CLICKHOUSE(...))` parameters
rather than canonicalizing it (verified: `TABLE 'x' DB 'y'` and `DB 'y' TABLE 'x'`
are both stored verbatim). hclexp's typed model is order-agnostic and emits the
conventional `DB … TABLE …` order. So a dictionary whose source was written in a
different order won't be byte-identical after a round-trip — a cosmetic, inherent
limitation, not a data change. The fixture uses the conventional order.

## Tests

- `TestDiff_NewDatabase_IncludesDictionaries` / `TestDiff_DroppedDatabase_DropsDictionaries`
  (CI, no ClickHouse) — failed before the fix.
- The dictionary is restored to `test/testdata/roundtrip/schema.sql`;
  `TestLive_RoundTripFidelity` passes for all six objects against ClickHouse 26.3
  (verified locally).

Closes #49.
