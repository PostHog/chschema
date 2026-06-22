# Fix #48: introspect captures a view's inferred column schema as column_aliases

## Root cause (evidence-based)

ClickHouse's `create_table_query` for a plain view **always** carries a fully
typed column schema, e.g. `CREATE VIEW v (team_id Int64, event String) AS SELECT
…`. `buildViewFromCreateView` captured every `TableSchema` column as
`ViewSpec.ColumnAliases`, so an inferred schema was recorded as if it were an
author-specified `CREATE VIEW v (a, b)` list. Re-emitting it changed the view.

Parser probe (real forms):

| Input | `ColumnDef.Type` | Meaning |
|---|---|---|
| `(team_id Int64, event String)` (CH's stored schema) | **set** | inferred — must NOT be captured |
| `(team_id, n)` (author list) | **nil** | genuine alias list — capture |
| `(team_id Int64, …) (team_id, …)` (what re-emitting produces) | — | **the parser can't even parse this** |

So the typed schema is always inferred metadata, and the only legitimate alias
list is typeless.

## Fix

`buildViewFromCreateView` now skips `ColumnDef`s that carry a type (`cd.Type !=
nil`) and captures only typeless entries. A view's columns are then defined by
its SELECT body (which ClickHouse re-infers on CREATE), so the round-trip is
faithful. The HCL `column_aliases` field and the #41 star-guard in `createViewSQL`
are unchanged — they still serve HCL-authored views.

## Tests

- `TestProcessIntrospectRows_ViewInferredSchemaNotCapturedAsAliases` — the
  issue's golden view introspects with empty `column_aliases` and regenerates
  `CREATE VIEW … AS SELECT team_id, event …` (no list). Failed before the fix.
- `TestProcessIntrospectRows_ViewWithColumnAliasesAndComment` (existing,
  typeless list) still passes — author lists are preserved.
- The plain view is added back to `test/testdata/roundtrip/schema.sql`; the live
  `TestLive_RoundTripFidelity` passes against ClickHouse 26.3 (verified locally).

Closes #48. Dictionary round-trip (#49) remains open.
