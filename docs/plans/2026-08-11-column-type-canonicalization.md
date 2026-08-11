# Column-type canonicalization — the #136 bug class, for types

Status: **implemented & verified** (2026-08-11). Extends
`2026-07-11-issue-136-introspect-roundtrip.md` (items 1–3) with the
column-*type* case, and closes the same gap on every other object kind that
carries a type.

## Context

`diff` compares types as plain strings. ClickHouse stores an Enum in
`create_table_query` **with spaces around `=`** — `Enum8('a' = 1, 'b' = 2)` —
while `formatNode`, the printer every introspected type is rendered through,
emits the compact `Enum8('a'=1, 'b'=2)`. An authored type was stored verbatim,
so a raw compare against its introspected counterpart never matched and the
diff emitted a no-op change on every apply, forever.

The cost differed by object kind, which is why it was worth fixing everywhere
rather than only where it was first noticed:

| kind | consequence of the mismatch |
|---|---|
| table column | perpetual no-op `MODIFY COLUMN` |
| materialized view column | `Recreate` → **permanent unsafe entry**, no statement emitted, never converges |
| dictionary attribute | `CREATE OR REPLACE DICTIONARY` on every apply; blocked outright when the source holds a `[HIDDEN]` credential (#179) |

## Root cause (same one bug class as #136)

`canonicalize(db)` — run at the tail of both the load path and the introspect
path — is the single place where both sides are reduced to identical text. It
covered queries, column expressions, index expressions, and table TTLs, but
**not types**, and its object coverage was table-shaped: `db.Tables`, plus the
patch collections. Materialized-view column lists, dictionary attributes, and a
`time_series` engine's inner column list were never walked at all, even though
introspection renders each of them through `formatNode`.

## Fix

`internal/loader/hcl/query_normalize.go`:

- `normalizeType` — parses the string as the column type of a throwaway
  `CREATE TABLE` (the same node path introspect uses) and renders it with
  `formatNode`, so authored and introspected forms reduce to the same text.
- `canonicalize` now walks every type-bearing field: declared MV column lists,
  dictionary attributes (type, `DEFAULT`, `EXPRESSION`), and TimeSeries inner
  columns, alongside the table and patch collections it already covered.
- `normalizeDictionaryAttrs` / `normalizeEngineInnerColumns` — the two new
  walkers.

`internal/loader/hcl/parser.go`: `canonicalize(db)` moved to the end of the
per-database loop. A TimeSeries inner column list is only reachable through
`EngineSpec.Decoded`, so the engine blocks must be decoded first; nothing
earlier in the loop depends on canonical text.

### Silent truncation — the guard

Each normalizer parses its fragment inside a throwaway statement, and SQL does
not stop at the fragment. `String DEFAULT 'x'` in a column-type position parses
as a type *plus* a DEFAULT clause; a stray `)` (`String) ENGINE = Log --`) lets
the input rewrite the rest of the statement outright. Extracting just the
fragment silently discarded the remainder — turning a malformed declaration
into a plausible-looking canonical value and erasing the DEFAULT from every
statement generated from it.

`normalizeFragment(tmpl, s, render)` closes that off for all three fragment
normalizers (`normalizeType`, `normalizeTTL`, `normalizeExpr`): it substitutes
the parsed fragment's own rendering back into the template and requires the two
statements to render identically. Anything the input contributed beyond the
fragment shows up in that comparison and is rejected with `ok=false`, which
keeps the raw text — the difference stays visible as drift instead of being
silently dropped.

The check needs a *structure-preserving* rendering of the fragment, which is
not always the canonical one: `formatTTLItems` folds `INTERVAL 7 DAY` into
`toIntervalDay(7)`, and the expression normalizer strips redundant parens. So
`fragment` carries both — `verbatim` for the guard, `canonical` for the caller.

A rejected column type is also logged (`warnRawType`), matching the existing
warning for an unparseable view query: loading still succeeds, but the type is
now the one field on the object comparing as raw text, and an unexplained
perpetual `MODIFY COLUMN` is worse than a warning.

## Tests

- `query_normalize_test.go` — Enum canonicalization (authored ≡ introspected,
  idempotent, nested); non-Enum and unparseable types unchanged; and the
  rejection sets for all three normalizers (type / expression / TTL), including
  that TTL interval folding and expression paren stripping still work.
- `roundtrip_enum_type_test.go` — the load-vs-introspect convergence tests:
  one authored Enum in a table column, an MV column list, and a dictionary
  attribute must diff clean against the DDL ClickHouse returns for them, with
  no unsafe entry; plus per-kind assertions on the parsed schema (including
  `patch_column`, `patch_table`, `patch_materialized_view`, and TimeSeries
  inner columns).

Counterfactual: reverting `query_normalize.go` + `parser.go` fails all three
round-trip tests.

## Verification

`go test ./internal/... ./cmd/... ./test/...` green (1019 tests); `gofmt` and
`go vet` clean. `normalizeType` costs ~2µs per column (two throwaway parses),
so canonicalizing every column on both paths is not a measurable load cost.
