# #60: readable long SQL in HCL — heredoc + `file()`, via beautify-canonical queries

## Problem

Introspected view/MV `query` rendered as one very long HCL line (the OPS
`custom_metrics` view is ~1.5 KB on a single line) — hard to read, review, diff.

## The crux: a canonical form

`diff` compares MV/view `query` by **raw string equality**. Round-trip worked
only because introspect emitted `formatNode(parse(create_table_query))`
(compact, via `PrintVisitor`) and the golden stored that exact one-liner. So
heredoc/`file()` are useless without normalization — a nicely-formatted query
would diff as drift on every run.

**Decision (with the user): make the canonical form the parser's _beautified_
output, everywhere.** Both diff sides collapse to the same multi-line form, and
introspected schemas become readable by default — not just when an author opts
into formatting.

## Implementation

1. **`beautifyNode` / `normalizeQuery`** (`query_normalize.go`) — beautify a
   SELECT via the parser's `BeautifyVisitor` (the readable counterpart to
   `formatNode`). `normalizeQuery` wraps a bare query in a throwaway
   `CREATE VIEW … AS` so it parses, then beautifies the SELECT subtree.
   Idempotent. Unparseable → keep raw, `ok=false`.
2. **Load-time normalization** (`parser.go`) — `ParseFile` runs
   `normalizeQueries(db)` over every view/MV. A query the parser can't handle is
   kept verbatim with a `slog.Warn` (never blocks a load) — the raw{} philosophy.
3. **Introspect emits beautified** (`introspect.go`) — `buildView` /
   `buildMaterializedView` switch the query from `formatNode` → `beautifyNode`,
   so the live side matches the loaded side.
4. **`dump` emits heredoc** (`dump.go`) — `setQueryAttribute` writes a multi-line
   query as a heredoc (reusing the existing `setSQLAttribute` raw{} helper), a
   quoted string when single-line. Re-normalized on load, so the round-trip is
   stable.
5. **`file()` function** (`hclfuncs.go`) — `ParseFile` decodes with an
   `EvalContext` exposing `file(path)`, resolving relative to the HCL file's
   directory. Works for `query` and any string attribute. The loaded content is
   normalized like any query, so external `.sql` formatting doesn't drift.

## Why beautify (not compact)

Compact normalization would have been zero-golden-churn (it's what introspect
already emitted), but it leaves readability opt-in. Beautify-canonical makes
**every** introspected schema readable and still satisfies the heredoc/`file()`
asks — at the cost of re-pinning query goldens and teaching `dump` to emit
heredocs. The user chose readability-by-default.

## Verification

- `query_normalize_test.go`: beautify + idempotency; unparseable keeps raw; the
  **anti-drift guarantee** — a one-liner, a heredoc, and a `file()` reference to
  the same query all load to an identical normalized string; `file()` resolves
  relative to the HCL file and errors clearly when missing.
- Updated three inline assertions to the beautified form; `dump_test` round-trip
  now canonicalizes its expected query. Snapshot/integration suite green
  unchanged. `go test ./...`, `vet`, `gofmt` clean.
- CLI smoke: `diff` of a heredoc-formatted query vs the same one-liner →
  `no differences`; `dump` emits a readable multi-line heredoc.

## Scope notes

- Same `file()` mechanism covers long `default`/`materialized`/`alias`
  expressions (any string attr) — only `query` is auto-beautified.

Closes #60.
