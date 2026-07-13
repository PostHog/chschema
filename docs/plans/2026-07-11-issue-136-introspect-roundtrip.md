# Issue #136 — introspect(apply(layer)) round-trips clean against load(layer)

Status: **implemented & verified** (2026-07-11). Scope: correctness items 1–3.
Item 4 (Distributed system-proxy column subset tolerance) deferred as a design
decision.

## Context

After posthog bumped the pinned `hclexp` image, the multinode live-convergence
CI gate failed: `hclexp diff -left <load(layer)> -right <introspect(apply)>`
reported operations for objects that did not change. The golden is proven
correct (diffs clean against itself); the spurious ops appear only against an
**introspected** live dump, so `compose(layer)` and `introspect(apply(layer))`
disagreed for four object kinds. The maturing introspection captured more
fields faithfully, and each newly-captured field exposed a canonicalization
asymmetry with the compose side.

## Root cause (one bug class)

`diff` compares expressions as **plain strings** (`columnsEqual`/`eqStrPtr`,
`reflect.DeepEqual` for indexes). Two asymmetries broke that:

1. **Coverage.** `normalizeQueries` ran on the load path only, and only over
   view/MV/projection *queries*. Authored column `default`/`materialized`/
   `alias` and index `expr`/`type` were stored verbatim; introspect rendered
   the same fields via `formatNode`. Verbatim-vs-`formatNode` → spurious
   MODIFY COLUMN (item 2) and index DROP+ADD + manual MATERIALIZE INDEX
   (item 3).
2. **Not paren-canonical.** View queries normalize through `beautifyNode` on
   both sides, but the beautifier preserved input parentheses. ClickHouse 26.3's
   `SHOW CREATE` wraps a `HAVING` conjunction in a redundant outer pair
   (`HAVING ((a) AND (b))`) the authored HCL lacks → item 1.

## Fix (hclexp only — no parser-fork change)

A parenthesised scalar `(x)` parses to a single-item `ParamExprList`, and the
parser wraps clause values / list items in alias-less `ColumnExpr`. Both are
transparent at an **expression-root position**, so the outermost pair is always
redundant regardless of inner precedence — a precedence-free, provably safe
strip. Inner parens are preserved (they'd need precedence analysis).

`internal/loader/hcl/query_normalize.go`:
- `unwrapRootParens` — peel alias-less `ColumnExpr` + single-item `ParamExprList`.
- `stripClauseParens` + `clauseParenStripper` — canonicalize WHERE/PREWHERE/
  HAVING of every SELECT (incl. nested CTE/subquery) inside `beautifyNode`,
  preserving the wrapper node so non-paren clauses stay byte-identical.
- `normalizeExpr` — scalar-expr sibling of `normalizeQuery` (parse `SELECT <e>`,
  `unwrapRootParens`, `formatNode`).
- `canonicalize(db)` — runs `normalizeQueries` + `normalizeExpr` over column
  default/materialized/alias/ephemeral and index expr/type. Called on **both**
  paths: `parser.go` (load) and `processIntrospectRowsOpt` (introspect).

No `diff.go` change; no `go.mod` bump.

## Tests

- `query_normalize_test.go` — paren/expr canonicalization, idempotence, keeps
  tuples and precedence-groups.
- `roundtrip_paren_test.go` — load-vs-introspect diff convergence for all three
  items via the `fakeRows` seam (fails without the fix).
- `test/issue136_roundtrip_live_test.go` — live acceptance (apply → introspect →
  diff clean).

## ClickHouse-version nuance

The local dev ClickHouse strips redundant parens itself, so it cannot reproduce
the divergence — the affected cluster is CH 26.3. The **unit tests** feed the
parser the exact CH-26.3 string and are the version-independent guard; the live
test asserts the general round-trip-clean property (passes on any version).

## Verification

`go test ./internal/... ./test` and `go test ./test -clickhouse` green;
gofmt/vet clean. Counterfactual: disabling the fix makes the unit round-trip
tests fail (spurious MODIFY COLUMN / DROP+ADD INDEX / MODIFY QUERY).
