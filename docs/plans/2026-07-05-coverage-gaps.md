# Coverage gaps (#99) — targeted unit tests + CI floor

Status: design approved 2026-07-05; execution via parallel subagents.
Issue: https://github.com/PostHog/chschema/issues/99

## Reality check (measured 2026-07-05, total 73.6%)

The issue (filed 2026-07-02) is partially stale — flows.go (88%),
hclfuncs.go (100%), dictionary_sqlgen.go (91%), named_collection_dump.go
(87%) got covered by the #105–#113 work. Remaining, measured:

- `internal/loader/hcl/macros_introspect.go` — **0%**, all 3 functions.
- Dictionary variant builders: `writeDictionarySource` 31%,
  `writeDictionaryLayout` 36%, `writeOptBool` 0%,
  `buildDictionaryLayoutFromAST` 41%, `optInt64`/`optBool` low, every
  layout/source `Kind()` 0%.
- `cmd/hclexp/plan.go` `renderPlanText` — 0% (pure rendering).
- `cmd/hclexp/web.go` builders: `viewProps`/`dictProps`/`find*`/
  `dictAttributesSection` 0%, `indexesSection`/`constraintsSection` 22%,
  `formatValue` 31%, `kindLabel` 43%.
- Introspect stragglers: `applyKafkaSetting` 38%, `stringSliceEqual` 33%,
  `parseBoolPtr` 50%, `setSQLAttribute`/`setQueryAttribute` 50%,
  `EngineDistributed.Virtuals` 0%.
- CI (`ci.yml` "Check test coverage" step) prints total, gates nothing.

**Excluded, deliberately:** the 0% CLI shells (`main`, `runDiff`,
`runIntrospect`, `runDumpCluster`, `runDrift`, `runLoad`, `runValidate`,
`runPlan`, `runSQL2HCL`, `runWeb`, `runWebManifest`, `runGitHubToken`,
`runDumpSQL`) — flag parsing + live connections + os.Exit. Their logic
lives in tested helpers; the shells are integration surface (live suite
today, #98 multi-node topology later). Not unit-test material.

## Design

Five disjoint test workstreams (parallel subagents, one per stream) +
the CI gate (inline):

1. **macros** — unit tests for macros_introspect.go via canned rows
   (no live server), mirroring the fakeRows pattern.
2. **dictionary round-trips** — canned `CREATE DICTIONARY` DDL for each
   layout/source variant → introspect AST builders → dump → assertions;
   lifts introspect + dump + `Kind()`s together.
3. **plan rendering** — `renderPlanText` from a canned plan structure;
   pin the text layout.
4. **web builders** — drive `buildHTMLView` (or the narrowest exported
   path) over a schema containing views, dictionaries, raws, indexes,
   constraints; assert section HTML fragments.
5. **stragglers** — table tests for `applyKafkaSetting` variants,
   `parseBoolPtr`, `stringSliceEqual`, `setQueryAttribute` (single-line
   vs heredoc branch), `EngineDistributed.Virtuals`.
6. **CI floor** — ci.yml coverage step fails under **73%**; ratchet
   upward in future PRs as coverage grows.

House rules for every stream: testify (`assert`/`require`),
whole-struct asserts over field-by-field, no comments that restate
code, `gofmt -s` clean, table-driven where natural. New files only
(`*_coverage_test.go` naming avoided — use descriptive names like
`macros_introspect_test.go`); do not modify production code — if a
function is untestable without changes, report back instead of
refactoring.

Verification: per-stream `go test ./<pkg> -run <NewTests> -v`; then
full `go test ./cmd/... ./internal/...`, coverage re-measure (expect
comfortably >73%), `go test ./test`, gofmt/vet, single PR.

Also: comment on #99 with the fresh numbers (stale claims) when the PR
is up; PR closes #99.
