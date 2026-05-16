# Live introspection: re-enable MaterializedView, View, Dictionary subtests

## Status

Skipped in CI as of 2026-05-16. The `test-live` job in
`.github/workflows/ci.yml` runs `go test -v -clickhouse` with
`-skip 'TestLive_Introspection_AllStatements/(MaterializedView|View|Dictionary)'`.
Other live tests run normally and pass.

## Symptom

Against a freshly-started ClickHouse stack
(`docker compose up -d --wait`), the affected subtests of
`TestLive_Introspection_AllStatements` (in
`test/introspection_live_test.go`) fail in two shapes:

- For some objects the CREATE statement errors out:
  `code: 57, message: Table default.<name> already exists` — even on a
  clean DB. This is the surface error reported by the SQL fixture loader.
- For the rest the CREATE succeeds but the introspector returns nil when
  queried back: `Expected value not to be nil. Object '<name>' should be
  found after introspection`.

Every failing subtest is an object type that **references other
objects**:

- `MaterializedView/*` — every MV in `test/testdata/posthog-create-statements/MaterializedView/`
  references a source table named in its `SELECT ... FROM <source>`. The
  test fixture doesn't create those sources first, so the MV either
  fails to materialize or introspection can't resolve it back through
  the source.
- `View/*` — same shape (views also reference base tables).
- `Dictionary/*` — dictionaries pull from a `SOURCE(...)`, often another
  table. Same missing-prerequisite story.

The "table already exists" variant likely comes from a previous
sub-test in the same group having half-created an MV against a
not-yet-created destination, leaving the destination orphan. The next
subtest tries to create the orphan again and collides.

## Root cause (working theory)

`TestLive_Introspection_AllStatements` walks
`test/testdata/posthog-create-statements/` and runs each `.sql` in
isolation against a per-test database. For self-contained engines
(MergeTree, Replicated*, Kafka, Distributed) the CREATE is independent
and the introspector round-trips it cleanly. For
`MaterializedView`/`View`/`Dictionary`, the CREATE has implicit
dependencies on tables that live in *other* fixture files and aren't
materialized as part of this test's setup.

## Fix sketch

The test harness needs to set up the dependency graph before running
each MV/View/Dictionary subtest. Options, roughly in order of effort:

1. **Pre-create dependency tables in a setup step.** Inspect each
   failing fixture, list the source tables it needs, add a `BeforeAll`
   that creates minimal stub versions of them in the per-test database.
   Brittle (manual mapping) but unblocks immediately.
2. **Group fixtures by dependency.** Run all `Tables/*` fixtures first
   to populate the test DB, then run dependents. Requires the walker to
   topologically order groups instead of running them in
   `filepath.Walk` order.
3. **Parse the `SELECT`/`SOURCE` of each fixture, auto-create stubs.**
   The HCL package already has a SELECT-source-table extractor for MV
   validation (`internal/loader/hcl/validate.go`). Reuse it to derive
   the required-tables set, then synthesize minimal-shape stubs at the
   start of each subtest.
4. **Use a single fixture corpus that's internally consistent.** Drop
   the per-file fixture model in favour of a curated multi-statement
   `.sql` file that the test runs end-to-end.

Option 3 is the cleanest end state; option 1 is the cheapest interim.

## Acceptance criteria

- Remove the `-skip` flag from the `test-live` step in
  `.github/workflows/ci.yml`.
- `go test -v -clickhouse ./test/... ./internal/loader/hcl/...` passes
  with no subtests skipped against a fresh `docker compose up -d --wait`.
- No flakes when re-run consecutively (cleanup between subtests works).

## References

- Failing test bodies: `test/introspection_live_test.go:324–419`.
- Fixture corpus: `test/testdata/posthog-create-statements/{MaterializedView,View,Dictionary}/`.
- Existing dependency parser: `internal/loader/hcl/validate.go`.
