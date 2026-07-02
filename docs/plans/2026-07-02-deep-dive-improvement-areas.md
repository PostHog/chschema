# Deep dive: improvement areas & gaps (2026-07-02)

A full audit of the codebase across three axes: core engine
(`internal/loader/hcl/`), tests/CI/tooling, and docs/CLI/product surface.
Each finding links to a tracking issue. Findings were verified against the
source before filing; one initially-suspected bug (unchecked `pem.Decode` in
`github_token.go`) turned out to be already handled and is not listed.

Two findings were withdrawn after maintainer clarification of operational
policy (see "By design" below).

## Operational policy (clarified during this audit, previously undocumented)

- **DDL is executed per-node, never with `ON CLUSTER`.** Each statement is run
  on each node of a cluster individually; `ON CLUSTER` caused too many
  operational problems. Note `docs/concept.md:20` still documents the old
  approach, and `createTableSQL` still emits `ON CLUSTER` when a table has
  `cluster` set (`sqlgen.go:627-630`) while ALTERs never do — tracked in
  [#93](https://github.com/PostHog/chschema/issues/93).
- **`MATERIALIZE INDEX` is never emitted automatically.** It is a heavy,
  unpredictable mutation and must be run by an operator.

### By design (filed, then closed)

- ~~ALTER TABLE emits no `ON CLUSTER`~~ — intentional (per-node execution).
  Closed as [#81](https://github.com/PostHog/chschema/issues/81).
- ~~ADD INDEX without MATERIALIZE INDEX~~ — intentional (operator-run only).
  Closed as [#84](https://github.com/PostHog/chschema/issues/84).

## A. Correctness (label: bug)

| # | Finding | Evidence | Issue |
|---|---------|----------|-------|
| A1 | Constraints, `primary_key`, table `comment`, and table `cluster` are never diffed. `TableDiff` covers columns/indexes/engine/order/partition/sample/ttl/settings only; drift in the rest is invisible and produces no DDL. | `diff.go:106-128` | [#82](https://github.com/PostHog/chschema/issues/82) |
| A2 | Constraint `ASSUME` round-trips as `CHECK` — the kind is lost during introspection (upstream parser doesn't preserve it). Needs an upstream chparser issue with a minimal repro. | `introspect.go:511-519` | [#83](https://github.com/PostHog/chschema/issues/83) |
| A3 | Swallowed parse errors degrade malformed input to zero values: index GRANULARITY `Sscanf` (`introspect.go:505`), inner-engine `engineSQL` (`sqlgen.go:1046`), dictionary `ParseInt` (`dictionary_introspect.go:203`). | as listed | [#91](https://github.com/PostHog/chschema/issues/91) |

## B. Robustness / engine gaps (label: enhancement)

| # | Finding | Evidence | Issue |
|---|---------|----------|-------|
| B1 | One unparseable object aborts a whole live `diff`: the live side hardcodes `allowRaw=false` and `diff` has no `-exclude` flag, so the exclude feature only helps `introspect`/`dump-cluster`. | `cmd/hclexp/hclexp.go:556` | [#85](https://github.com/PostHog/chschema/issues/85) |
| B2 | Unsupported engines abort introspection: S3, URL, File, MySQL, PostgreSQL, MongoDB, RabbitMQ, NATS, EmbeddedRocksDB, Dictionary-engine tables, VersionedCollapsingMergeTree, GraphiteMergeTree (+ Replicated variants). | `introspect.go:529-689` | [#86](https://github.com/PostHog/chschema/issues/86) |
| B3 | `PROJECTION`s are not modeled — tables with projections fail to round-trip or drop them. | `types.go` | [#87](https://github.com/PostHog/chschema/issues/87) |
| B4 | No column positioning: `ADD COLUMN` never emits `FIRST`/`AFTER`; new columns always land last regardless of declared position. | `sqlgen.go:724-725` | [#88](https://github.com/PostHog/chschema/issues/88) |
| B5 | No `MODIFY ORDER BY` (extend-only) path — every ORDER BY change is classed unsafe/recreate even when ClickHouse supports it in place. | `diff.go:263-266` | [#89](https://github.com/PostHog/chschema/issues/89) |
| B6 | Unsafe changes are reported in the Unsafe list but there is no opt-in way to emit the recreate DDL — the MV DROP+CREATE is never generated. | `sqlgen.go:128-134`, `sqlgen.go:1101-1144` | [#90](https://github.com/PostHog/chschema/issues/90) |
| B7 | Everything is single-threaded: `dump-cluster` walks nodes sequentially with a fresh connection each; databases are introspected serially. | `cmd/hclexp/hclexp.go:392-400` | [#92](https://github.com/PostHog/chschema/issues/92) |

## C. Product / CLI (labels: documentation, enhancement)

| # | Finding | Evidence | Issue |
|---|---------|----------|-------|
| C1 | The per-node execution model is undocumented and `docs/concept.md` contradicts it (`ON CLUSTER` line, `chschema --dry-run`/`--auto-approve` workflow that doesn't exist). No documented path from `diff -sql`/`plan -format json` to actual execution. CREATE/ALTER `ON CLUSTER` emission is inconsistent. | `docs/concept.md:20,36-41`, `sqlgen.go:627-630` | [#93](https://github.com/PostHog/chschema/issues/93) |
| C2 | No destructive-op guard: removing an object from HCL emits a runnable `DROP TABLE`; no `prevent_destroy`/protect attribute, no `-forbid-drops` mode for CI. | `sqlgen.go:168-174` | [#94](https://github.com/PostHog/chschema/issues/94) |
| C3 | No `version` subcommand, no build stamping, no CHANGELOG, zero git tags — though `publish.yml` is already wired for semver tags. | `.github/workflows/publish.yml:41,69` | [#95](https://github.com/PostHog/chschema/issues/95) |
| C4 | CLI inconsistencies: URI vs `-host` flags split between `diff` and `introspect`/`dump-*`; `-out` vs `-out-dir`; `-left` semantics differ between `diff` and `sql2hcl`; exit codes 1 vs 2 for the same error class; bare `-h` output. | `cmd/hclexp/*.go` | [#96](https://github.com/PostHog/chschema/issues/96) |

## D. Documentation (label: documentation)

| # | Finding | Evidence | Issue |
|---|---------|----------|-------|
| D1 | `plan`, `web`, `sql2hcl`, `dump-sql`, `dump-cluster` have no README sections; `dump-sql` absent from `README.hcl.md`/FAQ; `examples/manifest/` orphaned; CLAUDE.md:137 view/dictionary contradiction and stale YAML→Apply line; operational policies (per-node execution, MATERIALIZE INDEX) undocumented. | `README.md`, `docs/FAQ.md`, `CLAUDE.md` | [#97](https://github.com/PostHog/chschema/issues/97) |

## E. Tests / CI / hygiene

| # | Finding | Evidence | Issue |
|---|---------|----------|-------|
| E1 | No true multi-node test topology: docker-compose is one node; the three configured "clusters" all point at the same host. Per-node execution, cross-node drift, and `plan` cross-role ordering are never live-tested. | `docker-compose.yml`, `docker/clickhouse/config.d/default.xml` | [#98](https://github.com/PostHog/chschema/issues/98) |
| E2 | Test gaps: `cmd/hclexp/flows.go` untested; `macros_introspect.go` live-only; `plan.go`/`render_json.go` one test each; dictionary and named-collection files without direct unit tests; coverage measured but not gated in CI. | as listed | [#99](https://github.com/PostHog/chschema/issues/99) |
| E3 | Dev hygiene: `go.mod` (1.25) vs CI (1.26) Go version skew; justfile lacks `lint`/`fmt`/`vet`/`build`/`coverage`; root scratch files not gitignored; stale `test.sh`; no `.editorconfig`. | `go.mod`, `.github/workflows/ci.yml`, `justfile`, `.gitignore` | [#100](https://github.com/PostHog/chschema/issues/100) |

## Priority

| Tier | Issues | Why |
|------|--------|-----|
| P0 | #82 (missing diffs), #91 (swallowed errors) | Invisible drift / silently wrong specs |
| P1 | #85 (diff exclude/allow-raw), #93 (execution-model docs + ON CLUSTER consistency), #98 (multi-node tests) | Daily-use failure modes and the audit's own blind spot |
| P2 | #94 (destroy guard), #95 (version/release), #90 (opt-in recreate DDL) | Operational safety and traceability |
| P3 | #83, #86–#89, #92, #96, #97, #99, #100 | Incremental breadth, UX, hygiene |
