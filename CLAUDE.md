# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A declarative Infrastructure-as-Code (IaC) tool for managing ClickHouse
schemas in Go. The active code path is **HCL-first**: schemas are written
in HCL, layered for multi-environment setups, resolved into a flat
desired state, and round-tripped against a live cluster via the `hclexp`
binary. Terraform/Kubernetes-style state reconciliation; no sequential
migrations.

Execution model: generated DDL is run on each node of a cluster
individually — never with `ON CLUSTER` (too fragile in operation). Heavy
mutations (`MATERIALIZE INDEX`) are generated but marked manual and are
only ever run deliberately by an operator.

The single binary is `hclexp`. (A legacy YAML/protobuf `chschema` tool was
removed; the Go module is still named `github.com/posthog/chschema` for
import stability — do not rename it.)

## Architecture

The `hclexp` binary is composed of these packages:

1. **HCL Loader & Resolver** (`internal/loader/hcl/`) — parses HCL files
   and layer stacks, applies `patch_table` and `extend` inheritance,
   drops abstracts, cascades the database-level `cluster` default, and
   decodes typed `engine` blocks.
2. **Introspection Engine** (`internal/loader/hcl/introspect.go`) —
   connects to a live ClickHouse instance, reads each table's
   `create_table_query`, and parses it with the ClickHouse SQL parser to
   reconstruct the declarative schema (columns, indexes, constraints,
   engine, ORDER BY, PARTITION BY, SAMPLE BY, PRIMARY KEY, TTL,
   SETTINGS).
3. **Diff Engine** (`internal/loader/hcl/diff.go`) — compares two
   resolved schemas (HCL ↔ HCL, HCL ↔ live, live ↔ live).
4. **SQL Generator** (`internal/loader/hcl/sqlgen.go`) — turns the diff
   into CREATE/ALTER/DROP DDL; ordering respects MV and Distributed
   dependencies; in-place-impossible changes are flagged `-- UNSAFE`.
5. **Validators** (`internal/loader/hcl/validate.go`) — dependency checks
   for MVs (`query` source tables, `to_table` target) and Distributed
   tables (`remote_database`/`remote_table`).

## Development Commands

```bash
# Build the active binary
go build -o hclexp ./cmd/hclexp

# Run unit + snapshot tests
just test

# Live ClickHouse integration tests (needs: docker compose up -d)
just test-live

# Update snapshot fixtures
just test-update-snapshots
```

The `justfile` has the full recipe list.

## Project Structure

```
/
├── cmd/
│   └── hclexp/             # CLI entry point (subcommand dispatch)
├── internal/
│   ├── loader/hcl/         # HCL parser, resolver, diff, sqlgen,
│   │                       # introspection, validation, plan
│   └── logger/             # Shared logging
├── docs/                   # Reference + FAQ + plans
│   ├── README.hcl.md       # HCL language reference (authoritative)
│   ├── FAQ.md              # Worked examples
│   └── plans/              # Design docs for in-flight features
├── config/                 # ClickHouse connection config
└── test/                   # Snapshot + live integration tests
```

## Sample Workflow (hclexp)

1. Edit an HCL file under your `schema/` tree (e.g. add a column to a
   `table "events"` block).
2. Run `hclexp diff -left ./schema -right clickhouse://...` to preview
   the change summary, or `-sql` to see the migration DDL.
3. Commit and open a pull request; CI runs the same diff and asserts no
   unintended drift.

# Testing

## Unit Tests
- Run with: `go test ./internal/... -v`
- Located in each internal package

## Integration Tests (Snapshot-based)
- Run with: `go test ./test -v`
- SQL snapshot tests validate generated DDL
- Update snapshots: `go test ./test -update-snapshots`

## Live ClickHouse Integration Tests
- Run with: `go test ./test -v -clickhouse`
- Requires running ClickHouse instance (use `docker compose up -d`)
- Connection config from environment variables (see docker-compose.yml for defaults)
- Tests automatically create/cleanup isolated test databases
- Current test coverage:
  - ✅ Basic connectivity (ping, SELECT 1)
  - ✅ End-to-end: HCL → Diff → apply DDL → Introspect → Compare
    (round-trip fidelity, `test/roundtrip_fidelity_live_test.go`)
  - ✅ Table name, database, columns, ORDER BY validation
  - ✅ Engine validation (unconditional - fully implemented)
  - ⏳ Settings validation (conditional - needs introspection enhancement)

## Supported Features

### HCL schema language (`docs/README.hcl.md` is authoritative)
- ✅ `database` blocks with `cluster` default (cascades to tables)
- ✅ `table` blocks: `primary_key`, `order_by`, `partition_by`,
  `sample_by`, `ttl`, `settings`, `comment`, `cluster`
- ✅ `column` blocks: `nullable`, `default` / `materialized` /
  `ephemeral` / `alias` (mutually exclusive), `codec`, `ttl`,
  `comment`, `renamed_from` (drives `RENAME COLUMN` in the diff)
- ✅ `index` blocks; adding an index to an existing table also generates a
  `MATERIALIZE INDEX` marked manual (`-- MANUAL:` in `diff -sql`,
  `"manual": true` in JSON/plan) — heavy mutations are operator-run, never
  executed automatically
- ✅ `constraint` blocks with `check` or `assume` (exactly one)
- ✅ `materialized_view` blocks (TO-form): `to_table`, `query`,
  explicit `column` list, `cluster`, `comment`
- ✅ `view` and `dictionary` top-level blocks (parsed, resolved, diffed,
  and emitted as DDL)
- ✅ Long view/MV `query` as a one-liner, HCL heredoc, or `file("x.sql")`;
  all normalize to a canonical beautified form so formatting never diffs as
  drift (see `docs/README.hcl.md`)
- ✅ `patch_table` (strictly additive cross-layer column additions)
- ✅ `extend` inheritance with `abstract` bases and cycle detection
- ✅ `override = true` for cross-layer full replacement
- ✅ `node` top-level blocks (introspection metadata: hostname +
  `macros` from `system.macros`; ignored by diff)
- ✅ `raw "<kind>" "<name>"` escape-hatch blocks: opaque CREATE DDL stored
  verbatim for objects the parser/HCL model can't express. Diffed as text,
  recreated (DROP+CREATE) on change; a `table`-kind change is flagged
  `-- UNSAFE`. `introspect`/`dump-cluster` are strict by default and capture
  raw blocks only with `-allow-raw`.

### Introspection & Dumping
- ✅ **Tables** — `hclexp introspect` round-trips tables (columns,
  indexes, constraints, engine, ORDER/PARTITION/SAMPLE/TTL/SETTINGS)
- ✅ **Exclude patterns** — `introspect`/`dump-cluster` take `-exclude <file>`,
  an HCL config with an `exclude { patterns = [...] }` glob list. Objects whose
  name (or `db.name`) matches are skipped *before* their DDL is parsed, so
  transient tables (`_tmp_replace_*`, migration `tmp_*`, `*_backup`, `*_staging`,
  …) neither land in the dump nor abort introspection. See `examples/exclude.hcl`.
- ✅ **Materialized Views** — TO-form only; inner-engine, refreshable,
  and window views are rejected with a clear error
- ✅ **Views & Dictionaries** — round-tripped as HCL

### Dependency Validation (`hclexp validate`)
- ✅ Materialized views: source tables (parsed from `query`) and
  `to_table` destination must be declared
- ✅ Distributed tables: `remote_database`/`remote_table` must be declared
- ✅ Fails on references into databases that weren't loaded
- ✅ `-skip-validation=<name,...>` / `-skip-validation='*'` skips checks
  for named dependent objects
- ✅ `hclexp diff -sql` orders CREATE/DROP DDL by these dependencies

### Cross-Node Drift (`hclexp drift`)
- ✅ Compares per-node HCL dumps in a directory; groups nodes and diffs
  each group against its lexically-first reference node
- ✅ `-dir`, `-glob` (filename filter), `-group-by` (macro names or the
  pseudo-keys `role`/`shard`/`replica`), `-details`; exits non-zero on
  drift (CI guard)
- ✅ `-zk-paths` (default `mask-uuid`) normalizes ReplicatedMergeTree
  `zoo_path` (table UUID → `{uuid}`) so per-shard path noise isn't drift

### SQL → HCL edits (`hclexp sql2hcl`)
- ✅ Applies ClickHouse DDL to a left-side HCL schema and emits updated HCL,
  reusing the introspection AST builders (`upsertObjectFromStmt`) and the
  `ApplySQL` engine in `internal/loader/hcl/sql_edit.go`
- ✅ `CREATE TABLE/MV/VIEW/DICTIONARY` (add or replace by name); `ALTER TABLE`
  add/drop/modify/rename column, add/drop index, modify/remove TTL,
  modify/reset setting; `ALTER TABLE <mv> MODIFY QUERY`; `DROP …`; `RENAME TABLE`
- ✅ `-left` (file or layer dirs), `-in` (file/stdin), `-out` (stdout/file/dir),
  `-database` (default DB for unqualified names), `-allow-raw` (capture
  unexpressible CREATE as a `raw{}` block, like `introspect`)
- ✅ Schema DDL only — data/partition ops (`TRUNCATE`, `ALTER … DELETE`,
  partition ops, `MATERIALIZE …`) are rejected; output is the resolved (flat)
  schema, pair with `hclexp diff -sql` to preview the migration
- ❌ Does not rewrite layered source files in place

### Structured diff output (`hclexp diff -format json`)
- ✅ `-format text` (default) or `json`; JSON emits a dependency-ordered
  `operations` list (kind, object_type, database, object, engine, replicated,
  sql, unsafe) plus a top-level `unsafe` list, for migration generators / CI

### Cross-role planning (`hclexp plan`)
- ✅ Diffs every role in an HCL `-manifest` against a `-dump` topology in one
  run and emits a single globally-ordered, cross-role operation list (storage
  before its Distributed/Buffer proxies before the MV), with `roles` provenance
- ✅ Manifest is role-first HCL with nested `env` blocks selected by `-env`;
  dump nodes match roles by `hostClusterRole` macro, replicas collapse to one
  representative; `-format json|text`

### Browse a schema (`hclexp web`)
- ✅ Serves a read-only web UI to browse a resolved HCL schema (databases,
  objects, columns/engine/settings, dependency cross-links); `-config`/`-layer`
  source, `-addr` to bind
- ✅ Auto-reloads on source change: each request re-stats the source files at
  most once per `-reload-interval` (default 2s; 0 disables) and reloads when a
  file's mod time changes; a broken edit keeps the last good schema
- ✅ `-manifest` (role/env/layers, like `plan`) browses every composed schema in
  one server: a schema list at `/`, each `(env, role)` under `/s/<env>/<role>/`;
  `-env` filters to one env, `-layer-root` prefixes the manifest's layer paths

### Supported Table Engines
MergeTree, ReplicatedMergeTree, ReplacingMergeTree (with `version_column`),
ReplicatedReplacingMergeTree, SummingMergeTree (with `sum_columns`),
CollapsingMergeTree, ReplicatedCollapsingMergeTree, AggregatingMergeTree,
ReplicatedAggregatingMergeTree, Distributed (with optional
`sharding_key`), Log, Kafka. See `docs/README.hcl.md` for the
attribute table.

### Not Yet Supported
- ❌ ReplacingMergeTree `is_deleted_column` parameter
- ❌ Inner-engine MVs, `REFRESH` MVs, window views

## Configuration

### ClickHouse Connection
Connection configuration is managed in `config/clickhouse.go`:
- Uses environment variables for defaults:
  - `CLICKHOUSE_HOST` (default: localhost)
  - `CLICKHOUSE_PORT` (default: 9000)
  - `CLICKHOUSE_DB` (default: migration_test)
  - `CLICKHOUSE_USER` (default: user1)
  - `CLICKHOUSE_PASSWORD` (default: pass1)
- Command-line flags override environment variables
- Connection includes automatic ping validation

# Git Commit Messages

- 1 line summary
- empty line
- detailed description

No need to list files or exact changes, it's already in a git commit, unless some previous assumptions or architecture
is modified.

# Direct orders:

- always write the plan to markdown plan, when a plan changes, update the md file
- in go.mod never change module name or go version
- run tests with: `go test ./test -v` and `go test ./internal/... -v`
- run live tests with: `go test ./test -v -clickhouse` (requires docker compose up -d)
- you can run clickhouse client directly: clickhouse client
- use gopls-io mcp when possible
- every time a new feature is added, there should be a test covering a feature 
- before preparing PR description, run git status