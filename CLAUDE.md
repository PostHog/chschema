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

# Run every test CI runs (./internal/... ./cmd/... and ./test)
just test

# Live ClickHouse integration tests (needs: docker compose up -d)
just test-live
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
- Run with: `go test ./internal/... ./cmd/... -v`
- Located in each internal package, plus `cmd/hclexp` (CLI wiring and the
  text/JSON renderers — CI runs these, so `./internal/...` alone is not enough)

## Integration Tests
- Run with: `go test ./test -v`

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
- ✅ `projection` blocks (`query`, optional `settings` → `WITH SETTINGS`);
  modify diffs as DROP+ADD, and adding to an existing table generates a
  manual `MATERIALIZE PROJECTION` (operator-run, like `MATERIALIZE INDEX`);
  index-form projections (`PROJECTION p INDEX …`) are unsupported and fail
  loudly at parse
- ✅ `materialized_view` blocks (TO-form): `to_table`, `query`,
  explicit `column` list, `cluster`, `comment`
- ✅ `view` and `dictionary` top-level blocks (parsed, resolved, diffed,
  and emitted as DDL)
- ✅ A changed `dictionary` reconciles via `CREATE OR REPLACE DICTIONARY` — one
  atomic statement rewriting the whole object, safe (a dictionary holds no
  persistent data; it reloads from its source) and never flagged unsafe
- ✅ Secrets: ClickHouse redacts a credential to `[HIDDEN]` unless the
  introspecting user has `displaySecretsInShowAndSelect`. The marker is kept
  **in-band** (dictionary sources and named-collection params alike) so it
  round-trips through a dump and a comparison can tell "secret I cannot see"
  from "no secret". It compares as unknown (both sides hidden → equal; hidden
  vs a real value → reported unverifiable, excluded from the diff; hidden vs
  absent → a real difference), and `sqlgen` refuses to emit any statement
  containing it. What that costs differs by kind: a dictionary is rewritten
  whole, so an unknown secret blocks *any* change to it; a named collection has
  surgical `ALTER … SET`/`DELETE`, so only the statements that write every param
  (`CREATE`, and the DROP+CREATE an `ON CLUSTER` change forces — blocked as a
  pair) are refused. Authored `password = "[HIDDEN]"` declares a secret managed
  outside hclexp. See `docs/README.hcl.md`
- ✅ Long view/MV `query` as a one-liner, HCL heredoc, or `file("x.sql")`;
  all normalize to a canonical beautified form so formatting never diffs as
  drift (see `docs/README.hcl.md`)
- ✅ A layer stack entry is a directory (every `*.hcl` in it) **or a single
  `.hcl` file** — same merge semantics either way (`hclload.LayerFiles` owns the
  dir/file decision, so every `-layer`/`-left`/manifest `layers` path gets it);
  a non-`.hcl` or missing entry errors
- ✅ `patch_table` (cross-layer table modification; the target stays declared
  once): columns add/`modify_column`/`drop_columns` (modify → drop → add),
  indexes add/`drop_indexes` (drop first, so drop+add redefines),
  `order_by`/`partition_by`/`sample_by`/`ttl` replace when set, `engine`
  replaces wholesale, `settings` merge patch-wins; `primary_key`/comment/
  constraints/projections stay non-patchable (use `override = true`)
- ✅ `patch_view` (`query`/`comment` replace; query normalized like a declared
  view's) and `patch_dictionary` (`source`/`layout`/`lifetime` replace
  wholesale, `settings` merge) — unknown targets error; MVs have no patch form
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
- ✅ **Exclude patterns** — `introspect`/`dump-cluster`/`diff`/`plan`/`drift`/`load`
  take `-exclude <file>`, an HCL config with an `exclude { patterns = [...] }` glob
  list plus an optional `object_types = [...]` (drop a whole class, e.g.
  `named_collection`). Objects whose name (or `db.name`) matches are skipped
  *before* their DDL is parsed, so transient tables (`_tmp_replace_*`, migration
  `tmp_*`, `*_backup`, `*_staging`, …) neither land in the dump nor abort
  introspection. On the comparison commands `FilterSchema` drops them from *both*
  sides before the diff, so they appear in no output and no count. See
  `examples/exclude.hcl`.
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
- ✅ `-role <name>` (manifest-driven mode) validates only that role; the
  cluster set is still derived from the whole manifest, so a single role's
  cross-role Distributed proxies still resolve
- ✅ `hclexp diff -sql` orders CREATE/DROP DDL by these dependencies

### Cross-Node Drift (`hclexp drift`)
- ✅ Compares per-node HCL dumps in a directory; groups nodes and diffs
  each group against its lexically-first reference node
- ✅ `-dir`, `-glob` (filename filter), `-group-by` (macro names or the
  pseudo-keys `role`/`shard`/`replica`), `-details`, `-exclude`; exits non-zero
  on drift (CI guard)
- ✅ `-zk-paths` (default `mask-uuid`) normalizes ReplicatedMergeTree
  `zoo_path` (table UUID → `{uuid}`) so per-shard path noise isn't drift
- ✅ `-format text|json`: JSON emits groups → drifters, each carrying the same
  per-object comparisons `diff` emits plus derived counts. Direction is
  descriptive (reference → drifter), NOT a fix script. The text one-liner is
  rendered from the same counts, so raw-block and named-collection drift are
  counted instead of printing the bare `changed` fallback

### SQL → HCL edits (`hclexp sql2hcl`)
- ✅ Applies ClickHouse DDL to a left-side HCL schema and emits updated HCL,
  reusing the introspection AST builders (`upsertObjectFromStmt`) and the
  `ApplySQL` engine in `internal/loader/hcl/sql_edit.go`
- ✅ `CREATE TABLE/MV/VIEW/DICTIONARY` (add or replace by name); `ALTER TABLE`
  add/drop/modify/rename column, add/drop index, modify/remove TTL,
  modify/reset setting; `ALTER TABLE <mv> MODIFY QUERY`; `DROP …`; `RENAME TABLE`
- ✅ `-left` (layer stack: dirs or `.hcl` files), `-in` (file/stdin), `-out` (stdout/file/dir),
  `-database` (default DB for unqualified names), `-allow-raw` (capture
  unexpressible CREATE as a `raw{}` block, like `introspect`)
- ✅ Schema DDL only — data/partition ops (`TRUNCATE`, `ALTER … DELETE`,
  partition ops, `MATERIALIZE …`) are rejected; output is the resolved (flat)
  schema, pair with `hclexp diff -sql` to preview the migration
- ❌ Does not rewrite layered source files in place

### Structured comparison output (`diff -format json`, `plan`, `drift -format json`)
- ✅ One shared model (`internal/loader/hcl/compare.go`): `ObjectComparison` =
  one differing object, with attribute-level `FieldChange`s (old/new) and the
  DDL ops that reconcile it. It is a *serialization of the existing ChangeSet* —
  `BuildObjectComparisons(cs, gen, left, right)` — never a second diff engine
- ✅ `diff -format json` emits `objects` + `summary` (counts derived from
  `objects`) alongside the unchanged flat `operations`/`unsafe` lists; an
  object's nested ops carry their index into the global list, so the object view
  and the execution view can't disagree. `-exclude` filters both sides
- ✅ Text mode renders from the same `[]ObjectComparison`
  (`RenderObjectComparisons`), so text/JSON/counts cannot contradict each other
- ✅ `status` is right-relative: `added` = present only on the right side of the
  `Diff(left, right)` call. `diff`/`plan` put desired on the right; `drift` puts
  the drifter on the right
- ✅ The `field` vocabulary (`column:`/`index:`/`projection:`/`constraint:`/
  `setting:`/`param:`/`engine`/`order_by`/…) is a public contract, documented in
  `docs/README.hcl.md`
- ✅ **System-proxy column subsets** — a Distributed proxy with
  `remote_database = "system"` compares columns subset-tolerantly in the diff
  engine (`diff`/`plan`/`drift`): presence differences are suppressed in
  either direction (system tables gain columns with server versions, so a
  declared subset is the only pinnable state), while columns on both sides
  still compare fully and an engine change yields the full column diff.
  Non-system proxies stay exact (#136 item 4)

### Composing a node from the manifest (`hclexp load`)
- ✅ `-manifest`/`-env` compose a node straight from the same role manifest
  `validate` and `plan` consume, so callers never rebuild the layer stack by
  hand; `-layer-root` roots the manifest's layer paths
- ✅ `-role <name>` composes one role; without it every role deployed in `-env`
  is composed and `-out` must name a directory (one `<env>-<role>.hcl` each).
  Several roles cannot go to stdout — their `database` blocks would collide
- ✅ `-out-name` templates the file names written into the `-out` directory
  (default `{env}-{role}`); `{env}`/`{role}` expand, `.hcl` is appended, and
  template subdirectories are created — `-out-name '{env}/{role}'` writes the
  per-env tree `golden/<env>/<role>.hcl` directly. Unknown placeholders,
  paths escaping `-out`, and two roles rendering to one path are errors
- ✅ Object filters on the emitted schema (plain and manifest modes, every
  composed role): `-exclude <file>` (the shared exclude config),
  `-exclude-objects <glob,...>` (ad-hoc drops), `-only <glob,...>` (keep only
  matches — the layer-surgery selector). Kept iff matches `-only` (when given)
  and neither exclusion; emptied `database{}` blocks and `node{}` blocks
  survive; rejected with `-format json`
- ✅ `-format json` emits each role's declared and resolved layer stack, for
  callers that need the stack itself rather than the composed schema; the
  stacks come from the manifest alone (no composition), so it answers "what
  should I fetch?" before the layer dirs exist
- ✅ `-manifest` is mutually exclusive with `-layer`/`-config`; an unknown
  `-role` exits 2, as in `validate`

### Cross-role planning (`hclexp plan`)
- ✅ Diffs every role in an HCL `-manifest` against a `-dump` topology in one
  run and emits a single globally-ordered, cross-role operation list (storage
  before its Distributed/Buffer proxies before the MV), with `roles` provenance
- ✅ Manifest is role-first HCL with nested `env` blocks selected by `-env`;
  dump nodes match roles by `hostClusterRole` macro, replicas collapse to one
  representative; `-format json|text`; `-exclude` filters both sides
- ✅ Alongside the merged `operations`, emits `roles`: each role's own
  (non-deduped) object comparisons with derived counts — triage is per
  (env, role), execution stays on the deduped global list

### Locating declarations (`hclexp locate`)
- ✅ `locate <name-or-glob>...` lists every declaration site (`file:line` +
  abstract/override/patch/extend flags) of matching objects across all
  manifest layers, with derived (role, env) placements read from the
  manifest across *every* env; `-dump DIR` also lists the per-node dump
  files declaring the object, attributed to the node (`node{}` block, else
  filename stem); `-format text|json`
- ✅ Several patterns are independent existence checks: exits 1 when *any*
  pattern matches nothing; an extended object cross-links its children
  (`extended_by`), computed over all authored declarations, not just matches
- ✅ `-layer` searches ad-hoc layer dirs or `.hcl` files (alone or alongside
  `-manifest`, deduped against its layers); sites carry no placements
- ✅ `-duplicates` (no name argument; requires `-manifest` or `-layer`):
  exits 1 when any `(database, name)` has two or more plain declarations
  (patch_table/override/abstract sites are legitimate), so CI enforces
  once-only even for layers that never co-compose

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
MergeTree, ReplicatedMergeTree, ReplacingMergeTree (with `version_column`
and `is_deleted_column`; the latter requires the former, matching
ClickHouse), ReplicatedReplacingMergeTree, SummingMergeTree (with
`sum_columns`),
CollapsingMergeTree, ReplicatedCollapsingMergeTree, AggregatingMergeTree,
ReplicatedAggregatingMergeTree, Distributed (with optional
`sharding_key` and `policy_name`; the latter requires the former),
Log, Kafka. See `docs/README.hcl.md` for the
attribute table.

### Not Yet Supported
- ❌ Inner-engine MVs, `REFRESH` MVs, window views
- ❌ Distributed `policy_name` parameter (silently dropped on introspect)

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
- run tests with: `go test ./...` — CI runs `./internal/... ./cmd/...` (with
  `-race`) and `./test/...`, so never trust `./internal/...` alone: the CLI
  wiring and the text/JSON renderers live in `cmd/hclexp` and do assert on
  rendered output
- run live tests with: `go test ./test -v -clickhouse` (requires docker compose up -d)
- you can run clickhouse client directly: clickhouse client
- use gopls-io mcp when possible
- every time a new feature is added, there should be a test covering a feature 
- before preparing PR description, run git status