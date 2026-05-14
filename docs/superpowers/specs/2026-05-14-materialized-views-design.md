# Materialized View support in the HCL package

## Context

`hclexp` (the `internal/loader/hcl` package + `cmd/hclexp` CLI) is the future
of this project; the legacy `chschema` binary, its protobuf contracts, and
the YAML format are deprecated. The HCL package currently models only
**tables** — `DatabaseSpec` has `Tables` and `Patches` and nothing else.

This has two consequences:

1. Materialized views cannot be expressed in HCL, dumped, diffed, or
   generated.
2. `hcl.Introspect` is **actively broken** for any database that contains a
   materialized view: it reads `create_table_query` for every
   `system.tables` row and feeds it to a CREATE TABLE parser, which errors
   with "no CREATE TABLE statement found" on an MV.

This change makes materialized views first-class in the HCL package: a new
`MaterializedViewSpec` type that parses from HCL, is introspected from a
live cluster, dumps back to HCL, diffs, and generates DDL — the same
round-trip tables already have.

Scope is the **`TO <table>` form** of materialized view, which covers 100%
of the PostHog MV fixtures. Inner-engine MVs, refreshable MVs, window
views, and plain (non-materialized) views are explicitly out of scope and
must fail with a clear "unsupported" error rather than being silently
mishandled.

## Approach

Materialized views are modeled as a **sibling collection** on
`DatabaseSpec`, parallel to `Tables` — not as a flavored `TableSpec`. MVs
are a genuinely distinct object type; folding them into `TableSpec` would
pollute it with fields meaningless for tables (and vice versa). MVs do
**not** participate in the table inheritance system
(`extend` / `abstract` / `patch_table`).

This mirrors how the (deprecated) proto side already models them
(`DatabaseSpec.MaterializedViews`).

## Data model

`internal/loader/hcl/types.go` — new type:

```go
type MaterializedViewSpec struct {
    Name    string       `hcl:"name,label"`
    ToTable string       `hcl:"to_table"`          // TO <db.>table target (required)
    Columns []ColumnSpec `hcl:"column,block"`      // explicit column list
    Query   string       `hcl:"query"`            // the AS SELECT ... body
    Cluster *string      `hcl:"cluster,optional"`  // ON CLUSTER
    Comment *string      `hcl:"comment,optional"`
}
```

`DatabaseSpec` gains:

```go
MaterializedViews []MaterializedViewSpec `hcl:"materialized_view,block"`
```

`Columns` reuses the existing `ColumnSpec`. The PostHog fixtures only use
`name` + `type` in the MV column list, but reusing `ColumnSpec` keeps the
model consistent and costs nothing.

### HCL block

```hcl
database "posthog" {
  materialized_view "app_metrics_mv" {
    to_table = "sharded_app_metrics"
    query    = "SELECT team_id, category FROM kafka_app_metrics"
    column "team_id"  { type = "Int64" }
    column "category" { type = "LowCardinality(String)" }
  }
}
```

## Components

### Parsing — `parser.go`

Purely additive. `gohcl` decodes the new `materialized_view` block
automatically once the struct tag is on `DatabaseSpec`. No resolver
changes — MVs are not part of `extend` / `abstract` / `patch_table`.

### Introspection — `introspect.go`

`Introspect` reads `create_table_query` for every `system.tables` row.
Today it assumes every statement is a `CREATE TABLE`. Change: switch on the
parsed statement type:

- `*chparser.CreateTable` → `buildTableFromCreateSQL` (today's path)
- `*chparser.CreateMaterializedView` → new `buildMaterializedViewFromCreateSQL`
- `*chparser.CreateView` → skipped for now (plain views out of scope)

`buildMaterializedViewFromCreateSQL` reads from the
`*chparser.CreateMaterializedView` AST:

- `Destination` → `ToTable` (required; if `Destination` is nil the MV is
  inner-engine → return an "unsupported: inner-engine materialized view"
  error)
- `Refresh != nil` → return an "unsupported: refreshable materialized view"
  error
- `SubQuery` → `Query` (formatted back to SQL via the `formatNode` helper)
- the column list → `Columns` (reusing `columnFromAST`)
- `OnCluster` → `Cluster`
- `Comment` → `Comment`

### Dump — `dump.go`

New `writeMaterializedView`, called from `writeDatabase` after the tables
are written. Emits the `materialized_view` block (sorted by name, like
tables).

### Diff — `diff.go`

`DatabaseChange` gains three fields, mirroring the table ones:

```go
AddMaterializedViews   []MaterializedViewSpec  // -> CREATE MATERIALIZED VIEW
DropMaterializedViews  []string                // -> DROP VIEW
AlterMaterializedViews []MaterializedViewDiff
```

```go
type MaterializedViewDiff struct {
    Name        string
    QueryChange *StringChange  // the AS SELECT body changed
    Recreate    bool           // to_table or the column list changed
}
```

`MaterializedViewDiff.IsEmpty()` is true when `QueryChange == nil && !Recreate`.
`MaterializedViewDiff.IsUnsafe()` returns `Recreate`.

`diffDatabase` indexes MVs by name alongside tables and produces add / drop /
alter entries. `DatabaseChange.IsEmpty()` and `sortDatabaseChange` are
extended to cover the MV fields.

### SQL generation — `sqlgen.go`

`GenerateSQL` is extended. Statement order becomes: CREATE TABLE, CREATE
MATERIALIZED VIEW, ALTER TABLE, ALTER ... MODIFY QUERY, DROP VIEW, DROP
TABLE — MVs are created after their destination table and dropped before
it. For altered MVs:

- query-only change → `ALTER TABLE <mv> MODIFY QUERY <new query>`
- `Recreate` set → an `UnsafeChange` entry, no DDL synthesized (consistent
  with how engine swaps are handled today)

New helpers: `createMaterializedViewSQL`, `modifyQuerySQL`.

### CLI — `cmd/hclexp`

`renderChangeSet` gains `+ / - / ~ materialized_view ...` lines. No flag
changes: `diff` and `introspect` already operate on whole `DatabaseSpec`s,
so they pick up MVs for free once the type and the loaders support them.

## Testing

- **parser**: parse an HCL file containing a `materialized_view` block;
  assert the decoded `MaterializedViewSpec`.
- **introspect**: unit test feeding a `CREATE MATERIALIZED VIEW ... TO ...`
  statement and asserting the built spec; unit test asserting inner-engine
  and refreshable MVs return clear "unsupported" errors.
- **dump**: round-trip — parse → `Write` → parse, assert the
  `MaterializedViewSpec` is unchanged.
- **diff**: query-only change → `QueryChange` set, `Recreate` false →
  `MODIFY QUERY` DDL; `to_table` change → `Recreate` true → unsafe; plus
  add and drop cases.
- **live** (`-clickhouse`): create a destination table and a
  `TO`-form MV against a real ClickHouse instance, introspect, and assert
  the MV round-trips.

## Out of scope

These must fail with a clear, specific error (not be silently mishandled),
and are documented as unsupported in the README:

- inner-engine materialized views (`CREATE MATERIALIZED VIEW ... ENGINE = ...`)
- refreshable materialized views (`REFRESH EVERY|AFTER ...`)
- window views
- plain (non-materialized) views — skipped during introspection for now

## Verification

- `go build ./...` and `go test ./internal/... ./cmd/...` pass.
- `go test ./test -clickhouse` (with `docker compose up -d`): the new live
  MV round-trip test passes.
- Manual: `hclexp introspect -database <db>` against a database containing
  a `TO`-form MV emits a `materialized_view` block; `hclexp diff` between
  two HCL files differing in an MV reports the change and `-sql` emits
  `MODIFY QUERY` / `CREATE MATERIALIZED VIEW`.
- README gains a `materialized_view` section documenting the block and the
  out-of-scope list.
