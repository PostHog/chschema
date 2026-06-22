# Plan: SQL round-trip fidelity test + `hclexp dump-sql`

## Context

Issues #41 and #45 were fidelity gaps — `introspect`/`dump`/`diff -sql` producing
DDL that didn't faithfully recreate the source object. The existing tests check
pieces; we want one end-to-end guard that compares ClickHouse's own canonical
`CREATE` statements before and after a full hclexp round-trip, and that can be
seeded from a real production schema captured offline ("dump prod, test locally").

## Deliverables

### 1. `hclexp dump-sql` (capture tool)

`hclexp dump-sql -database <db> [-out file.sql]` connects to ClickHouse and writes
the raw `create_table_query` for every object in the database, in apply order
(plain tables → replicated → dictionaries → views → materialized views), each
terminated by `;` on its own line, with a leading `-- database: <db>` header so
the fixture is self-describing. Mirrors `runIntrospect`'s connection/flags. This
is how you capture prod into a replayable fixture.

### 2. Gated live fidelity test (`test/roundtrip_fidelity_live_test.go`)

Against a local ClickHouse, for a seed schema:

1. **Seed** — create the fixture's database, apply its `CREATE` statements in order.
2. **Golden** — read `create_table_query` for every object (CH-normalized).
3. **Round-trip through HCL** — `Introspect` → `Write` (HCL text) → `ParseFile` →
   `Resolve` → `Diff(empty, schema)` → `GenerateSQL`. Going through HCL **text** is
   essential — that's where #45 lived.
4. **Recreate** — `DROP DATABASE` + recreate it, apply the generated statements.
5. **Compare** — re-read `create_table_query` per object; assert byte-identical to
   the golden. Same database name throughout, so no normalization is needed.

Seed source: a checked-in fixture (`test/testdata/roundtrip/schema.sql`, database
`roundtrip`) runs in CI; `ROUNDTRIP_FIXTURE=<path>` points it at a captured prod
dump instead.

### 3. Default fixture (`test/testdata/roundtrip/schema.sql`)

Representative, cluster-free (so it recreates on any local CH): a MergeTree table
with every column modifier (default/materialized/alias/codec/ttl/comment) +
primary key + constraint + table comment, a ReplicatedMergeTree table, a plain
view, a TO-form materialized view + its target, and a dictionary over a source
table.

## Files
- `cmd/hclexp/dumpsql.go` (new) + dispatch/usage in `hclexp.go`.
- `test/roundtrip_fidelity_live_test.go` (new) + `test/testdata/roundtrip/schema.sql` (new).

## Reuse
- Connection: `config.GetDefaultConfig` / `config.NewConnection` (as `runIntrospect`).
- `Introspect`, `Write`, `ParseFile`, `Resolve`, `Diff`, `GenerateSQL`.
- `testhelpers.RequireClickHouse`.

## Limitations (documented)
- Objects referencing a named cluster (Distributed, `ON CLUSTER`) only recreate
  locally if the local CH defines that cluster; the default fixture avoids them.
  A prod dump containing them needs a local CH with matching cluster config.
- Depends on the #45 dumper fix; stacked on `fix/dump-column-modifiers`.

## Verification
- `go build`, `go vet`, gofmt; the gated test runs in CI's `test-live` job (and
  locally via `go test ./test -clickhouse`). Manual: `hclexp dump-sql` against a DB.
