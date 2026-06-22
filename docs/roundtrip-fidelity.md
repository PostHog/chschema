# Round-trip fidelity: verify hclexp recreates a schema exactly

`hclexp` is only safe to use as a source of truth if it can take a live schema,
represent it as HCL, and regenerate DDL that recreates **the same** objects. A
silent gap there hides drift and, worse, can rewrite columns (e.g. turning an
`ALIAS` column into a plain stored one). This page describes the tooling that
verifies that round-trip, including how to capture a production schema and test
it locally.

## The idea

For every object in a database, compare ClickHouse's own canonical `CREATE`
statement **before** and **after** a full hclexp round-trip:

1. capture the source schema's `CREATE` statements (the golden);
2. round-trip through hclexp — `introspect` → HCL → resolve → generate DDL;
3. recreate the objects from the generated DDL;
4. capture the `CREATE` statements again;
5. assert they are byte-identical to the golden.

ClickHouse normalizes `create_table_query` identically on both sides, so any
difference is a real fidelity gap in hclexp.

## Capture a schema with `hclexp dump-sql`

`dump-sql` writes every object's `create_table_query` (the `SHOW CREATE`
equivalent) for a database, in apply order, as a self-describing, replayable
SQL file:

```bash
# Capture a production database to a file
hclexp dump-sql -host prod-ch -port 9000 -user … -password … \
  -database posthog -out prod-posthog.sql
```

The output starts with a `-- database: <name>` header and contains one `CREATE`
per object, terminated by `;` on its own line. It is plain ClickHouse DDL — you
can apply it directly with `clickhouse client`, or feed it to the fidelity test.

Connection flags match `introspect` (`-host/-port/-user/-password`, `-secure`,
`-tls-skip-verify`). Omit `-out` to write to stdout.

## Run the fidelity test locally

The check lives as a gated Go test, `TestLive_RoundTripFidelity`, against a
local ClickHouse (the repo's `docker compose` stack):

```bash
docker compose up -d

# Run against the checked-in default fixture
go test ./test -run TestLive_RoundTripFidelity -v -clickhouse

# Verify a schema you captured from production
ROUNDTRIP_FIXTURE=$PWD/prod-posthog.sql \
  go test ./test -run TestLive_RoundTripFidelity -v -clickhouse
```

The test reads the database name from the fixture's `-- database:` header,
creates that database locally, seeds it, round-trips it through hclexp, recreates
it, and asserts every `CREATE` statement is unchanged. This is the **"dump prod,
test locally"** workflow: capture once against production, replay and verify on a
throwaway local cluster — no production access needed at test time.

A failure prints the per-object diff and the intermediate dumped HCL, so you can
see exactly which object and which clause changed.

## Limitations

- **Named clusters.** Objects that reference a cluster (`Distributed`, or any
  `ON CLUSTER`) only recreate locally if the local ClickHouse defines that
  cluster. The default fixture avoids them; to round-trip a production dump that
  contains them, run against a local cluster whose config defines the same
  cluster names.
- The default fixture is intentionally small (it proves the harness in CI). The
  real value is pointing `ROUNDTRIP_FIXTURE` at your own captured schema.
