# introspect/dump-cluster: exclude transient objects

## Problem

The committed `clickhouse-schema/` dumps carry transient objects that pollute
review and even break introspection: ClickHouse's atomic-replace temporaries
(`_tmp_replace_*`), migration/DAG scratch tables (`tmp_dag_*`, `tmp_person_0007`,
`infi_clickhouse_orm_migrations_tmp`), backups (`*_backup`, `*_backup_test`),
staging (`*_staging`), and backfills (`*_temp_backfill`). Their DDL frequently
can't be parsed, so without `-allow-raw` a single transient object aborts the
whole node dump.

## Approach

Tool-level **exclude config**: an HCL file with glob patterns; matching objects
are skipped **before** their DDL is parsed.

- `internal/loader/hcl/exclude.go`: `ExcludeMatcher` (glob patterns via
  `filepath.Match`, matched against both `name` and `<db>.<name>`) and
  `LoadExcludeConfig(path)` decoding `exclude { patterns = [...] }`. A nil
  matcher / empty config excludes nothing. Patterns validated at load.
- `IntrospectWithExclude(ctx, conn, db, allowRaw, exclude)` wraps `Introspect`
  (which stays nil-exclude for back-compat); `processIntrospectRowsOpt` skips
  rows whose name matches **before** calling `introspectOneObject`, so an
  excluded object's unparseable DDL never aborts the dump and it's never
  captured as raw.
- CLI: `introspect` and `dump-cluster` gain `-exclude <file>`, loaded via
  `loadExclude` and threaded through `introspectSchema` (and `dumpNode`).
- `examples/exclude.hcl`: a starter config covering the observed transient
  patterns.

## Verification

- `exclude_test.go`: matcher (bare + qualified globs, nil-safe); config load
  (parse, empty, invalid glob); and the committed `examples/exclude.hcl` matched
  against the real transient names from the production dumps while keeping
  genuine objects.
- `introspect_test.go`: an excluded object with **unparseable** DDL is skipped
  in strict mode (no error, not captured as raw) — proving skip-before-parse.
- Full unit + cmd + snapshot suites, `go vet`, golangci-lint, gofmt green.
  (No live ClickHouse smoke — the row-processing path is unit-covered with fake
  rows.)

## Scope notes

- Patterns are glob, not regex (intuitive; covers the hex-suffixed temp names
  via `*`). `*_old` / `*_new` are left commented in the example since some are
  intentionally kept (e.g. `query_log_archive_old`).
