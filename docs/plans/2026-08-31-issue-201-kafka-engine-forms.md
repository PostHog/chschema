# Issue #201: Kafka engine argument forms

## Goal

Make Kafka introspection lossless for every ClickHouse-supported constructor
shape, especially production `Kafka(<named_collection>, key = value, ...)`
DDL, and prevent named arguments from ever being mistaken for positional
values.

## Plan

- [x] Add focused regressions for constructor overrides, named collection plus
      `SETTINGS`, three-positionals plus `SETTINGS`, and deprecated long
      positional arguments, including malformed mixed-form failures.
- [x] Change the Kafka model and resolver so a named collection may carry typed
      setting overrides while complete broker/topic/group/format remain
      required only for the non-collection form.
- [x] Classify Kafka constructor arguments as named or positional, merge all
      supported overrides into typed fields, map legacy positional tails, and
      reject ambiguous/corrupt shapes loudly.
- [x] Preserve collection overrides in HCL dumps and emit the canonical
      `Kafka(<collection>) SETTINGS ...` SQL form.
- [x] Add a raw-SQL-first live regression and production-shaped fixture
      coverage; update the Kafka HCL documentation.
- [x] Run formatting, focused tests, and the required full `go test ./...`
      suite; record any live-test limitation.
- [x] Commit the verified fix on a clean branch from `main`, push it, and open
      the issue-closing pull request.
- [x] Resolve CI staticcheck feedback, rerun verification, and push the
      correction to the pull request.

## Verification

- `go test ./...` — 1,189 passed.
- `go test -race ./internal/loader/hcl` — 873 passed.
- `go vet ./...` — passed.
- `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 run` — 0 issues.
- `go test ./internal/loader/hcl -run '^TestCHLive_Kafka_RawNamedCollectionOverrides$' -v -clickhouse` — passed against ClickHouse 26.3.
- `go test ./test -v -clickhouse` — 10 passed.

## Non-goals

- Rewriting the already-corrupted `PostHog/clickhouse-schema` dumps; they must
  be re-dumped after this fix ships.
- Broad audits or behavior changes for non-Kafka engines.
