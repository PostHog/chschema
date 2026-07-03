# ReplacingMergeTree `is_deleted` support (#108)

## Problem

`hclexp introspect` captures only the ReplacingMergeTree /
ReplicatedReplacingMergeTree `ver` parameter and silently drops the optional
`is_deleted` parameter. The loss is symmetric (golden and fresh introspect both
drop it), so `diff` reports no difference while the golden's generated SQL
would recreate the table without soft-delete semantics. Affected in prod:
`pg_embeddings`, `adhoc_events_deletion`.

ClickHouse signature (docs: mergetree-family/replacingmergetree.md):
`ReplacingMergeTree([ver [, is_deleted]])`, and "is_deleted can only be
enabled when ver is used".

## Changes

All in `internal/loader/hcl/`:

1. **engines.go** — `IsDeletedColumn *string \`hcl:"is_deleted_column,optional"\``
   on `EngineReplacingMergeTree` and `EngineReplicatedReplacingMergeTree`.
2. **introspect.go** — both decode paths (chparser AST switch and the legacy
   `ParseEngineString`): capture the extra parameter. Also make both engines
   **error on more parameters than modeled** instead of silently dropping —
   the exact failure mode of this issue must abort introspection loudly next
   time ClickHouse grows a parameter.
3. **sqlgen.go** `engineSQL` — emit `ReplacingMergeTree(ver, is_deleted)` /
   `ReplicatedReplacingMergeTree('zoo', 'replica', ver, is_deleted)`.
4. **dump.go** — write `is_deleted_column` into dumped HCL.
5. **resolver.go** — new `validateReplacingEngines` (sibling of
   `validateKafkaEngines`): `is_deleted_column` without `version_column` is an
   error, per ClickHouse's own rule.
6. Diff needs no change: `diffEngine` compares decoded engine structs, so the
   new field participates automatically.

## Tests

- `testdata/engines_all_kinds.hcl` + `engines_test.go`: tables with
  `is_deleted_column` for both engine kinds.
- `introspect_test.go`: engine-decode cases `ReplacingMergeTree(ver, is_deleted)`,
  `ReplicatedReplacingMergeTree('/p', '{replica}', ver, is_deleted)`, plus
  too-many-params error cases; same for `TestParseEngineString`.
- `sqlgen_test.go`: engineSQL emission for both.
- `dump_test.go`: dumped HCL contains `is_deleted_column`.
- `resolver_test.go`: `is_deleted_column` without `version_column` errors.
- `test/hcl_introspect_live_test.go`: live round-trip table with
  `ReplacingMergeTree(version, is_deleted)` (gated by `-clickhouse`).

## Related research (from clickhouse-docs, requested alongside this fix)

Sweep of every engine we support vs its documented signature:

| Engine | Docs signature | Status |
|---|---|---|
| ReplacingMergeTree | `([ver [, is_deleted]])` | **this fix** |
| Distributed | `(cluster, db, table[, sharding_key[, policy_name]])` | **`policy_name` silently dropped — same bug class, separate issue** |
| CollapsingMergeTree | `(sign)` | complete |
| SummingMergeTree | `([columns])` | complete |
| AggregatingMergeTree | none | complete |
| Buffer | 9 + optional flush triplet | complete |
| Join | `(strictness, type, k1[, k2, ...])` | complete |
| Merge | `(db_regex, table_regexp)` | complete |
| Kafka | settings-based | complete |
| VersionedCollapsing / Graphite / Coalescing MergeTree | — | engines not supported at all; tracked in #86 |

## Verification

`go test ./internal/... ./cmd/... ./test`; live: `docker compose up -d` +
`go test ./test -v -clickhouse -run Replacing`; manual `dump-sql` on a table
created with `ReplacingMergeTree(ver, is_deleted)` round-trips both params.
