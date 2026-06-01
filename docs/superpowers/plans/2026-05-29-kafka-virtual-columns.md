# Kafka Virtual Columns Support

**Goal:** Teach `hclexp` about the implicit/virtual columns ClickHouse exposes on `Kafka`-engine
tables (`_topic`, `_key`, `_offset`, `_partition`, `_timestamp`, `_timestamp_ms`, `_headers.name`,
`_headers.value`, plus `_raw_message`/`_error` in `kafka_handle_error_mode = 'stream'`), so that
(1) materialized views reading those columns from a Kafka source validate cleanly, and (2)
introspection/diff never treat a virtual column as a managed column (no spurious `ADD`/`DROP`).

**Why:** Virtual columns are not part of a Kafka table's DDL (`create_table_query`), so the
DDL-parsing introspector never sees them and the rest of the tool has no notion that a Kafka source
exposes them. Today the only place that knows is a hard-coded `kafkaVirtualColumns` constant in the
live-test stub builder (`test/integration_live_test.go`) — duplicated, drift-prone, and slightly
wrong (it includes `_raw_message` unconditionally and omits `_error`). Reference:
`clickhouse-docs/en/engines/table-engines/integrations/kafka.md` §Virtual columns (lines 264-280).

**Architecture:** A single canonical, engine-setting-aware source of truth in the `hcl` package.
`KafkaVirtualColumns(EngineKafka)` returns the set (stream-only columns gated on
`HandleErrorMode`). `IsKafkaVirtualColumn(name)` is the membership predicate. `ColumnsProvidedBy(TableSpec)`
is the seam both surfaces consume: declared columns, plus the virtual set when the engine is Kafka.
The live-test constant is replaced by a call into this set.

**Conventions:** Go + `stretchr/testify`. Unit tests: `go test ./internal/... ./cmd/... -v`. Live:
`-clickhouse` + `docker compose up -d`. Engines are typed structs decoded in `engines.go`
(`EngineSpec.Decoded.(EngineKafka)`, `Engine.Kind()`). Do **not** add features to the legacy
`internal/introspection` / `internal/diff` / proto paths (frozen).

---

## Design

### Foundation — `internal/loader/hcl/kafka_virtual.go` (new)

```go
func KafkaVirtualColumns(e EngineKafka) []DeclaredColumn
func IsKafkaVirtualColumn(name string) bool
func ColumnsProvidedBy(t TableSpec) []DeclaredColumn
```

- Base set (always): `_topic LowCardinality(String)`, `_key String`, `_offset UInt64`,
  `_partition UInt64`, `_timestamp Nullable(DateTime)`, `_timestamp_ms Nullable(DateTime64(3))`,
  `_headers.name Array(String)`, `_headers.value Array(String)`.
- Stream-mode extras (when `e.HandleErrorMode != nil && *e.HandleErrorMode == "stream"`):
  `_raw_message String`, `_error String`.
- `IsKafkaVirtualColumn` matches the **full** name set (both modes) plus the `_headers` Nested parent
  — it is a name guard, mode-independent on purpose.
- `ColumnsProvidedBy` returns `t.Columns` mapped to `DeclaredColumn`, and if
  `t.Engine.Decoded` is `EngineKafka`, appends `KafkaVirtualColumns(e)` for names not already declared
  (a declared `_key`/`_timestamp` via `kafka_map_virtual_columns_on_write` wins — it is a real column).

### Surface 1 — MV column validation (`internal/loader/hcl/validate.go`)

Extend `Validate`. For each non-skipped MV with a **single resolvable source** (exactly one
distinct source table, which is declared in a loaded DB, and the query is not `SELECT *`):
- Build the source's provided-column name set via `ColumnsProvidedBy`.
- Walk the parsed query; collect column identifiers attributable to that single source.
- Emit a `ValidationError` (new `Kind = "mv_column"`) for any referenced column absent from the set.

**Conservatism (no false positives) is the governing constraint:**
- Only when there is exactly one source table and no `JOIN`/secondary table refs (so every bare
  column belongs to that source unambiguously).
- Skip `SELECT *` / `SELECT t.*`.
- Ignore identifiers that are function names, aliases introduced in the same query (`AS x`), or CTE
  columns. When attribution is uncertain, do not flag.
- Only runs for sources we can resolve to a `TableSpec`; unknown/system sources are skipped (the
  existing table-level check already reports truly missing sources).

This catches the real case (an MV reading `_offset`/`_timestamp` from a Kafka source now passes;
a typo'd `_offsett` is flagged) without risking valid complex MVs.

### Surface 2 — Introspect / diff guard (`internal/loader/hcl/diff.go`)

When diffing two tables whose engine is `Kafka`, filter both column lists through
`IsKafkaVirtualColumn` before comparison, so a virtual column can never become an `ADD COLUMN` /
`DROP COLUMN`. Note: the **declared-vs-implicit** rule means this only suppresses virtuals that leak
in from a column-listing path — declared columns named `_key`/`_timestamp` come from the DDL and are
kept. The active introspector parses `create_table_query` (virtuals never appear), so this is a
correctness guard for any future `system.columns`/`DESCRIBE`-based path and for hand-written HCL.
(The legacy `internal/introspection` `system.columns` query is frozen; if ever revived, add
`AND is_virtual = 0`.)

### Test harness — `test/integration_live_test.go`

Replace the hard-coded `kafkaVirtualColumns` constant with a render of the canonical set
(`KafkaVirtualColumns` + the existing `_headers Nested(...)` shape for the stub), so the stub builder
and the production library never drift, and `_error`/stream handling is correct.

---

## Tasks

- [ ] **Foundation** — create `internal/loader/hcl/kafka_virtual.go` with `KafkaVirtualColumns`,
      `IsKafkaVirtualColumn`, `ColumnsProvidedBy`.
- [ ] **Foundation tests** — `internal/loader/hcl/kafka_virtual_test.go`: base vs stream set;
      membership predicate; `ColumnsProvidedBy` for Kafka vs non-Kafka and the declared-wins case.
- [ ] **Diff guard** — filter Kafka virtual columns in `diff.go` table-column comparison; unit test
      that a stray `_offset` on a Kafka table yields no column diff while a real column change does.
- [ ] **MV column validation** — extend `validate.go` (`mv_column` kind, conservative single-source);
      unit tests: passes a real virtual ref, flags a bogus ref, no false-positive on `SELECT *`/JOIN.
- [ ] **Test harness** — point `buildStubColList` at the canonical set; run `just test-live`.
- [ ] **Docs** — note Kafka virtual-column handling in `README.md` / `docs/README.hcl.md`.

## Verification

- `go build ./... && go test ./internal/... ./cmd/... -v` (unit + snapshot).
- `go test ./test -v -clickhouse` (live) — the existing Kafka-source MV fixtures validate and
  round-trip with no virtual columns in the dump/diff.
