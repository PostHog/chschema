# Named Collections support in the HCL package

## Context

`hclexp` (the `internal/loader/hcl` package + `cmd/hclexp` CLI) is the
future of this project; the legacy `chschema` binary is deprecated. The
HCL package today models tables, materialized views, and dictionaries —
all of which are database-scoped objects living inside `database "X" {}`
blocks. It does not model **named collections**, ClickHouse's
cluster-wide key/value bags used to share configuration (most notably
Kafka credentials and connection settings) across many tables.

Today, a Kafka table can only be expressed by inlining its four required
parameters (`kafka_broker_list`, `kafka_topic_list`, `kafka_group_name`,
`kafka_format`) into the engine block. The other ~20 documented Kafka
settings (`kafka_security_protocol`, `kafka_sasl_*`, `kafka_num_consumers`,
`kafka_skip_broken_messages`, `kafka_handle_error_mode`,
`kafka_commit_on_select`, …) are **silently dropped** by introspection —
`parseKafkaEngine` strips all `kafka_*` settings and only the four typed
fields survive. That's a real round-trip gap.

This change adds named collections as a first-class top-level entity in
the HCL package — they round-trip through parse / introspect / dump / diff /
sqlgen — and reworks the Kafka engine model to either reference a named
collection or carry a complete, typed set of inline `kafka_*` settings.

## Approach

Named collections are **cluster-scoped, not database-scoped** in
ClickHouse. They live next to `database` blocks, not inside one — the
top-level HCL schema is therefore extended to carry both:

```hcl
named_collection "my_kafka" { ... }
database "posthog"          { ... }
```

`ParseFile` returns a new `Schema` aggregate instead of `[]DatabaseSpec`.
This is a breaking change to the package's public API; one round of
mechanical updates to `cmd/hclexp` and the test suite.

The clickhouse-sql-parser fork already has `*chparser.CreateNamedCollection`
in its AST. It does **not** have `AlterNamedCollection` or
`DropNamedCollection`. That's fine: we only need parsing for
introspection (where `system.named_collections.create_query` always
returns a CREATE statement), and we emit ALTER/DROP as plain SQL strings
at generation time. No parser fork work is required.

Diff handling is **surgical when possible**: parameter additions / updates
become `ALTER NAMED COLLECTION ... SET` statements, removals become
`ALTER NAMED COLLECTION ... DELETE`. `ALTER` cannot change `ON CLUSTER`,
so an `ON CLUSTER` change forces full recreation — emitted as a
`DROP NAMED COLLECTION` immediately followed by `CREATE NAMED COLLECTION`
at the front of the statement set (the drop-then-create pair is adjacent
so dependent tables see only a brief window without the collection).

The Kafka engine grows from four typed fields to a complete typed model of
the documented `kafka_*` settings plus a generic `extra` escape map for
keys ClickHouse adds in versions we don't yet model. A new optional
`collection` field references a named collection; `collection` and the
inline settings are **mutually exclusive** — ClickHouse rejects
overriding a named-collection setting via inline SETTINGS for the same key,
so the model doesn't permit mixed form.

## Data model

`internal/loader/hcl/types.go` — new types:

```go
// Schema is what ParseFile returns. It carries both top-level kinds
// hclexp tracks: databases (with their tables/MVs/dictionaries) and
// named collections (cluster-scoped, separate from any database).
type Schema struct {
    Databases        []DatabaseSpec
    NamedCollections []NamedCollectionSpec
}

// NamedCollectionSpec models a ClickHouse named collection — a
// cluster-scoped key/value bag of configuration values that other
// objects (most notably Kafka tables) can reference by name.
//
// External = true marks a collection as managed outside hclexp — for
// example, defined in the ClickHouse server XML config under
// /etc/clickhouse-server/config.d/. hclexp emits no DDL for external
// collections (no CREATE / ALTER / DROP); they exist in HCL only as
// declarations so engine `collection = "x"` references can resolve and
// be validated. Params is optional when External = true (the values
// live in the XML config, not in HCL).
type NamedCollectionSpec struct {
    Name     string                 `hcl:"name,label"`
    External bool                   `hcl:"external,optional"`
    Override bool                   `hcl:"override,optional" diff:"-"`
    Cluster  *string                `hcl:"cluster,optional"`
    Comment  *string                `hcl:"comment,optional"`
    Params   []NamedCollectionParam `hcl:"param,block"`
}

type NamedCollectionParam struct {
    Key         string  `hcl:"name,label"`
    Value       string  `hcl:"value"`
    Overridable *bool   `hcl:"overridable,optional"` // nil = use server default
}
```

### HCL block — named collection

```hcl
named_collection "my_kafka" {
  cluster = "posthog"
  comment = "shared kafka cluster for events ingestion"

  param "kafka_broker_list" { value = "k1:9092,k2:9092" }
  param "kafka_topic_list"  { value = "events" }
  param "kafka_group_name"  { value = "ch_events" }
  param "kafka_format"      { value = "JSONEachRow" }
  param "kafka_sasl_password" {
    value       = "[set via override layer]"
    overridable = false
  }
}
```

### Externally-managed (XML config) named collections

When a named collection is defined in the ClickHouse server's XML config
rather than via DDL (the PostHog production pattern), declare it in HCL
with `external = true`:

```hcl
named_collection "kafka_main" {
  external = true
  comment  = "managed in /etc/clickhouse-server/config.d/kafka_collections.xml"
}

database "posthog" {
  table "events_kafka" {
    engine "kafka" { collection = "kafka_main" }
  }
}
```

`hclexp` emits no `CREATE` / `ALTER` / `DROP` for external collections —
they're declared in HCL so:

- Kafka engine `collection = "..."` references can be validated against
  the declared set of collections (typo protection at parse time).
- The HCL is self-documenting: a reader can see which collections exist
  on the cluster without grepping the XML config.

Validation rules for external collections:

- `Params` is optional (values live in the XML config, not HCL).
- `Cluster` is optional.
- The spec round-trips through dump/parse but is **invisible to
  introspection** (introspection still filters `source = 'DDL'`).
- The spec is **invisible to diff** — comparing two schemas, any pair of
  matching external NCs is skipped regardless of attribute changes.
  Diff between a "from" with `external = true` and a "to" without (or
  vice versa) is an error: an NC can't be promoted from external to
  managed (or back) by hclexp; that's an operator-level migration.

### Production override pattern

The natural way to handle secret values in named collections is to put
the real values in a separate HCL file loaded as a later layer. The base
layer commits collection *references* (engine `collection = "my_kafka"`)
and placeholder values; the override layer commits the real values.

```bash
# Apply base + production-secret layer
hclexp -layer schema/base,schema/prod-secrets ...
```

Layer merging is extended so a `named_collection "x"` redeclared in a
later layer overrides the earlier one when `override = true`, otherwise
it errors — exactly the rule already in place for tables.

### Kafka engine

`EngineKafka` is reshaped to carry either a `Collection` reference or a
complete typed set of inline settings:

```go
type EngineKafka struct {
    // Collection is the named-collection reference. Mutually exclusive
    // with every other field; when set, no inline setting may be set.
    Collection *string `hcl:"collection,optional"`

    // Required when Collection is nil.
    BrokerList *string `hcl:"broker_list,optional"`
    TopicList  *string `hcl:"topic_list,optional"`
    GroupName  *string `hcl:"group_name,optional"`
    Format     *string `hcl:"format,optional"`

    // Optional auth.
    SecurityProtocol *string `hcl:"security_protocol,optional"`
    SaslMechanism    *string `hcl:"sasl_mechanism,optional"`
    SaslUsername     *string `hcl:"sasl_username,optional"`
    SaslPassword     *string `hcl:"sasl_password,optional"`

    // Optional numeric tuning.
    NumConsumers         *int64 `hcl:"num_consumers,optional"`
    MaxBlockSize         *int64 `hcl:"max_block_size,optional"`
    SkipBrokenMessages   *int64 `hcl:"skip_broken_messages,optional"`
    PollTimeoutMs        *int64 `hcl:"poll_timeout_ms,optional"`
    PollMaxBatchSize     *int64 `hcl:"poll_max_batch_size,optional"`
    FlushIntervalMs      *int64 `hcl:"flush_interval_ms,optional"`
    ConsumerRescheduleMs *int64 `hcl:"consumer_reschedule_ms,optional"`
    MaxRowsPerMessage    *int64 `hcl:"max_rows_per_message,optional"`
    CompressionLevel     *int64 `hcl:"compression_level,optional"`

    // Optional booleans (introspected as 0/1, presented as bool in HCL).
    CommitEveryBatch     *bool `hcl:"commit_every_batch,optional"`
    ThreadPerConsumer    *bool `hcl:"thread_per_consumer,optional"`
    CommitOnSelect       *bool `hcl:"commit_on_select,optional"`
    AutodetectClientRack *bool `hcl:"autodetect_client_rack,optional"`

    // Optional strings.
    ClientID         *string `hcl:"client_id,optional"`
    Schema           *string `hcl:"schema,optional"`
    HandleErrorMode  *string `hcl:"handle_error_mode,optional"`
    CompressionCodec *string `hcl:"compression_codec,optional"`

    // Extra is the escape valve for kafka_* settings ClickHouse adds in
    // versions we don't yet model. Keys are passed through verbatim and
    // MUST include the `kafka_` prefix (unlike the typed fields above,
    // which strip it). Use a typed field when one exists; reach for
    // `extra` only when no typed field covers the setting.
    Extra map[string]string `hcl:"extra,optional"`
}
```

### HCL — Kafka with named collection

```hcl
database "posthog" {
  table "events_kafka" {
    column "team_id" { type = "Int64" }
    column "payload" { type = "String" }
    engine "kafka" { collection = "my_kafka" }
  }
}
```

### HCL — Kafka with full inline settings (canonical form)

```hcl
database "posthog" {
  table "events_kafka" {
    column "team_id" { type = "Int64" }
    column "payload" { type = "String" }
    engine "kafka" {
      broker_list          = "kafka:9092"
      topic_list           = "events"
      group_name           = "ch_events"
      format               = "JSONEachRow"
      num_consumers        = 4
      max_block_size       = 1048576
      commit_on_select     = false
      skip_broken_messages = 100
      handle_error_mode    = "stream"
      sasl_mechanism       = "PLAIN"
      sasl_username        = "ch"
      sasl_password        = "[set via override layer]"
    }
  }
}
```

## Components

### Parsing — `parser.go`

`fileSpec` grows a `NamedCollections []NamedCollectionSpec` field
alongside `Databases`. `ParseFile`'s return type changes from
`[]DatabaseSpec` to `*Schema`. No post-parse `Decode*` pass is needed —
params are plain key/value/bool, not nested typed bodies.

### Layer merging — `layers.go`

`LoadLayers`'s return type also changes from `[]DatabaseSpec` to `*Schema`.
A new `mergeNamedCollections` carries collections across layers with a
duplicate-name guard mirroring the existing `mergeIntoDatabase` table
override logic: redeclaration without `override = true` errors; with it,
the later layer wins.

### Resolution — `resolver.go`

`Resolve(*Schema) error` runs the existing per-database resolution and
adds `validateNamedCollections` and `validateKafkaEngines`:

- Named collections: unique names; non-empty `Params` **when
  `External = false`** (external collections may have empty `Params`,
  since their values live in the XML config); unique `Key` within each
  collection.
- Kafka engine: XOR of `Collection` vs. inline settings. When inline:
  `BrokerList`, `TopicList`, `GroupName`, `Format` all required. When
  `Collection` set: every inline field, including `Extra`, must be
  nil/empty; the referenced collection must be declared in the schema
  (either managed or `external = true`).

### Introspection — `introspect.go` + new `named_collection_introspect.go`

A new top-level function:

```go
func IntrospectNamedCollections(ctx context.Context, conn driver.Conn) ([]NamedCollectionSpec, error)
```

queries `SELECT name, create_query FROM system.named_collections WHERE source = 'DDL' ORDER BY name`,
parses each `create_query` via `chparser`, and walks
`*chparser.CreateNamedCollection` into `NamedCollectionSpec` (name, cluster,
comment, params with overridable flags).

Config-file-based collections (`source != 'DDL'`) are filtered out — we
can't manage them via DDL.

Kafka introspection in `parseKafkaEngine` is extended to handle four cases:

1. `Kafka()` + `kafka_*` settings → walk every `kafka_*` setting through a
   per-setting decoder table; typed fields are populated, unknown keys
   land in `Extra` with their `kafka_` prefix intact.
2. `Kafka(<identifier>)` no settings → `EngineKafka{Collection: &name}`.
3. `Kafka('broker', 'topic', 'group', 'format')` legacy positional →
   normalize to case 1 by mapping the four args to `BrokerList`/`TopicList`/
   `GroupName`/`Format`.
4. `Kafka(<identifier>)` + `kafka_*` settings → introspection error
   (mixed form, not in our model).

The decoder table is shaped as a `map[string]func(*EngineKafka, string)`
keyed by full `kafka_*` setting name. Numeric settings parse via
`parseInt64Ptr`; bools via `parseBoolPtr` (accepts `0`/`1`/`false`/`true`).

### Dump — `dump.go` + new `named_collection_dump.go`

`Write` is updated to take `*Schema` instead of `[]DatabaseSpec`. After
the database loop it emits a `named_collection` block per collection,
sorted by name. `writeNamedCollection` emits `cluster`, `comment`,
then a `param "key" { value = "..." overridable = ... }` sub-block per
param (overridable omitted when nil).

`writeEngine` Kafka case is rewritten: when `Collection != nil`, emit
only `collection = "..."`. Otherwise walk every non-nil typed field and
emit each with native HCL types — numbers as numbers, bools as bools,
strings as strings; append `Extra` keys verbatim.

### Diff — `diff.go` + new `named_collection_diff.go`

`ChangeSet` gains a top-level field (collections aren't per-database):

```go
type ChangeSet struct {
    Databases        []DatabaseChange
    NamedCollections []NamedCollectionChange
}

type NamedCollectionChange struct {
    Name string
    Add  *NamedCollectionSpec  // present when adding (or as the create-half of a recreate)
    Drop bool                  // present when dropping (or as the drop-half of a recreate)
    Recreate bool              // ON CLUSTER changed — emit DROP then CREATE adjacently

    // For surgical (non-recreate) modifications:
    SetParams     []NamedCollectionParam  // upserts (changed + added)
    DeleteParams  []string                // removed keys
    CommentChange *StringChange
}
```

`diffNamedCollection(from, to *NamedCollectionSpec)`:
- **External handling:** if either side has `External = true` and the
  other side has `External = false`, return an error — promoting an NC
  from external to managed (or vice versa) is an operator-level
  migration, not a diff. If **both** sides are external, return an
  empty diff (no DDL for external collections, regardless of attribute
  changes). Otherwise proceed:
- `from.Cluster != to.Cluster` → `Recreate = true`; populate the full
  target `NamedCollectionSpec` in `Add` so the create-half has everything.
- Otherwise, walk params keyed by `Key`: changed values + added keys →
  `SetParams`; removed keys → `DeleteParams`. Comment delta →
  `CommentChange`.

A separate `NamedCollectionChange.IsEmpty()` returns true when no field
is set; `IsUnsafe()` always returns false (every change has a defined
DDL path — even the recreate is automated, not flagged for manual
intervention).

### SQL generation — `sqlgen.go` + new `named_collection_sqlgen.go`

New helpers:
- `createNamedCollectionSQL(d NamedCollectionSpec) string`
- `dropNamedCollectionSQL(name string) string`
- `alterNamedCollectionSetSQL(name string, params []NamedCollectionParam) string`
- `alterNamedCollectionDeleteSQL(name string, keys []string) string`

`GenerateSQL` statement ordering becomes:

1. **Recreate NCs (DROP+CREATE pairs, adjacent)** — for `Recreate: true` diffs.
2. CREATE NAMED COLLECTION — for fresh adds.
3. CREATE TABLE.
4. CREATE MATERIALIZED VIEW.
5. CREATE OR REPLACE DICTIONARY.
6. ALTER TABLE.
7. ALTER MATERIALIZED VIEW (MODIFY QUERY).
8. ALTER NAMED COLLECTION (SET then DELETE, surgical updates only).
9. DROP MATERIALIZED VIEW.
10. DROP DICTIONARY.
11. DROP TABLE.
12. DROP NAMED COLLECTION — for `Drop: true` diffs (after everything that may reference them is gone).

Recreate pairs at position 1 honor the "drop first, then create" invariant
adjacently; they're emitted **before** any other create so dependent
tables can rely on the new collection being present.

`engineSQL` Kafka case:

```go
case EngineKafka:
    if v.Collection != nil {
        return fmt.Sprintf("Kafka(%s)", *v.Collection), nil  // no settings
    }
    out := map[string]string{}
    // typed fields: format each non-nil pointer with the right SQL literal
    // (number bare, bool as 0/1, string single-quoted)
    if v.BrokerList != nil { out["kafka_broker_list"] = "'" + *v.BrokerList + "'" }
    if v.NumConsumers != nil { out["kafka_num_consumers"] = fmt.Sprintf("%d", *v.NumConsumers) }
    if v.CommitOnSelect != nil { out["kafka_commit_on_select"] = boolToSQL(*v.CommitOnSelect) }
    // ... one row per typed field ...
    for k, val := range v.Extra { out[k] = val }
    return "Kafka()", out
```

### CLI — `cmd/hclexp/hclexp.go`

`loadSide` returns `*Schema` instead of `[]DatabaseSpec`. The introspect
subcommand calls both `Introspect(...)` (per database) and
`IntrospectNamedCollections(...)` (once total) and stitches the result
into a `Schema`. `renderChangeSet` gains a top-level NC section printed
once per change set:

```
named_collections
  + named_collection my_kafka
  - named_collection old_kafka
  ~ named_collection prod_kafka
      ~ param kafka_topic_list: changed
      + param kafka_sasl_mechanism: added
      - param kafka_unused
```

The introspect summary log gains `"named_collections", len(schema.NamedCollections)`.

## Security caveat

Unlike dictionary `PASSWORD '[HIDDEN]'`, named-collection values are
**not redacted** in `system.named_collections` — passwords round-trip
plain through introspection. The HCL surface lets users mark a param as
`overridable = false`, but the value itself is visible.

Recommended pattern (documented in the README):

- Commit the base layer with `named_collection "x"` blocks using placeholder
  values (e.g. `value = "[set via override]"`).
- Keep a separate, gitignored layer (or a vault-sourced file at deploy
  time) declaring the same `named_collection "x"` with `override = true`
  and the real values.
- `hclexp -layer schema/base,schema/prod-secrets ...` merges the two; the
  override-layer values reach the cluster, the base layer never sees them.

## Migration cost

This is a breaking change for existing users:

- `ParseFile`'s return type changes from `[]DatabaseSpec` to `*Schema`.
- `LoadLayers`'s return type follows.
- `Write` takes `*Schema` instead of `[]DatabaseSpec`.
- `EngineKafka`'s field set is overhauled: `BrokerList []string` →
  `BrokerList *string` (comma-joined to match ClickHouse's native type);
  `Topic` → `TopicList`; `ConsumerGroup` → `GroupName`. Tests and fixtures
  using `EngineKafka{Topic: "x", ConsumerGroup: "g"}` literally need
  pointer wrapping and field renames.

The plan includes dedicated tasks to migrate `engines_all_kinds.hcl`,
`TestCHLive_HCLIntrospect`, and any caller of `ParseFile`/`Write` with the
old signature.

## Testing

### Unit

- `parser_test.go::TestParseFile_NamedCollection` — parse fixture, assert decoded `Schema.NamedCollections`.
- `parser_test.go::TestParseFile_KafkaWithCollection` — Kafka with `collection`; inline fields nil.
- `parser_test.go::TestParseFile_KafkaInlineSettings` — canonical inline form; typed fields populated; `Extra` populated for an unknown key.
- `resolver_test.go::TestResolve_KafkaEngine_XOR` — both empty errors; both set errors; correct shapes pass.
- `resolver_test.go::TestResolve_NamedCollection_*` — empty Params errors; duplicate Param keys error; unique names across collections.
- `named_collection_introspect_test.go::TestBuildNamedCollectionFromCreateNC_Full` — feed `CREATE NAMED COLLECTION` SQL, assert all fields including mixed `OVERRIDABLE`/`NOT OVERRIDABLE`/default.
- `introspect_test.go::TestParseKafkaEngine_Cases` — table-driven across 4 cases (inline, named collection, legacy positional, mixed-form error).
- `dump_test.go::TestWrite_RoundTrip_NamedCollection` — full round-trip.
- `dump_test.go::TestWrite_RoundTrip_KafkaCollection` — Kafka NC + canonical inline both round-trip with native types preserved.
- `diff_test.go::TestDiff_NamedCollections` — add, drop, SET (value change + add), DELETE (key removal), comment change, ON CLUSTER change → recreate, identical → empty.
- `named_collection_sqlgen_test.go::TestCreateNamedCollectionSQL` — DDL assertion across overridable shapes.
- `named_collection_sqlgen_test.go::TestAlterNamedCollectionSQL` — SET, DELETE, combined.
- `named_collection_sqlgen_test.go::TestRecreateOrdering` — synthesize an NC recreate diff; assert DROP precedes CREATE adjacently at the front of `out.Statements`.
- `named_collection_diff_test.go::TestDiff_ExternalNCs_Ignored` — two schemas where both have the NC marked `external = true` with different params; assert empty diff.
- `named_collection_diff_test.go::TestDiff_ExternalToManaged_Errors` — diff with one side `external = true` and the other side managed; assert error mentioning external↔managed migration.
- `resolver_test.go::TestResolve_KafkaCollectionReference` — Kafka engine references a `collection` that isn't declared in the schema → resolver error; references one that IS declared (managed or external) → passes.
- `dump_test.go::TestWrite_RoundTrip_ExternalNamedCollection` — round-trip a `named_collection { external = true }` block (cluster, comment, empty Params).
- `sqlgen_test.go::TestKafkaEngineSQL_Cases` — table-driven over the 4 emit shapes.

### CLI

- `TestRenderChangeSet_NamedCollections` — top-level NC section with `+/-/~ named_collection ...` lines.
- `TestLoadSide_SchemaReturnType` — existing `loadSide` tests adapt to `*Schema`.

### Live (require `-clickhouse`)

`named_collection_live_test.go`:

- `TestCHLive_NamedCollection_ApplyRoundTrip` — build NC spec → `createNamedCollectionSQL` → exec → `IntrospectNamedCollections` → assert match.
- `TestCHLive_NamedCollection_AlterSetDelete` — create then diff with SET + DELETE; emit ALTER statements; exec; introspect; assert result.
- `TestCHLive_NamedCollection_RecreateOnCluster` — create with cluster; diff with different cluster; emit DROP+CREATE adjacent; exec; introspect.

`kafka_namedcollection_live_test.go` — **the e2e headliner**:

- `TestCHLive_Kafka_WithNamedCollection_E2E` — full end-to-end:
  build NC spec + TableSpec with `EngineKafka{Collection: ptr("my_kafka")}` →
  `GenerateSQL` → exec `CREATE NAMED COLLECTION` then `CREATE TABLE ... ENGINE = Kafka(my_kafka)` →
  `Introspect()` + `IntrospectNamedCollections()` → assert the NC and the
  table both round-trip; the table's engine introspects as
  `EngineKafka{Collection: ptr("my_kafka")}` (the reference, not the
  resolved inline settings).
- `TestCHLive_Kafka_AllSettingsForm` — same e2e shape but using the
  canonical inline form with mixed numeric, bool, and string settings;
  assert typed introspection (numbers come back as int64, bools as bool,
  unknown keys land in `Extra`).

### Fixtures

- `internal/loader/hcl/testdata/named_collection.hcl`
- `internal/loader/hcl/testdata/kafka_with_collection.hcl`
- `internal/loader/hcl/testdata/kafka_inline_settings.hcl`

## Out of scope

- ALTER / DROP parser AST in the clickhouse-sql-parser fork. We emit
  these as plain strings; introspection only ever sees CREATE statements
  via `system.named_collections.create_query`.
- Managing server config-file (XML/YAML) named collections. We read only
  `source = 'DDL'` collections during introspection. Config-file
  collections can still be declared in HCL with `external = true` so
  Kafka references resolve and are validated, but hclexp emits no DDL
  for them — they remain the operator's responsibility to provision via
  the server config file.
- Other engines that consume named collections (S3, MySQL, PostgreSQL,
  Remote, MongoDB). Same pattern applies, but only Kafka is in this
  change.
- Secret-value masking or vault integration. Documented best-practice is
  the override-layer pattern.

## Verification

- `go build ./...` and `go test ./internal/... ./cmd/...` pass after the
  migration of existing tests/fixtures.
- `go test ./internal/loader/hcl -clickhouse` (with `docker compose up -d`):
  every new live test passes, including the e2e Kafka-with-NC test.
- Manual: `hclexp introspect -database <db>` against a cluster that has
  both a named collection and a Kafka table referencing it emits a
  `named_collection` block at the top and an engine `kafka { collection = "..." }`
  block on the table.
- README gains a `### Named collections` section documenting the block,
  the override-layer secret pattern, and the security caveat.
