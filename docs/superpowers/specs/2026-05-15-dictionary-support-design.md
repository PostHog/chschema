# Dictionary support in the HCL package

## Context

`hclexp` (the `internal/loader/hcl` package + `cmd/hclexp` CLI) is the
future of this project; the legacy `chschema` binary, its protobuf
contracts, and the YAML format are deprecated. The HCL package today
models tables and (after the materialized-view change) materialized
views. It does not model **dictionaries** at all — so dictionaries
cannot be expressed in HCL, dumped, diffed, or generated, and
`hcl.Introspect` skips them silently.

The legacy `chschema` did capture dictionaries, but only partially: it
records the layout kind, primary key, attributes (name + type only),
and a raw unparsed `source_config` string. Modifiers like `DEFAULT`,
`EXPRESSION`, `HIERARCHICAL`, `INJECTIVE`, `IS_OBJECT_ID`, layout
parameters, the `RANGE` clause, `ON CLUSTER`, `SETTINGS`, and `COMMENT`
are all dropped. That model is not enough to round-trip the PostHog
dictionaries.

This change makes dictionaries first-class in the HCL package: a new
`DictionarySpec` that parses from HCL, is introspected from a live
cluster, dumps back to HCL, diffs, and generates DDL — the same
round-trip tables and materialized views already have.

Scope is the **common subset** of ClickHouse dictionary sources and
layouts (the seven sources and ten layouts listed below). The other
~25 source/layout kinds aren't in scope; they fail introspection with
a clear, specific error rather than being silently mishandled, and the
framework is extensible — adding a kind later is a new typed struct and
one switch case.

## Approach

Dictionaries are modeled as a **third sibling collection** on
`DatabaseSpec`, parallel to `Tables` and `MaterializedViews` — not as
a flavored table. Dictionaries are a genuinely distinct ClickHouse
object type. They do **not** participate in the table inheritance
system (`extend` / `abstract` / `patch_table`).

`SOURCE(...)` and `LAYOUT(...)` are modeled **typed per kind** (one Go
struct + HCL block per source/layout kind), mirroring how engines are
modeled today (`engines.go` + `DecodeEngine`). This trades a moderate
amount of code (~17 small structs) for parse-time validation, IDE
autocompletion, and clearer dumps.

Diff handling is simple: ClickHouse has no useful in-place
`ALTER DICTIONARY`, so **any non-empty diff produces a `CREATE OR
REPLACE DICTIONARY` statement**. This is the idiomatic ClickHouse
update path and is treated as safe, not as an unsafe recreate.

## Data model

`internal/loader/hcl/types.go` — new types:

```go
type DictionarySpec struct {
    Name       string                 `hcl:"name,label"`
    PrimaryKey []string               `hcl:"primary_key"`
    Attributes []DictionaryAttribute  `hcl:"attribute,block"`
    Source     *DictionarySourceSpec  `hcl:"source,block"`       // exactly one
    Layout     *DictionaryLayoutSpec  `hcl:"layout,block"`       // exactly one
    Lifetime   *DictionaryLifetime    `hcl:"lifetime,block"`     // optional
    Range      *DictionaryRange       `hcl:"range,block"`        // optional; only for range_hashed / complex_key_range_hashed
    Settings   map[string]string      `hcl:"settings,optional"`  // dictionary-level SETTINGS
    Cluster    *string                `hcl:"cluster,optional"`
    Comment    *string                `hcl:"comment,optional"`
}

type DictionaryAttribute struct {
    Name         string  `hcl:"name,label"`
    Type         string  `hcl:"type"`
    Default      *string `hcl:"default,optional"`
    Expression   *string `hcl:"expression,optional"`
    Hierarchical bool    `hcl:"hierarchical,optional"`
    Injective    bool    `hcl:"injective,optional"`
    IsObjectID   bool    `hcl:"is_object_id,optional"`
}

type DictionaryLifetime struct {
    Min *int64 `hcl:"min,optional"`
    Max *int64 `hcl:"max,optional"`
    // LIFETIME(n) simple form: only Min set (or, equivalently, both
    // Min and Max set to the same value). LIFETIME(MIN x MAX y): both set.
}

type DictionaryRange struct {
    Min string `hcl:"min"`
    Max string `hcl:"max"`
}

// Mirrors EngineSpec: Kind label + opaque hcl.Body + decoded typed value.
type DictionarySourceSpec struct {
    Kind    string   `hcl:"kind,label"  diff:"-"`
    Body    hcl.Body `hcl:",remain"     diff:"-"`
    Decoded DictionarySource          // populated by DecodeDictionarySource post-parse
}
type DictionarySource interface{ Kind() string }

type DictionaryLayoutSpec struct {
    Kind    string   `hcl:"kind,label"  diff:"-"`
    Body    hcl.Body `hcl:",remain"     diff:"-"`
    Decoded DictionaryLayout
}
type DictionaryLayout interface{ Kind() string }
```

`DatabaseSpec` gains:

```go
Dictionaries []DictionarySpec `hcl:"dictionary,block"`
```

### Supported source kinds (typed structs in `dictionary_sources.go`)

| Kind          | HCL fields |
|---------------|------------|
| `clickhouse`  | `host`, `port`, `user`, `password`, `db`, `table`, `query`, `invalidate_query`, `update_field`, `update_lag` |
| `mysql`       | `host`, `port`, `user`, `password`, `db`, `table`, `query`, `invalidate_query`, `update_field`, `update_lag` |
| `postgresql`  | `host`, `port`, `user`, `password`, `db`, `table`, `query`, `invalidate_query`, `update_field`, `update_lag` |
| `http`        | `url`, `format`, `credentials_user`, `credentials_password`, optional `headers` |
| `file`        | `path`, `format` |
| `executable`  | `command`, `format`, optional `implicit_key` |
| `null`        | (no fields) |

All non-required fields are `*string` / `*int64`, so absent attributes round-trip cleanly.

### Supported layout kinds (typed structs in `dictionary_layouts.go`)

| Kind                           | HCL fields |
|--------------------------------|------------|
| `flat`                         | (none) |
| `hashed`                       | (none) |
| `sparse_hashed`                | (none) |
| `complex_key_hashed`           | optional `preallocate *int64` |
| `complex_key_sparse_hashed`    | (none) |
| `range_hashed`                 | optional `range_lookup_strategy *string` |
| `complex_key_range_hashed`     | optional `range_lookup_strategy *string` |
| `cache`                        | `size_in_cells int64` |
| `ip_trie`                      | optional `access_to_key_from_attributes bool` |
| `direct`                       | (none) |

Unsupported source/layout kinds (encountered during introspection)
return `unsupported dictionary source kind: <name>` /
`unsupported dictionary layout kind: <name>`.

### HCL block

```hcl
dictionary "exchange_rate_dict" {
  primary_key = ["currency"]
  lifetime { min = 3000  max = 3600 }
  range    { min = "start_date"  max = "end_date" }

  attribute "currency"   { type = "String" }
  attribute "start_date" { type = "Date" }
  attribute "end_date"   { type = "Nullable(Date)" }
  attribute "rate"       { type = "Decimal64(10)" }

  source "clickhouse" {
    query    = "SELECT currency, ... FROM exchange_rate"
    user     = "default"
    password = "[HIDDEN]"
  }
  layout "complex_key_range_hashed" {
    range_lookup_strategy = "max"
  }
}
```

## Components

### Parsing — `parser.go`

Purely additive: `gohcl` decodes the new `dictionary` block
automatically once the struct tag is on `DatabaseSpec`. After the
initial decode, `ParseFile` walks every dictionary and runs
`DecodeDictionarySource(spec.Source)` and
`DecodeDictionaryLayout(spec.Layout)`, populating
`spec.Source.Decoded` / `spec.Layout.Decoded` (the same way it already
calls `DecodeEngine` on each table's engine block).

Validation in `parser.go` / `resolver.go`:
- exactly one `source` and one `layout` block,
- non-empty `primary_key`,
- `range` block only allowed for `range_hashed` and
  `complex_key_range_hashed` layouts.

No resolver changes for inheritance — dictionaries are not part of
`extend` / `abstract` / `patch_table`.

### Source / layout dispatch — `dictionary_sources.go`, `dictionary_layouts.go`

Mirrors `engines.go::DecodeEngine`:

```go
func DecodeDictionarySource(spec *DictionarySourceSpec) (DictionarySource, error) {
    switch spec.Kind {
    case "clickhouse": var s SourceClickHouse; return s, gohcl.DecodeBody(spec.Body, nil, &s)
    case "mysql":      var s SourceMySQL;      ...
    case "postgresql": var s SourcePostgreSQL; ...
    case "http":       var s SourceHTTP;       ...
    case "file":       var s SourceFile;       ...
    case "executable": var s SourceExecutable; ...
    case "null":       return SourceNull{}, nil
    default: return nil, fmt.Errorf("unsupported dictionary source kind %q", spec.Kind)
    }
}
```

Same shape for `DecodeDictionaryLayout`.

### Introspection — `introspect.go`

`Introspect` already dispatches on statement type for `CreateTable`,
`CreateMaterializedView`, `CreateView`. Add a fourth case:

```go
case *chparser.CreateDictionary:
    d, err := buildDictionaryFromAST(s)
    if err != nil {
        return nil, fmt.Errorf("introspect dictionary %s.%s: %w", database, name, err)
    }
    d.Name = name
    db.Dictionaries = append(db.Dictionaries, d)
```

New helper `buildDictionaryFromAST(*chparser.CreateDictionary) (DictionarySpec, error)` walks the AST:

- `Schema.Attributes` → `Attributes` (mapping `Name`, `Type`, and the
  `Default` / `Expression` / `Hierarchical` / `Injective` /
  `IsObjectId` flags).
- `Engine.PrimaryKey.Keys` → `PrimaryKey []string` (via the existing
  `flattenTupleExpr` / `exprList` helpers).
- `Engine.Lifetime` → `*DictionaryLifetime` (handle both
  `LIFETIME(n)` simple form and `LIFETIME(MIN x MAX y)` range form).
- `Engine.Range` → `*DictionaryRange`.
- `Engine.Source` → typed source: switch on
  `lower(Source.Source.Name)`, iterate `Args` `(name, value)` pairs,
  call a per-kind setter that assigns into the matching struct field;
  unknown args produce a clear error.
- `Engine.Layout` → typed layout: same dispatch.
- `Engine.Settings` → `Settings map[string]string` (reusing the
  existing settings extractor).
- `OnCluster`, `Comment` → mapped exactly as for MVs.

The kind name comparisons are case-insensitive — ClickHouse accepts
both `CLICKHOUSE` and `clickhouse` etc. — and HCL stores the canonical
lowercase kind.

### Dump — `dump.go`

New `writeDictionary` called from `writeDatabase` after MVs, sorted by
name. Emits the `dictionary` block, then attribute sub-blocks, then
`source` / `layout` / `lifetime` / `range` sub-blocks. Source/layout
bodies are emitted via `writeDictionarySource(body, decoded)` /
`writeDictionaryLayout` — a case switch per kind, mirroring
`writeEngine`.

### Diff — `diff.go`

`DatabaseChange` gains three fields, mirroring the table and MV ones:

```go
AddDictionaries   []DictionarySpec
DropDictionaries  []string
AlterDictionaries []DictionaryDiff
```

```go
type DictionaryDiff struct {
    Name    string
    Changed []string  // human-readable list of field paths that differ; for rendering only
}

func (d DictionaryDiff) IsEmpty() bool  { return len(d.Changed) == 0 }
func (d DictionaryDiff) IsUnsafe() bool { return false }
```

`diffDictionary(from, to *DictionarySpec)` walks the spec field by
field; any difference (attributes, primary key, source, layout,
lifetime, range, settings, cluster, comment) adds the field path to
`Changed`. Source / layout comparison uses
`reflect.DeepEqual(from.Source.Decoded, to.Source.Decoded)` (the raw
`Body` and `Kind` are `diff:"-"`, the decoded value is what counts).

`diffDatabase` indexes dictionaries by name and produces add / drop /
alter entries. `DatabaseChange.IsEmpty()` and `sortDatabaseChange` are
extended. The `Diff` whole-database add / remove shortcut carries
dictionaries through (`diffDatabase` against an empty
`&DatabaseSpec{Name: name}` is the cleanest way — same fix the MV work
applied).

### SQL generation — `sqlgen.go`, `dictionary_sqlgen.go`

`GenerateSQL` statement order extends to:

1. CREATE TABLE
2. CREATE MATERIALIZED VIEW
3. **CREATE OR REPLACE DICTIONARY** (adds + alters; the same DDL covers both)
4. ALTER TABLE
5. ALTER MATERIALIZED VIEW (`MODIFY QUERY`)
6. DROP MATERIALIZED VIEW
7. **DROP DICTIONARY**
8. DROP TABLE

Dictionaries sit between MVs and tables in both directions because
they can reference tables (via `SOURCE(CLICKHOUSE(TABLE 'x'))`) but
tables don't depend on dictionaries by schema.

New helpers in `dictionary_sqlgen.go`:

- `createDictionarySQL(database string, d DictionarySpec) string` —
  emits the full `CREATE OR REPLACE DICTIONARY` statement.
- `dropDictionarySQL(database, name string) string` →
  `DROP DICTIONARY <db>.<name>`.
- `sourceSQL(s DictionarySource) string`,
  `layoutSQL(l DictionaryLayout) string` — render
  `SOURCE(<KIND>(<args>))` and `LAYOUT(<KIND>(<args>))`. Args follow
  the ClickHouse convention of space-separated `NAME 'value'` /
  `NAME value` pairs (strings quoted, numbers bare).

Example generated DDL for the `exchange_rate_dict` round-trip:

```sql
CREATE OR REPLACE DICTIONARY default.exchange_rate_dict
(
  `currency` String,
  `start_date` Date,
  `end_date` Nullable(Date),
  `rate` Decimal64(10)
)
PRIMARY KEY currency
SOURCE(CLICKHOUSE(QUERY 'SELECT ...' USER 'default' PASSWORD '[HIDDEN]'))
LAYOUT(COMPLEX_KEY_RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'max'))
LIFETIME(MIN 3000 MAX 3600)
RANGE(MIN start_date MAX end_date)
```

### CLI — `cmd/hclexp/hclexp.go`

`renderChangeSet` gains `+ / - / ~ dictionary ...` lines (alter
entries list the changed-field paths from `DictionaryDiff.Changed`).
The `introspect` summary `slog.Info` line gains a `"dictionaries"
len(spec.Dictionaries)` attribute, matching what the MV work added for
materialized views.

## Apply-time caveat

A round-tripped dictionary with `PASSWORD '[HIDDEN]'` will fail to
load data from its source if applied verbatim — the literal
`[HIDDEN]` isn't a usable secret. This is a property of ClickHouse's
`system.tables.create_table_query` redaction, not of the HCL model.
The README will note this and recommend editing dumped HCL before
deploying to a real environment, or wiring secrets through some
out-of-band mechanism. Secret-handling beyond pass-through is a future
follow-up.

## Testing

- **parser**: parse an HCL fixture containing a `dictionary` block
  (the `exchange_rate_dict` shape — clickhouse source, range-hashed
  layout, range clause, lifetime, every optional field set); assert
  the decoded `DictionarySpec`.
- **dictionary_sources_test.go**: table-driven `TestDecodeDictionarySource` covering each of
  the 7 supported source kinds + an unsupported-kind error.
- **dictionary_layouts_test.go**: same shape, 10 layouts + unsupported error.
- **introspect**: `TestBuildDictionaryFromAST_Full` feeds the parser a
  real `CREATE DICTIONARY` SQL (the `exchange_rate_dict` text) and
  asserts every `DictionarySpec` field; a second test asserts
  unsupported source / layout kinds return a clear error. The existing
  `TestProcessIntrospectRows_Dispatch` is extended with a fourth row
  (a `CREATE DICTIONARY`) and asserts it lands in `db.Dictionaries`.
- **dump**: round-trip — parse → `Write` → parse, assert
  `DictionarySpec` is unchanged. Added to the existing shared
  `roundTrip()` helper.
- **diff**: `TestDiff_Dictionaries` covers add / drop / change-anything
  → recreate, plus `DictionaryDiff.IsEmpty` / `IsUnsafe` and
  `DatabaseChange.IsEmpty` extension.
- **sqlgen**: table-driven, asserts the exact `CREATE OR REPLACE
  DICTIONARY` DDL for the `exchange_rate_dict` round-trip, a simple
  `flat`/`hashed` case, and a drop.
- **CLI**: `TestRenderChangeSet_Dictionaries` builds a `ChangeSet`
  with add / drop / alter dictionary entries and asserts exact
  rendered output (mirrors the MV test).
- **live** (`-clickhouse`): a sibling to
  `TestCHLive_IntrospectMaterializedView` — create a source table and
  a TO-form `CLICKHOUSE`-source `HASHED`-layout dictionary against a
  real ClickHouse instance, introspect, and assert the dictionary
  round-trips.

## Out of scope

These must fail with a clear, specific error during introspection
(not be silently mishandled), and are documented as unsupported in
the README:

- source kinds: odbc, mongodb, redis, cassandra, ytsaurus,
  yamlregexptree, executable_pool, and any other not listed above
- layout kinds: hashed_array, complex_key_hashed_array, ssd_cache,
  complex_key_ssd_cache, complex_key_cache, complex_key_direct, and
  any other not listed above
- Adding a new kind later is a one-struct + one-switch-case change.

## Verification

- `go build ./...` and `go test ./internal/... ./cmd/...` pass.
- `go test ./internal/loader/hcl -clickhouse` (with
  `docker compose up -d`): the new live dictionary round-trip test
  passes.
- Manual: `hclexp introspect -database <db>` against a database
  containing a dictionary emits a `dictionary` block; `hclexp diff`
  between two HCL files differing in a dictionary reports the change
  and `-sql` emits a `CREATE OR REPLACE DICTIONARY` statement.
- README gains a `### Dictionaries` section documenting the block, the
  supported source / layout kinds, and the out-of-scope list and
  `[HIDDEN]` caveat.
