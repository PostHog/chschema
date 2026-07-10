# Per-object schema comparison for `diff`, `plan`, and `drift`

## Context

The high-level goal spans three repos:

1. **Dumps exist** — `hclexp dump-cluster` captures per-node HCL from every
   production node; `posthog/clickhouse/hcl/dump-live.sh` captures the managed
   satellite roles.
2. **Composed schema must match the dumps** — `posthog/posthog/clickhouse/hcl`
   holds the base layers and per-`(env, role)` goldens; `posthog-cloud-infra`
   (B0 compose harness, plan dated 2026-07-09) vendors that base and layers
   env overrides on top. For dev / prod-us / prod-eu the composed golden must
   match the live dump; locally, the dump must match the local golden.
3. **On mismatch, compare object per object** — today the only structured
   output is `diff -format json`, a flat DDL operation list. The
   `check-live.sh` gate does string/JSON surgery in an inline Python filter to
   get per-object lines and to drop ignored objects. `drift` (node ↔ node)
   emits only prose.

Two comparison axes fall out:

- **golden ↔ dump** (`diff` pairwise, `plan` env-wide against a manifest) —
  prescriptive: the diff says how to make the live side match the golden.
- **node ↔ node within an env** (`drift`) — descriptive: how a drifter
  differs from its group reference.

Both need the same thing: a per-object comparison document with
attribute-level detail, JSON-first, with the human text rendered from the same
model. This design supersedes `2026-07-10-drift-json-output.md` (its
direction decision and bug analysis carry over; its flat per-drifter operation
list is replaced by the shared model).

## Decisions

- **One diff engine, three projections.** The document is a serialization of
  the existing `ChangeSet` — the structs `renderChangeSet` already walks
  (`TableDiff.ModifyColumns`, `SettingsChanged`, `EngineChange`,
  `MaterializedViewDiff.QueryChange`, …). Rejected: a reflect-based
  structural differ at output time (a second diff implementation that can
  contradict the first) and enriching DDL ops in place (objects whose change
  emits no statement would vanish; detail stays trapped in SQL strings).
- **JSON first, text renders from it.** CI/agents consume `objects` directly;
  the terminal renderer consumes the same `[]ObjectComparison`, so the two
  views cannot disagree.
- **Status is right-relative.** `added` = present only on the right side.
  `diff`/`plan` put the desired schema on the right (`added` → will be
  CREATEd); `drift` puts the drifter on the right (descriptive, direction
  unchanged from the superseded plan).
- **Counts derive from the object list.** `CompareSummary` is computed from
  `[]ObjectComparison`, which is computed from the full `ChangeSet` — so the
  raw/named-collection counting gap in drift's `summarizeChanges` (tables,
  MVs, dicts, views only; bare `"changed"` fallback) is fixed structurally,
  not patched. `summarizeChanges` is deleted.
- **Exclusions are native and pre-diff.** `-exclude <file>` (the existing
  `exclude { patterns = [...] }` config) filters both resolved schemas before
  `Diff`, so excluded objects appear in no view and no count. The config
  gains an optional `object_types` list so "ignore all named collections" is
  declarative. The Python `filter_drift` in check-live.sh becomes
  `jq -e '.objects == []'` (follow-up in the posthog repo, not here).

## The model

New `internal/loader/hcl/compare.go`:

```go
// ObjectComparison describes how one object differs between two schemas.
// Status is right-relative: "added" means the right side has the object and
// the left does not.
type ObjectComparison struct {
    Database     string          `json:"database"`    // empty for named collections
    Object       string          `json:"object"`
    ObjectType   string          `json:"object_type"` // table | materialized_view | view | dictionary | named_collection | raw
    Status       string          `json:"status"`      // added | dropped | altered
    Changes      []FieldChange   `json:"changes,omitempty"` // altered only
    Operations   []JSONOperation `json:"operations"`  // the DDL that reconciles this object; may be empty (unsafe-only changes)
    Unsafe       bool            `json:"unsafe"`
    UnsafeReason string          `json:"unsafe_reason,omitempty"`
    Error        string          `json:"error,omitempty"` // unsupported transition (e.g. named collection external↔managed); no DDL emitted
}

// FieldChange is one attribute-level difference on an altered object.
type FieldChange struct {
    Field  string `json:"field"`  // vocabulary below
    Change string `json:"change"` // add | drop | modify | rename
    Old    string `json:"old,omitempty"`
    New    string `json:"new,omitempty"`
}

// CompareSummary counts objects by type and status. Every field carries a
// snake_case JSON tag (tables_added, mvs_dropped, raws_altered,
// named_collections_changed, ...).
type CompareSummary struct {
    TablesAdded, TablesDropped, TablesAltered int
    MVsAdded, MVsDropped, MVsAltered          int
    ViewsAdded, ViewsDropped, ViewsAltered    int
    DictsAdded, DictsDropped, DictsAltered    int
    RawsAdded, RawsDropped, RawsAltered       int
    NamedCollectionsChanged                   int
}
```

`BuildObjectComparisons(cs ChangeSet, gen GeneratedSQL, left, right *Schema)
[]ObjectComparison` walks the ChangeSet exactly as `renderChangeSet` does,
assigns each generated op to its object by `(database, object)` (nested ops
keep their global `order`), and derives `Unsafe` from `gen.Unsafe` (as
`unsafeFor` does) plus `RawChange.IsUnsafe`. Old/new values render via the
existing HCL/SQL printers: a column as its DDL fragment, `order_by` as the
expression string, a query as its canonical beautified form.

### `field` vocabulary (public contract, documented in README.hcl.md)

| Field | Applies to |
|---|---|
| `column:<name>` | table, dictionary |
| `index:<name>`, `projection:<name>`, `constraint:<name>` | table |
| `setting:<name>` | table |
| `engine`, `order_by`, `partition_by`, `sample_by`, `primary_key`, `ttl` | table |
| `comment` | table, named collection |
| `query` | view, materialized view |
| `to_table`, `columns` | materialized view (structural → recreate) |
| `param:<name>`, `on_cluster` | named collection |
| `sql` | raw block |

Rename renders as `Change: "rename"`, `Old` = old name, `New` = new name on
`column:<new-name>`. A dictionary diff (path-list `Changed`) maps each path to
one `FieldChange` with `Change: "modify"`. Named-collection mapping: `Add` →
`added`, `Drop` → `dropped`, `Recreate` → `altered` with an `on_cluster`
change; `SetParams`/`DeleteParams` → `param:<name>` add/modify/drop; a
`SkippedRedactedParams` entry surfaces as `param:<name>` with `Old`/`New` =
`[HIDDEN]` (the diff could not verify equality — same signal the CLI prints
today); `NamedCollectionChange.Error` fills `ObjectComparison.Error`.

## `diff` integration

`DiffJSON` grows additively — current consumers (check-live.sh) are unbroken:

```go
type DiffJSON struct {
    Objects    []ObjectComparison `json:"objects"`
    Operations []JSONOperation    `json:"operations"`       // unchanged, global dependency order
    Unsafe     []JSONUnsafe       `json:"unsafe,omitempty"` // unchanged
    Summary    CompareSummary     `json:"summary"`
}
```

`RenderDiffJSON` gains the `ChangeSet` parameter:
`RenderDiffJSON(cs, gen, left, right)`. Objects with no statement (e.g. an
engine change flagged unsafe) still appear in `objects` with their `Changes`
and `Unsafe` — a gap the flat op list has today.

## `plan` integration

`BuildPlan` already computes `Diff(rd.Current, rd.Desired)` per role
(`plan.go:64`); it now also builds each role's object list:

```go
type PlanResult struct {
    Operations []PlanOperation  `json:"operations"` // unchanged
    Unsafe     []JSONUnsafe     `json:"unsafe,omitempty"`
    Roles      []RoleComparison `json:"roles"`
}

type RoleComparison struct {
    Role    string             `json:"role"`
    Objects []ObjectComparison `json:"objects"`
    Summary CompareSummary     `json:"summary"`
}
```

Deliberately **not** deduped across roles (unlike the global op list): triage
is per `(env, role)`, so a shared object drifting on two roles appears under
both. Nested ops carry the global plan `order` of the matching merged op
(matched by kind + database + object + SQL); ops deduped across roles share
that order value.

## `drift` rework

Structure from the superseded plan, payload swapped to the shared model:

```go
type DriftNode struct {
    Node    string             `json:"node"`
    File    string             `json:"file"`
    Macros  map[string]string  `json:"macros,omitempty"`
    Objects []ObjectComparison `json:"objects"`
    Summary CompareSummary     `json:"summary"`
}

type DriftGroup struct {
    Key       string      `json:"key"`
    Reference string      `json:"reference"`
    Nodes     int         `json:"nodes"`
    Drifters  []DriftNode `json:"drifters"`
}

type DriftJSON struct {
    Groups  []DriftGroup    `json:"groups"`
    Summary DriftRunSummary `json:"summary"`
}

// DriftRunSummary: nodes, groups, groups_with_drift, drifting_nodes (ints,
// snake_case tags).
```

Carried over unchanged: `-format text|json` validated like `-zk-paths`
(reject otherwise, exit 2); JSON to stdout, diagnostics to stderr (text-mode
group prose is suppressed in JSON mode); exit non-zero iff any node drifts;
descriptive reference → drifter direction. The text one-liner
(`~3 table, +1 mv`) renders from `CompareSummary`; `summarizeChanges` is
deleted.

## `-exclude` on `diff`, `plan`, `drift`

All three accept `-exclude <file>`, reusing `LoadExcludeConfig` /
`ExcludeMatcher` (globs matched against `name` and `db.name`, exactly as
`introspect`/`dump-cluster`). A new `FilterSchema(*Schema, *ExcludeMatcher)`
drops matched objects from both resolved schemas before `Diff`. The exclude
config gains an optional `object_types = [...]` attribute (values from the
`object_type` vocabulary); a listed type is dropped wholesale. `drift` gets
the flag for symmetry — dumps are usually pre-filtered at capture time, but
re-filtering covers dumps captured before an exclude rule existed.

## Text rendering

One renderer, `renderObjectComparisons(w, []ObjectComparison)`, replaces the
per-command walkers: `diff` text mode and `drift -details` consume it;
`renderChangeSet` retires. Output stays visually equivalent (`+`/`-`/`~` per
object, indented field changes); small wording diffs are acceptable and
reviewed via the snapshot update.

## Testing

- **Unit — `BuildObjectComparisons`**: per object kind (table with
  column/setting/engine/order_by changes incl. rename; MV query-only vs
  recreate; view; dictionary; raw; named collection). Testify, whole-struct
  comparison against expected `[]FieldChange` / `[]ObjectComparison`.
- **JSON shape**: fixture-pair diff unmarshalled and compared against a
  fully-constructed `DiffJSON`; one `plan` manifest fixture; one `drift`
  fixture dir.
- **Regression** (from the superseded plan): raw-only and
  named-collection-only drifters yield non-zero summary counts and a
  non-`"changed"` one-liner; direction test pins `added` = right-side-only;
  `drift -format yaml` exits 2.
- **Exclude**: a diff whose only difference is an excluded object yields
  empty `objects`, empty `operations`, exit 0; `object_types =
  ["named_collection"]` drops a named-collection-only diff.
- **Unsafe-only object**: an engine change appears in `objects` with
  `Unsafe: true` and empty `Operations`.
- **Named-collection edges**: a redacted param surfaces as `param:<name>`
  with `[HIDDEN]` on both sides; an external↔managed transition fills
  `ObjectComparison.Error` and emits no operations.
- Snapshot tests pin the shared text renderer.

```bash
go test ./internal/... -v
go test ./test -v
go build -o hclexp ./cmd/hclexp
./hclexp diff -left golden.hcl -right dump.hcl -format json | jq '.summary'
./hclexp drift -dir nodes/ -format json | jq '.groups[].drifters[].summary'
```

## Docs

- `docs/README.hcl.md`: the `field` vocabulary table, the right-relative
  status rule, `-exclude` on diff/plan/drift, `object_types`.
- `CLAUDE.md`: update the diff/plan/drift bullets.
- `cmd/hclexp/hclexp.go` usage text: mention `-format` on drift.
- Mark `docs/plans/2026-07-10-drift-json-output.md` superseded with a pointer
  here.

## Out of scope

- check-live.sh simplification (posthog repo follow-up).
- `hclexp web` diff view (possible later consumer of `[]ObjectComparison`).
- Inverting drift operations into a reconcile script (rejected in the
  superseded plan; rationale unchanged).
