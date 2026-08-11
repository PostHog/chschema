# Issue #75 — reference-gated pruning for subset schemas

Status: **proposed**. Issue: [#75](https://github.com/PostHog/chschema/issues/75).

## Goal

Allow `diff` and cross-role `plan` to reconcile a deliberately partial desired
schema against a full live schema without dropping objects chschema has never
owned, while still emitting a `DROP` when an object was present in the previous
committed desired schema and has now been deliberately removed.

The reference is the previous desired schema, normally loaded from the merge
base or prior deployed revision. It is an ownership record, not a third source
of field values:

```text
ADD/ALTER = reconcile current objects to desired objects
DROP      = objects present in reference but absent from desired
IGNORE    = current-only objects absent from both reference and desired
```

This is object-level ownership. Once a table is owned, its columns, indexes,
projections, constraints, engine, TTL, settings, and other modeled fields
remain fully authoritative in desired HCL.

## Current behavior

`Diff(from, to)` is a conventional two-way, whole-schema diff. Every object
present only in `from` becomes a drop. `BuildPlan` calls it directly as
`Diff(rd.Current, rd.Desired)`, so a full topology dump compared with a subset
composition emits executable drops for every unmanaged live-only object.

PR #139 added `-exclude` to `diff`, `plan`, and `drift`, filtering both sides
before comparison and allowing whole object types such as `named_collection`
to be ignored. That solves known exclusions, but it is not an ownership model:
every unmanaged ad-hoc or other-team object must be enumerated in advance.

## Decision

Do not build a second three-way field-diff engine. Scope the current schema to
the object identities owned by either desired or reference, then use the
existing `Diff` unchanged:

```text
owned          = identities(desired) union identities(reference)
scoped_current = current intersect owned
changes        = Diff(scoped_current, desired)
```

This gives the required behavior:

| Reference | Desired | Current | Result |
|---|---|---|---|
| absent | absent | present | ignore: never owned |
| present | absent | present | drop: intentionally removed |
| present | absent | absent | no operation: already gone |
| absent/present | present | absent | create |
| absent/present | present | different | alter or replace |
| absent/present | present | equal | no operation |

The desired side participates in `owned` so a newly declared object can adopt
and reconcile an object that already happens to exist live.

### Identity rules

- Database objects are keyed by `(database, name)` across modeled tables,
  materialized views, views, dictionaries, and `raw` representations. ClickHouse
  objects can move between a raw and modeled representation without falling out
  of the ownership scope; the ordinary diff still emits the required
  drop/create transition.
- Named collections use a separate top-level namespace keyed by name.
- A named collection declared `external = true` is a reference, not an owned
  object. It never enters the ownership set from either desired or reference,
  so removing an external declaration cannot schedule a collection drop.
- Database blocks and node metadata are not ownership units. Empty database
  wrappers stay available while their object slices are filtered; node metadata
  is preserved.

### Compatibility and safety

- `Diff(from, to)` remains unchanged.
- Omitting all reference flags preserves today's full two-way prune behavior.
  This is necessary for committed-desired-to-working-desired migrations and
  existing consumers.
- Supplying an explicit empty reference means "no previously owned objects":
  current is scoped to desired, so current-only objects are ignored.
- `-exclude` is applied to current, desired, and reference before ownership is
  calculated. An excluded object must not be reintroduced as owned by the
  reference.
- Output JSON and operation shapes do not change. Reference gating changes
  which comparisons and operations exist, not their serialization.
- This work does not change the existing `unsafe` classification of an
  intentional table drop. Any broader destructive-operation approval policy is
  separate from ownership/prune selection.

## CLI contract

### `diff`

Add an optional `-reference` side using the same HCL file/directory/layer-stack
forms as `-left` and `-right`:

```bash
hclexp diff \
  -left clickhouse://user@host:9000/posthog \
  -right ./schema/current \
  -reference ./schema/previous \
  -exclude exclude.hcl \
  -format json
```

Direction is explicit:

- `left` is current;
- `right` is desired;
- `reference` is the previous desired schema whose object identities establish
  prior ownership.

Without `-reference`, `diff` continues to mean the exact transformation from
left to right and may drop every left-only object.

### `plan`

The reference for a cross-role plan is another manifest composition. Add:

- `-reference-manifest <file>` — previous manifest; activates reference-gated
  pruning;
- `-reference-layer-root <dir>` — root for that manifest's layer paths;
  defaults to the reference manifest's directory.

Example using a temporary worktree for the prior deployed revision:

```bash
git worktree add /tmp/chschema-reference "$DEPLOYED_SHA"

hclexp plan \
  -manifest schema/manifest.hcl \
  -layer-root schema \
  -reference-manifest /tmp/chschema-reference/schema/manifest.hcl \
  -reference-layer-root /tmp/chschema-reference/schema \
  -env prod-us \
  -dump ./topology/prod-us \
  -exclude schema/exclude.hcl \
  -format json
```

Reference handling is per desired role:

- a role present in desired and reference loads both compositions;
- a desired role absent from the reference gets an explicit empty reference,
  making it newly managed without pruning unrelated live objects;
- a role present only in the reference is not planned, because it is no longer
  an execution target in the desired manifest. Decommissioning a role is a
  deployment operation, not schema pruning on a role `plan` no longer targets;
- malformed or duplicate roles in the reference manifest remain hard errors;
- `-reference-layer-root` without `-reference-manifest` is a usage error.

There is deliberately no built-in Git fetch/show logic. The caller resolves the
deployed revision into files; hclexp stays deterministic and filesystem-based.

## Implementation plan

### Task 1: Add a non-mutating ownership scope helper

**Files:**

- Create `internal/loader/hcl/ownership.go`.
- Create `internal/loader/hcl/ownership_test.go`.

- [ ] Define an internal identity type with separate database-object and
  named-collection namespaces.
- [ ] Collect identities from desired and reference, omitting external named
  collections.
- [ ] Implement:

  ```go
  func ScopeCurrentToOwnedObjects(
      current, desired, reference *Schema,
  ) *Schema
  ```

  It returns a schema containing only owned current objects and must not mutate
  any input. Copy the `Schema`, `Databases`, and filtered object slices; the
  object specs themselves can be shared because the helper does not edit them.
- [ ] Preserve database wrappers and node metadata.
- [ ] Match raw and modeled representations by logical database/name identity,
  not by Go slice or HCL block kind.

Tests:

- [ ] unmanaged current-only objects of every modeled kind are removed;
- [ ] desired objects and reference-owned objects are retained;
- [ ] raw-to-modeled and modeled-to-raw transitions stay in scope;
- [ ] managed named collections are retained;
- [ ] external named collections in desired/reference never establish
  ownership;
- [ ] the returned schema preserves database/node metadata;
- [ ] input schemas and backing slices are unchanged.

### Task 2: Make `BuildPlan` reference-aware

**Files:**

- Modify `internal/loader/hcl/plan.go`.
- Extend `internal/loader/hcl/plan_test.go`.

- [ ] Add `Reference *Schema` to `RoleDiff`. `nil` means legacy full-prune
  behavior; a non-nil empty schema means reference gating with no prior
  ownership.
- [ ] For each role, derive an effective current schema with
  `ScopeCurrentToOwnedObjects` only when `Reference != nil`.
- [ ] Feed that same effective current schema into `Diff`,
  `BuildObjectComparisons`, engine metadata lookup, and dependency ordering so
  the per-role object view and global operations cannot disagree.
- [ ] Build CREATE/ALTER dependency ranks from the merged desired schemas.
- [ ] Build DROP dependency ranks from the merged effective-current schemas.
  Removed objects are absent from desired, so ranking drops from desired alone
  cannot order their dependencies correctly.
- [ ] Populate table `engine`/`replicated` metadata from desired for
  CREATE/ALTER and effective current for DROP. A removed table is absent from
  desired but its current engine still matters to executors.

Tests:

- [ ] nil reference preserves the existing live-only DROP behavior;
- [ ] empty reference ignores live-only objects;
- [ ] a reference-owned object removed from desired produces one DROP;
- [ ] desired/live differences still produce ALTER/replace operations;
- [ ] desired objects missing live still produce CREATE operations;
- [ ] identical operations still deduplicate across roles and retain the union
  of role provenance;
- [ ] a removed dependent object is dropped before its removed dependency,
  including a cross-role dependency;
- [ ] a dropped replicated table keeps correct `engine` and `replicated`
  metadata;
- [ ] role-level comparisons and summaries omit unmanaged live-only objects.

### Task 3: Add `diff -reference`

**Files:**

- Modify `cmd/hclexp/hclexp.go`.
- Extend `cmd/hclexp/hclexp_test.go`.

- [ ] Register and document the optional `-reference` flag.
- [ ] Load it through `loadSide`, retaining the distinction between an omitted
  flag and a loaded empty schema.
- [ ] Apply `-exclude` to left/current, right/desired, and reference.
- [ ] Scope left/current before calling the unchanged `Diff`.
- [ ] Pass scoped current into `RenderDiffJSON` and text comparison rendering,
  ensuring unmanaged objects disappear from `objects`, `operations`, counts,
  and human-readable output together.

Tests:

- [ ] reproduce issue #75: current has managed + unmanaged, desired/reference
  have managed; result is empty;
- [ ] removing an object from desired while it remains in reference/current
  emits its DROP;
- [ ] adoption: a new desired object already exists live with drift and is
  altered rather than ignored;
- [ ] reference plus `-exclude` cannot reintroduce an excluded drop;
- [ ] omitted `-reference` retains exact two-way output;
- [ ] malformed/unloadable reference reports the side and exits non-zero.

### Task 4: Load previous role compositions in `plan`

**Files:**

- Modify `cmd/hclexp/plan.go`.
- Extend `cmd/hclexp/plan_test.go`.

- [ ] Add `-reference-manifest` and `-reference-layer-root` flags with the
  validation rules above.
- [ ] Refactor manifest environment selection so the desired manifest remains
  strict (at least one deployed role), while a reference manifest may validly
  contain no block for a newly introduced environment/role.
- [ ] Index reference roles by name and compose each matching reference stack
  from the reference layer root.
- [ ] Attach a non-nil reference schema to every desired `RoleDiff` when
  reference mode is active; use an empty schema for roles absent from the old
  manifest.
- [ ] Apply `-exclude` to all three per-role schemas before constructing
  `RoleDiff`.
- [ ] Keep desired-only role iteration and existing current-dump representative
  selection unchanged.

Tests:

- [ ] exact issue fixture through real manifest/layer/dump HCL produces no
  unmanaged DROP;
- [ ] an object removed between reference and desired produces a DROP with the
  correct role;
- [ ] manifest layer-list changes are respected by loading the previous
  manifest, rather than applying the current manifest to old files;
- [ ] a new desired role receives an empty reference and does not prune its
  unrelated live objects;
- [ ] a reference-only role emits no operations;
- [ ] default reference root is the reference manifest directory;
- [ ] missing/malformed reference layers, duplicate roles, and invalid flag
  combinations produce precise errors.

### Task 5: Document ownership and operational usage

**Files:**

- Modify `README.md` (`diff` flags and cross-role planning).
- Modify `docs/README.hcl.md` (comparison/exclude and `plan` reference).
- Modify `docs/concept.md` (desired subsets and prior-desired ownership).
- Optionally add a small runnable fixture under `examples/manifest/` if the
  existing example cannot show the previous/current roots clearly.

- [ ] State prominently that ordinary two-way diff treats the right side as a
  complete authoritative schema and may drop every left-only object.
- [ ] Explain reference-gated ownership with the three sets and truth table.
- [ ] Explain that the reference must be the previously deployed desired
  composition, not a previous live dump.
- [ ] Explain object-level authority: reference gating ignores whole unmanaged
  objects, not extra columns inside managed tables.
- [ ] Show the Git worktree/merge-base workflow and both CLI examples.
- [ ] Explain how `-exclude` composes with reference gating and why external
  named collections are never owned.
- [ ] State that a removed role is not a schema-prune target.

### Task 6: Verification

- [ ] `gofmt -s -l .` produces no output.
- [ ] `go test ./internal/loader/hcl` passes.
- [ ] `go test ./internal/... ./cmd/...` passes.
- [ ] `go test ./test/...` passes.
- [ ] `go test -race ./internal/... ./cmd/...` passes.
- [ ] `go vet ./...` passes.
- [ ] `git diff --check` passes.
- [ ] Build the CLI and smoke-test the issue fixture in all three modes:
  legacy full prune, reference-gated no-op, and intentional reference-gated
  removal.
- [ ] Counterfactual check: disable ownership scoping and confirm the issue
  fixture again emits `DROP TABLE ...unmanaged_adhoc`.

## Acceptance criteria

Given:

```text
reference = {managed, intentionally_removed}
desired   = {managed}
current   = {managed, intentionally_removed, unmanaged_adhoc}
```

both `diff -reference ...` and `plan -reference-manifest ...` emit exactly one
object comparison and one operation: the DROP for `intentionally_removed`.
They emit no comparison, count, unsafe entry, or operation for
`unmanaged_adhoc`. Omitting the reference retains today's exact two-way result,
including drops for both current-only objects.

## Non-goals

- Automatically reading Git history or deciding which revision was deployed.
- Field-level ownership or subset columns for ordinary managed tables.
- Applying migrations or adding an execution/approval protocol.
- Changing `drift`, whose peer-to-peer descriptive comparison has no desired or
  prior-owned side.
- Replacing explicit `-exclude`; exclusions remain useful for transient objects
  that should disappear even when declared accidentally.
- Changing unsafe/manual classifications of generated operations.
