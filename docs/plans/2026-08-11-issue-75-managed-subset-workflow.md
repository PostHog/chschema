# Issue #75 — managed-subset drift and deterministic migrations

Status: **proposed**. Issue: [#75](https://github.com/PostHog/chschema/issues/75).

## Goal

Support a repository workflow with three artifacts but two deliberately
separate comparisons:

1. A committed **reference schema** records the schema chschema manages.
2. Per-node **live dumps** record what is actually in production, including
   objects owned by other systems and teams.
3. A **proposed schema** is the working-tree change to the reference.

Live drift must be resolved before a migration is accepted: either production
is changed back to the reference, or the reference is updated to describe the
accepted production state. Once reference and production agree, migrations are
generated from the committed reference to the proposed schema — never adapted
to whatever happens to be live on one node.

```text
drift check:           reference <-> live, scoped to reference-owned objects
migration generation: reference  -> proposed, exact two-way diff
```

This makes migrations deterministic across nodes and environments while
allowing a managed schema to be a deliberate subset of every live node.

## Current behavior and the remaining gap

`Diff(from, to)` is a conventional exact two-way diff. Every object present on
only one side is a difference. That behavior is correct for migration
generation:

```bash
hclexp diff -left reference -right proposed -format json
```

It is too broad for the reference-to-live drift check. A live node can contain
ad-hoc, sensitive, or other-team objects that are intentionally absent from the
reference. They appear as live-only differences even though chschema does not
own them.

PR #139 added `-exclude` to `diff`, `plan`, and `drift`, filtering both sides
and allowing whole object types such as `named_collection` to be ignored. That
solves known exclusions, but every unmanaged object still has to be enumerated
in advance. The reference schema already provides the natural ownership set.

The original issue proposed a third, previous-desired `reference` input while
planning against live. That is not needed for this repository workflow and
would weaken an important invariant: unresolved production drift must not
change the migration generated from one committed schema revision to the next.

## Decision

### 1. Add an explicit object-scope mode to two-way `diff`

Add:

```text
-scope all|left|right
```

`all` is the default and preserves today's exact behavior.

- `-scope left` compares only logical object identities declared on the left.
  Right-only objects are ignored; left-only objects are still reported as
  missing from the right.
- `-scope right` compares only logical object identities declared on the
  right. Left-only objects are ignored; right-only objects are still reported
  as missing from the left.

The implementation scopes the opposite side and then calls the existing
two-way `Diff` unchanged:

```text
scope=left:  Diff(left,  right intersect identities(left))
scope=right: Diff(left intersect identities(right), right)
scope=all:   Diff(left, right)
```

This is an object-level ownership boundary, not a field-level subset mode. Once
a table is in scope, its columns, indexes, projections, constraints, engine,
TTL, settings, and every other modeled field compare normally and remain fully
authoritative.

### 2. Keep migration generation as an exact reference-to-proposed diff

No new reference input participates in migration generation:

```bash
hclexp diff \
  -left ./reference \
  -right ./proposed \
  -format json
```

Do not pass `-scope` here. The exact diff naturally provides the intended
semantics:

- a new table or column in proposed produces `CREATE`/`ALTER ADD`;
- an object removed from the previous reference produces `DROP`;
- a modification produces the normal in-place or recreate operation;
- the same two revisions always produce the same migration, independent of
  node state.

### 3. Add manifest-to-manifest cross-role planning

`plan` currently gets its `Current` schemas from a topology `-dump`. For
deterministic migrations with global cross-role dependency ordering, add an
alternative current source: the previous committed manifest composition.

```bash
hclexp plan \
  -from-manifest /tmp/reference/schema/manifest.hcl \
  -from-layer-root /tmp/reference/schema \
  -manifest schema/manifest.hcl \
  -layer-root schema \
  -env prod-us \
  -format json
```

`-from-manifest` and `-dump` are mutually exclusive. Both feed the existing
`RoleDiff{Current, Desired}` and `BuildPlan`; the former is the migration mode,
while the latter remains available for compatibility and topology analysis.
The dump-based mode must not be described as a safe migration generator for a
subset-managed node unless its live-only objects have been explicitly scoped
or excluded.

There is deliberately no built-in Git fetch/show logic. The caller resolves the
previous deployed revision into files, for example with a temporary worktree.

## Operational workflow

### Step 1: Detect drift

The repository keeps the reference on the left, so the live dump is scoped to
the reference's object identities:

```bash
hclexp diff \
  -left ./reference \
  -right ./clickhouse-schema/prod/node-01.hcl \
  -scope left \
  -exclude exclude.hcl \
  -format json
```

The operations in this direction are descriptive: they show how the live dump
differs from the reference. They are not automatically a production repair
script.

### Step 2: Resolve every managed drift

For each reported difference, choose one:

- production is correct: update and review the reference schema;
- the reference is correct: generate the reverse corrective DDL by putting the
  reference on the right and scoping to it:

  ```bash
  hclexp diff \
    -left ./clickhouse-schema/prod/node-01.hcl \
    -right ./reference \
    -scope right \
    -exclude exclude.hcl \
    -sql
  ```

After applying or accepting the change, regenerate the live dump and require
the managed drift check to be empty.

### Step 3: Generate the proposed migration

Compare committed reference with the working-tree proposal exactly:

```bash
hclexp diff \
  -left ./reference \
  -right ./proposed \
  -format json
```

For global cross-role ordering, compare the same two revisions as manifest
compositions:

```bash
git worktree add /tmp/chschema-reference "$DEPLOYED_SHA"

hclexp plan \
  -from-manifest /tmp/chschema-reference/schema/manifest.hcl \
  -from-layer-root /tmp/chschema-reference/schema \
  -manifest schema/manifest.hcl \
  -layer-root schema \
  -env prod-us \
  -exclude schema/exclude.hcl \
  -format json
```

### Step 4: Treat live equality as an apply precondition

The reference-scoped drift gate must pass immediately before applying the
reference-generated migration. If a newly proposed table name already exists
as an unmanaged live object, the deterministic `CREATE` also fails hard rather
than silently adapting to unreviewed state; the collision must be reconciled
explicitly.

## Identity and filtering rules

- Database objects use logical `(database, name)` identity across modeled
  tables, materialized views, views, dictionaries, and raw representations.
  An object can move between raw and modeled representation without falling
  out of scope; the ordinary diff still decides whether that is a recreate.
- Named collections use their own top-level namespace keyed by name.
- `external = true` named collections retain their existing ownership-boundary
  semantics. If the scope side declares one, its reference is retained but
  live values are not compared or mutated.
- Database blocks and node metadata are not ownership units. Empty database
  wrappers and node metadata are preserved while object slices are scoped.
- `-exclude` applies to both sides before `-scope` is calculated. An excluded
  object cannot be brought back into the comparison by the scope side.
- Output JSON and operation shapes do not change. Scoping changes which
  comparisons exist, not their serialization.
- A scoped-out live object appears in no object comparison, operation, unsafe
  entry, or summary count.

## Manifest-to-manifest role rules

- A role present in both manifests compares its previous and proposed composed
  layer stacks.
- A role present only in the proposed manifest has an empty previous schema and
  therefore produces creates.
- A role present only in the previous manifest is a hard planning error. Role
  removal is a deployment/decommissioning decision and must not silently turn
  into "drop every managed object on that role."
- A role present in both manifests but with a changed layer list loads each
  list from its own revision and root. The current manifest must never be
  applied retroactively to the previous revision's files.
- `-from-layer-root` defaults to the directory containing `-from-manifest`.
- `-from-layer-root` without `-from-manifest`, or combining `-from-manifest`
  with `-dump`, is a usage error.

## Implementation plan

### Task 1: Add a non-mutating schema scope helper

**Files:**

- Create `internal/loader/hcl/scope.go`.
- Create `internal/loader/hcl/scope_test.go`.

- [ ] Define an internal logical object identity with separate database-object
  and named-collection namespaces.
- [ ] Implement:

  ```go
  func ScopeSchemaToObjects(source, scope *Schema) *Schema
  ```

  It returns only source objects whose logical identities occur in scope and
  must not mutate either input. Copy `Schema`, `Databases`, and filtered object
  slices; object specs can be shared because the helper does not edit them.
- [ ] Preserve database wrappers and node metadata.
- [ ] Match raw and modeled representations by logical database/name identity,
  not by Go slice or HCL block kind.

Tests:

- [ ] source-only objects of every modeled kind are removed;
- [ ] identities present in scope are retained;
- [ ] raw-to-modeled and modeled-to-raw identities match;
- [ ] named collections use their separate namespace;
- [ ] database and node metadata are preserved;
- [ ] the source, scope, and all backing slices remain unchanged.

### Task 2: Add `diff -scope`

**Files:**

- Modify `cmd/hclexp/hclexp.go`.
- Extend `cmd/hclexp/hclexp_test.go`.

- [ ] Register `-scope all|left|right`, defaulting to `all`; reject other
  values as usage errors.
- [ ] Apply `-exclude` to both schemas first.
- [ ] For `left`, scope the right schema to left identities. For `right`, scope
  the left schema to right identities. For `all`, preserve both inputs.
- [ ] Pass the effective sides consistently into `Diff`, `RenderDiffJSON`, and
  text comparison rendering so objects, operations, summaries, and human text
  cannot disagree.
- [ ] Keep the existing `Diff(from, to)` API and behavior unchanged.

Tests:

- [ ] exact issue #75 fixture: reference has `managed`; live has `managed` and
  `unmanaged_adhoc`; `-scope left` yields no differences;
- [ ] a reference-owned object missing live remains a reported difference;
- [ ] an extra column inside a reference-owned live table remains a reported
  field change;
- [ ] reversing the sides with `-scope right -sql` generates corrective DDL
  without dropping unmanaged live objects;
- [ ] `-scope all` and omitted `-scope` retain byte-for-byte current output;
- [ ] `-exclude` plus scope cannot reintroduce excluded objects;
- [ ] raw/model transitions and external named collections preserve existing
  diff semantics;
- [ ] invalid scope values exit with a precise usage error.

### Task 3: Add manifest-to-manifest `plan`

**Files:**

- Modify `cmd/hclexp/plan.go`.
- Extend `cmd/hclexp/plan_test.go`.

- [ ] Add `-from-manifest` and `-from-layer-root` with the validation and
  defaults above.
- [ ] Refactor manifest environment selection so each manifest is parsed and
  indexed independently by role.
- [ ] In from-manifest mode, iterate proposed roles, compose the previous and
  proposed layer stacks from their respective roots, and build ordinary
  `RoleDiff{Current: previous, Desired: proposed}` values.
- [ ] Use an empty previous schema for proposed-only roles.
- [ ] Reject previous-only roles with a decommissioning-specific error.
- [ ] Apply `-exclude` to both revisions before building each role diff.
- [ ] Preserve dump mode unchanged when `-dump` is selected.

Tests:

- [ ] an added table/column is generated from previous reference regardless of
  the contents of any live dump;
- [ ] an intentional object removal produces a DROP with the correct role;
- [ ] changed layer lists load from their own manifests and roots;
- [ ] proposed-only roles produce creates;
- [ ] previous-only roles fail rather than dropping a role's schema;
- [ ] identical operations still deduplicate across roles and retain the union
  of role provenance;
- [ ] default roots and invalid flag combinations behave as documented.

### Task 4: Correct cross-role metadata and DROP ordering

**Files:**

- Modify `internal/loader/hcl/plan.go`.
- Extend `internal/loader/hcl/plan_test.go`.

Intentional removals are absent from desired, so existing desired-only lookup
cannot fully describe or dependency-order their DROP operations.

- [ ] Build CREATE/ALTER dependency ranks from merged desired schemas.
- [ ] Build DROP dependency ranks from merged current schemas.
- [ ] Populate table `engine` and `replicated` from desired for CREATE/ALTER,
  and current for DROP.
- [ ] Keep phase ordering CREATE, ALTER/RENAME, DROP and stable ordering for
  unrelated ties.

Tests:

- [ ] a removed dependent object drops before its removed dependency,
  including across roles;
- [ ] a dropped replicated table retains its actual engine and
  `replicated=true` metadata;
- [ ] CREATE/ALTER ordering and metadata remain unchanged;
- [ ] operation deduplication and per-role order remapping remain consistent.

### Task 5: Document the two-pipeline contract

**Files:**

- Modify `README.md` (`diff` flags and cross-role planning).
- Modify `docs/README.hcl.md` (comparison scope and plan modes).
- Modify `docs/concept.md` (reference/live/proposed workflow).

- [ ] Explain reference-scoped drift separately from migration generation.
- [ ] State that every managed live drift must be accepted into reference or
  corrected in production before migration generation/application.
- [ ] State prominently that unscoped two-way diff is exact and reports every
  side-only object.
- [ ] Document `-scope left|right` with drift and reverse-correction examples.
- [ ] Explain object-level authority: scope ignores whole unmanaged objects,
  not extra fields inside managed objects.
- [ ] Document manifest-to-manifest planning and its Git worktree workflow.
- [ ] Warn that dump-based planning over subset-managed nodes is not an apply
  migration unless live-only objects are explicitly controlled.
- [ ] Explain `-exclude`, external collections, role-set changes, and new-table
  name collisions.

### Task 6: Verification

- [ ] `gofmt -s -l .` produces no output.
- [ ] `go test ./internal/loader/hcl` passes.
- [ ] `go test ./internal/... ./cmd/...` passes.
- [ ] `go test ./test/...` passes.
- [ ] `go test -race ./internal/... ./cmd/...` passes.
- [ ] `go vet ./...` passes.
- [ ] `git diff --check` passes.
- [ ] Build the CLI and smoke-test:
  reference-scoped clean drift, managed drift, reverse correction, exact
  reference-to-proposed addition, and an intentional deletion.
- [ ] Counterfactual check: omit `-scope left` from the issue fixture and
  confirm the unmanaged live object reappears in the comparison.

## Acceptance criteria

Given:

```text
reference = {managed}
live      = {managed, unmanaged_adhoc}
proposed  = {managed, new_table}
```

the commands behave as follows:

```text
diff reference -> live     with scope=left: no differences
diff reference -> live     with scope=all:  unmanaged_adhoc is reported
diff reference -> proposed with scope=all:  exactly CREATE new_table
```

If the managed table differs live, the scoped drift reports it. Reversing the
sides with `scope=right` produces the correction toward reference without a
DROP for `unmanaged_adhoc`. Removing an object between reference revisions
produces its DROP in exact diff and manifest-to-manifest plan modes.

## Non-goals

- A three-way live/reference/proposed diff or a `-reference` input.
- Adapting a reviewed migration to node-specific live drift.
- Automatically reading Git history or deciding which revision was deployed.
- Field-level ownership or subset columns inside ordinary managed tables.
- Applying migrations or adding an execution/approval protocol.
- Changing peer-to-peer `drift` grouping semantics.
- Replacing explicit `-exclude` for transient objects.
- Changing unsafe/manual classifications of generated operations.
