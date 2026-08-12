# Issue #75 — managed-subset drift and deterministic migrations

Status: **implemented in PR #181**. Issue:
[#75](https://github.com/PostHog/chschema/issues/75).

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

### 3. Scope dump-based `plan` to desired objects

Directly address the original issue's command by adding a plan-specific scope:

```bash
hclexp plan \
  -manifest schema/manifest.hcl \
  -layer-root schema \
  -env prod-us \
  -dump ./clickhouse-schema/prod-us \
  -scope desired \
  -format json
```

For dump-based planning, accept `-scope all|desired`, defaulting to `all` for
compatibility. For each role:

```text
scope=desired: Diff(current intersect identities(desired), desired)
scope=all:     Diff(current, desired)
```

`desired` makes `plan -manifest ... -dump ...` a managed live-convergence plan:
it ignores whole live-only objects, still creates managed objects missing live,
and fully reconciles fields inside managed objects. It cannot infer an
intentional deletion because an object absent from desired is outside the
ownership set; intentional removals come from exact reference-to-proposed
migration planning.

### 4. Add manifest-to-manifest cross-role planning

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
while `-dump -scope desired` is the managed live-convergence mode. Unscoped dump
planning remains available for compatibility and exact whole-node management,
but may drop every live-only object.

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

For a globally ordered, cross-role convergence plan against the live topology,
use the same ownership rule through `plan`:

```bash
hclexp plan \
  -manifest schema/manifest.hcl \
  -layer-root schema \
  -env prod-us \
  -dump ./clickhouse-schema/prod-us \
  -scope desired \
  -exclude schema/exclude.hcl \
  -format json
```

This is the safe form of the command reported in #75. It emits CREATE/ALTER or
replace operations for managed objects but no DROP for a live-only unmanaged
object. Because it cannot represent intentional deletion, it does not replace
the exact reference-to-proposed migration plan below.

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
- In dump-based `plan`, `desired` means each role's composed desired schema;
  current is scoped independently for every role before `RoleDiff` is built.
- Output JSON and operation shapes do not change. Scoping changes which
  comparisons exist, not their serialization.
- A scoped-out live object appears in no object comparison, operation, unsafe
  entry, or summary count.

## Dump-based plan scope rules

- `-scope all` is the default and preserves today's exact full-node behavior.
- `-scope desired` is valid only with `-dump`; it scopes every role's current
  dump schema to that role's desired object identities.
- A desired object missing from the dump remains in the diff and produces a
  CREATE. A shared object's internal differences remain fully authoritative.
- A live-only object appears nowhere in role comparisons, merged operations,
  unsafe entries, or summary counts.
- `-exclude` filters desired and current before desired identities are
  collected.
- A role absent from the dump still uses an empty current schema, so all of its
  desired objects produce CREATE operations.
- `-scope desired` with `-from-manifest` is rejected: migration planning must
  remain exact and include intentional removals.

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

## Implementation checklist

### Task 1: Add a non-mutating schema scope helper

**Files:**

- Create `internal/loader/hcl/scope.go`.
- Create `internal/loader/hcl/scope_test.go`.

- [x] Define an internal logical object identity with separate database-object
  and named-collection namespaces.
- [x] Implement:

  ```go
  func ScopeSchemaToObjects(source, scope *Schema) *Schema
  ```

  It returns only source objects whose logical identities occur in scope and
  must not mutate either input. Copy `Schema`, `Databases`, and filtered object
  slices; object specs can be shared because the helper does not edit them.
- [x] Preserve database wrappers and node metadata.
- [x] Match raw and modeled representations by logical database/name identity,
  not by Go slice or HCL block kind.

Tests:

- [x] source-only objects of every modeled kind are removed;
- [x] identities present in scope are retained;
- [x] raw-to-modeled and modeled-to-raw identities match;
- [x] named collections use their separate namespace;
- [x] database and node metadata are preserved;
- [x] the source, scope, and all backing slices remain unchanged.

### Task 2: Add `diff -scope`

**Files:**

- Modify `cmd/hclexp/hclexp.go`.
- Extend `cmd/hclexp/hclexp_test.go`.

- [x] Register `-scope all|left|right`, defaulting to `all`; reject other
  values as usage errors.
- [x] Apply `-exclude` to both schemas first.
- [x] For `left`, scope the right schema to left identities. For `right`, scope
  the left schema to right identities. For `all`, preserve both inputs.
- [x] Pass the effective sides consistently into `Diff`, `RenderDiffJSON`, and
  text comparison rendering so objects, operations, summaries, and human text
  cannot disagree.
- [x] Keep the existing `Diff(from, to)` API and behavior unchanged.

Tests:

- [x] exact issue #75 fixture: reference has `managed`; live has `managed` and
  `unmanaged_adhoc`; `-scope left` yields no differences;
- [x] a reference-owned object missing live remains a reported difference;
- [x] an extra column inside a reference-owned live table remains a reported
  field change;
- [x] reversing the sides with `-scope right -sql` generates corrective DDL
  without dropping unmanaged live objects;
- [x] `-scope all` and omitted `-scope` retain byte-for-byte current output;
- [x] `-exclude` plus scope cannot reintroduce excluded objects;
- [x] raw/model transitions and external named collections preserve existing
  diff semantics;
- [x] invalid scope values exit with a precise usage error.

### Task 3: Add desired scope to dump-based `plan`

**Files:**

- Modify `cmd/hclexp/plan.go`.
- Extend `cmd/hclexp/plan_test.go`.

- [x] Register `-scope all|desired`, defaulting to `all`; reject other values
  and reject `desired` unless `-dump` is the current source.
- [x] Apply `-exclude` to every role's desired and current schemas before
  scoping.
- [x] In desired scope, replace current with
  `ScopeSchemaToObjects(current, desired)` before constructing `RoleDiff`.
- [x] Feed the effective current consistently into role comparisons, merged
  operations, engine metadata, and dependency ordering.
- [x] Preserve the existing empty-current behavior for roles absent from the
  dump.

Tests:

- [x] reproduce the exact issue #75 plan fixture: desired has `managed`; the
  matching dump role has `managed` and `unmanaged_adhoc`; `-scope desired`
  yields no DROP and no comparison for the unmanaged table;
- [x] omitted scope and `-scope all` retain the existing unmanaged DROP;
- [x] a managed object missing from live produces CREATE;
- [x] a shared managed object with field drift produces ALTER/replace;
- [x] live-only objects of every supported kind, including named collections
  and raw objects, are absent from per-role and global output;
- [x] scoping is independent per role and preserves cross-role operation
  deduplication and provenance;
- [x] `-exclude` cannot reintroduce a scoped-out object;
- [x] a role absent from the dump still plans all desired creates;
- [x] invalid scope values and scope/current-source combinations fail with
  precise usage errors.

### Task 4: Add manifest-to-manifest `plan`

**Files:**

- Modify `cmd/hclexp/plan.go`.
- Extend `cmd/hclexp/plan_test.go`.

- [x] Add `-from-manifest` and `-from-layer-root` with the validation and
  defaults above.
- [x] Refactor manifest environment selection so each manifest is parsed and
  indexed independently by role.
- [x] In from-manifest mode, iterate proposed roles, compose the previous and
  proposed layer stacks from their respective roots, and build ordinary
  `RoleDiff{Current: previous, Desired: proposed}` values.
- [x] Use an empty previous schema for proposed-only roles.
- [x] Reject previous-only roles with a decommissioning-specific error.
- [x] Apply `-exclude` to both revisions before building each role diff.
- [x] Preserve dump mode and its `all|desired` scope when `-dump` is selected.

Tests:

- [x] an added table/column is generated from previous reference regardless of
  the contents of any live dump;
- [x] an intentional object removal produces a DROP with the correct role;
- [x] changed layer lists load from their own manifests and roots;
- [x] proposed-only roles produce creates;
- [x] previous-only roles fail rather than dropping a role's schema;
- [x] identical operations still deduplicate across roles and retain the union
  of role provenance;
- [x] default roots and invalid flag combinations behave as documented.

### Task 5: Correct cross-role metadata and DROP ordering

**Files:**

- Modify `internal/loader/hcl/plan.go`.
- Extend `internal/loader/hcl/plan_test.go`.

Intentional removals are absent from desired, so existing desired-only lookup
cannot fully describe or dependency-order their DROP operations.

- [x] Build CREATE/ALTER dependency ranks from merged desired schemas.
- [x] Build DROP dependency ranks from merged current schemas.
- [x] Populate table `engine` and `replicated` from desired for CREATE/ALTER,
  and current for DROP.
- [x] Keep phase ordering CREATE, ALTER/RENAME, DROP and stable ordering for
  unrelated ties.

Tests:

- [x] a removed dependent object drops before its removed dependency,
  including across roles;
- [x] a dropped replicated table retains its actual engine and
  `replicated=true` metadata;
- [x] CREATE/ALTER ordering and metadata remain unchanged;
- [x] operation deduplication and per-role order remapping remain consistent.

### Task 6: Document the two-pipeline contract

**Files:**

- Modify `README.md` (`diff` flags and cross-role planning).
- Modify `docs/README.hcl.md` (comparison scope and plan modes).
- Modify `docs/concept.md` (reference/live/proposed workflow).

- [x] Explain reference-scoped drift separately from migration generation.
- [x] State that every managed live drift must be accepted into reference or
  corrected in production before migration generation/application.
- [x] State prominently that unscoped two-way diff is exact and reports every
  side-only object.
- [x] Document `-scope left|right` with drift and reverse-correction examples.
- [x] Explain object-level authority: scope ignores whole unmanaged objects,
  not extra fields inside managed objects.
- [x] Document manifest-to-manifest planning and its Git worktree workflow.
- [x] Document `plan -dump -scope desired` as the direct fix for #75 and warn
  that unscoped dump planning may drop live-only objects.
- [x] Explain that desired-scoped dump planning converges managed live objects
  but cannot express intentional deletion; exact manifest planning does that.
- [x] Explain `-exclude`, external collections, role-set changes, and new-table
  name collisions.

### Task 7: Verification

- [x] `gofmt -s -l .` produces no output.
- [x] `go test ./internal/loader/hcl` passes.
- [x] `go test ./internal/... ./cmd/...` passes.
- [x] `go test ./test/...` passes.
- [x] `go test -race ./internal/... ./cmd/...` passes.
- [x] `go vet ./...` passes.
- [x] `git diff --check` passes.
- [x] Build the CLI and smoke-test:
  reference-scoped clean drift, managed drift, reverse correction,
  desired-scoped dump plan, exact reference-to-proposed addition, and an
  intentional deletion.
- [x] Counterfactual check: omit `-scope left` from the issue fixture and
  confirm the unmanaged live object reappears in the comparison.
- [x] Counterfactual check: omit `-scope desired` from the issue's `plan`
  fixture and confirm it again emits `DROP TABLE ...unmanaged_adhoc`.

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
plan desired vs live dump  with scope=desired: no operations
plan desired vs live dump  with scope=all:     DROP unmanaged_adhoc
diff reference -> proposed with scope=all:  exactly CREATE new_table
```

If the managed table differs live, the scoped drift reports it. Reversing the
sides with `scope=right` produces the correction toward reference without a
DROP for `unmanaged_adhoc`. Desired-scoped dump planning produces the same
managed convergence operations with cross-role ordering and never mentions the
unmanaged object. Removing an object between reference revisions produces its
DROP in exact diff and manifest-to-manifest plan modes.

## Non-goals

- A three-way live/reference/proposed diff or a `-reference` input.
- Adapting a reviewed migration to node-specific live drift.
- Automatically reading Git history or deciding which revision was deployed.
- Field-level ownership or subset columns inside ordinary managed tables.
- Applying migrations or adding an execution/approval protocol.
- Changing peer-to-peer `drift` grouping semantics.
- Replacing explicit `-exclude` for transient objects.
- Changing unsafe/manual classifications of generated operations.
