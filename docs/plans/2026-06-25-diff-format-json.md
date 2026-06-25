# #64: `diff -format json` — structured, ordered diff output

## Problem

`hclexp diff` emits a human-readable change summary, and `diff -sql` emits the
migration DDL as text. Neither is machine-consumable: a migration generator or CI
gate that wants to reason about *which* objects change, in *what dependency
order*, and *which* changes are unsafe has to parse free-form text.

## Goal

Add `-format text|json` to `hclexp diff`.

- `text` (default) — byte-identical to today (summary, or DDL with `-sql`).
- `json` — an ordered operation list plus the unsafe-change list.

## Shape

```json
{
  "operations": [
    {
      "order": 0,
      "kind": "CREATE",
      "object_type": "table",
      "database": "posthog",
      "object": "archive",
      "engine": "MergeTree",
      "replicated": false,
      "sql": "CREATE TABLE posthog.archive (...) ENGINE = MergeTree() ORDER BY (id)",
      "unsafe": false,
      "unsafe_reason": ""
    }
  ],
  "unsafe": [
    { "database": "posthog", "object": "events", "reason": "ORDER BY change requires recreating the table" }
  ]
}
```

## Approach

A **new renderer over existing structs** — not a diff-engine change.

1. **`GenerateSQL` returns a parallel `Ops []Operation`** alongside `Statements`
   (`sqlgen.go`). An `emit(kind, objectType, db, object, sql)` closure appends to
   both lists in lockstep at every existing statement site, so the `-sql` text
   path stays byte-identical and the dependency order is the single source of
   truth. `orderedCreates` now returns `[]Operation` carrying the object type.
   `Operation{Kind, ObjectType, Database, Object, SQL}`.

2. **`RenderDiffJSON(gen, left, right)`** (`render_json.go`) walks `gen.Ops`,
   setting `order` = the index in the dependency-sorted list. It enriches each
   table op with its engine family:
   - **`engineFor(db, object, right, left)`** looks the table up in the resolved
     target schema first (then the current schema for DROPs) and derives the
     ClickHouse family name from the decoded engine's Go type via reflection
     (`EngineReplicatedMergeTree` → `"ReplicatedMergeTree"`). This is why an
     ALTER that does *not* change the engine still reports it — the op carries no
     engine of its own, so it comes from the resolved schema.
   - `replicated` = the family name has a `Replicated` prefix.

3. **Unsafe** comes from the existing `gen.Unsafe` (`UnsafeChange`), no separate
   path. Each op is flagged `unsafe` when its object has a destructive change
   (matched by db+object); the **top-level `unsafe` list** carries every
   `UnsafeChange`, including those that produce no statement (engine/ORDER BY
   recreates), mirroring text mode's `-- UNSAFE:` lines so nothing is lost.

4. **`runDiff`** (`cmd/hclexp/hclexp.go`) gains `-format` (validated to
   `text|json`); the json branch calls `RenderDiffJSON(GenerateSQL(cs), left,
   right)` and prints it. The text branches are untouched.

## Verification

- Unit (`render_json_test.go`): a fixture driving the real
  `Diff → GenerateSQL → RenderDiffJSON` asserts CREATE kind/object_type/engine/
  replicated; an ALTER that does **not** change the engine still reports
  `engine`/`replicated` from the resolved target; `order` = list index; an
  ORDER BY change surfaces in the top-level `unsafe` list and flags the safe
  ALTER on the same table.
- Existing snapshot/integration tests (`go test ./test`) stay green, proving the
  text path is byte-identical.
- Manual smoke: `diff -format json` and the unchanged default summary.

Closes #64.
