# chschema — Concept

## What it is

A declarative IaC tool for ClickHouse schemas. The desired state of clusters,
databases, tables, materialized views, views, and dictionaries lives in
version-controlled HCL files. The tool reconciles a live ClickHouse cluster to
that state.

## Goals

- **Declarative source of truth.** HCL files describe every schema object
  chschema owns; that ownership set may deliberately be a subset of a live
  node. No migration history is tracked.
- **Idempotent reconciliation.** Re-running with no schema change is a no-op.
  The diff engine compares desired vs introspected state.
- **Strong static validation.** Catch broken materialized views, missing
  references, and engine misconfigurations at PR time, before merge.
- **Layered configuration.** Combine a base schema with environment- and
  node-specific overlays without duplication.
- **Per-node execution.** Generated DDL is executed on each node of a cluster
  individually — never with `ON CLUSTER`, which proved too fragile in
  operation. `hclexp plan` / `diff -format json` emit the dependency-ordered
  statement list that an executor replays per node.
- **Cluster-side runtime secrets.** HCL and plan JSON never need dictionary
  passwords. A dictionary source refers to an externally provisioned ClickHouse
  named collection; the executor sends `SOURCE(... NAME ...)` unchanged and the
  target node resolves the credential locally.
- **Round-trippable.** `hclexp introspect` / `dump-cluster` turn an existing
  cluster into HCL files the loader can consume.

## Non-goals

- **Sequential migrations.** No ordered, append-only migration log. State, not history.
- **Data transformation / backfill.** The tool manages structure, not row content.
- **Multi-engine support.** ClickHouse only.
- **General-purpose macro language.** Reuse is limited to layered overlays and
  column templates. No conditional logic in user files.
- **Ad-hoc query interface.** Use `clickhouse client` for that.
- **Automatic heavy mutations.** Statements that rewrite existing data
  unpredictably (currently `MATERIALIZE INDEX`) are generated but marked
  manual (`-- MANUAL:` in text output, `"manual": true` in JSON); an operator
  runs them deliberately, never an automated apply.

## Workflow

The workflow keeps three artifacts and two comparisons separate:

```text
managed drift: reference <-> live, scoped to reference-owned objects
migration:     reference  -> proposed, exact and independent of live state
```

1. Dump live nodes, then compare the committed reference with live using
   `diff -scope left` or `plan -dump ... -scope desired`. Resolve every
   managed drift: correct production when the reference is right, or update
   the reference when production is intentionally right.
2. Edit the HCL proposal and run static validation.
3. Generate the migration from the committed reference to the proposal with
   an exact `diff`, or across roles with `plan -from-manifest`. This exact
   comparison includes intentional additions and removals and is deterministic
   across nodes; it must not adapt itself to an unresolved live state.
4. PR review and merge. Immediately before apply, require the managed live
   drift check to remain empty.
5. The deployment ensures every external named collection referenced by a
   dictionary exists on each target node (or in shared Keeper-backed storage).
6. The reviewed statements are executed on each node (per-node, no
   `ON CLUSTER`); `MANUAL` statements are run by an operator when
   appropriate.

Directional scope ignores whole objects outside the ownership set, never
fields inside a managed object. Unscoped diff and manifest-to-manifest plan
remain exact. Desired-scoped dump planning deliberately cannot express an
intentional deletion; that belongs to the exact reference-to-proposed plan.
