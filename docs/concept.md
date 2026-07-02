# chschema — Concept

## What it is

A declarative IaC tool for ClickHouse schemas. The desired state of clusters,
databases, tables, materialized views, views, and dictionaries lives in
version-controlled HCL files. The tool reconciles a live ClickHouse cluster to
that state.

## Goals

- **Declarative source of truth.** HCL files describe the entire schema; no
  migration history is tracked.
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

1. Edit HCL files describing tables, materialized views, dictionaries.
2. `hclexp validate -layer ... -layer ...` runs in CI: static dependency and
   engine checks.
3. `hclexp diff -left <layers> -right clickhouse://... -sql` (or
   `hclexp plan -manifest ... -dump ...` across roles) shows the ordered
   migration DDL; in-place-impossible changes surface as `-- UNSAFE`,
   operator-run statements as `-- MANUAL`.
4. PR review and merge.
5. The reviewed statements are executed on each node (per-node, no
   `ON CLUSTER`); `MANUAL` statements are run by an operator when
   appropriate.
