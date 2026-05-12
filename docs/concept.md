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
- **Cluster-aware execution.** Generated DDL uses `ON CLUSTER` where appropriate.
- **Round-trippable.** `chschema dump` introspects an existing cluster into HCL
  files the loader can consume.

## Non-goals

- **Sequential migrations.** No ordered, append-only migration log. State, not history.
- **Data transformation / backfill.** The tool manages structure, not row content.
- **Multi-engine support.** ClickHouse only.
- **General-purpose macro language.** Reuse is limited to layered overlays and
  column templates. No conditional logic in user files.
- **Ad-hoc query interface.** Use `clickhouse client` for that.

## Workflow

1. Edit HCL files describing tables, materialized views, dictionaries.
2. `chschema validate --layer ... --layer ...` runs in CI: static checks + live
   ClickHouse apply.
3. `chschema --dry-run` shows planned DDL on a target cluster.
4. PR review and merge.
5. `chschema --auto-approve` on merge applies the change.
