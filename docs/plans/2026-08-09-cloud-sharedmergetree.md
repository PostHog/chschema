# ClickHouse Cloud — first-class `Shared*MergeTree` engines

Status: implemented as the mergeable successor to #176.

## Problem

ClickHouse Cloud may take authored DDL such as:

```sql
CREATE TABLE t (...)
ENGINE = ReplacingMergeTree(version)
```

and report the stored table as:

```sql
CREATE TABLE t (...)
ENGINE = SharedReplacingMergeTree(
  '/clickhouse/tables/{uuid}/{shard}',
  '{replica}',
  version
)
```

Before this change, `engineFromAST` had no `Shared*MergeTree` cases, so live
introspection and `sql2hcl` aborted with `unsupported engine`.

The Shared family uses the replicated-family constructor shape: path and
replica arguments first, followed by the same variant-specific arguments as
the plain family.

## Decision: represent the reported engine exactly

`Shared*MergeTree` engines are first-class HCL kinds. The name and every
constructor argument are preserved:

| ClickHouse engine | HCL kind | Fields |
|---|---|---|
| `SharedMergeTree` | `shared_merge_tree` | `zoo_path`, `replica_name` |
| `SharedReplacingMergeTree` | `shared_replacing_merge_tree` | path/replica, optional `version_column`, `is_deleted_column` |
| `SharedSummingMergeTree` | `shared_summing_merge_tree` | path/replica, optional `sum_columns` |
| `SharedCollapsingMergeTree` | `shared_collapsing_merge_tree` | path/replica, `sign_column` |
| `SharedAggregatingMergeTree` | `shared_aggregating_merge_tree` | `zoo_path`, `replica_name` |

Example:

```hcl
engine "shared_replacing_merge_tree" {
  zoo_path          = "/clickhouse/tables/{uuid}/{shard}"
  replica_name      = "{replica}"
  version_column    = "version"
  is_deleted_column = "is_deleted"
}
```

The round trip is mechanical:

```text
SharedReplacingMergeTree(path, replica, version, is_deleted)
  -> EngineSharedReplacingMergeTree
  -> engine "shared_replacing_merge_tree" { ... }
  -> SharedReplacingMergeTree(path, replica, version, is_deleted)
```

No connection capability query, Cloud mode, inferred equivalence, or dropped
argument participates in that sequence.

## Why not normalize to the plain family

The original proposal collapsed `Shared<X>MergeTree` to `<X>MergeTree` when a
live server reported a rewriting `cloud_mode_engine`. That made one HCL engine
portable between Cloud and self-hosted deployments, but it made schema meaning
depend on external state:

- live introspection needed an extra query and permission on `system.settings`;
- the setting had to be threaded through every table and inner-engine parser;
- `sql2hcl` could not make the same decision because it has no server;
- constructor arguments were deliberately discarded;
- the diff treated two different engine names as equal only in one execution
  context.

That is harder to explain than direct representation and conflicts with the
rest of chschema's typed, structural comparison model.

## Invariants

- Introspection, HCL parsing, HCL dumping, `sql2hcl`, diff, and SQL generation
  use the same concrete engine type.
- Path and replica values are data, even when Cloud generated them. They are
  never inferred or discarded.
- Constructor arity is strict. Unknown extra arguments abort rather than
  round-tripping as false “no drift”.
- `SharedReplacingMergeTree.is_deleted_column` requires `version_column`, the
  same rule enforced for plain and replicated replacing engines.
- Shared engines expose the same stable MergeTree virtual-column set and are
  allowed wherever chschema accepts a MergeTree-family engine.

## Consequence for environment layering

A Cloud table reported as `SharedMergeTree` and a reference declared as
`MergeTree` are different engines. That difference is intentional and visible.
The Cloud reference should record `shared_merge_tree`. When one logical table
targets both Cloud and self-hosted ClickHouse, put the engine in an
environment-specific layer and use `patch_table`, just as for replicated versus
non-replicated self-hosted deployments.

## Parser containment

The live service used while investigating #176 also exposed an unrelated
third-party parser panic on `<=>` in a column default. Panic containment was
split and merged as chschema #184. Native `<=>` support and the nil-dereference
fix were merged in `orian/clickhouse-sql-parser` #23; chschema pins that fixed
revision independently of Shared engine support.

## Verification

- SQL AST to each Shared engine type, including optional and trailing fields.
- Every decoded engine re-emits the identical constructor.
- HCL write then parse preserves every concrete Shared engine value.
- `sql2hcl` accepts Shared DDL without a live server.
- Missing and extra constructor arguments fail loudly.
- Replacing `is_deleted_column` validation includes the Shared variant.
