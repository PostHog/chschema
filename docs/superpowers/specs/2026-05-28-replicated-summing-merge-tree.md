# ReplicatedSummingMergeTree Engine Support — Design Spec

**Date:** 2026-05-28
**Status:** Approved, ready for implementation

## Motivation

`hclexp introspect` against PostHog's production schema fails on one table — `posthog.sharded_distinct_id_usage` — because its `ReplicatedSummingMergeTree` engine is not modelled by hclexp. It's the only missing replicated/non-replicated pair in the MergeTree family: ReplicatedReplacing, ReplicatedCollapsing, ReplicatedAggregating, and `ReplicatedMergeTree` itself are all supported; the Summing variant was overlooked.

This is a small, mechanical extension that follows the exact pattern of the four existing pairs. No new design decisions — just filling the gap.

## Surface

New `EngineReplicatedSummingMergeTree` type, registered as a sibling to the four existing replicated MergeTree types:

```go
type EngineReplicatedSummingMergeTree struct {
    ZooPath     string   `hcl:"zoo_path"`
    ReplicaName string   `hcl:"replica_name"`
    SumColumns  []string `hcl:"sum_columns,optional"`
}

func (EngineReplicatedSummingMergeTree) Kind() string { return "replicated_summing_merge_tree" }
```

HCL block label: `"replicated_summing_merge_tree"`.

```hcl
table "sharded_distinct_id_usage" {
  order_by = ["team_id", "distinct_id"]

  column "team_id" { type = "Int64" }
  column "distinct_id" { type = "String" }
  column "count" { type = "UInt64" }

  engine "replicated_summing_merge_tree" {
    zoo_path     = "/clickhouse/tables/{shard}/distinct_id_usage"
    replica_name = "{replica}"
    sum_columns  = ["count"]   // optional; omit for the no-argument SummingMergeTree-style form
  }
}
```

ClickHouse syntax round-trip:

| HCL                                                                                      | ClickHouse DDL                                                              |
| ---------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| `engine "replicated_summing_merge_tree" { zoo_path = "...", replica_name = "..." }`      | `ENGINE = ReplicatedSummingMergeTree('zk', 'replica')`                      |
| `engine "replicated_summing_merge_tree" { zoo_path = ..., replica_name = ..., sum_columns = ["a", "b"] }` | `ENGINE = ReplicatedSummingMergeTree('zk', 'replica', a, b)`                |

The optional `sum_columns` exactly mirrors the existing `EngineSummingMergeTree.SumColumns` field; identical handling.

## Implementation

Five touch points, one consistent slice. Each is a near-direct copy of the matching code path for an existing replicated/non-replicated pair (`EngineReplicatedReplacingMergeTree` and `EngineReplacingMergeTree` are the closest models).

### 1. `internal/loader/hcl/engines.go`

- Add the new struct + `Kind()` method directly after `EngineSummingMergeTree` (line 46), matching the position of `EngineReplicatedReplacingMergeTree` relative to `EngineReplacingMergeTree`.
- Extend the `DecodeEngine` switch with a new `case "replicated_summing_merge_tree"` arm.

### 2. `internal/loader/hcl/introspect.go`

Two existing switch statements walk the `chparser.EngineExpr.Name` and need a new case each (line ~464 and ~700 — same pattern as `replicated_replacing_merge_tree` in both functions):

```go
case "ReplicatedSummingMergeTree":
    if len(params) < 2 {
        return nil, nil, fmt.Errorf("ReplicatedSummingMergeTree needs at least (zoo_path, replica_name)")
    }
    ee := EngineReplicatedSummingMergeTree{ZooPath: params[0], ReplicaName: params[1]}
    if len(params) > 2 {
        ee.SumColumns = params[2:]
    }
    return ee, allSettings, nil
```

The second occurrence (the simpler engineFromAST path around line 700) mirrors the structure of the `ReplicatedReplacingMergeTree` case there.

### 3. `internal/loader/hcl/sqlgen.go`

The `engineDDL` function (or equivalent — locate during impl) needs a new arm that emits:

```go
case EngineReplicatedSummingMergeTree:
    parts := []string{quoteSQL(v.ZooPath), quoteSQL(v.ReplicaName)}
    parts = append(parts, v.SumColumns...)
    return fmt.Sprintf("ReplicatedSummingMergeTree(%s)", strings.Join(parts, ", "))
```

Mirrors how `EngineReplicatedReplacingMergeTree` is emitted (zoo_path + replica_name quoted, version_column unquoted; here sum_columns are unquoted column identifiers).

### 4. Tests

- **Unit (`engines_test.go`):** HCL decode round-trip — write HCL, decode to struct, check Kind/fields.
- **Unit (`sqlgen_test.go`):** emit-and-compare for both with-and-without `sum_columns` shapes; assert the ClickHouse DDL is exactly correct.
- **Unit (`introspect_test.go`):** parse a `CREATE TABLE … ENGINE = ReplicatedSummingMergeTree('zk', 'r', a, b)` string and assert the decoded `EngineReplicatedSummingMergeTree` matches.
- **Live (`test/testdata/posthog-create-statements/ReplicatedSummingMergeTree/`):** new directory + at least one fixture (the real PostHog `sharded_distinct_id_usage`). The existing `TestLive_Introspection_AllStatements` discovery walks all directories automatically; the new fixture will round-trip via the default Table arm.

### 5. Docs

- `docs/README.hcl.md` "Supported Engines" table or list: add the new engine row, mirroring the `ReplicatedReplacingMergeTree` entry.
- `README.md` supported-engines section (if there is one): same addition.
- `CLAUDE.md`: update the supported-engines bullet list to include `ReplicatedSummingMergeTree`.

## Verification

- `go test ./internal/loader/hcl/... ./test/...` → green.
- `go test ./test -run TestLive_Introspection_AllStatements/ReplicatedSummingMergeTree -clickhouse` → pass against the new fixture.
- Re-introspecting the local PostHog database (when restarted) no longer logs `unsupported engine: ReplicatedSummingMergeTree`.

## Out of scope

- Refactoring the other replicated engines to share Go types (the existing one-type-per-engine convention stays).
- Modelling `database_atomic_*` / `database_replicated` macros — unrelated.
- Validation of zk_path uniqueness across tables — that's runtime ClickHouse behaviour, not schema-shape.
