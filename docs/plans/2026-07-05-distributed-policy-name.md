# Distributed `policy_name` — fix the #109 silent drop

Status: design approved 2026-07-05; implementation in progress.
Issue: https://github.com/PostHog/chschema/issues/109 (same bug class as
#108; fix mirrors PR #110).

## Problem

Docs signature: `Distributed(cluster, database, table[, sharding_key[, policy_name]])`.
`EngineDistributed` has no `policy_name` field and both introspect decode
paths capture `params[3]` then silently ignore a 5th parameter. The loss
is symmetric (golden and fresh dump both drop it), so `diff` reports no
drift while generated SQL would recreate the table without its storage
policy.

## Docs sweep — where `policy_name` lives ("we must have it everywhere")

Full-tree grep of clickhouse-docs for `policy_name`:

| Occurrence | Kind | Coverage |
|---|---|---|
| `Distributed(...)` 5th engine param | engine parameter | **this fix** |
| MergeTree-family `SETTINGS storage_policy = '…'` (mergetree.md:1052) | table setting | rides the generic settings round-trip; **add a guard test** (none exists today) |
| `<policies><policy_name_N>` XML (mergetree.md:949) | server config | not table schema — out of scope |
| `CREATE ROW POLICY` / masking policy | RBAC DDL, never in `create_table_query` | out of scope |
| `system.storage_policies`, server settings | informational | out of scope |

Buffer/Log/Kafka docs checked directly: no policy parameters. TTL
`TO VOLUME '…'` / `TO DISK '…'` clauses ride the verbatim TTL string.
No other supported engine takes a policy parameter.

## Design (mirrors #110)

- `EngineDistributed` gains `PolicyName *string` (`hcl:"policy_name,optional"`).
- Both introspect decode paths (`engineFromAST` case `"Distributed"`,
  `ParseEngineString` prefix case) capture `params[4]`; **>5 params
  aborts loudly** ("takes at most … got %v"), never silently drops.
- `dump.go` emits `policy_name = "…"` after `sharding_key`.
- `sqlgen.go` `engineSQL` emits the 5-arg form; **policy_name is a
  quoted string literal**, sharding_key stays a bare expression. The
  policy arm requires `ShardingKey != nil`; `engineSQL` has no error
  channel, so the resolver guards the invariant upstream (same division
  as #110's is_deleted-requires-version).
- `resolver.go`: new `validateDistributedEngines` — `policy_name`
  without `sharding_key` is rejected (positional signature), invoked
  right after `validateReplacingEngines` (resolver.go:36).
- `diff`: automatic (`diffEngine` compares decoded structs).
- Unaffected (verified): `validate.go` dependency checks,
  `virtual_columns.go`, `flows.go`, web.

## Implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans
> (inline; established session pattern). Steps use checkboxes.

**Goal:** Round-trip `Distributed(..., sharding_key, policy_name)` with
loud failure on unknown extra params; guard `storage_policy` setting.

**Tech stack:** Go 1.26, testify, chparser fork.

### Global constraints

- Module name / go version in go.mod are untouchable.
- testify for tests; compare whole structs, not field-by-field.
- No comments that restate code.
- Existing helpers to reuse: `fakeRows`/`fakeRow`, `strPtr` (package
  `hcl` test helpers), `ptr` (local to `TestParseEngineString`),
  `processIntrospectRows`, `engineSQL`.
- gofmt -s + go vet clean; suites: `go test ./cmd/... ./internal/...`
  and `go test ./test`.

---

### Task 1: capture — model field + both decode paths

**Files:**
- Modify: `internal/loader/hcl/engines.go` (~:83 `EngineDistributed`)
- Modify: `internal/loader/hcl/introspect.go` (case `"Distributed"`
  ~:606; `strings.HasPrefix(decl, "Distributed")` ~:960)
- Create: `internal/loader/hcl/distributed_policy_name_test.go`
- Modify: `internal/loader/hcl/introspect_test.go`
  (`TestParseEngineString` table)

**Interfaces:**
- Produces: `EngineDistributed.PolicyName *string` — Tasks 2–3 read it.

- [ ] **Step 1: failing tests.** New `distributed_policy_name_test.go`:

```go
package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Introspection must capture the optional 5th (policy_name) Distributed
// parameter (#109): dropping it is symmetric between a golden and a fresh
// dump, so diff cannot see the loss and generated SQL would recreate the
// table without its storage policy.
func TestIntrospect_Distributed_PolicyName(t *testing.T) {
	rows := &fakeRows{rows: []fakeRow{{
		name: "events_dist",
		sql: "CREATE TABLE posthog.events_dist (`id` UInt64) " +
			"ENGINE = Distributed('posthog', 'posthog', 'events', sipHash64(id), 'tiered')",
	}}}
	db := &DatabaseSpec{Name: "posthog"}
	require.NoError(t, processIntrospectRows(db, "posthog", rows))
	require.Len(t, db.Tables, 1)
	assert.Equal(t, EngineDistributed{
		ClusterName:    "posthog",
		RemoteDatabase: "posthog",
		RemoteTable:    "events",
		ShardingKey:    strPtr("sipHash64(id)"),
		PolicyName:     strPtr("tiered"),
	}, db.Tables[0].Engine.Decoded)
}

// A parameter beyond policy_name must abort introspection loudly (#109).
func TestIntrospect_Distributed_TooManyParamsErrors(t *testing.T) {
	rows := &fakeRows{rows: []fakeRow{{
		name: "t",
		sql:  "CREATE TABLE db.t (`id` UInt64) ENGINE = Distributed('c', 'db', 'r', rand(), 'p', extra)",
	}}}
	err := processIntrospectRows(&DatabaseSpec{Name: "db"}, "db", rows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at most")
}
```

In `TestParseEngineString`'s table (introspect_test.go), after the
existing distributed case(s), add:

```go
		{
			"distributed_with_policy",
			"Distributed('posthog', 'default', 'events', rand(), 'tiered')",
			EngineDistributed{
				ClusterName:    "posthog",
				RemoteDatabase: "default",
				RemoteTable:    "events",
				ShardingKey:    ptr("rand()"),
				PolicyName:     ptr("tiered"),
			},
		},
```

- [ ] **Step 2: run — expect failures** (`PolicyName` undefined →
compile error):
`go test ./internal/loader/hcl -run 'TestIntrospect_Distributed|TestParseEngineString' -v`

- [ ] **Step 3: implement.** engines.go:

```go
type EngineDistributed struct {
	ClusterName    string  `hcl:"cluster_name"`
	RemoteDatabase string  `hcl:"remote_database"`
	RemoteTable    string  `hcl:"remote_table"`
	ShardingKey    *string `hcl:"sharding_key,optional"`
	PolicyName     *string `hcl:"policy_name,optional"`
}
```

introspect.go `engineFromAST` case `"Distributed"` becomes:

```go
	case "Distributed":
		if len(params) < 3 {
			return nil, nil, fmt.Errorf("engine Distributed needs (cluster, db, table[, sharding_key[, policy_name]])")
		}
		// Unknown extra parameters must abort, not silently drop (#109).
		if len(params) > 5 {
			return nil, nil, fmt.Errorf("engine Distributed takes at most (cluster, db, table, sharding_key, policy_name); got %v", params)
		}
		ee := EngineDistributed{ClusterName: params[0], RemoteDatabase: params[1], RemoteTable: params[2]}
		if len(params) > 3 {
			ee.ShardingKey = &params[3]
		}
		if len(params) > 4 {
			ee.PolicyName = &params[4]
		}
		return ee, allSettings, nil
```

`ParseEngineString`'s `strings.HasPrefix(decl, "Distributed")` case: the
same shape with `p` instead of `params` (keep both messages identical).

- [ ] **Step 4: run — expect pass**, same command as Step 2.

- [ ] **Step 5: commit** — `introspect: capture the Distributed policy_name parameter`.

---

### Task 2: emission — dump + sqlgen + fixture round-trip

**Files:**
- Modify: `internal/loader/hcl/dump.go` (~:362, after sharding_key)
- Modify: `internal/loader/hcl/sqlgen.go` (~:925 `case EngineDistributed`)
- Modify: `internal/loader/hcl/testdata/engines_all_kinds.hcl`
- Modify: `internal/loader/hcl/distributed_policy_name_test.go`

**Interfaces:** consumes `PolicyName` from Task 1.

- [ ] **Step 1: failing test** (append to distributed_policy_name_test.go):

```go
func TestSQLGen_Engine_DistributedPolicyName(t *testing.T) {
	clause, extra := engineSQL(EngineDistributed{
		ClusterName:    "posthog",
		RemoteDatabase: "default",
		RemoteTable:    "events",
		ShardingKey:    strPtr("sipHash64(id)"),
		PolicyName:     strPtr("tiered"),
	})
	assert.Equal(t, "Distributed('posthog', 'default', 'events', sipHash64(id), 'tiered')", clause)
	assert.Nil(t, extra)
}
```

Run: `go test ./internal/loader/hcl -run TestSQLGen_Engine_DistributedPolicyName -v` — FAIL
(clause lacks the 5th arg).

- [ ] **Step 2: implement.** sqlgen.go `case EngineDistributed`:

```go
	case EngineDistributed:
		if v.ShardingKey != nil {
			if v.PolicyName != nil {
				// policy_name is a string literal in DDL; sharding_key stays
				// a bare expression. PolicyName without ShardingKey cannot
				// reach here — the resolver rejects it (positional signature).
				return fmt.Sprintf("Distributed('%s', '%s', '%s', %s, '%s')", v.ClusterName, v.RemoteDatabase, v.RemoteTable, *v.ShardingKey, *v.PolicyName), nil
			}
			return fmt.Sprintf("Distributed('%s', '%s', '%s', %s)", v.ClusterName, v.RemoteDatabase, v.RemoteTable, *v.ShardingKey), nil
		}
		return fmt.Sprintf("Distributed('%s', '%s', '%s')", v.ClusterName, v.RemoteDatabase, v.RemoteTable), nil
```

dump.go, after the sharding_key block:

```go
		if v.PolicyName != nil {
			b.SetAttributeValue("policy_name", cty.StringVal(*v.PolicyName))
		}
```

engines_all_kinds.hcl — new table after `t_distributed` (round-trips via
`TestWrite_RoundTrip_AllEngineKinds`):

```hcl
  table "t_distributed_policy" {
    column "id" { type = "UUID" }
    engine "distributed" {
      cluster_name    = "posthog"
      remote_database = "default"
      remote_table    = "t_merge_tree"
      sharding_key    = "sipHash64(id)"
      policy_name     = "default"
    }
  }
```

- [ ] **Step 3: run** `go test ./internal/loader/hcl -v -run 'TestSQLGen_Engine_Distributed|TestWrite_RoundTrip'` — PASS.

- [ ] **Step 4: commit** — `sqlgen/dump: emit Distributed policy_name`.

---

### Task 3: resolver — policy_name requires sharding_key

**Files:**
- Modify: `internal/loader/hcl/resolver.go` (call at :36 block end; func
  after `validateReplacingEngines`)
- Modify: `internal/loader/hcl/distributed_policy_name_test.go`

- [ ] **Step 1: failing test:**

```go
// The Distributed signature is positional: policy_name can only be sent
// when sharding_key occupies the 4th slot.
func TestValidateDistributedEngines_PolicyRequiresShardingKey(t *testing.T) {
	s := &Schema{Databases: []DatabaseSpec{{
		Name: "db",
		Tables: []TableSpec{{
			Name: "t",
			Engine: &EngineSpec{Decoded: EngineDistributed{
				ClusterName:    "c",
				RemoteDatabase: "db",
				RemoteTable:    "r",
				PolicyName:     strPtr("tiered"),
			}},
		}},
	}}}
	err := validateDistributedEngines(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "policy_name requires sharding_key")
}
```

Run: FAIL (`validateDistributedEngines` undefined).

- [ ] **Step 2: implement.** After `validateReplacingEngines` in
resolver.go (call site directly below the :36 block):

```go
	if err := validateDistributedEngines(s); err != nil {
		return err
	}
```

```go
// validateDistributedEngines enforces the positional Distributed
// signature (cluster, db, table[, sharding_key[, policy_name]]): a
// policy_name can only be sent to ClickHouse when a sharding_key
// occupies the 4th slot.
func validateDistributedEngines(s *Schema) error {
	for _, db := range s.Databases {
		for _, t := range db.Tables {
			if t.Engine == nil || t.Engine.Decoded == nil {
				continue
			}
			e, ok := t.Engine.Decoded.(EngineDistributed)
			if !ok {
				continue
			}
			if e.PolicyName != nil && e.ShardingKey == nil {
				return fmt.Errorf("%s.%s: distributed engine: policy_name requires sharding_key (positional engine parameters)",
					db.Name, t.Name)
			}
		}
	}
	return nil
}
```

- [ ] **Step 3: run + commit** — `resolver: reject Distributed policy_name without sharding_key`.

---

### Task 4: storage_policy setting guard test (test-only)

**Files:** `internal/loader/hcl/distributed_policy_name_test.go`

- [ ] **Step 1: add test** (expected to pass immediately — it guards the
generic settings path, the only other table-DDL storage-policy surface):

```go
// storage_policy is a table SETTING, not an engine parameter; the
// generic settings round-trip must preserve it (docs sweep for #109).
func TestIntrospect_StoragePolicySettingPreserved(t *testing.T) {
	rows := &fakeRows{rows: []fakeRow{{
		name: "hot_cold",
		sql: "CREATE TABLE db.hot_cold (`id` UInt64) ENGINE = MergeTree ORDER BY id " +
			"SETTINGS index_granularity = 8192, storage_policy = 'tiered'",
	}}}
	db := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRows(db, "db", rows))
	require.Len(t, db.Tables, 1)
	assert.Equal(t, "tiered", db.Tables[0].Settings["storage_policy"])
}
```

- [ ] **Step 2: run; if it FAILS, stop — that's a new bug to report
before proceeding.** If it passes: commit —
`test: guard storage_policy setting round-trip`.

---

### Task 5: live coverage (best-effort), docs, verification sweep

**Files:**
- Modify: `test/hcl_introspect_live_test.go` (only if docker CH exposes
  a usable cluster — see Step 1)
- Modify: `docs/README.hcl.md` (Distributed attribute table row)
- Modify: `CLAUDE.md` (supported-engines line)

- [ ] **Step 1: probe live CH:** `docker compose up -d` running? Then
`clickhouse client -q "SELECT DISTINCT cluster FROM system.clusters"`.
If a cluster exists, add a Distributed-with-policy table to
`TestLive_HCLIntrospect` using that cluster name and policy `default`
(always present), mirroring #110's soft_deleted addition (DDL string +
expected `TableSpec` with decoded `EngineDistributed` incl. both
pointers via `utils.Ptr`). If no cluster/no docker: skip, and say so in
the PR test plan.
- [ ] **Step 2: docs.** README.hcl.md: add `policy_name` (optional) to
the Distributed engine attribute table, marked as requiring
`sharding_key`. CLAUDE.md: `Distributed (with optional sharding_key)` →
`Distributed (with optional sharding_key, policy_name)`.
- [ ] **Step 3: sweep:** `go test ./cmd/... ./internal/...`,
`go test ./test`, gofmt -s check, `go vet ./...`; live suite if docker
is up: `go test ./test -v -clickhouse -run TestLive_HCLIntrospect`.
- [ ] **Step 4: commit** — docs + live test; push branch; PR referencing
`Fixes #109`.
