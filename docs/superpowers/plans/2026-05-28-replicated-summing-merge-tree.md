# ReplicatedSummingMergeTree Engine Support — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `ReplicatedSummingMergeTree` to hclexp's supported engine list so PostHog's `posthog.sharded_distinct_id_usage` table round-trips.

**Architecture:** New `EngineReplicatedSummingMergeTree` Go struct following the existing one-type-per-engine convention; mirrors `EngineReplicatedReplacingMergeTree` field-by-field with `SumColumns` instead of `VersionColumn`. Wire it into HCL decode, two introspection switches, sqlgen, plus tests and docs.

**Tech Stack:** Go 1.26, `hashicorp/hcl/v2` (gohcl), `clickhouse-sql-parser` fork (chparser).

**Spec:** [`docs/superpowers/specs/2026-05-28-replicated-summing-merge-tree.md`](../specs/2026-05-28-replicated-summing-merge-tree.md)

**File Map:**

| File | What changes |
|---|---|
| `internal/loader/hcl/engines.go` | New `EngineReplicatedSummingMergeTree` type + `Kind()`; new `case` in `DecodeEngine`. |
| `internal/loader/hcl/introspect.go` | New `case "ReplicatedSummingMergeTree"` in `engineFromAST` (~line 487); new `case strings.HasPrefix(decl, "ReplicatedSummingMergeTree")` in the engine-from-text path (~line 723). |
| `internal/loader/hcl/sqlgen.go` | New `case EngineReplicatedSummingMergeTree:` in `engineSQL` (~line 545). |
| `internal/loader/hcl/engines_test.go` | HCL decode round-trip test. |
| `internal/loader/hcl/sqlgen_test.go` | DDL emission tests (with and without `sum_columns`). |
| `internal/loader/hcl/introspect_test.go` | AST-side round-trip from a CREATE TABLE string. |
| `test/testdata/posthog-create-statements/ReplicatedSummingMergeTree/sharded_distinct_id_usage.sql` | New live-test fixture. |
| `docs/README.hcl.md` | Supported engines paragraph or table. |
| `README.md` | Supported engines table. |
| `CLAUDE.md` (if present) | Supported engines bullet list. |

---

### Task 1: Engine type + HCL decode case

**Files:**
- Modify: `internal/loader/hcl/engines.go`
- Modify: `internal/loader/hcl/engines_test.go`

- [ ] **Step 1: Write the failing test**

In `internal/loader/hcl/engines_test.go`, find the existing test for `EngineReplicatedReplacingMergeTree` (search `replicated_replacing_merge_tree`). Right after it, add:

```go
func TestDecodeEngine_ReplicatedSummingMergeTree(t *testing.T) {
	hcl := `
table "t" {
  column "id" { type = "UInt64" }
  engine "replicated_summing_merge_tree" {
    zoo_path     = "/clickhouse/tables/{shard}/t"
    replica_name = "{replica}"
    sum_columns  = ["count"]
  }
}`
	t.Run("with sum_columns", func(t *testing.T) {
		e := decodeTableEngine(t, hcl)
		got, ok := e.(EngineReplicatedSummingMergeTree)
		require.True(t, ok, "expected EngineReplicatedSummingMergeTree, got %T", e)
		assert.Equal(t, "/clickhouse/tables/{shard}/t", got.ZooPath)
		assert.Equal(t, "{replica}", got.ReplicaName)
		assert.Equal(t, []string{"count"}, got.SumColumns)
		assert.Equal(t, "replicated_summing_merge_tree", got.Kind())
	})

	t.Run("without sum_columns", func(t *testing.T) {
		hcl := `
table "t" {
  column "id" { type = "UInt64" }
  engine "replicated_summing_merge_tree" {
    zoo_path     = "/clickhouse/tables/{shard}/t"
    replica_name = "{replica}"
  }
}`
		e := decodeTableEngine(t, hcl)
		got, ok := e.(EngineReplicatedSummingMergeTree)
		require.True(t, ok, "expected EngineReplicatedSummingMergeTree, got %T", e)
		assert.Empty(t, got.SumColumns)
	})
}
```

If `decodeTableEngine` doesn't exist in `engines_test.go`, search for the helper the existing replicated tests use. Most likely it's an inline shape: parse the HCL fragment via `hclparse.NewParser().ParseHCL(...)` then `gohcl.DecodeBody` into a `TableSpec` then call `DecodeEngine` on `tbl.Engine`. Reuse whatever helper the closest sibling test uses; if there is none, write a small `decodeTableEngine(t, src string) Engine` helper at the top of the file:

```go
func decodeTableEngine(t *testing.T, src string) Engine {
	t.Helper()
	p := hclparse.NewParser()
	f, diags := p.ParseHCL([]byte(src), "test.hcl")
	require.False(t, diags.HasErrors(), "parse: %v", diags)
	var spec struct {
		Tables []TableSpec `hcl:"table,block"`
	}
	diags = gohcl.DecodeBody(f.Body, nil, &spec)
	require.False(t, diags.HasErrors(), "decode: %v", diags)
	require.Len(t, spec.Tables, 1)
	require.NotNil(t, spec.Tables[0].Engine)
	decoded, err := DecodeEngine(spec.Tables[0].Engine)
	require.NoError(t, err)
	return decoded
}
```

(Required imports: `"github.com/hashicorp/hcl/v2/hclparse"` and `"github.com/hashicorp/hcl/v2/gohcl"`.)

- [ ] **Step 2: Run the test to verify it fails**

```
go test ./internal/loader/hcl -run TestDecodeEngine_ReplicatedSummingMergeTree -v
```

Expected: FAIL — `EngineReplicatedSummingMergeTree` undefined.

- [ ] **Step 3: Add the type**

In `internal/loader/hcl/engines.go`, immediately after the existing `EngineSummingMergeTree` block (line 42-46), insert:

```go
type EngineReplicatedSummingMergeTree struct {
	ZooPath     string   `hcl:"zoo_path"`
	ReplicaName string   `hcl:"replica_name"`
	SumColumns  []string `hcl:"sum_columns,optional"`
}

func (EngineReplicatedSummingMergeTree) Kind() string {
	return "replicated_summing_merge_tree"
}
```

- [ ] **Step 4: Add the DecodeEngine case**

In the same file, find the `case "replicated_replacing_merge_tree"` arm in `DecodeEngine`. The `case "summing_merge_tree"` arm should be nearby — typically right after. Add a new arm:

```go
case "replicated_summing_merge_tree":
	var e EngineReplicatedSummingMergeTree
	diags = gohcl.DecodeBody(spec.Body, nil, &e)
	target = e
```

Place it right after the `summing_merge_tree` case so it's grouped with its non-replicated sibling.

- [ ] **Step 5: Re-run**

```
go test ./internal/loader/hcl -run TestDecodeEngine_ReplicatedSummingMergeTree -v
```

Expected: both subtests PASS.

- [ ] **Step 6: Commit**

```
git add internal/loader/hcl/engines.go internal/loader/hcl/engines_test.go
git commit -m "feat(hcl): EngineReplicatedSummingMergeTree type + HCL decode

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: SQL generation

**Files:**
- Modify: `internal/loader/hcl/sqlgen.go`
- Modify: `internal/loader/hcl/sqlgen_test.go`

- [ ] **Step 1: Write the failing tests**

In `internal/loader/hcl/sqlgen_test.go`, search for `EngineReplicatedAggregatingMergeTree` to find where existing replicated-engine sqlgen tests live. Add right after them:

```go
func TestSQLGen_Engine_ReplicatedSummingMergeTree(t *testing.T) {
	t.Run("with sum_columns", func(t *testing.T) {
		clause, extra := engineSQL(EngineReplicatedSummingMergeTree{
			ZooPath:     "/clickhouse/tables/{shard}/distinct_id_usage",
			ReplicaName: "{replica}",
			SumColumns:  []string{"count"},
		})
		assert.Equal(t, "ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/distinct_id_usage', '{replica}', (count))", clause)
		assert.Nil(t, extra)
	})

	t.Run("without sum_columns", func(t *testing.T) {
		clause, extra := engineSQL(EngineReplicatedSummingMergeTree{
			ZooPath:     "/clickhouse/tables/{shard}/distinct_id_usage",
			ReplicaName: "{replica}",
		})
		assert.Equal(t, "ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/distinct_id_usage', '{replica}')", clause)
		assert.Nil(t, extra)
	})

	t.Run("with multiple sum_columns", func(t *testing.T) {
		clause, extra := engineSQL(EngineReplicatedSummingMergeTree{
			ZooPath:     "/zk",
			ReplicaName: "r",
			SumColumns:  []string{"a", "b"},
		})
		assert.Equal(t, "ReplicatedSummingMergeTree('/zk', 'r', (a, b))", clause)
		assert.Nil(t, extra)
	})
}
```

The tuple-wrapping convention `(a, b)` mirrors the existing `EngineSummingMergeTree` emission (`sqlgen.go:535`), which is the ClickHouse-native form for a `Tuple` columns argument.

- [ ] **Step 2: Run the tests**

```
go test ./internal/loader/hcl -run TestSQLGen_Engine_ReplicatedSummingMergeTree -v
```

Expected: FAIL — `engineSQL` falls through default for the unknown type.

- [ ] **Step 3: Add the sqlgen arm**

In `internal/loader/hcl/sqlgen.go`, find the existing `case EngineReplicatedAggregatingMergeTree:` (around line 544). Add a new arm directly after the existing `EngineSummingMergeTree` arm (around line 533–537) so the replicated/non-replicated pair stays together. The full insertion:

```go
case EngineReplicatedSummingMergeTree:
	if len(v.SumColumns) > 0 {
		return fmt.Sprintf("ReplicatedSummingMergeTree('%s', '%s', (%s))", v.ZooPath, v.ReplicaName, strings.Join(v.SumColumns, ", ")), nil
	}
	return fmt.Sprintf("ReplicatedSummingMergeTree('%s', '%s')", v.ZooPath, v.ReplicaName), nil
```

- [ ] **Step 4: Re-run**

```
go test ./internal/loader/hcl -run TestSQLGen_Engine_ReplicatedSummingMergeTree -v
```

Expected: all three subtests PASS.

- [ ] **Step 5: Commit**

```
git add internal/loader/hcl/sqlgen.go internal/loader/hcl/sqlgen_test.go
git commit -m "feat(hcl): emit ReplicatedSummingMergeTree DDL

Mirrors EngineSummingMergeTree's tuple-wrapped column emission;
zoo_path and replica_name are quoted string literals, sum_columns
are bare column identifiers inside a Tuple.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Introspection — AST path

**Files:**
- Modify: `internal/loader/hcl/introspect.go`
- Modify: `internal/loader/hcl/introspect_test.go`

- [ ] **Step 1: Write the failing test**

In `internal/loader/hcl/introspect_test.go`, search for `ReplicatedReplacingMergeTree` to find the existing replicated-engine introspection tests. Right after the closest one, add:

```go
func TestEngineFromAST_ReplicatedSummingMergeTree(t *testing.T) {
	t.Run("with sum_columns", func(t *testing.T) {
		const create = `CREATE TABLE db.t (` + "`id`" + ` UInt64, ` + "`count`" + ` UInt64) ` +
			`ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/t', '{replica}', count) ` +
			`ORDER BY id`
		ts, err := buildTableFromCreateSQL(create)
		require.NoError(t, err)
		require.NotNil(t, ts.Engine)
		got, ok := ts.Engine.Decoded.(EngineReplicatedSummingMergeTree)
		require.True(t, ok, "expected EngineReplicatedSummingMergeTree, got %T", ts.Engine.Decoded)
		assert.Equal(t, "/clickhouse/tables/{shard}/t", got.ZooPath)
		assert.Equal(t, "{replica}", got.ReplicaName)
		assert.Equal(t, []string{"count"}, got.SumColumns)
	})

	t.Run("without sum_columns", func(t *testing.T) {
		const create = `CREATE TABLE db.t (` + "`id`" + ` UInt64) ` +
			`ENGINE = ReplicatedSummingMergeTree('/zk', 'r') ` +
			`ORDER BY id`
		ts, err := buildTableFromCreateSQL(create)
		require.NoError(t, err)
		got := ts.Engine.Decoded.(EngineReplicatedSummingMergeTree)
		assert.Equal(t, "/zk", got.ZooPath)
		assert.Equal(t, "r", got.ReplicaName)
		assert.Empty(t, got.SumColumns)
	})
}
```

If `buildTableFromCreateSQL` isn't directly accessible from tests, use whatever helper the existing `ReplicatedReplacingMergeTree` introspect tests use to drive `engineFromAST` (likely the same `buildTableFromCreateSQL` since it's exported at package scope).

- [ ] **Step 2: Run the tests**

```
go test ./internal/loader/hcl -run TestEngineFromAST_ReplicatedSummingMergeTree -v
```

Expected: FAIL — the AST switch returns `unsupported engine: ReplicatedSummingMergeTree`.

- [ ] **Step 3: Add the case to the AST switch**

In `internal/loader/hcl/introspect.go`, find the existing `case "ReplicatedAggregatingMergeTree":` arm (around line 483). Insert a new arm directly after the existing `SummingMergeTree` case (around line 469-470) so the replicated/non-replicated pair stays together:

```go
case "ReplicatedSummingMergeTree":
	if len(params) < 2 {
		return nil, nil, fmt.Errorf("ReplicatedSummingMergeTree needs at least (zoo_path, replica_name[, sum_columns...])")
	}
	ee := EngineReplicatedSummingMergeTree{ZooPath: params[0], ReplicaName: params[1]}
	if len(params) > 2 {
		ee.SumColumns = params[2:]
	}
	return ee, allSettings, nil
```

- [ ] **Step 4: Re-run**

```
go test ./internal/loader/hcl -run TestEngineFromAST_ReplicatedSummingMergeTree -v
```

Expected: both subtests PASS.

- [ ] **Step 5: Commit**

```
git add internal/loader/hcl/introspect.go internal/loader/hcl/introspect_test.go
git commit -m "feat(hcl): introspect ReplicatedSummingMergeTree (AST path)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Introspection — text-fallback path

**Files:**
- Modify: `internal/loader/hcl/introspect.go`
- Modify: `internal/loader/hcl/introspect_test.go`

- [ ] **Step 1: Find the second engine-parse code path**

```
grep -n "strings.HasPrefix(decl,\s*\"Replicated" internal/loader/hcl/introspect.go
```

This is the text-based fallback (~line 692 for ReplicatedReplacingMergeTree, ~line 705 for ReplicatedCollapsingMergeTree, ~line 714 for ReplicatedAggregatingMergeTree). It's the path that runs when the AST is unavailable. SummingMergeTree itself uses `parseSummingMergeTreeHCL` (line 733-734).

- [ ] **Step 2: Write the failing test**

The text-fallback path isn't always trivial to exercise directly — look for an existing test that hits `case strings.HasPrefix(decl, "ReplicatedAggregatingMergeTree")` to find the entry function (probably `parseEngineFromHCL` or similar — search:

```
grep -n "func .*\(decl string\) (Engine\|parseEngine" internal/loader/hcl/introspect.go
```

Mirror that test's shape:

```go
func TestParseEngineFromHCL_ReplicatedSummingMergeTree(t *testing.T) {
	t.Run("with sum_columns", func(t *testing.T) {
		e, err := <parseFunc>(`ReplicatedSummingMergeTree('/zk', 'r', count)`)
		require.NoError(t, err)
		got := e.(EngineReplicatedSummingMergeTree)
		assert.Equal(t, "/zk", got.ZooPath)
		assert.Equal(t, "r", got.ReplicaName)
		assert.Equal(t, []string{"count"}, got.SumColumns)
	})

	t.Run("without sum_columns", func(t *testing.T) {
		e, err := <parseFunc>(`ReplicatedSummingMergeTree('/zk', 'r')`)
		require.NoError(t, err)
		got := e.(EngineReplicatedSummingMergeTree)
		assert.Empty(t, got.SumColumns)
	})
}
```

Replace `<parseFunc>` with the actual function name found above. If the parse function is unexported and unreached from a test, skip this task's test file edit and rely on Task 5's live test to exercise the path.

- [ ] **Step 3: Run the test**

```
go test ./internal/loader/hcl -run TestParseEngineFromHCL_ReplicatedSummingMergeTree -v
```

Expected: FAIL.

- [ ] **Step 4: Add the case**

In `internal/loader/hcl/introspect.go`, immediately after the `case strings.HasPrefix(decl, "ReplicatedAggregatingMergeTree"):` arm (around line 714-722), insert:

```go
case strings.HasPrefix(decl, "ReplicatedSummingMergeTree"):
	p, err := extractEngineParams(decl)
	if err != nil {
		return nil, err
	}
	if len(p) < 2 {
		return nil, fmt.Errorf("ReplicatedSummingMergeTree needs at least (zoo_path, replica_name[, sum_columns...]); got %v", p)
	}
	e := EngineReplicatedSummingMergeTree{ZooPath: p[0], ReplicaName: p[1]}
	if len(p) > 2 {
		e.SumColumns = p[2:]
	}
	return e, nil
```

This must be ordered BEFORE the existing `case strings.HasPrefix(decl, "SummingMergeTree"):` arm — Go's `case` evaluation in a `switch true {}` is sequential and `"ReplicatedSummingMergeTree"` also has prefix `"R"` matching the Replicated checks above. Verify by viewing the switch structure around line 705-734.

The `extractEngineParams` helper already handles `(arg1, arg2, ...)` tokenisation — same path Replacing/Collapsing/Aggregating use.

- [ ] **Step 5: Re-run**

```
go test ./internal/loader/hcl -run TestParseEngineFromHCL_ReplicatedSummingMergeTree -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```
git add internal/loader/hcl/introspect.go internal/loader/hcl/introspect_test.go
git commit -m "feat(hcl): introspect ReplicatedSummingMergeTree (text-fallback path)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Live fixture + integration test

**Files:**
- Create: `test/testdata/posthog-create-statements/ReplicatedSummingMergeTree/sharded_distinct_id_usage.sql`

- [ ] **Step 1: Verify the live-test discovery walks all engine subdirectories**

```
grep -n "filepath.Walk\|posthog-create-statements" test/introspection_live_test.go | head -3
```

`TestLive_Introspection_AllStatements` walks every subdirectory of `test/testdata/posthog-create-statements/`, so adding a new directory + `.sql` file automatically registers a new subtest under `ReplicatedSummingMergeTree`. No test code edits required for this task — only a fixture.

- [ ] **Step 2: Write the fixture**

Use the production-shape statement. If the local ClickHouse is running with the production schema, capture it via:

```
docker exec posthog-clickhouse-1 clickhouse-client --query "SELECT create_table_query FROM system.tables WHERE database = 'posthog' AND name = 'sharded_distinct_id_usage'"
```

Replace the database prefix from `posthog` to `default` (the fixture convention — the test runner rewrites `default` to the per-test database).

If the production schema isn't available, use this hand-rolled fixture that mirrors PostHog's shape:

`test/testdata/posthog-create-statements/ReplicatedSummingMergeTree/sharded_distinct_id_usage.sql`:

```
CREATE TABLE default.sharded_distinct_id_usage (`team_id` Int64, `distinct_id` String, `count` UInt64) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/posthog.distinct_id_usage', '{replica}', (count)) ORDER BY (team_id, distinct_id) SETTINGS index_granularity = 8192
```

- [ ] **Step 3: Run the live test**

Start the docker compose ClickHouse if not running:

```
docker compose up -d --wait
```

Then:

```
ENABLE_CLICKHOUSE=1 go test ./test -run 'TestLive_Introspection_AllStatements/ReplicatedSummingMergeTree' -v 2>&1 | tail -10
```

Expected: `--- PASS: TestLive_Introspection_AllStatements/ReplicatedSummingMergeTree/sharded_distinct_id_usage`.

- [ ] **Step 4: Run the full live suite**

```
ENABLE_CLICKHOUSE=1 go test -count=1 ./test/...
```

Expected: all green.

- [ ] **Step 5: Commit**

```
git add test/testdata/posthog-create-statements/ReplicatedSummingMergeTree/
git commit -m "test(live): ReplicatedSummingMergeTree fixture (sharded_distinct_id_usage)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Docs

**Files:**
- Modify: `docs/README.hcl.md`
- Modify: `README.md`
- Modify: `CLAUDE.md` (only if it exists at repo root)

- [ ] **Step 1: Update README.md supported-engines table**

```
grep -n "ReplicatedAggregatingMergeTree\|ReplicatedReplacingMergeTree\|ReplicatedCollapsingMergeTree" README.md
```

Find the supported-engines table or list (most likely in a "Supported Table Engines" subsection or "Engine blocks" reference table). Add a row/entry for `replicated_summing_merge_tree` between the existing `summing_merge_tree` and `replicated_collapsing_merge_tree` rows (or following the alphabetical/family-grouping convention that's already in place).

If the table has columns "Kind | Attributes", the new row is:

```
| `replicated_summing_merge_tree` | `zoo_path`, `replica_name`, `sum_columns` |
```

- [ ] **Step 2: Update docs/README.hcl.md supported-engines list**

```
grep -n "summing_merge_tree\|replicated_replacing_merge_tree" docs/README.hcl.md
```

Find the engine list/reference. Add `replicated_summing_merge_tree` in the same family-grouped position. If the doc enumerates ClickHouse engine syntax round-trips, also add:

```
- `engine "replicated_summing_merge_tree" { zoo_path = "...", replica_name = "...", sum_columns = ["a", "b"] }`
  → `ENGINE = ReplicatedSummingMergeTree('zoo_path', 'replica_name', (a, b))`
```

- [ ] **Step 3: Update CLAUDE.md (if present)**

```
test -f CLAUDE.md && grep -n "ReplicatedAggregating\|ReplicatedReplacing" CLAUDE.md
```

If CLAUDE.md exists and lists supported engines, add `ReplicatedSummingMergeTree` to the list in family-grouped order. If it doesn't exist or doesn't enumerate engines, skip this step.

- [ ] **Step 4: Commit**

```
git add docs/README.hcl.md README.md $(ls CLAUDE.md 2>/dev/null)
git commit -m "docs: list ReplicatedSummingMergeTree as a supported engine

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Verification + push

- [ ] **Step 1: Run the full test suite**

```
go test ./internal/... ./config ./cmd/... ./test/...
```

Expected: all green.

- [ ] **Step 2: gofmt / go vet**

```
gofmt -s -l . && echo "fmt clean"
go vet ./...
```

Expected: `fmt clean` and no vet issues.

- [ ] **Step 3: Confirm coverage in the live suite**

```
ENABLE_CLICKHOUSE=1 go test -count=1 -v ./test -run 'TestLive_Introspection_AllStatements/ReplicatedSummingMergeTree' 2>&1 | tail -5
```

Expected: PASS for `sharded_distinct_id_usage`.

- [ ] **Step 4: Push the branch**

This task lives on the view-support worktree (the next PR can stack on top). If a separate branch is preferred, the engineer running this plan should resolve before push. Otherwise:

```
git push -u origin HEAD
```

- [ ] **Step 5: STOP — pause for PR creation/review**

Open the PR via `gh pr create` or the GitHub UI. The PR body should call out:
- Single new replicated-engine variant; fills the only gap in the MergeTree pair list.
- Production fixture `sharded_distinct_id_usage` round-trips.
- No infrastructure or external dependency changes.
