# TimeSeries Engine Support — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** PostHog's `posthog.prom_metrics` (and any other CH TimeSeries-engine table) round-trips end-to-end through `hclexp introspect` / `hclexp diff` / `hclexp validate`.

**Architecture:** New `EngineTimeSeries` value type holding optional `samples`/`tags`/`metrics` sub-blocks (each either an external table ref or an inline `INNER COLUMNS`+`INNER ENGINE` body), promoted `tags_to_columns` map, free-form `settings` map. Wired into the existing engine-decode → resolve → introspect → sqlgen → diff → validate pipeline at the same hook points every other engine uses. Inner-form target engines reuse the MergeTree-family struct types we already have.

**Tech Stack:** Go 1.26, `hashicorp/hcl/v2` (gohcl), `orian/clickhouse-sql-parser` (chparser), `ClickHouse/clickhouse-go/v2` driver.

**Spec:** [`docs/superpowers/specs/2026-06-01-timeseries-engine-support.md`](../specs/2026-06-01-timeseries-engine-support.md)

**Upstream chparser change:** [orian/clickhouse-sql-parser#8](https://github.com/orian/clickhouse-sql-parser/issues/8) — landed as commit `47aa1ff` on `refactor-visitor`. AST: `CreateTable.TimeSeriesTargets []*TimeSeriesTargetClause`, each with `Kind` (lowercase normalised), `Keyword` (verbatim), `External *TableIdentifier`, `InnerColumns *TableSchemaClause`, `InnerEngine *EngineExpr`. Pin is already bumped on this branch (`go.mod` replace points at `v0.0.0-20260601104648-4521cd37e69b`).

**File Map:**

| File | What changes |
|---|---|
| `internal/loader/hcl/engines.go` | `EngineTimeSeries`, `TimeSeriesTarget`, `TimeSeriesInnerTable` types + `Kind()`; `DecodeEngine` case; `Virtuals()` returns nil. |
| `internal/loader/hcl/engines_test.go` | HCL-decode round-trip tests for external, inner, mixed, settings, tags_to_columns. |
| `internal/loader/hcl/resolver.go` | `resolveTimeSeriesEngine` helper invoked by the existing engine pass; enforces both-or-neither per target, restricts inner engines to MergeTree-family. |
| `internal/loader/hcl/resolver_test.go` | Resolver rule tests. |
| `internal/loader/hcl/introspect.go` | `buildTimeSeriesFromCreateTable`; dispatch from `processIntrospectRows`; populate `KeywordHint`. |
| `internal/loader/hcl/introspect_test.go` | Introspect round-trip from CREATE strings (4 fixtures). |
| `internal/loader/hcl/sqlgen.go` | `createTimeSeriesEngineSQL`; tail-clause emission in `createTableSQL`. |
| `internal/loader/hcl/sqlgen_test.go` | Emission tests including KeywordHint behaviour. |
| `internal/loader/hcl/diff.go` | `TimeSeriesDiff`; ALTER-vs-recreate split; folded into `TableDiff`. |
| `internal/loader/hcl/diff_test.go` | Diff bucket tests. |
| `internal/loader/hcl/validate.go` | `DepTimeSeriesTarget = "ts_target"`; `CollectDependencies` extended. |
| `internal/loader/hcl/validate_test.go` | External-form dependency check; inner-form has none. |
| `test/testdata/posthog-create-statements/TimeSeries/prom_metrics_external.sql` | Real production CREATE. |
| `test/testdata/posthog-create-statements/TimeSeries/prom_metrics_inner.sql` | Doc-default inner form (all 3 INNER COLUMNS + INNER ENGINE). |
| `test/testdata/posthog-create-statements/TimeSeries/prom_metrics_minimal.sql` | Bare `ENGINE = TimeSeries`. |
| `docs/README.hcl.md` | New "TimeSeries" engine entry under Engine kinds. |
| `docs/FAQ.md` | "How do I model a Prometheus TimeSeries table?" |

---

### Task 1: Engine types + HCL decode

**Files:**
- Modify: `internal/loader/hcl/engines.go` (insert after `EngineKafka`)
- Modify: `internal/loader/hcl/engines_test.go`

- [ ] **Step 1: Add the types**

After `func (EngineKafka) Kind() string { return "kafka" }` in `engines.go`, add:

```go
// EngineTimeSeries models the (experimental) ClickHouse TimeSeries engine.
// It stores Prometheus-style time series across three sibling target tables
// (samples/tags/metrics), each either a reference to an external CH table or
// an inline declaration with INNER COLUMNS + INNER ENGINE.
//
// Virtual columns: none; TimeSeries exposes no engine-virtual columns. Outer
// table columns are real declared columns.
type EngineTimeSeries struct {
	// Settings declared via `engine "time_series" { settings = {...} }`.
	// Free-form for forward compatibility; only two are ALTER-able
	// (id_generator, filter_by_min_time_and_max_time) — the rest are baked
	// into the CREATE.
	Settings map[string]string `hcl:"settings,optional"`

	// TagsToColumns shapes the inner tags-target table by promoting specific
	// label keys into their own columns. CH treats this as a setting at the
	// SQL level; promoted here for HCL clarity. sqlgen folds it back into
	// the SETTINGS clause at emit time.
	TagsToColumns map[string]string `hcl:"tags_to_columns,optional"`

	// Sub-block per target kind. Nil = CH default applies (auto-generate the
	// inner target).
	Samples *TimeSeriesTarget `hcl:"samples,block"`
	Tags    *TimeSeriesTarget `hcl:"tags,block"`
	Metrics *TimeSeriesTarget `hcl:"metrics,block"`

	// KeywordHint preserves whether the samples target's source SQL used
	// SAMPLES or DATA. Populated by the introspector; sqlgen reads it to
	// round-trip the same word. HCL authors never set this.
	KeywordHint string `hcl:"-" diff:"-"`
}

func (EngineTimeSeries) Kind() string { return "time_series" }

func (EngineTimeSeries) Virtuals() []DeclaredColumn { return nil }

// TimeSeriesTarget is one of {samples, tags, metrics}. Exactly one of
// Target or Inner must be set; both/neither is a resolve-time error.
type TimeSeriesTarget struct {
	Target *string                `hcl:"target,optional"`
	Inner  *TimeSeriesInnerTable  `hcl:"inner,block"`
}

// TimeSeriesInnerTable mirrors a small slice of TableSpec — only the fields
// CH actually emits inside INNER COLUMNS / INNER ENGINE. No
// abstract/extend/cluster: those are outer-table concerns.
type TimeSeriesInnerTable struct {
	Columns     []ColumnSpec      `hcl:"column,block"`
	Engine      *EngineSpec       `hcl:"engine,block"`
	PrimaryKey  []string          `hcl:"primary_key,optional"`
	OrderBy     []string          `hcl:"order_by,optional"`
	PartitionBy *string           `hcl:"partition_by,optional"`
	Settings    map[string]string `hcl:"settings,optional"`
}
```

- [ ] **Step 2: Wire into `DecodeEngine`**

In the `switch spec.Kind` block in `DecodeEngine` (same file, around line 191 where `"kafka"` lives), add:

```go
case "time_series":
	var e EngineTimeSeries
	diags = gohcl.DecodeBody(spec.Body, nil, &e)
	if diags.HasErrors() {
		break
	}
	// Recursively decode inner engines so resolver/sqlgen see typed values.
	for _, t := range []*TimeSeriesTarget{e.Samples, e.Tags, e.Metrics} {
		if t == nil || t.Inner == nil || t.Inner.Engine == nil {
			continue
		}
		inner, err := DecodeEngine(t.Inner.Engine)
		if err != nil {
			return nil, fmt.Errorf("time_series inner engine: %w", err)
		}
		t.Inner.Engine.Decoded = inner
	}
	target = e
```

- [ ] **Step 3: Write decode tests**

Add to `engines_test.go` (after the Kafka tests). Use the same `decodeTableEngine` helper as the other engine tests (it's already there).

```go
func TestDecodeEngine_TimeSeries_External(t *testing.T) {
	e := decodeTableEngine(t, `
table "m" {
  engine "time_series" {
    settings = { id_generator = "sipHash64(metric_name, all_tags)" }
    samples { target = "db.m_data" }
    tags    { target = "db.m_tags" }
    metrics { target = "db.m_metrics" }
  }
}`)
	got, ok := e.(EngineTimeSeries)
	require.True(t, ok)
	assert.Equal(t, "time_series", got.Kind())
	assert.Equal(t, "sipHash64(metric_name, all_tags)", got.Settings["id_generator"])
	require.NotNil(t, got.Samples)
	require.NotNil(t, got.Samples.Target)
	assert.Equal(t, "db.m_data", *got.Samples.Target)
	assert.Nil(t, got.Samples.Inner)
}

func TestDecodeEngine_TimeSeries_Inner(t *testing.T) {
	e := decodeTableEngine(t, `
table "m" {
  engine "time_series" {
    samples {
      inner {
        column "id"        { type = "UUID" }
        column "timestamp" { type = "DateTime64(3)" }
        column "value"     { type = "Float64" }
        engine "merge_tree" {}
        order_by = ["id", "timestamp"]
      }
    }
  }
}`)
	got := e.(EngineTimeSeries)
	require.NotNil(t, got.Samples)
	require.NotNil(t, got.Samples.Inner)
	assert.Nil(t, got.Samples.Target)
	require.NotNil(t, got.Samples.Inner.Engine)
	_, ok := got.Samples.Inner.Engine.Decoded.(EngineMergeTree)
	assert.True(t, ok, "inner engine should be decoded recursively")
	assert.Equal(t, []string{"id", "timestamp"}, got.Samples.Inner.OrderBy)
	require.Len(t, got.Samples.Inner.Columns, 3)
}

func TestDecodeEngine_TimeSeries_TagsToColumns(t *testing.T) {
	e := decodeTableEngine(t, `
table "m" {
  engine "time_series" {
    tags_to_columns = { instance = "instance", job = "job" }
  }
}`)
	got := e.(EngineTimeSeries)
	assert.Equal(t, map[string]string{"instance": "instance", "job": "job"}, got.TagsToColumns)
}

func TestDecodeEngine_TimeSeries_BareEmpty(t *testing.T) {
	e := decodeTableEngine(t, `
table "m" {
  engine "time_series" {}
}`)
	got := e.(EngineTimeSeries)
	assert.Nil(t, got.Samples)
	assert.Nil(t, got.Tags)
	assert.Nil(t, got.Metrics)
}
```

- [ ] **Step 4: Run + commit**

```bash
go test ./internal/loader/hcl/ -run TestDecodeEngine_TimeSeries -v
git add internal/loader/hcl/engines.go internal/loader/hcl/engines_test.go
git commit -m "feat(hcl): EngineTimeSeries type + DecodeEngine wiring"
```

---

### Task 2: Resolver — both-or-neither + inner-engine restriction

**Files:**
- Modify: `internal/loader/hcl/resolver.go`
- Modify: `internal/loader/hcl/resolver_test.go`

- [ ] **Step 1: Write failing tests**

In `resolver_test.go`, add:

```go
func TestResolve_TimeSeries_BothExternalAndInnerOnSameTarget_Rejected(t *testing.T) {
	tgt := "db.x"
	e := EngineTimeSeries{Samples: &TimeSeriesTarget{
		Target: &tgt,
		Inner:  &TimeSeriesInnerTable{Engine: &EngineSpec{Decoded: EngineMergeTree{}}},
	}}
	s := &Schema{Databases: []DatabaseSpec{{
		Name: "db",
		Tables: []TableSpec{{Name: "m",
			Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
			Engine:  &EngineSpec{Kind: "time_series", Decoded: e},
		}},
	}}}
	err := Resolve(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "samples")
	assert.Contains(t, err.Error(), "either")
}

func TestResolve_TimeSeries_NeitherForm_Rejected(t *testing.T) {
	e := EngineTimeSeries{Samples: &TimeSeriesTarget{}}
	s := &Schema{Databases: []DatabaseSpec{{
		Name: "db",
		Tables: []TableSpec{{Name: "m",
			Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
			Engine:  &EngineSpec{Kind: "time_series", Decoded: e},
		}},
	}}}
	err := Resolve(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "samples")
}

func TestResolve_TimeSeries_InnerEngineRestrictedToMergeTreeFamily(t *testing.T) {
	// Kafka as inner engine on samples — should be rejected.
	e := EngineTimeSeries{Samples: &TimeSeriesTarget{
		Inner: &TimeSeriesInnerTable{Engine: &EngineSpec{Decoded: EngineKafka{}}},
	}}
	s := &Schema{Databases: []DatabaseSpec{{
		Name: "db",
		Tables: []TableSpec{{Name: "m",
			Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
			Engine:  &EngineSpec{Kind: "time_series", Decoded: e},
		}},
	}}}
	err := Resolve(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inner engine")
	assert.Contains(t, err.Error(), "MergeTree")
}

func TestResolve_TimeSeries_AllValidForms_OK(t *testing.T) {
	tgt := "db.m_data"
	e := EngineTimeSeries{
		Samples: &TimeSeriesTarget{Target: &tgt},
		Tags: &TimeSeriesTarget{Inner: &TimeSeriesInnerTable{
			Columns: []ColumnSpec{{Name: "id", Type: "UUID"}},
			Engine:  &EngineSpec{Decoded: EngineAggregatingMergeTree{}},
		}},
	}
	s := &Schema{Databases: []DatabaseSpec{{
		Name: "db",
		Tables: []TableSpec{{Name: "m",
			Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
			Engine:  &EngineSpec{Kind: "time_series", Decoded: e},
		}},
	}}}
	require.NoError(t, Resolve(s))
}
```

- [ ] **Step 2: Implement the resolver hook**

In `resolver.go`, add a new helper and call it from the existing per-table validation loop in `resolveDatabase` (just before the `for _, t := range db.Tables` block that validates engine + columns):

```go
// validateTimeSeriesEngine enforces the engine-specific resolve rules for
// EngineTimeSeries: each target sub-block must have exactly one of Target
// or Inner; inner engines must be MergeTree-family kinds.
func validateTimeSeriesEngine(dbName, tableName string, e EngineTimeSeries) error {
	for _, kv := range []struct {
		kind string
		t    *TimeSeriesTarget
	}{
		{"samples", e.Samples},
		{"tags", e.Tags},
		{"metrics", e.Metrics},
	} {
		if kv.t == nil {
			continue
		}
		hasExternal := kv.t.Target != nil
		hasInner := kv.t.Inner != nil
		switch {
		case hasExternal && hasInner:
			return fmt.Errorf("%s.%s: time_series %s target: set either target or inner, not both",
				dbName, tableName, kv.kind)
		case !hasExternal && !hasInner:
			return fmt.Errorf("%s.%s: time_series %s target: must set target or inner",
				dbName, tableName, kv.kind)
		}
		if hasInner && kv.t.Inner.Engine != nil && kv.t.Inner.Engine.Decoded != nil {
			if !isMergeTreeFamily(kv.t.Inner.Engine.Decoded) {
				return fmt.Errorf("%s.%s: time_series %s inner engine %q: only MergeTree-family engines are allowed",
					dbName, tableName, kv.kind, kv.t.Inner.Engine.Decoded.Kind())
			}
		}
	}
	return nil
}

func isMergeTreeFamily(e Engine) bool {
	switch e.(type) {
	case EngineMergeTree,
		EngineReplicatedMergeTree,
		EngineReplacingMergeTree,
		EngineReplicatedReplacingMergeTree,
		EngineSummingMergeTree,
		EngineReplicatedSummingMergeTree,
		EngineCollapsingMergeTree,
		EngineReplicatedCollapsingMergeTree,
		EngineAggregatingMergeTree,
		EngineReplicatedAggregatingMergeTree:
		return true
	}
	return false
}
```

Then, in `resolveDatabase`'s per-table validation loop (the one that checks engine+columns+constraints, around line 208 of the current file), call into the new helper:

```go
for _, t := range db.Tables {
	if t.Engine == nil || t.Engine.Decoded == nil {
		return fmt.Errorf("%s.%s: non-abstract table requires an engine", db.Name, t.Name)
	}
	if ts, ok := t.Engine.Decoded.(EngineTimeSeries); ok {
		if err := validateTimeSeriesEngine(db.Name, t.Name, ts); err != nil {
			return err
		}
	}
	if err := validateColumns(db.Name, t); err != nil {
		return err
	}
	if err := validateConstraints(db.Name, t); err != nil {
		return err
	}
}
```

- [ ] **Step 3: Run + commit**

```bash
go test ./internal/loader/hcl/ -run TestResolve_TimeSeries -v
git add internal/loader/hcl/resolver.go internal/loader/hcl/resolver_test.go
git commit -m "feat(hcl): resolve-time validation for TimeSeries engine targets"
```

---

### Task 3: Introspection — buildTimeSeriesFromCreateTable

**Files:**
- Modify: `internal/loader/hcl/introspect.go`
- Modify: `internal/loader/hcl/introspect_test.go`

- [ ] **Step 1: Write failing tests**

```go
func TestIntrospect_TimeSeries_External(t *testing.T) {
	sql := "CREATE TABLE db.m (`id` UUID, `timestamp` DateTime64(3), `value` Float64, " +
		"`metric_name` LowCardinality(String)) " +
		"ENGINE = TimeSeries DATA db.m_data TAGS db.m_tags METRICS db.m_metrics"
	db := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRows(db, "db", &stringRows{rows: [][2]string{{"m", sql}}}))
	require.Len(t, db.Tables, 1)
	tbl := db.Tables[0]
	assert.Equal(t, "m", tbl.Name)
	assert.Equal(t, 4, len(tbl.Columns))
	e, ok := tbl.Engine.Decoded.(EngineTimeSeries)
	require.True(t, ok)
	assert.Equal(t, "DATA", e.KeywordHint)
	require.NotNil(t, e.Samples)
	require.NotNil(t, e.Samples.Target)
	assert.Equal(t, "db.m_data", *e.Samples.Target)
	require.NotNil(t, e.Tags)
	assert.Equal(t, "db.m_tags", *e.Tags.Target)
	require.NotNil(t, e.Metrics)
	assert.Equal(t, "db.m_metrics", *e.Metrics.Target)
}

func TestIntrospect_TimeSeries_Inner(t *testing.T) {
	sql := "CREATE TABLE db.m () ENGINE = TimeSeries " +
		"SAMPLES INNER COLUMNS (`id` UUID, `timestamp` DateTime64(3), `value` Float64) " +
		"SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp)"
	db := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRows(db, "db", &stringRows{rows: [][2]string{{"m", sql}}}))
	e := db.Tables[0].Engine.Decoded.(EngineTimeSeries)
	assert.Equal(t, "SAMPLES", e.KeywordHint)
	require.NotNil(t, e.Samples)
	require.NotNil(t, e.Samples.Inner)
	assert.Nil(t, e.Samples.Target)
	require.Len(t, e.Samples.Inner.Columns, 3)
	_, ok := e.Samples.Inner.Engine.Decoded.(EngineMergeTree)
	assert.True(t, ok)
	assert.Equal(t, []string{"id", "timestamp"}, e.Samples.Inner.OrderBy)
}

func TestIntrospect_TimeSeries_Bare(t *testing.T) {
	sql := "CREATE TABLE db.m () ENGINE = TimeSeries"
	db := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRows(db, "db", &stringRows{rows: [][2]string{{"m", sql}}}))
	e := db.Tables[0].Engine.Decoded.(EngineTimeSeries)
	assert.Nil(t, e.Samples)
	assert.Nil(t, e.Tags)
	assert.Nil(t, e.Metrics)
}

func TestIntrospect_TimeSeries_Settings(t *testing.T) {
	sql := "CREATE TABLE db.m () ENGINE = TimeSeries " +
		"SETTINGS id_generator = 'sipHash64(metric_name, all_tags)', tags_to_columns = {'instance':'instance','job':'job'}"
	db := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRows(db, "db", &stringRows{rows: [][2]string{{"m", sql}}}))
	e := db.Tables[0].Engine.Decoded.(EngineTimeSeries)
	assert.Equal(t, "sipHash64(metric_name, all_tags)", e.Settings["id_generator"])
	assert.Equal(t, map[string]string{"instance": "instance", "job": "job"}, e.TagsToColumns)
}
```

(`stringRows` is the existing test helper — search `introspect_test.go` for it.)

- [ ] **Step 2: Implement the builder**

Add to `introspect.go`:

```go
// buildTimeSeriesFromCreateTable handles a CreateTable whose Engine.Name is
// "TimeSeries". Outer columns flow through the standard path; the engine
// becomes an EngineTimeSeries with its target sub-blocks populated from
// the chparser TimeSeriesTargetClause list.
func buildTimeSeriesFromCreateTable(ct *chparser.CreateTable) (TableSpec, error) {
	ts, err := buildTableFromCreateTable(ct)
	if err != nil {
		return TableSpec{}, err
	}
	// buildTableFromCreateTable already populated Columns/Settings/etc on the
	// outer table. We replace ts.Engine with an EngineTimeSeries, lifting
	// id_generator/tags_to_columns out of the engine SETTINGS the parser
	// returned and folding them into the typed engine struct.
	e := EngineTimeSeries{}

	// Engine-level SETTINGS the parser captured.
	if ct.Engine.Settings != nil {
		for _, item := range ct.Engine.Settings.Items {
			key := stripBackticks(identName(item.Name))
			val := settingValueString(item.Expr)
			switch key {
			case "tags_to_columns":
				m, err := parseTagsToColumnsMap(val)
				if err != nil {
					return TableSpec{}, fmt.Errorf("tags_to_columns: %w", err)
				}
				e.TagsToColumns = m
			default:
				if e.Settings == nil {
					e.Settings = map[string]string{}
				}
				e.Settings[key] = val
			}
		}
	}

	// Target clauses.
	for _, tc := range ct.TimeSeriesTargets {
		target, err := buildTimeSeriesTarget(tc)
		if err != nil {
			return TableSpec{}, fmt.Errorf("%s target: %w", tc.Kind, err)
		}
		switch tc.Kind {
		case "samples":
			e.Samples = target
			e.KeywordHint = tc.Keyword // SAMPLES or DATA
		case "tags":
			e.Tags = target
		case "metrics":
			e.Metrics = target
		default:
			return TableSpec{}, fmt.Errorf("unknown TimeSeries target kind: %q", tc.Kind)
		}
	}

	ts.Engine = &EngineSpec{Kind: "time_series", Decoded: e}
	return ts, nil
}

func buildTimeSeriesTarget(tc *chparser.TimeSeriesTargetClause) (*TimeSeriesTarget, error) {
	switch {
	case tc.External != nil:
		ref := tc.External.String()
		return &TimeSeriesTarget{Target: &ref}, nil
	case tc.InnerColumns != nil:
		cols := columnsFromTableSchema(tc.InnerColumns)
		inner := &TimeSeriesInnerTable{
			Columns: make([]ColumnSpec, len(cols)),
		}
		for i, c := range cols {
			inner.Columns[i] = ColumnSpec{Name: c.Name, Type: c.Type}
		}
		if tc.InnerEngine != nil {
			eng, _, err := engineFromAST(tc.InnerEngine)
			if err != nil {
				return nil, fmt.Errorf("inner engine: %w", err)
			}
			inner.Engine = &EngineSpec{
				Kind:    canonicalKindOf(eng),
				Decoded: eng,
			}
			if tc.InnerEngine.OrderBy != nil {
				inner.OrderBy = extractOrderBy(tc.InnerEngine.OrderBy)
			}
			if tc.InnerEngine.PrimaryKey != nil {
				inner.PrimaryKey = extractPrimaryKey(tc.InnerEngine.PrimaryKey)
			}
			if tc.InnerEngine.PartitionBy != nil {
				pb := tc.InnerEngine.PartitionBy.String()
				inner.PartitionBy = &pb
			}
		}
		return &TimeSeriesTarget{Inner: inner}, nil
	}
	return nil, errors.New("clause has neither external table nor inner columns")
}

// canonicalKindOf maps a decoded Engine back to its "kind" label used in HCL.
// E.g. EngineMergeTree -> "merge_tree", EngineAggregatingMergeTree ->
// "aggregating_merge_tree". Reuses the Kind() method on the typed value.
func canonicalKindOf(e Engine) string {
	if e == nil {
		return ""
	}
	return e.Kind()
}

// parseTagsToColumnsMap parses the `{'tag1':'col1','tag2':'col2',...}` map
// literal CH emits in SETTINGS.
func parseTagsToColumnsMap(s string) (map[string]string, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil, fmt.Errorf("expected map literal, got %q", s)
	}
	s = strings.TrimSpace(s[1 : len(s)-1])
	if s == "" {
		return map[string]string{}, nil
	}
	out := map[string]string{}
	for _, pair := range splitTopLevel(s, ',') {
		kv := splitTopLevel(strings.TrimSpace(pair), ':')
		if len(kv) != 2 {
			return nil, fmt.Errorf("expected `key:value`, got %q", pair)
		}
		k := unquoteString(strings.TrimSpace(kv[0]))
		v := unquoteString(strings.TrimSpace(kv[1]))
		out[k] = v
	}
	return out, nil
}

// splitTopLevel splits s on sep, respecting matched quotes. Tiny helper —
// the parsed maps are flat strings only, so we don't need full SQL-aware
// splitting.
func splitTopLevel(s string, sep byte) []string {
	var parts []string
	var current strings.Builder
	inSingle := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\'' && (i == 0 || s[i-1] != '\\') {
			inSingle = !inSingle
		}
		if c == sep && !inSingle {
			parts = append(parts, current.String())
			current.Reset()
			continue
		}
		current.WriteByte(c)
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

func unquoteString(s string) string {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}
```

The helpers `extractOrderBy` / `extractPrimaryKey` / `settingValueString` / `identName` should already exist in `introspect.go` — search for them and reuse. If `settingValueString` doesn't exist, see how the Kafka path handles `Engine.Settings.Items[i].Expr` and mirror it.

- [ ] **Step 3: Dispatch from `processIntrospectRows`**

Modify the `case *chparser.CreateTable:` branch in `processIntrospectRows` (around `introspect.go:71`):

```go
case *chparser.CreateTable:
	var ts TableSpec
	var err error
	if s.Engine != nil && s.Engine.Name == "TimeSeries" {
		ts, err = buildTimeSeriesFromCreateTable(s)
	} else {
		ts, err = buildTableFromCreateTable(s)
	}
	if err != nil {
		return fmt.Errorf("introspect table %s.%s: %w", database, name, err)
	}
	ts.Name = name
	db.Tables = append(db.Tables, ts)
```

- [ ] **Step 4: Run + commit**

```bash
go test ./internal/loader/hcl/ -run TestIntrospect_TimeSeries -v
git add internal/loader/hcl/introspect.go internal/loader/hcl/introspect_test.go
git commit -m "feat(hcl): introspect TimeSeries-engine tables"
```

---

### Task 4: SQL generation — engine clause + tail emission

**Files:**
- Modify: `internal/loader/hcl/sqlgen.go`
- Modify: `internal/loader/hcl/sqlgen_test.go`

- [ ] **Step 1: Write failing tests**

```go
func TestSQLGen_TimeSeries_External_DataAlias(t *testing.T) {
	tgtData := "default.m_data"
	tgtTags := "default.m_tags"
	tgtMetrics := "default.m_metrics"
	ts := TableSpec{Name: "m",
		Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
		Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
			Samples:     &TimeSeriesTarget{Target: &tgtData},
			Tags:        &TimeSeriesTarget{Target: &tgtTags},
			Metrics:     &TimeSeriesTarget{Target: &tgtMetrics},
			KeywordHint: "DATA",
		}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "default", AddTables: []TableSpec{ts},
	}}})
	require.Len(t, out.Statements, 1)
	stmt := out.Statements[0]
	assert.Contains(t, stmt, "ENGINE = TimeSeries")
	assert.Contains(t, stmt, "DATA default.m_data")
	assert.Contains(t, stmt, "TAGS default.m_tags")
	assert.Contains(t, stmt, "METRICS default.m_metrics")
	assert.NotContains(t, stmt, "SAMPLES")
}

func TestSQLGen_TimeSeries_External_DefaultsToSamplesKeyword(t *testing.T) {
	tgt := "default.m_data"
	ts := TableSpec{Name: "m",
		Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
		Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
			Samples: &TimeSeriesTarget{Target: &tgt},
			// no KeywordHint -> canonical SAMPLES
		}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "default", AddTables: []TableSpec{ts},
	}}})
	stmt := out.Statements[0]
	assert.Contains(t, stmt, "SAMPLES default.m_data")
	assert.NotContains(t, stmt, "DATA default.m_data")
}

func TestSQLGen_TimeSeries_Inner(t *testing.T) {
	inner := &TimeSeriesInnerTable{
		Columns: []ColumnSpec{
			{Name: "id", Type: "UUID"},
			{Name: "timestamp", Type: "DateTime64(3)"},
			{Name: "value", Type: "Float64"},
		},
		Engine:  &EngineSpec{Decoded: EngineMergeTree{}},
		OrderBy: []string{"id", "timestamp"},
	}
	ts := TableSpec{Name: "m",
		Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
		Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
			Samples: &TimeSeriesTarget{Inner: inner},
		}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "default", AddTables: []TableSpec{ts},
	}}})
	stmt := out.Statements[0]
	assert.Contains(t, stmt, "SAMPLES INNER COLUMNS (id UUID, timestamp DateTime64(3), value Float64)")
	assert.Contains(t, stmt, "SAMPLES INNER ENGINE = MergeTree")
	assert.Contains(t, stmt, "ORDER BY (id, timestamp)")
}

func TestSQLGen_TimeSeries_TagsToColumnsFolded(t *testing.T) {
	ts := TableSpec{Name: "m",
		Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
		Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
			TagsToColumns: map[string]string{"instance": "instance", "job": "job"},
		}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{{
		Database: "default", AddTables: []TableSpec{ts},
	}}})
	stmt := out.Statements[0]
	assert.Contains(t, stmt, "tags_to_columns = {'instance':'instance', 'job':'job'}")
}
```

- [ ] **Step 2: Implement engine emission**

In `sqlgen.go`'s `engineSQL` switch, add:

```go
case EngineTimeSeries:
	clause, extra := createTimeSeriesEngineSQL(e)
	return clause, extra
```

(Mirror the Kafka shape that already returns `(clause string, extra map[string]string)`.)

Then add:

```go
func createTimeSeriesEngineSQL(e EngineTimeSeries) (string, map[string]string) {
	settings := map[string]string{}
	for k, v := range e.Settings {
		settings[k] = v
	}
	if len(e.TagsToColumns) > 0 {
		settings["tags_to_columns"] = renderTagsToColumnsMap(e.TagsToColumns)
	}
	var b strings.Builder
	b.WriteString("TimeSeries")
	emitTimeSeriesTarget(&b, e.Samples, samplesKeyword(e.KeywordHint))
	emitTimeSeriesTarget(&b, e.Tags, "TAGS")
	emitTimeSeriesTarget(&b, e.Metrics, "METRICS")
	return b.String(), settings
}

func samplesKeyword(hint string) string {
	if hint == "DATA" {
		return "DATA"
	}
	return "SAMPLES"
}

func emitTimeSeriesTarget(b *strings.Builder, t *TimeSeriesTarget, keyword string) {
	if t == nil {
		return
	}
	if t.Target != nil {
		fmt.Fprintf(b, " %s %s", keyword, *t.Target)
		return
	}
	if t.Inner == nil {
		return
	}
	parts := make([]string, len(t.Inner.Columns))
	for i, c := range t.Inner.Columns {
		parts[i] = columnDefSQL(c)
	}
	fmt.Fprintf(b, " %s INNER COLUMNS (%s)", keyword, strings.Join(parts, ", "))
	if t.Inner.Engine != nil && t.Inner.Engine.Decoded != nil {
		inner, _ := engineSQL(t.Inner.Engine.Decoded)
		fmt.Fprintf(b, " %s INNER ENGINE = %s", keyword, inner)
	}
	if len(t.Inner.PrimaryKey) > 0 {
		fmt.Fprintf(b, " PRIMARY KEY (%s)", strings.Join(t.Inner.PrimaryKey, ", "))
	}
	if len(t.Inner.OrderBy) > 0 {
		fmt.Fprintf(b, " ORDER BY (%s)", strings.Join(t.Inner.OrderBy, ", "))
	}
	if t.Inner.PartitionBy != nil {
		fmt.Fprintf(b, " PARTITION BY %s", *t.Inner.PartitionBy)
	}
	if len(t.Inner.Settings) > 0 {
		keys := make([]string, 0, len(t.Inner.Settings))
		for k := range t.Inner.Settings {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var s strings.Builder
		for i, k := range keys {
			if i > 0 {
				s.WriteString(", ")
			}
			fmt.Fprintf(&s, "%s = %s", k, formatSettingValue(t.Inner.Settings[k]))
		}
		fmt.Fprintf(b, " SETTINGS %s", s.String())
	}
}

func renderTagsToColumnsMap(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("'%s':'%s'", k, m[k])
	}
	return "{" + strings.Join(parts, ", ") + "}"
}
```

`formatSettingValue` already exists for the Kafka path; reuse it. If `columnDefSQL` doesn't exist, look for `func columnDefSQL` in `sqlgen.go` — it's the one that emits `name Type [DEFAULT ...] [CODEC(...)]`. Reuse it.

- [ ] **Step 3: Run + commit**

```bash
go test ./internal/loader/hcl/ -run TestSQLGen_TimeSeries -v
git add internal/loader/hcl/sqlgen.go internal/loader/hcl/sqlgen_test.go
git commit -m "feat(hcl): sqlgen for TimeSeries engine (external + inner targets)"
```

---

### Task 5: Diff + ALTER policy

**Files:**
- Modify: `internal/loader/hcl/diff.go`
- Modify: `internal/loader/hcl/diff_test.go`
- Modify: `internal/loader/hcl/sqlgen.go` (ALTER emission for the two ALTER-able settings)

- [ ] **Step 1: Write failing tests**

```go
func TestDiff_TimeSeries_IdGeneratorChange_ALTERed(t *testing.T) {
	mk := func(idGen string) *Schema {
		return &Schema{Databases: []DatabaseSpec{{
			Name: "db",
			Tables: []TableSpec{{Name: "m",
				Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
				Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
					Settings: map[string]string{"id_generator": idGen},
				}},
			}},
		}}}
	}
	cs := Diff(mk("sipHash64(metric_name)"), mk("sipHash64(metric_name, all_tags)"))
	require.Len(t, cs.Databases, 1)
	require.Len(t, cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]
	assert.Len(t, td.SettingsChanged, 1)
	assert.Equal(t, "id_generator", td.SettingsChanged[0].Key)
	assert.False(t, td.IsUnsafe(), "id_generator change is in-place")
}

func TestDiff_TimeSeries_BakedSettingChange_Recreate(t *testing.T) {
	mk := func(val string) *Schema {
		return &Schema{Databases: []DatabaseSpec{{
			Name: "db",
			Tables: []TableSpec{{Name: "m",
				Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
				Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
					Settings: map[string]string{"store_min_time_and_max_time": val},
				}},
			}},
		}}}
	}
	cs := Diff(mk("0"), mk("1"))
	require.Len(t, cs.Databases, 1)
	require.Len(t, cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]
	assert.True(t, td.IsUnsafe(), "non-alterable setting change should be recreate")
}

func TestDiff_TimeSeries_TargetChange_Recreate(t *testing.T) {
	mk := func(target string) *Schema {
		return &Schema{Databases: []DatabaseSpec{{
			Name: "db",
			Tables: []TableSpec{{Name: "m",
				Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
				Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
					Samples: &TimeSeriesTarget{Target: &target},
				}},
			}},
		}}}
	}
	cs := Diff(mk("db.old"), mk("db.new"))
	require.Len(t, cs.Databases, 1)
	require.Len(t, cs.Databases[0].AlterTables, 1)
	assert.True(t, cs.Databases[0].AlterTables[0].IsUnsafe())
}

func TestDiff_TimeSeries_TagsToColumnsChange_Recreate(t *testing.T) {
	mk := func(m map[string]string) *Schema {
		return &Schema{Databases: []DatabaseSpec{{
			Name: "db",
			Tables: []TableSpec{{Name: "m",
				Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
				Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
					TagsToColumns: m,
				}},
			}},
		}}}
	}
	cs := Diff(mk(map[string]string{"instance": "instance"}), mk(map[string]string{"instance": "instance", "job": "job"}))
	td := cs.Databases[0].AlterTables[0]
	assert.True(t, td.IsUnsafe())
}
```

- [ ] **Step 2: Implement diff logic**

Extend `diffTable` so when both sides' engines are `EngineTimeSeries`, it splits the changes into ALTER-able-settings vs everything-else:

```go
// Inside diffTable, before the generic engine comparison:
if fromTS, fromOK := engineOf(*from).(EngineTimeSeries); fromOK {
	if toTS, toOK := engineOf(*to).(EngineTimeSeries); toOK {
		diffTimeSeries(&td, fromTS, toTS)
		// Skip the generic engine-change path; we've handled it.
		// (Other table-level fields like ordering still need normal diff —
		// continue past this block, just skip the EngineChange step.)
	}
}
```

```go
// timeSeriesAlterableSettings names the SETTINGS keys CH supports via
// ALTER TABLE ... MODIFY SETTING on a TimeSeries table.
var timeSeriesAlterableSettings = map[string]bool{
	"id_generator":                    true,
	"filter_by_min_time_and_max_time": true,
}

// diffTimeSeries fills td.SettingsAdded/Removed/Changed for the two
// alterable keys and marks the rest of the engine diff as recreate.
func diffTimeSeries(td *TableDiff, from, to EngineTimeSeries) {
	// Settings: split into alterable vs baked.
	bakedDiffers := false
	for k, vNew := range to.Settings {
		vOld, ok := from.Settings[k]
		if !ok {
			if timeSeriesAlterableSettings[k] {
				if td.SettingsAdded == nil {
					td.SettingsAdded = map[string]string{}
				}
				td.SettingsAdded[k] = vNew
			} else {
				bakedDiffers = true
			}
			continue
		}
		if vOld != vNew {
			if timeSeriesAlterableSettings[k] {
				td.SettingsChanged = append(td.SettingsChanged, SettingChange{Key: k, OldValue: vOld, NewValue: vNew})
			} else {
				bakedDiffers = true
			}
		}
	}
	for k, vOld := range from.Settings {
		if _, ok := to.Settings[k]; ok {
			continue
		}
		if timeSeriesAlterableSettings[k] {
			td.SettingsRemoved = append(td.SettingsRemoved, k)
			_ = vOld
		} else {
			bakedDiffers = true
		}
	}

	// TagsToColumns is baked.
	if !reflect.DeepEqual(from.TagsToColumns, to.TagsToColumns) {
		bakedDiffers = true
	}
	// Any change to targets is a recreate.
	if !reflect.DeepEqual(from.Samples, to.Samples) ||
		!reflect.DeepEqual(from.Tags, to.Tags) ||
		!reflect.DeepEqual(from.Metrics, to.Metrics) {
		bakedDiffers = true
	}

	if bakedDiffers {
		td.EngineChange = &EngineChange{Old: from, New: to}
	}
}
```

The existing `td.IsUnsafe()` already returns true when `EngineChange != nil`, so we get the recreate signal for free.

- [ ] **Step 3: ALTER SETTING emission**

In `sqlgen.go`'s table-alter loop, add (or extend the existing settings-change path so it works for TimeSeries too — the keys are already engine-agnostic):

Look for the existing block that handles `SettingsChanged` / `SettingsAdded` / `SettingsRemoved` for tables. Confirm it emits `ALTER TABLE ... MODIFY SETTING k = v`. If it doesn't already exist, add it:

```go
for _, sc := range td.SettingsChanged {
	out.Statements = append(out.Statements,
		fmt.Sprintf("ALTER TABLE %s.%s MODIFY SETTING %s = %s",
			dc.Database, td.Table, sc.Key, formatSettingValue(sc.NewValue)))
}
for k, v := range td.SettingsAdded {
	out.Statements = append(out.Statements,
		fmt.Sprintf("ALTER TABLE %s.%s MODIFY SETTING %s = %s",
			dc.Database, td.Table, k, formatSettingValue(v)))
}
for _, k := range td.SettingsRemoved {
	out.Statements = append(out.Statements,
		fmt.Sprintf("ALTER TABLE %s.%s RESET SETTING %s", dc.Database, td.Table, k))
}
```

If this block already exists for the MergeTree-family settings flow, no change needed.

- [ ] **Step 4: Run + commit**

```bash
go test ./internal/loader/hcl/ -run TestDiff_TimeSeries -v
git add internal/loader/hcl/diff.go internal/loader/hcl/diff_test.go internal/loader/hcl/sqlgen.go
git commit -m "feat(hcl): TimeSeries diff with ALTER-able settings split"
```

---

### Task 6: Dependency validation

**Files:**
- Modify: `internal/loader/hcl/validate.go`
- Modify: `internal/loader/hcl/validate_test.go`

- [ ] **Step 1: Failing test**

```go
func TestValidate_TimeSeries_ExternalTargetMustExist(t *testing.T) {
	tgt := "db.samples_does_not_exist"
	dbs := []DatabaseSpec{{Name: "db",
		Tables: []TableSpec{{Name: "m",
			Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
			Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
				Samples: &TimeSeriesTarget{Target: &tgt},
			}},
		}},
	}}
	errs := Validate(dbs, SkipSet{})
	var ferr *ValidationError
	for i := range errs {
		if errs[i].Kind == DepTimeSeriesTarget {
			ferr = &errs[i]
			break
		}
	}
	require.NotNil(t, ferr, "external TimeSeries target should be flagged")
	assert.Equal(t, "db.samples_does_not_exist", ferr.Missing.String())
}

func TestValidate_TimeSeries_InnerTargetNoDependency(t *testing.T) {
	dbs := []DatabaseSpec{{Name: "db",
		Tables: []TableSpec{{Name: "m",
			Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
			Engine: &EngineSpec{Kind: "time_series", Decoded: EngineTimeSeries{
				Samples: &TimeSeriesTarget{Inner: &TimeSeriesInnerTable{
					Columns: []ColumnSpec{{Name: "id", Type: "UUID"}},
					Engine:  &EngineSpec{Decoded: EngineMergeTree{}},
				}},
			}},
		}},
	}}
	errs := Validate(dbs, SkipSet{})
	for _, e := range errs {
		assert.NotEqual(t, DepTimeSeriesTarget, e.Kind, "inner-form target should create no dep")
	}
}
```

- [ ] **Step 2: Implement**

In `validate.go`'s constants block, add:

```go
DepTimeSeriesTarget = "ts_target" // a TimeSeries table references an external samples/tags/metrics target
```

Extend `CollectDependencies` (after the existing Distributed dep loop, before the materialized_view block):

```go
for _, t := range db.Tables {
	if t.Engine == nil {
		continue
	}
	ts, ok := t.Engine.Decoded.(EngineTimeSeries)
	if !ok {
		continue
	}
	from := ObjectRef{Database: db.Name, Name: t.Name}
	for _, kv := range []*TimeSeriesTarget{ts.Samples, ts.Tags, ts.Metrics} {
		if kv == nil || kv.Target == nil {
			continue
		}
		deps = append(deps, Dependency{
			From: from,
			To:   splitQualified(*kv.Target, db.Name),
			Kind: DepTimeSeriesTarget,
		})
	}
}
```

Extend `depPhrase`:

```go
case DepTimeSeriesTarget:
	return "TimeSeries target table"
```

- [ ] **Step 3: Run + commit**

```bash
go test ./internal/loader/hcl/ -run TestValidate_TimeSeries -v
git add internal/loader/hcl/validate.go internal/loader/hcl/validate_test.go
git commit -m "feat(hcl): validate TimeSeries external-form target dependencies"
```

---

### Task 7: Live fixtures + introspect_live_test

**Files:**
- Create: `test/testdata/posthog-create-statements/TimeSeries/prom_metrics_external.sql`
- Create: `test/testdata/posthog-create-statements/TimeSeries/prom_metrics_inner.sql`
- Create: `test/testdata/posthog-create-statements/TimeSeries/prom_metrics_minimal.sql`

- [ ] **Step 1: Write fixtures**

`prom_metrics_external.sql` — the actual production statement (already in our log; reuse it verbatim).

```sql
CREATE TABLE default.prom_metrics
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `timestamp` DateTime64(3),
    `value` Float64,
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String),
    `min_time` Nullable(DateTime64(3)),
    `max_time` Nullable(DateTime64(3)),
    `metric_family_name` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries
DATA default.prom_metrics_data
TAGS default.prom_metrics_tags
METRICS default.prom_metrics_metrics
```

`prom_metrics_minimal.sql`:

```sql
CREATE TABLE default.prom_metrics_minimal () ENGINE = TimeSeries
```

`prom_metrics_inner.sql` — the doc's "Creation" section example (all three inner forms):

```sql
CREATE TABLE default.prom_metrics_inner
(
    `metric_name` String,
    `tags` Map(String, String),
    `time_series` Array(Tuple(DateTime64(3), Float64)),
    `metric_family` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries
SAMPLES INNER COLUMNS
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp)
TAGS INNER COLUMNS
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
TAGS INNER ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id)
METRICS INNER COLUMNS
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
METRICS INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name
```

- [ ] **Step 2: Verify the existing live loop picks them up**

`hcl_introspect_live_test.go` already loops fixtures grouped by directory name. Run:

```bash
ENABLE_CLICKHOUSE=1 go test ./test -run TestLive_Introspection_AllStatements/TimeSeries -v
```

Expected: all three load against CH (need `SET allow_experimental_time_series_table = 1`; the test harness should set this — if it doesn't, add it to `setUpDB` in `test/testhelpers`).

- [ ] **Step 3: Commit**

```bash
git add test/testdata/posthog-create-statements/TimeSeries/
git commit -m "test: live-test fixtures for TimeSeries engine"
```

---

### Task 8: Docs

**Files:**
- Modify: `docs/README.hcl.md`
- Modify: `docs/FAQ.md`

- [ ] **Step 1: README.hcl.md — engine entry**

In the "Engine kinds" section, after `kafka`, add:

```markdown
### `time_series` (experimental)

Models ClickHouse's [TimeSeries](https://clickhouse.com/docs/en/engines/table-engines/special/time_series) engine for Prometheus-style metrics. Three sibling target tables (samples/tags/metrics) are declared via nested sub-blocks; each takes either an external `target = "db.table"` reference or an inline `inner {}` block with column list + nested engine.

\`\`\`hcl
table "prom_metrics" {
  column "metric_name" { type = "LowCardinality(String)" }
  # ... outer columns ...

  engine "time_series" {
    settings = {
      id_generator = "sipHash64(metric_name, all_tags)"
    }
    tags_to_columns = {
      instance = "instance"
      job      = "job"
    }
    samples { target = "default.prom_metrics_data" }
    tags    { target = "default.prom_metrics_tags" }
    metrics { target = "default.prom_metrics_metrics" }
  }
}
\`\`\`

- Each target sub-block is optional. Omitted = CH default inner targets.
- Exactly one of `target` or `inner` per sub-block.
- Inner-form engines restricted to MergeTree-family kinds.
- ALTER-able settings: `id_generator`, `filter_by_min_time_and_max_time`. Every other setting and every target change requires recreating the table.
- HCL authors always write `samples`; the `DATA` alias CH supports is preserved on dump-side only.
```

- [ ] **Step 2: FAQ.md — worked example**

Append:

```markdown
## How do I model a Prometheus TimeSeries table?

Use the `time_series` engine. For the common case of three pre-existing external target tables:

\`\`\`hcl
table "prom_metrics" {
  column "metric_name" { type = "LowCardinality(String)" }
  # ... outer columns ...

  engine "time_series" {
    samples { target = "default.prom_metrics_data" }
    tags    { target = "default.prom_metrics_tags" }
    metrics { target = "default.prom_metrics_metrics" }
  }
}
\`\`\`

`hclexp validate` will refuse to resolve until each target table is declared somewhere in the loaded schema. If you don't manage them via hclexp, declare them with `engine "merge_tree" {}` (or whichever they actually use) so the dependency check passes.

See the `time_series` reference in `README.hcl.md` for the full attribute list.
```

- [ ] **Step 3: Commit**

```bash
git add docs/README.hcl.md docs/FAQ.md
git commit -m "docs: TimeSeries engine reference + FAQ entry"
```

---

### Task 9: Final sweep + PR

- [ ] **Step 1: Build + vet + unit tests**

```bash
go build ./...
go vet ./...
go test ./... -count=1
```

Expected: all green.

- [ ] **Step 2: Live tests**

```bash
docker compose up -d
ENABLE_CLICKHOUSE=1 go test ./... -count=1
```

Expected: all green. The TimeSeries live fixtures join the existing engine-grouped loop.

- [ ] **Step 3: Push + PR**

```bash
git push -u origin feat/timeseries-engine
gh pr create --title "feat(hcl): TimeSeries engine support" --body-file <(cat <<'EOF'
## Summary

Adds first-class support for ClickHouse's experimental `TimeSeries` engine. PostHog's `posthog.prom_metrics` now round-trips end-to-end through `hclexp introspect` / `hclexp diff` / `hclexp validate`.

- New `EngineTimeSeries` value type with optional `samples`/`tags`/`metrics` sub-blocks (external or inline-`inner`), promoted `tags_to_columns`, free-form `settings`.
- Inner-form target engines restricted to MergeTree-family.
- `DATA` ↔ `SAMPLES` keyword round-trip via `KeywordHint` (diff-skipped, dump-side only).
- Two ALTER-able settings (`id_generator`, `filter_by_min_time_and_max_time`) → in-place `MODIFY SETTING`. Everything else baked → recreate + UNSAFE.
- New `DepTimeSeriesTarget` validation dependency for external-form targets.

Depends on chparser fork commit [`47aa1ff`](https://github.com/orian/clickhouse-sql-parser/commit/47aa1ff) (fixes [#8](https://github.com/orian/clickhouse-sql-parser/issues/8)), already pinned via `go.mod` replace.

## Test plan

- [x] `go build ./...`
- [x] `go vet ./...`
- [x] `go test ./... -count=1`
- [x] `ENABLE_CLICKHOUSE=1 go test ./... -count=1` (with docker compose up)
- [x] PostHog prom_metrics CREATE statement round-trips through introspect → sqlgen → introspect with no diff
- [ ] CI runs the same matrices

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)
```

- [ ] **Step 4: Watch CI**

```bash
gh pr checks
```
