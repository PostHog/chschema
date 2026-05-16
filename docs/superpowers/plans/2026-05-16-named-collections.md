# Named Collections + Kafka Engine Rework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add ClickHouse named collections as a first-class top-level entity in the `internal/loader/hcl` package — they round-trip through parse / introspect / dump / diff / sqlgen with surgical ALTER for param changes and adjacent DROP+CREATE for ON CLUSTER changes — and rework the Kafka engine to either reference a named collection or carry a complete typed model of all documented `kafka_*` settings.

**Architecture:** Named collections live at the top of the HCL document (sibling to `database` blocks, not nested inside). `ParseFile` returns a new `Schema` aggregate (breaking change to the package's public API). `EngineKafka` grows from 4 typed fields to ~25 typed fields plus a `collection` reference and an `extra` escape map; collection and inline settings are mutually exclusive. External (XML-config) named collections are declared in HCL with `external = true` and are invisible to introspection/diff but resolvable for engine `collection = "..."` references.

**Tech Stack:** Go, `hashicorp/hcl/v2` + `gohcl` + `hclwrite`, `github.com/AfterShip/clickhouse-sql-parser` (orian fork, `refactor-visitor` branch), `stretchr/testify`.

**Spec:** `docs/superpowers/specs/2026-05-16-named-collections-design.md` (committed `d2c4f3e`).

**Conventions:** Tests use `testify` (`assert`/`require`). Run unit tests with `go test ./internal/... ./cmd/... -v`. Live tests need `-clickhouse` and `docker compose up -d`. The package has these helpers this plan reuses without redefining: `ptr[T any](v T) *T` (parser_test.go), `formatNode(n chparser.Expr) string` (introspect.go), `unquoteString`, `strPtr`. The `EngineSpec`/`DecodeEngine` pattern in `engines.go` is the template for many of the typed-block patterns used here.

---

## File map

**New files:**

- `internal/loader/hcl/named_collection_introspect.go` — `IntrospectNamedCollections`, `buildNamedCollectionFromCreateNC`.
- `internal/loader/hcl/named_collection_introspect_test.go`.
- `internal/loader/hcl/named_collection_dump.go` — `writeNamedCollection`.
- `internal/loader/hcl/named_collection_diff.go` — `NamedCollectionChange` type, `diffNamedCollection`.
- `internal/loader/hcl/named_collection_sqlgen.go` — `createNamedCollectionSQL`, `dropNamedCollectionSQL`, `alterNamedCollectionSetSQL`, `alterNamedCollectionDeleteSQL`.
- `internal/loader/hcl/named_collection_sqlgen_test.go`.
- `internal/loader/hcl/named_collection_live_test.go`.
- `internal/loader/hcl/kafka_namedcollection_live_test.go`.
- `internal/loader/hcl/testdata/named_collection.hcl`.
- `internal/loader/hcl/testdata/kafka_with_collection.hcl`.
- `internal/loader/hcl/testdata/kafka_inline_settings.hcl`.

**Modified files:**

- `internal/loader/hcl/types.go` — add `Schema`, `NamedCollectionSpec`, `NamedCollectionParam`; reshape `EngineKafka`.
- `internal/loader/hcl/engines.go` — reshape `EngineKafka` struct + `DecodeEngine` kafka case.
- `internal/loader/hcl/parser.go` — `ParseFile` returns `*Schema`; `fileSpec` gains `NamedCollections`.
- `internal/loader/hcl/layers.go` — `LoadLayers` returns `*Schema`; new `mergeNamedCollections`.
- `internal/loader/hcl/resolver.go` — `Resolve(*Schema) error`; new `validateNamedCollections`, `validateKafkaEngine`, `validateCollectionReferences`.
- `internal/loader/hcl/introspect.go` — `parseKafkaEngine` four-case rewrite; introspect dispatch unchanged (NCs go through a separate top-level function).
- `internal/loader/hcl/dump.go` — `Write(*Schema)`; emit `named_collection` blocks after databases; `writeEngine` Kafka case rewrite.
- `internal/loader/hcl/diff.go` — `ChangeSet.NamedCollections`; diff integration.
- `internal/loader/hcl/sqlgen.go` — `engineSQL` Kafka case rewrite; `GenerateSQL` ordering with NC recreate-pair-at-front.
- `cmd/hclexp/hclexp.go` — `loadSide` returns `Schema`; calls `IntrospectNamedCollections`; `renderChangeSet` top-level NC section.
- `cmd/hclexp/hclexp_test.go` — adapt callers to `Schema`.
- `internal/loader/hcl/parser_test.go` — NC + Kafka parser tests.
- `internal/loader/hcl/resolver_test.go` — XOR + NC validation tests.
- `internal/loader/hcl/diff_test.go` — NC diff tests + external semantics.
- `internal/loader/hcl/dump_test.go` — NC round-trip tests.
- `internal/loader/hcl/sqlgen_test.go` — Kafka 4-case + NC sqlgen tests.
- `internal/loader/hcl/introspect_test.go` — `parseKafkaEngine` 4-case test.
- `internal/loader/hcl/engines_test.go` — Kafka decode test refresh.
- `internal/loader/hcl/testdata/engines_all_kinds.hcl` — Kafka block reshape.
- `internal/loader/hcl/introspect_live_test.go::TestCHLive_HCLIntrospect` — Kafka case adapt.
- `README.md` — Named collections section.

---

### Task 1: Add `Schema` + named-collection types in `types.go`

Adds new types without changing any signatures yet. Subsequent tasks wire them in.

**Files:**
- Modify: `internal/loader/hcl/types.go`

- [ ] **Step 1: Append types to `types.go`**

At the end of `internal/loader/hcl/types.go`, append:

```go
// Schema is what ParseFile returns. It carries both top-level kinds
// hclexp tracks: databases (with their tables/MVs/dictionaries) and
// named collections (cluster-scoped, separate from any database).
type Schema struct {
	Databases        []DatabaseSpec
	NamedCollections []NamedCollectionSpec
}

// NamedCollectionSpec models a ClickHouse named collection — a
// cluster-scoped key/value bag of configuration values that other
// objects (most notably Kafka tables) can reference by name.
//
// External = true marks a collection as managed outside hclexp — for
// example, defined in the ClickHouse server XML config under
// /etc/clickhouse-server/config.d/. hclexp emits no DDL for external
// collections (no CREATE / ALTER / DROP); they exist in HCL only as
// declarations so engine `collection = "x"` references can resolve and
// be validated. Params is optional when External = true (the values
// live in the XML config, not in HCL).
type NamedCollectionSpec struct {
	Name     string                 `hcl:"name,label"`
	External bool                   `hcl:"external,optional"`
	Override bool                   `hcl:"override,optional" diff:"-"`
	Cluster  *string                `hcl:"cluster,optional"`
	Comment  *string                `hcl:"comment,optional"`
	Params   []NamedCollectionParam `hcl:"param,block"`
}

type NamedCollectionParam struct {
	Key         string  `hcl:"name,label"`
	Value       string  `hcl:"value"`
	Overridable *bool   `hcl:"overridable,optional"`
}
```

- [ ] **Step 2: Verify build**

Run: `go build ./internal/loader/hcl`
Expected: PASS — types are declared but unused, which Go allows.

- [ ] **Step 3: Commit**

```bash
git add internal/loader/hcl/types.go
git commit -m "feat: add Schema and NamedCollectionSpec types

Schema is the new aggregate ParseFile will return after subsequent
tasks wire the top-level named_collection block. NamedCollectionSpec
and NamedCollectionParam describe a ClickHouse named collection;
External=true marks XML-config-managed collections that hclexp won't
issue DDL for.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Reshape `EngineKafka` + migrate all consumers

This is the largest task — the breaking change. The struct grows from 4 fields to ~25 typed fields + `Collection` + `Extra`, and every consumer (engines.go DecodeEngine, introspect.go parseKafkaEngine, sqlgen.go engineSQL, dump.go writeEngine, plus fixtures and tests) updates in lockstep.

**Files:**
- Modify: `internal/loader/hcl/engines.go`
- Modify: `internal/loader/hcl/introspect.go` (parseKafkaEngine)
- Modify: `internal/loader/hcl/sqlgen.go` (engineSQL Kafka case)
- Modify: `internal/loader/hcl/dump.go` (writeEngine Kafka case)
- Modify: `internal/loader/hcl/testdata/engines_all_kinds.hcl`
- Modify: `internal/loader/hcl/engines_test.go`
- Modify: `internal/loader/hcl/sqlgen_test.go::TestSQLGen_CreateKafkaFoldsSettings`
- Modify: `internal/loader/hcl/introspect_live_test.go::TestCHLive_HCLIntrospect` (Kafka case)

- [ ] **Step 1: Replace the `EngineKafka` struct in `engines.go`**

In `internal/loader/hcl/engines.go`, replace the existing `EngineKafka` definition (currently lines 86-93) with:

```go
type EngineKafka struct {
	// Collection is the named-collection reference. Mutually exclusive
	// with every other field; when set, no inline setting may be set.
	Collection *string `hcl:"collection,optional"`

	// Required when Collection is nil.
	BrokerList *string `hcl:"broker_list,optional"`
	TopicList  *string `hcl:"topic_list,optional"`
	GroupName  *string `hcl:"group_name,optional"`
	Format     *string `hcl:"format,optional"`

	// Optional auth.
	SecurityProtocol *string `hcl:"security_protocol,optional"`
	SaslMechanism    *string `hcl:"sasl_mechanism,optional"`
	SaslUsername     *string `hcl:"sasl_username,optional"`
	SaslPassword     *string `hcl:"sasl_password,optional"`

	// Optional numeric tuning.
	NumConsumers         *int64 `hcl:"num_consumers,optional"`
	MaxBlockSize         *int64 `hcl:"max_block_size,optional"`
	SkipBrokenMessages   *int64 `hcl:"skip_broken_messages,optional"`
	PollTimeoutMs        *int64 `hcl:"poll_timeout_ms,optional"`
	PollMaxBatchSize     *int64 `hcl:"poll_max_batch_size,optional"`
	FlushIntervalMs      *int64 `hcl:"flush_interval_ms,optional"`
	ConsumerRescheduleMs *int64 `hcl:"consumer_reschedule_ms,optional"`
	MaxRowsPerMessage    *int64 `hcl:"max_rows_per_message,optional"`
	CompressionLevel     *int64 `hcl:"compression_level,optional"`

	// Optional booleans (introspected as 0/1, presented as bool in HCL).
	CommitEveryBatch     *bool `hcl:"commit_every_batch,optional"`
	ThreadPerConsumer    *bool `hcl:"thread_per_consumer,optional"`
	CommitOnSelect       *bool `hcl:"commit_on_select,optional"`
	AutodetectClientRack *bool `hcl:"autodetect_client_rack,optional"`

	// Optional strings.
	ClientID         *string `hcl:"client_id,optional"`
	Schema           *string `hcl:"schema,optional"`
	HandleErrorMode  *string `hcl:"handle_error_mode,optional"`
	CompressionCodec *string `hcl:"compression_codec,optional"`

	// Extra is the escape valve for kafka_* settings ClickHouse adds in
	// versions we don't yet model. Keys are passed through verbatim and
	// MUST include the `kafka_` prefix (the typed fields above strip it).
	Extra map[string]string `hcl:"extra,optional"`
}

func (EngineKafka) Kind() string { return "kafka" }
```

- [ ] **Step 2: Run build, observe expected breakage**

Run: `go build ./...`
Expected: FAIL — many consumers reference the old field names (`Topic`, `ConsumerGroup`, `BrokerList []string`). The errors list every site to fix in the next steps.

- [ ] **Step 3: Update `parseKafkaEngine` in `introspect.go`**

Replace the existing `case "Kafka":` body (around lines 387-420) with:

```go
case "Kafka":
	k, err := buildKafkaEngine(params, allSettings)
	if err != nil {
		return nil, nil, err
	}
	// Strip every kafka_* setting from the table-level settings; they're
	// engine args, not table settings.
	stripped := make(map[string]string, len(allSettings))
	for kk, vv := range allSettings {
		if !strings.HasPrefix(kk, "kafka_") {
			stripped[kk] = vv
		}
	}
	if len(stripped) == 0 {
		stripped = nil
	}
	return k, stripped, nil
```

Then add a new helper function in the same file (e.g. just after `parseKafkaEngine`):

```go
// buildKafkaEngine decodes Kafka engine parameters into the typed
// EngineKafka struct across the four supported forms:
//   1. Kafka() + kafka_* settings (canonical inline form)
//   2. Kafka(<collection_name>) with no settings (named collection)
//   3. Kafka('broker', 'topic', 'group', 'format') legacy positional
//   4. Kafka(<collection>) + kafka_* settings → error (mixed form)
func buildKafkaEngine(params []string, allSettings map[string]string) (EngineKafka, error) {
	// Identify named-collection form: exactly one positional arg, no
	// surrounding quotes (an identifier, not a string literal). We
	// approximate by checking the param doesn't contain ',' or '/' (a
	// broker list always does) and that there are no kafka_* settings.
	hasKafkaSettings := false
	for k := range allSettings {
		if strings.HasPrefix(k, "kafka_") {
			hasKafkaSettings = true
			break
		}
	}

	if len(params) == 1 && !strings.Contains(params[0], ":") && !strings.Contains(params[0], ",") {
		// Identifier-shaped single arg → named collection reference.
		name := params[0]
		if hasKafkaSettings {
			return EngineKafka{}, fmt.Errorf("Kafka(%s) cannot be combined with kafka_* SETTINGS overrides — declare full inline settings instead", name)
		}
		return EngineKafka{Collection: &name}, nil
	}

	if len(params) == 4 {
		// Legacy positional form Kafka('broker', 'topic', 'group', 'format').
		// Normalize into the typed fields.
		broker := params[0]
		topic := params[1]
		group := params[2]
		format := params[3]
		k := EngineKafka{
			BrokerList: &broker,
			TopicList:  &topic,
			GroupName:  &group,
			Format:     &format,
		}
		applyExtraKafkaSettings(&k, allSettings)
		return k, nil
	}

	// Canonical inline form: Kafka() + kafka_* settings.
	if len(params) > 0 {
		return EngineKafka{}, fmt.Errorf("Kafka() unexpected positional args: %v", params)
	}
	k := EngineKafka{}
	for key, val := range allSettings {
		if !strings.HasPrefix(key, "kafka_") {
			continue
		}
		applyKafkaSetting(&k, key, val)
	}
	return k, nil
}

// applyKafkaSetting routes one kafka_* setting into the matching typed
// field. Unknown keys land in Extra with their prefix intact.
func applyKafkaSetting(k *EngineKafka, key, val string) {
	switch key {
	case "kafka_broker_list":
		k.BrokerList = &val
	case "kafka_topic_list":
		k.TopicList = &val
	case "kafka_group_name":
		k.GroupName = &val
	case "kafka_format":
		k.Format = &val
	case "kafka_security_protocol":
		k.SecurityProtocol = &val
	case "kafka_sasl_mechanism":
		k.SaslMechanism = &val
	case "kafka_sasl_username":
		k.SaslUsername = &val
	case "kafka_sasl_password":
		k.SaslPassword = &val
	case "kafka_client_id":
		k.ClientID = &val
	case "kafka_schema":
		k.Schema = &val
	case "kafka_handle_error_mode":
		k.HandleErrorMode = &val
	case "kafka_compression_codec":
		k.CompressionCodec = &val
	case "kafka_num_consumers":
		k.NumConsumers = parseInt64Ptr(val)
	case "kafka_max_block_size":
		k.MaxBlockSize = parseInt64Ptr(val)
	case "kafka_skip_broken_messages":
		k.SkipBrokenMessages = parseInt64Ptr(val)
	case "kafka_poll_timeout_ms":
		k.PollTimeoutMs = parseInt64Ptr(val)
	case "kafka_poll_max_batch_size":
		k.PollMaxBatchSize = parseInt64Ptr(val)
	case "kafka_flush_interval_ms":
		k.FlushIntervalMs = parseInt64Ptr(val)
	case "kafka_consumer_reschedule_ms":
		k.ConsumerRescheduleMs = parseInt64Ptr(val)
	case "kafka_max_rows_per_message":
		k.MaxRowsPerMessage = parseInt64Ptr(val)
	case "kafka_compression_level":
		k.CompressionLevel = parseInt64Ptr(val)
	case "kafka_commit_every_batch":
		k.CommitEveryBatch = parseBoolPtr(val)
	case "kafka_thread_per_consumer":
		k.ThreadPerConsumer = parseBoolPtr(val)
	case "kafka_commit_on_select":
		k.CommitOnSelect = parseBoolPtr(val)
	case "kafka_autodetect_client_rack":
		k.AutodetectClientRack = parseBoolPtr(val)
	default:
		if k.Extra == nil {
			k.Extra = map[string]string{}
		}
		k.Extra[key] = val
	}
}

// applyExtraKafkaSettings walks settings AFTER positional params are
// already assigned; only routes settings that aren't already covered by
// positional args.
func applyExtraKafkaSettings(k *EngineKafka, settings map[string]string) {
	for key, val := range settings {
		if !strings.HasPrefix(key, "kafka_") {
			continue
		}
		// Skip settings already populated by positional params.
		if (key == "kafka_broker_list" && k.BrokerList != nil) ||
			(key == "kafka_topic_list" && k.TopicList != nil) ||
			(key == "kafka_group_name" && k.GroupName != nil) ||
			(key == "kafka_format" && k.Format != nil) {
			continue
		}
		applyKafkaSetting(k, key, val)
	}
}

func parseInt64Ptr(s string) *int64 {
	if s == "" {
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return &n
}

func parseBoolPtr(s string) *bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true":
		v := true
		return &v
	case "0", "false":
		v := false
		return &v
	}
	return nil
}
```

Add `strconv` to the imports if not present.

- [ ] **Step 4: Update `engineSQL` Kafka case in `sqlgen.go`**

Replace the existing `case EngineKafka:` (around lines 445-451) with:

```go
case EngineKafka:
	if v.Collection != nil {
		// Named collection form: Kafka(<collection>); no settings emitted.
		return fmt.Sprintf("Kafka(%s)", *v.Collection), nil
	}
	settings := map[string]string{}
	if v.BrokerList != nil {
		settings["kafka_broker_list"] = "'" + strings.ReplaceAll(*v.BrokerList, "'", "''") + "'"
	}
	if v.TopicList != nil {
		settings["kafka_topic_list"] = "'" + strings.ReplaceAll(*v.TopicList, "'", "''") + "'"
	}
	if v.GroupName != nil {
		settings["kafka_group_name"] = "'" + strings.ReplaceAll(*v.GroupName, "'", "''") + "'"
	}
	if v.Format != nil {
		settings["kafka_format"] = "'" + *v.Format + "'"
	}
	addStrSetting := func(key string, p *string) {
		if p != nil {
			settings[key] = "'" + strings.ReplaceAll(*p, "'", "''") + "'"
		}
	}
	addIntSetting := func(key string, p *int64) {
		if p != nil {
			settings[key] = fmt.Sprintf("%d", *p)
		}
	}
	addBoolSetting := func(key string, p *bool) {
		if p != nil {
			if *p {
				settings[key] = "1"
			} else {
				settings[key] = "0"
			}
		}
	}
	addStrSetting("kafka_security_protocol", v.SecurityProtocol)
	addStrSetting("kafka_sasl_mechanism", v.SaslMechanism)
	addStrSetting("kafka_sasl_username", v.SaslUsername)
	addStrSetting("kafka_sasl_password", v.SaslPassword)
	addStrSetting("kafka_client_id", v.ClientID)
	addStrSetting("kafka_schema", v.Schema)
	addStrSetting("kafka_handle_error_mode", v.HandleErrorMode)
	addStrSetting("kafka_compression_codec", v.CompressionCodec)
	addIntSetting("kafka_num_consumers", v.NumConsumers)
	addIntSetting("kafka_max_block_size", v.MaxBlockSize)
	addIntSetting("kafka_skip_broken_messages", v.SkipBrokenMessages)
	addIntSetting("kafka_poll_timeout_ms", v.PollTimeoutMs)
	addIntSetting("kafka_poll_max_batch_size", v.PollMaxBatchSize)
	addIntSetting("kafka_flush_interval_ms", v.FlushIntervalMs)
	addIntSetting("kafka_consumer_reschedule_ms", v.ConsumerRescheduleMs)
	addIntSetting("kafka_max_rows_per_message", v.MaxRowsPerMessage)
	addIntSetting("kafka_compression_level", v.CompressionLevel)
	addBoolSetting("kafka_commit_every_batch", v.CommitEveryBatch)
	addBoolSetting("kafka_thread_per_consumer", v.ThreadPerConsumer)
	addBoolSetting("kafka_commit_on_select", v.CommitOnSelect)
	addBoolSetting("kafka_autodetect_client_rack", v.AutodetectClientRack)
	for k, val := range v.Extra {
		settings[k] = val
	}
	return "Kafka()", settings
```

NOTE: the existing `engineSQL` signature returns `(string, map[string]string)` where the second return is "extra settings to fold into the table-level SETTINGS clause". Confirm the helper inside the existing implementation handles `settings` values that are already SQL-formatted (e.g. wrapped in single quotes for strings, bare for numbers). Check `formatSettingsList` in `sqlgen.go` to see how it handles values — if it always single-quotes everything, then we need to NOT pre-quote here. **Read `formatSettingsList` first to determine the right format for setting values.**

If `formatSettingsList` already does the quoting, simplify the helpers above so they return raw values (no quotes), and let `formatSettingsList` handle it.

- [ ] **Step 5: Update `writeEngine` Kafka case in `dump.go`**

Replace the existing Kafka case (around lines 151-156) with:

```go
case EngineKafka:
	if v.Collection != nil {
		b.SetAttributeValue("collection", cty.StringVal(*v.Collection))
		return
	}
	setStr := func(name string, p *string) {
		if p != nil {
			b.SetAttributeValue(name, cty.StringVal(*p))
		}
	}
	setInt := func(name string, p *int64) {
		if p != nil {
			b.SetAttributeValue(name, cty.NumberIntVal(*p))
		}
	}
	setBool := func(name string, p *bool) {
		if p != nil {
			if *p {
				b.SetAttributeValue(name, cty.True)
			} else {
				b.SetAttributeValue(name, cty.False)
			}
		}
	}
	setStr("broker_list", v.BrokerList)
	setStr("topic_list", v.TopicList)
	setStr("group_name", v.GroupName)
	setStr("format", v.Format)
	setStr("security_protocol", v.SecurityProtocol)
	setStr("sasl_mechanism", v.SaslMechanism)
	setStr("sasl_username", v.SaslUsername)
	setStr("sasl_password", v.SaslPassword)
	setStr("client_id", v.ClientID)
	setStr("schema", v.Schema)
	setStr("handle_error_mode", v.HandleErrorMode)
	setStr("compression_codec", v.CompressionCodec)
	setInt("num_consumers", v.NumConsumers)
	setInt("max_block_size", v.MaxBlockSize)
	setInt("skip_broken_messages", v.SkipBrokenMessages)
	setInt("poll_timeout_ms", v.PollTimeoutMs)
	setInt("poll_max_batch_size", v.PollMaxBatchSize)
	setInt("flush_interval_ms", v.FlushIntervalMs)
	setInt("consumer_reschedule_ms", v.ConsumerRescheduleMs)
	setInt("max_rows_per_message", v.MaxRowsPerMessage)
	setInt("compression_level", v.CompressionLevel)
	setBool("commit_every_batch", v.CommitEveryBatch)
	setBool("thread_per_consumer", v.ThreadPerConsumer)
	setBool("commit_on_select", v.CommitOnSelect)
	setBool("autodetect_client_rack", v.AutodetectClientRack)
	if len(v.Extra) > 0 {
		b.SetAttributeValue("extra", stringMap(v.Extra))
	}
```

- [ ] **Step 6: Update `testdata/engines_all_kinds.hcl`**

Find the existing `engine "kafka" {}` block (likely uses `broker_list = ["..."]`, `topic`, `consumer_group`, `format`). Replace with:

```hcl
engine "kafka" {
  broker_list = "kafka:9092"
  topic_list  = "events"
  group_name  = "group1"
  format      = "JSONEachRow"
}
```

(Note: `broker_list` is now a single comma-joined string, not a list.)

- [ ] **Step 7: Update `engines_test.go` Kafka cases**

Find every reference to `EngineKafka{...}` in `engines_test.go`. Replace literal field usage:
- `BrokerList: []string{"kafka:9092"}` → `BrokerList: ptr("kafka:9092")`
- `Topic: "events"` → `TopicList: ptr("events")`
- `ConsumerGroup: "group1"` → `GroupName: ptr("group1")`
- `Format: "JSONEachRow"` → `Format: ptr("JSONEachRow")`

If `engines_test.go` doesn't already import the `ptr` helper from `parser_test.go`, it does (both files are in the same package).

- [ ] **Step 8: Update `sqlgen_test.go::TestSQLGen_CreateKafkaFoldsSettings`**

Find the existing test (around line 80). Update both the input EngineKafka literal (same field renames as Step 7) and the expected SQL. The new expected SQL emits typed fields ordered alphabetically by setting key after `formatSettingsList` sorts them; verify the actual output and pin the test's `expected` to match. Sample expected:

```go
expected := `CREATE TABLE posthog.ingest (
  id UUID
) ENGINE = Kafka() SETTINGS kafka_broker_list = 'kafka:9092', kafka_format = 'JSONEachRow', kafka_group_name = 'group1', kafka_topic_list = 'events'`
```

(Confirm exact format by running the test once after the other migrations and pinning to actual output.)

- [ ] **Step 9: Update `TestCHLive_HCLIntrospect` Kafka case in `introspect_live_test.go`**

Find the test's `expected` map for the Kafka subtest. Replace the existing EngineKafka literal field usage with the new shape (same renames as Step 7). The DDL string in the test's `sql` is independent — it's raw ClickHouse SQL that uses `kafka_*` settings, no change needed there.

- [ ] **Step 10: Run all unit tests**

Run: `go test ./internal/loader/hcl ./cmd/hclexp`
Expected: PASS — every Kafka-touching test should now build and pass.

- [ ] **Step 11: Commit**

```bash
git add internal/loader/hcl/engines.go internal/loader/hcl/introspect.go internal/loader/hcl/sqlgen.go internal/loader/hcl/dump.go internal/loader/hcl/testdata/engines_all_kinds.hcl internal/loader/hcl/engines_test.go internal/loader/hcl/sqlgen_test.go internal/loader/hcl/introspect_live_test.go
git commit -m "feat: reshape EngineKafka with typed settings + collection ref

EngineKafka grows from 4 fields to ~25 typed fields (one per documented
kafka_* setting) plus a Collection reference and an Extra escape map.
The 4 typed fields are mutually exclusive with Collection. Introspection
captures all kafka_* settings into typed fields with native types
(int64 for numbers, bool for 0/1 settings); unknown keys land in Extra.
SQL generation reverses the mapping at emit time.

Breaking change to EngineKafka's field set; updates engines_all_kinds.hcl,
TestCHLive_HCLIntrospect, and existing Kafka unit tests.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: `ParseFile` returns `*Schema`; migrate callers

**Files:**
- Modify: `internal/loader/hcl/parser.go`
- Modify: `internal/loader/hcl/layers.go`
- Modify: `internal/loader/hcl/dump.go` (Write signature)
- Modify: `internal/loader/hcl/resolver.go` (Resolve signature)
- Modify: `internal/loader/hcl/diff.go` (Diff signature)
- Modify: `cmd/hclexp/hclexp.go`
- Modify: `cmd/hclexp/hclexp_test.go`
- Modify: every other test file that calls `ParseFile`, `Resolve`, or `Write`.

- [ ] **Step 1: Update `fileSpec` and `ParseFile` in `parser.go`**

In `internal/loader/hcl/parser.go`:

```go
type fileSpec struct {
	Databases        []DatabaseSpec        `hcl:"database,block"`
	NamedCollections []NamedCollectionSpec `hcl:"named_collection,block"`
}

// ParseFile parses a single HCL file and returns the declared schema.
// Diagnostics are formatted into the returned error.
func ParseFile(path string) (*Schema, error) {
	parser := hclparse.NewParser()
	f, diags := parser.ParseHCLFile(path)
	if diags.HasErrors() {
		return nil, formatDiagnostics(parser, diags)
	}

	var spec fileSpec
	if diags := gohcl.DecodeBody(f.Body, nil, &spec); diags.HasErrors() {
		return nil, formatDiagnostics(parser, diags)
	}

	// existing per-database engine/dictionary decode loop, unchanged.
	for di := range spec.Databases {
		db := &spec.Databases[di]
		for ti := range db.Tables {
			tbl := &db.Tables[ti]
			if tbl.Engine == nil {
				continue
			}
			decoded, err := DecodeEngine(tbl.Engine)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", db.Name, tbl.Name, err)
			}
			tbl.Engine.Decoded = decoded
		}
		for i := range db.Dictionaries {
			d := &db.Dictionaries[i]
			if d.Source != nil {
				decoded, err := DecodeDictionarySource(d.Source)
				if err != nil {
					return nil, fmt.Errorf("%s.%s: %w", db.Name, d.Name, err)
				}
				d.Source.Decoded = decoded
			}
			if d.Layout != nil {
				decoded, err := DecodeDictionaryLayout(d.Layout)
				if err != nil {
					return nil, fmt.Errorf("%s.%s: %w", db.Name, d.Name, err)
				}
				d.Layout.Decoded = decoded
			}
		}
	}

	return &Schema{
		Databases:        spec.Databases,
		NamedCollections: spec.NamedCollections,
	}, nil
}
```

- [ ] **Step 2: Update `LoadLayers` in `layers.go`**

Change `LoadLayers([]string) ([]DatabaseSpec, error)` to `LoadLayers([]string) (*Schema, error)`. The body builds up databases as today but also tracks named collections from each parsed file. Adjacent layers' NCs accumulate with the override flag for duplicates.

Replace the relevant body sections:

```go
func LoadLayers(layerDirs []string) (*Schema, error) {
	registry := map[string]*DatabaseSpec{}
	var ordered []string
	ncByName := map[string]*NamedCollectionSpec{}
	var ncOrder []string

	for _, dir := range layerDirs {
		files, err := hclFilesIn(dir)
		if err != nil {
			return nil, err
		}
		for _, file := range files {
			parsed, err := ParseFile(file)
			if err != nil {
				return nil, err
			}
			for _, db := range parsed.Databases {
				if existing, ok := registry[db.Name]; ok {
					if err := mergeIntoDatabase(existing, db); err != nil {
						return nil, fmt.Errorf("%s: %w", file, err)
					}
				} else {
					cp := db
					registry[db.Name] = &cp
					ordered = append(ordered, db.Name)
				}
			}
			for _, nc := range parsed.NamedCollections {
				if existing, ok := ncByName[nc.Name]; ok {
					if !nc.Override {
						return nil, fmt.Errorf("%s: named collection %q redeclared without override = true", file, nc.Name)
					}
					*existing = nc
				} else {
					cp := nc
					ncByName[nc.Name] = &cp
					ncOrder = append(ncOrder, nc.Name)
				}
			}
		}
	}

	out := &Schema{}
	for _, name := range ordered {
		out.Databases = append(out.Databases, *registry[name])
	}
	for _, name := range ncOrder {
		out.NamedCollections = append(out.NamedCollections, *ncByName[name])
	}
	return out, nil
}
```

- [ ] **Step 3: Update `Resolve` in `resolver.go`**

Change signature from `Resolve([]DatabaseSpec) error` to `Resolve(*Schema) error`. Body iterates `s.Databases` as today; named-collection validation lands in Task 5.

```go
func Resolve(s *Schema) error {
	if s == nil {
		return errors.New("Resolve: nil schema")
	}
	for di := range s.Databases {
		if err := applyPatches(&s.Databases[di]); err != nil {
			return err
		}
		if err := resolveDatabase(&s.Databases[di]); err != nil {
			return err
		}
		if err := validateDictionaries(&s.Databases[di]); err != nil {
			return err
		}
	}
	return nil
}
```

- [ ] **Step 4: Update `Write` in `dump.go`**

Change signature from `Write(w io.Writer, dbs []DatabaseSpec) error` to `Write(w io.Writer, schema *Schema) error`. Body iterates `schema.Databases`. NC emission lands in Task 7.

```go
func Write(w io.Writer, schema *Schema) error {
	if schema == nil {
		return errors.New("Write: nil schema")
	}
	f := hclwrite.NewEmptyFile()
	body := f.Body()

	for i, db := range schema.Databases {
		if i > 0 {
			body.AppendNewline()
		}
		dbBlock := body.AppendNewBlock("database", []string{db.Name})
		writeDatabase(dbBlock.Body(), db)
	}

	_, err := w.Write(f.Bytes())
	return err
}
```

Add `"errors"` to imports if needed.

- [ ] **Step 5: Update `Diff` in `diff.go`**

Change signature from `Diff(from, to []DatabaseSpec) ChangeSet` to `Diff(from, to *Schema) ChangeSet`. Body uses `from.Databases` and `to.Databases`. NC integration lands in Task 9.

```go
func Diff(from, to *Schema) ChangeSet {
	if from == nil { from = &Schema{} }
	if to == nil { to = &Schema{} }
	fromIdx := indexDatabases(from.Databases)
	toIdx := indexDatabases(to.Databases)
	// ... rest of existing body, replacing `from` with `from.Databases` where it was used as a slice
}
```

- [ ] **Step 6: Update `cmd/hclexp/hclexp.go`**

Find every site where `ParseFile`, `LoadLayers`, `Resolve`, `Write`, or `Diff` is called. Update types:

- `dbs, err := ParseFile(path)` → `schema, err := ParseFile(path)`
- `dbs, err := LoadLayers(layers)` → `schema, err := LoadLayers(layers)`
- `Resolve(dbs)` → `Resolve(schema)`
- `Write(w, dbs)` → `Write(w, schema)`
- `Diff(a, b)` → `Diff(schemaA, schemaB)`

`loadSide(s string) ([]DatabaseSpec, error)` becomes `loadSide(s string) (*Schema, error)`. The introspect path (`Introspect(ctx, conn, db)`) currently returns `*DatabaseSpec`; wrap it in a Schema:

```go
db, err := hclload.Introspect(ctx, conn, dbName)
if err != nil {
	return nil, err
}
return &hclload.Schema{Databases: []hclload.DatabaseSpec{*db}}, nil
```

In Task 7 the introspect path will also call `IntrospectNamedCollections` and stitch it into the Schema.

- [ ] **Step 7: Update `cmd/hclexp/hclexp_test.go`**

Mechanical update: every test that calls `loadSide`, `ParseFile`, `Resolve`, or `Write` adapts to the new `*Schema` return type. For example:

```go
// before
dbs, err := loadSide(path)
require.NoError(t, err)
require.Len(t, dbs, 1)
require.Equal(t, "posthog", dbs[0].Name)

// after
schema, err := loadSide(path)
require.NoError(t, err)
require.Len(t, schema.Databases, 1)
require.Equal(t, "posthog", schema.Databases[0].Name)
```

`TestRunDiff_RenderChangeSet` calls `hclload.Diff(leftDBs, rightDBs)`; replace with `hclload.Diff(left, right)` where `left` and `right` are `*Schema`. `TestRunDiff_SelfDiffEmpty` similarly.

- [ ] **Step 8: Update every other test file in `internal/loader/hcl/`**

Mechanical sweep across these test files (find via `grep -rln "ParseFile\|Resolve\|Write\|Diff(" internal/loader/hcl/*_test.go`):

- `parser_test.go` — every `dbs, err := ParseFile(...)` becomes `schema, err := ParseFile(...)`; then `dbs[0]` becomes `schema.Databases[0]`.
- `resolver_test.go` — `Resolve([]DatabaseSpec{...})` becomes `Resolve(&Schema{Databases: []DatabaseSpec{...}})`.
- `dump_test.go::roundTrip` helper — `before, err := ParseFile(...)` returns `*Schema`; downstream code uses `before.Databases`.
- `diff_test.go` — `Diff(from, to)` where from/to are `[]DatabaseSpec` becomes `Diff(&Schema{Databases: from}, &Schema{Databases: to})`. Helper `mkDB` is unchanged.

This is purely mechanical type-threading. Do it carefully and re-run tests after each file.

- [ ] **Step 9: Run all unit tests**

Run: `go test ./internal/loader/hcl ./cmd/hclexp`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add internal/loader/hcl/parser.go internal/loader/hcl/layers.go internal/loader/hcl/resolver.go internal/loader/hcl/dump.go internal/loader/hcl/diff.go cmd/hclexp/hclexp.go cmd/hclexp/hclexp_test.go internal/loader/hcl/parser_test.go internal/loader/hcl/resolver_test.go internal/loader/hcl/dump_test.go internal/loader/hcl/diff_test.go
git commit -m "feat: ParseFile/LoadLayers/Write/Resolve/Diff use *Schema

Schema is the new aggregate that carries both databases and named
collections. ParseFile populates the NamedCollections field but no
downstream code reads it yet (subsequent tasks wire it up). Mechanical
migration of every caller in the package and the cmd/hclexp tree.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Parser test fixtures and parser tests

**Files:**
- Create: `internal/loader/hcl/testdata/named_collection.hcl`
- Create: `internal/loader/hcl/testdata/kafka_with_collection.hcl`
- Create: `internal/loader/hcl/testdata/kafka_inline_settings.hcl`
- Modify: `internal/loader/hcl/parser_test.go`

- [ ] **Step 1: Write `testdata/named_collection.hcl`**

```hcl
named_collection "my_kafka" {
  cluster = "posthog"
  comment = "shared kafka cluster for events ingestion"

  param "kafka_broker_list" { value = "k1:9092,k2:9092" }
  param "kafka_topic_list"  { value = "events" }
  param "kafka_group_name"  { value = "ch_events" }
  param "kafka_format"      { value = "JSONEachRow" }
  param "kafka_sasl_password" {
    value       = "[set via override layer]"
    overridable = false
  }
}

named_collection "external_xml_managed" {
  external = true
  comment  = "managed in /etc/clickhouse-server/config.d/kafka.xml"
}
```

- [ ] **Step 2: Write `testdata/kafka_with_collection.hcl`**

```hcl
named_collection "my_kafka" {
  param "kafka_broker_list" { value = "kafka:9092" }
  param "kafka_topic_list"  { value = "events" }
  param "kafka_group_name"  { value = "g1" }
  param "kafka_format"      { value = "JSONEachRow" }
}

database "posthog" {
  table "events_kafka" {
    column "team_id" { type = "Int64" }
    column "payload" { type = "String" }
    engine "kafka" { collection = "my_kafka" }
  }
}
```

- [ ] **Step 3: Write `testdata/kafka_inline_settings.hcl`**

```hcl
database "posthog" {
  table "events_kafka" {
    column "team_id" { type = "Int64" }
    column "payload" { type = "String" }
    engine "kafka" {
      broker_list          = "kafka:9092"
      topic_list           = "events"
      group_name           = "ch_events"
      format               = "JSONEachRow"
      num_consumers        = 4
      max_block_size       = 1048576
      commit_on_select     = false
      skip_broken_messages = 100
      handle_error_mode    = "stream"
      sasl_mechanism       = "PLAIN"
      sasl_username        = "ch"
      sasl_password        = "[set via override layer]"
      extra = {
        kafka_some_future_setting = "foo"
      }
    }
  }
}
```

- [ ] **Step 4: Add parser tests in `parser_test.go`**

Append to `internal/loader/hcl/parser_test.go`:

```go
func TestParseFile_NamedCollection(t *testing.T) {
	schema, err := ParseFile(filepath.Join("testdata", "named_collection.hcl"))
	require.NoError(t, err)

	require.Len(t, schema.NamedCollections, 2)

	mk := schema.NamedCollections[0]
	assert.Equal(t, "my_kafka", mk.Name)
	assert.False(t, mk.External)
	require.NotNil(t, mk.Cluster)
	assert.Equal(t, "posthog", *mk.Cluster)
	require.NotNil(t, mk.Comment)
	assert.Equal(t, "shared kafka cluster for events ingestion", *mk.Comment)
	require.Len(t, mk.Params, 5)
	assert.Equal(t, "kafka_broker_list", mk.Params[0].Key)
	assert.Equal(t, "k1:9092,k2:9092", mk.Params[0].Value)
	assert.Nil(t, mk.Params[0].Overridable)
	assert.Equal(t, "kafka_sasl_password", mk.Params[4].Key)
	require.NotNil(t, mk.Params[4].Overridable)
	assert.False(t, *mk.Params[4].Overridable)

	ext := schema.NamedCollections[1]
	assert.Equal(t, "external_xml_managed", ext.Name)
	assert.True(t, ext.External)
	assert.Empty(t, ext.Params)
}

func TestParseFile_KafkaWithCollection(t *testing.T) {
	schema, err := ParseFile(filepath.Join("testdata", "kafka_with_collection.hcl"))
	require.NoError(t, err)

	require.Len(t, schema.NamedCollections, 1)
	require.Len(t, schema.Databases, 1)
	require.Len(t, schema.Databases[0].Tables, 1)
	tbl := schema.Databases[0].Tables[0]
	require.NotNil(t, tbl.Engine)
	kafkaEng, ok := tbl.Engine.Decoded.(EngineKafka)
	require.True(t, ok)
	require.NotNil(t, kafkaEng.Collection)
	assert.Equal(t, "my_kafka", *kafkaEng.Collection)
	assert.Nil(t, kafkaEng.BrokerList)
	assert.Nil(t, kafkaEng.TopicList)
	assert.Nil(t, kafkaEng.GroupName)
	assert.Nil(t, kafkaEng.Format)
}

func TestParseFile_KafkaInlineSettings(t *testing.T) {
	schema, err := ParseFile(filepath.Join("testdata", "kafka_inline_settings.hcl"))
	require.NoError(t, err)

	require.Len(t, schema.Databases, 1)
	tbl := schema.Databases[0].Tables[0]
	kafkaEng, ok := tbl.Engine.Decoded.(EngineKafka)
	require.True(t, ok)

	assert.Nil(t, kafkaEng.Collection)
	require.NotNil(t, kafkaEng.BrokerList)
	assert.Equal(t, "kafka:9092", *kafkaEng.BrokerList)
	require.NotNil(t, kafkaEng.NumConsumers)
	assert.Equal(t, int64(4), *kafkaEng.NumConsumers)
	require.NotNil(t, kafkaEng.MaxBlockSize)
	assert.Equal(t, int64(1048576), *kafkaEng.MaxBlockSize)
	require.NotNil(t, kafkaEng.CommitOnSelect)
	assert.False(t, *kafkaEng.CommitOnSelect)
	require.NotNil(t, kafkaEng.SkipBrokenMessages)
	assert.Equal(t, int64(100), *kafkaEng.SkipBrokenMessages)
	require.NotNil(t, kafkaEng.HandleErrorMode)
	assert.Equal(t, "stream", *kafkaEng.HandleErrorMode)

	require.NotNil(t, kafkaEng.Extra)
	assert.Equal(t, "foo", kafkaEng.Extra["kafka_some_future_setting"])
}
```

- [ ] **Step 5: Run parser tests**

Run: `go test ./internal/loader/hcl -run TestParseFile_NamedCollection -v && go test ./internal/loader/hcl -run TestParseFile_Kafka -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/loader/hcl/testdata/named_collection.hcl internal/loader/hcl/testdata/kafka_with_collection.hcl internal/loader/hcl/testdata/kafka_inline_settings.hcl internal/loader/hcl/parser_test.go
git commit -m "test: HCL parser fixtures for named collections + Kafka

Covers: named_collection blocks (managed + external), Kafka engine
with named collection reference, Kafka engine with canonical inline
settings including typed numbers/bools and the extras escape map.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Resolver — named-collection + Kafka validation

**Files:**
- Modify: `internal/loader/hcl/resolver.go`
- Modify: `internal/loader/hcl/resolver_test.go`

- [ ] **Step 1: Write failing tests in `resolver_test.go`**

Append:

```go
func TestResolve_NamedCollection_Validation(t *testing.T) {
	cases := []struct {
		name    string
		schema  *Schema
		errSubs string
	}{
		{
			name: "duplicate names",
			schema: &Schema{NamedCollections: []NamedCollectionSpec{
				{Name: "x", Params: []NamedCollectionParam{{Key: "a", Value: "1"}}},
				{Name: "x", Params: []NamedCollectionParam{{Key: "b", Value: "2"}}},
			}},
			errSubs: "duplicate",
		},
		{
			name: "duplicate param keys",
			schema: &Schema{NamedCollections: []NamedCollectionSpec{
				{Name: "x", Params: []NamedCollectionParam{
					{Key: "a", Value: "1"},
					{Key: "a", Value: "2"},
				}},
			}},
			errSubs: "duplicate param",
		},
		{
			name: "empty params on managed NC",
			schema: &Schema{NamedCollections: []NamedCollectionSpec{
				{Name: "x"},
			}},
			errSubs: "non-empty",
		},
		{
			name: "empty params allowed on external NC",
			schema: &Schema{NamedCollections: []NamedCollectionSpec{
				{Name: "x", External: true},
			}},
			errSubs: "", // should pass
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Resolve(tc.schema)
			if tc.errSubs == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errSubs)
			}
		})
	}
}

func TestResolve_KafkaEngine_XOR(t *testing.T) {
	mkTblWithKafka := func(eng EngineKafka) *Schema {
		return &Schema{Databases: []DatabaseSpec{{
			Name: "db",
			Tables: []TableSpec{{
				Name:    "t",
				Columns: []ColumnSpec{{Name: "id", Type: "UInt64"}},
				OrderBy: []string{"id"},
				Engine: &EngineSpec{
					Kind:    "kafka",
					Decoded: eng,
				},
			}},
		}}}
	}
	cases := []struct {
		name    string
		eng     EngineKafka
		errSubs string
		setupNC bool
	}{
		{
			name:    "neither collection nor inline",
			eng:     EngineKafka{},
			errSubs: "requires either",
		},
		{
			name:    "collection AND inline broker_list",
			eng:     EngineKafka{Collection: ptr("nc1"), BrokerList: ptr("k:9092")},
			errSubs: "mutually exclusive",
			setupNC: true,
		},
		{
			name:    "collection AND extra",
			eng:     EngineKafka{Collection: ptr("nc1"), Extra: map[string]string{"kafka_x": "y"}},
			errSubs: "mutually exclusive",
			setupNC: true,
		},
		{
			name:    "inline missing topic_list",
			eng:     EngineKafka{BrokerList: ptr("k:9092"), GroupName: ptr("g"), Format: ptr("JSONEachRow")},
			errSubs: "topic_list",
		},
		{
			name: "valid inline",
			eng: EngineKafka{
				BrokerList: ptr("k:9092"),
				TopicList:  ptr("events"),
				GroupName:  ptr("g"),
				Format:     ptr("JSONEachRow"),
			},
		},
		{
			name:    "valid collection",
			eng:     EngineKafka{Collection: ptr("nc1")},
			setupNC: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mkTblWithKafka(tc.eng)
			if tc.setupNC {
				s.NamedCollections = []NamedCollectionSpec{{
					Name:   "nc1",
					Params: []NamedCollectionParam{{Key: "kafka_broker_list", Value: "k:9092"}},
				}}
			}
			err := Resolve(s)
			if tc.errSubs == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errSubs)
			}
		})
	}
}

func TestResolve_KafkaCollectionReference(t *testing.T) {
	s := &Schema{Databases: []DatabaseSpec{{
		Name: "db",
		Tables: []TableSpec{{
			Name:    "t",
			Columns: []ColumnSpec{{Name: "id", Type: "UInt64"}},
			OrderBy: []string{"id"},
			Engine: &EngineSpec{
				Kind:    "kafka",
				Decoded: EngineKafka{Collection: ptr("undeclared_nc")},
			},
		}},
	}}}
	err := Resolve(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "undeclared_nc")
	assert.Contains(t, err.Error(), "not declared")
}
```

- [ ] **Step 2: Run failing tests**

Run: `go test ./internal/loader/hcl -run 'TestResolve_NamedCollection|TestResolve_KafkaEngine|TestResolve_KafkaCollectionReference' -v`
Expected: FAIL — validators don't exist yet.

- [ ] **Step 3: Add validators in `resolver.go`**

Append:

```go
// validateNamedCollections enforces uniqueness of names, uniqueness of
// param keys within each collection, and that Params is non-empty for
// managed (non-external) collections.
func validateNamedCollections(s *Schema) error {
	seen := map[string]bool{}
	for _, nc := range s.NamedCollections {
		if seen[nc.Name] {
			return fmt.Errorf("named_collection %q: duplicate", nc.Name)
		}
		seen[nc.Name] = true
		if !nc.External && len(nc.Params) == 0 {
			return fmt.Errorf("named_collection %q: requires non-empty params (or external = true)", nc.Name)
		}
		keys := map[string]bool{}
		for _, p := range nc.Params {
			if keys[p.Key] {
				return fmt.Errorf("named_collection %q: duplicate param %q", nc.Name, p.Key)
			}
			keys[p.Key] = true
		}
	}
	return nil
}

// validateKafkaEngines enforces XOR between collection and inline settings,
// required-fields-when-inline, and that referenced collections exist.
func validateKafkaEngines(s *Schema) error {
	ncDeclared := map[string]bool{}
	for _, nc := range s.NamedCollections {
		ncDeclared[nc.Name] = true
	}
	for _, db := range s.Databases {
		for _, t := range db.Tables {
			if t.Engine == nil || t.Engine.Decoded == nil {
				continue
			}
			k, ok := t.Engine.Decoded.(EngineKafka)
			if !ok {
				continue
			}
			hasInline := k.BrokerList != nil || k.TopicList != nil || k.GroupName != nil || k.Format != nil ||
				k.SecurityProtocol != nil || k.SaslMechanism != nil || k.SaslUsername != nil || k.SaslPassword != nil ||
				k.ClientID != nil || k.Schema != nil || k.HandleErrorMode != nil || k.CompressionCodec != nil ||
				k.NumConsumers != nil || k.MaxBlockSize != nil || k.SkipBrokenMessages != nil ||
				k.PollTimeoutMs != nil || k.PollMaxBatchSize != nil || k.FlushIntervalMs != nil ||
				k.ConsumerRescheduleMs != nil || k.MaxRowsPerMessage != nil || k.CompressionLevel != nil ||
				k.CommitEveryBatch != nil || k.ThreadPerConsumer != nil || k.CommitOnSelect != nil ||
				k.AutodetectClientRack != nil || len(k.Extra) > 0

			if k.Collection == nil && !hasInline {
				return fmt.Errorf("%s.%s: kafka engine requires either `collection` or inline settings", db.Name, t.Name)
			}
			if k.Collection != nil && hasInline {
				return fmt.Errorf("%s.%s: kafka engine `collection` and inline settings are mutually exclusive", db.Name, t.Name)
			}
			if k.Collection != nil {
				if !ncDeclared[*k.Collection] {
					return fmt.Errorf("%s.%s: kafka engine references collection %q which is not declared in the schema (declare with `named_collection %q {...}` or `external = true`)", db.Name, t.Name, *k.Collection, *k.Collection)
				}
			}
			if k.Collection == nil {
				if k.BrokerList == nil {
					return fmt.Errorf("%s.%s: kafka engine inline form requires broker_list", db.Name, t.Name)
				}
				if k.TopicList == nil {
					return fmt.Errorf("%s.%s: kafka engine inline form requires topic_list", db.Name, t.Name)
				}
				if k.GroupName == nil {
					return fmt.Errorf("%s.%s: kafka engine inline form requires group_name", db.Name, t.Name)
				}
				if k.Format == nil {
					return fmt.Errorf("%s.%s: kafka engine inline form requires format", db.Name, t.Name)
				}
			}
		}
	}
	return nil
}
```

Wire both validators into `Resolve`:

```go
func Resolve(s *Schema) error {
	if s == nil {
		return errors.New("Resolve: nil schema")
	}
	if err := validateNamedCollections(s); err != nil {
		return err
	}
	for di := range s.Databases {
		if err := applyPatches(&s.Databases[di]); err != nil {
			return err
		}
		if err := resolveDatabase(&s.Databases[di]); err != nil {
			return err
		}
		if err := validateDictionaries(&s.Databases[di]); err != nil {
			return err
		}
	}
	if err := validateKafkaEngines(s); err != nil {
		return err
	}
	return nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/loader/hcl -run 'TestResolve_' -v`
Expected: PASS — all subcases.

- [ ] **Step 5: Commit**

```bash
git add internal/loader/hcl/resolver.go internal/loader/hcl/resolver_test.go
git commit -m "feat: resolver validates named collections and kafka XOR

validateNamedCollections checks name uniqueness, param-key uniqueness,
and non-empty Params on managed collections. validateKafkaEngines
enforces XOR between collection-reference and inline settings,
required-fields-when-inline, and that referenced collections exist
in the schema.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Introspect named collections

**Files:**
- Create: `internal/loader/hcl/named_collection_introspect.go`
- Create: `internal/loader/hcl/named_collection_introspect_test.go`
- Modify: `cmd/hclexp/hclexp.go` (call `IntrospectNamedCollections` in introspect path)

- [ ] **Step 1: Write the failing test**

Create `internal/loader/hcl/named_collection_introspect_test.go`:

```go
package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildNamedCollectionFromCreateNC_Full(t *testing.T) {
	src := `CREATE NAMED COLLECTION my_kafka ON CLUSTER posthog AS
kafka_broker_list = 'k1:9092,k2:9092',
kafka_topic_list = 'events',
kafka_group_name = 'ch_events' OVERRIDABLE,
kafka_format = 'JSONEachRow' NOT OVERRIDABLE,
kafka_sasl_password = 'secret'`

	got, err := buildNamedCollectionFromCreateSQL(src)
	require.NoError(t, err)

	assert.Equal(t, "my_kafka", got.Name)
	require.NotNil(t, got.Cluster)
	assert.Equal(t, "posthog", *got.Cluster)
	require.Len(t, got.Params, 5)

	assert.Equal(t, "kafka_broker_list", got.Params[0].Key)
	assert.Equal(t, "k1:9092,k2:9092", got.Params[0].Value)
	assert.Nil(t, got.Params[0].Overridable)

	assert.Equal(t, "kafka_group_name", got.Params[2].Key)
	assert.Equal(t, "ch_events", got.Params[2].Value)
	require.NotNil(t, got.Params[2].Overridable)
	assert.True(t, *got.Params[2].Overridable)

	assert.Equal(t, "kafka_format", got.Params[3].Key)
	assert.Equal(t, "JSONEachRow", got.Params[3].Value)
	require.NotNil(t, got.Params[3].Overridable)
	assert.False(t, *got.Params[3].Overridable)
}

func TestBuildNamedCollectionFromCreateNC_NoCluster(t *testing.T) {
	src := `CREATE NAMED COLLECTION simple AS a = '1', b = '2'`
	got, err := buildNamedCollectionFromCreateSQL(src)
	require.NoError(t, err)
	assert.Equal(t, "simple", got.Name)
	assert.Nil(t, got.Cluster)
	require.Len(t, got.Params, 2)
	assert.Equal(t, "a", got.Params[0].Key)
	assert.Equal(t, "1", got.Params[0].Value)
}
```

- [ ] **Step 2: Run failing test**

Run: `go test ./internal/loader/hcl -run TestBuildNamedCollectionFromCreateNC -v`
Expected: FAIL — `undefined: buildNamedCollectionFromCreateSQL`.

- [ ] **Step 3: Implement `named_collection_introspect.go`**

Create `internal/loader/hcl/named_collection_introspect.go`:

```go
package hcl

import (
	"context"
	"errors"
	"fmt"

	chparser "github.com/AfterShip/clickhouse-sql-parser/parser"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// IntrospectNamedCollections returns every DDL-managed named collection
// the live ClickHouse cluster knows about. Config-file collections
// (source != 'DDL') are filtered out — they're not ours to manage.
func IntrospectNamedCollections(ctx context.Context, conn driver.Conn) ([]NamedCollectionSpec, error) {
	const q = `SELECT name, create_query FROM system.named_collections WHERE source = 'DDL' ORDER BY name`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query system.named_collections: %w", err)
	}
	defer rows.Close()

	var out []NamedCollectionSpec
	for rows.Next() {
		var name, createSQL string
		if err := rows.Scan(&name, &createSQL); err != nil {
			return nil, fmt.Errorf("scan system.named_collections: %w", err)
		}
		nc, err := buildNamedCollectionFromCreateSQL(createSQL)
		if err != nil {
			return nil, fmt.Errorf("parse create_query for %s: %w", name, err)
		}
		// Sanity: name from system table should match the one in create_query.
		if nc.Name == "" {
			nc.Name = name
		}
		out = append(out, nc)
	}
	return out, rows.Err()
}

// buildNamedCollectionFromCreateSQL parses a CREATE NAMED COLLECTION
// statement and returns the corresponding NamedCollectionSpec.
func buildNamedCollectionFromCreateSQL(createSQL string) (NamedCollectionSpec, error) {
	stmt, err := parseCreateStatement(createSQL)
	if err != nil {
		return NamedCollectionSpec{}, err
	}
	cnc, ok := stmt.(*chparser.CreateNamedCollection)
	if !ok {
		return NamedCollectionSpec{}, errors.New("no CREATE NAMED COLLECTION statement found")
	}
	return buildNamedCollectionFromAST(cnc)
}

func buildNamedCollectionFromAST(cnc *chparser.CreateNamedCollection) (NamedCollectionSpec, error) {
	out := NamedCollectionSpec{}
	if cnc.Name != nil {
		out.Name = dictIdent(cnc.Name)
	}
	if cnc.OnCluster != nil && cnc.OnCluster.Expr != nil {
		out.Cluster = strPtr(formatNode(cnc.OnCluster.Expr))
	}
	for _, p := range cnc.Params {
		if p == nil || p.Name == nil {
			continue
		}
		key := dictIdent(p.Name)
		val := ""
		if p.Value != nil {
			val = dictArgValueString(p.Value)
		}
		ncp := NamedCollectionParam{Key: key, Value: val}
		switch {
		case p.Overridable:
			v := true
			ncp.Overridable = &v
		case p.NotOverridable:
			v := false
			ncp.Overridable = &v
		}
		out.Params = append(out.Params, ncp)
	}
	return out, nil
}
```

`dictIdent` and `dictArgValueString` already exist in `dictionary_introspect.go` and are reusable. `parseCreateStatement` exists in `introspect.go`.

- [ ] **Step 4: Run test**

Run: `go test ./internal/loader/hcl -run TestBuildNamedCollectionFromCreateNC -v`
Expected: PASS.

- [ ] **Step 5: Wire `IntrospectNamedCollections` into `cmd/hclexp/hclexp.go`**

Find the introspect subcommand's body where `Introspect(ctx, conn, dbName)` is called. After the database loop completes (or alongside it), call `IntrospectNamedCollections(ctx, conn)` once and store the result in the schema. Update the introspect path's return value to include both:

```go
// at the appropriate place in the introspect command body
ncs, err := hclload.IntrospectNamedCollections(ctx, conn)
if err != nil {
	return err
}
schema.NamedCollections = ncs
```

Add `"named_collections", len(schema.NamedCollections)` to the introspect summary `slog.Info` call.

- [ ] **Step 6: Build + run full unit tests**

Run: `go test ./internal/loader/hcl ./cmd/hclexp`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/loader/hcl/named_collection_introspect.go internal/loader/hcl/named_collection_introspect_test.go cmd/hclexp/hclexp.go
git commit -m "feat: introspect DDL-managed named collections

IntrospectNamedCollections queries system.named_collections (filtering
source = 'DDL') and parses each create_query via the chparser AST.
hclexp introspect now stitches the result into the Schema and reports
the count in the summary log line.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Dump named collections to HCL

**Files:**
- Create: `internal/loader/hcl/named_collection_dump.go`
- Modify: `internal/loader/hcl/dump.go` (call writeNamedCollection from Write)
- Modify: `internal/loader/hcl/dump_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/loader/hcl/dump_test.go`:

```go
func TestWrite_RoundTrip_NamedCollection(t *testing.T) {
	roundTripSchema(t, filepath.Join("testdata", "named_collection.hcl"))
}

func TestWrite_RoundTrip_KafkaWithCollection(t *testing.T) {
	roundTripSchema(t, filepath.Join("testdata", "kafka_with_collection.hcl"))
}

func TestWrite_RoundTrip_KafkaInlineSettings(t *testing.T) {
	roundTripSchema(t, filepath.Join("testdata", "kafka_inline_settings.hcl"))
}
```

If the existing `roundTrip` helper takes `[]DatabaseSpec`, add a `roundTripSchema` helper that takes a path and reads/dumps/re-reads as `*Schema`:

```go
func roundTripSchema(t *testing.T, file string) {
	t.Helper()
	before, err := ParseFile(file)
	require.NoError(t, err)
	require.NoError(t, Resolve(before))

	var buf bytes.Buffer
	require.NoError(t, Write(&buf, before))

	tmp := filepath.Join(t.TempDir(), "round_trip.hcl")
	require.NoError(t, os.WriteFile(tmp, buf.Bytes(), 0o644))

	after, err := ParseFile(tmp)
	require.NoError(t, err, "re-parse failed; dump output:\n%s", buf.String())
	require.NoError(t, Resolve(after))

	stripEngineBodies(before.Databases)
	stripEngineBodies(after.Databases)
	sortTables(before.Databases)
	sortTables(after.Databases)

	assert.Equal(t, before, after, "round-trip mismatch; dump output:\n%s", buf.String())
}
```

- [ ] **Step 2: Run failing test**

Run: `go test ./internal/loader/hcl -run TestWrite_RoundTrip_NamedCollection -v`
Expected: FAIL — `Write` doesn't emit `named_collection` blocks yet.

- [ ] **Step 3: Implement `named_collection_dump.go`**

Create `internal/loader/hcl/named_collection_dump.go`:

```go
package hcl

import (
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
)

func writeNamedCollection(body *hclwrite.Body, nc NamedCollectionSpec) {
	if nc.External {
		body.SetAttributeValue("external", cty.True)
	}
	if nc.Override {
		body.SetAttributeValue("override", cty.True)
	}
	if nc.Cluster != nil {
		body.SetAttributeValue("cluster", cty.StringVal(*nc.Cluster))
	}
	if nc.Comment != nil {
		body.SetAttributeValue("comment", cty.StringVal(*nc.Comment))
	}
	for _, p := range nc.Params {
		pb := body.AppendNewBlock("param", []string{p.Key}).Body()
		pb.SetAttributeValue("value", cty.StringVal(p.Value))
		if p.Overridable != nil {
			if *p.Overridable {
				pb.SetAttributeValue("overridable", cty.True)
			} else {
				pb.SetAttributeValue("overridable", cty.False)
			}
		}
	}
}
```

- [ ] **Step 4: Wire into `Write` in `dump.go`**

Extend `Write` to emit named collections after the database loop:

```go
func Write(w io.Writer, schema *Schema) error {
	if schema == nil {
		return errors.New("Write: nil schema")
	}
	f := hclwrite.NewEmptyFile()
	body := f.Body()

	for i, db := range schema.Databases {
		if i > 0 {
			body.AppendNewline()
		}
		dbBlock := body.AppendNewBlock("database", []string{db.Name})
		writeDatabase(dbBlock.Body(), db)
	}

	ncs := append([]NamedCollectionSpec(nil), schema.NamedCollections...)
	sort.Slice(ncs, func(i, j int) bool { return ncs[i].Name < ncs[j].Name })
	for i, nc := range ncs {
		if len(schema.Databases) > 0 || i > 0 {
			body.AppendNewline()
		}
		ncBlock := body.AppendNewBlock("named_collection", []string{nc.Name})
		writeNamedCollection(ncBlock.Body(), nc)
	}

	_, err := w.Write(f.Bytes())
	return err
}
```

- [ ] **Step 5: Run round-trip tests**

Run: `go test ./internal/loader/hcl -run TestWrite_RoundTrip -v`
Expected: PASS — all variants (basic, MV, dictionary, NC, kafka_with_collection, kafka_inline_settings).

- [ ] **Step 6: Commit**

```bash
git add internal/loader/hcl/named_collection_dump.go internal/loader/hcl/dump.go internal/loader/hcl/dump_test.go
git commit -m "feat: dump named collections as canonical HCL

Write emits a named_collection block per collection, sorted by name,
after the database blocks. External and Override flags, Cluster,
Comment, and each Param's value/overridable status all round-trip.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Diff named collections

**Files:**
- Create: `internal/loader/hcl/named_collection_diff.go`
- Modify: `internal/loader/hcl/diff.go` (ChangeSet field + Diff loop)
- Modify: `internal/loader/hcl/diff_test.go`

- [ ] **Step 1: Write failing tests in `diff_test.go`**

Append:

```go
func mkNC(name string, params ...NamedCollectionParam) NamedCollectionSpec {
	return NamedCollectionSpec{Name: name, Params: params}
}

func TestDiff_NamedCollections(t *testing.T) {
	base := mkNC("nc1",
		NamedCollectionParam{Key: "a", Value: "1"},
		NamedCollectionParam{Key: "b", Value: "2"},
	)

	t.Run("add", func(t *testing.T) {
		from := &Schema{}
		to := &Schema{NamedCollections: []NamedCollectionSpec{base}}
		cs := Diff(from, to)
		require.Len(t, cs.NamedCollections, 1)
		change := cs.NamedCollections[0]
		assert.Equal(t, "nc1", change.Name)
		require.NotNil(t, change.Add)
		assert.Equal(t, base, *change.Add)
	})

	t.Run("drop", func(t *testing.T) {
		from := &Schema{NamedCollections: []NamedCollectionSpec{base}}
		to := &Schema{}
		cs := Diff(from, to)
		require.Len(t, cs.NamedCollections, 1)
		assert.True(t, cs.NamedCollections[0].Drop)
		assert.Equal(t, "nc1", cs.NamedCollections[0].Name)
	})

	t.Run("set+delete params", func(t *testing.T) {
		changed := mkNC("nc1",
			NamedCollectionParam{Key: "a", Value: "1_new"},
			NamedCollectionParam{Key: "c", Value: "3"},
		)
		from := &Schema{NamedCollections: []NamedCollectionSpec{base}}
		to := &Schema{NamedCollections: []NamedCollectionSpec{changed}}
		cs := Diff(from, to)
		require.Len(t, cs.NamedCollections, 1)
		c := cs.NamedCollections[0]
		assert.False(t, c.Recreate)
		require.Len(t, c.SetParams, 2)
		setKeys := map[string]string{}
		for _, p := range c.SetParams {
			setKeys[p.Key] = p.Value
		}
		assert.Equal(t, "1_new", setKeys["a"])
		assert.Equal(t, "3", setKeys["c"])
		assert.Equal(t, []string{"b"}, c.DeleteParams)
	})

	t.Run("on cluster change recreates", func(t *testing.T) {
		from := &Schema{NamedCollections: []NamedCollectionSpec{base}}
		toNC := base
		c := "posthog"
		toNC.Cluster = &c
		to := &Schema{NamedCollections: []NamedCollectionSpec{toNC}}
		cs := Diff(from, to)
		require.Len(t, cs.NamedCollections, 1)
		assert.True(t, cs.NamedCollections[0].Recreate)
		require.NotNil(t, cs.NamedCollections[0].Add)
		assert.Equal(t, "posthog", *cs.NamedCollections[0].Add.Cluster)
	})

	t.Run("identical produces no change", func(t *testing.T) {
		schema := &Schema{NamedCollections: []NamedCollectionSpec{base}}
		assert.True(t, Diff(schema, schema).IsEmpty())
	})
}

func TestDiff_ExternalNCs_Ignored(t *testing.T) {
	from := &Schema{NamedCollections: []NamedCollectionSpec{
		{Name: "x", External: true, Params: []NamedCollectionParam{{Key: "a", Value: "1"}}},
	}}
	to := &Schema{NamedCollections: []NamedCollectionSpec{
		{Name: "x", External: true, Params: []NamedCollectionParam{{Key: "a", Value: "2"}}},
	}}
	cs := Diff(from, to)
	assert.True(t, cs.IsEmpty(), "external collections should be diff-skipped regardless of attribute changes")
}

func TestDiff_ExternalToManaged_Errors(t *testing.T) {
	// Promotion from external to managed (or vice versa) is an operator
	// migration, not a diff. The Diff API returns the change set; emit a
	// diagnostic ChangeSet that has the change but marks it as a domain
	// error visible to downstream consumers.
	from := &Schema{NamedCollections: []NamedCollectionSpec{
		{Name: "x", External: true},
	}}
	to := &Schema{NamedCollections: []NamedCollectionSpec{
		{Name: "x", Params: []NamedCollectionParam{{Key: "a", Value: "1"}}},
	}}
	cs := Diff(from, to)
	require.Len(t, cs.NamedCollections, 1)
	c := cs.NamedCollections[0]
	assert.NotEmpty(t, c.Error, "external↔managed migration should carry an Error message")
	assert.Contains(t, c.Error, "external")
}
```

- [ ] **Step 2: Run failing tests**

Run: `go test ./internal/loader/hcl -run 'TestDiff_NamedCollections|TestDiff_ExternalNCs|TestDiff_ExternalToManaged' -v`
Expected: FAIL — `ChangeSet.NamedCollections`, `NamedCollectionChange`, etc. don't exist.

- [ ] **Step 3: Add `ChangeSet.NamedCollections` field in `diff.go`**

Modify `ChangeSet` to add the field:

```go
type ChangeSet struct {
	Databases        []DatabaseChange
	NamedCollections []NamedCollectionChange
}
```

Update `ChangeSet.IsEmpty()`:

```go
func (cs ChangeSet) IsEmpty() bool {
	for _, dc := range cs.Databases {
		if !dc.IsEmpty() {
			return false
		}
	}
	for _, ncc := range cs.NamedCollections {
		if !ncc.IsEmpty() {
			return false
		}
	}
	return true
}
```

- [ ] **Step 4: Implement `named_collection_diff.go`**

Create `internal/loader/hcl/named_collection_diff.go`:

```go
package hcl

// NamedCollectionChange describes a planned change to a named collection.
type NamedCollectionChange struct {
	Name string

	// Add is set for fresh adds AND for the create-half of a recreate
	// (so the create has the full target spec).
	Add *NamedCollectionSpec

	// Drop is true for pure drops AND for the drop-half of a recreate.
	Drop bool

	// Recreate is true when ON CLUSTER changed; sqlgen will emit
	// DROP then CREATE adjacently.
	Recreate bool

	// Surgical (non-recreate) changes:
	SetParams     []NamedCollectionParam
	DeleteParams  []string
	CommentChange *StringChange

	// Error is non-empty when the diff itself describes an unsupported
	// transition (e.g. external↔managed). sqlgen emits no DDL; the CLI
	// surfaces the error message.
	Error string
}

func (c NamedCollectionChange) IsEmpty() bool {
	return c.Add == nil && !c.Drop && !c.Recreate &&
		len(c.SetParams) == 0 && len(c.DeleteParams) == 0 &&
		c.CommentChange == nil && c.Error == ""
}

func (c NamedCollectionChange) IsUnsafe() bool { return false }

// diffNamedCollections returns the per-collection changes between two
// schemas. External-on-both-sides changes are omitted. External-on-one-side
// transitions surface as Error entries.
func diffNamedCollections(from, to []NamedCollectionSpec) []NamedCollectionChange {
	fromIdx := map[string]*NamedCollectionSpec{}
	for i := range from {
		fromIdx[from[i].Name] = &from[i]
	}
	toIdx := map[string]*NamedCollectionSpec{}
	for i := range to {
		toIdx[to[i].Name] = &to[i]
	}

	names := map[string]bool{}
	for n := range fromIdx {
		names[n] = true
	}
	for n := range toIdx {
		names[n] = true
	}

	var out []NamedCollectionChange
	for _, n := range sortedSetKeys(names) {
		f, ft := fromIdx[n], toIdx[n]
		switch {
		case f == nil && ft != nil:
			if ft.External {
				continue // external add — hclexp does nothing
			}
			toCopy := *ft
			out = append(out, NamedCollectionChange{Name: n, Add: &toCopy})
		case f != nil && ft == nil:
			if f.External {
				continue // external drop — hclexp does nothing
			}
			out = append(out, NamedCollectionChange{Name: n, Drop: true})
		default:
			// both present
			if f.External && ft.External {
				continue // both external — skip entirely
			}
			if f.External != ft.External {
				out = append(out, NamedCollectionChange{
					Name:  n,
					Error: "external↔managed migration not supported; promote/demote manually",
				})
				continue
			}
			out = append(out, diffOneNamedCollection(n, f, ft))
		}
	}
	// Filter empty entries (changes with nothing to do).
	final := out[:0]
	for _, c := range out {
		if !c.IsEmpty() {
			final = append(final, c)
		}
	}
	return final
}

func diffOneNamedCollection(name string, f, ft *NamedCollectionSpec) NamedCollectionChange {
	change := NamedCollectionChange{Name: name}

	// ON CLUSTER mismatch → recreate.
	if !ptrStringEqual(f.Cluster, ft.Cluster) {
		toCopy := *ft
		change.Recreate = true
		change.Drop = true
		change.Add = &toCopy
		return change
	}

	// Param SET / DELETE.
	fromParams := map[string]NamedCollectionParam{}
	for _, p := range f.Params {
		fromParams[p.Key] = p
	}
	toParams := map[string]NamedCollectionParam{}
	for _, p := range ft.Params {
		toParams[p.Key] = p
	}
	for _, p := range ft.Params {
		fp, present := fromParams[p.Key]
		if !present || fp.Value != p.Value || !ptrBoolEqual(fp.Overridable, p.Overridable) {
			change.SetParams = append(change.SetParams, p)
		}
	}
	for _, p := range f.Params {
		if _, present := toParams[p.Key]; !present {
			change.DeleteParams = append(change.DeleteParams, p.Key)
		}
	}

	if !ptrStringEqual(f.Comment, ft.Comment) {
		change.CommentChange = &StringChange{Old: f.Comment, New: ft.Comment}
	}

	return change
}

func sortedSetKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func ptrStringEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func ptrBoolEqual(a, b *bool) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
```

Make sure to add `"sort"` to imports if not already present. (`sortedKeys[V any]` exists for generic maps in `diff.go`; `sortedSetKeys` is a separate, simpler helper for `map[string]bool`.)

- [ ] **Step 5: Wire into `Diff`**

Modify the existing `Diff` function:

```go
func Diff(from, to *Schema) ChangeSet {
	if from == nil {
		from = &Schema{}
	}
	if to == nil {
		to = &Schema{}
	}
	fromIdx := indexDatabases(from.Databases)
	toIdx := indexDatabases(to.Databases)
	// ... existing database-diff logic, unchanged
	cs := ChangeSet{Databases: ...}
	cs.NamedCollections = diffNamedCollections(from.NamedCollections, to.NamedCollections)
	return cs
}
```

- [ ] **Step 6: Run tests**

Run: `go test ./internal/loader/hcl -run 'TestDiff_' -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/loader/hcl/named_collection_diff.go internal/loader/hcl/diff.go internal/loader/hcl/diff_test.go
git commit -m "feat: diff named collections with surgical and recreate paths

NamedCollectionChange captures the four cases: add, drop, surgical
SET/DELETE, and full recreate (when ON CLUSTER changed). External
collections are diff-skipped on both sides; external↔managed
transitions surface as Error entries.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 9: SQL generation for named collections

**Files:**
- Create: `internal/loader/hcl/named_collection_sqlgen.go`
- Create: `internal/loader/hcl/named_collection_sqlgen_test.go`
- Modify: `internal/loader/hcl/sqlgen.go` (GenerateSQL ordering)

- [ ] **Step 1: Write failing tests**

Create `internal/loader/hcl/named_collection_sqlgen_test.go`:

```go
package hcl

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateNamedCollectionSQL(t *testing.T) {
	cluster := "posthog"
	overrideTrue := true
	overrideFalse := false
	nc := NamedCollectionSpec{
		Name:    "my_kafka",
		Cluster: &cluster,
		Params: []NamedCollectionParam{
			{Key: "kafka_broker_list", Value: "k:9092"},
			{Key: "kafka_topic_list", Value: "events"},
			{Key: "kafka_group_name", Value: "g1", Overridable: &overrideTrue},
			{Key: "kafka_format", Value: "JSONEachRow", Overridable: &overrideFalse},
		},
	}
	got := createNamedCollectionSQL(nc)
	want := "CREATE NAMED COLLECTION my_kafka ON CLUSTER posthog AS " +
		"kafka_broker_list = 'k:9092', " +
		"kafka_topic_list = 'events', " +
		"kafka_group_name = 'g1' OVERRIDABLE, " +
		"kafka_format = 'JSONEachRow' NOT OVERRIDABLE"
	assert.Equal(t, want, got)
}

func TestDropNamedCollectionSQL(t *testing.T) {
	assert.Equal(t, "DROP NAMED COLLECTION my_kafka", dropNamedCollectionSQL("my_kafka"))
}

func TestAlterNamedCollectionSetSQL(t *testing.T) {
	params := []NamedCollectionParam{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
	}
	want := "ALTER NAMED COLLECTION my_kafka SET a = '1', b = '2'"
	assert.Equal(t, want, alterNamedCollectionSetSQL("my_kafka", params))
}

func TestAlterNamedCollectionDeleteSQL(t *testing.T) {
	want := "ALTER NAMED COLLECTION my_kafka DELETE a, b"
	assert.Equal(t, want, alterNamedCollectionDeleteSQL("my_kafka", []string{"a", "b"}))
}

func TestGenerateSQL_NamedCollectionRecreateOrdering(t *testing.T) {
	// A recreate change emits DROP then CREATE adjacent and at the FRONT
	// of out.Statements, before any other create.
	newCluster := "posthog"
	nc := NamedCollectionSpec{Name: "nc", Cluster: &newCluster, Params: []NamedCollectionParam{
		{Key: "a", Value: "1"},
	}}
	cs := ChangeSet{NamedCollections: []NamedCollectionChange{{
		Name:     "nc",
		Recreate: true,
		Drop:     true,
		Add:      &nc,
	}}}
	out := GenerateSQL(cs)
	require.Len(t, out.Statements, 2)
	assert.True(t, strings.HasPrefix(out.Statements[0], "DROP NAMED COLLECTION nc"))
	assert.True(t, strings.HasPrefix(out.Statements[1], "CREATE NAMED COLLECTION nc"))
}

func TestGenerateSQL_NamedCollectionError_NoStatements(t *testing.T) {
	cs := ChangeSet{NamedCollections: []NamedCollectionChange{{
		Name:  "nc",
		Error: "external↔managed migration not supported",
	}}}
	out := GenerateSQL(cs)
	assert.Empty(t, out.Statements)
	require.Len(t, out.Unsafe, 1)
	assert.Contains(t, out.Unsafe[0].Reason, "external")
}
```

- [ ] **Step 2: Run failing tests**

Run: `go test ./internal/loader/hcl -run 'TestCreateNamedCollectionSQL|TestDropNamedCollectionSQL|TestAlterNamedCollection|TestGenerateSQL_NamedCollection' -v`
Expected: FAIL.

- [ ] **Step 3: Implement `named_collection_sqlgen.go`**

Create `internal/loader/hcl/named_collection_sqlgen.go`:

```go
package hcl

import (
	"fmt"
	"strings"
)

func createNamedCollectionSQL(nc NamedCollectionSpec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE NAMED COLLECTION %s", nc.Name)
	if nc.Cluster != nil {
		fmt.Fprintf(&b, " ON CLUSTER %s", *nc.Cluster)
	}
	b.WriteString(" AS ")
	parts := make([]string, len(nc.Params))
	for i, p := range nc.Params {
		parts[i] = formatNCParam(p)
	}
	b.WriteString(strings.Join(parts, ", "))
	return b.String()
}

func dropNamedCollectionSQL(name string) string {
	return fmt.Sprintf("DROP NAMED COLLECTION %s", name)
}

func alterNamedCollectionSetSQL(name string, params []NamedCollectionParam) string {
	if len(params) == 0 {
		return ""
	}
	parts := make([]string, len(params))
	for i, p := range params {
		parts[i] = formatNCParam(p)
	}
	return fmt.Sprintf("ALTER NAMED COLLECTION %s SET %s", name, strings.Join(parts, ", "))
}

func alterNamedCollectionDeleteSQL(name string, keys []string) string {
	if len(keys) == 0 {
		return ""
	}
	return fmt.Sprintf("ALTER NAMED COLLECTION %s DELETE %s", name, strings.Join(keys, ", "))
}

func formatNCParam(p NamedCollectionParam) string {
	val := "'" + strings.ReplaceAll(p.Value, "'", "''") + "'"
	suffix := ""
	if p.Overridable != nil {
		if *p.Overridable {
			suffix = " OVERRIDABLE"
		} else {
			suffix = " NOT OVERRIDABLE"
		}
	}
	return fmt.Sprintf("%s = %s%s", p.Key, val, suffix)
}
```

- [ ] **Step 4: Extend `GenerateSQL` in `sqlgen.go`**

The new statement ordering must put NC recreate-pairs at the FRONT, then NC adds, then everything else, then NC alters (after DDL changes that may depend on them), then NC drops at the very end.

Modify `GenerateSQL` to add NC-related blocks. Insert at the start of the function (before existing CREATE TABLE loop):

```go
func GenerateSQL(cs ChangeSet) GeneratedSQL {
	var out GeneratedSQL

	// 1. NC recreate pairs (DROP+CREATE adjacent, before any other create).
	for _, ncc := range cs.NamedCollections {
		if ncc.Recreate && ncc.Add != nil {
			out.Statements = append(out.Statements, dropNamedCollectionSQL(ncc.Name))
			out.Statements = append(out.Statements, createNamedCollectionSQL(*ncc.Add))
		}
		if ncc.Error != "" {
			out.Unsafe = append(out.Unsafe, UnsafeChange{
				Database: "",
				Table:    ncc.Name,
				Reason:   "named collection: " + ncc.Error,
			})
		}
	}

	// 2. Fresh NC adds (not recreate).
	for _, ncc := range cs.NamedCollections {
		if ncc.Add != nil && !ncc.Recreate {
			out.Statements = append(out.Statements, createNamedCollectionSQL(*ncc.Add))
		}
	}

	// 3. existing CREATE TABLE loop
	// 4. CREATE MV loop
	// 5. CREATE OR REPLACE DICTIONARY loop
	// 6. ALTER TABLE loop
	// 7. ALTER MV loop
	// (existing code, unchanged)

	// 8. NC surgical ALTER (SET then DELETE, only for non-recreate diffs).
	for _, ncc := range cs.NamedCollections {
		if ncc.Recreate || ncc.Add != nil || ncc.Drop {
			continue
		}
		if stmt := alterNamedCollectionSetSQL(ncc.Name, ncc.SetParams); stmt != "" {
			out.Statements = append(out.Statements, stmt)
		}
		if stmt := alterNamedCollectionDeleteSQL(ncc.Name, ncc.DeleteParams); stmt != "" {
			out.Statements = append(out.Statements, stmt)
		}
	}

	// 9. existing DROP MV loop
	// 10. DROP DICTIONARY loop
	// 11. DROP TABLE loop (existing)

	// 12. NC pure drops (not recreate).
	for _, ncc := range cs.NamedCollections {
		if ncc.Drop && !ncc.Recreate {
			out.Statements = append(out.Statements, dropNamedCollectionSQL(ncc.Name))
		}
	}

	return out
}
```

(In the actual file, blocks 3-11 already exist — the new blocks 1, 2, 8, and 12 are the additions. Place them precisely at the boundary positions described.)

- [ ] **Step 5: Run tests**

Run: `go test ./internal/loader/hcl -run 'TestCreateNamedCollection|TestDropNamedCollection|TestAlterNamedCollection|TestGenerateSQL' -v && go test ./internal/loader/hcl`
Expected: PASS — new tests pass, existing sqlgen tests still pass.

- [ ] **Step 6: Commit**

```bash
git add internal/loader/hcl/named_collection_sqlgen.go internal/loader/hcl/named_collection_sqlgen_test.go internal/loader/hcl/sqlgen.go
git commit -m "feat: generate DDL for named collections

CREATE NAMED COLLECTION emits at the front of the statement set;
recreates are DROP+CREATE adjacent (also at the front so dependent
tables see only a brief gap). Surgical SET/DELETE between CREATE and
DROP phases. Pure drops at the end (after dependent objects are gone).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: CLI render named collection changes

**Files:**
- Modify: `cmd/hclexp/hclexp.go` (renderChangeSet)
- Modify: `cmd/hclexp/hclexp_test.go`

- [ ] **Step 1: Write the failing test**

Append to `cmd/hclexp/hclexp_test.go`:

```go
func TestRenderChangeSet_NamedCollections(t *testing.T) {
	cs := hclload.ChangeSet{
		NamedCollections: []hclload.NamedCollectionChange{
			{Name: "new_nc", Add: &hclload.NamedCollectionSpec{Name: "new_nc"}},
			{Name: "old_nc", Drop: true},
			{Name: "prod_nc", SetParams: []hclload.NamedCollectionParam{
				{Key: "kafka_topic_list", Value: "new_topic"},
				{Key: "kafka_new_setting", Value: "added"},
			}, DeleteParams: []string{"kafka_unused"}},
		},
	}

	var buf bytes.Buffer
	renderChangeSet(&buf, cs)

	want := `named_collections
  + named_collection new_nc
  - named_collection old_nc
  ~ named_collection prod_nc
      ~ param kafka_topic_list (set)
      + param kafka_new_setting (set)
      - param kafka_unused
`
	require.Equal(t, want, buf.String())
}
```

- [ ] **Step 2: Run failing test**

Run: `go test ./cmd/hclexp -run TestRenderChangeSet_NamedCollections -v`
Expected: FAIL.

- [ ] **Step 3: Extend `renderChangeSet` in `cmd/hclexp/hclexp.go`**

After the database loop (at the end of `renderChangeSet`), add:

```go
	if len(cs.NamedCollections) > 0 {
		fmt.Fprintln(w, "named_collections")
		for _, ncc := range cs.NamedCollections {
			switch {
			case ncc.Error != "":
				fmt.Fprintf(w, "  ! named_collection %s: %s\n", ncc.Name, ncc.Error)
			case ncc.Recreate:
				fmt.Fprintf(w, "  ~ named_collection %s (recreate: ON CLUSTER changed)\n", ncc.Name)
			case ncc.Add != nil:
				fmt.Fprintf(w, "  + named_collection %s\n", ncc.Name)
			case ncc.Drop:
				fmt.Fprintf(w, "  - named_collection %s\n", ncc.Name)
			default:
				fmt.Fprintf(w, "  ~ named_collection %s\n", ncc.Name)
				for _, p := range ncc.SetParams {
					fmt.Fprintf(w, "      ~ param %s (set)\n", p.Key)
				}
				for _, k := range ncc.DeleteParams {
					fmt.Fprintf(w, "      - param %s\n", k)
				}
				if ncc.CommentChange != nil {
					fmt.Fprintln(w, "      ~ comment changed")
				}
			}
		}
	}
```

NOTE: The test expects `~ param X (set)` for the FIRST param (an update to an existing key) and `+ param X (set)` for the SECOND (a brand-new key). The `SetParams` field doesn't distinguish "added" from "modified" — both are in the same slice. To render the distinction, the rendering helper would need to know which keys already existed. For this CLI rendering, simplification: emit all `SetParams` as `~ param X (set)` (no `+` prefix). Update the test's `want` to match. Run the test, see it pass.

Actually, looking again, the test as written expects two formats:
```
~ param kafka_topic_list (set)
+ param kafka_new_setting (set)
```

To produce these, the diff layer needs to track "added vs modified" separately. Since `diffOneNamedCollection` already iterates over `to.Params` and checks `present` in the from-side, we can split `SetParams` into two fields: `SetParams` (modifications to existing keys) and `AddParams` (new keys). But this adds complexity to a representation that sqlgen treats uniformly (both go into ALTER ... SET).

**Decision**: keep `SetParams` as a single slice in the diff; in the CLI, emit them all as `~ param X (set)` (no add/modify distinction). Update the test's `want` to:

```go
want := `named_collections
  + named_collection new_nc
  - named_collection old_nc
  ~ named_collection prod_nc
      ~ param kafka_topic_list (set)
      ~ param kafka_new_setting (set)
      - param kafka_unused
`
```

- [ ] **Step 4: Run test**

Run: `go test ./cmd/hclexp -run TestRenderChangeSet_NamedCollections -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/hclexp/hclexp.go cmd/hclexp/hclexp_test.go
git commit -m "feat: render named collection changes in hclexp diff

renderChangeSet emits a top-level named_collections section after the
database sections, with +/-/~/! markers for add/drop/surgical/error.
Surgical alter entries list the SET / DELETE param diffs.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 11: Introspect dispatch — kafka 4-case unit tests

**Files:**
- Modify: `internal/loader/hcl/introspect_test.go`

- [ ] **Step 1: Write failing tests**

Append:

```go
func TestParseKafkaEngine_Cases(t *testing.T) {
	tests := []struct {
		name       string
		params     []string
		settings   map[string]string
		expectErr  bool
		errSubstr  string
		check      func(t *testing.T, k EngineKafka)
	}{
		{
			name:     "inline form: all kafka_* in settings",
			params:   nil,
			settings: map[string]string{
				"kafka_broker_list":         "k:9092",
				"kafka_topic_list":          "events",
				"kafka_group_name":          "g1",
				"kafka_format":              "JSONEachRow",
				"kafka_num_consumers":       "4",
				"kafka_commit_on_select":    "0",
				"kafka_handle_error_mode":   "stream",
				"kafka_some_future_setting": "passthrough",
			},
			check: func(t *testing.T, k EngineKafka) {
				assert.Nil(t, k.Collection)
				require.NotNil(t, k.BrokerList)
				assert.Equal(t, "k:9092", *k.BrokerList)
				require.NotNil(t, k.NumConsumers)
				assert.Equal(t, int64(4), *k.NumConsumers)
				require.NotNil(t, k.CommitOnSelect)
				assert.False(t, *k.CommitOnSelect)
				require.NotNil(t, k.HandleErrorMode)
				assert.Equal(t, "stream", *k.HandleErrorMode)
				assert.Equal(t, "passthrough", k.Extra["kafka_some_future_setting"])
			},
		},
		{
			name:   "named collection form: Kafka(my_nc)",
			params: []string{"my_nc"},
			check: func(t *testing.T, k EngineKafka) {
				require.NotNil(t, k.Collection)
				assert.Equal(t, "my_nc", *k.Collection)
				assert.Nil(t, k.BrokerList)
			},
		},
		{
			name:   "legacy positional form",
			params: []string{"k:9092", "events", "g1", "JSONEachRow"},
			check: func(t *testing.T, k EngineKafka) {
				assert.Nil(t, k.Collection)
				require.NotNil(t, k.BrokerList)
				assert.Equal(t, "k:9092", *k.BrokerList)
				require.NotNil(t, k.TopicList)
				assert.Equal(t, "events", *k.TopicList)
				require.NotNil(t, k.GroupName)
				assert.Equal(t, "g1", *k.GroupName)
				require.NotNil(t, k.Format)
				assert.Equal(t, "JSONEachRow", *k.Format)
			},
		},
		{
			name:      "mixed form: Kafka(my_nc) + kafka_* settings errors",
			params:    []string{"my_nc"},
			settings:  map[string]string{"kafka_num_consumers": "4"},
			expectErr: true,
			errSubstr: "cannot be combined",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k, err := buildKafkaEngine(tc.params, tc.settings)
			if tc.expectErr {
				require.Error(t, err)
				if tc.errSubstr != "" {
					assert.Contains(t, err.Error(), tc.errSubstr)
				}
				return
			}
			require.NoError(t, err)
			tc.check(t, k)
		})
	}
}
```

- [ ] **Step 2: Run test**

Run: `go test ./internal/loader/hcl -run TestParseKafkaEngine_Cases -v`
Expected: PASS — the function was implemented in Task 2.

- [ ] **Step 3: Commit**

```bash
git add internal/loader/hcl/introspect_test.go
git commit -m "test: parseKafkaEngine 4-case coverage

Locks in the four introspection paths: canonical inline, named
collection reference, legacy positional, and the mixed-form error.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 12: Live tests

**Files:**
- Create: `internal/loader/hcl/named_collection_live_test.go`
- Create: `internal/loader/hcl/kafka_namedcollection_live_test.go`

- [ ] **Step 1: Inspect existing live-test scaffolding**

Read `internal/loader/hcl/introspect_live_test.go` to confirm: the `clickhouseLive` flag, `testhelpers.RequireClickHouse(t)` connection helper, and `testhelpers.CreateTestDatabase(t, conn)` isolated-DB helper. Use the exact names you find.

- [ ] **Step 2: Create `named_collection_live_test.go`**

```go
package hcl

import (
	"context"
	"fmt"
	"testing"

	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// uniqueNCName returns a per-test NC name (named collections are
// cluster-scoped so they can collide across parallel test runs).
func uniqueNCName(t *testing.T, prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func TestCHLive_NamedCollection_ApplyRoundTrip(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	ctx := context.Background()
	name := uniqueNCName(t, "hclexp_apply_rt")
	t.Cleanup(func() { _ = conn.Exec(ctx, "DROP NAMED COLLECTION IF EXISTS "+name) })

	overrideTrue := true
	overrideFalse := false
	want := NamedCollectionSpec{
		Name: name,
		Params: []NamedCollectionParam{
			{Key: "kafka_broker_list", Value: "k:9092"},
			{Key: "kafka_topic_list", Value: "events"},
			{Key: "kafka_group_name", Value: "g1", Overridable: &overrideTrue},
			{Key: "kafka_format", Value: "JSONEachRow", Overridable: &overrideFalse},
			{Key: "kafka_sasl_password", Value: "secret"},
		},
	}

	require.NoError(t, conn.Exec(ctx, createNamedCollectionSQL(want)))

	all, err := IntrospectNamedCollections(ctx, conn)
	require.NoError(t, err)
	var got *NamedCollectionSpec
	for i := range all {
		if all[i].Name == name {
			got = &all[i]
			break
		}
	}
	require.NotNil(t, got, "introspected NCs missing %q", name)
	assert.Equal(t, want.Params, got.Params)
}

func TestCHLive_NamedCollection_AlterSetDelete(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	ctx := context.Background()
	name := uniqueNCName(t, "hclexp_alter")
	t.Cleanup(func() { _ = conn.Exec(ctx, "DROP NAMED COLLECTION IF EXISTS "+name) })

	initial := NamedCollectionSpec{Name: name, Params: []NamedCollectionParam{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
	}}
	require.NoError(t, conn.Exec(ctx, createNamedCollectionSQL(initial)))

	// SET: change a, add c. DELETE: remove b.
	setParams := []NamedCollectionParam{
		{Key: "a", Value: "1_new"},
		{Key: "c", Value: "3"},
	}
	require.NoError(t, conn.Exec(ctx, alterNamedCollectionSetSQL(name, setParams)))
	require.NoError(t, conn.Exec(ctx, alterNamedCollectionDeleteSQL(name, []string{"b"})))

	all, err := IntrospectNamedCollections(ctx, conn)
	require.NoError(t, err)
	var got *NamedCollectionSpec
	for i := range all {
		if all[i].Name == name {
			got = &all[i]
			break
		}
	}
	require.NotNil(t, got)
	gotByKey := map[string]string{}
	for _, p := range got.Params {
		gotByKey[p.Key] = p.Value
	}
	assert.Equal(t, "1_new", gotByKey["a"])
	assert.Equal(t, "3", gotByKey["c"])
	_, present := gotByKey["b"]
	assert.False(t, present, "b should have been deleted")
}
```

Add `"time"` to imports for `uniqueNCName`. The `clickhouseLive` flag and `testhelpers` import are inherited from sibling live tests.

- [ ] **Step 3: Create `kafka_namedcollection_live_test.go` (the e2e headliner)**

```go
package hcl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCHLive_Kafka_WithNamedCollection_E2E is the headline e2e test.
// Build a NamedCollectionSpec + TableSpec referencing it → emit DDL via
// the actual hclexp sqlgen path → apply against live ClickHouse →
// introspect both objects → assert round-trip preserves the typed
// model (NC params + EngineKafka{Collection}).
func TestCHLive_Kafka_WithNamedCollection_E2E(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()
	ncName := fmt.Sprintf("kafka_e2e_%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = conn.Exec(ctx, "DROP NAMED COLLECTION IF EXISTS "+ncName) })

	// 1. Build the NC spec.
	wantNC := NamedCollectionSpec{
		Name: ncName,
		Params: []NamedCollectionParam{
			{Key: "kafka_broker_list", Value: "kafka:9092"},
			{Key: "kafka_topic_list", Value: "test_events"},
			{Key: "kafka_group_name", Value: "test_group"},
			{Key: "kafka_format", Value: "JSONEachRow"},
		},
	}

	// 2. Build the table spec referencing the NC.
	wantTbl := TableSpec{
		Name: "kafka_consumer",
		Columns: []ColumnSpec{
			{Name: "id", Type: "UInt64"},
			{Name: "payload", Type: "String"},
		},
		Engine: &EngineSpec{Kind: "kafka", Decoded: EngineKafka{Collection: &ncName}},
	}

	// 3. Generate + apply DDL through the actual sqlgen path.
	cs := ChangeSet{
		NamedCollections: []NamedCollectionChange{{Name: ncName, Add: &wantNC}},
		Databases: []DatabaseChange{{Database: dbName, AddTables: []TableSpec{wantTbl}}},
	}
	gen := GenerateSQL(cs)
	require.NotEmpty(t, gen.Statements)
	for _, stmt := range gen.Statements {
		require.NoError(t, conn.Exec(ctx, stmt), "exec failed: %s", stmt)
	}

	// 4. Introspect both sides.
	ncs, err := IntrospectNamedCollections(ctx, conn)
	require.NoError(t, err)
	var gotNC *NamedCollectionSpec
	for i := range ncs {
		if ncs[i].Name == ncName {
			gotNC = &ncs[i]
			break
		}
	}
	require.NotNil(t, gotNC, "introspected NCs missing %q", ncName)
	assert.Equal(t, wantNC.Params, gotNC.Params)

	dbIntrospected, err := Introspect(ctx, conn, dbName)
	require.NoError(t, err)
	require.Len(t, dbIntrospected.Tables, 1)
	got := dbIntrospected.Tables[0]
	require.NotNil(t, got.Engine)
	gotKafka, ok := got.Engine.Decoded.(EngineKafka)
	require.True(t, ok, "expected EngineKafka, got %T", got.Engine.Decoded)
	require.NotNil(t, gotKafka.Collection)
	assert.Equal(t, ncName, *gotKafka.Collection)
	assert.Nil(t, gotKafka.BrokerList, "Kafka(<nc>) form should NOT resolve to inline settings on introspect")
}

// TestCHLive_Kafka_AllSettingsForm exercises the canonical Kafka() +
// SETTINGS form with mixed typed settings (numeric, bool, string,
// future-key in Extra) to confirm introspection captures every type.
func TestCHLive_Kafka_AllSettingsForm(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	brokerList := "kafka:9092"
	topicList := "test_events"
	groupName := "test_group"
	format := "JSONEachRow"
	numConsumers := int64(4)
	maxBlockSize := int64(1048576)
	skipBroken := int64(100)
	commitOnSelect := false
	handleErrorMode := "stream"

	wantTbl := TableSpec{
		Name: "kafka_inline",
		Columns: []ColumnSpec{
			{Name: "id", Type: "UInt64"},
			{Name: "payload", Type: "String"},
		},
		Engine: &EngineSpec{Kind: "kafka", Decoded: EngineKafka{
			BrokerList:         &brokerList,
			TopicList:          &topicList,
			GroupName:          &groupName,
			Format:             &format,
			NumConsumers:       &numConsumers,
			MaxBlockSize:       &maxBlockSize,
			SkipBrokenMessages: &skipBroken,
			CommitOnSelect:     &commitOnSelect,
			HandleErrorMode:    &handleErrorMode,
		}},
	}

	cs := ChangeSet{Databases: []DatabaseChange{{
		Database:  dbName,
		AddTables: []TableSpec{wantTbl},
	}}}
	for _, stmt := range GenerateSQL(cs).Statements {
		require.NoError(t, conn.Exec(ctx, stmt), "exec failed: %s", stmt)
	}

	dbIntrospected, err := Introspect(ctx, conn, dbName)
	require.NoError(t, err)
	require.Len(t, dbIntrospected.Tables, 1)
	got := dbIntrospected.Tables[0]
	gotKafka, ok := got.Engine.Decoded.(EngineKafka)
	require.True(t, ok)
	assert.Nil(t, gotKafka.Collection)
	require.NotNil(t, gotKafka.BrokerList)
	assert.Equal(t, "kafka:9092", *gotKafka.BrokerList)
	require.NotNil(t, gotKafka.NumConsumers)
	assert.Equal(t, int64(4), *gotKafka.NumConsumers)
	require.NotNil(t, gotKafka.MaxBlockSize)
	assert.Equal(t, int64(1048576), *gotKafka.MaxBlockSize)
	require.NotNil(t, gotKafka.SkipBrokenMessages)
	assert.Equal(t, int64(100), *gotKafka.SkipBrokenMessages)
	require.NotNil(t, gotKafka.CommitOnSelect)
	assert.False(t, *gotKafka.CommitOnSelect)
	require.NotNil(t, gotKafka.HandleErrorMode)
	assert.Equal(t, "stream", *gotKafka.HandleErrorMode)
}
```

- [ ] **Step 4: Compile + skip-test (no -clickhouse)**

Run: `go test ./internal/loader/hcl -run 'TestCHLive_NamedCollection|TestCHLive_Kafka'`
Expected: SKIP (each test t.Skips without `-clickhouse`); the build is clean.

- [ ] **Step 5: Run with live ClickHouse**

(Requires `docker compose up -d`.)
Run: `go test ./internal/loader/hcl -run 'TestCHLive_NamedCollection|TestCHLive_Kafka' -clickhouse -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/loader/hcl/named_collection_live_test.go internal/loader/hcl/kafka_namedcollection_live_test.go
git commit -m "test: live round-trip for named collections and Kafka+NC e2e

ApplyRoundTrip: create NC via sqlgen → exec → introspect → assert.
AlterSetDelete: SET (modify + add) + DELETE applied via ALTER.
Kafka E2E: full sqlgen path emits CREATE NC then CREATE TABLE
ENGINE = Kafka(nc); introspection confirms the table's engine is the
NC reference (not the resolved settings).
AllSettingsForm: canonical Kafka() + SETTINGS round-trips typed
numeric and bool settings.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 13: README — Named collections section

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Find insertion point**

The README has a `### Dictionaries` subsection followed by `## Layering & inheritance`. Insert the new `### Named collections` between them.

- [ ] **Step 2: Insert section**

After the `### Dictionaries` subsection ends (the last bullet of its out-of-scope list) and before `## Layering & inheritance`, insert:

````markdown
### Named collections

A `named_collection` block declares a ClickHouse named collection —
cluster-scoped key/value bags that other objects (most notably Kafka
tables) can reference by name. Unlike the other HCL blocks, named
collections sit **at the top level** of the document, next to `database`
blocks rather than inside one — they're cluster-scoped, not
database-scoped.

```hcl
named_collection "my_kafka" {
  cluster = "posthog"

  param "kafka_broker_list" { value = "kafka:9092" }
  param "kafka_topic_list"  { value = "events" }
  param "kafka_group_name"  { value = "ch_events" }
  param "kafka_format"      { value = "JSONEachRow" }
}

database "posthog" {
  table "events_kafka" {
    column "team_id" { type = "Int64" }
    column "payload" { type = "String" }
    engine "kafka" { collection = "my_kafka" }
  }
}
```

| Block / attribute | Required | Meaning |
|-------------------|----------|---------|
| `external`        | no       | `true` marks an NC managed outside hclexp (e.g. server XML config); hclexp emits no DDL for it but lets Kafka references resolve. |
| `cluster`         | no       | `ON CLUSTER` target. Changing it forces a DROP+CREATE recreate. |
| `comment`         | no       | NC comment. |
| `param`           | yes (unless `external = true`) | one per key, with required `value` and optional `overridable` boolean. |

**Diff behavior.** ClickHouse exposes both `CREATE` and `ALTER` paths. `hclexp diff` uses `ALTER NAMED COLLECTION ... SET / DELETE` for surgical param changes and a `DROP+CREATE` pair (emitted adjacently) when `cluster` changes. External↔managed transitions are flagged as unsupported migrations.

**Production secret pattern.** The natural way to keep secret NC values out of VCS is the layered HCL pattern:

```bash
hclexp -layer schema/base,schema/prod-secrets ...
```

The base layer commits the NC declaration with placeholder values; the
override layer (gitignored or vault-sourced) declares the same NC with
`override = true` and the real values. Layer merging applies the override.

**Externally-managed NCs.** For collections defined in the ClickHouse
server's XML config (rather than via DDL), declare them in HCL with
`external = true`:

```hcl
named_collection "kafka_main" {
  external = true
  comment  = "managed in /etc/clickhouse-server/config.d/kafka.xml"
}
```

`hclexp` emits no DDL for external collections, but their declaration
makes Kafka `collection = "..."` references resolvable and validatable at
parse time.

**Security caveat.** Unlike dictionary `PASSWORD '[HIDDEN]'`,
named-collection values are **not** redacted in `system.named_collections`
— passwords round-trip plain through introspection. Use the
override-layer pattern (or external NCs) to keep secrets out of VCS.

### Kafka engine with named collections

`engine "kafka" { ... }` accepts either a `collection` reference or a
complete inline set of `kafka_*` settings — never both. The inline form
is the canonical preferred shape, modeling all ~25 documented `kafka_*`
settings as typed HCL attributes (numbers, booleans, strings) with an
`extra` escape map for settings ClickHouse adds in versions hclexp
doesn't yet model:

```hcl
engine "kafka" {
  // option A: named collection reference (no other field allowed)
  collection = "my_kafka"
}

engine "kafka" {
  // option B: full inline (canonical Kafka() + SETTINGS form)
  broker_list          = "kafka:9092"
  topic_list           = "events"
  group_name           = "ch_events"
  format               = "JSONEachRow"
  num_consumers        = 4
  max_block_size       = 1048576
  commit_on_select     = false
  skip_broken_messages = 100
  handle_error_mode    = "stream"
  sasl_mechanism       = "PLAIN"
  sasl_username        = "ch"
  sasl_password        = "[set via override layer]"
  extra = {
    kafka_some_future_setting = "passthrough"
  }
}
```

Field names drop the `kafka_` prefix (it's implicit inside `engine
"kafka"`). The `extra` map carries any setting that doesn't have a typed
field; its keys must include the `kafka_` prefix.
````

- [ ] **Step 3: Sanity check + commit**

Run: `go build ./...`
Expected: clean.

```bash
git add README.md
git commit -m "docs: named collection block + Kafka engine reshape

Documents the top-level named_collection block, the layered secrets
pattern, externally-managed (XML) collections, the security caveat,
and the new shape of engine \"kafka\" with either a collection
reference or a complete typed set of kafka_* settings.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Verification

After all tasks:

1. **Unit + CLI tests:** `go test ./internal/... ./cmd/... -v` — all pass.
2. **Build:** `go build ./...` — clean.
3. **Live tests:** `docker compose up -d && go test ./internal/loader/hcl -clickhouse -v` — the new `TestCHLive_NamedCollection_*` tests and `TestCHLive_Kafka_WithNamedCollection_E2E` / `TestCHLive_Kafka_AllSettingsForm` all pass, alongside existing live coverage.
4. **Manual round-trip:**
   - `go build -o hclexp ./cmd/hclexp`
   - Against a cluster that has a DDL-managed NC and a Kafka table referencing it: `./hclexp introspect -database <db>` emits a top-level `named_collection` block and a table with `engine "kafka" { collection = "..." }`.
   - Two HCL files differing in one NC param: `./hclexp diff -left a.hcl -right b.hcl` reports `~ named_collection X / ~ param Y (set)`; `-sql` emits `ALTER NAMED COLLECTION X SET Y = '...'`.
5. **Spec check:** every section of `docs/superpowers/specs/2026-05-16-named-collections-design.md` maps to a task above:
   - Schema return type → Task 3
   - NamedCollectionSpec / Param → Task 1
   - EngineKafka reshape → Task 2
   - External flag semantics → Tasks 1, 5, 8
   - Layer merging → Task 3
   - Resolver validation → Task 5
   - Introspect → Task 6
   - Dump → Task 7
   - Diff → Task 8
   - Sqlgen ordering (with recreate-pair-at-front) → Task 9
   - CLI render → Task 10
   - Kafka 4-case unit tests → Task 11
   - Live tests including E2E Kafka+NC → Task 12
   - README → Task 13
