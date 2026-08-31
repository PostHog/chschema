package hcl

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaEngineIntrospection_SupportedClickHouseForms(t *testing.T) {
	tests := []struct {
		name          string
		engine        string
		want          EngineKafka
		wantTableSets map[string]string
	}{
		{
			name: "named collection with constructor overrides",
			engine: `Kafka(warpstream_ingestion,
				kafka_topic_list = 'clickhouse_events_json',
				kafka_group_name = 'clickhouse_events_json_ws',
				kafka_format = 'JSONEachRow')
				SETTINGS kafka_num_consumers = 1, kafka_max_block_size = 100000, stream_flush_interval_ms = 7500`,
			want: EngineKafka{
				Collection:   ptr("warpstream_ingestion"),
				TopicList:    ptr("clickhouse_events_json"),
				GroupName:    ptr("clickhouse_events_json_ws"),
				Format:       ptr("JSONEachRow"),
				NumConsumers: ptr(int64(1)),
				MaxBlockSize: ptr(int64(100000)),
			},
			wantTableSets: map[string]string{"stream_flush_interval_ms": "7500"},
		},
		{
			name:   "named collection with settings overrides",
			engine: `Kafka(warpstream_ingestion) SETTINGS kafka_num_consumers = 4, kafka_thread_per_consumer = 1`,
			want: EngineKafka{
				Collection:        ptr("warpstream_ingestion"),
				NumConsumers:      ptr(int64(4)),
				ThreadPerConsumer: ptr(true),
			},
		},
		{
			name:   "three positional arguments with settings",
			engine: `Kafka('kafka:9092', 'events', 'group1') SETTINGS kafka_format = 'JSONEachRow', kafka_num_consumers = 4`,
			want: EngineKafka{
				BrokerList:   ptr("kafka:9092"),
				TopicList:    ptr("events"),
				GroupName:    ptr("group1"),
				Format:       ptr("JSONEachRow"),
				NumConsumers: ptr(int64(4)),
			},
		},
		{
			name: "deprecated long positional form",
			engine: `Kafka('kafka:9092', 'events', 'group1', 'JSONEachRow', '\n',
				'schema.proto:Event', 4, 65536, 10, 1, 'hclexp', 5000, 1000,
				7500, 250, 1, 'stream', 1, 2)`,
			want: EngineKafka{
				BrokerList:           ptr("kafka:9092"),
				TopicList:            ptr("events"),
				GroupName:            ptr("group1"),
				Format:               ptr("JSONEachRow"),
				Schema:               ptr("schema.proto:Event"),
				NumConsumers:         ptr(int64(4)),
				MaxBlockSize:         ptr(int64(65536)),
				SkipBrokenMessages:   ptr(int64(10)),
				CommitEveryBatch:     ptr(true),
				ClientID:             ptr("hclexp"),
				PollTimeoutMs:        ptr(int64(5000)),
				PollMaxBatchSize:     ptr(int64(1000)),
				FlushIntervalMs:      ptr(int64(7500)),
				ConsumerRescheduleMs: ptr(int64(250)),
				ThreadPerConsumer:    ptr(true),
				HandleErrorMode:      ptr("stream"),
				CommitOnSelect:       ptr(true),
				MaxRowsPerMessage:    ptr(int64(2)),
				Extra:                map[string]string{"kafka_row_delimiter": `\n`},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			table, err := buildTableFromCreateSQL(
				"CREATE TABLE db.t (id UInt64) ENGINE = " + tc.engine,
			)
			require.NoError(t, err)
			require.NotNil(t, table.Engine)
			assert.Equal(t, tc.want, table.Engine.Decoded)
			assert.Equal(t, tc.wantTableSets, table.Settings)
		})
	}
}

func TestKafkaEngineIntrospection_RejectsAmbiguousOrLossyForms(t *testing.T) {
	tests := []struct {
		name   string
		params []string
		sets   map[string]string
		want   string
	}{
		{
			name:   "named argument in positional list",
			params: []string{"kafka:9092", "kafka_topic_list = 'events'", "group1", "JSONEachRow"},
			want:   "named argument",
		},
		{
			name:   "named argument without collection",
			params: []string{"kafka_topic_list = 'events'"},
			want:   "collection",
		},
		{
			name:   "collection followed by bare argument",
			params: []string{"warpstream_ingestion", "events"},
			want:   "unexpected positional",
		},
		{
			name: "too many positional arguments",
			params: []string{
				"broker", "topic", "group", "format", "delimiter", "schema", "1", "2", "3", "1",
				"client", "4", "5", "6", "7", "1", "stream", "1", "2", "extra",
			},
			want: "at most",
		},
		{
			name: "invalid typed setting",
			sets: map[string]string{"kafka_num_consumers": "many"},
			want: "kafka_num_consumers",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildKafkaEngine(tc.params, tc.sets)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestKafkaEngineSettingsOverrideConstructor(t *testing.T) {
	const collection = "kafka_audit"
	tests := []struct {
		name           string
		constructorArg string
		settings       map[string]string
		want           EngineKafka
	}{
		{
			name:           "string",
			constructorArg: "kafka_topic_list = 'constructor'",
			settings:       map[string]string{"kafka_topic_list": "settings"},
			want:           EngineKafka{Collection: ptr(collection), TopicList: ptr("settings")},
		},
		{
			name:           "integer",
			constructorArg: "kafka_num_consumers = 2",
			settings:       map[string]string{"kafka_num_consumers": "4"},
			want:           EngineKafka{Collection: ptr(collection), NumConsumers: ptr(int64(4))},
		},
		{
			name:           "boolean",
			constructorArg: "kafka_commit_on_select = 0",
			settings:       map[string]string{"kafka_commit_on_select": "1"},
			want:           EngineKafka{Collection: ptr(collection), CommitOnSelect: ptr(true)},
		},
		{
			name:           "unknown kafka setting",
			constructorArg: "kafka_future_setting = 'constructor'",
			settings:       map[string]string{"kafka_future_setting": "settings"},
			want: EngineKafka{
				Collection: ptr(collection),
				Extra:      map[string]string{"kafka_future_setting": "settings"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildKafkaEngine([]string{collection, tc.constructorArg}, tc.settings)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestKafkaEngineSettingsOverrideConstructor_HCLSQLRoundTrip(t *testing.T) {
	const createSQL = `CREATE TABLE db.kafka_precedence (x UInt8)
		ENGINE = Kafka(kafka_audit, kafka_topic_list = 'constructor')
		SETTINGS kafka_topic_list = 'settings'`

	table, err := buildTableFromCreateSQL(createSQL)
	require.NoError(t, err)
	table.Name = "kafka_precedence"
	parsed := table.Engine.Decoded.(EngineKafka)
	assert.Equal(t, ptr("settings"), parsed.TopicList)

	schema := &Schema{
		NamedCollections: []NamedCollectionSpec{{Name: "kafka_audit", External: true}},
		Databases:        []DatabaseSpec{{Name: "db", Tables: []TableSpec{table}}},
	}
	require.NoError(t, Resolve(schema))
	var dumped bytes.Buffer
	require.NoError(t, Write(&dumped, schema))
	assert.Contains(t, dumped.String(), `topic_list = "settings"`)
	assert.NotContains(t, dumped.String(), "constructor")

	path := filepath.Join(t.TempDir(), "schema.hcl")
	require.NoError(t, os.WriteFile(path, dumped.Bytes(), 0o600))
	reloaded, err := ParseFile(path)
	require.NoError(t, err, "re-parse failed; dump output:\n%s", dumped.String())
	require.NoError(t, Resolve(reloaded))

	generated := GenerateSQL(Diff(nil, reloaded))
	require.Len(t, generated.Statements, 1)
	assert.Contains(t, generated.Statements[0], "ENGINE = Kafka(kafka_audit)")
	assert.Contains(t, generated.Statements[0], "kafka_topic_list = 'settings'")
	assert.NotContains(t, generated.Statements[0], "constructor")
}

func TestKafkaEngineLegacyStringParser_PreservesConstructorAndSettings(t *testing.T) {
	got, err := ParseEngineString(
		`Kafka(warpstream_ingestion) SETTINGS kafka_num_consumers = 4, kafka_thread_per_consumer = 1`,
	)
	require.NoError(t, err)
	assert.Equal(t, EngineKafka{
		Collection:        ptr("warpstream_ingestion"),
		NumConsumers:      ptr(int64(4)),
		ThreadPerConsumer: ptr(true),
	}, got)
}

func TestKafkaEngineProductionShapedFixture_ParsesWithoutCorruption(t *testing.T) {
	fixture := filepath.Join("..", "..", "..", "test", "testdata", "posthog-create-statements", "Kafka", "kafka_events_json_ws_named_collection.sql")
	body, err := os.ReadFile(fixture)
	require.NoError(t, err)
	table, err := buildTableFromCreateSQL(string(body))
	require.NoError(t, err)

	want := EngineKafka{
		Collection:   ptr("warpstream_ingestion"),
		TopicList:    ptr("clickhouse_events_json"),
		GroupName:    ptr("clickhouse_events_json_ws"),
		Format:       ptr("JSONEachRow"),
		NumConsumers: ptr(int64(1)),
		MaxBlockSize: ptr(int64(100000)),
	}
	require.NotNil(t, table.Engine)
	assert.Equal(t, want, table.Engine.Decoded)
}

func TestKafkaEngineCollectionOverrides_HCLRoundTrip(t *testing.T) {
	roundTrip(t, filepath.Join("testdata", "kafka_with_collection_overrides.hcl"))
}
