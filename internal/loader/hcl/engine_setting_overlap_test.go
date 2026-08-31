package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func engineOverlapSchema(engine Engine, tableSettings map[string]string) *Schema {
	schema := &Schema{Databases: []DatabaseSpec{{
		Name: "db",
		Tables: []TableSpec{{
			Name:     "events",
			Columns:  []ColumnSpec{{Name: "x", Type: "UInt8"}},
			Settings: tableSettings,
			Engine:   &EngineSpec{Kind: engine.Kind(), Decoded: engine},
		}},
	}}}
	if engine.Kind() == "kafka" {
		schema.NamedCollections = []NamedCollectionSpec{{Name: "audit_kafka", External: true}}
	}
	return schema
}

func TestResolve_KafkaTypedAndExtraSettingOverlap_AllTypedKeys(t *testing.T) {
	tests := []struct {
		key   string
		value string
		attr  string
	}{
		{key: "kafka_broker_list", value: "broker:9092", attr: "broker_list"},
		{key: "kafka_topic_list", value: "events", attr: "topic_list"},
		{key: "kafka_group_name", value: "group", attr: "group_name"},
		{key: "kafka_format", value: "JSONEachRow", attr: "format"},
		{key: "kafka_security_protocol", value: "SASL_SSL", attr: "security_protocol"},
		{key: "kafka_sasl_mechanism", value: "PLAIN", attr: "sasl_mechanism"},
		{key: "kafka_sasl_username", value: "user", attr: "sasl_username"},
		{key: "kafka_sasl_password", value: "password", attr: "sasl_password"},
		{key: "kafka_client_id", value: "client", attr: "client_id"},
		{key: "kafka_schema", value: "schema", attr: "schema"},
		{key: "kafka_handle_error_mode", value: "stream", attr: "handle_error_mode"},
		{key: "kafka_compression_codec", value: "zstd", attr: "compression_codec"},
		{key: "kafka_autodetect_client_rack", value: "CLICKHOUSE", attr: "autodetect_client_rack"},
		{key: "kafka_num_consumers", value: "2", attr: "num_consumers"},
		{key: "kafka_max_block_size", value: "65536", attr: "max_block_size"},
		{key: "kafka_skip_broken_messages", value: "1", attr: "skip_broken_messages"},
		{key: "kafka_poll_timeout_ms", value: "5000", attr: "poll_timeout_ms"},
		{key: "kafka_poll_max_batch_size", value: "1000", attr: "poll_max_batch_size"},
		{key: "kafka_flush_interval_ms", value: "7500", attr: "flush_interval_ms"},
		{key: "kafka_consumer_reschedule_ms", value: "250", attr: "consumer_reschedule_ms"},
		{key: "kafka_max_rows_per_message", value: "3", attr: "max_rows_per_message"},
		{key: "kafka_compression_level", value: "4", attr: "compression_level"},
		{key: "kafka_commit_every_batch", value: "1", attr: "commit_every_batch"},
		{key: "kafka_thread_per_consumer", value: "1", attr: "thread_per_consumer"},
		{key: "kafka_commit_on_select", value: "1", attr: "commit_on_select"},
	}

	for _, tc := range tests {
		t.Run(tc.attr, func(t *testing.T) {
			engine := EngineKafka{Collection: ptr("audit_kafka")}
			require.NoError(t, applyKafkaSetting(&engine, tc.key, tc.value))
			require.Empty(t, engine.Extra, "%s must decode to a typed field", tc.key)
			engine.Extra = map[string]string{tc.key: "duplicate"}

			err := Resolve(engineOverlapSchema(engine, nil))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.key)
			assert.Contains(t, err.Error(), `engine "kafka".`+tc.attr)
			assert.Contains(t, err.Error(), `engine "kafka".extra["`+tc.key+`"]`)
		})
	}
}

func TestResolve_EngineSettingOverlapPairs(t *testing.T) {
	tests := []struct {
		name          string
		engine        Engine
		tableSettings map[string]string
		key           string
		leftPath      string
		rightPath     string
	}{
		{
			name: "kafka typed and table",
			engine: EngineKafka{
				Collection: ptr("audit_kafka"), NumConsumers: ptr(int64(1)),
			},
			tableSettings: map[string]string{"kafka_num_consumers": "2"},
			key:           "kafka_num_consumers",
			leftPath:      `engine "kafka".num_consumers`,
			rightPath:     `table.settings["kafka_num_consumers"]`,
		},
		{
			name: "kafka extra and table",
			engine: EngineKafka{
				Collection: ptr("audit_kafka"), Extra: map[string]string{"kafka_future_setting": "engine"},
			},
			tableSettings: map[string]string{"kafka_future_setting": "table"},
			key:           "kafka_future_setting",
			leftPath:      `engine "kafka".extra["kafka_future_setting"]`,
			rightPath:     `table.settings["kafka_future_setting"]`,
		},
		{
			name: "time series engine settings and table",
			engine: EngineTimeSeries{
				Settings: map[string]string{"id_generator": "cityHash64"},
			},
			tableSettings: map[string]string{"id_generator": "sipHash64"},
			key:           "id_generator",
			leftPath:      `engine "time_series".settings["id_generator"]`,
			rightPath:     `table.settings["id_generator"]`,
		},
		{
			name: "time series tags to columns and table",
			engine: EngineTimeSeries{
				TagsToColumns: map[string]string{"job": "job"},
			},
			tableSettings: map[string]string{"tags_to_columns": "{'instance':'instance'}"},
			key:           "tags_to_columns",
			leftPath:      `engine "time_series".tags_to_columns`,
			rightPath:     `table.settings["tags_to_columns"]`,
		},
		{
			name: "time series tags to columns and engine settings",
			engine: EngineTimeSeries{
				TagsToColumns: map[string]string{"job": "job"},
				Settings:      map[string]string{"tags_to_columns": "{'instance':'instance'}"},
			},
			key:       "tags_to_columns",
			leftPath:  `engine "time_series".tags_to_columns`,
			rightPath: `engine "time_series".settings["tags_to_columns"]`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Resolve(engineOverlapSchema(tc.engine, tc.tableSettings))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.key)
			assert.Contains(t, err.Error(), tc.leftPath)
			assert.Contains(t, err.Error(), tc.rightPath)
		})
	}
}

func TestResolve_NonOverlappingUnknownEngineSettingsPassThroughSQL(t *testing.T) {
	t.Run("kafka", func(t *testing.T) {
		schema := engineOverlapSchema(EngineKafka{
			Collection: ptr("audit_kafka"),
			Extra:      map[string]string{"kafka_future_engine": "engine"},
		}, map[string]string{"kafka_future_table": "table"})
		require.NoError(t, Resolve(schema))

		generated := GenerateSQL(Diff(nil, schema))
		require.Len(t, generated.Statements, 1)
		assert.Contains(t, generated.Statements[0], "kafka_future_engine = 'engine'")
		assert.Contains(t, generated.Statements[0], "kafka_future_table = 'table'")
	})

	t.Run("time series", func(t *testing.T) {
		schema := engineOverlapSchema(EngineTimeSeries{
			Settings: map[string]string{"future_engine_setting": "engine"},
		}, map[string]string{"future_table_setting": "table"})
		require.NoError(t, Resolve(schema))

		generated := GenerateSQL(Diff(nil, schema))
		require.Len(t, generated.Statements, 1)
		assert.Contains(t, generated.Statements[0], "future_engine_setting = 'engine'")
		assert.Contains(t, generated.Statements[0], "future_table_setting = 'table'")
	})
}
