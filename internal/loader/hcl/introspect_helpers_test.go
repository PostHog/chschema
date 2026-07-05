package hcl

import (
	"testing"

	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/stretchr/testify/assert"
)

func TestKafkaSetting_TypedFields(t *testing.T) {
	tests := []struct {
		name string
		key  string
		val  string
		want EngineKafka
	}{
		{"broker_list", "kafka_broker_list", "k1:9092,k2:9092", EngineKafka{BrokerList: strPtr("k1:9092,k2:9092")}},
		{"topic_list", "kafka_topic_list", "events", EngineKafka{TopicList: strPtr("events")}},
		{"group_name", "kafka_group_name", "ch-consumer", EngineKafka{GroupName: strPtr("ch-consumer")}},
		{"format", "kafka_format", "JSONEachRow", EngineKafka{Format: strPtr("JSONEachRow")}},
		{"security_protocol", "kafka_security_protocol", "SASL_SSL", EngineKafka{SecurityProtocol: strPtr("SASL_SSL")}},
		{"sasl_mechanism", "kafka_sasl_mechanism", "PLAIN", EngineKafka{SaslMechanism: strPtr("PLAIN")}},
		{"sasl_username", "kafka_sasl_username", "svc", EngineKafka{SaslUsername: strPtr("svc")}},
		{"sasl_password", "kafka_sasl_password", "hunter2", EngineKafka{SaslPassword: strPtr("hunter2")}},
		{"client_id", "kafka_client_id", "hclexp", EngineKafka{ClientID: strPtr("hclexp")}},
		{"schema", "kafka_schema", "schema.proto:Envelope", EngineKafka{Schema: strPtr("schema.proto:Envelope")}},
		{"handle_error_mode", "kafka_handle_error_mode", "stream", EngineKafka{HandleErrorMode: strPtr("stream")}},
		{"compression_codec", "kafka_compression_codec", "zstd", EngineKafka{CompressionCodec: strPtr("zstd")}},
		{"num_consumers", "kafka_num_consumers", "4", EngineKafka{NumConsumers: ptr(int64(4))}},
		{"max_block_size", "kafka_max_block_size", "65536", EngineKafka{MaxBlockSize: ptr(int64(65536))}},
		{"skip_broken_messages", "kafka_skip_broken_messages", "10", EngineKafka{SkipBrokenMessages: ptr(int64(10))}},
		{"poll_timeout_ms", "kafka_poll_timeout_ms", "5000", EngineKafka{PollTimeoutMs: ptr(int64(5000))}},
		{"poll_max_batch_size", "kafka_poll_max_batch_size", "1000", EngineKafka{PollMaxBatchSize: ptr(int64(1000))}},
		{"flush_interval_ms", "kafka_flush_interval_ms", "7500", EngineKafka{FlushIntervalMs: ptr(int64(7500))}},
		{"consumer_reschedule_ms", "kafka_consumer_reschedule_ms", "250", EngineKafka{ConsumerRescheduleMs: ptr(int64(250))}},
		{"max_rows_per_message", "kafka_max_rows_per_message", "1", EngineKafka{MaxRowsPerMessage: ptr(int64(1))}},
		{"compression_level", "kafka_compression_level", "3", EngineKafka{CompressionLevel: ptr(int64(3))}},
		{"commit_every_batch one", "kafka_commit_every_batch", "1", EngineKafka{CommitEveryBatch: ptr(true)}},
		{"commit_every_batch true", "kafka_commit_every_batch", "true", EngineKafka{CommitEveryBatch: ptr(true)}},
		{"thread_per_consumer zero", "kafka_thread_per_consumer", "0", EngineKafka{ThreadPerConsumer: ptr(false)}},
		{"thread_per_consumer false", "kafka_thread_per_consumer", "false", EngineKafka{ThreadPerConsumer: ptr(false)}},
		{"commit_on_select", "kafka_commit_on_select", "true", EngineKafka{CommitOnSelect: ptr(true)}},
		{"autodetect_client_rack", "kafka_autodetect_client_rack", "false", EngineKafka{AutodetectClientRack: ptr(false)}},
		{"malformed int leaves field unset", "kafka_num_consumers", "many", EngineKafka{}},
		{"empty int leaves field unset", "kafka_max_block_size", "", EngineKafka{}},
		{"malformed bool leaves field unset", "kafka_commit_every_batch", "maybe", EngineKafka{}},
		{"unknown key lands in Extra", "kafka_client_dns_lookup", "use_all_dns_ips", EngineKafka{Extra: map[string]string{"kafka_client_dns_lookup": "use_all_dns_ips"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k := EngineKafka{}
			applyKafkaSetting(&k, tc.key, tc.val)
			assert.Equal(t, tc.want, k)
		})
	}
}

func TestKafkaSetting_ExtraAccumulates(t *testing.T) {
	k := EngineKafka{}
	applyKafkaSetting(&k, "kafka_fetch_min_bytes", "1024")
	applyKafkaSetting(&k, "kafka_queued_min_messages", "100000")
	applyKafkaSetting(&k, "kafka_format", "Avro")

	want := EngineKafka{
		Format: strPtr("Avro"),
		Extra: map[string]string{
			"kafka_fetch_min_bytes":     "1024",
			"kafka_queued_min_messages": "100000",
		},
	}
	assert.Equal(t, want, k)
}

func TestHelperParseBoolPtr(t *testing.T) {
	tests := []struct {
		in   string
		want *bool
	}{
		{"1", ptr(true)},
		{"true", ptr(true)},
		{"TRUE", ptr(true)},
		{"  True\t", ptr(true)},
		{"0", ptr(false)},
		{"false", ptr(false)},
		{"FALSE", ptr(false)},
		{"", nil},
		{"yes", nil},
		{"10", nil},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			assert.Equal(t, tc.want, parseBoolPtr(tc.in))
		})
	}
}

func TestHelperStringSliceEqual(t *testing.T) {
	tests := []struct {
		name string
		a    []string
		b    []string
		want bool
	}{
		{"both nil", nil, nil, true},
		{"nil vs empty", nil, []string{}, true},
		{"equal content", []string{"id", "ts"}, []string{"id", "ts"}, true},
		{"different lengths", []string{"id"}, []string{"id", "ts"}, false},
		{"same length different content", []string{"id", "ts"}, []string{"id", "uuid"}, false},
		{"nil vs non-empty", nil, []string{"id"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, stringSliceEqual(tc.a, tc.b))
		})
	}
}

func TestHelperSetSQLAttribute(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"no newline", "SELECT 1", "sql = \"SELECT 1\"\n"},
		{"single trailing newline", "CREATE TABLE t (x Int64)\n", "sql = \"CREATE TABLE t (x Int64)\\n\"\n"},
		{"multi-line heredoc", "CREATE TABLE t\n(\n    x Int64\n)\n", "sql = <<SQL\nCREATE TABLE t\n(\n    x Int64\n)\nSQL\n\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := hclwrite.NewEmptyFile()
			setSQLAttribute(f.Body(), "sql", tc.sql)
			assert.Equal(t, tc.want, string(f.Bytes()))
		})
	}
}

func TestHelperSetQueryAttribute(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"single line quoted", "SELECT 1 FROM t", "query = \"SELECT 1 FROM t\"\n"},
		{"multi-line heredoc", "SELECT team_id\nFROM events", "query = <<SQL\nSELECT team_id\nFROM events\nSQL\n\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := hclwrite.NewEmptyFile()
			setQueryAttribute(f.Body(), tc.query)
			assert.Equal(t, tc.want, string(f.Bytes()))
		})
	}
}

func TestHelperDistributedVirtualsStaticFallback(t *testing.T) {
	want := []DeclaredColumn{{Name: "_shard_num", Type: "UInt32"}}
	assert.Equal(t, want, EngineDistributed{}.Virtuals())
	assert.Equal(t, want, EngineDistributed{}.DynamicVirtuals(nil))
}
