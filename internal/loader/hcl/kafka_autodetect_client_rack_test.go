package hcl

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaAutodetectClientRack_ParserHCLSQLRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		value *string
	}{
		{name: "absent"},
		{name: "empty disables", value: ptr("")},
		{name: "AWS zone ID", value: ptr("AWS_ZONE_ID")},
		{name: "AWS zone name", value: ptr("AWS_ZONE_NAME")},
		{name: "GCP zone", value: ptr("GCP_ZONE")},
		{name: "ClickHouse", value: ptr("CLICKHOUSE")},
		{name: "AWS then GCP fallback", value: ptr("AWS_ZONE_NAME_THEN_GCP_ZONE")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rackSetting := ""
			if tc.value != nil {
				rackSetting = fmt.Sprintf(", kafka_autodetect_client_rack = '%s'", *tc.value)
			}
			createSQL := `CREATE TABLE db.kafka_rack (id UInt64) ENGINE = Kafka SETTINGS ` +
				`kafka_broker_list = 'kafka:9092', kafka_topic_list = 'events', ` +
				`kafka_group_name = 'group1', kafka_format = 'JSONEachRow'` + rackSetting

			table, err := buildTableFromCreateSQL(createSQL)
			require.NoError(t, err)
			table.Name = "kafka_rack" // assigned by the production introspect caller
			parsed := table.Engine.Decoded.(EngineKafka)
			assert.Equal(t, tc.value, parsed.AutodetectClientRack)

			schema := &Schema{Databases: []DatabaseSpec{{Name: "db", Tables: []TableSpec{table}}}}
			require.NoError(t, Resolve(schema))
			var dumped bytes.Buffer
			require.NoError(t, Write(&dumped, schema))
			if tc.value == nil {
				assert.NotContains(t, dumped.String(), "autodetect_client_rack")
			} else {
				assert.Contains(t, dumped.String(), fmt.Sprintf("autodetect_client_rack = %q", *tc.value))
			}

			path := filepath.Join(t.TempDir(), "schema.hcl")
			require.NoError(t, os.WriteFile(path, dumped.Bytes(), 0o600))
			reloaded, err := ParseFile(path)
			require.NoError(t, err, "re-parse failed; dump output:\n%s", dumped.String())
			require.NoError(t, Resolve(reloaded))
			reloadedKafka := reloaded.Databases[0].Tables[0].Engine.Decoded.(EngineKafka)
			assert.Equal(t, tc.value, reloadedKafka.AutodetectClientRack)

			generated := GenerateSQL(Diff(nil, reloaded))
			require.Len(t, generated.Statements, 1)
			if tc.value == nil {
				assert.NotContains(t, generated.Statements[0], "kafka_autodetect_client_rack")
			} else {
				assert.Contains(t, generated.Statements[0],
					fmt.Sprintf("kafka_autodetect_client_rack = '%s'", *tc.value))
			}
		})
	}
}

func TestKafkaAutodetectClientRack_LegacyBooleanHCLIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-bool.hcl")
	require.NoError(t, os.WriteFile(path, []byte(`
database "db" {
  table "kafka_rack" {
    column "id" { type = "UInt64" }
    engine "kafka" {
      broker_list           = "kafka:9092"
      topic_list            = "events"
      group_name            = "group1"
      format                = "JSONEachRow"
      autodetect_client_rack = true
    }
  }
}
`), 0o600))

	_, err := ParseFile(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "autodetect_client_rack")
	assert.Contains(t, err.Error(), "must be a string")
	assert.Contains(t, err.Error(), "boolean HCL is no longer supported")
}
