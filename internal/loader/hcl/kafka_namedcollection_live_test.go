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

	wantNC := NamedCollectionSpec{
		Name: ncName,
		Params: []NamedCollectionParam{
			{Key: "kafka_broker_list", Value: "kafka:9092"},
			{Key: "kafka_topic_list", Value: "test_events"},
			{Key: "kafka_group_name", Value: "test_group"},
			{Key: "kafka_format", Value: "JSONEachRow"},
		},
	}

	wantTbl := TableSpec{
		Name: "kafka_consumer",
		Columns: []ColumnSpec{
			{Name: "id", Type: "UInt64"},
			{Name: "payload", Type: "String"},
		},
		Engine: &EngineSpec{Kind: "kafka", Decoded: EngineKafka{Collection: &ncName}},
	}

	cs := ChangeSet{
		NamedCollections: []NamedCollectionChange{{Name: ncName, Add: &wantNC}},
		Databases:        []DatabaseChange{{Database: dbName, AddTables: []TableSpec{wantTbl}}},
	}
	gen := GenerateSQL(cs)
	require.NotEmpty(t, gen.Statements)
	for _, stmt := range gen.Statements {
		require.NoError(t, conn.Exec(ctx, stmt), "exec failed: %s", stmt)
	}

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
	wantValues := map[string]string{}
	for _, p := range wantNC.Params {
		wantValues[p.Key] = p.Value
	}
	gotValues := map[string]string{}
	for _, p := range gotNC.Params {
		gotValues[p.Key] = p.Value
	}
	assertNCValuesMatchOrSkipRedacted(t, wantValues, gotValues)

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
// SETTINGS form with mixed typed settings (numeric, bool, string) to
// confirm introspection captures every type.
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
