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
