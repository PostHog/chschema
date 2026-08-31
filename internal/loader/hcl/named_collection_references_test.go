package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInferExternalNamedCollections(t *testing.T) {
	managed := "managed"
	kafkaExternal := "z_kafka_external"
	dictionaryExternal := "a_dictionary_external"
	schema := &Schema{
		NamedCollections: []NamedCollectionSpec{{
			Name:   managed,
			Params: []NamedCollectionParam{{Key: "kafka_broker_list", Value: "broker:9092"}},
		}},
		Databases: []DatabaseSpec{{
			Name: "db",
			Tables: []TableSpec{
				{Name: "managed", Engine: &EngineSpec{Kind: "kafka", Decoded: EngineKafka{Collection: &managed}}},
				{Name: "external", Engine: &EngineSpec{Kind: "kafka", Decoded: EngineKafka{Collection: &kafkaExternal}}},
				{Name: "external_again", Engine: &EngineSpec{Kind: "kafka", Decoded: EngineKafka{Collection: &kafkaExternal}}},
			},
			Dictionaries: []DictionarySpec{{
				Name: "external_dictionary",
				Source: &DictionarySourceSpec{
					Kind:    "http",
					Decoded: SourceHTTP{Collection: &dictionaryExternal},
				},
			}},
		}},
	}

	assert.Equal(t, []string{dictionaryExternal, kafkaExternal}, InferExternalNamedCollections(schema))
	require.Len(t, schema.NamedCollections, 3)
	assert.Equal(t, managed, schema.NamedCollections[0].Name)
	assert.Equal(t, NamedCollectionSpec{Name: dictionaryExternal, External: true}, schema.NamedCollections[1])
	assert.Equal(t, NamedCollectionSpec{Name: kafkaExternal, External: true}, schema.NamedCollections[2])

	assert.Empty(t, InferExternalNamedCollections(schema), "inference must be idempotent")
}

func TestInferExternalNamedCollections_LeavesAuthoredResolveStrict(t *testing.T) {
	collection := "external_kafka"
	schema := &Schema{Databases: []DatabaseSpec{{
		Name: "db",
		Tables: []TableSpec{{
			Name:    "events",
			Columns: []ColumnSpec{{Name: "id", Type: "UInt64"}},
			Engine:  &EngineSpec{Kind: "kafka", Decoded: EngineKafka{Collection: &collection}},
		}},
	}}}

	err := Resolve(schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "references collection \"external_kafka\" which is not declared")

	assert.Equal(t, []string{collection}, InferExternalNamedCollections(schema))
	require.NoError(t, Resolve(schema))
}
