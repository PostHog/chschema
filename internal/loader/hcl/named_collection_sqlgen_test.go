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

func TestGenerateSQL_NamedCollectionAlter(t *testing.T) {
	cs := ChangeSet{NamedCollections: []NamedCollectionChange{{
		Name: "nc",
		SetParams: []NamedCollectionParam{
			{Key: "a", Value: "1_new"},
			{Key: "c", Value: "3"},
		},
		DeleteParams: []string{"b"},
	}}}
	out := GenerateSQL(cs)
	require.Len(t, out.Statements, 2)
	assert.Contains(t, out.Statements[0], "SET")
	assert.Contains(t, out.Statements[1], "DELETE")
}
