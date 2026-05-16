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

// uniqueNCName returns a per-test NC name (named collections are
// cluster-scoped so they can collide across parallel test runs).
func uniqueNCName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func TestCHLive_NamedCollection_ApplyRoundTrip(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	ctx := context.Background()
	name := uniqueNCName("hclexp_apply_rt")
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
	// Compare as key→value maps. Introspection returns params alphabetically
	// sorted and doesn't recover OVERRIDABLE flags from the older
	// system.named_collections schema, so order- and flag-comparison would
	// be a false negative against the apply path.
	wantValues := map[string]string{}
	for _, p := range want.Params {
		wantValues[p.Key] = p.Value
	}
	gotValues := map[string]string{}
	for _, p := range got.Params {
		gotValues[p.Key] = p.Value
	}
	assert.Equal(t, wantValues, gotValues)
}

func TestCHLive_NamedCollection_AlterSetDelete(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	ctx := context.Background()
	name := uniqueNCName("hclexp_alter")
	t.Cleanup(func() { _ = conn.Exec(ctx, "DROP NAMED COLLECTION IF EXISTS "+name) })

	initial := NamedCollectionSpec{Name: name, Params: []NamedCollectionParam{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
	}}
	require.NoError(t, conn.Exec(ctx, createNamedCollectionSQL(initial)))

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
