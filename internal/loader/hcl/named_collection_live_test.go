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
	assertNCValuesMatchOrSkipRedacted(t, wantValues, gotValues)
}

// assertNCValuesMatchOrSkipRedacted compares NC params by keys, and by
// values only when ClickHouse has un-redacted them. When values come back
// as the literal "[HIDDEN]" (the user lacks displaySecretsInShowAndSelect
// or the cluster has SHOW NAMED COLLECTIONS SECRETS disabled at runtime),
// only the key set is asserted. The key set is always a meaningful
// round-trip signal — it proves CREATE/ALTER/DROP DDL applied — even when
// values are redacted by the cluster's access policy.
func assertNCValuesMatchOrSkipRedacted(t *testing.T, want, got map[string]string) {
	t.Helper()
	wantKeys := make([]string, 0, len(want))
	for k := range want {
		wantKeys = append(wantKeys, k)
	}
	gotKeys := make([]string, 0, len(got))
	for k := range got {
		gotKeys = append(gotKeys, k)
	}
	sortStrings(wantKeys)
	sortStrings(gotKeys)
	assert.Equal(t, wantKeys, gotKeys, "introspected NC keys mismatch")

	redacted := 0
	for _, v := range got {
		if v == "[HIDDEN]" {
			redacted++
		}
	}
	if redacted > 0 {
		t.Logf("NC values redacted to [HIDDEN] (%d of %d); skipping value comparison. "+
			"To unredact, grant displaySecretsInShowAndSelect to the connecting user "+
			"(in docker/clickhouse/users.d/grants.xml: <show_named_collections_secrets>1</show_named_collections_secrets>) "+
			"and ensure format_display_secrets_in_show_and_select takes effect.", redacted, len(got))
		return
	}
	assert.Equal(t, want, got, "introspected NC values mismatch")
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
	// Assert the SET / DELETE outcome at the key level (which always works);
	// value comparison only when ClickHouse has un-redacted the secrets.
	_, hasA := gotByKey["a"]
	_, hasC := gotByKey["c"]
	assert.True(t, hasA, "a should be present after SET")
	assert.True(t, hasC, "c should be present after SET (added)")
	if gotByKey["a"] != "[HIDDEN]" {
		assert.Equal(t, "1_new", gotByKey["a"])
	}
	if gotByKey["c"] != "[HIDDEN]" {
		assert.Equal(t, "3", gotByKey["c"])
	}
	_, present := gotByKey["b"]
	assert.False(t, present, "b should have been deleted")
}
