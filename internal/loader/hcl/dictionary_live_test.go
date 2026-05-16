package hcl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCHLive_Dictionary_ApplyRoundTrip exercises the full add-path:
// build a DictionarySpec → render via createDictionarySQL → apply with
// conn.Exec → introspect → assert the introspected spec matches.
func TestCHLive_Dictionary_ApplyRoundTrip(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.src (`k` UInt64, `v` String) ENGINE = MergeTree ORDER BY k",
		dbName)))

	// A spec with most optional bells: comment, settings, attribute flags, query.
	q := fmt.Sprintf("SELECT k, v FROM %s.src", dbName)
	want := DictionarySpec{
		Name:       "rich_dict",
		PrimaryKey: []string{"k"},
		Attributes: []DictionaryAttribute{
			{Name: "k", Type: "UInt64"},
			{Name: "v", Type: "String"},
		},
		Source: &DictionarySourceSpec{
			Kind: "clickhouse",
			Decoded: SourceClickHouse{
				Query: &q,
				User:  ptr("default"),
			},
		},
		Layout:   &DictionaryLayoutSpec{Kind: "hashed", Decoded: LayoutHashed{}},
		Lifetime: &DictionaryLifetime{Min: ptr(int64(0))},
		Comment:  ptr("a richly-attributed dictionary"),
	}
	stmt := createDictionarySQL(dbName, want)
	require.NoError(t, conn.Exec(ctx, stmt), "DDL rejected:\n%s", stmt)

	db, err := Introspect(ctx, conn, dbName)
	require.NoError(t, err)
	got := findDictByName(db.Dictionaries, "rich_dict")
	require.NotNil(t, got)

	assert.Equal(t, want.PrimaryKey, got.PrimaryKey)
	assert.Equal(t, want.Attributes, got.Attributes)
	require.NotNil(t, got.Source)
	assert.Equal(t, "clickhouse", got.Source.Kind)
	chSrc, ok := got.Source.Decoded.(SourceClickHouse)
	require.True(t, ok)
	require.NotNil(t, chSrc.Query)
	assert.Contains(t, *chSrc.Query, dbName+".src")
	require.NotNil(t, chSrc.User)
	assert.Equal(t, "default", *chSrc.User)
	require.NotNil(t, got.Layout)
	assert.Equal(t, "hashed", got.Layout.Kind)
	assert.Equal(t, LayoutHashed{}, got.Layout.Decoded)
	require.NotNil(t, got.Lifetime)
	require.NotNil(t, got.Comment)
	assert.Equal(t, "a richly-attributed dictionary", *got.Comment)
}

// TestCHLive_Dictionary_LayoutMatrix iterates the 10 supported layout kinds.
// Each is created, introspected, and the round-tripped layout kind +
// concrete decoded value is asserted.
func TestCHLive_Dictionary_LayoutMatrix(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.src (`k` UInt64, `v` String) ENGINE = MergeTree ORDER BY k",
		dbName)))
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.csrc (`a` String, `b` String, `v` String) ENGINE = MergeTree ORDER BY (a, b)",
		dbName)))
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.rsrc (`a` String, `s` Date, `e` Date, `v` String) ENGINE = MergeTree ORDER BY a",
		dbName)))
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.ipsrc (`ip` String, `v` String) ENGINE = MergeTree ORDER BY ip",
		dbName)))

	cases := []struct {
		name   string
		layout DictionaryLayout
		// composite key needed for complex_key_*; range layouts also need range cols and a Date primary key.
		composite bool
		rangeForm bool
		ipTrie    bool
	}{
		{"flat", LayoutFlat{}, false, false, false},
		{"hashed", LayoutHashed{}, false, false, false},
		{"sparse_hashed", LayoutSparseHashed{}, false, false, false},
		{"complex_key_hashed", LayoutComplexKeyHashed{}, true, false, false},
		{"complex_key_hashed_preallocate", LayoutComplexKeyHashed{Preallocate: ptr(int64(0))}, true, false, false},
		{"complex_key_sparse_hashed", LayoutComplexKeySparseHashed{}, true, false, false},
		{"range_hashed", LayoutRangeHashed{}, false, true, false},
		{"complex_key_range_hashed", LayoutComplexKeyRangeHashed{RangeLookupStrategy: ptr("max")}, true, true, false},
		{"cache", LayoutCache{SizeInCells: 1000}, false, false, false},
		{"ip_trie", LayoutIPTrie{}, false, false, true},
		{"direct", LayoutDirect{}, false, false, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dictName := strings.ReplaceAll(tc.name, "-", "_") + "_dict"

			var spec DictionarySpec
			switch {
			case tc.ipTrie:
				spec = DictionarySpec{
					Name:       dictName,
					PrimaryKey: []string{"ip"},
					Attributes: []DictionaryAttribute{
						{Name: "ip", Type: "String"},
						{Name: "v", Type: "String"},
					},
					Source: &DictionarySourceSpec{Kind: "clickhouse", Decoded: SourceClickHouse{
						Query: ptr(fmt.Sprintf("SELECT ip, v FROM %s.ipsrc", dbName)),
					}},
					Layout:   &DictionaryLayoutSpec{Kind: tc.layout.Kind(), Decoded: tc.layout},
					Lifetime: &DictionaryLifetime{Min: ptr(int64(0))},
				}
			case tc.rangeForm && tc.composite:
				spec = DictionarySpec{
					Name:       dictName,
					PrimaryKey: []string{"a"},
					Attributes: []DictionaryAttribute{
						{Name: "a", Type: "String"},
						{Name: "s", Type: "Date"},
						{Name: "e", Type: "Date"},
						{Name: "v", Type: "String"},
					},
					Source: &DictionarySourceSpec{Kind: "clickhouse", Decoded: SourceClickHouse{
						Query: ptr(fmt.Sprintf("SELECT a, s, e, v FROM %s.rsrc", dbName)),
					}},
					Layout:   &DictionaryLayoutSpec{Kind: tc.layout.Kind(), Decoded: tc.layout},
					Lifetime: &DictionaryLifetime{Min: ptr(int64(0))},
					Range:    &DictionaryRange{Min: "s", Max: "e"},
				}
			case tc.rangeForm:
				// range_hashed: primary key must be UInt64, range cols are Date/DateTime.
				spec = DictionarySpec{
					Name:       dictName,
					PrimaryKey: []string{"k"},
					Attributes: []DictionaryAttribute{
						{Name: "k", Type: "UInt64"},
						{Name: "s", Type: "Date"},
						{Name: "e", Type: "Date"},
						{Name: "v", Type: "String"},
					},
					Source: &DictionarySourceSpec{Kind: "clickhouse", Decoded: SourceClickHouse{
						Query: ptr(fmt.Sprintf("SELECT k, toDate('2024-01-01') AS s, toDate('2099-01-01') AS e, v FROM %s.src", dbName)),
					}},
					Layout:   &DictionaryLayoutSpec{Kind: tc.layout.Kind(), Decoded: tc.layout},
					Lifetime: &DictionaryLifetime{Min: ptr(int64(0))},
					Range:    &DictionaryRange{Min: "s", Max: "e"},
				}
			case tc.composite:
				spec = DictionarySpec{
					Name:       dictName,
					PrimaryKey: []string{"a", "b"},
					Attributes: []DictionaryAttribute{
						{Name: "a", Type: "String"},
						{Name: "b", Type: "String"},
						{Name: "v", Type: "String"},
					},
					Source: &DictionarySourceSpec{Kind: "clickhouse", Decoded: SourceClickHouse{
						Query: ptr(fmt.Sprintf("SELECT a, b, v FROM %s.csrc", dbName)),
					}},
					Layout:   &DictionaryLayoutSpec{Kind: tc.layout.Kind(), Decoded: tc.layout},
					Lifetime: &DictionaryLifetime{Min: ptr(int64(0))},
				}
			default:
				spec = DictionarySpec{
					Name:       dictName,
					PrimaryKey: []string{"k"},
					Attributes: []DictionaryAttribute{
						{Name: "k", Type: "UInt64"},
						{Name: "v", Type: "String"},
					},
					Source: &DictionarySourceSpec{Kind: "clickhouse", Decoded: SourceClickHouse{
						Query: ptr(fmt.Sprintf("SELECT k, v FROM %s.src", dbName)),
					}},
					Layout:   &DictionaryLayoutSpec{Kind: tc.layout.Kind(), Decoded: tc.layout},
					Lifetime: &DictionaryLifetime{Min: ptr(int64(0))},
				}
				// DIRECT layout rejects LIFETIME (sqlgen skips it but for
				// hygiene leave it nil in the spec to match what introspection
				// will return).
				if _, isDirect := tc.layout.(LayoutDirect); isDirect {
					spec.Lifetime = nil
				}
			}

			stmt := createDictionarySQL(dbName, spec)
			require.NoError(t, conn.Exec(ctx, stmt), "DDL rejected:\n%s", stmt)

			db, err := Introspect(ctx, conn, dbName)
			require.NoError(t, err)
			got := findDictByName(db.Dictionaries, dictName)
			require.NotNil(t, got, "introspected schema missing %s", dictName)
			require.NotNil(t, got.Layout)
			assert.Equal(t, tc.layout.Kind(), got.Layout.Kind)
			// Concrete decoded type must match.
			assert.IsType(t, tc.layout, got.Layout.Decoded)
		})
	}
}

// TestCHLive_Dictionary_LifetimeForms verifies LIFETIME(MIN x MAX y) and
// LIFETIME(n) both introspect into the right DictionaryLifetime shape.
func TestCHLive_Dictionary_LifetimeForms(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.src (`k` UInt64, `v` String) ENGINE = MergeTree ORDER BY k",
		dbName)))

	makeSpec := func(name string, lt *DictionaryLifetime) DictionarySpec {
		return DictionarySpec{
			Name:       name,
			PrimaryKey: []string{"k"},
			Attributes: []DictionaryAttribute{
				{Name: "k", Type: "UInt64"},
				{Name: "v", Type: "String"},
			},
			Source: &DictionarySourceSpec{Kind: "clickhouse", Decoded: SourceClickHouse{
				Query: ptr(fmt.Sprintf("SELECT k, v FROM %s.src", dbName)),
			}},
			Layout:   &DictionaryLayoutSpec{Kind: "hashed", Decoded: LayoutHashed{}},
			Lifetime: lt,
		}
	}

	t.Run("simple form LIFETIME(300)", func(t *testing.T) {
		spec := makeSpec("simple_lifetime_dict", &DictionaryLifetime{Min: ptr(int64(300))})
		require.NoError(t, conn.Exec(ctx, createDictionarySQL(dbName, spec)))
		db, err := Introspect(ctx, conn, dbName)
		require.NoError(t, err)
		got := findDictByName(db.Dictionaries, "simple_lifetime_dict")
		require.NotNil(t, got)
		require.NotNil(t, got.Lifetime)
		// ClickHouse normalizes LIFETIME(n) to (MIN 0 MAX n) in system.dictionaries
		// terms, but create_table_query keeps the simple form. We accept either:
		// either Min=300 alone, or Min=0 Max=300.
		if got.Lifetime.Max == nil {
			assert.Equal(t, int64(300), *got.Lifetime.Min, "simple form: Min should be 300")
		} else {
			assert.Equal(t, int64(0), *got.Lifetime.Min)
			assert.Equal(t, int64(300), *got.Lifetime.Max)
		}
	})

	t.Run("range form LIFETIME(MIN 300 MAX 600)", func(t *testing.T) {
		spec := makeSpec("range_lifetime_dict", &DictionaryLifetime{Min: ptr(int64(300)), Max: ptr(int64(600))})
		require.NoError(t, conn.Exec(ctx, createDictionarySQL(dbName, spec)))
		db, err := Introspect(ctx, conn, dbName)
		require.NoError(t, err)
		got := findDictByName(db.Dictionaries, "range_lifetime_dict")
		require.NotNil(t, got)
		require.NotNil(t, got.Lifetime)
		require.NotNil(t, got.Lifetime.Min)
		require.NotNil(t, got.Lifetime.Max)
		assert.Equal(t, int64(300), *got.Lifetime.Min)
		assert.Equal(t, int64(600), *got.Lifetime.Max)
	})
}

// TestCHLive_Dictionary_AttributeFlags verifies HIERARCHICAL and INJECTIVE
// flags survive the apply→introspect round-trip.
func TestCHLive_Dictionary_AttributeFlags(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE %s.src (`k` UInt64, `parent` UInt64, `label` String) ENGINE = MergeTree ORDER BY k",
		dbName)))

	spec := DictionarySpec{
		Name:       "flagged_dict",
		PrimaryKey: []string{"k"},
		Attributes: []DictionaryAttribute{
			{Name: "k", Type: "UInt64"},
			{Name: "parent", Type: "UInt64", Hierarchical: true},
			{Name: "label", Type: "String", Injective: true},
		},
		Source: &DictionarySourceSpec{Kind: "clickhouse", Decoded: SourceClickHouse{
			Query: ptr(fmt.Sprintf("SELECT k, parent, label FROM %s.src", dbName)),
		}},
		Layout:   &DictionaryLayoutSpec{Kind: "hashed", Decoded: LayoutHashed{}},
		Lifetime: &DictionaryLifetime{Min: ptr(int64(0))},
	}
	stmt := createDictionarySQL(dbName, spec)
	require.NoError(t, conn.Exec(ctx, stmt), "DDL rejected:\n%s", stmt)

	db, err := Introspect(ctx, conn, dbName)
	require.NoError(t, err)
	got := findDictByName(db.Dictionaries, "flagged_dict")
	require.NotNil(t, got)

	require.Len(t, got.Attributes, 3)
	// Find each attribute and check its flags.
	for _, a := range got.Attributes {
		switch a.Name {
		case "k":
			assert.False(t, a.Hierarchical, "k.Hierarchical")
			assert.False(t, a.Injective, "k.Injective")
		case "parent":
			assert.True(t, a.Hierarchical, "parent.Hierarchical")
		case "label":
			assert.True(t, a.Injective, "label.Injective")
		}
	}
}

// TestCHLive_Dictionary_PosthogFixtures walks every fixture in
// test/testdata/posthog-create-statements/Dictionary/, rewrites the database
// reference to an isolated test DB, applies the statement, and asserts the
// resulting dictionary introspects without error. Validates our introspect
// path against real-world PostHog dictionary shapes.
func TestCHLive_Dictionary_PosthogFixtures(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	// The fixtures reference tables in `default`. The dictionary CREATE itself
	// doesn't require the source table to exist (it loads lazily), so we
	// rewrite `default.` to <dbName>. and `default.tableX` keys aren't strictly
	// required for introspection — but we DO need the dictionary's own qualifier
	// to be in dbName so Introspect picks it up.
	fixtureDir := filepath.Join("..", "..", "..", "test", "testdata", "posthog-create-statements", "Dictionary")
	entries, err := os.ReadDir(fixtureDir)
	require.NoError(t, err, "fixture dir: %s", fixtureDir)

	count := 0
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		name := e.Name()
		t.Run(strings.TrimSuffix(name, ".sql"), func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join(fixtureDir, name))
			require.NoError(t, err)
			sql := string(body)
			// Rewrite the dictionary's own database qualifier (DICTIONARY default.X)
			// to dbName so it lands in the isolated test database. References inside
			// the QUERY remain pointing at `default.*`; that's fine since ClickHouse
			// doesn't validate the SOURCE QUERY at CREATE time.
			sql = strings.Replace(sql, "DICTIONARY default.", "DICTIONARY "+dbName+".", 1)
			// Strip [HIDDEN] passwords to empty so the CREATE statement is valid.
			sql = strings.ReplaceAll(sql, "PASSWORD '[HIDDEN]'", "PASSWORD ''")

			require.NoError(t, conn.Exec(ctx, sql), "fixture %s rejected:\n%s", name, sql)
			count++
		})
	}

	// One Introspect call covers all fixtures created above.
	db, err := Introspect(ctx, conn, dbName)
	require.NoError(t, err, "Introspect failed on posthog fixtures")
	assert.Len(t, db.Dictionaries, count, "all %d fixture dictionaries should round-trip through Introspect", count)
}

// findDictByName is a small linear search helper for live tests.
func findDictByName(ds []DictionarySpec, name string) *DictionarySpec {
	for i := range ds {
		if ds[i].Name == name {
			return &ds[i]
		}
	}
	return nil
}
