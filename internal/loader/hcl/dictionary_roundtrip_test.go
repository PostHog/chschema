package hcl

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Round-trips every modeled dictionary layout/source variant from canned
// CREATE DICTIONARY DDL (issue #99): DDL → spec → HCL dump → HCL decode,
// so the variant paths no longer depend on live-ClickHouse tests alone.

const (
	rtDictLayoutDDL = "CREATE DICTIONARY db.d (k UInt64, v String) PRIMARY KEY k SOURCE(NULL()) LIFETIME(0) LAYOUT(%s)"
	rtDictSourceDDL = "CREATE DICTIONARY db.d (k UInt64, v String) PRIMARY KEY k SOURCE(%s) LIFETIME(0) LAYOUT(HASHED())"
)

func rtDictFromDDL(t *testing.T, ddl string) DictionarySpec {
	t.Helper()
	d, err := buildDictionaryFromCreateSQL(ddl)
	require.NoError(t, err)
	// The introspect dispatch (upsertObjectFromStmt) assigns Name from the
	// system.tables row, not from the AST; mirror it.
	d.Name = "d"
	return d
}

func rtDumpDictHCL(t *testing.T, d DictionarySpec) string {
	t.Helper()
	db := &DatabaseSpec{Name: "db", Dictionaries: []DictionarySpec{d}}
	out, err := RenderObjectHCL("db", KindDictionary, d.Name, db)
	require.NoError(t, err)
	return out
}

// rtFlatWS collapses runs of whitespace so Contains assertions are immune to
// hclwrite's `=` column alignment.
func rtFlatWS(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// The redaction marker must survive introspect → dump file → load, because
// that is the path nearly every comparison actually takes: `drift` diffs
// per-node HCL dumps, and the golden gate diffs against a dump written
// earlier. If introspection dropped the redacted password (as it did before
// #140), the dump would merely have NO password — indistinguishable from a
// dictionary that genuinely has none — and the authored golden would diff as
// a phantom `source` change on every run, forever.
func TestDictionaryRT_RedactionMarkerSurvivesDumpAndLoad(t *testing.T) {
	introspected := rtDictFromDDL(t, "CREATE DICTIONARY db.d (k UInt64, v String) PRIMARY KEY k "+
		"SOURCE(MYSQL(HOST 'h' USER 'u' PASSWORD '[HIDDEN]')) LIFETIME(0) LAYOUT(HASHED())")

	var buf strings.Builder
	require.NoError(t, Write(&buf, &Schema{
		Databases: []DatabaseSpec{{Name: "db", Dictionaries: []DictionarySpec{introspected}}},
	}))
	assert.Contains(t, rtFlatWS(buf.String()), `password = "[HIDDEN]"`)

	dir := t.TempDir()
	path := filepath.Join(dir, "dump.hcl")
	require.NoError(t, os.WriteFile(path, []byte(buf.String()), 0o644))
	dumped, err := LoadLayers([]string{path})
	require.NoError(t, err)
	require.Len(t, dumped.Databases[0].Dictionaries, 1)

	// The authored golden holds the real secret. Against the dump file, that is
	// unverifiable — not a change.
	authored := rtDictFromDDL(t, "CREATE DICTIONARY db.d (k UInt64, v String) PRIMARY KEY k "+
		"SOURCE(MYSQL(HOST 'h' USER 'u' PASSWORD 's3cret')) LIFETIME(0) LAYOUT(HASHED())")
	golden := &Schema{Databases: []DatabaseSpec{{Name: "db", Dictionaries: []DictionarySpec{authored}}}}

	cs := Diff(dumped, golden)
	require.Len(t, cs.Databases[0].AlterDictionaries, 1)
	dd := cs.Databases[0].AlterDictionaries[0]
	assert.Empty(t, dd.Changed, "no phantom source change against a dump file")
	assert.Equal(t, []string{"password"}, dd.SkippedRedactedSecrets)
	assert.Empty(t, GenerateSQL(cs).Statements)

	// dump ↔ dump (the drift case): both nodes equally blind, so they compare
	// clean rather than showing permanent drift.
	assert.True(t, Diff(dumped, dumped).IsEmpty())
}

// Unknown layout/source args must abort, not silently drop (#115) — the
// #108 failure mode: symmetric loss is invisible to diff.
func TestDictionaryRT_UnknownArgsError(t *testing.T) {
	_, err := buildDictionaryFromCreateSQL(
		"CREATE DICTIONARY db.d (id UInt64) PRIMARY KEY id " +
			"SOURCE(CLICKHOUSE(TABLE 't')) LIFETIME(MIN 0 MAX 300) " +
			"LAYOUT(FLAT(INITIAL_ARRAY_SIZE 1024 BOGUS_KNOB 7))")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bogus_knob")

	_, err = buildDictionaryFromCreateSQL(
		"CREATE DICTIONARY db.d (id UInt64) PRIMARY KEY id " +
			"SOURCE(CLICKHOUSE(TABLE 't' FANCY_OPTION 'x')) LIFETIME(MIN 0 MAX 300) " +
			"LAYOUT(FLAT())")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fancy_option")
}

func TestDictionaryRT_Layouts(t *testing.T) {
	cases := []struct {
		name    string
		layout  string
		want    DictionaryLayout
		wantHCL []string
	}{
		{"flat", "FLAT()", LayoutFlat{}, nil},
		{"hashed", "HASHED()", LayoutHashed{}, nil},
		{"sparse_hashed", "SPARSE_HASHED()", LayoutSparseHashed{}, nil},
		{"regexp_tree", "REGEXP_TREE()", LayoutRegexpTree{}, nil},
		{"direct", "DIRECT()", LayoutDirect{}, nil},
		{"complex_key_direct", "COMPLEX_KEY_DIRECT()", LayoutComplexKeyDirect{}, nil},
		{"complex_key_sparse_hashed", "COMPLEX_KEY_SPARSE_HASHED()", LayoutComplexKeySparseHashed{}, nil},
		{"complex_key_hashed_bare", "COMPLEX_KEY_HASHED()", LayoutComplexKeyHashed{}, nil},
		{
			"complex_key_hashed_preallocate", "COMPLEX_KEY_HASHED(PREALLOCATE 1)",
			LayoutComplexKeyHashed{Preallocate: ptr(int64(1))},
			[]string{`preallocate = 1`},
		},
		{"range_hashed_bare", "RANGE_HASHED()", LayoutRangeHashed{}, nil},
		{
			"range_hashed_strategy", "RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'max')",
			LayoutRangeHashed{RangeLookupStrategy: ptr("max")},
			[]string{`range_lookup_strategy = "max"`},
		},
		{
			"complex_key_range_hashed", "COMPLEX_KEY_RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'min')",
			LayoutComplexKeyRangeHashed{RangeLookupStrategy: ptr("min")},
			[]string{`range_lookup_strategy = "min"`},
		},
		{
			"cache", "CACHE(SIZE_IN_CELLS 1000000)",
			LayoutCache{SizeInCells: 1000000},
			[]string{`size_in_cells = 1000000`},
		},
		{
			"complex_key_cache", "COMPLEX_KEY_CACHE(SIZE_IN_CELLS 5000)",
			LayoutComplexKeyCache{SizeInCells: 5000},
			[]string{`size_in_cells = 5000`},
		},
		{"hashed_array_bare", "HASHED_ARRAY()", LayoutHashedArray{}, nil},
		{
			"hashed_array_shards", "HASHED_ARRAY(SHARDS 4)",
			LayoutHashedArray{Shards: ptr(int64(4))},
			[]string{`shards = 4`},
		},
		{
			"complex_key_hashed_array_shards", "COMPLEX_KEY_HASHED_ARRAY(SHARDS 8)",
			LayoutComplexKeyHashedArray{Shards: ptr(int64(8))},
			[]string{`shards = 8`},
		},
		{"ip_trie_bare", "IP_TRIE()", LayoutIPTrie{}, nil},
		{
			"ip_trie_access_true", "IP_TRIE(ACCESS_TO_KEY_FROM_ATTRIBUTES 1)",
			LayoutIPTrie{AccessToKeyFromAttributes: ptr(true)},
			[]string{`access_to_key_from_attributes = true`},
		},
		{
			"ip_trie_access_false", "IP_TRIE(ACCESS_TO_KEY_FROM_ATTRIBUTES 0)",
			LayoutIPTrie{AccessToKeyFromAttributes: ptr(false)},
			[]string{`access_to_key_from_attributes = false`},
		},
		{
			"flat_sizes", "FLAT(INITIAL_ARRAY_SIZE 50000 MAX_ARRAY_SIZE 5000000)",
			LayoutFlat{InitialArraySize: ptr(int64(50000)), MaxArraySize: ptr(int64(5000000))},
			[]string{`initial_array_size = 50000`, `max_array_size = 5000000`},
		},
		{
			"hashed_full", "HASHED(SHARDS 16 SHARD_LOAD_QUEUE_BACKLOG 10000 MAX_LOAD_FACTOR 0.5)",
			LayoutHashed{Shards: ptr(int64(16)), ShardLoadQueueBacklog: ptr(int64(10000)), MaxLoadFactor: ptr(0.5)},
			[]string{`shards = 16`, `shard_load_queue_backlog = 10000`, `max_load_factor = 0.5`},
		},
		{
			"sparse_hashed_shards", "SPARSE_HASHED(SHARDS 2)",
			LayoutSparseHashed{Shards: ptr(int64(2))},
			[]string{`shards = 2`},
		},
		{
			"complex_key_hashed_full", "COMPLEX_KEY_HASHED(SHARDS 4 MAX_LOAD_FACTOR 0.75 PREALLOCATE 1)",
			LayoutComplexKeyHashed{Preallocate: ptr(int64(1)), Shards: ptr(int64(4)), MaxLoadFactor: ptr(0.75)},
			[]string{`shards = 4`, `max_load_factor = 0.75`, `preallocate = 1`},
		},
		{
			"complex_key_sparse_hashed_full", "COMPLEX_KEY_SPARSE_HASHED(SHARDS 8 SHARD_LOAD_QUEUE_BACKLOG 500)",
			LayoutComplexKeySparseHashed{Shards: ptr(int64(8)), ShardLoadQueueBacklog: ptr(int64(500))},
			[]string{`shards = 8`, `shard_load_queue_backlog = 500`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := rtDictFromDDL(t, fmt.Sprintf(rtDictLayoutDDL, tc.layout))
			assert.Equal(t, &DictionaryLayoutSpec{Kind: tc.want.Kind(), Decoded: tc.want}, d.Layout)

			out := rtDumpDictHCL(t, d)
			flat := rtFlatWS(out)
			assert.Contains(t, flat, `layout "`+tc.want.Kind()+`" {`)
			for _, frag := range tc.wantHCL {
				assert.Contains(t, flat, frag)
			}

			back, err := decodeLayout(t, out)
			require.NoError(t, err)
			assert.Equal(t, tc.want, back)
		})
	}
}

func TestDictionaryRT_Sources(t *testing.T) {
	cases := []struct {
		name       string
		source     string
		want       DictionarySource
		wantHCL    []string
		wantNotHCL []string
	}{
		{
			name: "clickhouse",
			source: "CLICKHOUSE(HOST 'ch1' PORT 9000 USER 'ro' PASSWORD 's3cret' DB 'default' TABLE 'src'" +
				" INVALIDATE_QUERY 'SELECT max(ts) FROM default.src' UPDATE_FIELD 'ts' UPDATE_LAG 15)",
			want: SourceClickHouse{
				Host: ptr("ch1"), Port: ptr(int64(9000)),
				User: ptr("ro"), Password: ptr("s3cret"),
				DB: ptr("default"), Table: ptr("src"),
				InvalidateQuery: ptr("SELECT max(ts) FROM default.src"),
				UpdateField:     ptr("ts"), UpdateLag: ptr(int64(15)),
			},
			wantHCL: []string{`host = "ch1"`, `port = 9000`, `update_lag = 15`},
		},
		{
			name: "mysql",
			source: "MYSQL(HOST 'my1' PORT 3306 USER 'app' PASSWORD 'pw' DB 'd1' TABLE 't1'" +
				" QUERY 'SELECT * FROM d1.t1')",
			want: SourceMySQL{
				Host: ptr("my1"), Port: ptr(int64(3306)),
				User: ptr("app"), Password: ptr("pw"),
				DB: ptr("d1"), Table: ptr("t1"),
				Query: ptr("SELECT * FROM d1.t1"),
			},
			wantHCL: []string{`port = 3306`, `query = "SELECT * FROM d1.t1"`},
		},
		{
			name:   "postgresql",
			source: "POSTGRESQL(HOST 'pg1' PORT 5432 USER 'app' DB 'd1' TABLE 't1')",
			want: SourcePostgreSQL{
				Host: ptr("pg1"), Port: ptr(int64(5432)),
				User: ptr("app"), DB: ptr("d1"), Table: ptr("t1"),
			},
			wantHCL: []string{`host = "pg1"`, `port = 5432`},
		},
		{
			name: "http",
			source: "HTTP(URL 'https://feeds.example/rates.json' FORMAT 'JSONEachRow'" +
				" CREDENTIALS_USER 'reader' CREDENTIALS_PASSWORD 'pw')",
			want: SourceHTTP{
				URL: "https://feeds.example/rates.json", Format: "JSONEachRow",
				CredentialsUser: ptr("reader"), CredentialsPassword: ptr("pw"),
			},
			wantHCL: []string{
				`url = "https://feeds.example/rates.json"`,
				`format = "JSONEachRow"`,
				`credentials_user = "reader"`,
			},
		},
		{
			name:    "file",
			source:  "FILE(PATH '/var/lib/clickhouse/user_files/data.csv' FORMAT 'CSV')",
			want:    SourceFile{Path: "/var/lib/clickhouse/user_files/data.csv", Format: "CSV"},
			wantHCL: []string{`path = "/var/lib/clickhouse/user_files/data.csv"`, `format = "CSV"`},
		},
		{
			name:    "executable_implicit_key_numeric",
			source:  "EXECUTABLE(COMMAND '/bin/gen' FORMAT 'TabSeparated' IMPLICIT_KEY 1)",
			want:    SourceExecutable{Command: "/bin/gen", Format: "TabSeparated", ImplicitKey: ptr(true)},
			wantHCL: []string{`command = "/bin/gen"`, `implicit_key = true`},
		},
		{
			name:    "executable_implicit_key_string_true",
			source:  "EXECUTABLE(COMMAND '/bin/gen' FORMAT 'TabSeparated' IMPLICIT_KEY 'true')",
			want:    SourceExecutable{Command: "/bin/gen", Format: "TabSeparated", ImplicitKey: ptr(true)},
			wantHCL: []string{`implicit_key = true`},
		},
		{
			name:    "executable_implicit_key_string_false",
			source:  "EXECUTABLE(COMMAND '/bin/gen' FORMAT 'TabSeparated' IMPLICIT_KEY 'false')",
			want:    SourceExecutable{Command: "/bin/gen", Format: "TabSeparated", ImplicitKey: ptr(false)},
			wantHCL: []string{`implicit_key = false`},
		},
		{
			name:       "executable_no_implicit_key",
			source:     "EXECUTABLE(COMMAND '/bin/gen' FORMAT 'TabSeparated')",
			want:       SourceExecutable{Command: "/bin/gen", Format: "TabSeparated"},
			wantNotHCL: []string{`implicit_key`},
		},
		{
			name:   "null",
			source: "NULL()",
			want:   SourceNull{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := rtDictFromDDL(t, fmt.Sprintf(rtDictSourceDDL, tc.source))
			assert.Equal(t, &DictionarySourceSpec{Kind: tc.want.Kind(), Decoded: tc.want}, d.Source)

			out := rtDumpDictHCL(t, d)
			flat := rtFlatWS(out)
			assert.Contains(t, flat, `source "`+tc.want.Kind()+`" {`)
			for _, frag := range tc.wantHCL {
				assert.Contains(t, flat, frag)
			}
			for _, frag := range tc.wantNotHCL {
				assert.NotContains(t, flat, frag)
			}

			back, err := decodeSource(t, out)
			require.NoError(t, err)
			assert.Equal(t, tc.want, back)
		})
	}
}

func TestDictionaryRT_CacheLayoutMissingSizeInCells(t *testing.T) {
	cases := []struct {
		name    string
		layout  string
		wantErr string
	}{
		{"cache", "CACHE()", "layout cache: missing size_in_cells"},
		{"complex_key_cache", "COMPLEX_KEY_CACHE()", "layout complex_key_cache: missing size_in_cells"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildDictionaryFromCreateSQL(fmt.Sprintf(rtDictLayoutDDL, tc.layout))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// Non-numeric / non-boolean arg values are dropped to nil rather than
// failing introspection; the whole-struct asserts pin that down.
func TestDictionaryRT_ScalarArgFallbacks(t *testing.T) {
	cases := []struct {
		name   string
		source string
		want   DictionarySource
	}{
		{
			name:   "port_non_numeric",
			source: "CLICKHOUSE(HOST 'h' PORT 'nan')",
			want:   SourceClickHouse{Host: ptr("h")},
		},
		{
			name:   "update_lag_non_numeric",
			source: "MYSQL(HOST 'h' UPDATE_LAG 'soon')",
			want:   SourceMySQL{Host: ptr("h")},
		},
		{
			name:   "implicit_key_unrecognized",
			source: "EXECUTABLE(COMMAND '/bin/gen' FORMAT 'CSV' IMPLICIT_KEY 'maybe')",
			want:   SourceExecutable{Command: "/bin/gen", Format: "CSV"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := rtDictFromDDL(t, fmt.Sprintf(rtDictSourceDDL, tc.source))
			assert.Equal(t, &DictionarySourceSpec{Kind: tc.want.Kind(), Decoded: tc.want}, d.Source)
		})
	}
}
