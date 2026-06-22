package hcl

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sortTables sorts each database's tables alphabetically so two resolved
// schemas can be compared without table-order skew.
func sortTables(dbs []DatabaseSpec) {
	for di := range dbs {
		sort.Slice(dbs[di].Tables, func(i, j int) bool {
			return dbs[di].Tables[i].Name < dbs[di].Tables[j].Name
		})
	}
}

func sortNamedCollections(ncs []NamedCollectionSpec) {
	sort.Slice(ncs, func(i, j int) bool { return ncs[i].Name < ncs[j].Name })
}

// roundTrip parses, resolves, dumps, parses, resolves again. The before and
// after schemas (with engine bodies cleared) must compare equal.
func roundTrip(t *testing.T, file string) {
	t.Helper()

	before, err := ParseFile(file)
	require.NoError(t, err)
	require.NoError(t, Resolve(before))

	var buf bytes.Buffer
	require.NoError(t, Write(&buf, before))

	tmp := filepath.Join(t.TempDir(), "round_trip.hcl")
	require.NoError(t, os.WriteFile(tmp, buf.Bytes(), 0o644))

	after, err := ParseFile(tmp)
	require.NoError(t, err, "re-parse failed; dump output:\n%s", buf.String())
	require.NoError(t, Resolve(after))

	stripEngineBodies(before.Databases)
	stripEngineBodies(after.Databases)
	sortTables(before.Databases)
	sortTables(after.Databases)
	sortNamedCollections(before.NamedCollections)
	sortNamedCollections(after.NamedCollections)

	assert.Equal(t, before, after, "round-trip mismatch; dump output:\n%s", buf.String())
}

func TestWrite_RoundTrip_BasicResolve(t *testing.T) {
	roundTrip(t, filepath.Join("testdata", "resolve_basic.hcl"))
}

func TestWrite_RoundTrip_AllEngineKinds(t *testing.T) {
	roundTrip(t, filepath.Join("testdata", "engines_all_kinds.hcl"))
}

func TestWrite_RoundTrip_FullTable(t *testing.T) {
	roundTrip(t, filepath.Join("testdata", "dump_round_trip_full.hcl"))
}

func TestWrite_RoundTrip_MaterializedView(t *testing.T) {
	roundTrip(t, filepath.Join("testdata", "materialized_view.hcl"))
}

func TestWrite_RoundTrip_Dictionary(t *testing.T) {
	roundTrip(t, filepath.Join("testdata", "dictionary.hcl"))
}

func TestWrite_RoundTrip_RawBlock(t *testing.T) {
	roundTrip(t, filepath.Join("testdata", "raw_block.hcl"))
}

// TestWrite_RoundTrip_IntrospectedColumnModifiers exercises the exact seam from
// issue #45: ClickHouse DDL parsed by the introspection AST builder must survive
// the dump → reparse round-trip with all column modifiers, primary key, table
// comment, and constraints intact. It complements the file-based round-trip
// (which starts from HCL) by starting from a CREATE TABLE statement, the way
// `hclexp introspect` does.
func TestWrite_RoundTrip_IntrospectedColumnModifiers(t *testing.T) {
	const createSQL = `CREATE TABLE migration_test.query_log_archive (
		query_id String,
		exception_code Int32,
		exception_name String ALIAS errorCodeToName(exception_code),
		created_at DateTime DEFAULT now() COMMENT 'ingestion time' CODEC(Delta, ZSTD(1)) TTL created_at + toIntervalYear(1),
		lc_team_id Int64,
		team_id Int64 ALIAS lc_team_id,
		team_id_doubled Int64 MATERIALIZED lc_team_id * 2,
		CONSTRAINT team_id_positive CHECK lc_team_id > 0
	) ENGINE = MergeTree PRIMARY KEY query_id ORDER BY (query_id, lc_team_id) COMMENT 'query log archive'`

	spec, err := buildTableFromCreateSQL(createSQL)
	require.NoError(t, err)
	spec.Name = "query_log_archive" // name is assigned by the introspect caller

	// Sanity: the parser captured the modifiers (the bug is in the dumper).
	byName := map[string]ColumnSpec{}
	for _, c := range spec.Columns {
		byName[c.Name] = c
	}
	require.NotNil(t, byName["exception_name"].Alias, "parser should capture ALIAS")
	require.NotNil(t, byName["team_id_doubled"].Materialized, "parser should capture MATERIALIZED")

	before := &Schema{Databases: []DatabaseSpec{{Name: "migration_test", Tables: []TableSpec{spec}}}}
	require.NoError(t, Resolve(before))

	var buf bytes.Buffer
	require.NoError(t, Write(&buf, before))

	tmp := filepath.Join(t.TempDir(), "introspect_round_trip.hcl")
	require.NoError(t, os.WriteFile(tmp, buf.Bytes(), 0o644))
	after, err := ParseFile(tmp)
	require.NoError(t, err, "re-parse failed; dump output:\n%s", buf.String())
	require.NoError(t, Resolve(after))

	stripEngineBodies(before.Databases)
	stripEngineBodies(after.Databases)
	assert.Equal(t, before, after, "introspect → dump → reparse lost data; dump output:\n%s", buf.String())
}

func TestWrite_OutputIsStable(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "resolve_basic.hcl"))
	require.NoError(t, err)
	require.NoError(t, Resolve(dbs))

	var a, b bytes.Buffer
	require.NoError(t, Write(&a, dbs))
	require.NoError(t, Write(&b, dbs))
	assert.Equal(t, a.String(), b.String(), "dump output should be deterministic")
}

func TestWrite_RoundTrip_NamedCollection(t *testing.T) {
	roundTrip(t, filepath.Join("testdata", "named_collection.hcl"))
}

func TestWrite_RoundTrip_KafkaWithCollection(t *testing.T) {
	roundTrip(t, filepath.Join("testdata", "kafka_with_collection.hcl"))
}

func TestWrite_RoundTrip_KafkaInlineSettings(t *testing.T) {
	roundTrip(t, filepath.Join("testdata", "kafka_inline_settings.hcl"))
}

func TestWrite_RoundTrip_NodeMacros(t *testing.T) {
	roundTrip(t, filepath.Join("testdata", "node_macros.hcl"))
}

func TestDumpSchema_RendersNodeBlock(t *testing.T) {
	want := NodeSpec{
		Name: "prod-us-iad-ch-1d-ops",
		Macros: map[string]string{
			"shard":           "1",
			"replica":         "d",
			"hostClusterRole": "ops",
			"hostClusterType": "online",
		},
	}
	s := &Schema{Nodes: []NodeSpec{want}}

	var buf bytes.Buffer
	require.NoError(t, Write(&buf, s))

	got := buf.String()
	assert.Contains(t, got, `node "prod-us-iad-ch-1d-ops" {`)

	tmp := filepath.Join(t.TempDir(), "node.hcl")
	require.NoError(t, os.WriteFile(tmp, buf.Bytes(), 0o600))
	reparsed, err := ParseFile(tmp)
	require.NoError(t, err)
	require.Len(t, reparsed.Nodes, 1)
	assert.Equal(t, want, reparsed.Nodes[0])
}

func TestDumpSchema_RendersViewBlocks(t *testing.T) {
	want := ViewSpec{
		Name:          "metrics",
		Query:         "SELECT team_id, count() AS n FROM events GROUP BY team_id",
		ColumnAliases: []string{"team_id", "n"},
		SQLSecurity:   ptr("definer"),
		Definer:       ptr("alice"),
		Comment:       ptr("team-level event counter"),
	}
	s := &Schema{Databases: []DatabaseSpec{{Name: "posthog", Views: []ViewSpec{want}}}}

	var buf bytes.Buffer
	require.NoError(t, Write(&buf, s))

	// Output should contain the block header. The remaining attributes
	// are alignment-formatted by hclwrite, so we round-trip back through
	// ParseFile to assert structural equivalence.
	got := buf.String()
	assert.Contains(t, got, `view "metrics" {`)

	tmp := filepath.Join(t.TempDir(), "schema.hcl")
	require.NoError(t, os.WriteFile(tmp, buf.Bytes(), 0o600))
	reparsed, err := ParseFile(tmp)
	require.NoError(t, err)
	require.NoError(t, Resolve(reparsed))
	require.Len(t, reparsed.Databases, 1)
	require.Len(t, reparsed.Databases[0].Views, 1)
	assert.Equal(t, want, reparsed.Databases[0].Views[0])
}
