package hcl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Issue #136, column-type case. ClickHouse stores an Enum in
// create_table_query with spaces around '=' (`Enum8('a' = 1)`), while the
// printer every introspected type is rendered through emits the compact
// `Enum8('a'=1)`. Unless the load path canonicalizes types through that same
// printer, an authored Enum can never string-match its introspected form and
// the object diffs forever — as a no-op MODIFY COLUMN on a table, an
// unappliable Recreate on a materialized view, and a whole-object rewrite on a
// dictionary. These tests pin the round trip for every object kind that
// carries a type, because canonicalize covering one kind and missing another
// is exactly how this bug survived.

// enumRoundTripHCL declares one Enum column in each place a type can be
// authored: a table column, an MV's explicit column list, and a dictionary
// attribute.
const enumRoundTripHCL = `
database "db" {
  table "dst" {
    column "k"  { type = "UInt64" }
    column "st" { type = "Enum8('a' = 1, 'b' = 2)" }

    engine "merge_tree" {}
    order_by = ["k"]
  }

  materialized_view "mv" {
    to_table = "db.dst"

    column "k"  { type = "UInt64" }
    column "st" { type = "Enum8('a' = 1, 'b' = 2)" }

    query = "SELECT k, st FROM db.dst"
  }

  dictionary "dic" {
    primary_key = ["k"]

    attribute "k"  { type = "UInt64" }
    attribute "st" { type = "Enum8('a' = 1, 'b' = 2)" }

    source "clickhouse" { table = "dst" }
    layout "flat" {}
    lifetime { min = 0 }
  }
}
`

// enumRoundTripDDL is what ClickHouse returns for those three objects —
// byte-for-byte the spaced Enum form, which is the whole problem.
var enumRoundTripDDL = []fakeRow{
	{
		name: "dst",
		sql: "CREATE TABLE db.dst (`k` UInt64, `st` Enum8('a' = 1, 'b' = 2)) " +
			"ENGINE = MergeTree ORDER BY k",
	},
	{
		name: "mv",
		sql: "CREATE MATERIALIZED VIEW db.mv TO db.dst " +
			"(`k` UInt64, `st` Enum8('a' = 1, 'b' = 2)) AS SELECT k, st FROM db.dst",
	},
	{
		name: "dic",
		sql: "CREATE DICTIONARY db.dic (`k` UInt64, `st` Enum8('a' = 1, 'b' = 2)) " +
			"PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'dst')) LIFETIME(0) LAYOUT(FLAT())",
	},
}

func loadEnumRoundTripSchema(t *testing.T) *Schema {
	t.Helper()
	path := filepath.Join(t.TempDir(), "schema.hcl")
	require.NoError(t, os.WriteFile(path, []byte(enumRoundTripHCL), 0o644))
	declared, err := LoadLayers([]string{path})
	require.NoError(t, err)
	return declared
}

// A table column, an MV column list, and a dictionary attribute holding the
// same authored Enum must all diff clean against the live cluster.
func TestDiff_EnumTypes_RoundTripCleanForEveryObjectKind(t *testing.T) {
	introspected := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRows(introspected, "db", &fakeRows{rows: enumRoundTripDDL}))

	cs := Diff(&Schema{Databases: []DatabaseSpec{*introspected}}, loadEnumRoundTripSchema(t))
	assert.True(t, cs.IsEmpty(),
		"authored Enum types must round-trip without drift, got %+v", cs.Databases)
	assert.Empty(t, GenerateSQL(cs).Unsafe,
		"an Enum-only difference must not surface as an unappliable MV recreate")
}

// The load path must canonicalize a type wherever it can be authored. Asserted
// on the parsed schema (not only through a diff) so a future object kind that
// grows a type field fails here rather than silently drifting in production.
func TestParseFile_CanonicalizesTypesEverywhereTheyAreAuthored(t *testing.T) {
	const src = `
database "db" {
  table "base" {
    abstract = true
    column "st" { type = "Enum8('a' = 1)" }
  }

  table "t" {
    extend = "base"
    patch_column "st" { type = "Enum8('a' = 1, 'b' = 2)" }

    engine "merge_tree" {}
    order_by = ["st"]
  }

  patch_table "t" {
    column "extra" { type = "Enum8('c' = 3)" }
  }

  materialized_view "mv" {
    to_table = "t"
    column "st" { type = "Enum8('a' = 1)" }
    query = "SELECT st FROM db.t"
  }

  patch_materialized_view "mv" {
    column "extra" { type = "Enum8('c' = 3)" }
  }

  dictionary "dic" {
    primary_key = ["k"]
    attribute "k"  { type = "UInt64" }
    attribute "st" { type = "Enum8('a' = 1)" }
    source "clickhouse" { table = "t" }
    layout "flat" {}
    lifetime { min = 0 }
  }
}
`
	path := filepath.Join(t.TempDir(), "schema.hcl")
	require.NoError(t, os.WriteFile(path, []byte(src), 0o644))
	s, err := ParseFile(path)
	require.NoError(t, err)
	db := s.Databases[0]

	assert.Equal(t, "Enum8('a'=1)", db.Tables[0].Columns[0].Type, "abstract table column")
	require.NotNil(t, db.Tables[1].ColumnPatches[0].Type)
	assert.Equal(t, "Enum8('a'=1, 'b'=2)", *db.Tables[1].ColumnPatches[0].Type, "patch_column")
	assert.Equal(t, "Enum8('c'=3)", db.Patches[0].Columns[0].Type, "patch_table column")
	assert.Equal(t, "Enum8('a'=1)", db.MaterializedViews[0].Columns[0].Type, "materialized_view column")
	assert.Equal(t, "Enum8('c'=3)", db.MaterializedViewPatches[0].Columns[0].Type, "patch_materialized_view column")
	assert.Equal(t, "Enum8('a'=1)", db.Dictionaries[0].Attributes[1].Type, "dictionary attribute")
}

// A TimeSeries engine's inner column list is the one type-bearing field that
// lives inside an engine block, reachable only through EngineSpec.Decoded —
// which is why canonicalization runs after the engines are decoded.
func TestParseFile_CanonicalizesTimeSeriesInnerColumnTypes(t *testing.T) {
	const src = `
database "db" {
  table "ts" {
    engine "time_series" {
      tags {
        inner {
          column "kind" { type = "Enum8('counter' = 1, 'gauge' = 2)" }
          engine "merge_tree" {}
          order_by = ["kind"]
        }
      }
    }
  }
}
`
	path := filepath.Join(t.TempDir(), "schema.hcl")
	require.NoError(t, os.WriteFile(path, []byte(src), 0o644))
	s, err := ParseFile(path)
	require.NoError(t, err)

	ts, ok := s.Databases[0].Tables[0].Engine.Decoded.(EngineTimeSeries)
	require.True(t, ok, "engine must decode to EngineTimeSeries")
	require.NotNil(t, ts.Tags)
	require.NotNil(t, ts.Tags.Inner)
	assert.Equal(t, "Enum8('counter'=1, 'gauge'=2)", ts.Tags.Inner.Columns[0].Type)
}
