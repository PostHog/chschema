package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScopeSchemaToObjects_AllKindsAndLogicalRawIdentity(t *testing.T) {
	source := &Schema{
		Databases: []DatabaseSpec{{
			Name:              "db",
			Tables:            []TableSpec{{Name: "keep_table"}, {Name: "drop_table"}, {Name: "raw_to_model"}},
			MaterializedViews: []MaterializedViewSpec{{Name: "keep_mv"}, {Name: "drop_mv"}},
			Views:             []ViewSpec{{Name: "keep_view"}, {Name: "drop_view"}},
			Dictionaries:      []DictionarySpec{{Name: "keep_dict"}, {Name: "drop_dict"}},
			Raws: []RawSpec{
				{Kind: KindTable, Name: "model_to_raw"},
				{Kind: KindView, Name: "drop_raw"},
			},
		}},
		NamedCollections: []NamedCollectionSpec{{Name: "keep_nc"}, {Name: "drop_nc"}},
		Nodes:            []NodeSpec{{Name: "node-1", Macros: map[string]string{"role": "ops"}}},
	}
	scope := &Schema{
		Databases: []DatabaseSpec{{
			Name:              "db",
			Tables:            []TableSpec{{Name: "keep_table"}, {Name: "model_to_raw"}},
			MaterializedViews: []MaterializedViewSpec{{Name: "keep_mv"}},
			Views:             []ViewSpec{{Name: "keep_view"}},
			Dictionaries:      []DictionarySpec{{Name: "keep_dict"}},
			Raws:              []RawSpec{{Kind: KindTable, Name: "raw_to_model"}},
		}},
		NamedCollections: []NamedCollectionSpec{{Name: "keep_nc", External: true}},
	}

	got := ScopeSchemaToObjects(source, scope)
	require.Len(t, got.Databases, 1)
	db := got.Databases[0]
	assert.Equal(t, []string{"keep_table", "raw_to_model"}, tableNames(db.Tables))
	assert.Equal(t, []string{"keep_mv"}, materializedViewNames(db.MaterializedViews))
	assert.Equal(t, []string{"keep_view"}, viewNames(db.Views))
	assert.Equal(t, []string{"keep_dict"}, dictionaryNames(db.Dictionaries))
	require.Len(t, db.Raws, 1)
	assert.Equal(t, "model_to_raw", db.Raws[0].Name)
	require.Len(t, got.NamedCollections, 1)
	assert.Equal(t, "keep_nc", got.NamedCollections[0].Name)
	assert.Equal(t, source.Nodes, got.Nodes)
}

func TestScopeSchemaToObjects_DoesNotMutateOrAliasSlices(t *testing.T) {
	source := &Schema{
		Databases:        []DatabaseSpec{{Name: "db", Tables: []TableSpec{{Name: "a"}, {Name: "b"}}}},
		NamedCollections: []NamedCollectionSpec{{Name: "nc"}},
		Nodes:            []NodeSpec{{Name: "node"}},
	}
	scope := &Schema{Databases: []DatabaseSpec{{Name: "db", Tables: []TableSpec{{Name: "a"}}}}}

	got := ScopeSchemaToObjects(source, scope)
	require.Len(t, got.Databases[0].Tables, 1)
	got.Databases[0].Tables[0].Name = "changed"
	got.Nodes[0].Name = "changed-node"

	assert.Equal(t, []string{"a", "b"}, tableNames(source.Databases[0].Tables))
	assert.Equal(t, "node", source.Nodes[0].Name)
	assert.Equal(t, "a", scope.Databases[0].Tables[0].Name)
	assert.Len(t, source.NamedCollections, 1)
}

func TestScopeSchemaToObjects_NilSourcePreservesEmptyContract(t *testing.T) {
	got := ScopeSchemaToObjects(nil, &Schema{})
	require.NotNil(t, got)
	assert.Empty(t, got.Databases)
	assert.Empty(t, got.NamedCollections)
}

func tableNames(in []TableSpec) []string {
	out := make([]string, len(in))
	for i := range in {
		out[i] = in[i].Name
	}
	return out
}

func materializedViewNames(in []MaterializedViewSpec) []string {
	out := make([]string, len(in))
	for i := range in {
		out[i] = in[i].Name
	}
	return out
}

func viewNames(in []ViewSpec) []string {
	out := make([]string, len(in))
	for i := range in {
		out[i] = in[i].Name
	}
	return out
}

func dictionaryNames(in []DictionarySpec) []string {
	out := make([]string, len(in))
	for i := range in {
		out[i] = in[i].Name
	}
	return out
}
