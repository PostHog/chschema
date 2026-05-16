package hcl

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stripEngineBodies clears the opaque hcl.Body on every engine so resolved
// structures can be compared whole. Decoded values remain.
func stripEngineBodies(dbs []DatabaseSpec) {
	for di := range dbs {
		for ti := range dbs[di].Tables {
			tbl := &dbs[di].Tables[ti]
			if tbl.Engine != nil {
				tbl.Engine.Body = nil
			}
		}
		for i := range dbs[di].Dictionaries {
			d := &dbs[di].Dictionaries[i]
			if d.Source != nil {
				d.Source.Body = nil
			}
			if d.Layout != nil {
				d.Layout.Body = nil
			}
		}
	}
}

func TestResolve_BasicHappyPath(t *testing.T) {
	schema, err := ParseFile(filepath.Join("testdata", "resolve_basic.hcl"))
	require.NoError(t, err)
	require.NoError(t, Resolve(schema))
	stripEngineBodies(schema.Databases)

	rmtEngine := &EngineSpec{
		Kind: "replicated_merge_tree",
		Decoded: EngineReplicatedMergeTree{
			ZooPath:     "/clickhouse/tables/{shard}/events_local",
			ReplicaName: "{replica}",
		},
	}
	commonCols := []ColumnSpec{
		{Name: "timestamp", Type: "DateTime"},
		{Name: "team_id", Type: "UInt64"},
		{Name: "event", Type: "String"},
	}

	expected := []DatabaseSpec{
		{
			Name: "posthog",
			Tables: []TableSpec{
				{
					Name:    "events_local",
					OrderBy: []string{"timestamp", "team_id"},
					Columns: commonCols,
					Engine:  rmtEngine,
				},
				{
					Name:    "events_by_team",
					OrderBy: []string{"team_id", "timestamp"},
					Columns: commonCols,
					Engine:  rmtEngine,
				},
				{
					Name:    "events_distributed",
					OrderBy: []string{"timestamp", "team_id"},
					Columns: commonCols,
					Engine: &EngineSpec{
						Kind: "distributed",
						Decoded: EngineDistributed{
							ClusterName:    "posthog",
							RemoteDatabase: "default",
							RemoteTable:    "events_local",
						},
					},
				},
			},
		},
	}
	assert.Equal(t, expected, schema.Databases)
}

func TestResolve_Cycle(t *testing.T) {
	schema, err := ParseFile(filepath.Join("testdata", "resolve_cycle.hcl"))
	require.NoError(t, err)
	err = Resolve(schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle")
}

func TestResolve_SelfCycle(t *testing.T) {
	schema, err := ParseFile(filepath.Join("testdata", "resolve_self_cycle.hcl"))
	require.NoError(t, err)
	err = Resolve(schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle")
}

func TestResolve_MissingParent(t *testing.T) {
	schema, err := ParseFile(filepath.Join("testdata", "resolve_missing_parent.hcl"))
	require.NoError(t, err)
	err = Resolve(schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does_not_exist")
}

func TestResolve_ColumnCollision(t *testing.T) {
	schema, err := ParseFile(filepath.Join("testdata", "resolve_column_collision.hcl"))
	require.NoError(t, err)
	err = Resolve(schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "collides")
}

func TestResolve_NoEngineOnNonAbstract(t *testing.T) {
	schema, err := ParseFile(filepath.Join("testdata", "resolve_no_engine.hcl"))
	require.NoError(t, err)
	err = Resolve(schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "engine")
}

func TestResolve_Dictionary_RequiresSourceLayoutPrimaryKey(t *testing.T) {
	cases := []struct {
		name    string
		dict    DictionarySpec
		errSubs string
	}{
		{
			name:    "missing source",
			dict:    DictionarySpec{Name: "d", PrimaryKey: []string{"k"}, Layout: &DictionaryLayoutSpec{Kind: "hashed", Decoded: LayoutHashed{}}},
			errSubs: "source",
		},
		{
			name:    "missing layout",
			dict:    DictionarySpec{Name: "d", PrimaryKey: []string{"k"}, Source: &DictionarySourceSpec{Kind: "null", Decoded: SourceNull{}}},
			errSubs: "layout",
		},
		{
			name:    "missing primary_key",
			dict:    DictionarySpec{Name: "d", Source: &DictionarySourceSpec{Kind: "null", Decoded: SourceNull{}}, Layout: &DictionaryLayoutSpec{Kind: "hashed", Decoded: LayoutHashed{}}},
			errSubs: "primary_key",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dbs := []DatabaseSpec{{Name: "db", Dictionaries: []DictionarySpec{tc.dict}}}
			err := Resolve(&Schema{Databases: dbs})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errSubs)
		})
	}
}

func TestResolve_Dictionary_RangeOnlyForRangeLayouts(t *testing.T) {
	dbs := []DatabaseSpec{{Name: "db", Dictionaries: []DictionarySpec{{
		Name:       "d",
		PrimaryKey: []string{"k"},
		Source:     &DictionarySourceSpec{Kind: "null", Decoded: SourceNull{}},
		Layout:     &DictionaryLayoutSpec{Kind: "hashed", Decoded: LayoutHashed{}},
		Range:      &DictionaryRange{Min: "a", Max: "b"},
	}}}}
	err := Resolve(&Schema{Databases: dbs})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "range")
	assert.Contains(t, err.Error(), "hashed")
}

func TestResolve_NamedCollection_Validation(t *testing.T) {
	cases := []struct {
		name    string
		schema  *Schema
		errSubs string
	}{
		{
			name: "duplicate names",
			schema: &Schema{NamedCollections: []NamedCollectionSpec{
				{Name: "x", Params: []NamedCollectionParam{{Key: "a", Value: "1"}}},
				{Name: "x", Params: []NamedCollectionParam{{Key: "b", Value: "2"}}},
			}},
			errSubs: "duplicate",
		},
		{
			name: "duplicate param keys",
			schema: &Schema{NamedCollections: []NamedCollectionSpec{
				{Name: "x", Params: []NamedCollectionParam{
					{Key: "a", Value: "1"},
					{Key: "a", Value: "2"},
				}},
			}},
			errSubs: "duplicate param",
		},
		{
			name: "empty params on managed NC",
			schema: &Schema{NamedCollections: []NamedCollectionSpec{
				{Name: "x"},
			}},
			errSubs: "non-empty",
		},
		{
			name: "empty params allowed on external NC",
			schema: &Schema{NamedCollections: []NamedCollectionSpec{
				{Name: "x", External: true},
			}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Resolve(tc.schema)
			if tc.errSubs == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errSubs)
			}
		})
	}
}

func TestResolve_KafkaEngine_XOR(t *testing.T) {
	mkTblWithKafka := func(eng EngineKafka) *Schema {
		return &Schema{Databases: []DatabaseSpec{{
			Name: "db",
			Tables: []TableSpec{{
				Name:    "t",
				Columns: []ColumnSpec{{Name: "id", Type: "UInt64"}},
				OrderBy: []string{"id"},
				Engine: &EngineSpec{
					Kind:    "kafka",
					Decoded: eng,
				},
			}},
		}}}
	}
	cases := []struct {
		name    string
		eng     EngineKafka
		errSubs string
		setupNC bool
	}{
		{
			name:    "neither collection nor inline",
			eng:     EngineKafka{},
			errSubs: "requires either",
		},
		{
			name:    "collection AND inline broker_list",
			eng:     EngineKafka{Collection: ptr("nc1"), BrokerList: ptr("k:9092")},
			errSubs: "mutually exclusive",
			setupNC: true,
		},
		{
			name:    "collection AND extra",
			eng:     EngineKafka{Collection: ptr("nc1"), Extra: map[string]string{"kafka_x": "y"}},
			errSubs: "mutually exclusive",
			setupNC: true,
		},
		{
			name:    "inline missing topic_list",
			eng:     EngineKafka{BrokerList: ptr("k:9092"), GroupName: ptr("g"), Format: ptr("JSONEachRow")},
			errSubs: "topic_list",
		},
		{
			name: "valid inline",
			eng: EngineKafka{
				BrokerList: ptr("k:9092"),
				TopicList:  ptr("events"),
				GroupName:  ptr("g"),
				Format:     ptr("JSONEachRow"),
			},
		},
		{
			name:    "valid collection",
			eng:     EngineKafka{Collection: ptr("nc1")},
			setupNC: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mkTblWithKafka(tc.eng)
			if tc.setupNC {
				s.NamedCollections = []NamedCollectionSpec{{
					Name:   "nc1",
					Params: []NamedCollectionParam{{Key: "kafka_broker_list", Value: "k:9092"}},
				}}
			}
			err := Resolve(s)
			if tc.errSubs == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errSubs)
			}
		})
	}
}

func TestResolve_KafkaCollectionReference(t *testing.T) {
	s := &Schema{Databases: []DatabaseSpec{{
		Name: "db",
		Tables: []TableSpec{{
			Name:    "t",
			Columns: []ColumnSpec{{Name: "id", Type: "UInt64"}},
			OrderBy: []string{"id"},
			Engine: &EngineSpec{
				Kind:    "kafka",
				Decoded: EngineKafka{Collection: ptr("undeclared_nc")},
			},
		}},
	}}}
	err := Resolve(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "undeclared_nc")
	assert.Contains(t, err.Error(), "not declared")
}
