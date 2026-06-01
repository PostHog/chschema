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

// mvExtendBase returns the canonical four-tier schema used by the MV-extend
// tests: an abstract base table, three concrete tables (Kafka/local/Distributed)
// extending it, and an MV that also extends it. Built in memory so individual
// tests can mutate single fields to exercise edge cases without copy-paste.
func mvExtendBase() *Schema {
	cols := []ColumnSpec{
		{Name: "timestamp", Type: "DateTime64(3)"},
		{Name: "team_id", Type: "UInt32"},
		{Name: "event", Type: "String"},
	}
	return &Schema{Databases: []DatabaseSpec{{
		Name: "default",
		Tables: []TableSpec{
			{Name: "events_base", Abstract: true, Columns: cols},
			{
				Name:    "events_local",
				Extend:  ptr("events_base"),
				OrderBy: []string{"team_id", "timestamp"},
				Engine: &EngineSpec{
					Kind: "replicated_merge_tree",
					Decoded: EngineReplicatedMergeTree{
						ZooPath:     "/clickhouse/tables/{shard}/events",
						ReplicaName: "{replica}",
					},
				},
			},
		},
		MaterializedViews: []MaterializedViewSpec{{
			Name:    "events_mv",
			Extend:  ptr("events_base"),
			ToTable: "events_local",
			Query:   "SELECT timestamp, team_id, event FROM events_kafka",
		}},
	}}}
}

func TestResolve_MVExtendsAbstractTable(t *testing.T) {
	schema, err := ParseFile(filepath.Join("testdata", "resolve_mv_extend.hcl"))
	require.NoError(t, err)
	require.NoError(t, Resolve(schema))

	require.Len(t, schema.Databases, 1)
	db := schema.Databases[0]

	// Abstract base is dropped; three concrete tables remain.
	assert.Equal(t, []string{"events_kafka", "events_local", "events_distributed"},
		[]string{db.Tables[0].Name, db.Tables[1].Name, db.Tables[2].Name})
	require.Len(t, db.MaterializedViews, 1)
	mv := db.MaterializedViews[0]
	assert.Equal(t, "events_mv", mv.Name)
	assert.Nil(t, mv.Extend, "extend should be cleared after resolution")
	assert.Equal(t, []ColumnSpec{
		{Name: "timestamp", Type: "DateTime64(3)"},
		{Name: "team_id", Type: "UInt32"},
		{Name: "event", Type: "String"},
		{Name: "properties", Type: "String", Codec: ptr("ZSTD(3)")},
	}, mv.Columns)
	// db.Cluster cascades into the MV.
	require.NotNil(t, mv.Cluster)
	assert.Equal(t, "main", *mv.Cluster)
}

func TestResolve_MVExtendsAbstract_WithExtraColumns(t *testing.T) {
	s := mvExtendBase()
	mv := &s.Databases[0].MaterializedViews[0]
	mv.Columns = []ColumnSpec{{Name: "extra", Type: "UInt8"}}
	require.NoError(t, Resolve(s))
	assert.Equal(t, []ColumnSpec{
		{Name: "timestamp", Type: "DateTime64(3)"},
		{Name: "team_id", Type: "UInt32"},
		{Name: "event", Type: "String"},
		{Name: "extra", Type: "UInt8"},
	}, s.Databases[0].MaterializedViews[0].Columns)
}

func TestResolve_MVExtendsAbstract_ColumnCollision(t *testing.T) {
	s := mvExtendBase()
	mv := &s.Databases[0].MaterializedViews[0]
	mv.Columns = []ColumnSpec{{Name: "team_id", Type: "UInt64"}}
	err := Resolve(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "team_id")
	assert.Contains(t, err.Error(), "collides")
}

func TestResolve_MVExtendsConcreteTable_Rejected(t *testing.T) {
	s := mvExtendBase()
	mv := &s.Databases[0].MaterializedViews[0]
	mv.Extend = ptr("events_local")
	err := Resolve(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be an abstract table")
}

func TestResolve_MVExtendsUnknownName(t *testing.T) {
	s := mvExtendBase()
	mv := &s.Databases[0].MaterializedViews[0]
	mv.Extend = ptr("does_not_exist")
	err := Resolve(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does_not_exist")
}

func TestResolve_MVExtendsMV_Rejected(t *testing.T) {
	s := mvExtendBase()
	db := &s.Databases[0]
	db.MaterializedViews = append(db.MaterializedViews, MaterializedViewSpec{
		Name:    "events_mv_2",
		Extend:  ptr("events_mv"),
		ToTable: "events_local",
		Query:   "SELECT * FROM events_kafka",
	})
	err := Resolve(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot extend another materialized_view")
}

func TestResolve_AbstractMV_Dropped(t *testing.T) {
	s := mvExtendBase()
	db := &s.Databases[0]
	db.MaterializedViews[0] = MaterializedViewSpec{
		Name:     "events_mv_template",
		Abstract: true,
		Columns:  []ColumnSpec{{Name: "x", Type: "UInt8"}},
	}
	require.NoError(t, Resolve(s))
	assert.Empty(t, s.Databases[0].MaterializedViews, "abstract MV should be dropped")
}

func TestResolve_NonAbstractMV_RequiresToTable(t *testing.T) {
	s := mvExtendBase()
	mv := &s.Databases[0].MaterializedViews[0]
	mv.ToTable = ""
	err := Resolve(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires to_table")
}

func TestResolve_NonAbstractMV_RequiresQuery(t *testing.T) {
	s := mvExtendBase()
	mv := &s.Databases[0].MaterializedViews[0]
	mv.Query = ""
	err := Resolve(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires query")
}

func TestResolve_DatabaseClusterCascadesToMV(t *testing.T) {
	s := mvExtendBase()
	s.Databases[0].Cluster = ptr("posthog")
	// MV does not extend, does not set its own cluster.
	s.Databases[0].MaterializedViews[0].Extend = nil
	s.Databases[0].MaterializedViews[0].Cluster = nil
	require.NoError(t, Resolve(s))
	mv := s.Databases[0].MaterializedViews[0]
	require.NotNil(t, mv.Cluster)
	assert.Equal(t, "posthog", *mv.Cluster)
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

func TestResolve_TimeSeries_BothExternalAndInnerOnSameTarget_Rejected(t *testing.T) {
	tgt := "db.x"
	e := EngineTimeSeries{Samples: &TimeSeriesTarget{
		Target: &tgt,
		Inner:  &TimeSeriesInnerTable{Engine: &EngineSpec{Decoded: EngineMergeTree{}}},
	}}
	s := &Schema{Databases: []DatabaseSpec{{
		Name: "db",
		Tables: []TableSpec{{Name: "m",
			Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
			Engine:  &EngineSpec{Kind: "time_series", Decoded: e},
		}},
	}}}
	err := Resolve(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "samples")
	assert.Contains(t, err.Error(), "either")
}

func TestResolve_TimeSeries_NeitherForm_Rejected(t *testing.T) {
	e := EngineTimeSeries{Samples: &TimeSeriesTarget{}}
	s := &Schema{Databases: []DatabaseSpec{{
		Name: "db",
		Tables: []TableSpec{{Name: "m",
			Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
			Engine:  &EngineSpec{Kind: "time_series", Decoded: e},
		}},
	}}}
	err := Resolve(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "samples")
	assert.Contains(t, err.Error(), "target or inner")
}

func TestResolve_TimeSeries_InnerEngineRestrictedToMergeTreeFamily(t *testing.T) {
	e := EngineTimeSeries{Samples: &TimeSeriesTarget{
		Inner: &TimeSeriesInnerTable{Engine: &EngineSpec{Decoded: EngineKafka{}}},
	}}
	s := &Schema{Databases: []DatabaseSpec{{
		Name: "db",
		Tables: []TableSpec{{Name: "m",
			Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
			Engine:  &EngineSpec{Kind: "time_series", Decoded: e},
		}},
	}}}
	err := Resolve(s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inner engine")
	assert.Contains(t, err.Error(), "MergeTree")
}

func TestResolve_TimeSeries_AllValidForms_OK(t *testing.T) {
	tgt := "db.m_data"
	e := EngineTimeSeries{
		Samples: &TimeSeriesTarget{Target: &tgt},
		Tags: &TimeSeriesTarget{Inner: &TimeSeriesInnerTable{
			Columns: []ColumnSpec{{Name: "id", Type: "UUID"}},
			Engine:  &EngineSpec{Decoded: EngineAggregatingMergeTree{}},
		}},
	}
	s := &Schema{Databases: []DatabaseSpec{{
		Name: "db",
		Tables: []TableSpec{{Name: "m",
			Columns: []ColumnSpec{{Name: "metric_name", Type: "LowCardinality(String)"}},
			Engine:  &EngineSpec{Kind: "time_series", Decoded: e},
		}},
	}}}
	require.NoError(t, Resolve(s))
}
