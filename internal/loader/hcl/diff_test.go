package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mkTable(name string, engine Engine, cols ...ColumnSpec) TableSpec {
	return TableSpec{
		Name:    name,
		Columns: cols,
		Engine: &EngineSpec{
			Kind:    engine.Kind(),
			Decoded: engine,
		},
	}
}

func mkDB(name string, tables ...TableSpec) DatabaseSpec {
	return DatabaseSpec{Name: name, Tables: tables}
}

func TestDiff_IdenticalSchemasEmpty(t *testing.T) {
	a := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"}))}
	b := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"}))}
	cs := Diff(&Schema{Databases: a}, &Schema{Databases: b})
	assert.True(t, cs.IsEmpty())
	assert.Empty(t, cs.Databases)
}

func TestDiff_AddTable(t *testing.T) {
	from := []DatabaseSpec{mkDB("posthog")}
	newTable := mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"})
	to := []DatabaseSpec{mkDB("posthog", newTable)}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	expected := ChangeSet{
		Databases: []DatabaseChange{
			{Database: "posthog", AddTables: []TableSpec{newTable}},
		},
	}
	assert.Equal(t, expected, cs)
}

func TestDiff_DropTable(t *testing.T) {
	from := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"}))}
	to := []DatabaseSpec{mkDB("posthog")}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	expected := ChangeSet{
		Databases: []DatabaseChange{
			{Database: "posthog", DropTables: []TableSpec{mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"})}},
		},
	}
	assert.Equal(t, expected, cs)
}

func TestDiff_AddDropColumns(t *testing.T) {
	from := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{},
		ColumnSpec{Name: "id", Type: "UUID"},
		ColumnSpec{Name: "old_col", Type: "String"},
	))}
	to := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{},
		ColumnSpec{Name: "id", Type: "UUID"},
		ColumnSpec{Name: "new_col", Type: "UInt64"},
	))}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	expected := ChangeSet{
		Databases: []DatabaseChange{
			{
				Database: "posthog",
				AlterTables: []TableDiff{
					{
						Table:       "events",
						AddColumns:  []ColumnSpec{{Name: "new_col", Type: "UInt64"}},
						DropColumns: []string{"old_col"},
					},
				},
			},
		},
	}
	assert.Equal(t, expected, cs)
}

func TestDiff_ModifyColumnType(t *testing.T) {
	from := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{},
		ColumnSpec{Name: "id", Type: "UUID"},
		ColumnSpec{Name: "count", Type: "UInt32"},
	))}
	to := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{},
		ColumnSpec{Name: "id", Type: "UUID"},
		ColumnSpec{Name: "count", Type: "UInt64"},
	))}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	require := assert.New(t)
	require.Len(cs.Databases, 1)
	require.Len(cs.Databases[0].AlterTables, 1)
	assert.Equal(t, []ColumnChange{
		{Name: "count", OldType: "UInt32", NewType: "UInt64"},
	}, cs.Databases[0].AlterTables[0].ModifyColumns)
}

func TestDiff_EngineChange(t *testing.T) {
	from := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"}))}
	to := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineLog{}, ColumnSpec{Name: "id", Type: "UUID"}))}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	require := assert.New(t)
	require.Len(cs.Databases, 1)
	require.Len(cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]
	assert.Equal(t, &EngineChange{Old: EngineMergeTree{}, New: EngineLog{}}, td.EngineChange)
	assert.True(t, td.IsUnsafe())
}

func TestDiff_EngineFieldsChange(t *testing.T) {
	from := []DatabaseSpec{mkDB("posthog", mkTable("events",
		EngineReplicatedMergeTree{ZooPath: "/path/a", ReplicaName: "{replica}"},
		ColumnSpec{Name: "id", Type: "UUID"},
	))}
	to := []DatabaseSpec{mkDB("posthog", mkTable("events",
		EngineReplicatedMergeTree{ZooPath: "/path/b", ReplicaName: "{replica}"},
		ColumnSpec{Name: "id", Type: "UUID"},
	))}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	require := assert.New(t)
	require.Len(cs.Databases, 1)
	require.Len(cs.Databases[0].AlterTables, 1)
	assert.NotNil(t, cs.Databases[0].AlterTables[0].EngineChange)
}

func TestDiff_OrderByChange(t *testing.T) {
	tFrom := mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"})
	tFrom.OrderBy = []string{"id"}
	tTo := mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"})
	tTo.OrderBy = []string{"id", "ts"}

	cs := Diff(&Schema{Databases: []DatabaseSpec{mkDB("posthog", tFrom)}}, &Schema{Databases: []DatabaseSpec{mkDB("posthog", tTo)}})
	require := assert.New(t)
	require.Len(cs.Databases, 1)
	require.Len(cs.Databases[0].AlterTables, 1)
	assert.Equal(t, &OrderByChange{Old: []string{"id"}, New: []string{"id", "ts"}}, cs.Databases[0].AlterTables[0].OrderByChange)
}

func TestDiff_SettingsAddRemoveChange(t *testing.T) {
	tFrom := mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"})
	tFrom.Settings = map[string]string{"keep": "1", "remove": "2", "change": "3"}
	tTo := mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"})
	tTo.Settings = map[string]string{"keep": "1", "change": "9", "add": "4"}

	cs := Diff(&Schema{Databases: []DatabaseSpec{mkDB("posthog", tFrom)}}, &Schema{Databases: []DatabaseSpec{mkDB("posthog", tTo)}})
	require := assert.New(t)
	require.Len(cs.Databases, 1)
	require.Len(cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]
	assert.Equal(t, map[string]string{"add": "4"}, td.SettingsAdded)
	assert.Equal(t, []string{"remove"}, td.SettingsRemoved)
	assert.Equal(t, []SettingChange{{Key: "change", OldValue: "3", NewValue: "9"}}, td.SettingsChanged)
}

func TestDiff_StringPtrAttributes(t *testing.T) {
	pt := func(s string) *string { return &s }
	tFrom := mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"})
	tTo := mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"})
	tTo.PartitionBy = pt("toYYYYMM(ts)")
	tFrom.TTL = pt("ts + INTERVAL 1 YEAR")
	tTo.TTL = pt("ts + INTERVAL 2 YEAR")

	cs := Diff(&Schema{Databases: []DatabaseSpec{mkDB("posthog", tFrom)}}, &Schema{Databases: []DatabaseSpec{mkDB("posthog", tTo)}})
	require := assert.New(t)
	require.Len(cs.Databases, 1)
	require.Len(cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]
	assert.Equal(t, &StringChange{Old: nil, New: pt("toYYYYMM(ts)")}, td.PartitionByChange)
	assert.Equal(t, &StringChange{Old: pt("ts + INTERVAL 1 YEAR"), New: pt("ts + INTERVAL 2 YEAR")}, td.TTLChange)
}

func TestDiff_IndexAddDropChange(t *testing.T) {
	tFrom := mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"})
	tFrom.Indexes = []IndexSpec{
		{Name: "drop_me", Expr: "id", Type: "minmax", Granularity: 4},
		{Name: "change_me", Expr: "id", Type: "minmax", Granularity: 4},
	}
	tTo := mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"})
	tTo.Indexes = []IndexSpec{
		{Name: "change_me", Expr: "id", Type: "set(0)", Granularity: 4},
		{Name: "add_me", Expr: "id", Type: "minmax", Granularity: 4},
	}

	cs := Diff(&Schema{Databases: []DatabaseSpec{mkDB("posthog", tFrom)}}, &Schema{Databases: []DatabaseSpec{mkDB("posthog", tTo)}})
	require := assert.New(t)
	require.Len(cs.Databases, 1)
	require.Len(cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]
	assert.ElementsMatch(t, []IndexSpec{
		{Name: "add_me", Expr: "id", Type: "minmax", Granularity: 4},
		{Name: "change_me", Expr: "id", Type: "set(0)", Granularity: 4},
	}, td.AddIndexes)
	assert.ElementsMatch(t, []string{"drop_me", "change_me"}, td.DropIndexes)
}

func TestDiff_NewDatabase(t *testing.T) {
	from := []DatabaseSpec{}
	newTable := mkTable("events", EngineMergeTree{}, ColumnSpec{Name: "id", Type: "UUID"})
	to := []DatabaseSpec{mkDB("posthog", newTable)}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	expected := ChangeSet{
		Databases: []DatabaseChange{
			{Database: "posthog", AddTables: []TableSpec{newTable}},
		},
	}
	assert.Equal(t, expected, cs)
}

func TestDiff_DroppedDatabaseDropsAllTables(t *testing.T) {
	from := []DatabaseSpec{mkDB("posthog",
		mkTable("a", EngineMergeTree{}),
		mkTable("b", EngineMergeTree{}),
	)}
	to := []DatabaseSpec{}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	require := assert.New(t)
	require.Len(cs.Databases, 1)
	assert.ElementsMatch(t, []TableSpec{
		mkTable("a", EngineMergeTree{}),
		mkTable("b", EngineMergeTree{}),
	}, cs.Databases[0].DropTables)
}

func TestDiff_RenameColumn(t *testing.T) {
	pt := func(s string) *string { return &s }
	from := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{},
		ColumnSpec{Name: "id", Type: "UUID"},
		ColumnSpec{Name: "event_name", Type: "String"},
	))}
	to := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{},
		ColumnSpec{Name: "id", Type: "UUID"},
		ColumnSpec{Name: "event_old", Type: "String", RenamedFrom: pt("event_name")},
	))}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	require := assert.New(t)
	require.Len(cs.Databases, 1)
	require.Len(cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]
	assert.Equal(t, []RenameColumn{{Old: "event_name", New: "event_old"}}, td.RenameColumns)
	assert.Empty(t, td.AddColumns)
	assert.Empty(t, td.DropColumns)
	assert.Empty(t, td.ModifyColumns)
}

func TestDiff_RenameAndIntroduceNewColumnWithOldName(t *testing.T) {
	pt := func(s string) *string { return &s }
	from := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{},
		ColumnSpec{Name: "id", Type: "UUID"},
		ColumnSpec{Name: "event_name", Type: "String"},
	))}
	to := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{},
		ColumnSpec{Name: "id", Type: "UUID"},
		ColumnSpec{Name: "event_old", Type: "String", RenamedFrom: pt("event_name")},
		ColumnSpec{Name: "event_name", Type: "Int64"},
	))}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	require := assert.New(t)
	require.Len(cs.Databases, 1)
	require.Len(cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]
	assert.Equal(t, []RenameColumn{{Old: "event_name", New: "event_old"}}, td.RenameColumns)
	assert.Equal(t, []ColumnSpec{{Name: "event_name", Type: "Int64"}}, td.AddColumns)
	assert.Empty(t, td.DropColumns)
	assert.Empty(t, td.ModifyColumns)
}

func TestDiff_RenameWithTypeChange(t *testing.T) {
	pt := func(s string) *string { return &s }
	from := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{},
		ColumnSpec{Name: "old", Type: "UInt32"},
	))}
	to := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{},
		ColumnSpec{Name: "new", Type: "UInt64", RenamedFrom: pt("old")},
	))}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	require := assert.New(t)
	require.Len(cs.Databases, 1)
	require.Len(cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]
	assert.Equal(t, []RenameColumn{{Old: "old", New: "new"}}, td.RenameColumns)
	assert.Equal(t, []ColumnChange{{Name: "new", OldType: "UInt32", NewType: "UInt64"}}, td.ModifyColumns)
}

func TestDiff_RenameStaleDirectiveIsNoOp(t *testing.T) {
	// `from` already has the post-rename name; the directive is left over
	// from a previous apply. Diff should treat it as a no-op.
	pt := func(s string) *string { return &s }
	from := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{},
		ColumnSpec{Name: "new_name", Type: "String"},
	))}
	to := []DatabaseSpec{mkDB("posthog", mkTable("events", EngineMergeTree{},
		ColumnSpec{Name: "new_name", Type: "String", RenamedFrom: pt("old_name")},
	))}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	assert.True(t, cs.IsEmpty())
}

func TestSQLGen_RenameColumn(t *testing.T) {
	td := TableDiff{
		Table:         "events",
		RenameColumns: []RenameColumn{{Old: "a", New: "b"}},
		AddColumns:    []ColumnSpec{{Name: "a", Type: "Int64"}},
	}
	out := GenerateSQL(ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AlterTables: []TableDiff{td}},
	}})
	expected := "ALTER TABLE posthog.events RENAME COLUMN a TO b, ADD COLUMN a Int64"
	assert.Equal(t, []string{expected}, out.Statements)
}

func mkMV(name, toTable, query string) MaterializedViewSpec {
	return MaterializedViewSpec{Name: name, ToTable: toTable, Query: query}
}

func mkDBWithMVs(name string, mvs ...MaterializedViewSpec) DatabaseSpec {
	return DatabaseSpec{Name: name, MaterializedViews: mvs}
}

func TestDiff_AddMaterializedView(t *testing.T) {
	from := []DatabaseSpec{mkDB("posthog")}
	mv := mkMV("metrics_mv", "default.metrics", "SELECT id FROM default.src")
	to := []DatabaseSpec{mkDBWithMVs("posthog", mv)}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	expected := ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", AddMaterializedViews: []MaterializedViewSpec{mv}},
	}}
	assert.Equal(t, expected, cs)
}

func TestDiff_DropMaterializedView(t *testing.T) {
	from := []DatabaseSpec{mkDBWithMVs("posthog", mkMV("metrics_mv", "default.metrics", "SELECT id FROM default.src"))}
	to := []DatabaseSpec{mkDB("posthog")}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	expected := ChangeSet{Databases: []DatabaseChange{
		{Database: "posthog", DropMaterializedViews: []string{"metrics_mv"}},
	}}
	assert.Equal(t, expected, cs)
}

func TestDiff_AlterMaterializedViewQueryOnly(t *testing.T) {
	from := []DatabaseSpec{mkDBWithMVs("posthog", mkMV("metrics_mv", "default.metrics", "SELECT id FROM default.src"))}
	to := []DatabaseSpec{mkDBWithMVs("posthog", mkMV("metrics_mv", "default.metrics", "SELECT id, ts FROM default.src"))}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	require := assert.New(t)
	require.Len(cs.Databases, 1)
	require.Len(cs.Databases[0].AlterMaterializedViews, 1)
	mvd := cs.Databases[0].AlterMaterializedViews[0]
	assert.Equal(t, "metrics_mv", mvd.Name)
	assert.False(t, mvd.Recreate)
	assert.False(t, mvd.IsUnsafe())
	assert.Equal(t, &StringChange{
		Old: ptr("SELECT id FROM default.src"),
		New: ptr("SELECT id, ts FROM default.src"),
	}, mvd.QueryChange)
}

func TestDiff_AlterMaterializedViewToTableRecreate(t *testing.T) {
	from := []DatabaseSpec{mkDBWithMVs("posthog", mkMV("metrics_mv", "default.metrics_a", "SELECT id FROM default.src"))}
	to := []DatabaseSpec{mkDBWithMVs("posthog", mkMV("metrics_mv", "default.metrics_b", "SELECT id FROM default.src"))}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	require := assert.New(t)
	require.Len(cs.Databases, 1)
	require.Len(cs.Databases[0].AlterMaterializedViews, 1)
	mvd := cs.Databases[0].AlterMaterializedViews[0]
	assert.True(t, mvd.Recreate)
	assert.True(t, mvd.IsUnsafe())
	assert.Nil(t, mvd.QueryChange)
}

func TestDiff_IdenticalMaterializedViewsEmpty(t *testing.T) {
	mv := mkMV("metrics_mv", "default.metrics", "SELECT id FROM default.src")
	cs := Diff(
		&Schema{Databases: []DatabaseSpec{mkDBWithMVs("posthog", mv)}},
		&Schema{Databases: []DatabaseSpec{mkDBWithMVs("posthog", mv)}},
	)
	assert.True(t, cs.IsEmpty())
}

func TestMaterializedViewDiff_IsEmptyIsUnsafe(t *testing.T) {
	// Empty diff: no change at all.
	empty := MaterializedViewDiff{Name: "mv"}
	assert.True(t, empty.IsEmpty())
	assert.False(t, empty.IsUnsafe())

	// Query-only diff: not empty, not unsafe.
	qOnly := MaterializedViewDiff{
		Name:        "mv",
		QueryChange: &StringChange{Old: ptr("SELECT 1"), New: ptr("SELECT 2")},
	}
	assert.False(t, qOnly.IsEmpty())
	assert.False(t, qOnly.IsUnsafe())

	// Recreate diff: not empty, unsafe.
	recreate := MaterializedViewDiff{Name: "mv", Recreate: true}
	assert.False(t, recreate.IsEmpty())
	assert.True(t, recreate.IsUnsafe())
}

func TestDatabaseChange_IsEmptyWithMVs(t *testing.T) {
	// A DatabaseChange with only AddMaterializedViews is not empty.
	dc := DatabaseChange{
		Database:             "posthog",
		AddMaterializedViews: []MaterializedViewSpec{mkMV("mv", "dst", "SELECT 1")},
	}
	assert.False(t, dc.IsEmpty())
}

func TestDiff_AlterMaterializedViewColumnListRecreate(t *testing.T) {
	fromMV := MaterializedViewSpec{
		Name:    "metrics_mv",
		ToTable: "default.metrics",
		Query:   "SELECT id FROM default.src",
		Columns: []ColumnSpec{{Name: "id", Type: "UInt64"}},
	}
	toMV := MaterializedViewSpec{
		Name:    "metrics_mv",
		ToTable: "default.metrics",
		Query:   "SELECT id FROM default.src",
		Columns: []ColumnSpec{{Name: "id", Type: "UInt64"}, {Name: "ts", Type: "DateTime"}},
	}

	cs := Diff(
		&Schema{Databases: []DatabaseSpec{{Name: "posthog", MaterializedViews: []MaterializedViewSpec{fromMV}}}},
		&Schema{Databases: []DatabaseSpec{{Name: "posthog", MaterializedViews: []MaterializedViewSpec{toMV}}}},
	)
	require := assert.New(t)
	require.Len(cs.Databases, 1)
	require.Len(cs.Databases[0].AlterMaterializedViews, 1)
	mvd := cs.Databases[0].AlterMaterializedViews[0]
	assert.True(t, mvd.Recreate)
	assert.True(t, mvd.IsUnsafe())
	assert.Nil(t, mvd.QueryChange)
}

func TestDiff_AlterMaterializedViewBothToTableAndQueryRecreateOnly(t *testing.T) {
	// When both to_table and query change, Recreate supersedes QueryChange.
	from := []DatabaseSpec{mkDBWithMVs("posthog",
		mkMV("metrics_mv", "default.metrics_a", "SELECT id FROM default.src"),
	)}
	to := []DatabaseSpec{mkDBWithMVs("posthog",
		mkMV("metrics_mv", "default.metrics_b", "SELECT id, ts FROM default.src"),
	)}

	cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
	require := assert.New(t)
	require.Len(cs.Databases, 1)
	require.Len(cs.Databases[0].AlterMaterializedViews, 1)
	mvd := cs.Databases[0].AlterMaterializedViews[0]
	assert.True(t, mvd.Recreate)
	assert.Nil(t, mvd.QueryChange, "QueryChange must be nil when Recreate is set")
}

func TestDiff_TableDiffIsUnsafe(t *testing.T) {
	// Engine change → unsafe.
	td := TableDiff{EngineChange: &EngineChange{Old: EngineMergeTree{}, New: EngineLog{}}}
	assert.True(t, td.IsUnsafe())

	// OrderBy change → unsafe.
	td = TableDiff{OrderByChange: &OrderByChange{Old: []string{"a"}, New: []string{"b"}}}
	assert.True(t, td.IsUnsafe())

	// Column add only → safe.
	td = TableDiff{AddColumns: []ColumnSpec{{Name: "x", Type: "UInt64"}}}
	assert.False(t, td.IsUnsafe())
}

func mkDict(name string, source DictionarySource, layout DictionaryLayout, attrs ...DictionaryAttribute) DictionarySpec {
	return DictionarySpec{
		Name:       name,
		PrimaryKey: []string{"k"},
		Attributes: attrs,
		Source:     &DictionarySourceSpec{Kind: source.Kind(), Decoded: source},
		Layout:     &DictionaryLayoutSpec{Kind: layout.Kind(), Decoded: layout},
	}
}

func TestDictionaryDiff_EmptyAndUnsafe(t *testing.T) {
	var empty DictionaryDiff
	assert.True(t, empty.IsEmpty())
	assert.False(t, empty.IsUnsafe())

	d := DictionaryDiff{Name: "d", Changed: []string{"query"}}
	assert.False(t, d.IsEmpty())
	assert.False(t, d.IsUnsafe())
}

func TestDatabaseChange_IsEmpty_CoversDictionaries(t *testing.T) {
	dc := DatabaseChange{Database: "db", AddDictionaries: []DictionarySpec{{Name: "d"}}}
	assert.False(t, dc.IsEmpty())
}

func TestDiff_Dictionaries(t *testing.T) {
	base := mkDict("d", SourceNull{}, LayoutHashed{},
		DictionaryAttribute{Name: "k", Type: "UInt64"},
		DictionaryAttribute{Name: "v", Type: "String"},
	)

	t.Run("add", func(t *testing.T) {
		from := []DatabaseSpec{{Name: "db"}}
		to := []DatabaseSpec{{Name: "db", Dictionaries: []DictionarySpec{base}}}
		cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
		require.Len(t, cs.Databases, 1)
		assert.Equal(t, []DictionarySpec{base}, cs.Databases[0].AddDictionaries)
	})

	t.Run("drop", func(t *testing.T) {
		from := []DatabaseSpec{{Name: "db", Dictionaries: []DictionarySpec{base}}}
		to := []DatabaseSpec{{Name: "db"}}
		cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
		require.Len(t, cs.Databases, 1)
		assert.Equal(t, []string{"d"}, cs.Databases[0].DropDictionaries)
	})

	t.Run("layout change", func(t *testing.T) {
		changed := base
		changed.Layout = &DictionaryLayoutSpec{Kind: "flat", Decoded: LayoutFlat{}}
		from := []DatabaseSpec{{Name: "db", Dictionaries: []DictionarySpec{base}}}
		to := []DatabaseSpec{{Name: "db", Dictionaries: []DictionarySpec{changed}}}
		cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
		require.Len(t, cs.Databases, 1)
		require.Len(t, cs.Databases[0].AlterDictionaries, 1)
		dd := cs.Databases[0].AlterDictionaries[0]
		assert.Equal(t, "d", dd.Name)
		assert.Contains(t, dd.Changed, "layout")
	})

	t.Run("attributes change", func(t *testing.T) {
		changed := base
		changed.Attributes = []DictionaryAttribute{
			{Name: "k", Type: "UInt64"},
			{Name: "v", Type: "String"},
			{Name: "extra", Type: "Int32"},
		}
		from := []DatabaseSpec{{Name: "db", Dictionaries: []DictionarySpec{base}}}
		to := []DatabaseSpec{{Name: "db", Dictionaries: []DictionarySpec{changed}}}
		cs := Diff(&Schema{Databases: from}, &Schema{Databases: to})
		require.Len(t, cs.Databases, 1)
		require.Len(t, cs.Databases[0].AlterDictionaries, 1)
		assert.Contains(t, cs.Databases[0].AlterDictionaries[0].Changed, "attributes")
	})

	t.Run("identical produces no change", func(t *testing.T) {
		dbs := []DatabaseSpec{{Name: "db", Dictionaries: []DictionarySpec{base}}}
		assert.True(t, Diff(&Schema{Databases: dbs}, &Schema{Databases: dbs}).IsEmpty())
	})
}

func mkNC(name string, params ...NamedCollectionParam) NamedCollectionSpec {
	return NamedCollectionSpec{Name: name, Params: params}
}

func TestDiff_NamedCollections(t *testing.T) {
	base := mkNC("nc1",
		NamedCollectionParam{Key: "a", Value: "1"},
		NamedCollectionParam{Key: "b", Value: "2"},
	)

	t.Run("add", func(t *testing.T) {
		from := &Schema{}
		to := &Schema{NamedCollections: []NamedCollectionSpec{base}}
		cs := Diff(from, to)
		require.Len(t, cs.NamedCollections, 1)
		change := cs.NamedCollections[0]
		assert.Equal(t, "nc1", change.Name)
		require.NotNil(t, change.Add)
		assert.Equal(t, base, *change.Add)
	})

	t.Run("drop", func(t *testing.T) {
		from := &Schema{NamedCollections: []NamedCollectionSpec{base}}
		to := &Schema{}
		cs := Diff(from, to)
		require.Len(t, cs.NamedCollections, 1)
		assert.True(t, cs.NamedCollections[0].Drop)
		assert.Equal(t, "nc1", cs.NamedCollections[0].Name)
	})

	t.Run("set+delete params", func(t *testing.T) {
		changed := mkNC("nc1",
			NamedCollectionParam{Key: "a", Value: "1_new"},
			NamedCollectionParam{Key: "c", Value: "3"},
		)
		from := &Schema{NamedCollections: []NamedCollectionSpec{base}}
		to := &Schema{NamedCollections: []NamedCollectionSpec{changed}}
		cs := Diff(from, to)
		require.Len(t, cs.NamedCollections, 1)
		c := cs.NamedCollections[0]
		assert.False(t, c.Recreate)
		require.Len(t, c.SetParams, 2)
		setKeys := map[string]string{}
		for _, p := range c.SetParams {
			setKeys[p.Key] = p.Value
		}
		assert.Equal(t, "1_new", setKeys["a"])
		assert.Equal(t, "3", setKeys["c"])
		assert.Equal(t, []string{"b"}, c.DeleteParams)
	})

	t.Run("on cluster change recreates", func(t *testing.T) {
		from := &Schema{NamedCollections: []NamedCollectionSpec{base}}
		toNC := base
		c := "posthog"
		toNC.Cluster = &c
		to := &Schema{NamedCollections: []NamedCollectionSpec{toNC}}
		cs := Diff(from, to)
		require.Len(t, cs.NamedCollections, 1)
		assert.True(t, cs.NamedCollections[0].Recreate)
		require.NotNil(t, cs.NamedCollections[0].Add)
		assert.Equal(t, "posthog", *cs.NamedCollections[0].Add.Cluster)
	})

	t.Run("identical produces no change", func(t *testing.T) {
		schema := &Schema{NamedCollections: []NamedCollectionSpec{base}}
		assert.True(t, Diff(schema, schema).IsEmpty())
	})
}

func TestDiff_ExternalNCs_Ignored(t *testing.T) {
	from := &Schema{NamedCollections: []NamedCollectionSpec{
		{Name: "x", External: true, Params: []NamedCollectionParam{{Key: "a", Value: "1"}}},
	}}
	to := &Schema{NamedCollections: []NamedCollectionSpec{
		{Name: "x", External: true, Params: []NamedCollectionParam{{Key: "a", Value: "2"}}},
	}}
	cs := Diff(from, to)
	assert.True(t, cs.IsEmpty(), "external collections should be diff-skipped regardless of attribute changes")
}

func TestDiff_ExternalToManaged_Errors(t *testing.T) {
	from := &Schema{NamedCollections: []NamedCollectionSpec{
		{Name: "x", External: true},
	}}
	to := &Schema{NamedCollections: []NamedCollectionSpec{
		{Name: "x", Params: []NamedCollectionParam{{Key: "a", Value: "1"}}},
	}}
	cs := Diff(from, to)
	require.Len(t, cs.NamedCollections, 1)
	c := cs.NamedCollections[0]
	assert.NotEmpty(t, c.Error)
	assert.Contains(t, c.Error, "external")
}
