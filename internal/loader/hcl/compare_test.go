package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Drives the real Diff -> GenerateSQL -> BuildObjectComparisons pipeline:
// one added table (with its CREATE op attached, keeping the global order
// index), one altered table whose ORDER BY change is unsafe, and direction
// pinning (added = present only on the right).
func TestBuildObjectComparisons(t *testing.T) {
	idCol := ColumnSpec{Name: "id", Type: "UInt64"}
	tsCol := ColumnSpec{Name: "ts", Type: "DateTime"}

	sessionsLeft := mkTable("sessions", EngineMergeTree{}, idCol)
	sessionsLeft.OrderBy = []string{"id"}
	sessionsRight := mkTable("sessions", EngineMergeTree{}, idCol)
	sessionsRight.OrderBy = []string{"id", "ts"} // unsafe, no DDL emitted
	archive := mkTable("archive", EngineMergeTree{}, idCol, tsCol)
	archive.OrderBy = []string{"id"}

	left := &Schema{Databases: []DatabaseSpec{mkDB("posthog", sessionsLeft)}}
	right := &Schema{Databases: []DatabaseSpec{mkDB("posthog", archive, sessionsRight)}}

	cs := Diff(left, right)
	gen := GenerateSQL(cs)
	objs := BuildObjectComparisons(cs, gen, left, right)

	byName := map[string]ObjectComparison{}
	for _, o := range objs {
		byName[o.Object] = o
	}

	created := byName["archive"]
	assert.Equal(t, StatusAdded, created.Status)
	assert.Equal(t, KindTable, created.ObjectType)
	assert.Equal(t, "posthog", created.Database)
	require.Len(t, created.Operations, 1)
	assert.Equal(t, OpCreate, created.Operations[0].Kind)
	assert.Contains(t, created.Operations[0].SQL, "CREATE TABLE")
	assert.False(t, created.Unsafe)

	altered := byName["sessions"]
	assert.Equal(t, StatusAltered, altered.Status)
	assert.True(t, altered.Unsafe)
	assert.Contains(t, altered.UnsafeReason, "ORDER BY")
	assert.Empty(t, altered.Operations) // unsafe-only change emits no DDL
	assert.Equal(t, []FieldChange{
		{Field: "order_by", Change: "modify", Old: "id", New: "id, ts"},
	}, altered.Changes)

	// Nested op Order is the index into the global dependency-sorted list.
	globalOps := buildJSONOperations(gen, left, right)
	for _, o := range objs {
		for _, op := range o.Operations {
			assert.Equal(t, globalOps[op.Order], op)
		}
	}
}

// The object view of a dictionary change: an altered dictionary carries the
// CREATE OR REPLACE that reconciles it (before #140 it carried nothing and was
// flagged unsafe), while a dictionary whose only divergence is an unverifiable
// secret is reported with zero operations.
func TestBuildObjectComparisons_Dictionaries(t *testing.T) {
	attr := DictionaryAttribute{Name: "k", Type: "UInt64"}
	dictDB := func(ds ...DictionarySpec) []DatabaseSpec {
		return []DatabaseSpec{{Name: "posthog", Dictionaries: ds}}
	}

	rates := mkDict("rates", SourceNull{}, LayoutHashed{}, attr)
	ratesNew := rates
	ratesNew.Lifetime = &DictionaryLifetime{Min: ptr(int64(60)), Max: ptr(int64(600))}

	// Introspected: the server would not show this password. Authored: it does.
	usersLive := mkDict("users", SourceMySQL{Host: ptr("h"), Password: ptr(RedactedValue)}, LayoutHashed{}, attr)
	usersAuthored := mkDict("users", SourceMySQL{Host: ptr("h"), Password: ptr("s3cret")}, LayoutHashed{}, attr)

	left := &Schema{Databases: dictDB(rates, usersLive)}
	right := &Schema{Databases: dictDB(ratesNew, usersAuthored)}

	cs := Diff(left, right)
	gen := GenerateSQL(cs)
	byName := map[string]ObjectComparison{}
	for _, o := range BuildObjectComparisons(cs, gen, left, right) {
		byName[o.Object] = o
	}

	altered := byName["rates"]
	assert.Equal(t, StatusAltered, altered.Status)
	assert.Equal(t, KindDictionary, altered.ObjectType)
	assert.False(t, altered.Unsafe, "a dictionary reloads from its source; replacing it loses nothing")
	assert.Equal(t, []FieldChange{{Field: "lifetime", Change: "modify"}}, altered.Changes)
	require.Len(t, altered.Operations, 1)
	assert.Contains(t, altered.Operations[0].SQL, "CREATE OR REPLACE DICTIONARY posthog.rates")

	unverifiable := byName["users"]
	assert.Equal(t, StatusAltered, unverifiable.Status)
	assert.Empty(t, unverifiable.Operations, "hclexp cannot rewrite a dictionary around a secret it does not know")
	assert.Equal(t, []FieldChange{
		{Field: "source.password", Change: "modify", Old: RedactedValue, New: RedactedValue},
	}, unverifiable.Changes)
}

func TestSummarizeComparisonsAndOneLiner(t *testing.T) {
	objs := []ObjectComparison{
		{ObjectType: KindTable, Status: StatusAdded},
		{ObjectType: KindTable, Status: StatusAltered},
		{ObjectType: KindMaterializedView, Status: StatusDropped},
		{ObjectType: KindRaw, Status: StatusAltered},
		{ObjectType: KindNamedCollection, Status: StatusAltered},
	}
	s := SummarizeComparisons(objs)
	assert.Equal(t, CompareSummary{
		TablesAdded: 1, TablesAltered: 1,
		MVsDropped: 1, RawsAltered: 1, NamedCollectionsChanged: 1,
	}, s)
	assert.Equal(t, "+1 table, ~1 table, -1 mv, ~1 raw, ~1 named_collection", s.OneLiner())
	assert.Equal(t, "changed", CompareSummary{}.OneLiner())
}

// fieldChangesForTable flattens every TableDiff aspect into the documented
// field vocabulary, in declaration order (columns, indexes, projections,
// constraints, engine, order_by, primary_key, partition_by, sample_by, ttl,
// comment, settings).
func TestFieldChangesForTable(t *testing.T) {
	td := TableDiff{
		Table:         "events",
		RenameColumns: []RenameColumn{{Old: "ts_old", New: "ts"}},
		AddColumns:    []ColumnSpec{{Name: "added", Type: "UInt8", Default: strPtr("1")}},
		DropColumns:   []string{"gone"},
		ModifyColumns: []ColumnChange{{
			Name: "id",
			Old:  ColumnSpec{Name: "id", Type: "UInt32"},
			New:  ColumnSpec{Name: "id", Type: "UInt64", Codec: strPtr("ZSTD")},
		}},
		AddIndexes:        []IndexSpec{{Name: "idx_a", Expr: "id", Type: "minmax", Granularity: 1}},
		DropIndexes:       []string{"idx_b"},
		AddProjections:    []ProjectionSpec{{Name: "p_a", Query: "SELECT id ORDER BY id"}},
		DropProjections:   []string{"p_b"},
		AddConstraints:    []ConstraintSpec{{Name: "c_a", Check: strPtr("id > 0")}},
		DropConstraints:   []string{"c_b"},
		ModifyConstraints: []ConstraintChange{{Name: "c_c"}},
		EngineChange: &EngineChange{
			Old: EngineMergeTree{},
			New: EngineReplacingMergeTree{},
		},
		OrderByChange:     &OrderByChange{Old: []string{"id"}, New: []string{"id", "ts"}},
		PrimaryKeyChange:  &OrderByChange{Old: []string{"id"}, New: []string{"ts"}},
		PartitionByChange: &StringChange{Old: nil, New: strPtr("toYYYYMM(ts)")},
		SampleByChange:    &StringChange{Old: strPtr("id"), New: nil},
		TTLChange:         &StringChange{Old: strPtr("ts + INTERVAL 1 DAY"), New: strPtr("ts + INTERVAL 7 DAY")},
		CommentChange:     &StringChange{Old: strPtr("old"), New: strPtr("new")},
		SettingsAdded:     map[string]string{"ttl_only_drop_parts": "1"},
		SettingsRemoved:   []string{"old_setting"},
		SettingsChanged:   []SettingChange{{Key: "index_granularity", OldValue: "8192", NewValue: "4096"}},
	}

	engOld, _ := engineSQL(EngineMergeTree{})
	engNew, _ := engineSQL(EngineReplacingMergeTree{})

	want := []FieldChange{
		{Field: "column:ts", Change: "rename", Old: "ts_old", New: "ts"},
		{Field: "column:added", Change: "add", New: "UInt8 DEFAULT 1"},
		{Field: "column:gone", Change: "drop"},
		{Field: "column:id", Change: "modify", Old: "UInt32", New: "UInt64 CODEC(ZSTD)"},
		{Field: "index:idx_a", Change: "add"},
		{Field: "index:idx_b", Change: "drop"},
		{Field: "projection:p_a", Change: "add"},
		{Field: "projection:p_b", Change: "drop"},
		{Field: "constraint:c_a", Change: "add"},
		{Field: "constraint:c_b", Change: "drop"},
		{Field: "constraint:c_c", Change: "modify"},
		{Field: "engine", Change: "modify", Old: engOld, New: engNew},
		{Field: "order_by", Change: "modify", Old: "id", New: "id, ts"},
		{Field: "primary_key", Change: "modify", Old: "id", New: "ts"},
		{Field: "partition_by", Change: "modify", New: "toYYYYMM(ts)"},
		{Field: "sample_by", Change: "modify", Old: "id"},
		{Field: "ttl", Change: "modify", Old: "ts + INTERVAL 1 DAY", New: "ts + INTERVAL 7 DAY"},
		{Field: "comment", Change: "modify", Old: "old", New: "new"},
		{Field: "setting:ttl_only_drop_parts", Change: "add", New: "1"},
		{Field: "setting:old_setting", Change: "drop"},
		{Field: "setting:index_granularity", Change: "modify", Old: "8192", New: "4096"},
	}
	assert.Equal(t, want, fieldChangesForTable(td))
}

func TestFieldChangesForMaterializedView(t *testing.T) {
	// Structural change: to_table and columns both differ.
	mvd := diffMaterializedView(
		&MaterializedViewSpec{Name: "mv", ToTable: "t_old", Query: "SELECT 1",
			Columns: []ColumnSpec{{Name: "a", Type: "UInt8"}}},
		&MaterializedViewSpec{Name: "mv", ToTable: "t_new", Query: "SELECT 1",
			Columns: []ColumnSpec{{Name: "b", Type: "UInt8"}}},
	)
	assert.True(t, mvd.Recreate)
	assert.Equal(t, []FieldChange{
		{Field: "to_table", Change: "modify", Old: "t_old", New: "t_new"},
		{Field: "columns", Change: "modify"},
	}, fieldChangesForMaterializedView(mvd))

	// Query-only change.
	mvd = diffMaterializedView(
		&MaterializedViewSpec{Name: "mv", ToTable: "t", Query: "SELECT 1"},
		&MaterializedViewSpec{Name: "mv", ToTable: "t", Query: "SELECT 2"},
	)
	assert.False(t, mvd.Recreate)
	assert.Equal(t, []FieldChange{
		{Field: "query", Change: "modify", Old: "SELECT 1", New: "SELECT 2"},
	}, fieldChangesForMaterializedView(mvd))
}

func TestFieldChangesForView(t *testing.T) {
	// Recreate: sql_security and cluster changed; each surfaces by name.
	vd := diffView(
		&ViewSpec{Name: "v", Query: "SELECT 1", SQLSecurity: strPtr("DEFINER")},
		&ViewSpec{Name: "v", Query: "SELECT 1", Cluster: strPtr("main")},
	)
	assert.True(t, vd.Recreate)
	assert.Equal(t, []FieldChange{
		{Field: "sql_security", Change: "modify"},
		{Field: "cluster", Change: "modify"},
	}, fieldChangesForView(vd))

	// In-place: query + comment.
	vd = diffView(
		&ViewSpec{Name: "v", Query: "SELECT 1", Comment: strPtr("a")},
		&ViewSpec{Name: "v", Query: "SELECT 2", Comment: strPtr("b")},
	)
	assert.Equal(t, []FieldChange{
		{Field: "query", Change: "modify", Old: "SELECT 1", New: "SELECT 2"},
		{Field: "comment", Change: "modify", Old: "a", New: "b"},
	}, fieldChangesForView(vd))
}

func TestFieldChangesForDictionaryRawNamedCollection(t *testing.T) {
	assert.Equal(t, []FieldChange{
		{Field: "layout", Change: "modify"},
		{Field: "source.clickhouse.table", Change: "modify"},
	}, fieldChangesForDictionary(DictionaryDiff{Name: "d", Changed: []string{"layout", "source.clickhouse.table"}}))

	// An unverifiable secret reports [HIDDEN] on BOTH sides even though the
	// authored side's real value is known — diff output lands in CI logs.
	assert.Equal(t, []FieldChange{
		{Field: "lifetime", Change: "modify"},
		{Field: "source.password", Change: "modify", Old: "[HIDDEN]", New: "[HIDDEN]"},
	}, fieldChangesForDictionary(DictionaryDiff{
		Name:                   "d",
		Changed:                []string{"lifetime"},
		SkippedRedactedSecrets: []string{"password"},
	}))

	assert.Equal(t, []FieldChange{
		{Field: "sql", Change: "modify", Old: "CREATE TABLE a", New: "CREATE TABLE b"},
	}, fieldChangesForRaw(RawChange{Kind: "table", Name: "r", OldSQL: "CREATE TABLE a", NewSQL: "CREATE TABLE b"}))

	ncc := NamedCollectionChange{
		Name:                  "s3",
		SetParams:             []NamedCollectionParam{{Key: "url", Value: "https://b"}},
		DeleteParams:          []string{"token"},
		SkippedRedactedParams: []string{"secret"},
		CommentChange:         &StringChange{Old: strPtr("a"), New: strPtr("b")},
	}
	assert.Equal(t, []FieldChange{
		{Field: "param:url", Change: "modify", New: "https://b"},
		{Field: "param:token", Change: "drop"},
		{Field: "param:secret", Change: "modify", Old: "[HIDDEN]", New: "[HIDDEN]"},
		{Field: "comment", Change: "modify", Old: "a", New: "b"},
	}, fieldChangesForNamedCollection(ncc))

	assert.Equal(t, []FieldChange{
		{Field: "on_cluster", Change: "modify"},
	}, fieldChangesForNamedCollection(NamedCollectionChange{Name: "s3", Recreate: true}))
}

func TestColumnDesc(t *testing.T) {
	c := ColumnSpec{Name: "v", Type: "String", Nullable: true,
		Materialized: strPtr("upper(s)"), Codec: strPtr("LZ4"),
		TTL: strPtr("d + INTERVAL 1 DAY"), Comment: strPtr("c")}
	assert.Equal(t,
		"Nullable(String) MATERIALIZED upper(s) CODEC(LZ4) TTL d + INTERVAL 1 DAY COMMENT c",
		columnDesc(c))
}

// A blocked NC add still surfaces in the object view: status added, zero
// operations, unsafe with the precise reason — same wiring dictionaries use.
func TestBuildObjectComparisons_NamedCollectionRedactedAddBlocked(t *testing.T) {
	cs := ChangeSet{NamedCollections: []NamedCollectionChange{{
		Name: "nc",
		Add: &NamedCollectionSpec{
			Name:   "nc",
			Params: []NamedCollectionParam{{Key: "password", Value: RedactedValue}},
		},
	}}}
	gen := GenerateSQL(cs)
	objs := BuildObjectComparisons(cs, gen, nil, nil)
	require.Len(t, objs, 1)
	assert.Equal(t, StatusAdded, objs[0].Status)
	assert.Empty(t, objs[0].Operations)
	assert.True(t, objs[0].Unsafe)
	assert.Contains(t, objs[0].UnsafeReason, "param(s) [password]")
}
