package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func TestColumnDesc(t *testing.T) {
	c := ColumnSpec{Name: "v", Type: "String", Nullable: true,
		Materialized: strPtr("upper(s)"), Codec: strPtr("LZ4"),
		TTL: strPtr("d + INTERVAL 1 DAY"), Comment: strPtr("c")}
	assert.Equal(t,
		"Nullable(String) MATERIALIZED upper(s) CODEC(LZ4) TTL d + INTERVAL 1 DAY COMMENT c",
		columnDesc(c))
}
