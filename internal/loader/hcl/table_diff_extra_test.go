package hcl

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Issue #82: TableDiff previously covered only columns/indexes/engine/order_by/
// partition_by/sample_by/ttl/settings, so changes to constraints, primary_key
// and comment produced no diff and no DDL (invisible drift). These tests lock
// in the new coverage. (Cluster is intentionally excluded — ON CLUSTER is not
// recoverable from a live table, so diffing it would report spurious drift.)

// tsMk builds a single-table schema and applies mut to the table, so each test
// varies exactly one aspect while everything else stays identical.
func tsMk(mut func(*TableSpec)) *Schema {
	tbl := TableSpec{
		Name:    "t",
		Columns: []ColumnSpec{{Name: "id", Type: "UInt64"}},
		OrderBy: []string{"id"},
		Engine:  &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
	}
	if mut != nil {
		mut(&tbl)
	}
	return &Schema{Databases: []DatabaseSpec{{Name: "db", Tables: []TableSpec{tbl}}}}
}

func TestDiff_ConstraintsAddDropModify(t *testing.T) {
	from := tsMk(func(tb *TableSpec) {
		tb.Constraints = []ConstraintSpec{
			{Name: "keep", Check: ptr("id > 0")},
			{Name: "gone", Check: ptr("id < 100")},
			{Name: "changed", Check: ptr("id != 1")},
		}
	})
	to := tsMk(func(tb *TableSpec) {
		tb.Constraints = []ConstraintSpec{
			{Name: "keep", Check: ptr("id > 0")},
			{Name: "fresh", Assume: ptr("id > 0")},
			{Name: "changed", Check: ptr("id != 2")},
		}
	})

	cs := Diff(from, to)
	require.Len(t, cs.Databases, 1)
	require.Len(t, cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]

	require.Len(t, td.AddConstraints, 1)
	assert.Equal(t, "fresh", td.AddConstraints[0].Name)
	assert.Equal(t, []string{"gone"}, td.DropConstraints)
	require.Len(t, td.ModifyConstraints, 1)
	assert.Equal(t, "changed", td.ModifyConstraints[0].Name)
	assert.False(t, td.IsUnsafe(), "constraint changes are in-place ALTER-able")

	joined := strings.Join(GenerateSQL(cs).Statements, "\n")
	assert.Contains(t, joined, "ADD CONSTRAINT fresh ASSUME id > 0")
	assert.Contains(t, joined, "DROP CONSTRAINT gone")
	// modify = drop + re-add
	assert.Contains(t, joined, "DROP CONSTRAINT changed")
	assert.Contains(t, joined, "ADD CONSTRAINT changed CHECK id != 2")
}

func TestDiff_TableCommentChange(t *testing.T) {
	from := tsMk(func(tb *TableSpec) { tb.Comment = ptr("old note") })
	to := tsMk(func(tb *TableSpec) { tb.Comment = ptr("new note") })

	cs := Diff(from, to)
	require.Len(t, cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]
	require.NotNil(t, td.CommentChange)
	assert.False(t, td.IsUnsafe(), "MODIFY COMMENT is in-place ALTER-able")

	joined := strings.Join(GenerateSQL(cs).Statements, "\n")
	assert.Contains(t, joined, "MODIFY COMMENT 'new note'")
}

func TestDiff_PrimaryKeyChangeIsUnsafe(t *testing.T) {
	from := tsMk(func(tb *TableSpec) {
		tb.Columns = append(tb.Columns, ColumnSpec{Name: "id2", Type: "UInt64"})
		tb.PrimaryKey = []string{"id"}
	})
	to := tsMk(func(tb *TableSpec) {
		tb.Columns = append(tb.Columns, ColumnSpec{Name: "id2", Type: "UInt64"})
		tb.PrimaryKey = []string{"id", "id2"}
	})

	cs := Diff(from, to)
	require.Len(t, cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]
	require.NotNil(t, td.PrimaryKeyChange)
	assert.True(t, td.IsUnsafe(), "a primary_key change requires recreating the table")

	gen := GenerateSQL(cs)
	found := false
	for _, u := range gen.Unsafe {
		if strings.Contains(u.Reason, "PRIMARY KEY") {
			found = true
		}
	}
	assert.True(t, found, "unsafe reasons must mention PRIMARY KEY")
}

// A table identical in constraints/primary_key/comment must not produce any
// diff — the new coverage must not manufacture spurious drift.
func TestDiff_NoConstraintOrCommentChange_Empty(t *testing.T) {
	mk := func() *Schema {
		return tsMk(func(tb *TableSpec) {
			tb.Constraints = []ConstraintSpec{{Name: "c", Check: ptr("id > 0")}}
			tb.Comment = ptr("stable")
			tb.PrimaryKey = []string{"id"}
		})
	}
	assert.True(t, Diff(mk(), mk()).IsEmpty(), "identical tables must yield an empty diff")
}
