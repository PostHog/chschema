package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Issue #83: a table constraint's CHECK/ASSUME kind must survive introspection.
// The parser preserves the kind as of chparser#17 (ConstraintClause.Type), and
// constraintFromAST now maps it onto ConstraintSpec.Check / .Assume. Previously
// every constraint became CHECK (and ASSUME failed to parse at all), producing
// a spurious diff against an assume-declared table.

func TestIntrospect_ConstraintCheck(t *testing.T) {
	ts, err := buildTableFromCreateSQL(
		"CREATE TABLE t (`x` Int32, CONSTRAINT c CHECK x > 0) ENGINE = MergeTree ORDER BY x")
	require.NoError(t, err)
	require.Len(t, ts.Constraints, 1)
	assert.Equal(t, "c", ts.Constraints[0].Name)
	require.NotNil(t, ts.Constraints[0].Check)
	assert.Equal(t, "x > 0", *ts.Constraints[0].Check)
	assert.Nil(t, ts.Constraints[0].Assume)
}

func TestIntrospect_ConstraintAssume(t *testing.T) {
	ts, err := buildTableFromCreateSQL(
		"CREATE TABLE t (`x` Int32, CONSTRAINT c ASSUME x > 0) ENGINE = MergeTree ORDER BY x")
	require.NoError(t, err)
	require.Len(t, ts.Constraints, 1)
	assert.Equal(t, "c", ts.Constraints[0].Name)
	require.NotNil(t, ts.Constraints[0].Assume, "ASSUME must round-trip as Assume, not Check")
	assert.Equal(t, "x > 0", *ts.Constraints[0].Assume)
	assert.Nil(t, ts.Constraints[0].Check)
}

// The original #83 symptom: an assume-declared table must not diff against its
// own introspected form (previously it did, because assume came back as check).
func TestDiff_AssumeConstraintRoundTripsClean(t *testing.T) {
	introspected, err := buildTableFromCreateSQL(
		"CREATE TABLE t (`x` Int32, CONSTRAINT c ASSUME x > 0) ENGINE = MergeTree ORDER BY x")
	require.NoError(t, err)
	// buildTableFromCreateSQL leaves Name empty (set from system.tables by the
	// introspection caller, not the DDL); align it for the comparison.
	introspected.Name = "t"

	declared := TableSpec{
		Name:        "t",
		Columns:     []ColumnSpec{{Name: "x", Type: "Int32"}},
		OrderBy:     []string{"x"},
		Engine:      &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
		Constraints: []ConstraintSpec{{Name: "c", Assume: ptr("x > 0")}},
	}
	wrap := func(ts TableSpec) *Schema {
		return &Schema{Databases: []DatabaseSpec{{Name: "db", Tables: []TableSpec{ts}}}}
	}
	assert.True(t, Diff(wrap(declared), wrap(introspected)).IsEmpty(),
		"an ASSUME constraint must round-trip without spurious drift")
}
