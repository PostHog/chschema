package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Issue #136: introspect(apply(layer)) must diff clean against load(layer).
// ClickHouse's SHOW CREATE renders forms that are semantically identical to the
// authored HCL but textually different — a redundant outer paren pair around a
// HAVING conjunction, and around a MATERIALIZED column expression. Both the
// load path and the introspect path run canonicalize(), so the two forms
// reduce to the same string and the round trip diffs clean.

// Item 1: a view whose HAVING comes back double-parenthesised from the cluster
// must not diff against the authored single-paren form.
func TestDiff_ViewHavingParens_RoundTripsClean(t *testing.T) {
	rows := &fakeRows{rows: []fakeRow{{
		name: "v",
		sql:  "CREATE VIEW db.v AS SELECT id FROM db.t GROUP BY id HAVING ((a >= 1) AND (b < 2))",
	}}}
	introspected := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRows(introspected, "db", rows))

	declared := &DatabaseSpec{Name: "db", Views: []ViewSpec{{
		Name:  "v",
		Query: "SELECT id FROM db.t GROUP BY id HAVING (a >= 1) AND (b < 2)",
	}}}
	canonicalize(declared)

	assert.True(t, Diff(
		&Schema{Databases: []DatabaseSpec{*declared}},
		&Schema{Databases: []DatabaseSpec{*introspected}},
	).IsEmpty(), "a double-parenthesised HAVING must round-trip without drift")
}

// Items 2 and 3: a MATERIALIZED column (redundant outer parens in SHOW CREATE)
// and a minmax skip index must round-trip clean — no MODIFY COLUMN, no
// DROP+ADD INDEX / MATERIALIZE INDEX.
func TestDiff_MaterializedColumnAndMinmaxIndex_RoundTripClean(t *testing.T) {
	rows := &fakeRows{rows: []fakeRow{{
		name: "t",
		sql: "CREATE TABLE db.t (`a` UInt64, `b` UInt64, " +
			"`x` UInt64 MATERIALIZED (a + b), " +
			"INDEX idx x TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a",
	}}}
	introspected := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRows(introspected, "db", rows))

	declared := &DatabaseSpec{Name: "db", Tables: []TableSpec{{
		Name: "t",
		Columns: []ColumnSpec{
			{Name: "a", Type: "UInt64"},
			{Name: "b", Type: "UInt64"},
			{Name: "x", Type: "UInt64", Materialized: ptr("a + b")},
		},
		OrderBy: []string{"a"},
		Engine:  &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
		Indexes: []IndexSpec{{Name: "idx", Expr: "x", Type: "minmax", Granularity: 1}},
	}}}
	canonicalize(declared)

	cs := Diff(
		&Schema{Databases: []DatabaseSpec{*declared}},
		&Schema{Databases: []DatabaseSpec{*introspected}},
	)
	assert.True(t, cs.IsEmpty(),
		"a MATERIALIZED column and a minmax index must round-trip without drift")
}
