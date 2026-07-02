package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Issue #83: the SQL parser only accepts CHECK table constraints — its
// CREATE TABLE constraint branch hard-requires the CHECK keyword — so a valid
// ClickHouse ASSUME constraint is a parse error and never round-trips. Fixing
// it needs the parser to accept ASSUME and record the kind
// (github.com/orian/clickhouse-sql-parser#17).

// A CHECK constraint introspects as Check (baseline that must keep working).
func TestIntrospect_ConstraintCheck(t *testing.T) {
	ts, err := buildTableFromCreateSQL(
		"CREATE TABLE t (`x` Int32, CONSTRAINT c CHECK x > 0) ENGINE = MergeTree ORDER BY x")
	require.NoError(t, err)
	require.Len(t, ts.Constraints, 1)
	assert.Equal(t, "c", ts.Constraints[0].Name)
	require.NotNil(t, ts.Constraints[0].Check)
	assert.Nil(t, ts.Constraints[0].Assume)
}

// Canary for #83: an ASSUME constraint currently fails to parse. When the
// upstream parser learns ASSUME this test will start failing (err == nil) —
// that is the signal to wire the kind through constraintFromAST (setting
// Assume instead of Check) and drop this canary.
func TestIntrospect_ConstraintAssume_ParserLimitation(t *testing.T) {
	_, err := buildTableFromCreateSQL(
		"CREATE TABLE t (`x` Int32, CONSTRAINT c ASSUME x > 0) ENGINE = MergeTree ORDER BY x")
	require.Error(t, err, "if this now parses, the parser supports ASSUME — see issue #83 / chparser#17")
	assert.Contains(t, err.Error(), "CHECK",
		"current failure is the parser hard-requiring the CHECK keyword")
}
