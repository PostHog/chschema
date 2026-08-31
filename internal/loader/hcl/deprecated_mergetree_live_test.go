package hcl

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCHLive_DeprecatedMergeTreeConstructors_RawFallback(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"allow_deprecated_syntax_for_merge_tree": 1,
	}))

	ddls := []string{
		fmt.Sprintf("CREATE TABLE %s.mt (d Date, x UInt64) ENGINE = MergeTree(d, (x), 8192)", dbName),
		fmt.Sprintf("CREATE TABLE %s.amt (d Date, x UInt64) ENGINE = AggregatingMergeTree(d, (x), 8192)", dbName),
		fmt.Sprintf("CREATE TABLE %s.rmt (d Date, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/%s/rmt', '{replica}', d, (x), 8192)", dbName, dbName),
		fmt.Sprintf("CREATE TABLE %s.ramt (d Date, x UInt64) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/%s/ramt', '{replica}', d, (x), 8192)", dbName, dbName),
		fmt.Sprintf("CREATE TABLE %s.smt (d Date, x UInt64, v UInt64) ENGINE = SummingMergeTree(d, (x), 8192, (v))", dbName),
		fmt.Sprintf("CREATE TABLE %s.rsmt (d Date, x UInt64, v UInt64) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/%s/rsmt', '{replica}', d, (x), 8192, (v))", dbName, dbName),
	}
	for _, ddl := range ddls {
		require.NoError(t, conn.Exec(ctx, ddl), "ClickHouse rejected deprecated constructor:\n%s", ddl)
	}

	_, err := Introspect(ctx, conn, dbName, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deprecated MergeTree constructor")
	assert.Contains(t, err.Error(), "-allow-raw")

	db, err := Introspect(ctx, conn, dbName, true)
	require.NoError(t, err)
	assert.Empty(t, db.Tables)
	require.Len(t, db.Raws, len(ddls))
	for _, raw := range db.Raws {
		assert.Equal(t, "table", raw.Kind)
		assert.Contains(t, raw.SQL, "8192")
	}
}
