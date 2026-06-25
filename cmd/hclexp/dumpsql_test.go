package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDumpOrderPriority(t *testing.T) {
	// Dependencies first: plain tables (0) < dictionaries (1) < views (2) < MVs (3).
	assert.Equal(t, 0, dumpOrderPriority("ReplicatedMergeTree"))
	assert.Equal(t, 0, dumpOrderPriority("Distributed"))
	assert.Equal(t, 1, dumpOrderPriority("Dictionary"))
	assert.Equal(t, 2, dumpOrderPriority("View"))
	assert.Equal(t, 3, dumpOrderPriority("MaterializedView"))
}

func TestRenderCreate_BeautifiesLongViews(t *testing.T) {
	longView := "CREATE VIEW posthog.v AS SELECT a, b, c, d, e FROM posthog.events WHERE team_id = 1 AND ts > now() - 86400 GROUP BY a, b, c, d, e"
	require.Greater(t, len(longView), beautifyThreshold)

	// A long view is beautified to multi-line.
	got := renderCreate(dumpObject{name: "v", engine: "View", create: longView})
	assert.Contains(t, got, "\n", "long view should be beautified")
	assert.Contains(t, got, "FROM posthog.events")

	// A long MV is beautified too.
	longMV := "CREATE MATERIALIZED VIEW posthog.mv TO posthog.dest AS SELECT a, count() AS n FROM posthog.src WHERE team_id = 1 GROUP BY a"
	require.Greater(t, len(longMV), beautifyThreshold)
	assert.Contains(t, renderCreate(dumpObject{name: "mv", engine: "MaterializedView", create: longMV}), "\n")

	// A long TABLE is left verbatim (beautification is reserved for SELECT-bearing objects).
	longTable := "CREATE TABLE posthog.events (id UInt64, a String, b String, c String, d String, e String, f String) ENGINE = MergeTree ORDER BY id"
	require.Greater(t, len(longTable), beautifyThreshold)
	assert.Equal(t, longTable, renderCreate(dumpObject{name: "events", engine: "MergeTree", create: longTable}))

	// A short view stays inline (under threshold).
	shortView := "CREATE VIEW posthog.s AS SELECT 1"
	require.LessOrEqual(t, len(shortView), beautifyThreshold)
	assert.Equal(t, shortView, renderCreate(dumpObject{name: "s", engine: "View", create: shortView}))
}

// TestRenderDump verifies the apply-order sort (MV last, tables first) and the
// output format, without a live connection.
func TestRenderDump(t *testing.T) {
	// Deliberately scrambled input order.
	objs := []dumpObject{
		{name: "events_mv", engine: "MaterializedView", create: "CREATE MATERIALIZED VIEW db.events_mv ..."},
		{name: "events", engine: "MergeTree", create: "CREATE TABLE db.events ..."},
		{name: "dict", engine: "Dictionary", create: "CREATE DICTIONARY db.dict ..."},
		{name: "events_view", engine: "View", create: "CREATE VIEW db.events_view ..."},
		{name: "alpha", engine: "MergeTree", create: "CREATE TABLE db.alpha ..."},
	}

	out := renderDump(objs, "posthog")

	assert.True(t, strings.HasPrefix(out, "-- database: posthog\n"), "header first")

	// Each CREATE is terminated by a lone ';'.
	assert.Contains(t, out, "CREATE TABLE db.events ...\n;\n")

	// Order: tables (by name) -> dictionary -> view -> MV.
	order := []string{
		"CREATE TABLE db.alpha",
		"CREATE TABLE db.events",
		"CREATE DICTIONARY db.dict",
		"CREATE VIEW db.events_view",
		"CREATE MATERIALIZED VIEW db.events_mv",
	}
	last := -1
	for _, marker := range order {
		idx := strings.Index(out, marker)
		require.GreaterOrEqual(t, idx, 0, "missing %q", marker)
		assert.Greater(t, idx, last, "%q out of apply order", marker)
		last = idx
	}
}
