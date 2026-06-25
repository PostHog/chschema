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
