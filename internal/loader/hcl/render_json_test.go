package hcl

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRenderDiffJSON drives the real Diff -> GenerateSQL -> RenderDiffJSON
// pipeline and asserts the structured output: operation kind/object_type,
// engine/replicated enrichment (including an ALTER that does not change the
// engine, which must still report the engine from the resolved target schema),
// dependency order, and the unsafe surfacing.
func TestRenderDiffJSON(t *testing.T) {
	idCol := ColumnSpec{Name: "id", Type: "UInt64"}

	// left: events and sessions both exist; sessions orders by id.
	eventsLeft := mkTable("events", EngineReplicatedMergeTree{}, idCol)
	eventsLeft.OrderBy = []string{"id"}
	sessionsLeft := mkTable("sessions", EngineReplicatedMergeTree{}, idCol)
	sessionsLeft.OrderBy = []string{"id"}

	// right: query_log_archive is new; events gains a column (safe ALTER, engine
	// unchanged); sessions gains a column (safe ALTER) AND changes ORDER BY
	// (unsafe, no auto-emitted DDL).
	archive := mkTable("query_log_archive", EngineReplicatedMergeTree{}, idCol)
	archive.OrderBy = []string{"id"}
	tsCol := ColumnSpec{Name: "ts", Type: "DateTime"}
	eventsRight := mkTable("events", EngineReplicatedMergeTree{}, idCol, tsCol)
	eventsRight.OrderBy = []string{"id"}
	sessionsRight := mkTable("sessions", EngineReplicatedMergeTree{}, idCol, tsCol)
	sessionsRight.OrderBy = []string{"id", "ts"}

	left := &Schema{Databases: []DatabaseSpec{mkDB("posthog", eventsLeft, sessionsLeft)}}
	right := &Schema{Databases: []DatabaseSpec{mkDB("posthog", archive, eventsRight, sessionsRight)}}

	cs := Diff(left, right)
	gen := GenerateSQL(cs)
	out, err := RenderDiffJSON(gen, left, right)
	require.NoError(t, err)

	var doc DiffJSON
	require.NoError(t, json.Unmarshal(out, &doc))

	byObject := make(map[string]JSONOperation, len(doc.Operations))
	for _, op := range doc.Operations {
		byObject[op.Object] = op
	}

	// CREATE: engine/replicated come from the new table's resolved engine.
	create := byObject["query_log_archive"]
	assert.Equal(t, OpCreate, create.Kind)
	assert.Equal(t, KindTable, create.ObjectType)
	assert.Equal(t, "posthog", create.Database)
	assert.Equal(t, "ReplicatedMergeTree", create.Engine)
	assert.True(t, create.Replicated)
	assert.False(t, create.Unsafe)
	assert.Contains(t, create.SQL, "CREATE TABLE")

	// ALTER without an engine change still reports the engine, looked up from
	// the resolved target schema (the ALTER op carries no engine of its own).
	events := byObject["events"]
	assert.Equal(t, OpAlter, events.Kind)
	assert.Equal(t, KindTable, events.ObjectType)
	assert.Equal(t, "ReplicatedMergeTree", events.Engine)
	assert.True(t, events.Replicated)
	assert.False(t, events.Unsafe)

	// order is the index into the dependency-sorted operation list.
	for i, op := range doc.Operations {
		assert.Equal(t, i, op.Order)
	}

	// The unsafe ORDER BY change surfaces in the top-level unsafe list...
	var sawSessionsUnsafe bool
	for _, u := range doc.Unsafe {
		if u.Object == "sessions" {
			sawSessionsUnsafe = true
			assert.Equal(t, "posthog", u.Database)
			assert.Contains(t, u.Reason, "ORDER BY")
		}
	}
	assert.True(t, sawSessionsUnsafe, "sessions ORDER BY change must appear in the unsafe list")

	// ...and the safe ADD COLUMN ALTER on the same table is flagged unsafe.
	sessions := byObject["sessions"]
	assert.Equal(t, OpAlter, sessions.Kind)
	assert.True(t, sessions.Unsafe)
	assert.Contains(t, sessions.UnsafeReason, "ORDER BY")
}
