package hcl

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderObjectComparisons(t *testing.T) {
	objs := []ObjectComparison{
		{Database: "posthog", Object: "archive", ObjectType: KindTable, Status: StatusAdded},
		{Database: "posthog", Object: "sessions", ObjectType: KindTable, Status: StatusAltered,
			Unsafe: true, UnsafeReason: "ORDER BY changed",
			Changes: []FieldChange{
				{Field: "column:ts", Change: "add", New: "DateTime"},
				{Field: "column:old", Change: "drop"},
				{Field: "column:id", Change: "modify", Old: "UInt32", New: "UInt64"},
				{Field: "column:v", Change: "rename", Old: "v_old", New: "v"},
				{Field: "order_by", Change: "modify", Old: "id", New: "id, ts"},
			}},
		{Database: "posthog", Object: "mv_events", ObjectType: KindMaterializedView, Status: StatusAltered,
			Changes: []FieldChange{{Field: "query", Change: "modify", Old: "SELECT 1", New: "SELECT 2"}}},
		{Database: "posthog", Object: "legacy", ObjectType: KindRaw, RawKind: "table", Status: StatusAltered,
			Unsafe: true, UnsafeReason: "raw table change requires DROP + CREATE, which destroys the table's data",
			Changes: []FieldChange{{Field: "sql", Change: "modify", Old: "CREATE TABLE a", New: "CREATE TABLE b"}}},
		{Object: "s3", ObjectType: KindNamedCollection, Status: StatusAltered,
			Changes: []FieldChange{{Field: "param:url", Change: "modify", New: "https://b"}}},
		{Object: "bad", ObjectType: KindNamedCollection, Status: StatusAltered,
			Error: "cannot change an external collection"},
	}

	var buf bytes.Buffer
	RenderObjectComparisons(&buf, objs)
	assert.Equal(t, `database "posthog"
  + table archive
  ~ table sessions (UNSAFE: ORDER BY changed)
      + column ts = DateTime
      - column old
      ~ column id: UInt32 -> UInt64
      ~ column v (renamed from v_old)
      ~ order_by: id -> id, ts
  ~ materialized_view mv_events
      ~ query changed
  ~ raw table legacy (UNSAFE: raw table change requires DROP + CREATE, which destroys the table's data)
      ~ sql: CREATE TABLE a -> CREATE TABLE b
named_collections
  ~ named_collection s3
      ~ param url = https://b
  ! named_collection bad: cannot change an external collection
`, buf.String())
}

// A raw block's stored DDL is canonicalized to multiple lines. Inlining it as
// "~ sql: <old> -> <new>" would spill across lines and wreck the layout (and
// drift -details indents the whole block), so a multi-line value renders as a
// bare "changed" — the JSON still carries the full text.
func TestRenderObjectComparisons_MultilineValue(t *testing.T) {
	var buf bytes.Buffer
	RenderObjectComparisons(&buf, []ObjectComparison{{
		Database: "d", Object: "legacy", ObjectType: KindRaw, RawKind: "table",
		Status: StatusAltered,
		Changes: []FieldChange{{Field: "sql", Change: "modify",
			Old: "CREATE TABLE d.legacy\n(\n  id UInt64\n)\nENGINE = MergeTree",
			New: "CREATE TABLE d.legacy\n(\n  id UInt32\n)\nENGINE = MergeTree"}},
	}})
	assert.Equal(t, `database "d"
  ~ raw table legacy
      ~ sql changed
`, buf.String())
}
