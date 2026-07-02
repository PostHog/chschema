package hcl

import (
	"encoding/json"
	"reflect"
	"strings"
)

// JSONOperation is one entry in the structured, dependency-ordered diff output
// emitted by `hclexp diff -format json`. It mirrors one generated statement
// (Operations is parallel to GeneratedSQL.Statements), enriched with the
// object's engine family and an unsafe flag.
type JSONOperation struct {
	Order        int    `json:"order"`
	Kind         string `json:"kind"`        // CREATE | ALTER | DROP | RENAME
	ObjectType   string `json:"object_type"` // table | materialized_view | view | dictionary | named_collection | raw
	Database     string `json:"database"`    // empty for named collections
	Object       string `json:"object"`
	Engine       string `json:"engine"`     // ClickHouse engine family, tables only (e.g. ReplicatedMergeTree)
	Replicated   bool   `json:"replicated"` // true when the engine family is a Replicated* variant
	SQL          string `json:"sql"`        // statement without trailing ';'
	Manual       bool   `json:"manual"`     // operator-run only (e.g. MATERIALIZE INDEX); executors must skip it
	Unsafe       bool   `json:"unsafe"`     // this object has a change that can't be applied in place
	UnsafeReason string `json:"unsafe_reason"`
}

// JSONUnsafe is one destructive change that is never auto-emitted. The
// top-level list surfaces every UnsafeChange — including those that produce no
// statement (e.g. an engine or ORDER BY change that requires a full recreate) —
// so consumers of the JSON output see the same warnings the text mode prints as
// `-- UNSAFE:` comments.
type JSONUnsafe struct {
	Database string `json:"database"`
	Object   string `json:"object"`
	Reason   string `json:"reason"`
}

// DiffJSON is the top-level document emitted by `hclexp diff -format json`.
type DiffJSON struct {
	Operations []JSONOperation `json:"operations"`
	Unsafe     []JSONUnsafe    `json:"unsafe,omitempty"`
}

// RenderDiffJSON builds the structured, dependency-ordered operation list from a
// GenerateSQL result. The operation order is the index in the already
// dependency-sorted statement list. Engine/replicated are looked up from the
// resolved schemas — the target (right) for CREATE/ALTER, falling back to the
// current (left) for DROP — because an ALTER that does not change the engine
// carries no engine information of its own. Unsafe flags come from the existing
// gen.Unsafe list (no separate diff path), matched by database + object.
func RenderDiffJSON(gen GeneratedSQL, left, right *Schema) ([]byte, error) {
	doc := DiffJSON{Operations: make([]JSONOperation, 0, len(gen.Ops))}
	for i, op := range gen.Ops {
		engine := ""
		if op.ObjectType == KindTable {
			engine = engineFor(op.Database, op.Object, right, left)
		}
		unsafe, reason := unsafeFor(gen.Unsafe, op.Database, op.Object)
		doc.Operations = append(doc.Operations, JSONOperation{
			Order:        i,
			Kind:         op.Kind,
			ObjectType:   op.ObjectType,
			Database:     op.Database,
			Object:       op.Object,
			Engine:       engine,
			Replicated:   strings.HasPrefix(engine, "Replicated"),
			SQL:          op.SQL,
			Manual:       op.Manual,
			Unsafe:       unsafe,
			UnsafeReason: reason,
		})
	}
	for _, u := range gen.Unsafe {
		doc.Unsafe = append(doc.Unsafe, JSONUnsafe{Database: u.Database, Object: u.Table, Reason: u.Reason})
	}
	return json.MarshalIndent(doc, "", "  ")
}

// engineFor returns the ClickHouse engine family name (e.g.
// "ReplicatedMergeTree") of a table, searching the given schemas in order and
// returning the first match. Non-table objects and tables without a decoded
// engine yield "".
func engineFor(db, object string, schemas ...*Schema) string {
	for _, s := range schemas {
		if s == nil {
			continue
		}
		for di := range s.Databases {
			if s.Databases[di].Name != db {
				continue
			}
			for ti := range s.Databases[di].Tables {
				t := &s.Databases[di].Tables[ti]
				if t.Name != object {
					continue
				}
				if t.Engine == nil || t.Engine.Decoded == nil {
					return ""
				}
				return engineFamilyName(t.Engine.Decoded)
			}
		}
	}
	return ""
}

// engineFamilyName derives the ClickHouse family name from the decoded engine's
// Go type: EngineReplicatedMergeTree -> "ReplicatedMergeTree".
func engineFamilyName(e Engine) string {
	rt := reflect.TypeOf(e)
	for rt != nil && rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if rt == nil {
		return ""
	}
	return strings.TrimPrefix(rt.Name(), "Engine")
}

// unsafeFor reports whether the named object has a destructive change, returning
// its reason. An ALTER statement on a table that also has a separate unsafe
// change (e.g. a safe ADD COLUMN alongside an engine change) is flagged so the
// consumer knows the object needs manual attention.
func unsafeFor(unsafe []UnsafeChange, db, object string) (bool, string) {
	for _, u := range unsafe {
		if u.Database == db && u.Table == object {
			return true, u.Reason
		}
	}
	return false, ""
}
