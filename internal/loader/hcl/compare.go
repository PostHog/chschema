package hcl

import (
	"fmt"
	"strings"
)

// Comparison statuses. Status is right-relative: "added" means the right
// side of the Diff has the object and the left does not.
const (
	StatusAdded   = "added"
	StatusDropped = "dropped"
	StatusAltered = "altered"
)

// ObjectComparison describes how one object differs between two schemas. It
// is a projection of the ChangeSet — the same structs the text summary
// renders — so JSON, text, and counts can never contradict each other.
type ObjectComparison struct {
	Database   string `json:"database"` // empty for named collections
	Object     string `json:"object"`
	ObjectType string `json:"object_type"` // table | materialized_view | view | dictionary | named_collection | raw

	// RawKind is a raw{} block's inner object kind (table | view |
	// dictionary | materialized_view). Only a raw table holds rows on disk,
	// so its DROP+CREATE recreate is the destructive one — the kind is what
	// tells a consumer that apart.
	RawKind string `json:"raw_kind,omitempty"`

	Status       string          `json:"status"`            // added | dropped | altered
	Changes      []FieldChange   `json:"changes,omitempty"` // altered only
	Operations   []JSONOperation `json:"operations"`        // the DDL that reconciles this object; may be empty (unsafe-only changes)
	Unsafe       bool            `json:"unsafe"`
	UnsafeReason string          `json:"unsafe_reason,omitempty"`
	Error        string          `json:"error,omitempty"` // unsupported transition (e.g. named collection external<->managed)
}

// FieldChange is one attribute-level difference on an altered object. The
// Field vocabulary is a public contract, documented in docs/README.hcl.md.
type FieldChange struct {
	Field  string `json:"field"`
	Change string `json:"change"` // add | drop | modify | rename
	Old    string `json:"old,omitempty"`
	New    string `json:"new,omitempty"`
}

// CompareSummary counts comparisons by object type and status.
type CompareSummary struct {
	TablesAdded             int `json:"tables_added"`
	TablesDropped           int `json:"tables_dropped"`
	TablesAltered           int `json:"tables_altered"`
	MVsAdded                int `json:"mvs_added"`
	MVsDropped              int `json:"mvs_dropped"`
	MVsAltered              int `json:"mvs_altered"`
	ViewsAdded              int `json:"views_added"`
	ViewsDropped            int `json:"views_dropped"`
	ViewsAltered            int `json:"views_altered"`
	DictsAdded              int `json:"dicts_added"`
	DictsDropped            int `json:"dicts_dropped"`
	DictsAltered            int `json:"dicts_altered"`
	RawsAdded               int `json:"raws_added"`
	RawsDropped             int `json:"raws_dropped"`
	RawsAltered             int `json:"raws_altered"`
	NamedCollectionsChanged int `json:"named_collections_changed"`
}

// BuildObjectComparisons flattens a ChangeSet into one entry per differing
// object, attaching each generated operation to its object. Status is
// right-relative; nested operations keep their global dependency order, so
// the object view and the flat operation list can never disagree about
// sequencing.
func BuildObjectComparisons(cs ChangeSet, gen GeneratedSQL, left, right *Schema) []ObjectComparison {
	type ref struct{ db, object string }
	opsByRef := map[ref][]JSONOperation{}
	for _, op := range buildJSONOperations(gen, left, right) {
		k := ref{op.Database, op.Object}
		opsByRef[k] = append(opsByRef[k], op)
	}

	// Non-nil so an empty diff marshals as "objects": [] (the jq contract in
	// check-live.sh is `.objects == []`), never null.
	out := []ObjectComparison{}
	add := func(db, name, objType, status string, changes []FieldChange) int {
		oc := ObjectComparison{
			Database: db, Object: name, ObjectType: objType, Status: status,
			Changes: changes, Operations: opsByRef[ref{db, name}],
		}
		if oc.Operations == nil {
			oc.Operations = []JSONOperation{}
		}
		oc.Unsafe, oc.UnsafeReason = unsafeFor(gen.Unsafe, db, name)
		out = append(out, oc)
		return len(out) - 1
	}

	for _, dc := range cs.Databases {
		for _, t := range dc.AddTables {
			add(dc.Database, t.Name, KindTable, StatusAdded, nil)
		}
		for _, t := range dc.DropTables {
			add(dc.Database, t.Name, KindTable, StatusDropped, nil)
		}
		for _, td := range dc.AlterTables {
			add(dc.Database, td.Table, KindTable, StatusAltered, fieldChangesForTable(td))
		}
		for _, mv := range dc.AddMaterializedViews {
			add(dc.Database, mv.Name, KindMaterializedView, StatusAdded, nil)
		}
		for _, name := range dc.DropMaterializedViews {
			add(dc.Database, name, KindMaterializedView, StatusDropped, nil)
		}
		for _, mvd := range dc.AlterMaterializedViews {
			add(dc.Database, mvd.Name, KindMaterializedView, StatusAltered, fieldChangesForMaterializedView(mvd))
		}
		for _, v := range dc.AddViews {
			add(dc.Database, v.Name, KindView, StatusAdded, nil)
		}
		for _, name := range dc.DropViews {
			add(dc.Database, name, KindView, StatusDropped, nil)
		}
		for _, vd := range dc.AlterViews {
			add(dc.Database, vd.Name, KindView, StatusAltered, fieldChangesForView(vd))
		}
		for _, d := range dc.AddDictionaries {
			add(dc.Database, d.Name, KindDictionary, StatusAdded, nil)
		}
		for _, name := range dc.DropDictionaries {
			add(dc.Database, name, KindDictionary, StatusDropped, nil)
		}
		for _, dd := range dc.AlterDictionaries {
			add(dc.Database, dd.Name, KindDictionary, StatusAltered, fieldChangesForDictionary(dd))
		}
		for _, r := range dc.AddRaws {
			out[add(dc.Database, r.Name, KindRaw, StatusAdded, nil)].RawKind = r.Kind
		}
		for _, r := range dc.DropRaws {
			out[add(dc.Database, r.Name, KindRaw, StatusDropped, nil)].RawKind = r.Kind
		}
		for _, rc := range dc.AlterRaws {
			i := add(dc.Database, rc.Name, KindRaw, StatusAltered, fieldChangesForRaw(rc))
			out[i].RawKind = rc.Kind
			if rc.IsUnsafe() && !out[i].Unsafe {
				out[i].Unsafe = true
				out[i].UnsafeReason = "recreating a raw table drops its data"
			}
		}
	}

	for _, ncc := range cs.NamedCollections {
		status := StatusAltered
		switch {
		case ncc.Recreate:
			// DROP+CREATE pair, surfaced as altered with an on_cluster change
		case ncc.Add != nil:
			status = StatusAdded
		case ncc.Drop:
			status = StatusDropped
		}
		i := add("", ncc.Name, KindNamedCollection, status, fieldChangesForNamedCollection(ncc))
		out[i].Error = ncc.Error
	}
	return out
}

// SummarizeComparisons counts comparisons by object type and status.
func SummarizeComparisons(objs []ObjectComparison) CompareSummary {
	var s CompareSummary
	for _, o := range objs {
		var added, dropped, altered *int
		switch o.ObjectType {
		case KindTable:
			added, dropped, altered = &s.TablesAdded, &s.TablesDropped, &s.TablesAltered
		case KindMaterializedView:
			added, dropped, altered = &s.MVsAdded, &s.MVsDropped, &s.MVsAltered
		case KindView:
			added, dropped, altered = &s.ViewsAdded, &s.ViewsDropped, &s.ViewsAltered
		case KindDictionary:
			added, dropped, altered = &s.DictsAdded, &s.DictsDropped, &s.DictsAltered
		case KindRaw:
			added, dropped, altered = &s.RawsAdded, &s.RawsDropped, &s.RawsAltered
		case KindNamedCollection:
			s.NamedCollectionsChanged++
			continue
		default:
			continue
		}
		switch o.Status {
		case StatusAdded:
			*added++
		case StatusDropped:
			*dropped++
		case StatusAltered:
			*altered++
		}
	}
	return s
}

// OneLiner renders the summary as the compact drift line, e.g.
// "+1 table, ~2 mv, ~1 raw". An all-zero summary falls back to "changed".
func (s CompareSummary) OneLiner() string {
	var parts []string
	add := func(label string, plus, minus, alt int) {
		if plus > 0 {
			parts = append(parts, fmt.Sprintf("+%d %s", plus, label))
		}
		if minus > 0 {
			parts = append(parts, fmt.Sprintf("-%d %s", minus, label))
		}
		if alt > 0 {
			parts = append(parts, fmt.Sprintf("~%d %s", alt, label))
		}
	}
	add("table", s.TablesAdded, s.TablesDropped, s.TablesAltered)
	add("mv", s.MVsAdded, s.MVsDropped, s.MVsAltered)
	add("dict", s.DictsAdded, s.DictsDropped, s.DictsAltered)
	add("view", s.ViewsAdded, s.ViewsDropped, s.ViewsAltered)
	add("raw", s.RawsAdded, s.RawsDropped, s.RawsAltered)
	if s.NamedCollectionsChanged > 0 {
		parts = append(parts, fmt.Sprintf("~%d named_collection", s.NamedCollectionsChanged))
	}
	if len(parts) == 0 {
		return "changed"
	}
	return strings.Join(parts, ", ")
}

// fieldChangesForTable flattens a TableDiff into attribute-level changes.
func fieldChangesForTable(td TableDiff) []FieldChange {
	var out []FieldChange
	for _, r := range td.RenameColumns {
		out = append(out, FieldChange{Field: "column:" + r.New, Change: "rename", Old: r.Old, New: r.New})
	}
	for _, c := range td.AddColumns {
		out = append(out, FieldChange{Field: "column:" + c.Name, Change: "add", New: columnDesc(c)})
	}
	for _, name := range td.DropColumns {
		out = append(out, FieldChange{Field: "column:" + name, Change: "drop"})
	}
	for _, c := range td.ModifyColumns {
		out = append(out, FieldChange{Field: "column:" + c.Name, Change: "modify", Old: columnDesc(c.Old), New: columnDesc(c.New)})
	}
	for _, idx := range td.AddIndexes {
		out = append(out, FieldChange{Field: "index:" + idx.Name, Change: "add"})
	}
	for _, name := range td.DropIndexes {
		out = append(out, FieldChange{Field: "index:" + name, Change: "drop"})
	}
	for _, p := range td.AddProjections {
		out = append(out, FieldChange{Field: "projection:" + p.Name, Change: "add"})
	}
	for _, name := range td.DropProjections {
		out = append(out, FieldChange{Field: "projection:" + name, Change: "drop"})
	}
	for _, c := range td.AddConstraints {
		out = append(out, FieldChange{Field: "constraint:" + c.Name, Change: "add"})
	}
	for _, name := range td.DropConstraints {
		out = append(out, FieldChange{Field: "constraint:" + name, Change: "drop"})
	}
	for _, c := range td.ModifyConstraints {
		out = append(out, FieldChange{Field: "constraint:" + c.Name, Change: "modify"})
	}
	if c := td.EngineChange; c != nil {
		oldSQL, _ := engineSQL(c.Old)
		newSQL, _ := engineSQL(c.New)
		out = append(out, FieldChange{Field: "engine", Change: "modify", Old: oldSQL, New: newSQL})
	}
	if c := td.OrderByChange; c != nil {
		out = append(out, FieldChange{Field: "order_by", Change: "modify",
			Old: strings.Join(c.Old, ", "), New: strings.Join(c.New, ", ")})
	}
	if c := td.PrimaryKeyChange; c != nil {
		out = append(out, FieldChange{Field: "primary_key", Change: "modify",
			Old: strings.Join(c.Old, ", "), New: strings.Join(c.New, ", ")})
	}
	if c := td.PartitionByChange; c != nil {
		out = append(out, stringChangeField("partition_by", c))
	}
	if c := td.SampleByChange; c != nil {
		out = append(out, stringChangeField("sample_by", c))
	}
	if c := td.TTLChange; c != nil {
		out = append(out, stringChangeField("ttl", c))
	}
	if c := td.CommentChange; c != nil {
		out = append(out, stringChangeField("comment", c))
	}
	for _, k := range sortedKeys(td.SettingsAdded) {
		out = append(out, FieldChange{Field: "setting:" + k, Change: "add", New: td.SettingsAdded[k]})
	}
	for _, k := range td.SettingsRemoved {
		out = append(out, FieldChange{Field: "setting:" + k, Change: "drop"})
	}
	for _, c := range td.SettingsChanged {
		out = append(out, FieldChange{Field: "setting:" + c.Key, Change: "modify", Old: c.OldValue, New: c.NewValue})
	}
	return out
}

// fieldChangesForMaterializedView flattens an MV diff. A structural change
// (to_table / columns) implies recreation; query-only maps to MODIFY QUERY.
func fieldChangesForMaterializedView(mvd MaterializedViewDiff) []FieldChange {
	var out []FieldChange
	if c := mvd.ToTableChange; c != nil {
		out = append(out, stringChangeField("to_table", c))
	}
	if mvd.ColumnsChanged {
		out = append(out, FieldChange{Field: "columns", Change: "modify"})
	}
	if c := mvd.QueryChange; c != nil {
		out = append(out, stringChangeField("query", c))
	}
	return out
}

func fieldChangesForView(vd ViewDiff) []FieldChange {
	var out []FieldChange
	for _, attr := range vd.RecreateChanged {
		out = append(out, FieldChange{Field: attr, Change: "modify"})
	}
	if c := vd.QueryChange; c != nil {
		out = append(out, stringChangeField("query", c))
	}
	if c := vd.Comment; c != nil {
		out = append(out, stringChangeField("comment", c))
	}
	return out
}

// fieldChangesForDictionary maps each changed field path to one modify entry
// (ClickHouse dictionaries reconcile via CREATE OR REPLACE, so there are no
// per-field old/new values in the diff).
//
// A source secret hclexp could not verify is reported as [HIDDEN] on BOTH
// sides even though one side's real value is known: diff output lands in CI
// logs, and a real credential must never be printed there.
func fieldChangesForDictionary(dd DictionaryDiff) []FieldChange {
	out := make([]FieldChange, 0, len(dd.Changed)+len(dd.SkippedRedactedSecrets))
	for _, path := range dd.Changed {
		out = append(out, FieldChange{Field: path, Change: "modify"})
	}
	for _, field := range dd.SkippedRedactedSecrets {
		out = append(out, FieldChange{Field: "source." + field, Change: "modify", Old: RedactedValue, New: RedactedValue})
	}
	return out
}

// fieldChangesForRaw: raw SQL is opaque, the whole stored DDL is the value.
func fieldChangesForRaw(rc RawChange) []FieldChange {
	return []FieldChange{{Field: "sql", Change: "modify", Old: rc.OldSQL, New: rc.NewSQL}}
}

// fieldChangesForNamedCollection flattens the surgical NC changes. SetParams
// has no old value (ALTER ... SET overwrites); a redacted param reports
// [HIDDEN] on both sides — the diff could not verify equality.
func fieldChangesForNamedCollection(ncc NamedCollectionChange) []FieldChange {
	var out []FieldChange
	if ncc.Recreate {
		out = append(out, FieldChange{Field: "on_cluster", Change: "modify"})
	}
	for _, p := range ncc.SetParams {
		out = append(out, FieldChange{Field: "param:" + p.Key, Change: "modify", New: p.Value})
	}
	for _, k := range ncc.DeleteParams {
		out = append(out, FieldChange{Field: "param:" + k, Change: "drop"})
	}
	for _, k := range ncc.SkippedRedactedParams {
		out = append(out, FieldChange{Field: "param:" + k, Change: "modify", Old: RedactedValue, New: RedactedValue})
	}
	if c := ncc.CommentChange; c != nil {
		out = append(out, stringChangeField("comment", c))
	}
	return out
}

// stringChangeField maps an optional-string transition to a FieldChange; a
// nil side stays empty and is omitted from JSON.
func stringChangeField(field string, c *StringChange) FieldChange {
	fc := FieldChange{Field: field, Change: "modify"}
	if c.Old != nil {
		fc.Old = *c.Old
	}
	if c.New != nil {
		fc.New = *c.New
	}
	return fc
}

// columnDesc renders a compact one-line column descriptor (type plus default
// form and codec/ttl/comment markers) for FieldChange values and the text
// summary.
func columnDesc(c ColumnSpec) string {
	t := c.Type
	if c.Nullable {
		t = "Nullable(" + t + ")"
	}
	switch {
	case c.Alias != nil:
		t += " ALIAS " + *c.Alias
	case c.Materialized != nil:
		t += " MATERIALIZED " + *c.Materialized
	case c.Ephemeral != nil:
		t += " EPHEMERAL"
	case c.Default != nil:
		t += " DEFAULT " + *c.Default
	}
	if c.Codec != nil {
		t += " CODEC(" + *c.Codec + ")"
	}
	if c.TTL != nil {
		t += " TTL " + *c.TTL
	}
	if c.Comment != nil {
		t += " COMMENT " + *c.Comment
	}
	return t
}
