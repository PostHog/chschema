package hcl

import "strings"

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
func fieldChangesForDictionary(dd DictionaryDiff) []FieldChange {
	out := make([]FieldChange, 0, len(dd.Changed))
	for _, path := range dd.Changed {
		out = append(out, FieldChange{Field: path, Change: "modify"})
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
