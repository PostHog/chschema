package hcl

import (
	"reflect"
	"sort"
	"strings"
)

// ChangeSet describes the changes required to evolve a `from` schema into a
// `to` schema. Empty databases (no tables to add/drop/alter) are omitted.
type ChangeSet struct {
	Databases        []DatabaseChange
	NamedCollections []NamedCollectionChange
}

// DatabaseChange holds the per-database differences.
type DatabaseChange struct {
	Database    string
	AddTables   []TableSpec // emitted via CREATE TABLE
	DropTables  []TableSpec // emitted via DROP TABLE
	AlterTables []TableDiff

	AddMaterializedViews   []MaterializedViewSpec // emitted via CREATE MATERIALIZED VIEW
	DropMaterializedViews  []string               // emitted via DROP VIEW
	AlterMaterializedViews []MaterializedViewDiff

	AddDictionaries   []DictionarySpec // emitted via CREATE OR REPLACE DICTIONARY
	DropDictionaries  []string         // emitted via DROP DICTIONARY
	AlterDictionaries []DictionaryDiff

	AddViews   []ViewSpec // emitted via CREATE VIEW
	DropViews  []string   // emitted via DROP VIEW
	AlterViews []ViewDiff
}

// DictionaryDiff describes a change to a dictionary. ClickHouse has no
// useful in-place ALTER DICTIONARY, so any non-empty diff is materialized
// as a CREATE OR REPLACE DICTIONARY statement — safe, not flagged as
// unsafe. Changed lists field paths that differ, for rendering.
type DictionaryDiff struct {
	Name    string
	Changed []string
}

func (d DictionaryDiff) IsEmpty() bool  { return len(d.Changed) == 0 }
func (d DictionaryDiff) IsUnsafe() bool { return false }

// MaterializedViewDiff is the set of mutations to a single existing
// materialized view. A query-only change is applied in place via
// ALTER TABLE ... MODIFY QUERY; any structural change (to_table or the
// column list) requires recreating the view and is flagged unsafe.
type MaterializedViewDiff struct {
	Name        string
	QueryChange *StringChange // the AS SELECT body changed
	Recreate    bool          // to_table or the column list changed
}

func (mvd MaterializedViewDiff) IsEmpty() bool {
	return mvd.QueryChange == nil && !mvd.Recreate
}

// IsUnsafe reports whether the diff requires recreating the view (ClickHouse
// can't change a materialized view's destination or columns in place).
func (mvd MaterializedViewDiff) IsUnsafe() bool {
	return mvd.Recreate
}

// ViewDiff is the set of mutations to a single existing plain view. A
// query-only change is applied in place via ALTER TABLE ... MODIFY
// QUERY. A comment-only change is applied via ALTER TABLE ... MODIFY
// COMMENT. Any change to ColumnAliases / SQLSecurity / Definer /
// Cluster requires recreating the view (DROP + CREATE) and is flagged
// unsafe.
type ViewDiff struct {
	Name        string
	QueryChange *StringChange
	Comment     *StringChange
	Recreate    bool
}

func (vd ViewDiff) IsEmpty() bool {
	return vd.QueryChange == nil && vd.Comment == nil && !vd.Recreate
}

func (vd ViewDiff) IsUnsafe() bool { return vd.Recreate }

// TableDiff is the set of mutations to a single existing table. Any unset
// field means "no change in that aspect."
type TableDiff struct {
	Table string

	RenameColumns []RenameColumn
	AddColumns    []ColumnSpec
	DropColumns   []string
	ModifyColumns []ColumnChange

	AddIndexes  []IndexSpec
	DropIndexes []string

	EngineChange      *EngineChange
	OrderByChange     *OrderByChange
	PartitionByChange *StringChange
	SampleByChange    *StringChange
	TTLChange         *StringChange

	SettingsAdded   map[string]string
	SettingsRemoved []string
	SettingsChanged []SettingChange
}

type ColumnChange struct {
	Name    string
	OldType string
	NewType string
}

// RenameColumn captures an explicit renamed_from directive that the diff
// engine has matched against the source schema. Old is the previous name,
// New is the current name.
type RenameColumn struct {
	Old string
	New string
}

type EngineChange struct {
	Old Engine
	New Engine
}

type OrderByChange struct {
	Old []string
	New []string
}

// StringChange describes the transition of an optional string-valued
// attribute. A nil pointer on a side means "unset on that side."
type StringChange struct {
	Old *string
	New *string
}

type SettingChange struct {
	Key      string
	OldValue string
	NewValue string
}

// IsEmpty reports whether the change set has no work to do.
func (cs ChangeSet) IsEmpty() bool {
	for _, dc := range cs.Databases {
		if !dc.IsEmpty() {
			return false
		}
	}
	for _, ncc := range cs.NamedCollections {
		if !ncc.IsEmpty() {
			return false
		}
	}
	return true
}

func (dc DatabaseChange) IsEmpty() bool {
	return len(dc.AddTables) == 0 && len(dc.DropTables) == 0 && len(dc.AlterTables) == 0 &&
		len(dc.AddMaterializedViews) == 0 && len(dc.DropMaterializedViews) == 0 &&
		len(dc.AlterMaterializedViews) == 0 &&
		len(dc.AddViews) == 0 && len(dc.DropViews) == 0 &&
		len(dc.AlterViews) == 0 &&
		len(dc.AddDictionaries) == 0 && len(dc.DropDictionaries) == 0 &&
		len(dc.AlterDictionaries) == 0
}

func (td TableDiff) IsEmpty() bool {
	return len(td.RenameColumns) == 0 &&
		len(td.AddColumns) == 0 && len(td.DropColumns) == 0 && len(td.ModifyColumns) == 0 &&
		len(td.AddIndexes) == 0 && len(td.DropIndexes) == 0 &&
		td.EngineChange == nil && td.OrderByChange == nil &&
		td.PartitionByChange == nil && td.SampleByChange == nil && td.TTLChange == nil &&
		len(td.SettingsAdded) == 0 && len(td.SettingsRemoved) == 0 && len(td.SettingsChanged) == 0
}

// IsUnsafe reports whether the diff includes a change ClickHouse can't apply
// in place (engine swap, order_by/partition_by change). Such changes require
// table recreation.
func (td TableDiff) IsUnsafe() bool {
	return td.EngineChange != nil || td.OrderByChange != nil ||
		td.PartitionByChange != nil || td.SampleByChange != nil
}

// Diff compares two resolved schemas and returns a deterministic ChangeSet.
// Both inputs must already have been resolved (engines decoded, abstracts
// dropped, extend/patches consumed).
func Diff(from, to *Schema) ChangeSet {
	if from == nil {
		from = &Schema{}
	}
	if to == nil {
		to = &Schema{}
	}
	fromIdx := indexDatabases(from.Databases)
	toIdx := indexDatabases(to.Databases)
	names := mergedKeys(fromIdx, toIdx)

	// Resolvers built once per side so the virtual-column guard in
	// diffTable can resolve Distributed → remote-table transitive sets.
	fromR := NewSchemaResolver(from.Databases)
	toR := NewSchemaResolver(to.Databases)

	var cs ChangeSet
	for _, name := range names {
		f, fOK := fromIdx[name]
		t, tOK := toIdx[name]

		var dc DatabaseChange
		switch {
		case !fOK:
			dc = DatabaseChange{
				Database:             name,
				AddTables:            append([]TableSpec(nil), t.Tables...),
				AddMaterializedViews: append([]MaterializedViewSpec(nil), t.MaterializedViews...),
				AddViews:             append([]ViewSpec(nil), t.Views...),
			}
		case !tOK:
			dc = DatabaseChange{Database: name}
			for _, tbl := range f.Tables {
				dc.DropTables = append(dc.DropTables, tbl)
			}
			for _, mv := range f.MaterializedViews {
				dc.DropMaterializedViews = append(dc.DropMaterializedViews, mv.Name)
			}
			for _, v := range f.Views {
				dc.DropViews = append(dc.DropViews, v.Name)
			}
		default:
			dc = diffDatabase(name, f, t, fromR, toR)
		}
		if dc.IsEmpty() {
			continue
		}
		sortDatabaseChange(&dc)
		cs.Databases = append(cs.Databases, dc)
	}
	cs.NamedCollections = diffNamedCollections(from.NamedCollections, to.NamedCollections)
	return cs
}

func indexDatabases(dbs []DatabaseSpec) map[string]*DatabaseSpec {
	out := make(map[string]*DatabaseSpec, len(dbs))
	for i := range dbs {
		out[dbs[i].Name] = &dbs[i]
	}
	return out
}

func mergedKeys(a, b map[string]*DatabaseSpec) []string {
	seen := make(map[string]bool, len(a)+len(b))
	for k := range a {
		seen[k] = true
	}
	for k := range b {
		seen[k] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func diffDatabase(name string, from, to *DatabaseSpec, fromR, toR TableResolver) DatabaseChange {
	dc := DatabaseChange{Database: name}

	fromTables := indexTables(from.Tables)
	toTables := indexTables(to.Tables)

	for _, n := range sortedKeys(toTables) {
		if _, ok := fromTables[n]; !ok {
			dc.AddTables = append(dc.AddTables, *toTables[n])
		}
	}
	for _, n := range sortedKeys(fromTables) {
		if _, ok := toTables[n]; !ok {
			dc.DropTables = append(dc.DropTables, *fromTables[n])
		}
	}
	for _, n := range sortedKeys(fromTables) {
		t, ok := toTables[n]
		if !ok {
			continue
		}
		td := diffTable(fromTables[n], t, fromR, toR)
		if !td.IsEmpty() {
			dc.AlterTables = append(dc.AlterTables, td)
		}
	}

	fromMVs := indexMaterializedViews(from.MaterializedViews)
	toMVs := indexMaterializedViews(to.MaterializedViews)
	for _, n := range sortedKeys(toMVs) {
		if _, ok := fromMVs[n]; !ok {
			dc.AddMaterializedViews = append(dc.AddMaterializedViews, *toMVs[n])
		}
	}
	for _, n := range sortedKeys(fromMVs) {
		if _, ok := toMVs[n]; !ok {
			dc.DropMaterializedViews = append(dc.DropMaterializedViews, n)
		}
	}
	for _, n := range sortedKeys(fromMVs) {
		t, ok := toMVs[n]
		if !ok {
			continue
		}
		mvd := diffMaterializedView(fromMVs[n], t)
		if !mvd.IsEmpty() {
			dc.AlterMaterializedViews = append(dc.AlterMaterializedViews, mvd)
		}
	}

	fromViews := indexViews(from.Views)
	toViews := indexViews(to.Views)
	for _, n := range sortedKeys(toViews) {
		if _, ok := fromViews[n]; !ok {
			dc.AddViews = append(dc.AddViews, *toViews[n])
		}
	}
	for _, n := range sortedKeys(fromViews) {
		if _, ok := toViews[n]; !ok {
			dc.DropViews = append(dc.DropViews, n)
		}
	}
	for _, n := range sortedKeys(fromViews) {
		t, ok := toViews[n]
		if !ok {
			continue
		}
		vd := diffView(fromViews[n], t)
		if !vd.IsEmpty() {
			dc.AlterViews = append(dc.AlterViews, vd)
		}
	}

	fromDicts := indexDictionaries(from.Dictionaries)
	toDicts := indexDictionaries(to.Dictionaries)
	for _, n := range sortedKeys(toDicts) {
		if _, ok := fromDicts[n]; !ok {
			dc.AddDictionaries = append(dc.AddDictionaries, *toDicts[n])
		}
	}
	for _, n := range sortedKeys(fromDicts) {
		if _, ok := toDicts[n]; !ok {
			dc.DropDictionaries = append(dc.DropDictionaries, n)
		}
	}
	for _, n := range sortedKeys(fromDicts) {
		t, ok := toDicts[n]
		if !ok {
			continue
		}
		dd := diffDictionary(fromDicts[n], t)
		if !dd.IsEmpty() {
			dc.AlterDictionaries = append(dc.AlterDictionaries, dd)
		}
	}
	return dc
}

func indexDictionaries(ds []DictionarySpec) map[string]*DictionarySpec {
	out := make(map[string]*DictionarySpec, len(ds))
	for i := range ds {
		out[ds[i].Name] = &ds[i]
	}
	return out
}

// diffDictionary walks two dictionaries field-by-field and records every
// path that differs. Source/layout comparison uses reflect.DeepEqual on
// the decoded typed value (Body and Kind are diff-skipped artifacts).
func diffDictionary(from, to *DictionarySpec) DictionaryDiff {
	d := DictionaryDiff{Name: to.Name}
	if !reflect.DeepEqual(from.PrimaryKey, to.PrimaryKey) {
		d.Changed = append(d.Changed, "primary_key")
	}
	if !reflect.DeepEqual(from.Attributes, to.Attributes) {
		d.Changed = append(d.Changed, "attributes")
	}
	if !dictSourceEqual(from.Source, to.Source) {
		d.Changed = append(d.Changed, "source")
	}
	if !dictLayoutEqual(from.Layout, to.Layout) {
		d.Changed = append(d.Changed, "layout")
	}
	if !reflect.DeepEqual(from.Lifetime, to.Lifetime) {
		d.Changed = append(d.Changed, "lifetime")
	}
	if !reflect.DeepEqual(from.Range, to.Range) {
		d.Changed = append(d.Changed, "range")
	}
	if !reflect.DeepEqual(from.Settings, to.Settings) {
		d.Changed = append(d.Changed, "settings")
	}
	if !reflect.DeepEqual(from.Cluster, to.Cluster) {
		d.Changed = append(d.Changed, "cluster")
	}
	if !reflect.DeepEqual(from.Comment, to.Comment) {
		d.Changed = append(d.Changed, "comment")
	}
	return d
}

func dictSourceEqual(a, b *DictionarySourceSpec) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Kind == b.Kind && reflect.DeepEqual(a.Decoded, b.Decoded)
}

func dictLayoutEqual(a, b *DictionaryLayoutSpec) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Kind == b.Kind && reflect.DeepEqual(a.Decoded, b.Decoded)
}

func indexViews(views []ViewSpec) map[string]*ViewSpec {
	out := make(map[string]*ViewSpec, len(views))
	for i := range views {
		out[views[i].Name] = &views[i]
	}
	return out
}

// diffView compares two plain views with the same name. Query and Comment
// can be modified in place; ColumnAliases, SQLSecurity, Definer, and
// Cluster changes require DROP + CREATE.
func diffView(from, to *ViewSpec) ViewDiff {
	vd := ViewDiff{Name: to.Name}
	if !reflect.DeepEqual(from.ColumnAliases, to.ColumnAliases) ||
		!reflect.DeepEqual(from.SQLSecurity, to.SQLSecurity) ||
		!reflect.DeepEqual(from.Definer, to.Definer) ||
		!reflect.DeepEqual(from.Cluster, to.Cluster) {
		vd.Recreate = true
		return vd
	}
	if from.Query != to.Query {
		q1, q2 := from.Query, to.Query
		vd.QueryChange = &StringChange{Old: &q1, New: &q2}
	}
	if !reflect.DeepEqual(from.Comment, to.Comment) {
		vd.Comment = &StringChange{Old: from.Comment, New: to.Comment}
	}
	return vd
}

func indexMaterializedViews(mvs []MaterializedViewSpec) map[string]*MaterializedViewSpec {
	out := make(map[string]*MaterializedViewSpec, len(mvs))
	for i := range mvs {
		out[mvs[i].Name] = &mvs[i]
	}
	return out
}

// diffMaterializedView compares two materialized views with the same name. A
// changed to_table or column list can't be applied in place, so it sets
// Recreate; an otherwise-identical view with a changed query yields a
// QueryChange that maps to ALTER TABLE ... MODIFY QUERY. Recreate supersedes
// QueryChange — the two are mutually exclusive.
func diffMaterializedView(from, to *MaterializedViewSpec) MaterializedViewDiff {
	mvd := MaterializedViewDiff{Name: to.Name}
	if from.ToTable != to.ToTable || !reflect.DeepEqual(from.Columns, to.Columns) {
		mvd.Recreate = true
		return mvd
	}
	if from.Query != to.Query {
		q1, q2 := from.Query, to.Query
		mvd.QueryChange = &StringChange{Old: &q1, New: &q2}
	}
	return mvd
}

func indexTables(tables []TableSpec) map[string]*TableSpec {
	out := make(map[string]*TableSpec, len(tables))
	for i := range tables {
		out[tables[i].Name] = &tables[i]
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func diffTable(from, to *TableSpec, fromR, toR TableResolver) TableDiff {
	td := TableDiff{Table: to.Name}

	fromCols := indexColumns(from.Columns)
	toCols := indexColumns(to.Columns)

	// Asymmetric virtual-column guard. A column name recognised as
	// virtual by an engine is dropped from that side only when the
	// OTHER side has not declared it — that suppresses would-be
	// ADD/DROP DDL on a virtual (CH rejects DROP COLUMN of a virtual
	// and a stray "_offset" leak from a future system.columns-based
	// introspector would otherwise generate one). When both sides
	// declare the column (e.g. real `_key FixedString(8)` on both),
	// it stays and a type change still surfaces normally.
	fromVirtuals := virtualNameSet(engineOf(*from), fromR)
	toVirtuals := virtualNameSet(engineOf(*to), toR)
	for n := range fromCols {
		if !fromVirtuals[n] {
			continue
		}
		if _, declaredOnOther := toCols[n]; !declaredOnOther {
			delete(fromCols, n)
		}
	}
	for n := range toCols {
		if !toVirtuals[n] {
			continue
		}
		if _, declaredOnOther := fromCols[n]; !declaredOnOther {
			delete(toCols, n)
		}
	}

	// Resolve renamed_from directives: a rename applies only when the old
	// name exists in `from` AND the new name does not. This makes stale
	// directives (left in HCL after a prior apply) a no-op.
	renamed := map[string]bool{} // names in `from` consumed by a rename
	created := map[string]bool{} // names in `to` consumed by a rename
	for _, n := range sortedKeys(toCols) {
		toCol := toCols[n]
		if toCol.RenamedFrom == nil {
			continue
		}
		oldName := *toCol.RenamedFrom
		_, oldInFrom := fromCols[oldName]
		_, newInFrom := fromCols[toCol.Name]
		if !oldInFrom || newInFrom {
			continue
		}
		td.RenameColumns = append(td.RenameColumns, RenameColumn{Old: oldName, New: toCol.Name})
		renamed[oldName] = true
		created[toCol.Name] = true
		// Type changes on a renamed column still need MODIFY COLUMN.
		if fromCols[oldName].Type != toCol.Type {
			td.ModifyColumns = append(td.ModifyColumns, ColumnChange{
				Name: toCol.Name, OldType: fromCols[oldName].Type, NewType: toCol.Type,
			})
		}
	}

	for _, n := range sortedKeys(toCols) {
		if created[n] {
			continue
		}
		_, inFrom := fromCols[n]
		if !inFrom || renamed[n] {
			td.AddColumns = append(td.AddColumns, *toCols[n])
		}
	}
	for _, n := range sortedKeys(fromCols) {
		if renamed[n] {
			continue
		}
		if _, ok := toCols[n]; !ok {
			td.DropColumns = append(td.DropColumns, n)
		}
	}
	for _, n := range sortedKeys(fromCols) {
		if renamed[n] {
			continue
		}
		t, ok := toCols[n]
		if !ok {
			continue
		}
		f := fromCols[n]
		if f.Type != t.Type {
			td.ModifyColumns = append(td.ModifyColumns, ColumnChange{
				Name: n, OldType: f.Type, NewType: t.Type,
			})
		}
	}

	fromIdx := indexIndexes(from.Indexes)
	toIdx := indexIndexes(to.Indexes)
	for _, n := range sortedKeys(toIdx) {
		f, ok := fromIdx[n]
		if !ok {
			td.AddIndexes = append(td.AddIndexes, *toIdx[n])
			continue
		}
		if !reflect.DeepEqual(*f, *toIdx[n]) {
			td.DropIndexes = append(td.DropIndexes, n)
			td.AddIndexes = append(td.AddIndexes, *toIdx[n])
		}
	}
	for _, n := range sortedKeys(fromIdx) {
		if _, ok := toIdx[n]; !ok {
			td.DropIndexes = append(td.DropIndexes, n)
		}
	}

	// TimeSeries gets a smarter engine-diff path: changes to two specific
	// SETTINGS (id_generator, filter_by_min_time_and_max_time) are
	// ALTER-able in CH; everything else (other settings, target swaps,
	// tags_to_columns) is a recreate. This split lets the user evolve the
	// two alterable settings without losing the table.
	if fromTS, fromOK := engineOf(*from).(EngineTimeSeries); fromOK {
		if toTS, toOK := engineOf(*to).(EngineTimeSeries); toOK {
			diffTimeSeries(&td, from.Engine, to.Engine, fromTS, toTS)
			td.OrderByChange = diffStringSlice(from.OrderBy, to.OrderBy)
			td.PartitionByChange = diffStringPtr(from.PartitionBy, to.PartitionBy)
			td.SampleByChange = diffStringPtr(from.SampleBy, to.SampleBy)
			td.TTLChange = diffStringPtr(from.TTL, to.TTL)
			// Merge in table-level Settings (independent of engine settings
			// on TimeSeries). We append rather than overwrite so the engine
			// settings populated by diffTimeSeries above aren't dropped.
			added, removed, changed := diffSettings(from.Settings, to.Settings)
			for k, v := range added {
				if td.SettingsAdded == nil {
					td.SettingsAdded = map[string]string{}
				}
				td.SettingsAdded[k] = v
			}
			td.SettingsRemoved = append(td.SettingsRemoved, removed...)
			td.SettingsChanged = append(td.SettingsChanged, changed...)
			return td
		}
	}

	td.EngineChange = diffEngine(from.Engine, to.Engine)
	td.OrderByChange = diffStringSlice(from.OrderBy, to.OrderBy)
	td.PartitionByChange = diffStringPtr(from.PartitionBy, to.PartitionBy)
	td.SampleByChange = diffStringPtr(from.SampleBy, to.SampleBy)
	td.TTLChange = diffStringPtr(from.TTL, to.TTL)

	added, removed, changed := diffSettings(from.Settings, to.Settings)
	td.SettingsAdded = added
	td.SettingsRemoved = removed
	td.SettingsChanged = changed

	return td
}

// timeSeriesAlterableSettings names the SETTINGS keys CH supports via
// ALTER TABLE ... MODIFY SETTING on a TimeSeries table.
var timeSeriesAlterableSettings = map[string]bool{
	"id_generator":                    true,
	"filter_by_min_time_and_max_time": true,
}

// diffTimeSeries fills td.SettingsAdded/Removed/Changed for the two
// ALTER-able TimeSeries settings, and marks the rest of the engine diff
// as a recreate via EngineChange.
func diffTimeSeries(td *TableDiff, fromSpec, toSpec *EngineSpec, from, to EngineTimeSeries) {
	bakedDiffers := false
	for k, vNew := range to.Settings {
		vOld, ok := from.Settings[k]
		if !ok {
			if timeSeriesAlterableSettings[k] {
				if td.SettingsAdded == nil {
					td.SettingsAdded = map[string]string{}
				}
				td.SettingsAdded[k] = vNew
			} else {
				bakedDiffers = true
			}
			continue
		}
		if vOld != vNew {
			if timeSeriesAlterableSettings[k] {
				td.SettingsChanged = append(td.SettingsChanged, SettingChange{Key: k, OldValue: vOld, NewValue: vNew})
			} else {
				bakedDiffers = true
			}
		}
	}
	for k := range from.Settings {
		if _, ok := to.Settings[k]; ok {
			continue
		}
		if timeSeriesAlterableSettings[k] {
			td.SettingsRemoved = append(td.SettingsRemoved, k)
		} else {
			bakedDiffers = true
		}
	}

	if !reflect.DeepEqual(from.TagsToColumns, to.TagsToColumns) {
		bakedDiffers = true
	}
	if !reflect.DeepEqual(from.Samples, to.Samples) ||
		!reflect.DeepEqual(from.Tags, to.Tags) ||
		!reflect.DeepEqual(from.Metrics, to.Metrics) {
		bakedDiffers = true
	}

	if bakedDiffers {
		td.EngineChange = &EngineChange{Old: from, New: to}
	}
}

func indexColumns(cols []ColumnSpec) map[string]*ColumnSpec {
	out := make(map[string]*ColumnSpec, len(cols))
	for i := range cols {
		out[cols[i].Name] = &cols[i]
	}
	return out
}

func indexIndexes(idx []IndexSpec) map[string]*IndexSpec {
	out := make(map[string]*IndexSpec, len(idx))
	for i := range idx {
		out[idx[i].Name] = &idx[i]
	}
	return out
}

func diffEngine(from, to *EngineSpec) *EngineChange {
	var fromE, toE Engine
	if from != nil {
		fromE = from.Decoded
	}
	if to != nil {
		toE = to.Decoded
	}
	if reflect.DeepEqual(fromE, toE) {
		return nil
	}
	return &EngineChange{Old: fromE, New: toE}
}

func diffStringSlice(from, to []string) *OrderByChange {
	if len(from) == 0 && len(to) == 0 {
		return nil
	}
	if reflect.DeepEqual(from, to) {
		return nil
	}
	return &OrderByChange{Old: from, New: to}
}

func diffStringPtr(from, to *string) *StringChange {
	if from == nil && to == nil {
		return nil
	}
	if from != nil && to != nil && *from == *to {
		return nil
	}
	return &StringChange{Old: from, New: to}
}

func diffSettings(from, to map[string]string) (added map[string]string, removed []string, changed []SettingChange) {
	if from == nil && to == nil {
		return nil, nil, nil
	}
	for _, k := range sortedKeys(to) {
		if _, ok := from[k]; !ok {
			if added == nil {
				added = map[string]string{}
			}
			added[k] = to[k]
		}
	}
	for _, k := range sortedKeys(from) {
		v, ok := to[k]
		switch {
		case !ok:
			removed = append(removed, k)
		case v != from[k]:
			changed = append(changed, SettingChange{Key: k, OldValue: from[k], NewValue: v})
		}
	}
	return added, removed, changed
}

func sortDatabaseChange(dc *DatabaseChange) {
	sort.Slice(dc.AddTables, func(i, j int) bool { return dc.AddTables[i].Name < dc.AddTables[j].Name })
	sort.Slice(dc.DropTables, func(i, j int) bool { return dc.DropTables[i].Name < dc.DropTables[j].Name })
	sort.Slice(dc.AlterTables, func(i, j int) bool { return dc.AlterTables[i].Table < dc.AlterTables[j].Table })
	sort.Slice(dc.AddMaterializedViews, func(i, j int) bool {
		return dc.AddMaterializedViews[i].Name < dc.AddMaterializedViews[j].Name
	})
	sort.Strings(dc.DropMaterializedViews)
	sort.Slice(dc.AlterMaterializedViews, func(i, j int) bool {
		return dc.AlterMaterializedViews[i].Name < dc.AlterMaterializedViews[j].Name
	})
	sort.Slice(dc.AddDictionaries, func(i, j int) bool {
		return dc.AddDictionaries[i].Name < dc.AddDictionaries[j].Name
	})
	sort.Strings(dc.DropDictionaries)
	sort.Slice(dc.AlterDictionaries, func(i, j int) bool {
		return dc.AlterDictionaries[i].Name < dc.AlterDictionaries[j].Name
	})
	sort.Slice(dc.AddViews, func(i, j int) bool { return dc.AddViews[i].Name < dc.AddViews[j].Name })
	sort.Strings(dc.DropViews)
	sort.Slice(dc.AlterViews, func(i, j int) bool { return dc.AlterViews[i].Name < dc.AlterViews[j].Name })
}

// virtualNameSet returns the names recognised as virtual on engine e in
// the context of resolver r. Includes the bare Nested parent for any
// dot-access name (e.g. "_headers" when "_headers.name" is present), so
// a stray declared parent is recognised even though Virtuals() only
// reports the dot-access form.
func virtualNameSet(e Engine, r TableResolver) map[string]bool {
	if e == nil {
		return nil
	}
	cols := VirtualColumnsFor(e, r)
	if len(cols) == 0 {
		return nil
	}
	out := make(map[string]bool, len(cols)+1)
	hasHeadersDotted := false
	for _, c := range cols {
		out[c.Name] = true
		if !hasHeadersDotted && strings.HasPrefix(c.Name, "_headers.") {
			hasHeadersDotted = true
		}
	}
	if hasHeadersDotted {
		out["_headers"] = true
	}
	return out
}
