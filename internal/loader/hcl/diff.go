package hcl

import (
	"reflect"
	"sort"
)

// ChangeSet describes the changes required to evolve a `from` schema into a
// `to` schema. Empty databases (no tables to add/drop/alter) are omitted.
type ChangeSet struct {
	Databases []DatabaseChange
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
	return true
}

func (dc DatabaseChange) IsEmpty() bool {
	return len(dc.AddTables) == 0 && len(dc.DropTables) == 0 && len(dc.AlterTables) == 0 &&
		len(dc.AddMaterializedViews) == 0 && len(dc.DropMaterializedViews) == 0 &&
		len(dc.AlterMaterializedViews) == 0 &&
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
			}
		case !tOK:
			dc = DatabaseChange{Database: name}
			for _, tbl := range f.Tables {
				dc.DropTables = append(dc.DropTables, tbl)
			}
			for _, mv := range f.MaterializedViews {
				dc.DropMaterializedViews = append(dc.DropMaterializedViews, mv.Name)
			}
		default:
			dc = diffDatabase(name, f, t)
		}
		if dc.IsEmpty() {
			continue
		}
		sortDatabaseChange(&dc)
		cs.Databases = append(cs.Databases, dc)
	}
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

func diffDatabase(name string, from, to *DatabaseSpec) DatabaseChange {
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
		td := diffTable(fromTables[n], t)
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

func diffTable(from, to *TableSpec) TableDiff {
	td := TableDiff{Table: to.Name}

	fromCols := indexColumns(from.Columns)
	toCols := indexColumns(to.Columns)

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
}
