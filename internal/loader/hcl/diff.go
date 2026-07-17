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

	AddRaws   []RawSpec   // emitted verbatim via the stored CREATE DDL
	DropRaws  []RawSpec   // emitted via a kind-mapped DROP
	AlterRaws []RawChange // emitted as DROP + CREATE (recreate)
}

// RawChange describes a change to a raw escape-hatch object. Raw SQL is
// opaque, so the only reconciliation is DROP + CREATE. Kind is the object's
// kind (drives the DROP form); OldSQL/NewSQL are the stored DDL on each side.
type RawChange struct {
	Kind   string
	Name   string
	OldSQL string
	NewSQL string
}

// IsUnsafe reports whether recreating the object risks data loss. Only a
// table holds rows on disk; recreating a view/dictionary/materialized_view is
// not destructive.
func (rc RawChange) IsUnsafe() bool { return rc.Kind == "table" }

// DictionaryDiff describes a change to a dictionary. ClickHouse has no
// useful in-place ALTER DICTIONARY, so any non-empty diff is materialized
// as a CREATE OR REPLACE DICTIONARY statement — safe, not flagged as
// unsafe. Changed lists field paths that differ, for rendering.
type DictionaryDiff struct {
	Name    string
	Changed []string

	// New is the target spec. The CREATE OR REPLACE DICTIONARY that
	// reconciles the change is rendered from it.
	New DictionarySpec

	// SkippedRedactedSecrets names source fields (the DDL argument name, e.g.
	// "password") whose comparison was suppressed because one side holds the
	// RedactedValue marker and the other a real value. hclexp cannot tell
	// whether they differ, so it says so rather than guessing. Mirrors
	// NamedCollectionChange.SkippedRedactedParams.
	SkippedRedactedSecrets []string
}

// IsEmpty counts a skipped secret as a difference: a dictionary whose only
// divergence is an unverifiable credential is reported (with no DDL) rather
// than certified equal.
func (d DictionaryDiff) IsEmpty() bool {
	return len(d.Changed) == 0 && len(d.SkippedRedactedSecrets) == 0
}

func (d DictionaryDiff) IsUnsafe() bool { return false }

// MaterializedViewDiff is the set of mutations to a single existing
// materialized view. A query-only change is applied in place via
// ALTER TABLE ... MODIFY QUERY; any structural change (to_table or the
// column list) requires recreating the view and is flagged unsafe.
type MaterializedViewDiff struct {
	Name        string
	QueryChange *StringChange // the AS SELECT body changed
	Recreate    bool          // to_table or the column list changed

	// Set alongside Recreate so consumers can tell WHAT forced it.
	ToTableChange  *StringChange
	ColumnsChanged bool
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

	// RecreateChanged names the attributes that forced Recreate, in a fixed
	// order, so consumers can report which one moved.
	RecreateChanged []string
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

	// Projections keyed by name; a modify is emitted as DROP + ADD (there
	// is no ALTER MODIFY PROJECTION). Adding one to an existing table also
	// generates a manual MATERIALIZE PROJECTION.
	AddProjections  []ProjectionSpec
	DropProjections []string

	EngineChange      *EngineChange
	OrderByChange     *OrderByChange
	PartitionByChange *StringChange
	SampleByChange    *StringChange
	TTLChange         *StringChange

	SettingsAdded   map[string]string
	SettingsRemoved []string
	SettingsChanged []SettingChange

	// Constraints keyed by name; a modify is emitted as DROP + ADD. All
	// in-place ALTER-able.
	AddConstraints    []ConstraintSpec
	DropConstraints   []string
	ModifyConstraints []ConstraintChange

	// PrimaryKeyChange is in-place-impossible (surfaced via unsafeReasons,
	// like ORDER BY). CommentChange is ALTER-able (MODIFY COMMENT).
	PrimaryKeyChange *OrderByChange
	CommentChange    *StringChange
}

// ConstraintChange is an in-name modification of a table constraint: its
// CHECK/ASSUME predicate differs between Old and New. ClickHouse has no
// MODIFY CONSTRAINT, so it is emitted as DROP CONSTRAINT + ADD CONSTRAINT.
type ConstraintChange struct {
	Name string
	Old  ConstraintSpec
	New  ConstraintSpec
}

// ColumnChange is an in-name modification of a column: its type and/or any
// modifier (default/materialized/ephemeral/alias/codec/ttl/comment/nullable)
// differs between Old and New. Name is the column's (current) name.
type ColumnChange struct {
	Name string
	Old  ColumnSpec
	New  ColumnSpec
}

// IsUnsafe reports whether the change switches the column's storage class —
// into or out of ALIAS / MATERIALIZED / EPHEMERAL. ClickHouse accepts such a
// MODIFY COLUMN but it is data-affecting (e.g. plain → ALIAS silently replaces
// stored values with the computed expression), so it is never auto-emitted.
// Changes within the same kind, or plain ↔ DEFAULT, plus codec/comment/ttl/
// nullable/type changes, are in-place safe.
func (c ColumnChange) IsUnsafe() bool {
	ok, nk := columnKind(c.Old), columnKind(c.New)
	if ok == nk {
		return false
	}
	computed := func(k string) bool {
		return k == "alias" || k == "materialized" || k == "ephemeral"
	}
	return computed(ok) || computed(nk)
}

// columnKind names a column's mutually-exclusive default form.
func columnKind(c ColumnSpec) string {
	switch {
	case c.Alias != nil:
		return "alias"
	case c.Materialized != nil:
		return "materialized"
	case c.Ephemeral != nil:
		return "ephemeral"
	case c.Default != nil:
		return "default"
	default:
		return "plain"
	}
}

// columnsEqual reports whether two columns are identical for diff purposes:
// same type and every modifier. Name is the comparison key (handled by the
// caller) and RenamedFrom is diff-transient, so neither is compared here.
func columnsEqual(a, b ColumnSpec) bool {
	return a.Type == b.Type &&
		a.Nullable == b.Nullable &&
		eqStrPtr(a.Default, b.Default) &&
		eqStrPtr(a.Materialized, b.Materialized) &&
		eqStrPtr(a.Ephemeral, b.Ephemeral) &&
		eqStrPtr(a.Alias, b.Alias) &&
		eqStrPtr(a.Codec, b.Codec) &&
		eqStrPtr(a.TTL, b.TTL) &&
		eqStrPtr(a.Comment, b.Comment)
}

func eqStrPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
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
		len(dc.AlterDictionaries) == 0 &&
		len(dc.AddRaws) == 0 && len(dc.DropRaws) == 0 && len(dc.AlterRaws) == 0
}

func (td TableDiff) IsEmpty() bool {
	return len(td.RenameColumns) == 0 &&
		len(td.AddColumns) == 0 && len(td.DropColumns) == 0 && len(td.ModifyColumns) == 0 &&
		len(td.AddIndexes) == 0 && len(td.DropIndexes) == 0 &&
		len(td.AddProjections) == 0 && len(td.DropProjections) == 0 &&
		td.EngineChange == nil && td.OrderByChange == nil &&
		td.PartitionByChange == nil && td.SampleByChange == nil && td.TTLChange == nil &&
		len(td.SettingsAdded) == 0 && len(td.SettingsRemoved) == 0 && len(td.SettingsChanged) == 0 &&
		len(td.AddConstraints) == 0 && len(td.DropConstraints) == 0 && len(td.ModifyConstraints) == 0 &&
		td.PrimaryKeyChange == nil && td.CommentChange == nil
}

// IsUnsafe reports whether the diff includes a change ClickHouse can't apply
// in place (engine swap, order_by/partition_by/sample_by, primary_key). Such
// changes require table recreation.
func (td TableDiff) IsUnsafe() bool {
	if td.EngineChange != nil || td.OrderByChange != nil ||
		td.PartitionByChange != nil || td.SampleByChange != nil ||
		td.PrimaryKeyChange != nil {
		return true
	}
	for _, c := range td.ModifyColumns {
		if c.IsUnsafe() {
			return true
		}
	}
	return false
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
				AddDictionaries:      append([]DictionarySpec(nil), t.Dictionaries...),
				AddRaws:              append([]RawSpec(nil), t.Raws...),
			}
		case !tOK:
			dc = DatabaseChange{Database: name}
			dc.DropTables = append(dc.DropTables, f.Tables...)
			for _, mv := range f.MaterializedViews {
				dc.DropMaterializedViews = append(dc.DropMaterializedViews, mv.Name)
			}
			for _, v := range f.Views {
				dc.DropViews = append(dc.DropViews, v.Name)
			}
			for _, d := range f.Dictionaries {
				dc.DropDictionaries = append(dc.DropDictionaries, d.Name)
			}
			dc.DropRaws = append(dc.DropRaws, f.Raws...)
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

	fromRaws := indexRaws(from.Raws)
	toRaws := indexRaws(to.Raws)
	for _, k := range sortedKeys(toRaws) {
		if _, ok := fromRaws[k]; !ok {
			dc.AddRaws = append(dc.AddRaws, *toRaws[k])
		}
	}
	for _, k := range sortedKeys(fromRaws) {
		if _, ok := toRaws[k]; !ok {
			dc.DropRaws = append(dc.DropRaws, *fromRaws[k])
		}
	}
	for _, k := range sortedKeys(fromRaws) {
		t, ok := toRaws[k]
		if !ok {
			continue
		}
		f := fromRaws[k]
		if !rawSQLEqual(f.SQL, t.SQL) {
			dc.AlterRaws = append(dc.AlterRaws, RawChange{
				Kind: t.Kind, Name: t.Name, OldSQL: f.SQL, NewSQL: t.SQL,
			})
		}
	}
	return dc
}

// indexRaws keys raw objects by (kind, name): a raw object's identity is its
// kind plus its name, so renaming the kind for the same name reads as a
// drop-of-old-kind plus an add-of-new-kind rather than an in-place change.
func indexRaws(rs []RawSpec) map[string]*RawSpec {
	out := make(map[string]*RawSpec, len(rs))
	for i := range rs {
		out[rs[i].Kind+"\x00"+rs[i].Name] = &rs[i]
	}
	return out
}

// rawSQLEqual compares two raw SQL bodies. Trailing newlines are never
// semantically significant in a CREATE statement, so they are ignored; all
// other whitespace is significant (it may sit inside a string literal) and is
// compared exactly.
func rawSQLEqual(a, b string) bool {
	return strings.TrimRight(a, "\n") == strings.TrimRight(b, "\n")
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
	d := DictionaryDiff{Name: to.Name, New: *to}
	if !reflect.DeepEqual(from.PrimaryKey, to.PrimaryKey) {
		d.Changed = append(d.Changed, "primary_key")
	}
	if !reflect.DeepEqual(from.Attributes, to.Attributes) {
		d.Changed = append(d.Changed, "attributes")
	}
	sourceEqual, unverifiable := compareDictSources(from.Source, to.Source)
	if !sourceEqual {
		d.Changed = append(d.Changed, "source")
	}
	d.SkippedRedactedSecrets = unverifiable
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

// compareDictSources compares two dictionary sources, accounting for a
// credential neither observer may be able to see (RedactedValue, "[HIDDEN]").
// It returns whether the sources are equal, plus the DDL argument names of any
// secret whose equality could not be established.
//
// Per secret field, three verdicts:
//
//	[HIDDEN] vs [HIDDEN] — equal, silently. Both sides are equally blind, which
//	  is the normal drift case (every node dumped by the same user). The blind
//	  spot is real and unfixable from here: two genuinely different hidden
//	  secrets read as equal. No observer without displaySecretsInShowAndSelect
//	  can do better.
//	[HIDDEN] vs a real value — unverifiable. The field is masked out of the
//	  equality check and reported, so an authored secret is never mistaken for
//	  a change against a cluster that would not show its own.
//	[HIDDEN] vs absent — a real difference. Present-vs-absent is visible even
//	  when the value is not.
//
// Every other field compares exactly as before.
func compareDictSources(a, b *DictionarySourceSpec) (equal bool, unverifiable []string) {
	if a == nil || b == nil {
		return a == b, nil
	}
	if a.Kind != b.Kind {
		return false, nil
	}

	da, db := a.Decoded, b.Decoded
	if da != nil && db != nil {
		field, aRedacted := dictSecretIsRedacted(da)
		_, bRedacted := dictSecretIsRedacted(db)
		_, av := dictSecret(da)
		_, bv := dictSecret(db)
		// Asymmetric redaction with a value on the other side: mask the field on
		// both copies so the rest of the source still compares, and report it.
		// (Redacted-vs-absent falls through: the marker differs from nil, so it
		// stays a genuine difference.)
		if aRedacted != bRedacted && av != nil && bv != nil {
			unverifiable = append(unverifiable, field)
			da = withDictSecret(da, nil)
			db = withDictSecret(db, nil)
		}
	}
	return reflect.DeepEqual(da, db), unverifiable
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
	if !reflect.DeepEqual(from.ColumnAliases, to.ColumnAliases) {
		vd.RecreateChanged = append(vd.RecreateChanged, "column_aliases")
	}
	if !reflect.DeepEqual(from.SQLSecurity, to.SQLSecurity) {
		vd.RecreateChanged = append(vd.RecreateChanged, "sql_security")
	}
	if !reflect.DeepEqual(from.Definer, to.Definer) {
		vd.RecreateChanged = append(vd.RecreateChanged, "definer")
	}
	if !reflect.DeepEqual(from.Cluster, to.Cluster) {
		vd.RecreateChanged = append(vd.RecreateChanged, "cluster")
	}
	if len(vd.RecreateChanged) > 0 {
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
	if from.ToTable != to.ToTable {
		o, n := from.ToTable, to.ToTable
		mvd.ToTableChange = &StringChange{Old: &o, New: &n}
	}
	if !reflect.DeepEqual(from.Columns, to.Columns) {
		mvd.ColumnsChanged = true
	}
	if mvd.ToTableChange != nil || mvd.ColumnsChanged {
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

// isSystemProxy reports whether t is a Distributed proxy over a system.*
// remote. System tables are server-defined and version-evolving, so any
// declared column subset is a valid proxy — the same reason validate's
// proxy-column check is subset-tolerant by default.
func isSystemProxy(t TableSpec) bool {
	d, ok := engineOf(t).(EngineDistributed)
	return ok && d.RemoteDatabase == "system"
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

	// A Distributed proxy over a system.* table declares a column subset by
	// design: the server owns the full set and grows it with every version,
	// so a proxy created from a fuller set (or on a newer server) carries
	// columns the layer intentionally omits — presence differences are not
	// drift and would otherwise block convergence forever (#136 item 4).
	// Compare only the columns declared on both sides; a type change on
	// those still surfaces. Suppression is symmetric (Diff has no notion of
	// which operand is live) and applies only when both sides are system
	// proxies — an engine change still yields the full column diff.
	// Non-system proxies keep exact semantics: their remotes are
	// layer-managed, so an extra column there is real drift.
	if isSystemProxy(*from) && isSystemProxy(*to) {
		for n := range fromCols {
			if _, ok := toCols[n]; !ok {
				delete(fromCols, n)
			}
		}
		for n := range toCols {
			if _, ok := fromCols[n]; !ok {
				delete(toCols, n)
			}
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
		// Type/modifier changes on a renamed column still need MODIFY COLUMN.
		if !columnsEqual(*fromCols[oldName], *toCol) {
			td.ModifyColumns = append(td.ModifyColumns, ColumnChange{
				Name: toCol.Name, Old: *fromCols[oldName], New: *toCol,
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
		if !columnsEqual(*f, *t) {
			td.ModifyColumns = append(td.ModifyColumns, ColumnChange{
				Name: n, Old: *f, New: *t,
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

	fromProj := indexProjections(from.Projections)
	toProj := indexProjections(to.Projections)
	for _, n := range sortedKeys(toProj) {
		f, ok := fromProj[n]
		if !ok {
			td.AddProjections = append(td.AddProjections, *toProj[n])
			continue
		}
		if !reflect.DeepEqual(*f, *toProj[n]) {
			td.DropProjections = append(td.DropProjections, n)
			td.AddProjections = append(td.AddProjections, *toProj[n])
		}
	}
	for _, n := range sortedKeys(fromProj) {
		if _, ok := toProj[n]; !ok {
			td.DropProjections = append(td.DropProjections, n)
		}
	}

	// Constraints, primary_key and comment. Computed before the TimeSeries
	// branch below so both it and the normal path pick them up. (Cluster is
	// intentionally not diffed: ON CLUSTER is not recoverable from a live
	// table, so diffing it would report spurious drift on every table.)
	fromCon := indexConstraints(from.Constraints)
	toCon := indexConstraints(to.Constraints)
	for _, n := range sortedKeys(toCon) {
		f, ok := fromCon[n]
		if !ok {
			td.AddConstraints = append(td.AddConstraints, *toCon[n])
			continue
		}
		if !reflect.DeepEqual(*f, *toCon[n]) {
			td.ModifyConstraints = append(td.ModifyConstraints, ConstraintChange{Name: n, Old: *f, New: *toCon[n]})
		}
	}
	for _, n := range sortedKeys(fromCon) {
		if _, ok := toCon[n]; !ok {
			td.DropConstraints = append(td.DropConstraints, n)
		}
	}
	td.PrimaryKeyChange = diffStringSlice(from.PrimaryKey, to.PrimaryKey)
	td.CommentChange = diffStringPtr(from.Comment, to.Comment)

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

func indexProjections(ps []ProjectionSpec) map[string]*ProjectionSpec {
	out := make(map[string]*ProjectionSpec, len(ps))
	for i := range ps {
		out[ps[i].Name] = &ps[i]
	}
	return out
}

func indexConstraints(cs []ConstraintSpec) map[string]*ConstraintSpec {
	out := make(map[string]*ConstraintSpec, len(cs))
	for i := range cs {
		out[cs[i].Name] = &cs[i]
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
	sort.Slice(dc.AddRaws, func(i, j int) bool { return dc.AddRaws[i].Name < dc.AddRaws[j].Name })
	sort.Slice(dc.DropRaws, func(i, j int) bool { return dc.DropRaws[i].Name < dc.DropRaws[j].Name })
	sort.Slice(dc.AlterRaws, func(i, j int) bool { return dc.AlterRaws[i].Name < dc.AlterRaws[j].Name })
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
