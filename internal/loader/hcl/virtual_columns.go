package hcl

// EngineWithVirtuals is the optional interface an Engine implements when
// ClickHouse exposes implicit columns on tables of that kind. The returned
// set is static — derived from the engine's settings only, not from other
// tables in the schema. Engines with no virtuals (e.g. Log) simply don't
// implement it.
type EngineWithVirtuals interface {
	Engine
	Virtuals() []DeclaredColumn
}

// EngineWithDynamicVirtuals is implemented by engines whose implicit
// column set depends on other tables in the schema — for example,
// Distributed, which exposes _shard_num plus the virtuals of its remote
// table. When an engine implements both interfaces, the dynamic form is
// authoritative.
type EngineWithDynamicVirtuals interface {
	Engine
	DynamicVirtuals(r TableResolver) []DeclaredColumn
}

// TableResolver is the minimum lookup surface dynamic-virtuals engines
// need. Pass nil when no schema context is available — dynamic engines
// must degrade gracefully (Distributed returns just _shard_num).
type TableResolver interface {
	LookupTable(db, name string) (TableSpec, bool)
}

// VirtualColumnsFor returns the engine-defined implicit columns for e,
// or nil if the engine has none / e is nil. Dispatch is by interface
// assertion, not by Kind() switch — adding a new engine with virtuals
// only requires implementing one of the optional interfaces.
func VirtualColumnsFor(e Engine, r TableResolver) []DeclaredColumn {
	if e == nil {
		return nil
	}
	if d, ok := e.(EngineWithDynamicVirtuals); ok {
		return d.DynamicVirtuals(r)
	}
	if v, ok := e.(EngineWithVirtuals); ok {
		return v.Virtuals()
	}
	return nil
}

// IsVirtualColumn reports whether name is one of the implicit columns the
// engine contributes. Used as a name guard in the diff path; mode- and
// settings-independent on purpose — Kafka's stream-mode extras are
// included in the membership test so a column never silently shifts
// status when a setting changes.
//
// The bare Nested parent (e.g. "_headers" for Kafka) is also recognised
// in addition to the dot-access form ("_headers.name").
func IsVirtualColumn(e Engine, name string) bool {
	for _, c := range allVirtualNames(e) {
		if c == name {
			return true
		}
	}
	return false
}

// ColumnsProvidedBy returns every column readable from t: declared
// columns first (in declaration order), then engine-contributed virtual
// columns the user has not shadowed by declaring a same-named column.
//
// Declared wins: a user-written `column "_key" { ... }` on a Kafka table
// (e.g. via kafka_map_virtual_columns_on_write) appears in the output
// and the virtual is suppressed.
func ColumnsProvidedBy(t TableSpec, r TableResolver) []DeclaredColumn {
	out := make([]DeclaredColumn, 0, len(t.Columns))
	declared := make(map[string]bool, len(t.Columns))
	for _, c := range t.Columns {
		declared[c.Name] = true
		out = append(out, DeclaredColumn{Name: c.Name, Type: c.Type})
	}
	if t.Engine == nil {
		return out
	}
	for _, v := range VirtualColumnsFor(t.Engine.Decoded, r) {
		if !declared[v.Name] {
			out = append(out, v)
		}
	}
	return out
}

// schemaResolver is the standard TableResolver, indexing every table in a
// flat slice of DatabaseSpec by "db.name". Pass the post-Resolve()
// databases — abstract bases are dropped and Engine.Decoded is populated.
type schemaResolver struct {
	byKey map[string]TableSpec
}

// NewSchemaResolver builds a resolver over the loaded databases. Safe to
// call with an empty slice; the resolver returns ok=false for every
// lookup. Tables collide on (db, name); the last entry wins (Resolve
// already errors on duplicates within a single db, so collisions only
// happen across multiple DatabaseSpec entries for the same db name —
// rare, and the legacy multi-layer loader already merged them).
func NewSchemaResolver(dbs []DatabaseSpec) TableResolver {
	r := &schemaResolver{byKey: make(map[string]TableSpec)}
	for _, db := range dbs {
		for _, t := range db.Tables {
			r.byKey[db.Name+"."+t.Name] = t
		}
	}
	return r
}

func (r *schemaResolver) LookupTable(db, name string) (TableSpec, bool) {
	t, ok := r.byKey[db+"."+name]
	return t, ok
}

// allVirtualNames returns the full name set IsVirtualColumn considers
// virtual: the engine's normal static/dynamic virtuals plus any extra
// well-known forms (e.g. the bare Nested parent for Kafka's "_headers").
// Mode-independent: stream-mode names are always present so the predicate
// is stable across setting changes.
func allVirtualNames(e Engine) []string {
	if e == nil {
		return nil
	}
	// For the membership predicate we want the *superset* of names that
	// might be virtual on this engine kind, independent of current
	// settings. Build it from a settings-maximal probe.
	var cols []DeclaredColumn
	switch typed := e.(type) {
	case EngineKafka:
		// Force stream mode for the predicate so _raw_message / _error
		// are recognised regardless of HandleErrorMode.
		stream := "stream"
		typed.HandleErrorMode = &stream
		cols = typed.Virtuals()
	case EngineDistributed:
		// Static fallback only — the membership predicate doesn't recurse
		// into remote tables. Diff/validate sites call VirtualColumnsFor
		// directly with a resolver when they need the transitive set.
		cols = typed.Virtuals()
	default:
		if v, ok := e.(EngineWithVirtuals); ok {
			cols = v.Virtuals()
		}
	}
	names := make([]string, 0, len(cols)+1)
	hasHeaders := false
	for _, c := range cols {
		names = append(names, c.Name)
		if !hasHeaders && len(c.Name) > len("_headers.") && c.Name[:len("_headers.")] == "_headers." {
			hasHeaders = true
		}
	}
	if hasHeaders {
		names = append(names, "_headers")
	}
	return names
}
