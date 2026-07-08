package hcl

// ClusterSet maps external cluster names to the schema objects each cluster
// declares. It lets Validate resolve a Distributed table's remote against the
// composition of *another* cluster: the remote database is almost always
// "posthog" on every cluster, so the cluster name is the only discriminator.
//
// A cluster is either mapped (a composition was loaded for it), absent (the
// env has no local composition for it, e.g. a node type with no instance here),
// or an alias of another cluster. References into an absent cluster are
// structurally unresolvable and count as satisfied; a ClickHouse remote_servers
// alias (e.g. "posthog_writable") shares the composition of its base cluster,
// so it resolves through that base. The zero value is a valid empty set.
type ClusterSet struct {
	declared map[string]map[ObjectRef]bool // name → declared object set
	resolver map[string]TableResolver      // name → resolver (used by the column check in #119)
	absent   map[string]bool               // name → declared @absent
	alias    map[string]string             // alias name → base cluster name
}

// NewClusterSet returns an empty, initialized ClusterSet.
func NewClusterSet() ClusterSet {
	return ClusterSet{
		declared: map[string]map[ObjectRef]bool{},
		resolver: map[string]TableResolver{},
		absent:   map[string]bool{},
		alias:    map[string]string{},
	}
}

// Add records the composition of an external cluster: every table, view,
// materialized view, and raw object it declares becomes resolvable under name.
func (cs *ClusterSet) Add(name string, dbs []DatabaseSpec) {
	if cs.declared == nil {
		*cs = NewClusterSet()
	}
	cs.declared[name] = declaredObjects(dbs)
	cs.resolver[name] = NewSchemaResolver(dbs)
}

// AddAbsent declares a cluster that has no composition available in this env.
// References into it are treated as satisfied (see the resolution algorithm in
// Validate).
func (cs *ClusterSet) AddAbsent(name string) {
	if cs.absent == nil {
		*cs = NewClusterSet()
	}
	cs.absent[name] = true
}

// AddAlias records that alias resolves to base: a Distributed remote on the
// alias cluster is resolved against base's composition (or base's @absent
// status). base itself may be another alias; resolveCluster follows the chain.
func (cs *ClusterSet) AddAlias(alias, base string) {
	if cs.alias == nil {
		*cs = NewClusterSet()
	}
	cs.alias[alias] = base
}

// resolveCluster follows alias links to the terminal (base) cluster name. A
// name that is not an alias returns unchanged. A cycle stops at the first
// repeated name (defensive; the CLI does not create cycles).
func (cs ClusterSet) resolveCluster(name string) string {
	seen := map[string]bool{}
	for {
		base, ok := cs.alias[name]
		if !ok || seen[name] {
			return name
		}
		seen[name] = true
		name = base
	}
}

// mapped reports whether a composition was loaded for the named cluster.
func (cs ClusterSet) mapped(name string) bool {
	_, ok := cs.declared[name]
	return ok
}

// isAbsent reports whether the named cluster was declared @absent.
func (cs ClusterSet) isAbsent(name string) bool {
	return cs.absent[name]
}

// declaredInAnyMapped reports whether the object is declared in any mapped
// cluster. Used to resolve MV/View sources that read a table on a co-located
// sibling cluster: the SELECT carries no cluster name, so the reference is
// satisfied when the table exists in the composition of any mapped cluster.
// @absent clusters contribute no composition and never match.
func (cs ClusterSet) declaredInAnyMapped(ref ObjectRef) bool {
	for _, set := range cs.declared {
		if set[ref] {
			return true
		}
	}
	return false
}

// lookupTable returns the remote's TableSpec from the named cluster's
// composition, if that cluster is mapped and declares the object as a table.
// Used by the Distributed proxy column check to inspect a cross-cluster
// remote's columns.
func (cs ClusterSet) lookupTable(name string, ref ObjectRef) (TableSpec, bool) {
	r, ok := cs.resolver[name]
	if !ok {
		return TableSpec{}, false
	}
	return r.LookupTable(ref.Database, ref.Name)
}

// declares reports whether the named cluster declares the given object.
func (cs ClusterSet) declares(name string, ref ObjectRef) bool {
	return cs.declared[name][ref]
}

// declaredObjects builds the set of every object (table, materialized view,
// view, or raw block) declared across the given databases. Raw blocks are
// opaque but still register their name so a remote_table pointing at one
// resolves. Shared by Validate and ClusterSet.Add.
func declaredObjects(dbs []DatabaseSpec) map[ObjectRef]bool {
	declared := map[ObjectRef]bool{}
	for _, db := range dbs {
		for _, t := range db.Tables {
			declared[ObjectRef{Database: db.Name, Name: t.Name}] = true
		}
		for _, mv := range db.MaterializedViews {
			declared[ObjectRef{Database: db.Name, Name: mv.Name}] = true
		}
		for _, v := range db.Views {
			declared[ObjectRef{Database: db.Name, Name: v.Name}] = true
		}
		for _, r := range db.Raws {
			declared[ObjectRef{Database: db.Name, Name: r.Name}] = true
		}
	}
	return declared
}
