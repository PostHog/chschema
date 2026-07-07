package hcl

// ClusterSet maps external cluster names to the schema objects each cluster
// declares. It lets Validate resolve a Distributed table's remote against the
// composition of *another* cluster: the remote database is almost always
// "posthog" on every cluster, so the cluster name is the only discriminator.
//
// A cluster is either mapped (a composition was loaded for it) or absent (the
// env has no local composition for it, e.g. a node type with no instance here).
// References into an absent cluster are structurally unresolvable and count as
// satisfied. The zero value is a valid empty set: no mappings, no absent names.
type ClusterSet struct {
	declared map[string]map[ObjectRef]bool // name → declared object set
	resolver map[string]TableResolver      // name → resolver (used by the column check in #119)
	absent   map[string]bool               // name → declared @absent
}

// NewClusterSet returns an empty, initialized ClusterSet.
func NewClusterSet() ClusterSet {
	return ClusterSet{
		declared: map[string]map[ObjectRef]bool{},
		resolver: map[string]TableResolver{},
		absent:   map[string]bool{},
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

// mapped reports whether a composition was loaded for the named cluster.
func (cs ClusterSet) mapped(name string) bool {
	_, ok := cs.declared[name]
	return ok
}

// isAbsent reports whether the named cluster was declared @absent.
func (cs ClusterSet) isAbsent(name string) bool {
	return cs.absent[name]
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
