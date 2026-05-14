package hcl

import (
	"fmt"
	"sort"
	"strings"

	chparser "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// Dependency kinds.
const (
	DepMVSource          = "mv_source"          // a materialized view reads from this table
	DepMVDest            = "mv_dest"            // a materialized view writes into this table
	DepDistributedRemote = "distributed_remote" // a Distributed table forwards to this table
)

// ObjectRef identifies a schema object (table or materialized view) by its
// database and name.
type ObjectRef struct {
	Database string
	Name     string
}

func (o ObjectRef) String() string {
	if o.Database == "" {
		return o.Name
	}
	return o.Database + "." + o.Name
}

// Dependency records that one object (From) requires another (To) to exist
// before it can be created.
type Dependency struct {
	From ObjectRef // the dependent object (a materialized view or Distributed table)
	To   ObjectRef // the object it requires
	Kind string    // one of the Dep* constants
}

// ValidationError describes a dependency that could not be satisfied.
type ValidationError struct {
	Object  ObjectRef // the dependent object
	Missing ObjectRef // the unresolved reference
	Kind    string
	Reason  string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Object, e.Reason)
}

// SkipSet selects dependent objects whose dependency validation should be
// skipped. It is built from a comma-separated list of object names; the
// special value "*" skips every object.
type SkipSet struct {
	all   bool
	names map[string]bool
}

// ParseSkipSet parses a comma-separated list of dependent object names. Each
// entry is matched against an object's bare name or its "database.name" form.
// The single entry "*" skips all objects.
func ParseSkipSet(csv string) SkipSet {
	s := SkipSet{names: map[string]bool{}}
	for _, part := range strings.Split(csv, ",") {
		part = strings.TrimSpace(part)
		switch part {
		case "":
			continue
		case "*":
			s.all = true
		default:
			s.names[part] = true
		}
	}
	return s
}

// Skips reports whether validation for the given dependent object is skipped.
func (s SkipSet) Skips(o ObjectRef) bool {
	if s.all {
		return true
	}
	return s.names[o.Name] || s.names[o.String()]
}

// splitQualified turns a possibly database-qualified name ("db.table" or bare
// "table") into an ObjectRef, defaulting an unqualified name to defaultDB.
// Backtick quoting is stripped to match how names are written in HCL.
func splitQualified(name, defaultDB string) ObjectRef {
	name = strings.ReplaceAll(name, "`", "")
	if i := strings.IndexByte(name, '.'); i >= 0 {
		return ObjectRef{Database: name[:i], Name: name[i+1:]}
	}
	return ObjectRef{Database: defaultDB, Name: name}
}

// CollectDependencies walks every database and returns the creation-order
// dependencies carried by materialized views (source + destination tables)
// and Distributed-engine tables (their remote table).
func CollectDependencies(dbs []DatabaseSpec) ([]Dependency, error) {
	var deps []Dependency
	for _, db := range dbs {
		for _, mv := range db.MaterializedViews {
			from := ObjectRef{Database: db.Name, Name: mv.Name}

			deps = append(deps, Dependency{
				From: from,
				To:   splitQualified(mv.ToTable, db.Name),
				Kind: DepMVDest,
			})

			sources, err := extractSourceTables(mv.Query)
			if err != nil {
				return nil, fmt.Errorf("materialized_view %s: parsing query: %w", from, err)
			}
			for _, src := range sources {
				if src.Database == "" {
					src.Database = db.Name
				}
				deps = append(deps, Dependency{From: from, To: src, Kind: DepMVSource})
			}
		}

		for _, t := range db.Tables {
			if t.Engine == nil {
				continue
			}
			dist, ok := t.Engine.Decoded.(EngineDistributed)
			if !ok {
				continue
			}
			deps = append(deps, Dependency{
				From: ObjectRef{Database: db.Name, Name: t.Name},
				To:   ObjectRef{Database: dist.RemoteDatabase, Name: dist.RemoteTable},
				Kind: DepDistributedRemote,
			})
		}
	}
	return deps, nil
}

// extractSourceTables parses a materialized view's SELECT query and returns
// every table it reads from (FROM and JOIN targets, including those nested in
// subqueries). Names introduced by WITH ... AS common table expressions are
// filtered out; they are query-local, not real tables. Database-unqualified
// references are returned with an empty Database for the caller to default.
func extractSourceTables(query string) ([]ObjectRef, error) {
	stmts, err := chparser.NewParser(query).ParseStmts()
	if err != nil {
		return nil, err
	}

	cteNames := map[string]bool{}
	var refs []ObjectRef
	for _, stmt := range stmts {
		for _, n := range chparser.FindAll(stmt, isSelectQuery) {
			sel := n.(*chparser.SelectQuery)
			if sel.With == nil {
				continue
			}
			for _, cte := range sel.With.CTEs {
				if id, ok := cte.Expr.(*chparser.Ident); ok {
					cteNames[id.Name] = true
				}
			}
		}
		for _, n := range chparser.FindAll(stmt, isTableIdentifier) {
			id := n.(*chparser.TableIdentifier)
			if id.Table == nil {
				continue
			}
			ref := ObjectRef{Name: id.Table.Name}
			if id.Database != nil {
				ref.Database = id.Database.Name
			}
			if ref.Database == "" && cteNames[ref.Name] {
				continue
			}
			refs = append(refs, ref)
		}
	}
	return dedupeRefs(refs), nil
}

func isSelectQuery(e chparser.Expr) bool {
	_, ok := e.(*chparser.SelectQuery)
	return ok
}

func isTableIdentifier(e chparser.Expr) bool {
	_, ok := e.(*chparser.TableIdentifier)
	return ok
}

func dedupeRefs(refs []ObjectRef) []ObjectRef {
	seen := map[ObjectRef]bool{}
	var out []ObjectRef
	for _, r := range refs {
		if seen[r] {
			continue
		}
		seen[r] = true
		out = append(out, r)
	}
	return out
}

// Validate checks that every dependency carried by the loaded databases is
// satisfied: the referenced table or materialized view must be declared in
// one of the loaded databases. A reference into a database that is not loaded
// is itself an error, since it cannot be checked. Dependent objects matched
// by skip are not validated. All errors are collected and returned together.
func Validate(dbs []DatabaseSpec, skip SkipSet) []ValidationError {
	declared := map[ObjectRef]bool{}
	loadedDBs := map[string]bool{}
	for _, db := range dbs {
		loadedDBs[db.Name] = true
		for _, t := range db.Tables {
			declared[ObjectRef{Database: db.Name, Name: t.Name}] = true
		}
		for _, mv := range db.MaterializedViews {
			declared[ObjectRef{Database: db.Name, Name: mv.Name}] = true
		}
	}

	deps, err := CollectDependencies(dbs)
	if err != nil {
		return []ValidationError{{Reason: err.Error()}}
	}

	var errs []ValidationError
	for _, dep := range deps {
		if skip.Skips(dep.From) {
			continue
		}
		if !loadedDBs[dep.To.Database] {
			errs = append(errs, ValidationError{
				Object:  dep.From,
				Missing: dep.To,
				Kind:    dep.Kind,
				Reason: fmt.Sprintf("%s references %q, but its database %q is not loaded",
					depPhrase(dep.Kind), dep.To, dep.To.Database),
			})
			continue
		}
		if !declared[dep.To] {
			errs = append(errs, ValidationError{
				Object:  dep.From,
				Missing: dep.To,
				Kind:    dep.Kind,
				Reason: fmt.Sprintf("%s references %q, which is not declared in the schema",
					depPhrase(dep.Kind), dep.To),
			})
		}
	}

	sort.Slice(errs, func(i, j int) bool {
		if errs[i].Object != errs[j].Object {
			return errs[i].Object.String() < errs[j].Object.String()
		}
		return errs[i].Missing.String() < errs[j].Missing.String()
	})
	return errs
}

func depPhrase(kind string) string {
	switch kind {
	case DepMVSource:
		return "materialized view source table"
	case DepMVDest:
		return "materialized view destination table"
	case DepDistributedRemote:
		return "Distributed table remote table"
	default:
		return "dependency"
	}
}
