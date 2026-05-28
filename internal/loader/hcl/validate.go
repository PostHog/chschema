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
	DepViewSource        = "view_source"        // a plain view reads from this table
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

		for _, v := range db.Views {
			from := ObjectRef{Database: db.Name, Name: v.Name}
			sources, err := extractSourceTables(v.Query)
			if err != nil {
				return nil, fmt.Errorf("view %s: parsing query: %w", from, err)
			}
			for _, src := range sources {
				if src.Database == "" {
					src.Database = db.Name
				}
				deps = append(deps, Dependency{From: from, To: src, Kind: DepViewSource})
			}
		}
	}
	return deps, nil
}

// ExtractReferencedTables parses a CREATE statement (TABLE, MATERIALIZED
// VIEW, VIEW, or DICTIONARY) and returns every table it references that
// is NOT the object being created itself:
//   - MV destination (TO dest) and SELECT FROM/JOIN sources
//   - View SELECT FROM/JOIN sources
//   - Dictionary SOURCE(CLICKHOUSE(TABLE 'x')) targets and
//     SOURCE(CLICKHOUSE(QUERY 'SELECT ... FROM y')) inner-query sources
//   - Distributed engine remote_table
//
// CTE names introduced by WITH ... AS are filtered out (they're
// query-local, not real tables). The object being created itself is
// excluded. Database-unqualified refs are returned with Database = "".
//
// Use this from test setup to derive the list of tables that must
// exist before the CREATE statement is executable.
func ExtractReferencedTables(createSQL string) ([]ObjectRef, error) {
	stmts, err := chparser.NewParser(createSQL).ParseStmts()
	if err != nil {
		return nil, err
	}
	self := map[string]bool{}
	cteNames := map[string]bool{}
	var refs []ObjectRef

	for _, stmt := range stmts {
		// Identify the object being created so we can exclude it from refs.
		switch s := stmt.(type) {
		case *chparser.CreateTable:
			if s.Name != nil && s.Name.Table != nil {
				self[s.Name.Table.Name] = true
			}
		case *chparser.CreateMaterializedView:
			if s.Name != nil && s.Name.Table != nil {
				self[s.Name.Table.Name] = true
			}
		case *chparser.CreateView:
			if s.Name != nil && s.Name.Table != nil {
				self[s.Name.Table.Name] = true
			}
		case *chparser.CreateDictionary:
			if s.Name != nil && s.Name.Table != nil {
				self[s.Name.Table.Name] = true
			}
		}

		// Collect CTE names so we don't treat them as real table refs.
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

		// Collect every TableIdentifier, dropping self and CTEs.
		for _, n := range chparser.FindAll(stmt, isTableIdentifier) {
			id := n.(*chparser.TableIdentifier)
			if id.Table == nil {
				continue
			}
			name := id.Table.Name
			if self[name] || cteNames[name] {
				continue
			}
			ref := ObjectRef{Name: name}
			if id.Database != nil {
				ref.Database = id.Database.Name
			}
			refs = append(refs, ref)
		}

		// Dictionary SOURCE(CLICKHOUSE(TABLE 'x' | QUERY 'SELECT … FROM y')).
		// The chparser walks the AST nodes but does NOT descend into string
		// literals; we have to look at the source args by hand.
		if cd, ok := stmt.(*chparser.CreateDictionary); ok && cd.Engine != nil && cd.Engine.Source != nil {
			for _, arg := range cd.Engine.Source.Args {
				if arg == nil || arg.Name == nil {
					continue
				}
				lit, isLit := arg.Value.(*chparser.StringLiteral)
				if !isLit {
					continue
				}
				switch strings.ToLower(arg.Name.Name) {
				case "table":
					refs = append(refs, ObjectRef{Name: lit.Literal})
				case "query":
					inner, err := extractSourceTables(lit.Literal)
					if err != nil {
						continue
					}
					for _, r := range inner {
						if !self[r.Name] && !cteNames[r.Name] {
							refs = append(refs, r)
						}
					}
				}
			}
		}
	}
	return dedupeRefs(refs), nil
}

// DeclaredColumn is a single column from the object declared in a CREATE
// statement: name + ClickHouse type expression as plain text. Used by
// tests that need to materialize stub source tables.
type DeclaredColumn struct {
	Name string
	Type string
}

// ExtractDeclaredColumns parses a CREATE statement and returns the
// columns declared on the object being created:
//   - CREATE TABLE / VIEW with explicit columns: the column list
//   - CREATE MATERIALIZED VIEW: the destination column list (TO dest (…))
//   - CREATE DICTIONARY: the attribute list (name+type only)
//
// Returns an empty slice (no error) when the statement carries no column
// list (e.g. CREATE VIEW v AS SELECT … without an explicit schema).
func ExtractDeclaredColumns(createSQL string) ([]DeclaredColumn, error) {
	stmts, err := chparser.NewParser(createSQL).ParseStmts()
	if err != nil {
		return nil, err
	}
	for _, stmt := range stmts {
		switch s := stmt.(type) {
		case *chparser.CreateTable:
			return columnsFromTableSchema(s.TableSchema), nil
		case *chparser.CreateMaterializedView:
			if s.Destination != nil {
				return columnsFromTableSchema(s.Destination.TableSchema), nil
			}
		case *chparser.CreateView:
			return columnsFromTableSchema(s.TableSchema), nil
		case *chparser.CreateDictionary:
			if s.Schema == nil {
				return nil, nil
			}
			out := make([]DeclaredColumn, 0, len(s.Schema.Attributes))
			for _, a := range s.Schema.Attributes {
				if a == nil || a.Name == nil {
					continue
				}
				out = append(out, DeclaredColumn{
					Name: stripBackticks(a.Name.Name),
					Type: formatNode(a.Type),
				})
			}
			return out, nil
		}
	}
	return nil, nil
}

func columnsFromTableSchema(ts *chparser.TableSchemaClause) []DeclaredColumn {
	if ts == nil {
		return nil
	}
	out := make([]DeclaredColumn, 0, len(ts.Columns))
	for _, c := range ts.Columns {
		cd, ok := c.(*chparser.ColumnDef)
		if !ok {
			continue
		}
		if cd.Name == nil {
			continue
		}
		out = append(out, DeclaredColumn{
			Name: stripBackticks(identName(cd.Name)),
			Type: formatNode(cd.Type),
		})
	}
	return out
}

func stripBackticks(s string) string {
	if len(s) >= 2 && s[0] == '`' && s[len(s)-1] == '`' {
		return s[1 : len(s)-1]
	}
	return s
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
		for _, v := range db.Views {
			declared[ObjectRef{Database: db.Name, Name: v.Name}] = true
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
	case DepViewSource:
		return "view source table"
	default:
		return "dependency"
	}
}
