package hcl

import (
	"fmt"
	"sort"
	"strings"

	chparser "github.com/orian/clickhouse-sql-parser/parser"
)

// Dependency kinds.
const (
	DepMVSource          = "mv_source"          // a materialized view reads from this table
	DepMVDest            = "mv_dest"            // a materialized view writes into this table
	DepDistributedRemote = "distributed_remote" // a Distributed table forwards to this table
	DepTimeSeriesTarget  = "ts_target"          // a TimeSeries table references an external samples/tags/metrics target
	DepBufferDestination = "buffer_dest"        // a Buffer table forwards writes into this table
	DepViewSource        = "view_source"        // a plain view reads from this table

	// KindMVColumn flags a materialized view that references a column its
	// single source table does not provide (declared columns plus the
	// engine's virtual columns). This is a heuristic check, restricted to
	// virtual-prefixed names (those starting with `_`) and to MVs with a
	// single resolvable source and no JOIN/CTE/UNION/subquery/SELECT *.
	KindMVColumn = "mv_column"
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

	// Cluster is the ClickHouse cluster the remote is expected on. It is set
	// only for DepDistributedRemote (from EngineDistributed.ClusterName) and
	// is the discriminator for cross-cluster resolution: the remote database
	// is almost always "posthog" on every cluster. Empty for all other kinds.
	Cluster string
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
			switch eng := t.Engine.Decoded.(type) {
			case EngineDistributed:
				deps = append(deps, Dependency{
					From:    ObjectRef{Database: db.Name, Name: t.Name},
					To:      ObjectRef{Database: eng.RemoteDatabase, Name: eng.RemoteTable},
					Kind:    DepDistributedRemote,
					Cluster: eng.ClusterName,
				})
			case EngineTimeSeries:
				from := ObjectRef{Database: db.Name, Name: t.Name}
				for _, target := range []*TimeSeriesTarget{eng.Samples, eng.Tags, eng.Metrics} {
					if target == nil || target.Target == nil {
						continue
					}
					deps = append(deps, Dependency{
						From: from,
						To:   splitQualified(*target.Target, db.Name),
						Kind: DepTimeSeriesTarget,
					})
				}
			case EngineBuffer:
				// Buffer forwards writes to (database, table). Empty
				// database means current — defaults to the buffer's own
				// database (matches CH's currentDatabase() semantic).
				destDB := eng.Database
				if destDB == "" {
					destDB = db.Name
				}
				deps = append(deps, Dependency{
					From: ObjectRef{Database: db.Name, Name: t.Name},
					To:   ObjectRef{Database: destDB, Name: eng.Table},
					Kind: DepBufferDestination,
				})
			}
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
//
// Distributed remotes are resolved cluster-aware against clusters: a proxy
// routinely forwards to a storage table that lives on another cluster's
// composition (same database name, different cluster). See
// resolveDistributedRemote for the routing rules. Materialized-view and plain-
// view sources are likewise resolved against the union of mapped clusters when
// missing locally (a co-located read carries no cluster name). Pass
// ClusterSet{} when no external cluster mappings are available.
func Validate(dbs []DatabaseSpec, skip SkipSet, clusters ClusterSet) []ValidationError {
	declared := declaredObjects(dbs)
	loadedDBs := map[string]bool{}
	for _, db := range dbs {
		loadedDBs[db.Name] = true
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
		// Built-in system database: system.* tables are always present in a
		// live server and cannot be validated offline, so any reference into
		// them (view/MV source or Distributed remote) is satisfied.
		if dep.To.Database == "system" {
			continue
		}
		// Distributed remotes route through the cluster-aware algorithm; the
		// cluster name (not the database, which is uniform across clusters) is
		// the discriminator.
		if dep.Kind == DepDistributedRemote {
			if e := resolveDistributedRemote(dep, declared, clusters); e != nil {
				errs = append(errs, *e)
			}
			continue
		}
		// A materialized view or plain view reads its source from the local
		// server, which at composition time is the union of all co-located
		// sibling clusters. A source missing locally is satisfied when it is
		// declared in any mapped cluster. (Only SELECT sources read across the
		// composition; write targets — MV dest, Buffer, TimeSeries — stay local.)
		if dep.Kind == DepMVSource || dep.Kind == DepViewSource {
			if !declared[dep.To] && clusters.declaredInAnyMapped(dep.To) {
				continue
			}
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

	// MV column validation (heuristic, virtual-prefixed names only).
	// Runs in addition to the dependency check above. Same skip rules.
	resolver := NewSchemaResolver(dbs)
	tablesByRef := map[ObjectRef]TableSpec{}
	for _, db := range dbs {
		for _, t := range db.Tables {
			tablesByRef[ObjectRef{Database: db.Name, Name: t.Name}] = t
		}
	}
	for _, db := range dbs {
		for _, mv := range db.MaterializedViews {
			from := ObjectRef{Database: db.Name, Name: mv.Name}
			if skip.Skips(from) {
				continue
			}
			errs = append(errs, validateMVColumns(from, mv, db.Name, tablesByRef, resolver)...)
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

// resolveDistributedRemote resolves a Distributed table's remote reference
// (dep.To on cluster dep.Cluster) and returns a ValidationError when it cannot
// be satisfied. A "system" remote is handled by the caller before this runs.
// The routing, in order:
//
//  1. Local: the remote is declared in the node's own schema (own-cluster
//     proxies and same-node aliases) → satisfied.
//  2. Mapped external: the cluster has a loaded composition → the remote must
//     be declared there, else it is real cross-cluster drift (error).
//  3. Absent: the cluster was declared @absent → structurally unresolvable,
//     counts as satisfied.
//  4. Unknown: the remote is neither local nor in a mapped/absent cluster →
//     error. This is the anti-staleness guarantee: a new cross-cluster proxy
//     cannot be silently accepted; the caller must map the cluster or mark it
//     @absent (or -skip-validation the proxy).
//
// A remote_servers alias (e.g. "posthog_writable") is resolved to its base
// cluster before steps 2–4, so it shares that base's composition and @absent
// status. Error messages name the alias and the base it resolves to.
func resolveDistributedRemote(dep Dependency, declared map[ObjectRef]bool, clusters ClusterSet) *ValidationError {
	remote := dep.To

	// 1. Local to the node's own schema (alias-independent).
	if declared[remote] {
		return nil
	}

	// Follow alias links to the base cluster for the mapped/absent checks.
	cluster := clusters.resolveCluster(dep.Cluster)
	clusterDesc := fmt.Sprintf("%q", dep.Cluster)
	if cluster != dep.Cluster {
		clusterDesc = fmt.Sprintf("%q (alias of %q)", dep.Cluster, cluster)
	}

	// 2. Mapped external cluster.
	if clusters.mapped(cluster) {
		if clusters.declares(cluster, remote) {
			return nil
		}
		return &ValidationError{
			Object:  dep.From,
			Missing: remote,
			Kind:    dep.Kind,
			Reason: fmt.Sprintf("%s references remote %q on cluster %s, which is not declared in that cluster's schema",
				depPhrase(dep.Kind), remote, clusterDesc),
		}
	}
	// 3. Cluster declared absent.
	if clusters.isAbsent(cluster) {
		return nil
	}
	// 4. Unknown cluster, no mapping.
	return &ValidationError{
		Object:  dep.From,
		Missing: remote,
		Kind:    dep.Kind,
		Reason: fmt.Sprintf("%s references remote %q on cluster %s, which is not declared locally and has no -cluster mapping (add it or mark @absent)",
			depPhrase(dep.Kind), remote, clusterDesc),
	}
}

// validateMVColumns runs the heuristic virtual-column reference check on
// a single MV. Returns an empty slice when the query is too complex to
// attribute, when the source can't be resolved to a known TableSpec, or
// when no virtual-prefixed refs are unknown.
func validateMVColumns(from ObjectRef, mv MaterializedViewSpec, defaultDB string, tables map[ObjectRef]TableSpec, r TableResolver) []ValidationError {
	sources, err := extractSourceTables(mv.Query)
	if err != nil || len(sources) != 1 {
		return nil
	}
	src := sources[0]
	if src.Database == "" {
		src.Database = defaultDB
	}
	srcSpec, ok := tables[src]
	if !ok {
		return nil
	}
	refs, ok := mvVirtualPrefixedRefs(mv.Query)
	if !ok {
		return nil
	}
	provided := ColumnsProvidedBy(srcSpec, r)
	providedNames := make(map[string]bool, len(provided))
	hasHeadersDotted := false
	for _, c := range provided {
		providedNames[c.Name] = true
		if !hasHeadersDotted && strings.HasPrefix(c.Name, "_headers.") {
			hasHeadersDotted = true
		}
	}
	if hasHeadersDotted {
		providedNames["_headers"] = true // accept the bare Nested parent too
	}

	// Sort refs for deterministic errors.
	names := make([]string, 0, len(refs))
	for n := range refs {
		names = append(names, n)
	}
	sort.Strings(names)

	var errs []ValidationError
	for _, ref := range names {
		if providedNames[ref] {
			continue
		}
		errs = append(errs, ValidationError{
			Object:  from,
			Missing: ObjectRef{Database: src.Database, Name: ref},
			Kind:    KindMVColumn,
			Reason: fmt.Sprintf("materialized view references column %q which is not provided by source table %q (declared columns or %s virtual columns)",
				ref, src, sourceEngineKind(srcSpec)),
		})
	}
	return errs
}

// sourceEngineKind returns the engine kind name for the error message, or
// "(no engine)" when the source has none (shouldn't happen post-resolve).
func sourceEngineKind(t TableSpec) string {
	if t.Engine != nil && t.Engine.Decoded != nil {
		return t.Engine.Decoded.Kind()
	}
	return "(no engine)"
}

// mvVirtualPrefixedRefs parses an MV query and returns the set of
// underscore-prefixed identifier names it references. ok=false signals
// the caller should skip the check (query has JOIN, UNION, CTE, subquery
// in FROM, or SELECT *).
//
// The leading-underscore heuristic deliberately scopes the check to
// virtual-column-like names so the bare-Ident walk can't false-positive
// on regular columns we couldn't attribute precisely.
func mvVirtualPrefixedRefs(query string) (map[string]bool, bool) {
	stmts, err := chparser.NewParser(query).ParseStmts()
	if err != nil || len(stmts) == 0 {
		return nil, false
	}
	var sel *chparser.SelectQuery
	for _, n := range chparser.FindAll(stmts[0], isSelectQuery) {
		sel = n.(*chparser.SelectQuery)
		break
	}
	if sel == nil {
		return nil, false
	}
	// Bail on structural complexity.
	if sel.UnionAll != nil || sel.UnionDistinct != nil || sel.Except != nil {
		return nil, false
	}
	if sel.With != nil && len(sel.With.CTEs) > 0 {
		return nil, false
	}
	if !isSimpleFrom(sel.From) {
		return nil, false
	}
	if hasStarSelectItem(sel) {
		return nil, false
	}

	// Collect alias names introduced by SELECT items so they don't
	// surface as refs.
	aliasNames := map[string]bool{}
	for _, item := range sel.SelectItems {
		if item.Alias != nil {
			aliasNames[stripBackticks(item.Alias.Name)] = true
		}
	}

	// Collect Idents we must NOT treat as column refs: function names,
	// table-qualifier Idents, alias-target Idents. Compared by pointer
	// identity since FindAll returns shared nodes.
	skip := map[*chparser.Ident]bool{}
	for _, n := range chparser.FindAll(sel, func(e chparser.Expr) bool {
		_, ok := e.(*chparser.FunctionExpr)
		return ok
	}) {
		f := n.(*chparser.FunctionExpr)
		if f.Name != nil {
			skip[f.Name] = true
		}
	}
	for _, n := range chparser.FindAll(sel, isTableIdentifier) {
		t := n.(*chparser.TableIdentifier)
		if t.Database != nil {
			skip[t.Database] = true
		}
		if t.Table != nil {
			skip[t.Table] = true
		}
	}
	for _, n := range chparser.FindAll(sel, func(e chparser.Expr) bool {
		_, ok := e.(*chparser.Path)
		return ok
	}) {
		p := n.(*chparser.Path)
		// Path qualifier idents (db, table) are not column refs; only
		// the final Ident is. Skip every Ident except the last.
		if len(p.Fields) > 1 {
			for i := 0; i < len(p.Fields)-1; i++ {
				skip[p.Fields[i]] = true
			}
		}
	}
	for _, item := range sel.SelectItems {
		if item.Alias != nil {
			skip[item.Alias] = true
		}
	}

	refs := map[string]bool{}
	for _, n := range chparser.FindAll(sel, func(e chparser.Expr) bool {
		_, ok := e.(*chparser.Ident)
		return ok
	}) {
		id := n.(*chparser.Ident)
		if skip[id] {
			continue
		}
		name := stripBackticks(id.Name)
		if !strings.HasPrefix(name, "_") {
			continue
		}
		if aliasNames[name] {
			continue
		}
		refs[name] = true
	}
	for _, n := range chparser.FindAll(sel, func(e chparser.Expr) bool {
		_, ok := e.(*chparser.NestedIdentifier)
		return ok
	}) {
		ni := n.(*chparser.NestedIdentifier)
		if ni.Ident == nil || ni.DotIdent == nil {
			continue
		}
		name := stripBackticks(ni.Ident.Name) + "." + stripBackticks(ni.DotIdent.Name)
		if !strings.HasPrefix(name, "_") {
			continue
		}
		if aliasNames[name] {
			continue
		}
		refs[name] = true
	}
	return refs, true
}

// isSimpleFrom reports whether the FROM clause is a single, plain
// TableIdentifier — no JOIN, no subquery, no table-function call. The
// parser wraps every FROM in JoinTableExpr → TableExpr → inner; the inner
// must be a TableIdentifier or bare Ident. JoinExpr at the top means a
// real JOIN; SubQuery at the inner means a FROM-subquery. Both bail.
func isSimpleFrom(f *chparser.FromClause) bool {
	if f == nil {
		return false
	}
	jte, ok := f.Expr.(*chparser.JoinTableExpr)
	if !ok {
		return false
	}
	if jte.Table == nil {
		return false
	}
	switch jte.Table.Expr.(type) {
	case *chparser.TableIdentifier, *chparser.Ident:
		return true
	default:
		return false
	}
}

// hasStarSelectItem reports whether any SELECT item is a `*` or `t.*`.
// Detected by walking the SelectItem expressions for any Ident whose
// name is "*" — the chparser models the star as such.
func hasStarSelectItem(sel *chparser.SelectQuery) bool {
	for _, item := range sel.SelectItems {
		if item == nil || item.Expr == nil {
			continue
		}
		for _, n := range chparser.FindAll(item.Expr, func(e chparser.Expr) bool {
			id, ok := e.(*chparser.Ident)
			return ok && id != nil && id.Name == "*"
		}) {
			_ = n
			return true
		}
	}
	return false
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
	case DepTimeSeriesTarget:
		return "TimeSeries target table"
	case DepBufferDestination:
		return "Buffer destination table"
	default:
		return "dependency"
	}
}
