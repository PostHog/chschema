package hcl

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	chparser "github.com/orian/clickhouse-sql-parser/parser"
)

// GeneratedSQL holds the result of GenerateSQL: the safe DDL statements ready
// to execute, plus a list of unsafe changes (engine, order_by, partition_by,
// sample_by) that ClickHouse cannot apply in place and require manual table
// recreation.
type GeneratedSQL struct {
	Statements []string
	Unsafe     []UnsafeChange
}

// UnsafeChange describes a diff entry that can't be expressed as an ALTER.
// Database and Table identify the target; Reason is a human-readable
// explanation of what would need to change.
type UnsafeChange struct {
	Database string
	Table    string
	Reason   string
}

// GenerateSQL turns a ChangeSet into ClickHouse DDL. Statements are ordered:
// CREATE TABLE, CREATE MATERIALIZED VIEW, ALTER TABLE, ALTER ... MODIFY QUERY,
// DROP VIEW, DROP TABLE — so materialized views are created after their
// destination table and dropped before it. Within the CREATE TABLE phase,
// tables are ordered by dependency so a Distributed table comes after the
// local table it forwards to; the DROP TABLE phase uses the reverse order.
// Unsafe changes (engine swap, ORDER BY change, materialized view recreation,
// etc.) are collected into Unsafe; the generator does not synthesize a
// recreate-and-swap procedure.
func GenerateSQL(cs ChangeSet) GeneratedSQL {
	var out GeneratedSQL

	// 1. Named-collection recreates (DROP+CREATE adjacent, at the FRONT
	// before any other create — dependent tables can rely on the new NC).
	for _, ncc := range cs.NamedCollections {
		if ncc.Recreate && ncc.Add != nil {
			out.Statements = append(out.Statements, dropNamedCollectionSQL(ncc.Name))
			out.Statements = append(out.Statements, createNamedCollectionSQL(*ncc.Add))
		}
		if ncc.Error != "" {
			out.Unsafe = append(out.Unsafe, UnsafeChange{
				Database: "",
				Table:    ncc.Name,
				Reason:   "named collection: " + ncc.Error,
			})
		}
	}

	// 2. Fresh NC adds.
	for _, ncc := range cs.NamedCollections {
		if ncc.Add != nil && !ncc.Recreate {
			out.Statements = append(out.Statements, createNamedCollectionSQL(*ncc.Add))
		}
	}

	// Tables, materialized views, views, and dictionaries are emitted in one
	// dependency-respecting order so a referenced object is always created
	// before the object that references it (Distributed→remote, MV→source/
	// to_table, view→source view, Buffer→destination, dictionary→source table,
	// etc.). Objects without a dependency relationship keep their historical
	// relative order.
	out.Statements = append(out.Statements, orderedCreates(cs)...)
	// Raw adds last among creates: a raw object (typically a leaf dictionary
	// or view) may reference tables created above. Its dependencies cannot be
	// analysed, so no finer ordering is attempted.
	for _, dc := range cs.Databases {
		for _, r := range dc.AddRaws {
			out.Statements = append(out.Statements, createRawSQL(r))
		}
	}
	for _, dc := range cs.Databases {
		for _, td := range dc.AlterTables {
			if td.IsUnsafe() {
				out.Unsafe = append(out.Unsafe, unsafeReasons(dc.Database, td)...)
			}
			if stmt := alterTableSQL(dc.Database, td); stmt != "" {
				out.Statements = append(out.Statements, stmt)
			}
		}
	}
	for _, dc := range cs.Databases {
		for _, mvd := range dc.AlterMaterializedViews {
			if mvd.Recreate {
				out.Unsafe = append(out.Unsafe, UnsafeChange{
					Database: dc.Database, Table: mvd.Name,
					Reason: "materialized view to_table or column list change requires recreating the view",
				})
				continue
			}
			if mvd.QueryChange != nil && mvd.QueryChange.New != nil {
				out.Statements = append(out.Statements, modifyQuerySQL(dc.Database, mvd.Name, *mvd.QueryChange.New))
			}
		}
	}
	for _, dc := range cs.Databases {
		for _, vd := range dc.AlterViews {
			if vd.Recreate {
				out.Unsafe = append(out.Unsafe, UnsafeChange{
					Database: dc.Database, Table: vd.Name,
					Reason: "view column_aliases / sql_security / definer / cluster change requires recreating the view",
				})
				continue
			}
			if vd.QueryChange != nil && vd.QueryChange.New != nil {
				out.Statements = append(out.Statements, modifyQuerySQL(dc.Database, vd.Name, *vd.QueryChange.New))
			}
			if vd.Comment != nil && vd.Comment.New != nil {
				out.Statements = append(out.Statements, modifyCommentSQL(dc.Database, vd.Name, *vd.Comment.New))
			}
		}
	}
	for _, dc := range cs.Databases {
		for _, dd := range dc.AlterDictionaries {
			out.Unsafe = append(out.Unsafe, UnsafeChange{
				Database: dc.Database,
				Table:    dd.Name,
				Reason:   fmt.Sprintf("dictionary change requires CREATE OR REPLACE DICTIONARY (changed: %s)", strings.Join(dd.Changed, ", ")),
			})
		}
	}
	// Raw recreates: a raw object is opaque, so the only reconciliation is
	// DROP + CREATE. For a table this destroys on-disk data, so it is flagged
	// unsafe and the destructive DDL is NOT auto-emitted (manual review). For
	// a view/dictionary/materialized_view the recreate is lossless and emitted
	// as adjacent DROP + CREATE statements.
	for _, dc := range cs.Databases {
		for _, rc := range dc.AlterRaws {
			if rc.IsUnsafe() {
				out.Unsafe = append(out.Unsafe, UnsafeChange{
					Database: dc.Database, Table: rc.Name,
					Reason: "raw table change requires DROP + CREATE, which destroys the table's data",
				})
				continue
			}
			out.Statements = append(out.Statements, dropRawSQL(rc.Kind, dc.Database, rc.Name))
			out.Statements = append(out.Statements, createRawSQL(RawSpec{Kind: rc.Kind, Name: rc.Name, SQL: rc.NewSQL}))
		}
	}

	// 8. ALTER NAMED COLLECTION (SET then DELETE, only for non-recreate diffs).
	for _, ncc := range cs.NamedCollections {
		if ncc.Recreate || ncc.Add != nil || ncc.Drop {
			continue
		}
		if stmt := alterNamedCollectionSetSQL(ncc.Name, ncc.SetParams); stmt != "" {
			out.Statements = append(out.Statements, stmt)
		}
		if stmt := alterNamedCollectionDeleteSQL(ncc.Name, ncc.DeleteParams); stmt != "" {
			out.Statements = append(out.Statements, stmt)
		}
	}

	for _, dc := range cs.Databases {
		for _, name := range dc.DropMaterializedViews {
			out.Statements = append(out.Statements, dropViewSQL(dc.Database, name))
		}
	}
	for _, dc := range cs.Databases {
		names := append([]string(nil), dc.DropViews...)
		sort.Strings(names)
		for _, name := range names {
			out.Statements = append(out.Statements, dropViewSQL(dc.Database, name))
		}
	}
	for _, dc := range cs.Databases {
		names := append([]string(nil), dc.DropDictionaries...)
		sort.Strings(names)
		for _, name := range names {
			out.Statements = append(out.Statements, dropDictionarySQL(dc.Database, name))
		}
	}
	// Raw drops before table drops: a raw object may read from a table (e.g. a
	// raw materialized view), so it must be removed first.
	for _, dc := range cs.Databases {
		for _, r := range dc.DropRaws {
			out.Statements = append(out.Statements, dropRawSQL(r.Kind, dc.Database, r.Name))
		}
	}
	for _, dt := range orderTablesByDependency(gatherTables(cs, dropTablesOf), true) {
		out.Statements = append(out.Statements, dropTableSQL(dt.Database, dt.Table.Name))
	}

	// 12. NC pure drops (not recreate). After tables — anything referencing them is gone.
	for _, ncc := range cs.NamedCollections {
		if ncc.Drop && !ncc.Recreate {
			out.Statements = append(out.Statements, dropNamedCollectionSQL(ncc.Name))
		}
	}
	return out
}

// dbTable pairs a table with its database, so a flat slice can carry tables
// drawn from every database in a ChangeSet.
type dbTable struct {
	Database string
	Table    TableSpec
}

func dropTablesOf(dc DatabaseChange) []TableSpec { return dc.DropTables }

// createNode is one object to be created, carrying its dependency identity and
// the DDL that creates it.
type createNode struct {
	ref ObjectRef
	sql string
}

// orderedCreates returns the CREATE statements for every added table,
// materialized view, view, and dictionary in a single dependency-respecting
// order: an object is emitted only after every object it references that is also
// being created in this change set. Objects without a dependency relationship
// keep their historical relative order (tables, then materialized views, then
// views, then dictionaries by name). Raw objects are handled by the caller.
func orderedCreates(cs ChangeSet) []string {
	var nodes []createNode
	add := func(db, name, sql string) {
		nodes = append(nodes, createNode{ref: ObjectRef{Database: db, Name: name}, sql: sql})
	}
	for _, dc := range cs.Databases {
		for _, t := range dc.AddTables {
			add(dc.Database, t.Name, createTableSQL(dc.Database, t))
		}
	}
	for _, dc := range cs.Databases {
		for _, mv := range dc.AddMaterializedViews {
			add(dc.Database, mv.Name, createMaterializedViewSQL(dc.Database, mv))
		}
	}
	for _, dc := range cs.Databases {
		for _, v := range dc.AddViews {
			add(dc.Database, v.Name, createViewSQL(dc.Database, v))
		}
	}
	for _, dc := range cs.Databases {
		for _, d := range dictionariesByName(dc.AddDictionaries) {
			add(dc.Database, d.Name, createDictionarySQL(dc.Database, d))
		}
	}

	order := topoSortNodes(nodes, createDependencyEdges(cs))
	out := make([]string, 0, len(order))
	for _, n := range order {
		out = append(out, n.sql)
	}
	return out
}

// createDependencyEdges returns {from, to} edges among the added objects: from
// references to (so to must be created first). It reuses CollectDependencies for
// tables/materialized views/views and derives dictionary→source edges from the
// typed dictionary spec, which CollectDependencies does not cover.
func createDependencyEdges(cs ChangeSet) [][2]ObjectRef {
	var edges [][2]ObjectRef
	if deps, err := CollectDependencies(addedSchema(cs)); err == nil {
		for _, d := range deps {
			edges = append(edges, [2]ObjectRef{d.From, d.To})
		}
	}
	for _, dc := range cs.Databases {
		for _, d := range dc.AddDictionaries {
			from := ObjectRef{Database: dc.Database, Name: d.Name}
			for _, to := range dictionarySourceRefs(dc.Database, d) {
				edges = append(edges, [2]ObjectRef{from, to})
			}
		}
	}
	return edges
}

// addedSchema projects the change set's added objects into a []DatabaseSpec so
// the shared dependency collector can analyse them.
func addedSchema(cs ChangeSet) []DatabaseSpec {
	dbs := make([]DatabaseSpec, 0, len(cs.Databases))
	for _, dc := range cs.Databases {
		dbs = append(dbs, DatabaseSpec{
			Name:              dc.Database,
			Tables:            dc.AddTables,
			MaterializedViews: dc.AddMaterializedViews,
			Views:             dc.AddViews,
			Dictionaries:      dc.AddDictionaries,
		})
	}
	return dbs
}

// dictionarySourceRefs returns the table(s) a dictionary reads from via a
// ClickHouse source (TABLE or QUERY). Other source kinds (HTTP, file, ...) have
// no in-schema dependency. An unqualified name defaults to the dictionary's
// database.
func dictionarySourceRefs(db string, d DictionarySpec) []ObjectRef {
	if d.Source == nil {
		return nil
	}
	ch, ok := d.Source.Decoded.(SourceClickHouse)
	if !ok {
		return nil
	}
	if ch.Table != nil && *ch.Table != "" {
		ref := ObjectRef{Database: db, Name: *ch.Table}
		if ch.DB != nil && *ch.DB != "" {
			ref.Database = *ch.DB
		}
		return []ObjectRef{ref}
	}
	if ch.Query != nil && *ch.Query != "" {
		refs, err := extractSourceTables(*ch.Query)
		if err != nil {
			return nil
		}
		for i := range refs {
			if refs[i].Database == "" {
				refs[i].Database = db
			}
		}
		return refs
	}
	return nil
}

// topoSortNodes returns nodes in dependency order: for every edge {from, to}
// where both endpoints are nodes, to is emitted before from. Independent nodes
// keep their input order; a dependency cycle is broken by emitting the remaining
// nodes in input order. Mirrors orderTablesByDependency's Kahn's algorithm,
// generalised across object kinds.
func topoSortNodes(nodes []createNode, edges [][2]ObjectRef) []createNode {
	if len(nodes) < 2 {
		return nodes
	}
	index := make(map[ObjectRef]int, len(nodes))
	for i, n := range nodes {
		index[n.ref] = i
	}
	indegree := make([]int, len(nodes))
	dependents := make([][]int, len(nodes))
	seen := make(map[[2]int]bool)
	for _, e := range edges {
		from, okF := index[e[0]]
		to, okT := index[e[1]]
		if !okF || !okT || from == to || seen[[2]int{from, to}] {
			continue
		}
		seen[[2]int{from, to}] = true
		indegree[from]++
		dependents[to] = append(dependents[to], from)
	}

	done := make([]bool, len(nodes))
	order := make([]createNode, 0, len(nodes))
	for len(order) < len(nodes) {
		progressed := false
		for i := range nodes {
			if done[i] || indegree[i] != 0 {
				continue
			}
			order = append(order, nodes[i])
			done[i] = true
			progressed = true
			for _, d := range dependents[i] {
				indegree[d]--
			}
		}
		if !progressed { // dependency cycle: emit the rest in input order
			for i := range nodes {
				if !done[i] {
					order = append(order, nodes[i])
					done[i] = true
				}
			}
		}
	}
	return order
}

// gatherTables flattens one table collection (adds or drops) across every
// database in the ChangeSet into a single ordered slice.
func gatherTables(cs ChangeSet, pick func(DatabaseChange) []TableSpec) []dbTable {
	var out []dbTable
	for _, dc := range cs.Databases {
		for _, t := range pick(dc) {
			out = append(out, dbTable{Database: dc.Database, Table: t})
		}
	}
	return out
}

// orderTablesByDependency orders tables so a Distributed table comes after the
// local table it forwards to, when that table is part of the same set.
// Dependencies on tables outside the set impose no constraint. When reverse is
// true the order is flipped, so dependents come before their dependencies —
// used for DROP statements. Tables in a dependency cycle keep their input
// order. The sort is otherwise stable.
func orderTablesByDependency(tables []dbTable, reverse bool) []dbTable {
	index := make(map[ObjectRef]int, len(tables))
	for i, dt := range tables {
		index[ObjectRef{Database: dt.Database, Name: dt.Table.Name}] = i
	}

	indegree := make([]int, len(tables))     // count of unresolved dependencies
	dependents := make([][]int, len(tables)) // dependents[j] = tables that depend on j
	for i, dt := range tables {
		if dt.Table.Engine == nil {
			continue
		}
		dist, ok := dt.Table.Engine.Decoded.(EngineDistributed)
		if !ok {
			continue
		}
		remote := ObjectRef{Database: dist.RemoteDatabase, Name: dist.RemoteTable}
		if j, ok := index[remote]; ok && j != i {
			indegree[i]++
			dependents[j] = append(dependents[j], i)
		}
	}

	// Kahn's algorithm; ready nodes are emitted in input order for stability.
	done := make([]bool, len(tables))
	order := make([]int, 0, len(tables))
	for len(order) < len(tables) {
		progressed := false
		for i := range tables {
			if done[i] || indegree[i] != 0 {
				continue
			}
			order = append(order, i)
			done[i] = true
			progressed = true
			for _, d := range dependents[i] {
				indegree[d]--
			}
		}
		if !progressed { // dependency cycle: emit the rest in input order
			for i := range tables {
				if !done[i] {
					order = append(order, i)
					done[i] = true
				}
			}
		}
	}

	out := make([]dbTable, len(tables))
	for pos, i := range order {
		if reverse {
			out[len(out)-1-pos] = tables[i]
		} else {
			out[pos] = tables[i]
		}
	}
	return out
}

// createMaterializedViewSQL renders a CREATE MATERIALIZED VIEW in its
// `TO <table>` form. The column list, when present, is emitted between the
// destination table and AS — matching ClickHouse's accepted syntax.
func createMaterializedViewSQL(database string, mv MaterializedViewSpec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE MATERIALIZED VIEW %s.%s", database, mv.Name)
	if mv.Cluster != nil {
		fmt.Fprintf(&b, " ON CLUSTER %s", *mv.Cluster)
	}
	fmt.Fprintf(&b, " TO %s", mv.ToTable)
	if len(mv.Columns) > 0 {
		parts := make([]string, len(mv.Columns))
		for i, c := range mv.Columns {
			parts[i] = columnDefSQL(c)
		}
		fmt.Fprintf(&b, " (%s)", strings.Join(parts, ", "))
	}
	fmt.Fprintf(&b, " AS %s", mv.Query)
	// COMMENT comes last, after AS SELECT, per the CREATE MATERIALIZED VIEW grammar.
	if mv.Comment != nil {
		fmt.Fprintf(&b, " COMMENT %s", quoteString(*mv.Comment))
	}
	return b.String()
}

// modifyQuerySQL renders an in-place query update for a materialized view
// or plain view (the syntax is identical: ALTER TABLE ... MODIFY QUERY).
func modifyQuerySQL(database, name, query string) string {
	return fmt.Sprintf("ALTER TABLE %s.%s MODIFY QUERY %s", database, name, query)
}

// modifyCommentSQL renders an in-place comment update via ALTER TABLE.
// ClickHouse accepts this form on plain views.
func modifyCommentSQL(database, name, comment string) string {
	return fmt.Sprintf("ALTER TABLE %s.%s MODIFY COMMENT %s", database, name, quoteString(comment))
}

func dropViewSQL(database, name string) string {
	return fmt.Sprintf("DROP VIEW %s.%s", database, name)
}

// createViewSQL renders the full CREATE VIEW statement for a ViewSpec.
// Field order follows the ClickHouse grammar:
//
//	CREATE VIEW [ON CLUSTER c] db.name [(aliases)] [DEFINER = u]
//	  [SQL SECURITY ...] AS <query> [COMMENT '...']
//
// viewQueryProjectsStar reports whether the view's top-level output projection
// (the outer SELECT and any UNION/EXCEPT branch) includes a star (`*` or a
// qualified `x.*`). It deliberately does not descend into FROM subqueries or
// expression subqueries — stars there do not determine the view's own output
// columns. When the query can't be parsed it returns false, leaving alias
// emission unchanged.
func viewQueryProjectsStar(query string) bool {
	stmts, err := chparser.NewParser(query).ParseStmts()
	if err != nil || len(stmts) == 0 {
		return false
	}
	head, ok := stmts[0].(*chparser.SelectQuery)
	if !ok {
		// Fall back to the outermost SelectQuery the walker reports first.
		for _, n := range chparser.FindAll(stmts[0], isSelectQuery) {
			head = n.(*chparser.SelectQuery)
			break
		}
	}
	for sq := head; sq != nil; sq = nextUnionBranch(sq) {
		for _, item := range sq.SelectItems {
			if item == nil || item.Expr == nil {
				continue
			}
			expr := strings.TrimSpace(formatNode(item.Expr))
			if expr == "*" || strings.HasSuffix(expr, ".*") {
				return true
			}
		}
	}
	return false
}

// nextUnionBranch returns the next branch in a UNION ALL / UNION DISTINCT /
// EXCEPT chain, or nil at the end.
func nextUnionBranch(sq *chparser.SelectQuery) *chparser.SelectQuery {
	switch {
	case sq.UnionAll != nil:
		return sq.UnionAll
	case sq.UnionDistinct != nil:
		return sq.UnionDistinct
	case sq.Except != nil:
		return sq.Except
	}
	return nil
}

func createViewSQL(database string, v ViewSpec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE VIEW %s.%s", database, v.Name)
	if v.Cluster != nil {
		fmt.Fprintf(&b, " ON CLUSTER %s", *v.Cluster)
	}
	// A view whose output projection includes a star can't carry an explicit
	// column-alias list — ClickHouse rejects `CREATE VIEW v (a, b) AS SELECT *`
	// because the column count isn't statically known. The list captured at
	// introspection is ClickHouse-inferred, so omit it and let ClickHouse
	// re-infer the columns (issue #41).
	if len(v.ColumnAliases) > 0 && !viewQueryProjectsStar(v.Query) {
		fmt.Fprintf(&b, " (%s)", strings.Join(v.ColumnAliases, ", "))
	}
	if v.Definer != nil {
		fmt.Fprintf(&b, " DEFINER = %s", *v.Definer)
	}
	if v.SQLSecurity != nil {
		fmt.Fprintf(&b, " SQL SECURITY %s", strings.ToUpper(*v.SQLSecurity))
	}
	fmt.Fprintf(&b, " AS %s", v.Query)
	if v.Comment != nil {
		fmt.Fprintf(&b, " COMMENT %s", quoteString(*v.Comment))
	}
	return b.String()
}

func createTableSQL(database string, t TableSpec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s.%s", database, t.Name)
	if t.Cluster != nil {
		fmt.Fprintf(&b, " ON CLUSTER %s", *t.Cluster)
	}
	b.WriteString(" (\n")

	var parts []string
	for _, c := range t.Columns {
		parts = append(parts, "  "+columnDefSQL(c))
	}
	for _, con := range t.Constraints {
		parts = append(parts, "  "+constraintClause(con))
	}
	for _, idx := range t.Indexes {
		parts = append(parts, fmt.Sprintf("  INDEX %s", indexClause(idx)))
	}
	b.WriteString(strings.Join(parts, ",\n"))
	b.WriteString("\n)")

	clause, extraSettings := engineSQL(engineOf(t))
	fmt.Fprintf(&b, " ENGINE = %s", clause)

	// TimeSeries: emit the SAMPLES/TAGS/METRICS tail clauses between the
	// ENGINE clause and the storage clauses (which are mostly inapplicable
	// to TimeSeries anyway — the outer table has no ORDER BY etc.).
	if ts, ok := engineOf(t).(EngineTimeSeries); ok {
		b.WriteString(timeSeriesTailSQL(ts))
	}

	if len(t.PrimaryKey) > 0 {
		fmt.Fprintf(&b, " PRIMARY KEY (%s)", strings.Join(t.PrimaryKey, ", "))
	}
	if len(t.OrderBy) > 0 {
		fmt.Fprintf(&b, " ORDER BY (%s)", strings.Join(t.OrderBy, ", "))
	}
	if t.PartitionBy != nil {
		fmt.Fprintf(&b, " PARTITION BY %s", *t.PartitionBy)
	}
	if t.SampleBy != nil {
		fmt.Fprintf(&b, " SAMPLE BY %s", *t.SampleBy)
	}
	if t.TTL != nil {
		fmt.Fprintf(&b, " TTL %s", *t.TTL)
	}

	settings := mergeSettings(t.Settings, extraSettings)
	if len(settings) > 0 {
		fmt.Fprintf(&b, " SETTINGS %s", formatSettingsList(settings))
	}

	// COMMENT must come after all storage clauses (per docs).
	if t.Comment != nil {
		fmt.Fprintf(&b, " COMMENT %s", quoteString(*t.Comment))
	}
	return b.String()
}

func constraintClause(c ConstraintSpec) string {
	if c.Check != nil {
		return fmt.Sprintf("CONSTRAINT %s CHECK %s", c.Name, *c.Check)
	}
	if c.Assume != nil {
		return fmt.Sprintf("CONSTRAINT %s ASSUME %s", c.Name, *c.Assume)
	}
	return fmt.Sprintf("CONSTRAINT %s", c.Name) // shouldn't happen post-validation
}

func dropTableSQL(database, table string) string {
	return fmt.Sprintf("DROP TABLE %s.%s", database, table)
}

// createRawSQL emits a raw object's stored CREATE DDL verbatim, trimming the
// canonical trailing newline so it matches the formatting of other generated
// statements.
func createRawSQL(r RawSpec) string {
	return strings.TrimRight(r.SQL, "\n")
}

// dropRawSQL renders the DROP for a raw object, choosing the statement form
// from its kind. Dictionaries need DROP DICTIONARY and plain views DROP VIEW;
// tables and (TO-form) materialized views are dropped with DROP TABLE.
func dropRawSQL(kind, database, name string) string {
	form := "TABLE"
	switch kind {
	case "dictionary":
		form = "DICTIONARY"
	case "view":
		form = "VIEW"
	}
	return fmt.Sprintf("DROP %s IF EXISTS %s.%s", form, database, name)
}

func alterTableSQL(database string, td TableDiff) string {
	var ops []string
	for _, r := range td.RenameColumns {
		ops = append(ops, fmt.Sprintf("RENAME COLUMN %s TO %s", r.Old, r.New))
	}
	for _, c := range td.AddColumns {
		ops = append(ops, "ADD COLUMN "+columnDefSQL(c))
	}
	for _, n := range td.DropColumns {
		ops = append(ops, fmt.Sprintf("DROP COLUMN %s", n))
	}
	for _, c := range td.ModifyColumns {
		// A storage-class switch (to/from ALIAS/MATERIALIZED/EPHEMERAL) is
		// data-affecting and not auto-emitted; it surfaces via unsafeReasons.
		if c.IsUnsafe() {
			continue
		}
		ops = append(ops, "MODIFY COLUMN "+columnDefSQL(c.New))
	}
	for _, n := range td.DropIndexes {
		ops = append(ops, fmt.Sprintf("DROP INDEX %s", n))
	}
	for _, idx := range td.AddIndexes {
		ops = append(ops, fmt.Sprintf("ADD INDEX %s", indexClause(idx)))
	}
	for _, k := range sortedKeys(td.SettingsAdded) {
		ops = append(ops, fmt.Sprintf("MODIFY SETTING %s = %s", k, formatSettingValue(td.SettingsAdded[k])))
	}
	for _, c := range td.SettingsChanged {
		ops = append(ops, fmt.Sprintf("MODIFY SETTING %s = %s", c.Key, formatSettingValue(c.NewValue)))
	}
	for _, k := range td.SettingsRemoved {
		ops = append(ops, fmt.Sprintf("RESET SETTING %s", k))
	}
	if td.TTLChange != nil {
		if td.TTLChange.New != nil {
			ops = append(ops, fmt.Sprintf("MODIFY TTL %s", *td.TTLChange.New))
		} else {
			ops = append(ops, "REMOVE TTL")
		}
	}
	if len(ops) == 0 {
		return ""
	}
	return fmt.Sprintf("ALTER TABLE %s.%s %s", database, td.Table, strings.Join(ops, ", "))
}

// columnDefSQL renders one column definition in the order ClickHouse
// documents:
//
//	name type [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr]
//	  [COMMENT 'text'] [CODEC(...)] [TTL expr]
//
// Nullable expansion happens here: `nullable = true` wraps the type in
// Nullable(...), unless the type is already a Nullable(...).
func columnDefSQL(c ColumnSpec) string {
	var sb strings.Builder
	sb.WriteString(c.Name)
	sb.WriteByte(' ')
	sb.WriteString(effectiveType(c))

	switch {
	case c.Default != nil:
		fmt.Fprintf(&sb, " DEFAULT %s", *c.Default)
	case c.Materialized != nil:
		fmt.Fprintf(&sb, " MATERIALIZED %s", *c.Materialized)
	case c.Ephemeral != nil:
		if *c.Ephemeral == "" {
			sb.WriteString(" EPHEMERAL")
		} else {
			fmt.Fprintf(&sb, " EPHEMERAL %s", *c.Ephemeral)
		}
	case c.Alias != nil:
		fmt.Fprintf(&sb, " ALIAS %s", *c.Alias)
	}

	if c.Comment != nil {
		fmt.Fprintf(&sb, " COMMENT %s", quoteString(*c.Comment))
	}
	if c.Codec != nil {
		fmt.Fprintf(&sb, " CODEC(%s)", *c.Codec)
	}
	if c.TTL != nil {
		fmt.Fprintf(&sb, " TTL %s", *c.TTL)
	}
	return sb.String()
}

// effectiveType returns Type wrapped in Nullable(...) when c.Nullable is set
// and Type isn't already Nullable. The conflict case (nullable = true with a
// pre-wrapped Type) is rejected by the resolver, not here.
func effectiveType(c ColumnSpec) string {
	if c.Nullable && !strings.HasPrefix(c.Type, "Nullable(") {
		return "Nullable(" + c.Type + ")"
	}
	return c.Type
}

func quoteString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "\\'") + "'"
}

func indexClause(idx IndexSpec) string {
	if idx.Granularity > 0 {
		return fmt.Sprintf("%s %s TYPE %s GRANULARITY %d", idx.Name, idx.Expr, idx.Type, idx.Granularity)
	}
	return fmt.Sprintf("%s %s TYPE %s", idx.Name, idx.Expr, idx.Type)
}

// engineOf returns the decoded Engine value from a TableSpec, or nil if the
// table has no engine (only abstract tables should ever satisfy that, and
// they don't reach SQL generation).
func engineOf(t TableSpec) Engine {
	if t.Engine == nil {
		return nil
	}
	return t.Engine.Decoded
}

// engineSQL renders an Engine as a ClickHouse engine clause. The second
// return is any extra SETTINGS that should be folded into the CREATE TABLE
// SETTINGS clause (used by Kafka, which expresses its arguments via SETTINGS
// rather than constructor args).
func engineSQL(e Engine) (clause string, extraSettings map[string]string) {
	switch v := e.(type) {
	case EngineMergeTree:
		return "MergeTree()", nil
	case EngineReplicatedMergeTree:
		return fmt.Sprintf("ReplicatedMergeTree('%s', '%s')", v.ZooPath, v.ReplicaName), nil
	case EngineReplacingMergeTree:
		if v.VersionColumn != nil {
			return fmt.Sprintf("ReplacingMergeTree(%s)", *v.VersionColumn), nil
		}
		return "ReplacingMergeTree()", nil
	case EngineReplicatedReplacingMergeTree:
		if v.VersionColumn != nil {
			return fmt.Sprintf("ReplicatedReplacingMergeTree('%s', '%s', %s)", v.ZooPath, v.ReplicaName, *v.VersionColumn), nil
		}
		return fmt.Sprintf("ReplicatedReplacingMergeTree('%s', '%s')", v.ZooPath, v.ReplicaName), nil
	case EngineSummingMergeTree:
		if len(v.SumColumns) > 0 {
			return fmt.Sprintf("SummingMergeTree((%s))", strings.Join(v.SumColumns, ", ")), nil
		}
		return "SummingMergeTree()", nil
	case EngineReplicatedSummingMergeTree:
		if len(v.SumColumns) > 0 {
			return fmt.Sprintf("ReplicatedSummingMergeTree('%s', '%s', (%s))", v.ZooPath, v.ReplicaName, strings.Join(v.SumColumns, ", ")), nil
		}
		return fmt.Sprintf("ReplicatedSummingMergeTree('%s', '%s')", v.ZooPath, v.ReplicaName), nil
	case EngineCollapsingMergeTree:
		return fmt.Sprintf("CollapsingMergeTree(%s)", v.SignColumn), nil
	case EngineReplicatedCollapsingMergeTree:
		return fmt.Sprintf("ReplicatedCollapsingMergeTree('%s', '%s', %s)", v.ZooPath, v.ReplicaName, v.SignColumn), nil
	case EngineAggregatingMergeTree:
		return "AggregatingMergeTree()", nil
	case EngineReplicatedAggregatingMergeTree:
		return fmt.Sprintf("ReplicatedAggregatingMergeTree('%s', '%s')", v.ZooPath, v.ReplicaName), nil
	case EngineDistributed:
		if v.ShardingKey != nil {
			return fmt.Sprintf("Distributed('%s', '%s', '%s', %s)", v.ClusterName, v.RemoteDatabase, v.RemoteTable, *v.ShardingKey), nil
		}
		return fmt.Sprintf("Distributed('%s', '%s', '%s')", v.ClusterName, v.RemoteDatabase, v.RemoteTable), nil
	case EngineLog:
		return "Log()", nil
	case EngineJoin:
		// CH expects bare (unquoted) strictness, type, and key columns:
		// Join(ANY, LEFT, k1, k2, ...).
		parts := append([]string{v.Strictness, v.JoinType}, v.Keys...)
		return fmt.Sprintf("Join(%s)", strings.Join(parts, ", ")), nil
	case EngineNull:
		return "Null()", nil
	case EngineMemory:
		return "Memory()", nil
	case EngineMerge:
		return fmt.Sprintf("Merge('%s', '%s')", v.DBRegex, v.TableRegex), nil
	case EngineBuffer:
		// Buffer(db, table, num_layers, min_time, max_time, min_rows,
		//        max_rows, min_bytes, max_bytes [, flush_time [, flush_rows [, flush_bytes]]]).
		// The database arg uses currentDatabase() when empty (CH convention).
		dbArg := "''"
		if v.Database != "" {
			dbArg = fmt.Sprintf("'%s'", v.Database)
		}
		args := []string{
			dbArg,
			fmt.Sprintf("'%s'", v.Table),
			fmt.Sprintf("%d", v.NumLayers),
			fmt.Sprintf("%d", v.MinTime),
			fmt.Sprintf("%d", v.MaxTime),
			fmt.Sprintf("%d", v.MinRows),
			fmt.Sprintf("%d", v.MaxRows),
			fmt.Sprintf("%d", v.MinBytes),
			fmt.Sprintf("%d", v.MaxBytes),
		}
		if v.FlushTime != nil {
			args = append(args, fmt.Sprintf("%d", *v.FlushTime))
			if v.FlushRows != nil {
				args = append(args, fmt.Sprintf("%d", *v.FlushRows))
				if v.FlushBytes != nil {
					args = append(args, fmt.Sprintf("%d", *v.FlushBytes))
				}
			}
		}
		return fmt.Sprintf("Buffer(%s)", strings.Join(args, ", ")), nil
	case EngineKafka:
		if v.Collection != nil {
			// Named collection form: Kafka(<collection>); no settings emitted.
			return fmt.Sprintf("Kafka(%s)", *v.Collection), nil
		}
		settings := map[string]string{}
		setStr := func(name string, p *string) {
			if p != nil {
				settings[name] = *p
			}
		}
		setInt := func(name string, p *int64) {
			if p != nil {
				settings[name] = fmt.Sprintf("%d", *p)
			}
		}
		setBool := func(name string, p *bool) {
			if p != nil {
				if *p {
					settings[name] = "1"
				} else {
					settings[name] = "0"
				}
			}
		}
		setStr("kafka_broker_list", v.BrokerList)
		setStr("kafka_topic_list", v.TopicList)
		setStr("kafka_group_name", v.GroupName)
		setStr("kafka_format", v.Format)
		setStr("kafka_security_protocol", v.SecurityProtocol)
		setStr("kafka_sasl_mechanism", v.SaslMechanism)
		setStr("kafka_sasl_username", v.SaslUsername)
		setStr("kafka_sasl_password", v.SaslPassword)
		setStr("kafka_client_id", v.ClientID)
		setStr("kafka_schema", v.Schema)
		setStr("kafka_handle_error_mode", v.HandleErrorMode)
		setStr("kafka_compression_codec", v.CompressionCodec)
		setInt("kafka_num_consumers", v.NumConsumers)
		setInt("kafka_max_block_size", v.MaxBlockSize)
		setInt("kafka_skip_broken_messages", v.SkipBrokenMessages)
		setInt("kafka_poll_timeout_ms", v.PollTimeoutMs)
		setInt("kafka_poll_max_batch_size", v.PollMaxBatchSize)
		setInt("kafka_flush_interval_ms", v.FlushIntervalMs)
		setInt("kafka_consumer_reschedule_ms", v.ConsumerRescheduleMs)
		setInt("kafka_max_rows_per_message", v.MaxRowsPerMessage)
		setInt("kafka_compression_level", v.CompressionLevel)
		setBool("kafka_commit_every_batch", v.CommitEveryBatch)
		setBool("kafka_thread_per_consumer", v.ThreadPerConsumer)
		setBool("kafka_commit_on_select", v.CommitOnSelect)
		setBool("kafka_autodetect_client_rack", v.AutodetectClientRack)
		for k, val := range v.Extra {
			settings[k] = val
		}
		return "Kafka()", settings
	case EngineTimeSeries:
		// TimeSeries takes no constructor args. Its tail clauses
		// (SAMPLES/TAGS/METRICS) are emitted separately by
		// createTableSQL via timeSeriesTailSQL. SETTINGS flow through the
		// extraSettings map, with TagsToColumns folded back to its
		// CH map-literal form.
		settings := map[string]string{}
		for k, val := range v.Settings {
			settings[k] = val
		}
		if len(v.TagsToColumns) > 0 {
			settings["tags_to_columns"] = renderTagsToColumnsMap(v.TagsToColumns)
		}
		return "TimeSeries", settings
	}
	return "", nil
}

// renderTagsToColumnsMap renders the typed TagsToColumns map back to the
// `{'tag1':'col1', 'tag2':'col2'}` literal CH expects in SETTINGS. Keys
// sorted for deterministic output.
func renderTagsToColumnsMap(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("'%s':'%s'", k, m[k])
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// timeSeriesTailSQL renders the SAMPLES/TAGS/METRICS tail clauses that
// follow `ENGINE = TimeSeries` on a CREATE TABLE statement. Empty string
// when no target sub-blocks are set.
func timeSeriesTailSQL(e EngineTimeSeries) string {
	var b strings.Builder
	emitTimeSeriesTarget(&b, e.Samples, samplesKeyword(e.KeywordHint))
	emitTimeSeriesTarget(&b, e.Tags, "TAGS")
	emitTimeSeriesTarget(&b, e.Metrics, "METRICS")
	return b.String()
}

func samplesKeyword(hint string) string {
	if hint == "DATA" {
		return "DATA"
	}
	return "SAMPLES"
}

func emitTimeSeriesTarget(b *strings.Builder, t *TimeSeriesTarget, keyword string) {
	if t == nil {
		return
	}
	if t.Target != nil {
		fmt.Fprintf(b, " %s %s", keyword, *t.Target)
		return
	}
	if t.Inner == nil {
		return
	}
	parts := make([]string, len(t.Inner.Columns))
	for i, c := range t.Inner.Columns {
		parts[i] = columnDefSQL(c)
	}
	fmt.Fprintf(b, " %s INNER COLUMNS (%s)", keyword, strings.Join(parts, ", "))
	if t.Inner.Engine != nil && t.Inner.Engine.Decoded != nil {
		innerClause, _ := engineSQL(t.Inner.Engine.Decoded)
		fmt.Fprintf(b, " %s INNER ENGINE = %s", keyword, innerClause)
	}
	if len(t.Inner.PrimaryKey) > 0 {
		fmt.Fprintf(b, " PRIMARY KEY (%s)", strings.Join(t.Inner.PrimaryKey, ", "))
	}
	if len(t.Inner.OrderBy) > 0 {
		fmt.Fprintf(b, " ORDER BY (%s)", strings.Join(t.Inner.OrderBy, ", "))
	}
	if t.Inner.PartitionBy != nil {
		fmt.Fprintf(b, " PARTITION BY %s", *t.Inner.PartitionBy)
	}
}

func mergeSettings(user, extra map[string]string) map[string]string {
	if len(user) == 0 && len(extra) == 0 {
		return nil
	}
	out := make(map[string]string, len(user)+len(extra))
	for k, v := range extra {
		out[k] = v
	}
	for k, v := range user {
		out[k] = v
	}
	return out
}

func formatSettingsList(settings map[string]string) string {
	keys := make([]string, 0, len(settings))
	for k := range settings {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s = %s", k, formatSettingValue(settings[k]))
	}
	return strings.Join(parts, ", ")
}

var numericRe = regexp.MustCompile(`^-?\d+(\.\d+)?$`)

func formatSettingValue(v string) string {
	if numericRe.MatchString(v) {
		return v
	}
	// Map and array literals (e.g. tags_to_columns = {'k':'v'}) are emitted
	// bare. CH parses them as compound literals, not as quoted strings.
	if len(v) >= 2 && ((v[0] == '{' && v[len(v)-1] == '}') || (v[0] == '[' && v[len(v)-1] == ']')) {
		return v
	}
	return "'" + strings.ReplaceAll(v, "'", "\\'") + "'"
}

func unsafeReasons(database string, td TableDiff) []UnsafeChange {
	var out []UnsafeChange
	if td.EngineChange != nil {
		fromKind, toKind := "(none)", "(none)"
		if td.EngineChange.Old != nil {
			fromKind = td.EngineChange.Old.Kind()
		}
		if td.EngineChange.New != nil {
			toKind = td.EngineChange.New.Kind()
		}
		out = append(out, UnsafeChange{
			Database: database, Table: td.Table,
			Reason: fmt.Sprintf("engine change from %s to %s requires recreating the table", fromKind, toKind),
		})
	}
	if td.OrderByChange != nil {
		out = append(out, UnsafeChange{
			Database: database, Table: td.Table,
			Reason: "ORDER BY change requires recreating the table",
		})
	}
	if td.PartitionByChange != nil {
		out = append(out, UnsafeChange{
			Database: database, Table: td.Table,
			Reason: "PARTITION BY change requires recreating the table",
		})
	}
	if td.SampleByChange != nil {
		out = append(out, UnsafeChange{
			Database: database, Table: td.Table,
			Reason: "SAMPLE BY change requires recreating the table",
		})
	}
	for _, c := range td.ModifyColumns {
		if c.IsUnsafe() {
			out = append(out, UnsafeChange{
				Database: database, Table: td.Table,
				Reason: fmt.Sprintf("column %q change from %s to %s switches its storage class and is data-affecting; not auto-applied",
					c.Name, columnKind(c.Old), columnKind(c.New)),
			})
		}
	}
	return out
}
