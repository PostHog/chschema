package hcl

import (
	"fmt"
	"strings"

	chparser "github.com/orian/clickhouse-sql-parser/parser"
)

// ApplySQL parses ClickHouse DDL and applies each statement, in order, to
// schema. It is the engine behind `hclexp sql2hcl`: developers express a change
// as the SQL they already know and it is folded into their declarative HCL.
//
// Supported statements (declarative schema changes only):
//
//   - CREATE TABLE | MATERIALIZED VIEW | VIEW | DICTIONARY — adds the object, or
//     replaces an existing object of the same name.
//   - ALTER TABLE … ADD/DROP/MODIFY/RENAME COLUMN, ADD/DROP INDEX,
//     MODIFY/REMOVE TTL, MODIFY/RESET SETTING — edits the matching table block.
//   - ALTER TABLE <mv> MODIFY QUERY … — replaces a materialized view's query.
//   - DROP TABLE | VIEW | DICTIONARY | DATABASE — removes the object.
//   - RENAME TABLE a TO b — renames (and moves across databases).
//
// Data and partition operations (TRUNCATE, ALTER … DELETE, attach/detach/drop/
// freeze/replace partition, materialize index/projection) carry no declarative
// schema meaning and are rejected with a clear error.
//
// defaultDatabase supplies the database for unqualified object names. When it is
// empty and the schema has exactly one database, that database is used.
//
// allowRaw mirrors `introspect`: a CREATE the schema model cannot express is
// captured verbatim as a raw{} block instead of failing. (A statement that does
// not even parse aborts the whole call — per-statement isolation is impossible
// once the parser has rejected the input.)
//
// It returns the number of statements applied.
func ApplySQL(schema *Schema, sql, defaultDatabase string, allowRaw bool) (int, error) {
	p := chparser.NewParser(sql)
	stmts, err := p.ParseStmts()
	if err != nil {
		return 0, fmt.Errorf("parse SQL: %w", err)
	}
	applied := 0
	for _, stmt := range stmts {
		if err := applyStatement(schema, stmt, defaultDatabase, allowRaw); err != nil {
			return applied, err
		}
		applied++
	}
	return applied, nil
}

// applyStatement dispatches a single parsed statement to the right mutation.
func applyStatement(schema *Schema, stmt chparser.Expr, defaultDatabase string, allowRaw bool) error {
	switch s := stmt.(type) {
	case *chparser.CreateTable, *chparser.CreateMaterializedView,
		*chparser.CreateView, *chparser.CreateDictionary:
		return applyCreate(schema, stmt, defaultDatabase, allowRaw)
	case *chparser.AlterTable:
		return applyAlter(schema, s, defaultDatabase)
	case *chparser.DropStmt:
		return applyDrop(schema, s, defaultDatabase)
	case *chparser.DropDatabase:
		return applyDropDatabase(schema, s)
	case *chparser.RenameStmt:
		return applyRename(schema, s, defaultDatabase)
	default:
		return fmt.Errorf("unsupported statement %T (sql2hcl applies declarative schema DDL: CREATE / ALTER TABLE / DROP / RENAME)", stmt)
	}
}

// applyCreate adds or replaces an object from a CREATE statement, honoring the
// allowRaw escape hatch when the schema model cannot express it.
func applyCreate(schema *Schema, stmt chparser.Expr, defaultDatabase string, allowRaw bool) error {
	dbName, name := createTarget(stmt)
	resolved, err := resolveDatabaseName(schema, dbName, defaultDatabase)
	if err != nil {
		return err
	}
	db := findOrCreateDatabase(schema, resolved)
	if err := upsertObjectFromStmt(db, name, stmt); err != nil {
		if !allowRaw {
			return fmt.Errorf("%w (re-run with -allow-raw to capture this object as a raw SQL block instead of failing)", err)
		}
		kind := rawKindForStmt(stmt)
		upsertRaw(db, RawSpec{Kind: kind, Name: name, SQL: normalizeRawSQL(formatNode(stmt))})
	}
	return nil
}

// applyAlter applies an ALTER TABLE statement. The target is resolved as a
// table first, then a materialized view (MODIFY QUERY only).
func applyAlter(schema *Schema, a *chparser.AlterTable, defaultDatabase string) error {
	dbName, name := identifierParts(a.TableIdentifier, "")
	resolved, err := resolveDatabaseName(schema, dbName, defaultDatabase)
	if err != nil {
		return err
	}
	db := findDatabase(schema, resolved)
	if db == nil {
		return fmt.Errorf("ALTER TABLE %s.%s: database %q not found in schema", resolved, name, resolved)
	}

	if t := findTable(db, name); t != nil {
		for _, clause := range a.AlterExprs {
			if err := applyAlterClause(t, clause); err != nil {
				return fmt.Errorf("ALTER TABLE %s.%s: %w", resolved, name, err)
			}
		}
		return nil
	}

	if mv := findMaterializedView(db, name); mv != nil {
		for _, clause := range a.AlterExprs {
			mq, ok := clause.(*chparser.AlterTableModifyQuery)
			if !ok {
				return fmt.Errorf("ALTER TABLE %s.%s: only MODIFY QUERY is supported on a materialized view, got %s", resolved, name, clause.AlterType())
			}
			mv.Query = strings.TrimSpace(formatNode(mq.SelectExpr))
		}
		return nil
	}

	return fmt.Errorf("ALTER TABLE %s.%s: no such table or materialized view in schema", resolved, name)
}

// applyAlterClause applies one ALTER TABLE clause to a table block, reusing the
// introspection AST converters so column/index decoding stays identical.
func applyAlterClause(t *TableSpec, clause chparser.AlterTableClause) error {
	switch c := clause.(type) {
	case *chparser.AlterTableAddColumn:
		col := columnFromAST(c.Column)
		if findColumn(t, col.Name) >= 0 {
			if c.IfNotExists {
				return nil
			}
			return fmt.Errorf("ADD COLUMN %q: column already exists", col.Name)
		}
		after := ""
		if c.After != nil {
			after = identName(c.After)
		}
		insertColumnAfter(t, col, after)
	case *chparser.AlterTableModifyColumn:
		if c.RemovePropertyType != nil {
			return fmt.Errorf("MODIFY COLUMN %s REMOVE …: not a representable declarative change", identName(c.Column.Name))
		}
		col := columnFromAST(c.Column)
		idx := findColumn(t, col.Name)
		if idx < 0 {
			if c.IfExists {
				return nil
			}
			return fmt.Errorf("MODIFY COLUMN %q: no such column", col.Name)
		}
		t.Columns[idx] = col
	case *chparser.AlterTableDropColumn:
		name := identName(c.ColumnName)
		idx := findColumn(t, name)
		if idx < 0 {
			if c.IfExists {
				return nil
			}
			return fmt.Errorf("DROP COLUMN %q: no such column", name)
		}
		t.Columns = append(t.Columns[:idx], t.Columns[idx+1:]...)
	case *chparser.AlterTableRenameColumn:
		old := identName(c.OldColumnName)
		newName := identName(c.NewColumnName)
		idx := findColumn(t, old)
		if idx < 0 {
			if c.IfExists {
				return nil
			}
			return fmt.Errorf("RENAME COLUMN %q: no such column", old)
		}
		t.Columns[idx].Name = newName
		// Record the prior name so the diff engine emits RENAME COLUMN rather
		// than DROP + ADD when this schema is later compared against a live DB.
		t.Columns[idx].RenamedFrom = strPtr(old)
	case *chparser.AlterTableAddIndex:
		idx, err := indexFromAST(c.Index)
		if err != nil {
			return err
		}
		if findIndex(t, idx.Name) >= 0 {
			if c.IfNotExists {
				return nil
			}
			return fmt.Errorf("ADD INDEX %q: index already exists", idx.Name)
		}
		t.Indexes = append(t.Indexes, idx)
	case *chparser.AlterTableDropIndex:
		name := identName(c.IndexName)
		i := findIndex(t, name)
		if i < 0 {
			if c.IfExists {
				return nil
			}
			return fmt.Errorf("DROP INDEX %q: no such index", name)
		}
		t.Indexes = append(t.Indexes[:i], t.Indexes[i+1:]...)
	case *chparser.AlterTableAddProjection:
		p := projectionFromAST(c.TableProjection)
		if findProjection(t, p.Name) >= 0 {
			if c.IfNotExists {
				return nil
			}
			return fmt.Errorf("ADD PROJECTION %q: projection already exists", p.Name)
		}
		t.Projections = append(t.Projections, p)
	case *chparser.AlterTableDropProjection:
		name := identName(c.ProjectionName)
		i := findProjection(t, name)
		if i < 0 {
			if c.IfExists {
				return nil
			}
			return fmt.Errorf("DROP PROJECTION %q: no such projection", name)
		}
		t.Projections = append(t.Projections[:i], t.Projections[i+1:]...)
	case *chparser.AlterTableModifyTTL:
		if c.TTL != nil && len(c.TTL.Items) > 0 {
			t.TTL = strPtr(formatNode(c.TTL.Items[0].Expr))
		}
	case *chparser.AlterTableRemoveTTL:
		t.TTL = nil
	case *chparser.AlterTableModifySetting:
		if t.Settings == nil {
			t.Settings = map[string]string{}
		}
		for _, s := range c.Settings {
			if s.Name == nil {
				continue
			}
			t.Settings[s.Name.Name] = formatNode(s.Expr)
		}
	case *chparser.AlterTableResetSetting:
		for _, s := range c.Settings {
			delete(t.Settings, s.Name)
		}
	default:
		return fmt.Errorf("unsupported ALTER operation %s (not a declarative schema change)", clause.AlterType())
	}
	return nil
}

// applyDrop removes a table, view, materialized view, or dictionary.
func applyDrop(schema *Schema, d *chparser.DropStmt, defaultDatabase string) error {
	dbName, name := identifierParts(d.Name, "")
	resolved, err := resolveDatabaseName(schema, dbName, defaultDatabase)
	if err != nil {
		return err
	}
	db := findDatabase(schema, resolved)
	if db == nil {
		if d.IfExists {
			return nil
		}
		return fmt.Errorf("DROP %s %s.%s: database %q not found in schema", d.DropTarget, resolved, name, resolved)
	}

	removed := false
	switch d.DropTarget {
	case "DICTIONARY":
		removed = removeDictionary(db, name)
	case "VIEW":
		// A ClickHouse materialized view is also dropped via DROP VIEW.
		removed = removeView(db, name) || removeMaterializedView(db, name)
	default: // TABLE
		removed = removeTable(db, name) || removeMaterializedView(db, name) || removeRaw(db, name)
	}
	if !removed && !d.IfExists {
		return fmt.Errorf("DROP %s %s.%s: no such object in schema", d.DropTarget, resolved, name)
	}
	return nil
}

// applyDropDatabase removes a whole database block.
func applyDropDatabase(schema *Schema, d *chparser.DropDatabase) error {
	name := ""
	if d.Name != nil {
		name = d.Name.Name
	}
	for i := range schema.Databases {
		if schema.Databases[i].Name == name {
			schema.Databases = append(schema.Databases[:i], schema.Databases[i+1:]...)
			return nil
		}
	}
	if d.IfExists {
		return nil
	}
	return fmt.Errorf("DROP DATABASE %q: no such database in schema", name)
}

// applyRename renames objects (tables, MVs, views, dictionaries), moving them
// across databases when the rename targets a different database.
func applyRename(schema *Schema, r *chparser.RenameStmt, defaultDatabase string) error {
	for _, pair := range r.TargetPairList {
		oldDBName, oldName := identifierParts(pair.Old, "")
		newDBName, newName := identifierParts(pair.New, "")
		oldDB, err := resolveDatabaseName(schema, oldDBName, defaultDatabase)
		if err != nil {
			return err
		}
		newDB, err := resolveDatabaseName(schema, newDBName, defaultDatabase)
		if err != nil {
			return err
		}
		if err := renameObject(schema, oldDB, oldName, newDB, newName); err != nil {
			return err
		}
	}
	return nil
}

// renameObject finds the object named oldName in oldDB, renames it to newName,
// and moves it into newDB when the databases differ.
func renameObject(schema *Schema, oldDB, oldName, newDB, newName string) error {
	src := findDatabase(schema, oldDB)
	if src == nil {
		return fmt.Errorf("RENAME %s.%s: database %q not found in schema", oldDB, oldName, oldDB)
	}
	dst := findOrCreateDatabase(schema, newDB)

	if t := findTable(src, oldName); t != nil {
		moved := *t
		moved.Name = newName
		removeTable(src, oldName)
		upsertTable(dst, moved)
		return nil
	}
	if mv := findMaterializedView(src, oldName); mv != nil {
		moved := *mv
		moved.Name = newName
		removeMaterializedView(src, oldName)
		upsertMaterializedView(dst, moved)
		return nil
	}
	if v := findView(src, oldName); v != nil {
		moved := *v
		moved.Name = newName
		removeView(src, oldName)
		upsertView(dst, moved)
		return nil
	}
	if d := findDictionary(src, oldName); d != nil {
		moved := *d
		moved.Name = newName
		removeDictionary(src, oldName)
		upsertDictionary(dst, moved)
		return nil
	}
	return fmt.Errorf("RENAME %s.%s: no such object in schema", oldDB, oldName)
}

// --- target / name extraction helpers ---

// createTarget returns the (database, name) declared by a CREATE statement.
func createTarget(stmt chparser.Expr) (database, name string) {
	switch s := stmt.(type) {
	case *chparser.CreateTable:
		return identifierParts(s.Name, "")
	case *chparser.CreateMaterializedView:
		return identifierParts(s.Name, "")
	case *chparser.CreateView:
		return identifierParts(s.Name, "")
	case *chparser.CreateDictionary:
		return identifierParts(s.Name, "")
	}
	return "", ""
}

// identifierParts splits a TableIdentifier into (database, name). database falls
// back to defaultDB when the identifier is unqualified.
func identifierParts(t *chparser.TableIdentifier, defaultDB string) (database, name string) {
	if t == nil {
		return defaultDB, ""
	}
	if t.Table != nil {
		name = t.Table.Name
	}
	if t.Database != nil && t.Database.Name != "" {
		return t.Database.Name, name
	}
	return defaultDB, name
}

// rawKindForStmt maps a parsed CREATE statement to its RawSpec kind.
func rawKindForStmt(stmt chparser.Expr) string {
	switch stmt.(type) {
	case *chparser.CreateMaterializedView:
		return "materialized_view"
	case *chparser.CreateView:
		return "view"
	case *chparser.CreateDictionary:
		return "dictionary"
	default:
		return "table"
	}
}

// resolveDatabaseName picks the database for an object: the explicit name from
// the SQL, else the -database default, else the sole database in the schema.
func resolveDatabaseName(schema *Schema, fromSQL, defaultDatabase string) (string, error) {
	if fromSQL != "" {
		return fromSQL, nil
	}
	if defaultDatabase != "" {
		return defaultDatabase, nil
	}
	if len(schema.Databases) == 1 {
		return schema.Databases[0].Name, nil
	}
	return "", fmt.Errorf("object name is unqualified; pass -database or qualify the name as db.object")
}

// --- collection lookup / mutation helpers ---

func findDatabase(schema *Schema, name string) *DatabaseSpec {
	for i := range schema.Databases {
		if schema.Databases[i].Name == name {
			return &schema.Databases[i]
		}
	}
	return nil
}

func findOrCreateDatabase(schema *Schema, name string) *DatabaseSpec {
	if db := findDatabase(schema, name); db != nil {
		return db
	}
	schema.Databases = append(schema.Databases, DatabaseSpec{Name: name})
	return &schema.Databases[len(schema.Databases)-1]
}

func findTable(db *DatabaseSpec, name string) *TableSpec {
	for i := range db.Tables {
		if db.Tables[i].Name == name {
			return &db.Tables[i]
		}
	}
	return nil
}

func findMaterializedView(db *DatabaseSpec, name string) *MaterializedViewSpec {
	for i := range db.MaterializedViews {
		if db.MaterializedViews[i].Name == name {
			return &db.MaterializedViews[i]
		}
	}
	return nil
}

func findView(db *DatabaseSpec, name string) *ViewSpec {
	for i := range db.Views {
		if db.Views[i].Name == name {
			return &db.Views[i]
		}
	}
	return nil
}

func findDictionary(db *DatabaseSpec, name string) *DictionarySpec {
	for i := range db.Dictionaries {
		if db.Dictionaries[i].Name == name {
			return &db.Dictionaries[i]
		}
	}
	return nil
}

func removeTable(db *DatabaseSpec, name string) bool {
	for i := range db.Tables {
		if db.Tables[i].Name == name {
			db.Tables = append(db.Tables[:i], db.Tables[i+1:]...)
			return true
		}
	}
	return false
}

func removeMaterializedView(db *DatabaseSpec, name string) bool {
	for i := range db.MaterializedViews {
		if db.MaterializedViews[i].Name == name {
			db.MaterializedViews = append(db.MaterializedViews[:i], db.MaterializedViews[i+1:]...)
			return true
		}
	}
	return false
}

func removeView(db *DatabaseSpec, name string) bool {
	for i := range db.Views {
		if db.Views[i].Name == name {
			db.Views = append(db.Views[:i], db.Views[i+1:]...)
			return true
		}
	}
	return false
}

func removeDictionary(db *DatabaseSpec, name string) bool {
	for i := range db.Dictionaries {
		if db.Dictionaries[i].Name == name {
			db.Dictionaries = append(db.Dictionaries[:i], db.Dictionaries[i+1:]...)
			return true
		}
	}
	return false
}

func removeRaw(db *DatabaseSpec, name string) bool {
	for i := range db.Raws {
		if db.Raws[i].Name == name {
			db.Raws = append(db.Raws[:i], db.Raws[i+1:]...)
			return true
		}
	}
	return false
}

func upsertRaw(db *DatabaseSpec, r RawSpec) {
	for i := range db.Raws {
		if db.Raws[i].Name == r.Name {
			db.Raws[i] = r
			return
		}
	}
	db.Raws = append(db.Raws, r)
}

func findColumn(t *TableSpec, name string) int {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return i
		}
	}
	return -1
}

func findIndex(t *TableSpec, name string) int {
	for i := range t.Indexes {
		if t.Indexes[i].Name == name {
			return i
		}
	}
	return -1
}

func findProjection(t *TableSpec, name string) int {
	for i := range t.Projections {
		if t.Projections[i].Name == name {
			return i
		}
	}
	return -1
}

// insertColumnAfter inserts col after the column named after; when after is
// empty or not found, col is appended.
func insertColumnAfter(t *TableSpec, col ColumnSpec, after string) {
	if after != "" {
		if idx := findColumn(t, after); idx >= 0 {
			t.Columns = append(t.Columns[:idx+1], append([]ColumnSpec{col}, t.Columns[idx+1:]...)...)
			return
		}
	}
	t.Columns = append(t.Columns, col)
}
