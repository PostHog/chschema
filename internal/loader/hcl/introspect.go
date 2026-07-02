package hcl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	chparser "github.com/orian/clickhouse-sql-parser/parser"
)

// rowScanner is the minimal subset of driver.Rows used by the introspection
// loop. Extracted so that processIntrospectRows can be tested without a live
// ClickHouse connection.
type rowScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

// Introspect builds a DatabaseSpec from a live ClickHouse database.
// The returned spec is "flat" — no extend/abstract/patch_table — and every
// TableSpec has its Engine.Decoded populated. Tables and column order match
// the declaration order ClickHouse reports.
//
// The function reads `create_table_query` for each table and parses it with
// a ClickHouse SQL parser (orian/clickhouse-sql-parser). All
// per-table data is derived from that AST: columns (including TTL, CODEC,
// default/materialized/ephemeral/alias, comment), indexes, constraints,
// engine, ORDER BY, PARTITION BY, SAMPLE BY, table TTL, SETTINGS, and
// table comment.
func Introspect(ctx context.Context, conn driver.Conn, database string, allowRaw bool) (*DatabaseSpec, error) {
	return IntrospectWithExclude(ctx, conn, database, allowRaw, nil)
}

// IntrospectWithExclude is Introspect with an optional exclude matcher: objects
// whose name matches a pattern are skipped before their DDL is parsed, so
// transient tables (e.g. ClickHouse's _tmp_replace_*, migration tmp_* tables)
// neither appear in the dump nor abort introspection when their DDL can't be
// parsed. A nil matcher excludes nothing.
func IntrospectWithExclude(ctx context.Context, conn driver.Conn, database string, allowRaw bool, exclude *ExcludeMatcher) (*DatabaseSpec, error) {
	db := &DatabaseSpec{Name: database}

	const q = `SELECT name, create_table_query, engine
		FROM system.tables
		WHERE database = ? AND NOT is_temporary
		ORDER BY name`
	rows, err := conn.Query(ctx, q, database)
	if err != nil {
		return nil, fmt.Errorf("query system.tables: %w", err)
	}
	defer rows.Close()

	if err := processIntrospectRowsOpt(db, database, rows, allowRaw, exclude); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return db, nil
}

// rawKindForEngine maps a system.tables.engine value to a RawSpec kind. The
// engine column is populated even when create_table_query cannot be parsed, so
// it is the reliable source for the kind of a captured raw object.
func rawKindForEngine(engine string) string {
	switch engine {
	case "Dictionary":
		return "dictionary"
	case "View":
		return "view"
	case "MaterializedView":
		return "materialized_view"
	default:
		return "table"
	}
}

// processIntrospectRows fills db with tables and materialized views parsed
// from rows produced by a system.tables query. Each row must yield (name,
// create_table_query) via Scan. Plain views (CREATE VIEW) are silently
// skipped; inner-engine and refreshable MVs return an error.
func processIntrospectRows(db *DatabaseSpec, database string, rows rowScanner) error {
	return processIntrospectRowsOpt(db, database, rows, false, nil)
}

// processIntrospectRowsOpt fills db from rows. When allowRaw is false (the
// strict default) any object whose CREATE DDL cannot be parsed or expressed in
// the schema language aborts with an error that names the -allow-raw flag.
// When allowRaw is true, such an object is instead captured verbatim as a
// RawSpec (its kind taken from system.tables.engine) and introspection
// continues, so one unparseable object never breaks the whole dump.
func processIntrospectRowsOpt(db *DatabaseSpec, database string, rows rowScanner, allowRaw bool, exclude *ExcludeMatcher) error {
	for rows.Next() {
		var name, createSQL, engine string
		if err := rows.Scan(&name, &createSQL, &engine); err != nil {
			return fmt.Errorf("scan system.tables: %w", err)
		}
		if pattern, ok := exclude.Match(database, name); ok {
			// Skip before parsing: transient objects (tmp_*, _tmp_replace_*, …)
			// shouldn't land in the dump, and their DDL often can't be parsed.
			slog.Info("skipping excluded object", "object", database+"."+name, "pattern", pattern)
			continue
		}
		if err := introspectOneObject(db, database, name, createSQL); err != nil {
			if !allowRaw {
				return fmt.Errorf("%w (re-run with -allow-raw to capture this object as a raw SQL block instead of failing)", err)
			}
			kind := rawKindForEngine(engine)
			db.Raws = append(db.Raws, RawSpec{Kind: kind, Name: name, SQL: normalizeRawSQL(createSQL)})
			slog.Warn("captured object as raw SQL", "object", database+"."+name, "kind", kind, "reason", err)
		}
	}
	return nil
}

// introspectOneObject parses one create_table_query and appends the resulting
// typed spec to db. It returns an error when the DDL cannot be parsed or the
// object uses an engine/form the schema language does not express; callers
// decide whether that is fatal (strict) or captured as raw.
func introspectOneObject(db *DatabaseSpec, database, name, createSQL string) error {
	stmt, err := parseCreateStatement(createSQL)
	if err != nil {
		return fmt.Errorf("parse create_table_query for %s.%s: %w", database, name, err)
	}
	if err := upsertObjectFromStmt(db, name, stmt); err != nil {
		return fmt.Errorf("introspect %s.%s: %w", database, name, err)
	}
	return nil
}

// upsertObjectFromStmt builds the typed spec for an already-parsed CREATE
// statement and stores it on db under name. When an object of the same kind
// already exists with that name it is replaced in place (otherwise appended),
// so the helper is safe both for introspection — where names are unique — and
// for the sql2hcl edit path, where a CREATE redefines an existing object.
func upsertObjectFromStmt(db *DatabaseSpec, name string, stmt chparser.Expr) error {
	switch s := stmt.(type) {
	case *chparser.CreateTable:
		ts, err := buildTableFromCreateTable(s)
		if err != nil {
			return fmt.Errorf("table %s: %w", name, err)
		}
		ts.Name = name
		upsertTable(db, ts)
	case *chparser.CreateMaterializedView:
		mv, err := buildMaterializedViewFromCreateMV(s)
		if err != nil {
			return fmt.Errorf("materialized view %s: %w", name, err)
		}
		mv.Name = name
		upsertMaterializedView(db, mv)
	case *chparser.CreateDictionary:
		d, err := buildDictionaryFromCreateDictionary(s)
		if err != nil {
			return fmt.Errorf("dictionary %s: %w", name, err)
		}
		d.Name = name
		upsertDictionary(db, d)
	case *chparser.CreateView:
		v, err := buildViewFromCreateView(s)
		if err != nil {
			return fmt.Errorf("view %s: %w", name, err)
		}
		v.Name = name
		upsertView(db, v)
	default:
		return fmt.Errorf("unsupported statement type %T", stmt)
	}
	return nil
}

// upsertTable replaces the table named t.Name in db, or appends it when absent.
func upsertTable(db *DatabaseSpec, t TableSpec) {
	for i := range db.Tables {
		if db.Tables[i].Name == t.Name {
			db.Tables[i] = t
			return
		}
	}
	db.Tables = append(db.Tables, t)
}

func upsertMaterializedView(db *DatabaseSpec, mv MaterializedViewSpec) {
	for i := range db.MaterializedViews {
		if db.MaterializedViews[i].Name == mv.Name {
			db.MaterializedViews[i] = mv
			return
		}
	}
	db.MaterializedViews = append(db.MaterializedViews, mv)
}

func upsertDictionary(db *DatabaseSpec, d DictionarySpec) {
	for i := range db.Dictionaries {
		if db.Dictionaries[i].Name == d.Name {
			db.Dictionaries[i] = d
			return
		}
	}
	db.Dictionaries = append(db.Dictionaries, d)
}

func upsertView(db *DatabaseSpec, v ViewSpec) {
	for i := range db.Views {
		if db.Views[i].Name == v.Name {
			db.Views[i] = v
			return
		}
	}
	db.Views = append(db.Views, v)
}

// parseCreateStatement parses a single DDL statement (the value of
// system.tables.create_table_query) and returns the first statement node.
func parseCreateStatement(createSQL string) (chparser.Expr, error) {
	p := chparser.NewParser(createSQL)
	stmts, err := p.ParseStmts()
	if err != nil {
		return nil, fmt.Errorf("parser: %w", err)
	}
	if len(stmts) == 0 {
		return nil, errors.New("no statement found in create_table_query")
	}
	return stmts[0], nil
}

// buildTableFromCreateSQL turns a CREATE TABLE statement into a TableSpec by
// parsing it with the ClickHouse SQL parser and walking the AST.
func buildTableFromCreateSQL(createSQL string) (TableSpec, error) {
	stmt, err := parseCreateStatement(createSQL)
	if err != nil {
		return TableSpec{}, err
	}
	ct, ok := stmt.(*chparser.CreateTable)
	if !ok {
		return TableSpec{}, errors.New("no CREATE TABLE statement found in create_table_query")
	}
	return buildTableFromCreateTable(ct)
}

// buildTableFromCreateTable walks an already-parsed CREATE TABLE AST.
func buildTableFromCreateTable(ct *chparser.CreateTable) (TableSpec, error) {
	t := TableSpec{}

	if ct.TableSchema != nil {
		for _, col := range ct.TableSchema.Columns {
			switch n := col.(type) {
			case *chparser.ColumnDef:
				t.Columns = append(t.Columns, columnFromAST(n))
			case *chparser.TableIndex:
				idx, err := indexFromAST(n)
				if err != nil {
					return TableSpec{}, err
				}
				t.Indexes = append(t.Indexes, idx)
			case *chparser.ConstraintClause:
				if cs := constraintFromAST(n); cs != nil {
					t.Constraints = append(t.Constraints, *cs)
				}
			}
		}
	}

	if ct.Engine != nil {
		eng, settings, err := engineFromAST(ct.Engine)
		if err != nil {
			return TableSpec{}, err
		}
		// TimeSeries target clauses live on CreateTable, not EngineExpr —
		// fold them in here before stamping the engine onto the table.
		if ts, ok := eng.(EngineTimeSeries); ok && len(ct.TimeSeriesTargets) > 0 {
			for _, tc := range ct.TimeSeriesTargets {
				target, err := buildTimeSeriesTarget(tc)
				if err != nil {
					return TableSpec{}, fmt.Errorf("%s target: %w", tc.Kind, err)
				}
				switch tc.Kind {
				case "samples":
					ts.Samples = target
					ts.KeywordHint = tc.Keyword // SAMPLES or DATA
				case "tags":
					ts.Tags = target
				case "metrics":
					ts.Metrics = target
				default:
					return TableSpec{}, fmt.Errorf("unknown TimeSeries target kind: %q", tc.Kind)
				}
			}
			eng = ts
		}
		t.Engine = &EngineSpec{Kind: eng.Kind(), Decoded: eng}

		if ct.Engine.OrderBy != nil {
			for _, it := range ct.Engine.OrderBy.Items {
				t.OrderBy = append(t.OrderBy, flattenTupleExpr(it)...)
			}
		}
		if ct.Engine.PartitionBy != nil {
			t.PartitionBy = strPtr(formatNode(ct.Engine.PartitionBy.Expr))
		}
		if ct.Engine.SampleBy != nil {
			t.SampleBy = strPtr(formatNode(ct.Engine.SampleBy.Expr))
		}
		if ct.Engine.PrimaryKey != nil {
			if pk := exprList(ct.Engine.PrimaryKey.Expr); !stringSliceEqual(pk, t.OrderBy) {
				t.PrimaryKey = pk
			}
		}
		if ct.Engine.TTL != nil && len(ct.Engine.TTL.Items) > 0 {
			t.TTL = strPtr(formatNode(ct.Engine.TTL.Items[0].Expr))
		}
		if len(settings) > 0 {
			t.Settings = settings
		}
	}

	if ct.OnCluster != nil && ct.OnCluster.Expr != nil {
		t.Cluster = strPtr(formatNode(ct.OnCluster.Expr))
	}
	if ct.Comment != nil {
		t.Comment = strPtr(unquoteString(ct.Comment.Literal))
	}

	return t, nil
}

// buildMaterializedViewFromCreateSQL turns a CREATE MATERIALIZED VIEW
// statement into a MaterializedViewSpec by parsing it and walking the AST.
func buildMaterializedViewFromCreateSQL(createSQL string) (MaterializedViewSpec, error) {
	stmt, err := parseCreateStatement(createSQL)
	if err != nil {
		return MaterializedViewSpec{}, err
	}
	mv, ok := stmt.(*chparser.CreateMaterializedView)
	if !ok {
		return MaterializedViewSpec{}, errors.New("no CREATE MATERIALIZED VIEW statement found")
	}
	return buildMaterializedViewFromCreateMV(mv)
}

// buildMaterializedViewFromCreateMV walks an already-parsed CREATE
// MATERIALIZED VIEW AST. Only the `TO <table>` form is supported;
// inner-engine and refreshable MVs are rejected with a clear error.
// buildViewFromCreateView walks a parsed CREATE VIEW AST and produces a
// ViewSpec. The SELECT body is rendered back to text via formatNode,
// matching the way MV bodies are captured.
func buildViewFromCreateView(cv *chparser.CreateView) (ViewSpec, error) {
	v := ViewSpec{}

	if cv.SubQuery != nil && cv.SubQuery.Select != nil {
		v.Query = beautifyNode(cv.SubQuery.Select)
	}

	if cv.OnCluster != nil && cv.OnCluster.Expr != nil {
		v.Cluster = strPtr(formatNode(cv.OnCluster.Expr))
	}

	// chparser exposes CREATE VIEW v (a, b, c) AS … via TableSchema with
	// ColumnDef entries that carry only a Name (a typeless "bare alias").
	//
	// ClickHouse, however, ALWAYS stores a fully *typed* column schema for a
	// view in create_table_query (e.g. `(team_id Int64, event String)`). That
	// is inferred metadata, not an author-specified column list: re-emitting it
	// produces a different, malformed CREATE VIEW (issue #48). So only capture
	// typeless entries — the genuine `CREATE VIEW v (a, b)` author list — and
	// skip the typed schema, letting the SELECT body define the columns.
	if cv.TableSchema != nil {
		for _, c := range cv.TableSchema.Columns {
			cd, ok := c.(*chparser.ColumnDef)
			if !ok || cd.Name == nil {
				continue
			}
			if cd.Type != nil {
				continue // ClickHouse's inferred column schema, not an alias list
			}
			v.ColumnAliases = append(v.ColumnAliases, identName(cd.Name))
		}
	}

	if cv.Comment != nil {
		v.Comment = strPtr(unquoteString(cv.Comment.Literal))
	}
	if cv.Definer != nil {
		d := stripBackticks(cv.Definer.Name)
		v.Definer = &d
	}
	if cv.SQLSecurity != "" {
		s := strings.ToLower(cv.SQLSecurity)
		v.SQLSecurity = &s
	}

	return v, nil
}

func buildMaterializedViewFromCreateMV(mv *chparser.CreateMaterializedView) (MaterializedViewSpec, error) {
	if mv.Refresh != nil {
		return MaterializedViewSpec{}, errors.New("unsupported: refreshable materialized view")
	}
	if mv.Engine != nil {
		return MaterializedViewSpec{}, errors.New("unsupported: inner-engine materialized view (only the TO <table> form is supported)")
	}
	if mv.Destination == nil || mv.Destination.TableIdentifier == nil {
		return MaterializedViewSpec{}, errors.New("unsupported: materialized view has no TO clause")
	}

	out := MaterializedViewSpec{
		ToTable: tableIdentName(mv.Destination.TableIdentifier),
	}

	if mv.Destination.TableSchema != nil {
		for _, col := range mv.Destination.TableSchema.Columns {
			if cd, ok := col.(*chparser.ColumnDef); ok {
				// MV destination columns are introspected as name+type only,
				// matching what the HCL dumper emits (writeMaterializedView
				// only writes Name and Type). Using columnFromAST here would
				// populate Default/Codec/Comment/TTL fields that the dumper
				// silently drops, causing spurious Recreate diffs on live-vs-dumped
				// comparisons. TO-form MV column lists are bare name+type in practice.
				out.Columns = append(out.Columns, ColumnSpec{
					Name: identName(cd.Name),
					Type: formatNode(cd.Type),
				})
			}
		}
	}

	if mv.SubQuery != nil && mv.SubQuery.Select != nil {
		out.Query = beautifyNode(mv.SubQuery.Select)
	}

	if mv.OnCluster != nil && mv.OnCluster.Expr != nil {
		out.Cluster = strPtr(formatNode(mv.OnCluster.Expr))
	}
	if mv.Comment != nil {
		out.Comment = strPtr(unquoteString(mv.Comment.Literal))
	}

	return out, nil
}

// tableIdentName renders a TableIdentifier as `db.table` (or `table`),
// stripping ClickHouse's backtick quoting so the value matches how it would
// be written in HCL.
func tableIdentName(t *chparser.TableIdentifier) string {
	if t == nil {
		return ""
	}
	return strings.ReplaceAll(formatNode(t), "`", "")
}

// formatNode renders an AST node back to compact SQL text. The fork's
// refactor-visitor branch replaced the old chparser.Format helper with a
// visitor-based API, so node rendering now goes through PrintVisitor.
func formatNode(n chparser.Expr) string {
	if n == nil {
		return ""
	}
	v := chparser.NewPrintVisitor()
	if err := n.Accept(v); err != nil {
		return ""
	}
	return v.String()
}

func columnFromAST(c *chparser.ColumnDef) ColumnSpec {
	out := ColumnSpec{Name: identName(c.Name), Type: formatNode(c.Type)}
	if c.DefaultExpr != nil {
		s := formatNode(c.DefaultExpr)
		out.Default = &s
	}
	if c.MaterializedExpr != nil {
		s := formatNode(c.MaterializedExpr)
		out.Materialized = &s
	}
	if c.AliasExpr != nil {
		s := formatNode(c.AliasExpr)
		out.Alias = &s
	}
	if c.Codec != nil {
		out.Codec = parseCompressionCodec(c.Codec)
	}
	if c.TTL != nil && len(c.TTL.Items) > 0 {
		s := formatNode(c.TTL.Items[0].Expr)
		out.TTL = &s
	}
	if c.Comment != nil {
		s := unquoteString(c.Comment.Literal)
		out.Comment = &s
	}
	return out
}

func indexFromAST(i *chparser.TableIndex) (IndexSpec, error) {
	out := IndexSpec{Name: identName(i.Name)}
	if i.ColumnExpr != nil {
		out.Expr = formatNode(i.ColumnExpr.Expr)
	}
	if i.ColumnType != nil {
		out.Type = formatNode(i.ColumnType)
	}
	if i.Granularity != nil {
		// Granularity is a NumberLiteral; convert via Format then parse.
		s := formatNode(i.Granularity)
		n, err := strconv.Atoi(s)
		if err != nil {
			return IndexSpec{}, fmt.Errorf("index %q: invalid GRANULARITY %q: %w", out.Name, s, err)
		}
		out.Granularity = n
	}
	return out, nil
}

func constraintFromAST(c *chparser.ConstraintClause) *ConstraintSpec {
	if c.Constraint == nil || c.Expr == nil {
		return nil
	}
	expr := formatNode(c.Expr)
	// The parser only accepts (and reaches here for) CHECK constraints: its
	// CREATE TABLE constraint branch hard-requires the CHECK keyword, so an
	// ASSUME constraint is a parse error upstream and never reaches this
	// function. Every constraint we see is therefore a CHECK. Once the parser
	// accepts ASSUME and records the kind, read it here and drop this note.
	// Tracked: github.com/orian/clickhouse-sql-parser#17 (this repo's #83).
	return &ConstraintSpec{Name: c.Constraint.Name, Check: &expr}
}

// engineFromAST returns (engine, table-level settings) from an EngineExpr.
// The settings map excludes kafka_* keys (those are folded into the typed
// Kafka engine), matching the legacy extractTableSettings behavior.
func engineFromAST(e *chparser.EngineExpr) (Engine, map[string]string, error) {
	params := engineParamStrings(e.Params)
	allSettings := engineSettingsMap(e.Settings)

	switch e.Name {
	case "MergeTree":
		return EngineMergeTree{}, allSettings, nil
	case "ReplicatedMergeTree":
		if len(params) < 2 {
			return nil, nil, fmt.Errorf("ReplicatedMergeTree needs (zoo_path, replica_name)")
		}
		return EngineReplicatedMergeTree{ZooPath: params[0], ReplicaName: params[1]}, allSettings, nil
	case "ReplacingMergeTree":
		e := EngineReplacingMergeTree{}
		if len(params) > 0 && params[0] != "" {
			e.VersionColumn = &params[0]
		}
		return e, allSettings, nil
	case "ReplicatedReplacingMergeTree":
		if len(params) < 2 {
			return nil, nil, fmt.Errorf("ReplicatedReplacingMergeTree needs (zoo_path, replica_name[, version_column])")
		}
		ee := EngineReplicatedReplacingMergeTree{ZooPath: params[0], ReplicaName: params[1]}
		if len(params) > 2 {
			ee.VersionColumn = &params[2]
		}
		return ee, allSettings, nil
	case "SummingMergeTree":
		return EngineSummingMergeTree{SumColumns: params}, allSettings, nil
	case "ReplicatedSummingMergeTree":
		if len(params) < 2 {
			return nil, nil, fmt.Errorf("ReplicatedSummingMergeTree needs at least (zoo_path, replica_name[, sum_columns...])")
		}
		ee := EngineReplicatedSummingMergeTree{ZooPath: params[0], ReplicaName: params[1]}
		if len(params) > 2 {
			ee.SumColumns = params[2:]
		}
		return ee, allSettings, nil
	case "CollapsingMergeTree":
		if len(params) != 1 {
			return nil, nil, fmt.Errorf("CollapsingMergeTree needs (sign_column)")
		}
		return EngineCollapsingMergeTree{SignColumn: params[0]}, allSettings, nil
	case "ReplicatedCollapsingMergeTree":
		if len(params) != 3 {
			return nil, nil, fmt.Errorf("ReplicatedCollapsingMergeTree needs (zoo_path, replica_name, sign_column)")
		}
		return EngineReplicatedCollapsingMergeTree{ZooPath: params[0], ReplicaName: params[1], SignColumn: params[2]}, allSettings, nil
	case "AggregatingMergeTree":
		return EngineAggregatingMergeTree{}, allSettings, nil
	case "ReplicatedAggregatingMergeTree":
		if len(params) < 2 {
			return nil, nil, fmt.Errorf("ReplicatedAggregatingMergeTree needs (zoo_path, replica_name)")
		}
		return EngineReplicatedAggregatingMergeTree{ZooPath: params[0], ReplicaName: params[1]}, allSettings, nil
	case "Distributed":
		if len(params) < 3 {
			return nil, nil, fmt.Errorf("engine Distributed needs (cluster, db, table[, sharding_key])")
		}
		ee := EngineDistributed{ClusterName: params[0], RemoteDatabase: params[1], RemoteTable: params[2]}
		if len(params) > 3 {
			ee.ShardingKey = &params[3]
		}
		return ee, allSettings, nil
	case "Log":
		return EngineLog{}, allSettings, nil
	case "Join":
		if len(params) < 3 {
			return nil, nil, fmt.Errorf("engine Join needs (strictness, type, key[, key...])")
		}
		return EngineJoin{
			Strictness: params[0],
			JoinType:   params[1],
			Keys:       params[2:],
		}, allSettings, nil
	case "Null":
		return EngineNull{}, allSettings, nil
	case "Memory":
		return EngineMemory{}, allSettings, nil
	case "Merge":
		if len(params) != 2 {
			return nil, nil, fmt.Errorf("engine Merge needs (db_regex, table_regex)")
		}
		return EngineMerge{DBRegex: params[0], TableRegex: params[1]}, allSettings, nil
	case "Buffer":
		if len(params) < 9 || len(params) > 12 {
			return nil, nil, fmt.Errorf("engine Buffer needs 9-12 args (db, table, num_layers, min/max time/rows/bytes [, flush_time [, flush_rows [, flush_bytes]]])")
		}
		toInt := func(s string) (int64, error) {
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("engine Buffer arg %q: %w", s, err)
			}
			return n, nil
		}
		nums := make([]int64, 0, len(params)-2)
		for _, p := range params[2:] {
			n, err := toInt(p)
			if err != nil {
				return nil, nil, err
			}
			nums = append(nums, n)
		}
		e := EngineBuffer{
			Database:  params[0],
			Table:     params[1],
			NumLayers: nums[0],
			MinTime:   nums[1],
			MaxTime:   nums[2],
			MinRows:   nums[3],
			MaxRows:   nums[4],
			MinBytes:  nums[5],
			MaxBytes:  nums[6],
		}
		if len(nums) > 7 {
			ft := nums[7]
			e.FlushTime = &ft
		}
		if len(nums) > 8 {
			fr := nums[8]
			e.FlushRows = &fr
		}
		if len(nums) > 9 {
			fb := nums[9]
			e.FlushBytes = &fb
		}
		return e, allSettings, nil
	case "Kafka":
		k, err := buildKafkaEngine(params, allSettings)
		if err != nil {
			return nil, nil, err
		}
		// kafka_* settings are engine args, not table settings.
		stripped := make(map[string]string, len(allSettings))
		for kk, vv := range allSettings {
			if !strings.HasPrefix(kk, "kafka_") {
				stripped[kk] = vv
			}
		}
		if len(stripped) == 0 {
			stripped = nil
		}
		return k, stripped, nil
	case "TimeSeries":
		// All SETTINGS on a TimeSeries engine belong to the engine itself,
		// not to the table — promote tags_to_columns to a typed field,
		// keep the rest in Settings.
		ts := EngineTimeSeries{}
		for k, v := range allSettings {
			if k == "tags_to_columns" {
				m, err := parseTagsToColumnsMap(v)
				if err != nil {
					return nil, nil, fmt.Errorf("tags_to_columns: %w", err)
				}
				ts.TagsToColumns = m
				continue
			}
			if ts.Settings == nil {
				ts.Settings = map[string]string{}
			}
			ts.Settings[k] = v
		}
		return ts, nil, nil
	}
	return nil, nil, fmt.Errorf("unsupported engine: %s", e.Name)
}

func engineParamStrings(p *chparser.ParamExprList) []string {
	if p == nil || p.Items == nil {
		return nil
	}
	out := make([]string, 0, len(p.Items.Items))
	for _, it := range p.Items.Items {
		out = append(out, unquoteString(formatNode(it)))
	}
	return out
}

func engineSettingsMap(s *chparser.SettingsClause) map[string]string {
	if s == nil {
		return nil
	}
	out := make(map[string]string, len(s.Items))
	for _, it := range s.Items {
		out[it.Name.Name] = unquoteString(formatNode(it.Expr))
	}
	return out
}

// flattenTupleExpr returns the comma-separated members of a tuple expression
// like `(a, b, c)`, or the formatted expression itself if it isn't a tuple.
// Used to normalize `ORDER BY (a, b)` (one tuple item) and `ORDER BY a, b`
// (two items) into the same `[a, b]` representation.
func flattenTupleExpr(e chparser.Expr) []string {
	s := strings.TrimSpace(formatNode(e))
	if !strings.HasPrefix(s, "(") || !strings.HasSuffix(s, ")") {
		return []string{s}
	}
	inner := s[1 : len(s)-1]
	parts := splitTopLevelCSV(inner)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, strings.TrimSpace(p))
	}
	return out
}

func exprList(e chparser.Expr) []string {
	if e == nil {
		return nil
	}
	// A multi-key clause arrives as a tuple expression; single-key as a
	// bare identifier. Format() handles both consistently; we then split.
	s := formatNode(e)
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "(")
	s = strings.TrimSuffix(s, ")")
	if s == "" {
		return nil
	}
	parts := splitTopLevelCSV(s)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, strings.TrimSpace(p))
	}
	return out
}

// splitTopLevelCSV splits on commas that aren't inside parens or quotes.
// Used to parse ORDER BY / PRIMARY KEY column lists when the surrounding
// tuple parens have already been stripped.
func splitTopLevelCSV(s string) []string {
	var out []string
	var b strings.Builder
	depth := 0
	inQuote := false
	var quote rune
	for _, r := range s {
		switch {
		case (r == '\'' || r == '"' || r == '`') && !inQuote:
			inQuote = true
			quote = r
			b.WriteRune(r)
		case r == quote && inQuote:
			inQuote = false
			b.WriteRune(r)
		case !inQuote && r == '(':
			depth++
			b.WriteRune(r)
		case !inQuote && r == ')':
			depth--
			b.WriteRune(r)
		case !inQuote && depth == 0 && r == ',':
			out = append(out, b.String())
			b.Reset()
		default:
			b.WriteRune(r)
		}
	}
	if b.Len() > 0 {
		out = append(out, b.String())
	}
	return out
}

func identName(n *chparser.NestedIdentifier) string {
	if n == nil {
		return ""
	}
	s := formatNode(n)
	// Strip ClickHouse's backtick quoting around bare identifiers so the
	// stored column/index names match how a user would write them in HCL.
	if len(s) >= 2 && s[0] == '`' && s[len(s)-1] == '`' {
		return s[1 : len(s)-1]
	}
	return s
}

func parseCompressionCodec(c *chparser.CompressionCodec) *string {
	if c == nil {
		return nil
	}
	s := formatNode(c)
	if strings.HasPrefix(s, "CODEC(") && strings.HasSuffix(s, ")") {
		inner := s[len("CODEC(") : len(s)-1]
		return &inner
	}
	return &s
}

// unquoteString strips matching surrounding single or double quotes and
// unescapes embedded ones. Used because parser.Format() returns quoted
// SQL literals while we store the underlying string.
func unquoteString(s string) string {
	if len(s) >= 2 {
		first := s[0]
		last := s[len(s)-1]
		if (first == '\'' || first == '"' || first == '`') && first == last {
			inner := s[1 : len(s)-1]
			inner = strings.ReplaceAll(inner, "\\'", "'")
			inner = strings.ReplaceAll(inner, "\\\"", "\"")
			return inner
		}
	}
	return s
}

func strPtr(s string) *string {
	return &s
}

// --- Legacy engine_full string parser (kept for the unit tests) ---

// ParseEngineString parses a ClickHouse engine_full value into a typed
// HCL Engine. Strips the trailing ORDER BY / PARTITION BY / SAMPLE BY / TTL
// / SETTINGS / COMMENT clauses, then dispatches on the engine name.
func ParseEngineString(engineFull string) (Engine, error) {
	decl := extractEngineDeclaration(engineFull)
	switch {
	case strings.HasPrefix(decl, "ReplicatedMergeTree"):
		p, err := extractEngineParams(decl)
		if err != nil {
			return nil, err
		}
		if len(p) < 2 {
			return nil, fmt.Errorf("ReplicatedMergeTree needs (zoo_path, replica_name); got %v", p)
		}
		return EngineReplicatedMergeTree{ZooPath: p[0], ReplicaName: p[1]}, nil
	case strings.HasPrefix(decl, "ReplicatedReplacingMergeTree"):
		p, err := extractEngineParams(decl)
		if err != nil {
			return nil, err
		}
		if len(p) < 2 {
			return nil, fmt.Errorf("ReplicatedReplacingMergeTree needs (zoo_path, replica_name[, version_column]); got %v", p)
		}
		e := EngineReplicatedReplacingMergeTree{ZooPath: p[0], ReplicaName: p[1]}
		if len(p) > 2 {
			e.VersionColumn = &p[2]
		}
		return e, nil
	case strings.HasPrefix(decl, "ReplicatedCollapsingMergeTree"):
		p, err := extractEngineParams(decl)
		if err != nil {
			return nil, err
		}
		if len(p) != 3 {
			return nil, fmt.Errorf("ReplicatedCollapsingMergeTree needs (zoo_path, replica_name, sign_column); got %v", p)
		}
		return EngineReplicatedCollapsingMergeTree{ZooPath: p[0], ReplicaName: p[1], SignColumn: p[2]}, nil
	case strings.HasPrefix(decl, "ReplicatedAggregatingMergeTree"):
		p, err := extractEngineParams(decl)
		if err != nil {
			return nil, err
		}
		if len(p) < 2 {
			return nil, fmt.Errorf("ReplicatedAggregatingMergeTree needs (zoo_path, replica_name); got %v", p)
		}
		return EngineReplicatedAggregatingMergeTree{ZooPath: p[0], ReplicaName: p[1]}, nil
	case strings.HasPrefix(decl, "ReplicatedSummingMergeTree"):
		p, err := extractEngineParams(decl)
		if err != nil {
			return nil, err
		}
		if len(p) < 2 {
			return nil, fmt.Errorf("ReplicatedSummingMergeTree needs at least (zoo_path, replica_name[, sum_columns...]); got %v", p)
		}
		e := EngineReplicatedSummingMergeTree{ZooPath: p[0], ReplicaName: p[1]}
		if len(p) > 2 {
			e.SumColumns = p[2:]
		}
		return e, nil
	case strings.HasPrefix(decl, "ReplacingMergeTree"):
		p, err := extractEngineParams(decl)
		if err != nil {
			return nil, err
		}
		e := EngineReplacingMergeTree{}
		if len(p) > 0 && p[0] != "" {
			e.VersionColumn = &p[0]
		}
		return e, nil
	case strings.HasPrefix(decl, "SummingMergeTree"):
		return parseSummingMergeTreeHCL(decl)
	case strings.HasPrefix(decl, "CollapsingMergeTree"):
		p, err := extractEngineParams(decl)
		if err != nil {
			return nil, err
		}
		if len(p) != 1 {
			return nil, fmt.Errorf("CollapsingMergeTree needs (sign_column); got %v", p)
		}
		return EngineCollapsingMergeTree{SignColumn: p[0]}, nil
	case strings.HasPrefix(decl, "AggregatingMergeTree"):
		return EngineAggregatingMergeTree{}, nil
	case strings.HasPrefix(decl, "MergeTree"):
		return EngineMergeTree{}, nil
	case strings.HasPrefix(decl, "Distributed"):
		p, err := extractEngineParams(decl)
		if err != nil {
			return nil, err
		}
		if len(p) < 3 {
			return nil, fmt.Errorf("engine Distributed needs (cluster, db, table[, sharding_key]); got %v", p)
		}
		e := EngineDistributed{ClusterName: p[0], RemoteDatabase: p[1], RemoteTable: p[2]}
		if len(p) > 3 {
			e.ShardingKey = &p[3]
		}
		return e, nil
	case strings.HasPrefix(decl, "Log"):
		return EngineLog{}, nil
	case strings.HasPrefix(decl, "Kafka"):
		return parseKafkaEngine(engineFull, decl)
	}
	return nil, fmt.Errorf("unsupported engine: %q", decl)
}

func extractEngineDeclaration(engineFull string) string {
	keywords := []string{" ORDER BY", " PARTITION BY", " SAMPLE BY", " TTL", " SETTINGS", " COMMENT", " PRIMARY KEY"}
	end := len(engineFull)
	for _, kw := range keywords {
		if i := strings.Index(engineFull, kw); i != -1 && i < end {
			end = i
		}
	}
	return strings.TrimSpace(engineFull[:end])
}

func extractEngineParams(decl string) ([]string, error) {
	open := strings.Index(decl, "(")
	closeIdx := strings.LastIndex(decl, ")")
	if open == -1 || closeIdx == -1 || open >= closeIdx {
		return nil, nil
	}
	inner := strings.TrimSpace(decl[open+1 : closeIdx])
	if inner == "" {
		return nil, nil
	}
	var parts []string
	var b strings.Builder
	inQuote := false
	var quote rune
	for _, r := range inner {
		switch {
		case (r == '\'' || r == '"') && !inQuote:
			inQuote = true
			quote = r
			b.WriteRune(r)
		case r == quote && inQuote:
			inQuote = false
			quote = 0
			b.WriteRune(r)
		case r == ',' && !inQuote:
			parts = append(parts, strings.TrimSpace(b.String()))
			b.Reset()
		default:
			b.WriteRune(r)
		}
	}
	if b.Len() > 0 {
		parts = append(parts, strings.TrimSpace(b.String()))
	}
	for i, p := range parts {
		parts[i] = strings.Trim(p, `'"`)
	}
	return parts, nil
}

func parseSummingMergeTreeHCL(decl string) (Engine, error) {
	re := regexp.MustCompile(`SummingMergeTree\(\((.*?)\)\)`)
	m := re.FindStringSubmatch(decl)
	e := EngineSummingMergeTree{}
	if len(m) > 1 && strings.TrimSpace(m[1]) != "" {
		for _, c := range strings.Split(m[1], ",") {
			e.SumColumns = append(e.SumColumns, strings.TrimSpace(c))
		}
	}
	return e, nil
}

func parseKafkaEngine(engineFull, decl string) (Engine, error) {
	settings := extractEngineSettings(engineFull)
	if len(settings) > 0 {
		k, err := buildKafkaEngine(nil, settings)
		if err != nil {
			return nil, err
		}
		return k, nil
	}
	p, err := extractEngineParams(decl)
	if err != nil {
		return nil, err
	}
	return buildKafkaEngine(p, nil)
}

// buildKafkaEngine decodes Kafka engine parameters into the typed
// EngineKafka struct across the four supported forms:
//  1. Kafka() + kafka_* settings (canonical inline form)
//  2. Kafka(<collection_name>) with no settings (named collection)
//  3. Kafka('broker', 'topic', 'group', 'format') legacy positional
//  4. Kafka(<collection>) + kafka_* settings → error (mixed form)
func buildKafkaEngine(params []string, allSettings map[string]string) (EngineKafka, error) {
	hasKafkaSettings := false
	for k := range allSettings {
		if strings.HasPrefix(k, "kafka_") {
			hasKafkaSettings = true
			break
		}
	}

	// Form 2 / 4: single arg that looks like an identifier — no colon
	// (broker list always has host:port), no comma (broker list is
	// comma-joined), no slash, no whitespace.
	if len(params) == 1 && !strings.ContainsAny(params[0], ":,/ ") {
		name := params[0]
		if hasKafkaSettings {
			return EngineKafka{}, fmt.Errorf("Kafka(%s) cannot be combined with kafka_* SETTINGS overrides — declare full inline settings instead", name)
		}
		return EngineKafka{Collection: &name}, nil
	}

	// Form 3: legacy positional Kafka('broker', 'topic', 'group', 'format').
	if len(params) == 4 {
		broker := params[0]
		topic := params[1]
		group := params[2]
		format := params[3]
		k := EngineKafka{
			BrokerList: &broker,
			TopicList:  &topic,
			GroupName:  &group,
			Format:     &format,
		}
		for key, val := range allSettings {
			if !strings.HasPrefix(key, "kafka_") {
				continue
			}
			// Skip keys already populated by positional args.
			switch key {
			case "kafka_broker_list", "kafka_topic_list", "kafka_group_name", "kafka_format":
				continue
			}
			applyKafkaSetting(&k, key, val)
		}
		return k, nil
	}

	// Form 1: Kafka() + kafka_* settings.
	if len(params) > 0 {
		return EngineKafka{}, fmt.Errorf("Kafka() unexpected positional args: %v", params)
	}
	k := EngineKafka{}
	for key, val := range allSettings {
		if !strings.HasPrefix(key, "kafka_") {
			continue
		}
		applyKafkaSetting(&k, key, val)
	}
	return k, nil
}

// applyKafkaSetting routes one kafka_* setting into the matching typed
// field. Unknown keys land in Extra with their prefix intact.
func applyKafkaSetting(k *EngineKafka, key, val string) {
	switch key {
	case "kafka_broker_list":
		k.BrokerList = &val
	case "kafka_topic_list":
		k.TopicList = &val
	case "kafka_group_name":
		k.GroupName = &val
	case "kafka_format":
		k.Format = &val
	case "kafka_security_protocol":
		k.SecurityProtocol = &val
	case "kafka_sasl_mechanism":
		k.SaslMechanism = &val
	case "kafka_sasl_username":
		k.SaslUsername = &val
	case "kafka_sasl_password":
		k.SaslPassword = &val
	case "kafka_client_id":
		k.ClientID = &val
	case "kafka_schema":
		k.Schema = &val
	case "kafka_handle_error_mode":
		k.HandleErrorMode = &val
	case "kafka_compression_codec":
		k.CompressionCodec = &val
	case "kafka_num_consumers":
		k.NumConsumers = parseInt64Ptr(val)
	case "kafka_max_block_size":
		k.MaxBlockSize = parseInt64Ptr(val)
	case "kafka_skip_broken_messages":
		k.SkipBrokenMessages = parseInt64Ptr(val)
	case "kafka_poll_timeout_ms":
		k.PollTimeoutMs = parseInt64Ptr(val)
	case "kafka_poll_max_batch_size":
		k.PollMaxBatchSize = parseInt64Ptr(val)
	case "kafka_flush_interval_ms":
		k.FlushIntervalMs = parseInt64Ptr(val)
	case "kafka_consumer_reschedule_ms":
		k.ConsumerRescheduleMs = parseInt64Ptr(val)
	case "kafka_max_rows_per_message":
		k.MaxRowsPerMessage = parseInt64Ptr(val)
	case "kafka_compression_level":
		k.CompressionLevel = parseInt64Ptr(val)
	case "kafka_commit_every_batch":
		k.CommitEveryBatch = parseBoolPtr(val)
	case "kafka_thread_per_consumer":
		k.ThreadPerConsumer = parseBoolPtr(val)
	case "kafka_commit_on_select":
		k.CommitOnSelect = parseBoolPtr(val)
	case "kafka_autodetect_client_rack":
		k.AutodetectClientRack = parseBoolPtr(val)
	default:
		if k.Extra == nil {
			k.Extra = map[string]string{}
		}
		k.Extra[key] = val
	}
}

func parseInt64Ptr(s string) *int64 {
	if s == "" {
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return &n
}

func parseBoolPtr(s string) *bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true":
		v := true
		return &v
	case "0", "false":
		v := false
		return &v
	}
	return nil
}

func extractEngineSettings(engineFull string) map[string]string {
	i := strings.Index(engineFull, " SETTINGS ")
	if i == -1 {
		return nil
	}
	tail := engineFull[i+len(" SETTINGS "):]
	if j := strings.Index(tail, " COMMENT"); j != -1 {
		tail = tail[:j]
	}
	out := map[string]string{}
	for _, part := range splitTopLevelCSV(tail) {
		eq := strings.Index(part, "=")
		if eq == -1 {
			continue
		}
		k := strings.TrimSpace(part[:eq])
		v := strings.TrimSpace(part[eq+1:])
		v = strings.Trim(v, `'"`)
		out[k] = v
	}
	return out
}

// parseCodecExpression turns ClickHouse's "CODEC(LZ4)" wrapping into the
// inner argument list. Returns nil for empty / default codec.
func parseCodecExpression(s string) *string {
	if s == "" {
		return nil
	}
	if strings.HasPrefix(s, "CODEC(") && strings.HasSuffix(s, ")") {
		inner := s[len("CODEC(") : len(s)-1]
		return &inner
	}
	return &s
}

// splitKeyList parses ClickHouse-formatted key lists. Single-column keys
// come back as "id"; multi-column as "id, ts" or "(id, ts)".
func splitKeyList(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	s = strings.TrimPrefix(s, "(")
	s = strings.TrimSuffix(s, ")")
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, strings.TrimSpace(p))
	}
	return out
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// buildTimeSeriesTarget turns a chparser.TimeSeriesTargetClause into a typed
// TimeSeriesTarget. Either External or Inner must be set on the clause;
// the resolver enforces that both-or-neither is rejected.
func buildTimeSeriesTarget(tc *chparser.TimeSeriesTargetClause) (*TimeSeriesTarget, error) {
	if tc.External != nil {
		ref := stripBackticks(tc.External.String())
		return &TimeSeriesTarget{Target: &ref}, nil
	}
	if tc.InnerColumns == nil {
		return nil, errors.New("clause has neither external table nor inner columns")
	}
	cols := columnsFromTableSchema(tc.InnerColumns)
	inner := &TimeSeriesInnerTable{
		Columns: make([]ColumnSpec, len(cols)),
	}
	for i, c := range cols {
		inner.Columns[i] = ColumnSpec{Name: c.Name, Type: c.Type}
	}
	if tc.InnerEngine != nil {
		eng, _, err := engineFromAST(tc.InnerEngine)
		if err != nil {
			return nil, fmt.Errorf("inner engine: %w", err)
		}
		inner.Engine = &EngineSpec{Kind: eng.Kind(), Decoded: eng}
		if tc.InnerEngine.OrderBy != nil {
			for _, it := range tc.InnerEngine.OrderBy.Items {
				inner.OrderBy = append(inner.OrderBy, flattenTupleExpr(it)...)
			}
		}
		if tc.InnerEngine.PrimaryKey != nil {
			if pk := exprList(tc.InnerEngine.PrimaryKey.Expr); len(pk) > 0 && !stringSliceEqual(pk, inner.OrderBy) {
				inner.PrimaryKey = pk
			}
		}
		if tc.InnerEngine.PartitionBy != nil {
			pb := formatNode(tc.InnerEngine.PartitionBy.Expr)
			inner.PartitionBy = &pb
		}
	}
	return &TimeSeriesTarget{Inner: inner}, nil
}

// parseTagsToColumnsMap parses the `{'tag1':'col1','tag2':'col2',...}` map
// literal CH emits in SETTINGS for the TimeSeries tags_to_columns setting.
func parseTagsToColumnsMap(s string) (map[string]string, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil, fmt.Errorf("expected map literal, got %q", s)
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return map[string]string{}, nil
	}
	out := map[string]string{}
	for _, pair := range splitTopLevel(inner, ',') {
		kv := splitTopLevel(strings.TrimSpace(pair), ':')
		if len(kv) != 2 {
			return nil, fmt.Errorf("expected `key:value`, got %q", pair)
		}
		k := unquoteString(strings.TrimSpace(kv[0]))
		v := unquoteString(strings.TrimSpace(kv[1]))
		out[k] = v
	}
	return out, nil
}

// splitTopLevel splits s on sep, respecting single-quoted strings. The
// TimeSeries map literal values are flat strings, so quote awareness is
// enough — no nested brackets or commas inside values.
func splitTopLevel(s string, sep byte) []string {
	var parts []string
	var current strings.Builder
	inSingle := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\'' && (i == 0 || s[i-1] != '\\') {
			inSingle = !inSingle
		}
		if c == sep && !inSingle {
			parts = append(parts, current.String())
			current.Reset()
			continue
		}
		current.WriteByte(c)
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}
