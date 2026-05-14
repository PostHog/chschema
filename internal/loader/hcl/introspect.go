package hcl

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	chparser "github.com/AfterShip/clickhouse-sql-parser/parser"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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
// a ClickHouse SQL parser (AfterShip/clickhouse-sql-parser). All
// per-table data is derived from that AST: columns (including TTL, CODEC,
// default/materialized/ephemeral/alias, comment), indexes, constraints,
// engine, ORDER BY, PARTITION BY, SAMPLE BY, table TTL, SETTINGS, and
// table comment.
func Introspect(ctx context.Context, conn driver.Conn, database string) (*DatabaseSpec, error) {
	db := &DatabaseSpec{Name: database}

	const q = `SELECT name, create_table_query
		FROM system.tables
		WHERE database = ? AND NOT is_temporary
		ORDER BY name`
	rows, err := conn.Query(ctx, q, database)
	if err != nil {
		return nil, fmt.Errorf("query system.tables: %w", err)
	}
	defer rows.Close()

	if err := processIntrospectRows(db, database, rows); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return db, nil
}

// processIntrospectRows fills db with tables and materialized views parsed
// from rows produced by a system.tables query. Each row must yield (name,
// create_table_query) via Scan. Plain views (CREATE VIEW) are silently
// skipped; inner-engine and refreshable MVs return an error.
func processIntrospectRows(db *DatabaseSpec, database string, rows rowScanner) error {
	for rows.Next() {
		var name, createSQL string
		if err := rows.Scan(&name, &createSQL); err != nil {
			return fmt.Errorf("scan system.tables: %w", err)
		}
		stmt, err := parseCreateStatement(createSQL)
		if err != nil {
			return fmt.Errorf("parse create_table_query for %s.%s: %w", database, name, err)
		}
		switch s := stmt.(type) {
		case *chparser.CreateTable:
			ts, err := buildTableFromCreateTable(s)
			if err != nil {
				return fmt.Errorf("introspect table %s.%s: %w", database, name, err)
			}
			ts.Name = name
			db.Tables = append(db.Tables, ts)
		case *chparser.CreateMaterializedView:
			mv, err := buildMaterializedViewFromCreateMV(s)
			if err != nil {
				return fmt.Errorf("introspect materialized view %s.%s: %w", database, name, err)
			}
			mv.Name = name
			db.MaterializedViews = append(db.MaterializedViews, mv)
		case *chparser.CreateView:
			// Plain (non-materialized) views are out of scope; skip them
			// rather than failing the whole introspection.
			continue
		default:
			return fmt.Errorf("introspect %s.%s: unsupported statement type %T", database, name, stmt)
		}
	}
	return nil
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
				t.Indexes = append(t.Indexes, indexFromAST(n))
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
				out.Columns = append(out.Columns, columnFromAST(cd))
			}
		}
	}

	if mv.SubQuery != nil && mv.SubQuery.Select != nil {
		out.Query = formatNode(mv.SubQuery.Select)
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

func indexFromAST(i *chparser.TableIndex) IndexSpec {
	out := IndexSpec{Name: identName(i.Name)}
	if i.ColumnExpr != nil {
		out.Expr = formatNode(i.ColumnExpr.Expr)
	}
	if i.ColumnType != nil {
		out.Type = formatNode(i.ColumnType)
	}
	if i.Granularity != nil {
		// Granularity is a NumberLiteral; convert via Format then parse.
		var n int
		_, _ = fmt.Sscanf(formatNode(i.Granularity), "%d", &n)
		out.Granularity = n
	}
	return out
}

func constraintFromAST(c *chparser.ConstraintClause) *ConstraintSpec {
	if c.Constraint == nil || c.Expr == nil {
		return nil
	}
	expr := formatNode(c.Expr)
	// ClickHouse stores both CHECK and ASSUME under ConstraintClause; the
	// upstream library doesn't preserve the kind in v0.5.1. Treat as CHECK
	// by default — that's the common case.
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
			return nil, nil, fmt.Errorf("Distributed needs (cluster, db, table[, sharding_key])")
		}
		ee := EngineDistributed{ClusterName: params[0], RemoteDatabase: params[1], RemoteTable: params[2]}
		if len(params) > 3 {
			ee.ShardingKey = &params[3]
		}
		return ee, allSettings, nil
	case "Log":
		return EngineLog{}, allSettings, nil
	case "Kafka":
		k := EngineKafka{
			BrokerList:    splitCSV(allSettings["kafka_broker_list"]),
			Topic:         allSettings["kafka_topic_list"],
			ConsumerGroup: allSettings["kafka_group_name"],
			Format:        allSettings["kafka_format"],
		}
		// Constructor form (legacy): Kafka(broker_list, topic, group, format).
		if k.Topic == "" && len(params) >= 4 {
			k = EngineKafka{
				BrokerList:    splitCSV(params[0]),
				Topic:         params[1],
				ConsumerGroup: params[2],
				Format:        params[3],
			}
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

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, strings.TrimSpace(p))
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
			return nil, fmt.Errorf("Distributed needs (cluster, db, table[, sharding_key]); got %v", p)
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
		return EngineKafka{
			BrokerList:    splitCSV(settings["kafka_broker_list"]),
			Topic:         settings["kafka_topic_list"],
			ConsumerGroup: settings["kafka_group_name"],
			Format:        settings["kafka_format"],
		}, nil
	}
	p, err := extractEngineParams(decl)
	if err != nil {
		return nil, err
	}
	if len(p) < 4 {
		return nil, fmt.Errorf("Kafka needs (broker_list, topic, group, format) or SETTINGS form; got %v", p)
	}
	return EngineKafka{
		BrokerList:    splitCSV(p[0]),
		Topic:         p[1],
		ConsumerGroup: p[2],
		Format:        p[3],
	}, nil
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

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
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
