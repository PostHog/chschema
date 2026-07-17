package hcl

import (
	"log/slog"
	"strings"

	chparser "github.com/orian/clickhouse-sql-parser/parser"
)

// normalizeQueries canonicalizes every view and materialized-view query in db
// to the beautified form. A query the parser can't handle is kept verbatim and
// a warning is logged: loading never fails on a query that is valid to
// ClickHouse but not yet expressible by the parser (it may, however, diff as
// drift until the parser catches up).
func normalizeQueries(db *DatabaseSpec) {
	for i := range db.Views {
		if q, ok := normalizeQuery(db.Views[i].Query); ok {
			db.Views[i].Query = q
		} else if strings.TrimSpace(db.Views[i].Query) != "" {
			slog.Warn("view query could not be parsed for normalization; keeping raw (may diff as drift)",
				"database", db.Name, "view", db.Views[i].Name)
		}
	}
	for i := range db.MaterializedViews {
		if q, ok := normalizeQuery(db.MaterializedViews[i].Query); ok {
			db.MaterializedViews[i].Query = q
		} else if strings.TrimSpace(db.MaterializedViews[i].Query) != "" {
			slog.Warn("materialized view query could not be parsed for normalization; keeping raw (may diff as drift)",
				"database", db.Name, "materialized_view", db.MaterializedViews[i].Name)
		}
	}
	for ti := range db.Tables {
		for pi := range db.Tables[ti].Projections {
			p := &db.Tables[ti].Projections[pi]
			if q, ok := normalizeQuery(p.Query); ok {
				p.Query = q
			} else if strings.TrimSpace(p.Query) != "" {
				slog.Warn("projection query could not be parsed for normalization; keeping raw (may diff as drift)",
					"database", db.Name, "table", db.Tables[ti].Name, "projection", p.Name)
			}
		}
	}
}

// beautifyNode renders an AST node as indented, multi-line SQL via the parser's
// BeautifyVisitor — the readable counterpart to formatNode. This is the
// canonical form for view / materialized-view queries: the same logical query
// renders identically whether it was authored (one-line, heredoc, or via
// file()) or introspected from a live cluster, so formatting never shows as
// drift. Redundant outermost clause parentheses are stripped first (see
// stripRedundantClauseParens) so ClickHouse's HAVING ((a) AND (b)) and the
// authored HAVING (a) AND (b) converge.
func beautifyNode(n chparser.Expr) string {
	if n == nil {
		return ""
	}
	stripRedundantClauseParens(n)
	v := chparser.NewBeautifyVisitor()
	if err := n.Accept(v); err != nil {
		return ""
	}
	return strings.TrimSpace(v.String())
}

// unwrapRootParens removes redundant outermost parentheses from a standalone
// expression. A parenthesised scalar `(x)` parses to a single-item
// ParamExprList (ColumnArgList == nil, exactly one Item), and the parser wraps
// every list item and clause value in an alias-less ColumnExpr; both are
// transparent at an expression-root position — a clause value, or a whole
// column / index expression — so peeling them is safe regardless of the inner
// operator's precedence. Tuples `(a, b)` (len > 1), aliased ColumnExprs, and
// subqueries (a distinct AST node) are left untouched. Only the outermost
// layer(s) are removed; inner parentheses are preserved because dropping them
// would require precedence analysis (e.g. `(a + b) * c`).
func unwrapRootParens(e chparser.Expr) chparser.Expr {
	for {
		switch n := e.(type) {
		case *chparser.ColumnExpr:
			if n.Alias != nil {
				return e
			}
			e = n.Expr
		case *chparser.ParamExprList:
			if n.ColumnArgList != nil || n.Items == nil || len(n.Items.Items) != 1 {
				return e
			}
			e = n.Items.Items[0]
		default:
			return e
		}
	}
}

// stripClauseParens canonicalizes a WHERE / PREWHERE / HAVING value. The parser
// stores it as an alias-less ColumnExpr; we keep that wrapper node and only
// canonicalize the expression inside it, so a clause with no redundant parens
// is left byte-identical (no snapshot churn) while ClickHouse's redundant outer
// pair is dropped — and both sides end up with the same node shape, so long
// clauses that the beautifier line-wraps format identically.
func stripClauseParens(e chparser.Expr) chparser.Expr {
	if ce, ok := e.(*chparser.ColumnExpr); ok && ce.Alias == nil {
		ce.Expr = unwrapRootParens(ce.Expr)
		return ce
	}
	return unwrapRootParens(e)
}

// clauseParenStripper unwraps redundant outermost parentheses from the WHERE /
// PREWHERE / HAVING value of every SELECT it visits, including nested CTE and
// subquery SELECTs.
type clauseParenStripper struct {
	chparser.DefaultASTVisitor
}

func (v *clauseParenStripper) Enter(e chparser.Expr) {
	sq, ok := e.(*chparser.SelectQuery)
	if !ok {
		return
	}
	if sq.Prewhere != nil {
		sq.Prewhere.Expr = stripClauseParens(sq.Prewhere.Expr)
	}
	if sq.Where != nil {
		sq.Where.Expr = stripClauseParens(sq.Where.Expr)
	}
	if sq.Having != nil {
		sq.Having.Expr = stripClauseParens(sq.Having.Expr)
	}
}

// stripRedundantClauseParens walks n in place, canonicalizing clause-level
// parentheses in every SELECT it contains. The AST is always a throwaway parse
// here, so mutating it is safe.
func stripRedundantClauseParens(n chparser.Expr) {
	v := &clauseParenStripper{}
	v.Self = v
	_ = n.Accept(v)
}

// normalizeExpr canonicalizes a single scalar expression — a column
// DEFAULT / MATERIALIZED / ALIAS expression, an index expression or type — to
// the same compact form introspection renders, so an authored expression and
// its live-introspected counterpart compare equal (issue #136 items 2 and 3).
// It parses the expression (wrapped in a throwaway SELECT so a bare expression
// is accepted), strips redundant outermost parentheses, and renders it via the
// same printer introspect uses. Returns ok=false with the input unchanged when
// it can't be parsed, so the caller can keep the raw text.
func normalizeExpr(s string) (string, bool) {
	if strings.TrimSpace(s) == "" {
		return s, true
	}
	stmt, err := parseCreateStatement("SELECT " + s)
	if err != nil {
		return s, false
	}
	sel, ok := stmt.(*chparser.SelectQuery)
	if !ok || len(sel.SelectItems) != 1 || sel.SelectItems[0].Alias != nil {
		return s, false
	}
	return formatNode(unwrapRootParens(sel.SelectItems[0].Expr)), true
}

// canonicalize brings every expression-bearing field of db to a single
// canonical string form, so a schema composed from HCL and the same schema
// introspected from a live cluster reduce to identical text and diff clean
// (issue #136). It is run at the tail of both the load path (ParseFile) and the
// introspect path.
func canonicalize(db *DatabaseSpec) {
	normalizeQueries(db)
	for ti := range db.Tables {
		t := &db.Tables[ti]
		normalizeColumnExprs(t.Columns)
		normalizeIndexExprs(t.Indexes)
	}
	// Patch fields land verbatim on their targets at resolution, so they
	// must be canonicalized exactly like declared fields — otherwise a
	// patched expression would diff against its own introspected form.
	// (order_by/partition_by/sample_by/ttl are deliberately left verbatim,
	// exactly as they are on declared tables.)
	for pi := range db.Patches {
		p := &db.Patches[pi]
		normalizeColumnExprs(p.Columns)
		normalizeColumnExprs(p.ModifyColumns)
		normalizeIndexExprs(p.Indexes)
	}
	for pi := range db.ViewPatches {
		p := &db.ViewPatches[pi]
		if p.Query == nil {
			continue
		}
		if q, ok := normalizeQuery(*p.Query); ok {
			p.Query = &q
		} else if strings.TrimSpace(*p.Query) != "" {
			slog.Warn("patch_view query could not be parsed for normalization; keeping raw (may diff as drift)",
				"database", db.Name, "view", p.Name)
		}
	}
}

// normalizeColumnExprs canonicalizes the expression-bearing fields of each
// column in place.
func normalizeColumnExprs(cols []ColumnSpec) {
	for ci := range cols {
		c := &cols[ci]
		normalizeExprPtr(&c.Default)
		normalizeExprPtr(&c.Materialized)
		normalizeExprPtr(&c.Alias)
		normalizeExprPtr(&c.Ephemeral)
	}
}

// normalizeIndexExprs canonicalizes each index's expr and type in place.
func normalizeIndexExprs(idxs []IndexSpec) {
	for ii := range idxs {
		idx := &idxs[ii]
		if nx, ok := normalizeExpr(idx.Expr); ok {
			idx.Expr = nx
		}
		if nt, ok := normalizeExpr(idx.Type); ok {
			idx.Type = nt
		}
	}
}

// normalizeExprPtr canonicalizes an optional expression string in place,
// leaving it untouched when unset or unparseable.
func normalizeExprPtr(p **string) {
	if *p == nil {
		return
	}
	if nx, ok := normalizeExpr(**p); ok {
		*p = &nx
	}
}

// BeautifySQL parses a single CREATE statement and returns it re-rendered in
// the parser's beautified (indented, multi-line) form — the same visitor that
// produces readable view/MV queries elsewhere. It returns ok=false with the
// input unchanged when the statement can't be parsed, so callers can fall back
// to the verbatim SQL (e.g. for DDL the parser doesn't yet handle).
func BeautifySQL(sql string) (string, bool) {
	stmt, err := parseCreateStatement(sql)
	if err != nil {
		return sql, false
	}
	return beautifyNode(stmt), true
}

// normalizeQuery canonicalizes a view/MV SELECT body to the beautified form. It
// parses the query — wrapped in a throwaway CREATE VIEW so a bare SELECT is
// accepted — and beautifies the SELECT subtree, matching what introspect emits
// for the same query. Returns ok=false (and the input unchanged) when the query
// can't be parsed, so the caller can keep the raw text and warn.
func normalizeQuery(sql string) (string, bool) {
	if strings.TrimSpace(sql) == "" {
		return sql, true
	}
	stmt, err := parseCreateStatement("CREATE VIEW __normalize__ AS " + sql)
	if err != nil {
		return sql, false
	}
	cv, ok := stmt.(*chparser.CreateView)
	if !ok || cv.SubQuery == nil || cv.SubQuery.Select == nil {
		return sql, false
	}
	return beautifyNode(cv.SubQuery.Select), true
}
