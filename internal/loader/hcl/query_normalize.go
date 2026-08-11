package hcl

import (
	"fmt"
	"log/slog"
	"strings"

	chparser "github.com/orian/clickhouse-sql-parser/parser"
)

// Throwaway statement templates that make a bare fragment parseable. Each has
// exactly one %s — the slot the authored text fills — and nothing else in the
// statement may come from that text (see normalizeFragment).
const (
	normExprTmpl = "SELECT %s"
	normTypeTmpl = "CREATE TABLE _norm_type (_x %s) ENGINE = MergeTree ORDER BY _x"
	normTTLTmpl  = "CREATE TABLE _norm_ttl (_x Int) ENGINE = MergeTree ORDER BY _x TTL %s"
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

// fragment is what a normalizer pulled out of the throwaway statement it
// parsed its input inside. Verbatim is the fragment re-rendered with no
// canonicalization beyond the printer's own — it must reparse into the same
// node, so normalizeFragment can use it to prove the input contributed nothing
// but the fragment. Canonical is the text the caller actually wants, after any
// further rewriting (paren stripping, INTERVAL folding) that deliberately
// changes the rendering.
type fragment struct {
	verbatim  string
	canonical string
}

// normalizeFragment parses s as the single %s slot of tmpl — a throwaway
// statement that makes a bare fragment parseable — and returns the canonical
// rendering of that fragment.
//
// The input is only ever meant to fill the slot, but SQL does not stop at the
// slot: `String DEFAULT 'x'` in a column-type position parses as a type *plus*
// a DEFAULT clause, and a stray `)` lets the input rewrite the rest of the
// statement outright. Extracting just the fragment would silently discard
// everything else, turning a malformed declaration into a plausible-looking
// canonical value and dropping the DEFAULT from every statement generated from
// it. So the fragment's own rendering is substituted back into the template and
// the two statements are compared: anything the input contributed beyond the
// fragment lands somewhere in that comparison and is rejected with ok=false,
// which keeps the raw text and leaves the difference visible in the diff.
// Unparseable input is rejected the same way.
func normalizeFragment(tmpl, s string, render func(chparser.Expr) (fragment, bool)) (string, bool) {
	stmt, err := parseCreateStatement(fmt.Sprintf(tmpl, s))
	if err != nil {
		return s, false
	}
	frag, ok := render(stmt)
	if !ok {
		return s, false
	}
	back, err := parseCreateStatement(fmt.Sprintf(tmpl, frag.verbatim))
	if err != nil {
		return s, false
	}
	if formatNode(back) != formatNode(stmt) {
		return s, false
	}
	return frag.canonical, true
}

// normalizeExpr canonicalizes a single scalar expression — a column
// DEFAULT / MATERIALIZED / ALIAS expression, an index expression or type — to
// the same compact form introspection renders, so an authored expression and
// its live-introspected counterpart compare equal (issue #136 items 2 and 3).
// It parses the expression (wrapped in a throwaway SELECT so a bare expression
// is accepted), strips redundant outermost parentheses, and renders it via the
// same printer introspect uses. Returns ok=false with the input unchanged when
// it can't be parsed or reaches beyond the expression (see normalizeFragment),
// so the caller can keep the raw text.
func normalizeExpr(s string) (string, bool) {
	if strings.TrimSpace(s) == "" {
		return s, true
	}
	return normalizeFragment(normExprTmpl, s, func(stmt chparser.Expr) (fragment, bool) {
		sel, ok := stmt.(*chparser.SelectQuery)
		if !ok || len(sel.SelectItems) != 1 || sel.SelectItems[0].Alias != nil {
			return fragment{}, false
		}
		e := sel.SelectItems[0].Expr
		return fragment{
			verbatim:  formatNode(e),
			canonical: formatNode(unwrapRootParens(e)),
		}, true
	})
}

// normalizeTTL canonicalizes a table TTL clause to the same text introspection
// renders (formatTTLItems), so an authored TTL and its live-introspected
// counterpart compare equal. A stored TTL is rewritten by ClickHouse — INTERVAL
// 7 DAY becomes toIntervalDay(7), and a move rule (TO DISK / TO VOLUME) rides on
// the clause — so a raw string compare of authored vs introspected TTL never
// matches and the diff emits a perpetual no-op MODIFY TTL. Parsing both sides
// through the same printer removes that asymmetry (issue #136, TTL case).
// Returns ok=false with the input unchanged when it can't be parsed or reaches
// beyond the TTL clause (see normalizeFragment), so the caller keeps the raw
// text.
func normalizeTTL(s string) (string, bool) {
	if strings.TrimSpace(s) == "" {
		return s, true
	}
	return normalizeFragment(normTTLTmpl, s, func(stmt chparser.Expr) (fragment, bool) {
		ct, ok := stmt.(*chparser.CreateTable)
		if !ok || ct.Engine == nil || ct.Engine.TTL == nil || len(ct.Engine.TTL.Items) == 0 {
			return fragment{}, false
		}
		// formatTTLItems rewrites INTERVAL literals to ClickHouse's stored
		// toInterval<Unit>(n) form, so it is not structure-preserving and
		// cannot serve as the guard's verbatim rendering; the printer's own
		// per-item output can.
		parts := make([]string, 0, len(ct.Engine.TTL.Items))
		for _, it := range ct.Engine.TTL.Items {
			parts = append(parts, formatNode(it))
		}
		return fragment{
			verbatim:  strings.Join(parts, ", "),
			canonical: formatTTLItems(ct.Engine.TTL.Items),
		}, true
	})
}

// normalizeTTLPtr canonicalizes a *string TTL field in place, leaving it
// untouched when nil or unparseable.
func normalizeTTLPtr(p **string) {
	if *p == nil {
		return
	}
	if v, ok := normalizeTTL(**p); ok {
		*p = &v
	}
}

// normalizeType canonicalizes a column type to the same compact form
// introspection renders, so an authored type and its live-introspected
// counterpart compare equal (issue #136, column-type case). ClickHouse stores
// an Enum in create_table_query with spaces around '=' (Enum8('a' = 1, 'b' =
// 2)), while the printer every introspected type is rendered through emits
// Enum8('a'=1, 'b'=2); a raw string compare of an authored Enum8('a' = 1)
// against its introspected form therefore never matches and the diff emits a
// perpetual no-op MODIFY COLUMN. Parsing both sides through the same printer
// removes the asymmetry. The type is parsed as the column type of a throwaway
// CREATE TABLE — the same node path introspect uses — and rendered via
// formatNode. Returns ok=false with the input unchanged when it can't be
// parsed, or when it is more than a type: `String DEFAULT 'x'` belongs in the
// column's own `default` attribute, and keeping it raw makes that visible as
// drift instead of silently dropping the clause (see normalizeFragment).
func normalizeType(s string) (string, bool) {
	if strings.TrimSpace(s) == "" {
		return s, true
	}
	return normalizeFragment(normTypeTmpl, s, func(stmt chparser.Expr) (fragment, bool) {
		ct, ok := stmt.(*chparser.CreateTable)
		if !ok || ct.TableSchema == nil || len(ct.TableSchema.Columns) != 1 {
			return fragment{}, false
		}
		cd, ok := ct.TableSchema.Columns[0].(*chparser.ColumnDef)
		if !ok || cd.Type == nil {
			return fragment{}, false
		}
		t := formatNode(cd.Type)
		return fragment{verbatim: t, canonical: t}, true
	})
}

// canonicalize brings every type- and expression-bearing field of db to a
// single canonical string form, so a schema composed from HCL and the same
// schema introspected from a live cluster reduce to identical text and diff
// clean (issue #136). It is run at the tail of both the load path (ParseFile)
// and the introspect path, and must cover every field introspection renders
// through formatNode — a field it misses is a field that diffs as permanent
// drift on one kind of object while comparing clean on another.
func canonicalize(db *DatabaseSpec) {
	normalizeQueries(db)
	for ti := range db.Tables {
		t := &db.Tables[ti]
		object := db.Name + "." + t.Name
		normalizeColumnExprs(object, t.Columns)
		normalizePatchColumnExprs(object, t.ColumnPatches)
		normalizeIndexExprs(t.Indexes)
		normalizeTTLPtr(&t.TTL)
		normalizeEngineInnerColumns(object, t.Engine)
	}
	// A materialized view's explicit column list and a dictionary's attributes
	// are introspected through the same formatNode path as table columns, so
	// they need the same canonicalization — and need it more: an MV column
	// mismatch is not an in-place MODIFY COLUMN but a Recreate the generator
	// refuses to emit (a permanent unsafe entry), and a dictionary mismatch
	// rewrites the whole object on every apply.
	for mi := range db.MaterializedViews {
		mv := &db.MaterializedViews[mi]
		normalizeColumnExprs(db.Name+"."+mv.Name, mv.Columns)
	}
	for di := range db.Dictionaries {
		d := &db.Dictionaries[di]
		normalizeDictionaryAttrs(db.Name+"."+d.Name, d.Attributes)
	}
	// Patch fields land verbatim on their targets at resolution, so they
	// must be canonicalized exactly like declared fields — otherwise a
	// patched expression would diff against its own introspected form.
	// (order_by/partition_by/sample_by are deliberately left verbatim, exactly
	// as they are on declared tables; ttl is normalized because ClickHouse
	// rewrites it — see normalizeTTL.)
	for pi := range db.Patches {
		p := &db.Patches[pi]
		object := db.Name + "." + p.Name + " (patch_table)"
		normalizeColumnExprs(object, p.Columns)
		normalizeColumnExprs(object, p.ModifyColumns)
		normalizeIndexExprs(p.Indexes)
		for i := range p.Projections {
			projection := &p.Projections[i]
			if q, ok := normalizeQuery(projection.Query); ok {
				projection.Query = q
			} else if strings.TrimSpace(projection.Query) != "" {
				slog.Warn("patch_table projection query could not be parsed for normalization; keeping raw (may diff as drift)",
					"database", db.Name, "table", p.Name, "projection", projection.Name)
			}
		}
		normalizeTTLPtr(&p.TTL)
		normalizeEngineInnerColumns(object, p.Engine)
	}
	for pi := range db.MaterializedViewPatches {
		p := &db.MaterializedViewPatches[pi]
		object := db.Name + "." + p.Name + " (patch_materialized_view)"
		normalizeColumnExprs(object, p.Columns)
		normalizeColumnExprs(object, p.ModifyColumns)
		if p.Query == nil {
			continue
		}
		if q, ok := normalizeQuery(*p.Query); ok {
			p.Query = &q
		} else if strings.TrimSpace(*p.Query) != "" {
			slog.Warn("patch_materialized_view query could not be parsed for normalization; keeping raw (may diff as drift)",
				"database", db.Name, "materialized_view", p.Name)
		}
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

// warnRawType reports a column type kept verbatim because it did not
// canonicalize — either the parser can't express it, or it carries more than a
// type (a DEFAULT / CODEC / COMMENT clause that belongs in its own attribute).
// Loading still succeeds, but the type is now the one field on the object that
// compares raw against introspected text, so say so: the alternative is an
// unexplained perpetual MODIFY COLUMN.
func warnRawType(object, column, typ string) {
	slog.Warn("column type could not be canonicalized; keeping raw (may diff as drift)",
		"object", object, "column", column, "type", typ)
}

// normalizeColumnExprs canonicalizes the type and expression-bearing fields of
// each column in place. object labels the owning table / view / dictionary in
// warnings.
func normalizeColumnExprs(object string, cols []ColumnSpec) {
	for ci := range cols {
		c := &cols[ci]
		if nt, ok := normalizeType(c.Type); ok {
			c.Type = nt
		} else {
			warnRawType(object, c.Name, c.Type)
		}
		normalizeExprPtr(&c.Default)
		normalizeExprPtr(&c.Materialized)
		normalizeExprPtr(&c.Alias)
		normalizeExprPtr(&c.Ephemeral)
	}
}

// normalizePatchColumnExprs canonicalizes the type and expression-bearing
// fields of partial inherited-column patches exactly like full column
// declarations.
func normalizePatchColumnExprs(object string, patches []PatchColumnSpec) {
	for i := range patches {
		p := &patches[i]
		if p.Type != nil {
			if nt, ok := normalizeType(*p.Type); ok {
				p.Type = &nt
			} else {
				warnRawType(object, p.Name, *p.Type)
			}
		}
		normalizeExprPtr(&p.Default)
		normalizeExprPtr(&p.Materialized)
		normalizeExprPtr(&p.Alias)
		normalizeExprPtr(&p.Ephemeral)
	}
}

// normalizeDictionaryAttrs canonicalizes the type and expression-bearing
// fields of each dictionary attribute in place, the column-block treatment
// applied to the dictionary's equivalent of a column: introspection renders
// attribute type, DEFAULT, and EXPRESSION through formatNode just as it does a
// table column's.
func normalizeDictionaryAttrs(object string, attrs []DictionaryAttribute) {
	for i := range attrs {
		a := &attrs[i]
		if nt, ok := normalizeType(a.Type); ok {
			a.Type = nt
		} else {
			warnRawType(object, a.Name, a.Type)
		}
		normalizeExprPtr(&a.Default)
		normalizeExprPtr(&a.Expression)
	}
}

// normalizeEngineInnerColumns canonicalizes the columns of a TimeSeries
// engine's inner tables — the one column list that lives inside an engine
// block rather than on the table. It runs off EngineSpec.Decoded, which is why
// canonicalize is called after the engines are decoded; the target sub-blocks
// are pointers, so mutating through them updates the decoded engine in place.
func normalizeEngineInnerColumns(object string, e *EngineSpec) {
	if e == nil {
		return
	}
	ts, ok := e.Decoded.(EngineTimeSeries)
	if !ok {
		return
	}
	for _, t := range []*TimeSeriesTarget{ts.Samples, ts.Tags, ts.Metrics} {
		if t == nil || t.Inner == nil {
			continue
		}
		normalizeColumnExprs(object+" (inner)", t.Inner.Columns)
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
