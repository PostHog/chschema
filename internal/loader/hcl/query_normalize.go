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
}

// beautifyNode renders an AST node as indented, multi-line SQL via the parser's
// BeautifyVisitor — the readable counterpart to formatNode. This is the
// canonical form for view / materialized-view queries: the same logical query
// renders identically whether it was authored (one-line, heredoc, or via
// file()) or introspected from a live cluster, so formatting never shows as
// drift.
func beautifyNode(n chparser.Expr) string {
	if n == nil {
		return ""
	}
	v := chparser.NewBeautifyVisitor()
	if err := n.Accept(v); err != nil {
		return ""
	}
	return strings.TrimSpace(v.String())
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
