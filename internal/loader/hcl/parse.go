package hcl

import (
	"fmt"

	chparser "github.com/orian/clickhouse-sql-parser/parser"
)

// safeParseStmts parses SQL with the ClickHouse SQL parser, turning a parser
// panic into an ordinary error.
//
// Call this rather than chparser.NewParser(...).ParseStmts() directly. The
// input is arbitrary SQL — from a server's create_table_query, from an
// authored view or materialized-view query, or from a file handed to
// sql2hcl — so every call site can meet the same grammar.
// TestNoDirectParserCalls keeps that rule honest.
func safeParseStmts(sql string) (stmts []chparser.Expr, err error) {
	defer func() {
		if p := recover(); p != nil {
			stmts = nil
			err = fmt.Errorf("SQL parser panicked (unsupported syntax): %v", p)
		}
	}()
	return chparser.NewParser(sql).ParseStmts()
}
