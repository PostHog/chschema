package hcl

import (
	"fmt"

	chparser "github.com/orian/clickhouse-sql-parser/parser"
)

// safeParseStmts parses SQL with the ClickHouse SQL parser, turning a parser
// panic into an ordinary error.
//
// Call this rather than chparser.NewParser(...).ParseStmts() directly. Inputs
// include arbitrary server DDL, authored view and materialized-view queries,
// and sql2hcl input, so every parser entry point needs the same containment.
func safeParseStmts(sql string) (stmts []chparser.Expr, err error) {
	return recoverParserPanic(func() ([]chparser.Expr, error) {
		return chparser.NewParser(sql).ParseStmts()
	})
}

// recoverParserPanic is separate from safeParseStmts so the containment can be
// tested without depending on a particular upstream parser defect remaining
// unfixed.
func recoverParserPanic(parse func() ([]chparser.Expr, error)) (stmts []chparser.Expr, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			stmts = nil
			err = fmt.Errorf("SQL parser panicked (unsupported syntax): %v", recovered)
		}
	}()
	return parse()
}
