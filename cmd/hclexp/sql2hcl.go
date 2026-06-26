package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

// runSQL2HCL applies SQL DDL edits to an existing HCL schema and emits the
// updated HCL. Developers express a change as the ClickHouse SQL they already
// know — CREATE / ALTER TABLE / DROP / RENAME — and it is folded into their
// declarative schema (the "left side"). The output is the resolved (flat)
// schema; pair it with `hclexp diff` to preview the migration the edit implies.
func runSQL2HCL(args []string) {
	fs := flag.NewFlagSet("hclexp sql2hcl", flag.ExitOnError)
	leftFlag := fs.String("left", "", "HCL schema to modify: a single file, or comma-separated layer directories")
	inFlag := fs.String("in", "", "SQL file to apply (default: stdin; \"-\" also means stdin)")
	outFlag := fs.String("out", "", "where to write updated HCL: empty or '-'=stdout, a directory=one <db>.hcl per database, else a single file")
	dbFlag := fs.String("database", "", "default database for unqualified object names")
	allowRaw := fs.Bool("allow-raw", false, "capture a CREATE the schema model can't express as a raw{} block instead of failing")
	_ = fs.Parse(args)

	if *leftFlag == "" {
		fmt.Fprintln(os.Stderr, "hclexp sql2hcl: -left is required (the HCL schema to modify)")
		os.Exit(2)
	}

	applied, dbs, err := applySQL2HCL(*leftFlag, *inFlag, *outFlag, *dbFlag, *allowRaw)
	if err != nil {
		slog.Error("sql2hcl failed", "err", err)
		os.Exit(1)
	}
	slog.Info("SQL applied", "statements", applied, "databases", dbs)
}

// applySQL2HCL is the testable core of runSQL2HCL: it loads and resolves the
// left schema, reads the SQL from in (file or stdin), folds the DDL into the
// schema, and writes the updated HCL to out. All I/O is parameterized so it can
// be driven from tests without touching os.Exit. Returns the number of applied
// statements and the resulting database count.
func applySQL2HCL(left, in, out, db string, allowRaw bool) (applied, databases int, err error) {
	schema, err := loadLeft(left)
	if err != nil {
		return 0, 0, fmt.Errorf("load -left schema: %w", err)
	}
	if err := hclload.Resolve(schema); err != nil {
		return 0, 0, fmt.Errorf("resolve -left schema: %w", err)
	}

	sql, err := readSQL(in)
	if err != nil {
		return 0, 0, fmt.Errorf("read SQL input: %w", err)
	}

	applied, err = hclload.ApplySQL(schema, sql, db, allowRaw)
	if err != nil {
		return 0, 0, fmt.Errorf("apply SQL: %w", err)
	}

	if err := writeIntrospected(out, schema); err != nil {
		return 0, 0, fmt.Errorf("write updated schema: %w", err)
	}
	return applied, len(schema.Databases), nil
}

// loadLeft loads the left-side schema. A path naming a directory (or a
// comma-separated list of directories) is loaded as layers; a single path that
// is a file is parsed directly.
func loadLeft(path string) (*hclload.Schema, error) {
	parts := splitList(path)
	if len(parts) > 1 {
		return hclload.LoadLayers(parts)
	}
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return hclload.LoadLayers([]string{path})
	}
	return hclload.ParseFile(path)
}

// readSQL reads the SQL to apply from a file, or from stdin when the path is
// empty or "-".
func readSQL(path string) (string, error) {
	if path == "" || path == "-" {
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
