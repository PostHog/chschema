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
	outFlag := fs.String("out", "", "where to write updated HCL: empty=stdout, a directory=one <db>.hcl per database, else a single file")
	dbFlag := fs.String("database", "", "default database for unqualified object names")
	allowRaw := fs.Bool("allow-raw", false, "capture a CREATE the schema model can't express as a raw{} block instead of failing")
	_ = fs.Parse(args)

	if *leftFlag == "" {
		fmt.Fprintln(os.Stderr, "hclexp sql2hcl: -left is required (the HCL schema to modify)")
		os.Exit(2)
	}

	schema, err := loadLeft(*leftFlag)
	if err != nil {
		slog.Error("failed to load -left schema", "err", err)
		os.Exit(1)
	}
	if err := hclload.Resolve(schema); err != nil {
		slog.Error("failed to resolve -left schema", "err", err)
		os.Exit(1)
	}

	sql, err := readSQL(*inFlag)
	if err != nil {
		slog.Error("failed to read SQL input", "err", err)
		os.Exit(1)
	}

	applied, err := hclload.ApplySQL(schema, sql, *dbFlag, *allowRaw)
	if err != nil {
		slog.Error("failed to apply SQL", "err", err)
		os.Exit(1)
	}

	if err := writeIntrospected(*outFlag, schema); err != nil {
		slog.Error("failed to write updated schema", "err", err)
		os.Exit(1)
	}
	slog.Info("SQL applied", "statements", applied, "databases", len(schema.Databases))
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
