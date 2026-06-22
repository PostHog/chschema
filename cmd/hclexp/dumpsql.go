package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/posthog/chschema/config"
)

// runDumpSQL connects to ClickHouse and writes the raw create_table_query for
// every object in a database, in apply order, each terminated by `;` on its own
// line. The output is a self-describing, replayable seed: capture a production
// schema with this command, then recreate it on a local ClickHouse for testing
// (see the round-trip fidelity test). Unlike `introspect` (which emits HCL),
// dump-sql emits ClickHouse's own canonical DDL verbatim.
func runDumpSQL(args []string) {
	cfg := config.GetDefaultConfig()

	fs := flag.NewFlagSet("hclexp dump-sql", flag.ExitOnError)
	host := fs.String("host", cfg.Host, "ClickHouse host")
	port := fs.Int("port", cfg.Port, "ClickHouse port")
	dbFlag := fs.String("database", cfg.Database, "database to dump")
	user := fs.String("user", cfg.User, "ClickHouse user")
	password := fs.String("password", cfg.Password, "ClickHouse password")
	outFlag := fs.String("out", "", "output .sql file; empty writes to stdout")
	secure := fs.Bool("secure", cfg.Secure, "connect to ClickHouse over TLS")
	skipVerify := fs.Bool("tls-skip-verify", cfg.TLSSkipVerify, "skip TLS certificate verification (requires -secure)")
	_ = fs.Parse(args)

	if *dbFlag == "" {
		slog.Error("no database specified")
		os.Exit(1)
	}

	cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database = *host, *port, *user, *password, *dbFlag
	if err := applyTLSFlags(&cfg, *secure, *skipVerify); err != nil {
		slog.Error("invalid TLS flag combination", "err", err)
		os.Exit(2)
	}

	conn, err := config.NewConnection(cfg)
	if err != nil {
		slog.Error("failed to connect to ClickHouse", "host", cfg.Host, "port", cfg.Port, "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	out, err := dumpCreateStatements(context.Background(), conn, *dbFlag)
	if err != nil {
		slog.Error("failed to dump create statements", "database", *dbFlag, "err", err)
		os.Exit(1)
	}

	if *outFlag == "" {
		fmt.Print(out)
		return
	}
	if err := os.WriteFile(*outFlag, []byte(out), 0o644); err != nil {
		slog.Error("failed to write output file", "out", *outFlag, "err", err)
		os.Exit(1)
	}
	slog.Info("wrote create statements", "database", *dbFlag, "out", *outFlag)
}

// dumpObject is one object's canonical CREATE DDL plus its engine, used to order
// the dump so dependencies are applied before dependents.
type dumpObject struct {
	name   string
	engine string
	create string
}

// dumpCreateStatements reads every object's create_table_query and renders them
// in apply order with a `-- database: <db>` header.
func dumpCreateStatements(ctx context.Context, conn driver.Conn, database string) (string, error) {
	const q = `SELECT name, create_table_query, engine
		FROM system.tables
		WHERE database = ? AND NOT is_temporary AND create_table_query != ''
		ORDER BY name`
	rows, err := conn.Query(ctx, q, database)
	if err != nil {
		return "", fmt.Errorf("query system.tables: %w", err)
	}
	defer rows.Close()

	var objs []dumpObject
	for rows.Next() {
		var o dumpObject
		if err := rows.Scan(&o.name, &o.create, &o.engine); err != nil {
			return "", fmt.Errorf("scan system.tables: %w", err)
		}
		objs = append(objs, o)
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("rows iteration: %w", err)
	}

	sort.SliceStable(objs, func(i, j int) bool {
		pi, pj := dumpOrderPriority(objs[i].engine), dumpOrderPriority(objs[j].engine)
		if pi != pj {
			return pi < pj
		}
		return objs[i].name < objs[j].name
	})

	var b strings.Builder
	fmt.Fprintf(&b, "-- database: %s\n", database)
	for _, o := range objs {
		fmt.Fprintf(&b, "\n%s\n;\n", strings.TrimSpace(o.create))
	}
	return b.String(), nil
}

// dumpOrderPriority orders objects so dependencies come first: plain tables,
// then dictionaries, plain views, and finally materialized views (which require
// their target/source to exist).
func dumpOrderPriority(engine string) int {
	switch engine {
	case "MaterializedView":
		return 3
	case "View":
		return 2
	case "Dictionary":
		return 1
	default:
		return 0
	}
}
