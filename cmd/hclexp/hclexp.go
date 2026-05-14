package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/posthog/chschema/config"
	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "introspect" {
		runIntrospect(os.Args[2:])
		return
	}
	runLoad(os.Args[1:])
}

// runLoad loads an HCL config (single file or layers), resolves it, and
// optionally writes the resolved schema back out as canonical HCL.
func runLoad(args []string) {
	fs := flag.NewFlagSet("hclexp", flag.ExitOnError)
	configFlag := fs.String("config", "./cmd/hclexp/node.conf", "path to a single HCL config file (mutually exclusive with -layer)")
	layersFlag := fs.String("layer", "", "comma-separated list of layer directories (loaded in order)")
	outFlag := fs.String("out", "", "if set, write the resolved schema to this file as canonical HCL")
	_ = fs.Parse(args)

	slog.Info("HCL experiment is up")

	dbs, err := load(*configFlag, *layersFlag)
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	if err := hclload.Resolve(dbs); err != nil {
		slog.Error("failed to resolve schema", "err", err)
		os.Exit(1)
	}

	slog.Info("schema resolved", "databases", len(dbs))
	for _, db := range dbs {
		slog.Info("database", "name", db.Name, "tables", len(db.Tables))
		for _, tbl := range db.Tables {
			slog.Info("table",
				"database", db.Name,
				"name", tbl.Name,
				"columns", len(tbl.Columns),
				"engine", engineKind(tbl.Engine),
			)
		}
	}

	if *outFlag != "" {
		f, err := os.Create(*outFlag)
		if err != nil {
			slog.Error("failed to open output file", "path", *outFlag, "err", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := hclload.Write(f, dbs); err != nil {
			slog.Error("failed to write resolved schema", "err", err)
			os.Exit(1)
		}
		slog.Info("resolved schema written", "path", *outFlag)
	}
}

// runIntrospect connects to a live ClickHouse instance, introspects one or
// more databases, and dumps them as canonical HCL.
func runIntrospect(args []string) {
	cfg := config.GetDefaultConfig()

	fs := flag.NewFlagSet("hclexp introspect", flag.ExitOnError)
	host := fs.String("host", cfg.Host, "ClickHouse host")
	port := fs.Int("port", cfg.Port, "ClickHouse port")
	dbFlag := fs.String("database", cfg.Database, "comma-separated databases to introspect")
	user := fs.String("user", cfg.User, "ClickHouse user")
	password := fs.String("password", cfg.Password, "ClickHouse password")
	outFlag := fs.String("out", "", "output .hcl file, or a directory (one <db>.hcl per database); empty writes to stdout")
	_ = fs.Parse(args)

	databases := splitList(*dbFlag)
	if len(databases) == 0 {
		slog.Error("no database specified")
		os.Exit(1)
	}

	cfg.Host, cfg.Port, cfg.User, cfg.Password = *host, *port, *user, *password
	cfg.Database = databases[0] // connection requires a database to bind to

	conn, err := config.NewConnection(cfg)
	if err != nil {
		slog.Error("failed to connect to ClickHouse", "host", cfg.Host, "port", cfg.Port, "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx := context.Background()
	var dbs []hclload.DatabaseSpec
	for _, name := range databases {
		spec, err := hclload.Introspect(ctx, conn, name)
		if err != nil {
			slog.Error("failed to introspect database", "database", name, "err", err)
			os.Exit(1)
		}
		slog.Info("introspected database", "name", spec.Name, "tables", len(spec.Tables))
		dbs = append(dbs, *spec)
	}

	if err := writeIntrospected(*outFlag, dbs); err != nil {
		slog.Error("failed to write introspected schema", "out", *outFlag, "err", err)
		os.Exit(1)
	}
}

// writeIntrospected dumps the introspected databases. An empty target writes
// to stdout; a directory target writes one <db>.hcl file per database;
// anything else is treated as a single output file holding all databases.
func writeIntrospected(out string, dbs []hclload.DatabaseSpec) error {
	if out == "" {
		return hclload.Write(os.Stdout, dbs)
	}

	if info, err := os.Stat(out); err == nil && info.IsDir() {
		for _, db := range dbs {
			path := filepath.Join(out, db.Name+".hcl")
			if err := writeFile(path, []hclload.DatabaseSpec{db}); err != nil {
				return err
			}
			slog.Info("schema written", "path", path)
		}
		return nil
	}

	if err := writeFile(out, dbs); err != nil {
		return err
	}
	slog.Info("schema written", "path", out)
	return nil
}

func writeFile(path string, dbs []hclload.DatabaseSpec) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return hclload.Write(f, dbs)
}

func load(configFlag, layersFlag string) ([]hclload.DatabaseSpec, error) {
	if layersFlag != "" {
		layers := strings.Split(layersFlag, ",")
		slog.Debug("loading layers", "layers", layers)
		return hclload.LoadLayers(layers)
	}
	slog.Debug("loading single file", "path", configFlag)
	return hclload.ParseFile(configFlag)
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func engineKind(e *hclload.EngineSpec) string {
	if e == nil {
		return "(none)"
	}
	return e.Kind
}
