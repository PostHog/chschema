package main

import (
	"flag"
	"log/slog"
	"os"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

func main() {
	configFlag := flag.String("config", "./cmd/hclexp/node.conf", "path to hcl config file")
	flag.Parse()
	slog.Info("HCL experiment is up")
	slog.Debug("Loading configuration", "config", *configFlag)

	dbs, err := hclload.ParseFile(*configFlag)
	if err != nil {
		slog.Error("failed to parse config", "err", err)
		os.Exit(1)
	}

	slog.Info("config decoded", "databases", len(dbs))
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
}

func engineKind(e *hclload.EngineSpec) string {
	if e == nil {
		return "(none)"
	}
	return e.Kind
}
