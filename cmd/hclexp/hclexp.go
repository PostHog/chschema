package main

import (
	"flag"
	"log/slog"
	"os"
	"strings"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

func main() {
	configFlag := flag.String("config", "./cmd/hclexp/node.conf", "path to a single HCL config file (mutually exclusive with -layer)")
	layersFlag := flag.String("layer", "", "comma-separated list of layer directories (loaded in order)")
	outFlag := flag.String("out", "", "if set, write the resolved schema to this file as canonical HCL")
	flag.Parse()

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

func load(configFlag, layersFlag string) ([]hclload.DatabaseSpec, error) {
	if layersFlag != "" {
		layers := strings.Split(layersFlag, ",")
		slog.Debug("loading layers", "layers", layers)
		return hclload.LoadLayers(layers)
	}
	slog.Debug("loading single file", "path", configFlag)
	return hclload.ParseFile(configFlag)
}

func engineKind(e *hclload.EngineSpec) string {
	if e == nil {
		return "(none)"
	}
	return e.Kind
}
