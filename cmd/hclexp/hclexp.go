package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/posthog/chschema/config"
	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "introspect":
			runIntrospect(os.Args[2:])
			return
		case "diff":
			runDiff(os.Args[2:])
			return
		case "validate":
			runValidate(os.Args[2:])
			return
		}
	}
	runLoad(os.Args[1:])
}

// runValidate loads an HCL config (single file or layers), resolves it, and
// checks that every materialized view's source/destination tables and every
// Distributed table's remote table are declared in the loaded schema. It
// exits non-zero when any dependency is unsatisfied. The -skip-validation
// flag takes a comma-separated list of dependent object names (or "*" for
// all) whose dependency checks should be skipped.
func runValidate(args []string) {
	fs := flag.NewFlagSet("hclexp validate", flag.ExitOnError)
	configFlag := fs.String("config", "./cmd/hclexp/node.conf", "path to a single HCL config file (mutually exclusive with -layer)")
	layersFlag := fs.String("layer", "", "comma-separated list of layer directories (loaded in order)")
	skipFlag := fs.String("skip-validation", "", "comma-separated dependent object names to skip, or \"*\" for all")
	_ = fs.Parse(args)

	schema, err := load(*configFlag, *layersFlag)
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}
	if err := hclload.Resolve(schema); err != nil {
		slog.Error("failed to resolve schema", "err", err)
		os.Exit(1)
	}

	errs := hclload.Validate(schema.Databases, hclload.ParseSkipSet(*skipFlag))
	if len(errs) > 0 {
		for _, e := range errs {
			fmt.Fprintf(os.Stderr, "validation error: %s\n", e.Error())
		}
		slog.Error("schema validation failed", "errors", len(errs))
		os.Exit(1)
	}

	slog.Info("schema validation passed", "databases", len(schema.Databases))
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

	schema, err := load(*configFlag, *layersFlag)
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	if err := hclload.Resolve(schema); err != nil {
		slog.Error("failed to resolve schema", "err", err)
		os.Exit(1)
	}

	slog.Info("schema resolved", "databases", len(schema.Databases), "named_collections", len(schema.NamedCollections))
	for _, db := range schema.Databases {
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
		if err := hclload.Write(f, schema); err != nil {
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
	schema := &hclload.Schema{}
	for _, name := range databases {
		spec, err := hclload.Introspect(ctx, conn, name)
		if err != nil {
			slog.Error("failed to introspect database", "database", name, "err", err)
			os.Exit(1)
		}
		slog.Info("introspected database", "name", spec.Name,
			"tables", len(spec.Tables), "materialized_views", len(spec.MaterializedViews),
			"dictionaries", len(spec.Dictionaries))
		schema.Databases = append(schema.Databases, *spec)
	}

	if err := writeIntrospected(*outFlag, schema); err != nil {
		slog.Error("failed to write introspected schema", "out", *outFlag, "err", err)
		os.Exit(1)
	}
}

// runDiff loads a left and a right schema and reports the changes needed to
// turn the left side into the right side. Each side is either an HCL source
// (a file, or comma-separated layer directories) or a live ClickHouse
// instance addressed as clickhouse://user:password@host:port/db1,db2.
func runDiff(args []string) {
	fs := flag.NewFlagSet("hclexp diff", flag.ExitOnError)
	leftFlag := fs.String("left", "", "left side: HCL file, comma-separated layer dirs, or clickhouse://user:pass@host:port/db")
	rightFlag := fs.String("right", "", "right side: same forms as -left")
	asSQL := fs.Bool("sql", false, "emit migration DDL (left -> right) instead of a change summary")
	_ = fs.Parse(args)

	if *leftFlag == "" || *rightFlag == "" {
		slog.Error("both -left and -right are required")
		os.Exit(2)
	}

	left, err := loadSide(*leftFlag)
	if err != nil {
		slog.Error("failed to load left side", "spec", *leftFlag, "err", err)
		os.Exit(1)
	}
	right, err := loadSide(*rightFlag)
	if err != nil {
		slog.Error("failed to load right side", "spec", *rightFlag, "err", err)
		os.Exit(1)
	}

	cs := hclload.Diff(left, right)

	if *asSQL {
		gen := hclload.GenerateSQL(cs)
		for _, u := range gen.Unsafe {
			fmt.Printf("-- UNSAFE: %s.%s: %s\n", u.Database, u.Table, u.Reason)
		}
		for _, stmt := range gen.Statements {
			fmt.Println(stmt + ";")
		}
		if len(gen.Statements) == 0 {
			fmt.Println("-- no changes")
		}
		return
	}

	if cs.IsEmpty() {
		fmt.Println("no differences")
		return
	}
	renderChangeSet(os.Stdout, cs)
}

// loadSide loads one diff operand. A spec starting with clickhouse:// is
// introspected from a live instance; anything else is treated as a
// filesystem HCL source (a single file, or comma-separated layer dirs) and
// resolved.
func loadSide(spec string) (*hclload.Schema, error) {
	if strings.HasPrefix(spec, "clickhouse://") {
		return loadFromClickHouse(spec)
	}

	paths := splitList(spec)
	var (
		schema *hclload.Schema
		err    error
	)
	if len(paths) == 1 {
		if info, statErr := os.Stat(paths[0]); statErr == nil && !info.IsDir() {
			schema, err = hclload.ParseFile(paths[0])
		} else {
			schema, err = hclload.LoadLayers(paths)
		}
	} else {
		schema, err = hclload.LoadLayers(paths)
	}
	if err != nil {
		return nil, err
	}
	if err := hclload.Resolve(schema); err != nil {
		return nil, fmt.Errorf("resolve: %w", err)
	}
	return schema, nil
}

// loadFromClickHouse connects to and introspects the databases named in a
// clickhouse:// URI.
func loadFromClickHouse(uri string) (*hclload.Schema, error) {
	cfg, databases, err := parseClickHouseURI(uri)
	if err != nil {
		return nil, err
	}
	conn, err := config.NewConnection(cfg)
	if err != nil {
		return nil, fmt.Errorf("connect %s:%d: %w", cfg.Host, cfg.Port, err)
	}
	defer conn.Close()

	ctx := context.Background()
	schema := &hclload.Schema{}
	for _, name := range databases {
		spec, err := hclload.Introspect(ctx, conn, name)
		if err != nil {
			return nil, fmt.Errorf("introspect %s: %w", name, err)
		}
		schema.Databases = append(schema.Databases, *spec)
	}
	return schema, nil
}

// parseClickHouseURI turns clickhouse://user:password@host:port/db1,db2 into
// a connection config and the list of databases to introspect. Missing
// pieces fall back to the environment-driven defaults.
func parseClickHouseURI(uri string) (config.ClickHouseConfig, []string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return config.ClickHouseConfig{}, nil, fmt.Errorf("invalid clickhouse URI: %w", err)
	}

	cfg := config.GetDefaultConfig()
	if h := u.Hostname(); h != "" {
		cfg.Host = h
	}
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil {
			return config.ClickHouseConfig{}, nil, fmt.Errorf("invalid port %q: %w", p, err)
		}
		cfg.Port = n
	}
	if u.User != nil {
		if name := u.User.Username(); name != "" {
			cfg.User = name
		}
		if pw, ok := u.User.Password(); ok {
			cfg.Password = pw
		}
	}

	databases := splitList(strings.TrimPrefix(u.Path, "/"))
	if len(databases) == 0 {
		databases = []string{cfg.Database}
	}
	cfg.Database = databases[0] // connection requires a database to bind to
	return cfg, databases, nil
}

// renderChangeSet prints a ChangeSet as an indented, +/-/~ marked summary.
func renderChangeSet(w io.Writer, cs hclload.ChangeSet) {
	for _, dc := range cs.Databases {
		fmt.Fprintf(w, "database %q\n", dc.Database)
		for _, t := range dc.AddTables {
			fmt.Fprintf(w, "  + table %s\n", t.Name)
		}
		for _, t := range dc.DropTables {
			fmt.Fprintf(w, "  - table %s\n", t.Name)
		}
		for _, td := range dc.AlterTables {
			fmt.Fprintf(w, "  ~ table %s\n", td.Table)
			renderTableDiff(w, td)
		}
		for _, mv := range dc.AddMaterializedViews {
			fmt.Fprintf(w, "  + materialized_view %s -> %s\n", mv.Name, mv.ToTable)
		}
		for _, name := range dc.DropMaterializedViews {
			fmt.Fprintf(w, "  - materialized_view %s\n", name)
		}
		for _, mvd := range dc.AlterMaterializedViews {
			fmt.Fprintf(w, "  ~ materialized_view %s\n", mvd.Name)
			if mvd.Recreate {
				fmt.Fprintf(w, "      ! requires recreation (to_table or columns changed)\n")
			}
			if mvd.QueryChange != nil {
				fmt.Fprintf(w, "      ~ query changed\n")
			}
		}
		for _, d := range dc.AddDictionaries {
			fmt.Fprintf(w, "  + dictionary %s\n", d.Name)
		}
		for _, name := range dc.DropDictionaries {
			fmt.Fprintf(w, "  - dictionary %s\n", name)
		}
		for _, dd := range dc.AlterDictionaries {
			fmt.Fprintf(w, "  ~ dictionary %s (changed: %s)\n", dd.Name, strings.Join(dd.Changed, ", "))
		}
	}
}

func renderTableDiff(w io.Writer, td hclload.TableDiff) {
	for _, r := range td.RenameColumns {
		fmt.Fprintf(w, "      ~ column %s (renamed from %s)\n", r.New, r.Old)
	}
	for _, c := range td.AddColumns {
		fmt.Fprintf(w, "      + column %s %s\n", c.Name, c.Type)
	}
	for _, name := range td.DropColumns {
		fmt.Fprintf(w, "      - column %s\n", name)
	}
	for _, c := range td.ModifyColumns {
		fmt.Fprintf(w, "      ~ column %s: %s -> %s\n", c.Name, c.OldType, c.NewType)
	}
	for _, idx := range td.AddIndexes {
		fmt.Fprintf(w, "      + index %s\n", idx.Name)
	}
	for _, name := range td.DropIndexes {
		fmt.Fprintf(w, "      - index %s\n", name)
	}
	if td.EngineChange != nil {
		fmt.Fprintf(w, "      ~ engine: %s -> %s\n", engineKindName(td.EngineChange.Old), engineKindName(td.EngineChange.New))
	}
	if c := td.OrderByChange; c != nil {
		fmt.Fprintf(w, "      ~ order_by: %v -> %v\n", c.Old, c.New)
	}
	if c := td.PartitionByChange; c != nil {
		fmt.Fprintf(w, "      ~ partition_by: %s -> %s\n", strOrNone(c.Old), strOrNone(c.New))
	}
	if c := td.SampleByChange; c != nil {
		fmt.Fprintf(w, "      ~ sample_by: %s -> %s\n", strOrNone(c.Old), strOrNone(c.New))
	}
	if c := td.TTLChange; c != nil {
		fmt.Fprintf(w, "      ~ ttl: %s -> %s\n", strOrNone(c.Old), strOrNone(c.New))
	}
	for _, k := range sortedMapKeys(td.SettingsAdded) {
		fmt.Fprintf(w, "      + setting %s = %s\n", k, td.SettingsAdded[k])
	}
	for _, k := range td.SettingsRemoved {
		fmt.Fprintf(w, "      - setting %s\n", k)
	}
	for _, c := range td.SettingsChanged {
		fmt.Fprintf(w, "      ~ setting %s: %s -> %s\n", c.Key, c.OldValue, c.NewValue)
	}
}

func load(configFlag, layersFlag string) (*hclload.Schema, error) {
	if layersFlag != "" {
		layers := strings.Split(layersFlag, ",")
		slog.Debug("loading layers", "layers", layers)
		return hclload.LoadLayers(layers)
	}
	slog.Debug("loading single file", "path", configFlag)
	return hclload.ParseFile(configFlag)
}

// writeIntrospected dumps the introspected databases. An empty target writes
// to stdout; a directory target writes one <db>.hcl file per database;
// anything else is treated as a single output file holding all databases.
func writeIntrospected(out string, schema *hclload.Schema) error {
	if out == "" {
		return hclload.Write(os.Stdout, schema)
	}

	if info, err := os.Stat(out); err == nil && info.IsDir() {
		for _, db := range schema.Databases {
			path := filepath.Join(out, db.Name+".hcl")
			if err := writeFile(path, &hclload.Schema{Databases: []hclload.DatabaseSpec{db}}); err != nil {
				return err
			}
			slog.Info("schema written", "path", path)
		}
		return nil
	}

	if err := writeFile(out, schema); err != nil {
		return err
	}
	slog.Info("schema written", "path", out)
	return nil
}

func writeFile(path string, schema *hclload.Schema) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return hclload.Write(f, schema)
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

func sortedMapKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func strOrNone(s *string) string {
	if s == nil {
		return "(none)"
	}
	return *s
}

func engineKind(e *hclload.EngineSpec) string {
	if e == nil {
		return "(none)"
	}
	return e.Kind
}

func engineKindName(e hclload.Engine) string {
	if e == nil {
		return "(none)"
	}
	return e.Kind()
}
