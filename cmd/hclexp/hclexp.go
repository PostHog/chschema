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

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/posthog/chschema/config"
	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

func main() {
	if len(os.Args) <= 1 {
		usage(os.Stdout)
		return
	}

	switch os.Args[1] {
	case "-h", "--help", "help":
		usage(os.Stdout)
		return
	case "introspect":
		runIntrospect(os.Args[2:])
		return
	case "dump-cluster":
		runDumpCluster(os.Args[2:])
		return
	case "diff":
		runDiff(os.Args[2:])
		return
	case "validate":
		runValidate(os.Args[2:])
		return
	case "drift":
		runDrift(os.Args[2:])
		return
	case "sql2hcl":
		runSQL2HCL(os.Args[2:])
		return
	case "load":
		runLoad(os.Args[2:])
		return
	case "web":
		runWeb(os.Args[2:])
		return
	}

	// A flag as the first argument (e.g. -config, -layer) is the
	// backward-compatible way to invoke the default load behavior.
	if strings.HasPrefix(os.Args[1], "-") {
		runLoad(os.Args[1:])
		return
	}

	fmt.Fprintf(os.Stderr, "hclexp: unknown command %q\n\n", os.Args[1])
	usage(os.Stderr)
	os.Exit(2)
}

// usage prints a short description of hclexp and its available subcommands.
func usage(w io.Writer) {
	fmt.Fprint(w, `hclexp manages ClickHouse schemas declaratively from HCL.

Usage:
  hclexp <command> [flags]
  hclexp [flags]            run the default load behavior

Commands:
  introspect   dump a live ClickHouse schema as canonical HCL
  dump-cluster enumerate a cluster's nodes and dump one <host>.hcl per node
  diff         compare two schemas (HCL or live), optionally emit migration DDL
  validate     check that MV and Distributed dependency references resolve
  drift        detect cross-node schema drift across per-node HCL dumps
  sql2hcl      apply SQL DDL edits (CREATE/ALTER/DROP/RENAME) to an HCL schema
  load         parse and resolve an HCL config (default when flags are given)
  web          serve a read-only web UI to browse the resolved schema
  help         print this help

Run "hclexp <command> -h" for command-specific flags.
`)
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
	nodeFlag := fs.String("node", "", "node name for the emitted node{} block; defaults to the server's hostName()")
	secure := fs.Bool("secure", cfg.Secure, "connect to ClickHouse over TLS")
	skipVerify := fs.Bool("tls-skip-verify", cfg.TLSSkipVerify, "skip TLS certificate verification (requires -secure)")
	allowRaw := fs.Bool("allow-raw", false, "capture objects whose CREATE DDL cannot be parsed or expressed as a raw{} block instead of failing")
	_ = fs.Parse(args)

	databases := splitList(*dbFlag)
	if len(databases) == 0 {
		slog.Error("no database specified")
		os.Exit(1)
	}

	cfg.Host, cfg.Port, cfg.User, cfg.Password = *host, *port, *user, *password
	cfg.Database = databases[0] // connection requires a database to bind to
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

	ctx := context.Background()
	schema, err := introspectSchema(ctx, conn, databases, *nodeFlag, *allowRaw)
	if err != nil {
		slog.Error("failed to introspect schema", "err", err)
		os.Exit(1)
	}

	if err := writeIntrospected(*outFlag, schema); err != nil {
		slog.Error("failed to write introspected schema", "out", *outFlag, "err", err)
		os.Exit(1)
	}
}

// introspectSchema runs the full introspection pipeline against an open
// connection — every database in databases, named collections, and the node
// identity — and assembles them into a single *hclload.Schema. The nodeName
// override is passed through to IntrospectNode (empty string makes it use the
// server's hostName()). It is shared by runIntrospect and runDumpCluster.
func introspectSchema(ctx context.Context, conn driver.Conn, databases []string, nodeName string, allowRaw bool) (*hclload.Schema, error) {
	schema := &hclload.Schema{}
	for _, name := range databases {
		spec, err := hclload.Introspect(ctx, conn, name, allowRaw)
		if err != nil {
			return nil, fmt.Errorf("introspect database %q: %w", name, err)
		}
		slog.Info("introspected database", "name", spec.Name,
			"tables", len(spec.Tables), "materialized_views", len(spec.MaterializedViews),
			"dictionaries", len(spec.Dictionaries), "raw", len(spec.Raws))
		schema.Databases = append(schema.Databases, *spec)
	}

	ncs, err := hclload.IntrospectNamedCollections(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("introspect named collections: %w", err)
	}
	schema.NamedCollections = ncs
	slog.Info("introspected named collections", "count", len(schema.NamedCollections))

	node, err := hclload.IntrospectNode(ctx, conn, nodeName)
	if err != nil {
		return nil, fmt.Errorf("introspect node macros: %w", err)
	}
	schema.Nodes = []hclload.NodeSpec{node}
	slog.Info("introspected node", "name", node.Name, "macros", len(node.Macros))

	for _, nc := range schema.NamedCollections {
		if redacted := hclload.RedactedParamKeys(nc); len(redacted) > 0 {
			slog.Warn("named collection has redacted values; diff will skip these params to avoid overwriting real secrets with '[HIDDEN]'",
				"collection", nc.Name, "redacted_keys", redacted,
				"hint", "to unredact, grant displaySecretsInShowAndSelect to this user AND set display_secrets_in_show_and_select = 1 on the server")
		}
	}

	return schema, nil
}

// runDumpCluster connects to one entry host, enumerates every node of a named
// cluster from system.clusters, introspects each node natively, and writes one
// <short-host>.hcl per node into -out-dir. Per-node failures are non-fatal: it
// logs and continues, reporting the failure count at the end. Enumeration runs
// over the native protocol, avoiding the HTTP egress proxy that rejects
// internal private-range IPs.
func runDumpCluster(args []string) {
	cfg := config.GetDefaultConfig()

	fs := flag.NewFlagSet("hclexp dump-cluster", flag.ExitOnError)
	host := fs.String("host", cfg.Host, "ClickHouse entry host to enumerate the cluster from")
	port := fs.Int("port", cfg.Port, "ClickHouse port")
	dbFlag := fs.String("database", cfg.Database, "comma-separated databases to introspect on each node")
	user := fs.String("user", cfg.User, "ClickHouse user")
	password := fs.String("password", cfg.Password, "ClickHouse password")
	clusterFlag := fs.String("cluster", "", "system.clusters cluster name to enumerate (required)")
	outDirFlag := fs.String("out-dir", "", "directory to write one <short-host>.hcl per node (required)")
	secure := fs.Bool("secure", cfg.Secure, "connect to ClickHouse over TLS")
	skipVerify := fs.Bool("tls-skip-verify", cfg.TLSSkipVerify, "skip TLS certificate verification (requires -secure)")
	allowRaw := fs.Bool("allow-raw", false, "capture objects whose CREATE DDL cannot be parsed or expressed as a raw{} block instead of failing the node")
	_ = fs.Parse(args)

	databases := splitList(*dbFlag)
	if len(databases) == 0 {
		slog.Error("no database specified")
		os.Exit(2)
	}
	if *clusterFlag == "" {
		slog.Error("-cluster is required")
		os.Exit(2)
	}
	if *outDirFlag == "" {
		slog.Error("-out-dir is required")
		os.Exit(2)
	}

	cfg.Host, cfg.Port, cfg.User, cfg.Password = *host, *port, *user, *password
	cfg.Database = databases[0] // connection requires a database to bind to
	if err := applyTLSFlags(&cfg, *secure, *skipVerify); err != nil {
		slog.Error("invalid TLS flag combination", "err", err)
		os.Exit(2)
	}

	ctx := context.Background()

	// Enumerate the cluster's nodes from the entry host.
	entry, err := config.NewConnection(cfg)
	if err != nil {
		slog.Error("failed to connect to ClickHouse", "host", cfg.Host, "port", cfg.Port, "err", err)
		os.Exit(1)
	}
	var hosts []string
	err = entry.Select(ctx, &hosts,
		"SELECT DISTINCT host_name FROM system.clusters WHERE cluster = ? ORDER BY host_name", *clusterFlag)
	entry.Close()
	if err != nil {
		slog.Error("failed to enumerate cluster nodes", "cluster", *clusterFlag, "err", err)
		os.Exit(1)
	}
	if len(hosts) == 0 {
		slog.Warn("no hosts in cluster", "cluster", *clusterFlag)
		return
	}
	slog.Info("enumerated cluster nodes", "cluster", *clusterFlag, "count", len(hosts))

	// Reset the directory so decommissioned nodes disappear from the dump.
	if err := os.MkdirAll(*outDirFlag, 0o755); err != nil {
		slog.Error("failed to create out-dir", "out-dir", *outDirFlag, "err", err)
		os.Exit(1)
	}
	stale, err := filepath.Glob(filepath.Join(*outDirFlag, "*.hcl"))
	if err != nil {
		slog.Error("failed to list existing dumps", "out-dir", *outDirFlag, "err", err)
		os.Exit(1)
	}
	for _, p := range stale {
		if err := os.Remove(p); err != nil {
			slog.Error("failed to remove stale dump", "path", p, "err", err)
			os.Exit(1)
		}
	}

	failures := 0
	for _, h := range hosts {
		nodeCfg := cfg
		nodeCfg.Host = h
		if err := dumpNode(ctx, nodeCfg, databases, *outDirFlag, *allowRaw); err != nil {
			slog.Warn("failed to dump node; continuing", "host", h, "err", err)
			failures++
			continue
		}
	}

	slog.Info("cluster dump complete", "cluster", *clusterFlag,
		"nodes", len(hosts), "dumped", len(hosts)-failures, "failed", failures)
}

// dumpNode opens a fresh native connection to one host, introspects the
// requested databases, and writes the whole schema (all databases + named
// collections + the node block) to <out-dir>/<short-host>.hcl.
func dumpNode(ctx context.Context, cfg config.ClickHouseConfig, databases []string, outDir string, allowRaw bool) error {
	conn, err := config.NewConnection(cfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	// Empty node name: let IntrospectNode use the server's own hostName().
	schema, err := introspectSchema(ctx, conn, databases, "", allowRaw)
	if err != nil {
		return err
	}

	path := filepath.Join(outDir, shortHost(cfg.Host)+".hcl")
	if err := writeFile(path, schema); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	slog.Info("node dumped", "host", cfg.Host, "path", path)
	return nil
}

// shortHost returns the first DNS label of host (everything before the first
// '.'), used to name per-node dump files.
func shortHost(host string) string {
	if i := strings.IndexByte(host, '.'); i >= 0 {
		return host[:i]
	}
	return host
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
		// Diff's live side stays strict: an unparseable object surfaces as a
		// diff error rather than being silently captured. Use `introspect
		// -allow-raw` to materialize raw blocks into HCL first.
		spec, err := hclload.Introspect(ctx, conn, name, false)
		if err != nil {
			return nil, fmt.Errorf("introspect %s: %w", name, err)
		}
		schema.Databases = append(schema.Databases, *spec)
	}
	return schema, nil
}

// applyTLSFlags merges the two TLS toggles into cfg, validating that
// -tls-skip-verify is only set together with -secure.
func applyTLSFlags(cfg *config.ClickHouseConfig, secure, skipVerify bool) error {
	if skipVerify && !secure {
		return fmt.Errorf("-tls-skip-verify requires -secure")
	}
	cfg.Secure = secure
	cfg.TLSSkipVerify = skipVerify
	return nil
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

	q := u.Query()
	secure := parseBoolQuery(q.Get("secure"))
	skipVerify := parseBoolQuery(q.Get("skip-verify"))
	if skipVerify && !secure {
		return config.ClickHouseConfig{}, nil, fmt.Errorf("skip-verify requires secure=true in clickhouse:// URL")
	}
	cfg.Secure = secure
	cfg.TLSSkipVerify = skipVerify

	return cfg, databases, nil
}

// parseBoolQuery interprets a URL query value as a boolean. Empty/unknown
// values map to false; "1"/"true"/"yes"/"on" (case-insensitive) map to true.
func parseBoolQuery(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
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
		for _, v := range dc.AddViews {
			fmt.Fprintf(w, "  + view %s\n", v.Name)
		}
		for _, name := range dc.DropViews {
			fmt.Fprintf(w, "  - view %s\n", name)
		}
		for _, vd := range dc.AlterViews {
			fmt.Fprintf(w, "  ~ view %s\n", vd.Name)
			if vd.Recreate {
				fmt.Fprintf(w, "      ! requires recreation (column_aliases / sql_security / definer / cluster changed)\n")
			}
			if vd.QueryChange != nil {
				fmt.Fprintf(w, "      ~ query changed\n")
			}
			if vd.Comment != nil {
				fmt.Fprintf(w, "      ~ comment changed\n")
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
		for _, r := range dc.AddRaws {
			fmt.Fprintf(w, "  + raw %s %s\n", r.Kind, r.Name)
		}
		for _, r := range dc.DropRaws {
			fmt.Fprintf(w, "  - raw %s %s\n", r.Kind, r.Name)
		}
		for _, rc := range dc.AlterRaws {
			if rc.IsUnsafe() {
				fmt.Fprintf(w, "  ~ raw %s %s (recreate) ! UNSAFE: destroys data\n", rc.Kind, rc.Name)
			} else {
				fmt.Fprintf(w, "  ~ raw %s %s (recreate)\n", rc.Kind, rc.Name)
			}
		}
	}

	if len(cs.NamedCollections) > 0 {
		fmt.Fprintln(w, "named_collections")
		for _, ncc := range cs.NamedCollections {
			switch {
			case ncc.Error != "":
				fmt.Fprintf(w, "  ! named_collection %s: %s\n", ncc.Name, ncc.Error)
			case ncc.Recreate:
				fmt.Fprintf(w, "  ~ named_collection %s (recreate: ON CLUSTER changed)\n", ncc.Name)
			case ncc.Add != nil:
				fmt.Fprintf(w, "  + named_collection %s\n", ncc.Name)
			case ncc.Drop:
				fmt.Fprintf(w, "  - named_collection %s\n", ncc.Name)
			default:
				fmt.Fprintf(w, "  ~ named_collection %s\n", ncc.Name)
				for _, p := range ncc.SetParams {
					fmt.Fprintf(w, "      ~ param %s (set)\n", p.Key)
				}
				for _, k := range ncc.DeleteParams {
					fmt.Fprintf(w, "      - param %s\n", k)
				}
				for _, k := range ncc.SkippedRedactedParams {
					fmt.Fprintf(w, "      ? param %s (skipped: value is '[HIDDEN]' on at least one side; grant displaySecretsInShowAndSelect to compare)\n", k)
				}
				if ncc.CommentChange != nil {
					fmt.Fprintln(w, "      ~ comment changed")
				}
			}
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
			// Include node identity in every per-database file so the
			// dump carries its source node's macros regardless of which
			// <db>.hcl a reader opens.
			if err := writeFile(path, &hclload.Schema{Databases: []hclload.DatabaseSpec{db}, Nodes: schema.Nodes}); err != nil {
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
