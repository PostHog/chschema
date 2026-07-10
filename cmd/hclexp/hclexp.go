package main

import (
	"context"
	"errors"
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
	case "plan":
		runPlan(os.Args[2:])
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
	case "dump-sql":
		runDumpSQL(os.Args[2:])
		return
	case "github-token":
		runGitHubToken(os.Args[2:])
		return
	case "version", "-version", "--version":
		runVersion(os.Stdout)
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
  dump-sql     dump a database's CREATE statements as ClickHouse DDL (replayable seed)
  diff         compare two schemas (HCL or live), optionally emit migration DDL
  plan         diff every role in a manifest against a topology dump, emitting a
               single globally-ordered, cross-role operation list
  validate     check that MV and Distributed dependency references resolve
  drift        detect cross-node schema drift across per-node HCL dumps
  sql2hcl      apply SQL DDL edits (CREATE/ALTER/DROP/RENAME) to an HCL schema
  load         parse and resolve an HCL config, layer stack, or manifest role
               (default when flags are given)
  web          serve a read-only web UI to browse the resolved schema
  github-token mint a short-lived GitHub App installation token (prints to stdout)
  version      print the hclexp build version, commit and build time
  help         print this help

Run "hclexp <command> -h" for command-specific flags.
`)
}

// clusterFlag collects repeatable -cluster NAME=STACK mappings. STACK is a
// list of layer directories joined by the OS list separator (':' on unix), so
// it never clashes with the comma that separates -layer dirs. The sentinel
// STACK == "@absent" declares a cluster with no composition in this env.
type clusterFlag struct {
	entries []clusterEntry
}

type clusterEntry struct {
	name  string
	stack string // OS-list-separated dirs, or "@absent"
}

func (c *clusterFlag) String() string {
	parts := make([]string, 0, len(c.entries))
	for _, e := range c.entries {
		parts = append(parts, e.name+"="+e.stack)
	}
	return strings.Join(parts, ",")
}

func (c *clusterFlag) Set(v string) error {
	i := strings.IndexByte(v, '=')
	if i <= 0 {
		return fmt.Errorf("invalid -cluster %q: want NAME=STACK", v)
	}
	c.entries = append(c.entries, clusterEntry{name: v[:i], stack: v[i+1:]})
	return nil
}

// absentStack is the sentinel STACK value marking a cluster with no local
// composition; references into it validate as satisfied. aliasPrefix marks an
// alias mapping (@alias=BASE): the cluster shares BASE's composition.
const (
	absentStack = "@absent"
	aliasPrefix = "@alias="
)

// buildClusterSet loads and resolves each -cluster mapping into its own schema
// and assembles a ClusterSet. An @absent stack registers the cluster without a
// composition; an @alias=BASE stack points the cluster at BASE's composition.
func buildClusterSet(entries []clusterEntry) (hclload.ClusterSet, error) {
	cs := hclload.NewClusterSet()
	err := applyClusterEntries(&cs, entries)
	return cs, err
}

// applyClusterEntries adds each -cluster flag entry to cs. Entries are applied
// in order, so a later entry (or a flag applied after a manifest) overrides an
// earlier mapping for the same name.
func applyClusterEntries(cs *hclload.ClusterSet, entries []clusterEntry) error {
	for _, e := range entries {
		switch {
		case e.stack == absentStack:
			cs.AddAbsent(e.name)
			continue
		case strings.HasPrefix(e.stack, aliasPrefix):
			base := strings.TrimPrefix(e.stack, aliasPrefix)
			if base == "" {
				return fmt.Errorf("cluster %q: %s requires a base cluster name", e.name, aliasPrefix)
			}
			cs.AddAlias(e.name, base)
			continue
		}
		dirs := filepath.SplitList(e.stack)
		schema, err := hclload.LoadLayers(dirs)
		if err != nil {
			return fmt.Errorf("cluster %q: loading %v: %w", e.name, dirs, err)
		}
		if err := hclload.Resolve(schema); err != nil {
			return fmt.Errorf("cluster %q: resolving %v: %w", e.name, dirs, err)
		}
		cs.Add(e.name, schema.Databases)
	}
	return nil
}

// buildManifestClusters adds cluster mappings derived from the role manifest to
// cs. A ClickHouse cluster is composed of nodes from one or more roles, so each
// cluster block's schema is the union of its member roles' compositions for env
// (layer paths resolved under layerRoot). Roles not deployed in env are skipped.
// Each cluster's aliases map to @alias=cluster.
func buildManifestClusters(cs *hclload.ClusterSet, path, env, layerRoot string) error {
	roles, err := parseManifest(path, env)
	if err != nil {
		return err
	}
	clusters, err := parseManifestClusters(path)
	if err != nil {
		return err
	}
	// Load only the roles some cluster references (single-node mode needs the
	// mappings, not every role's schema).
	referenced := map[string]bool{}
	for _, cl := range clusters {
		for _, role := range cl.Roles {
			referenced[role] = true
		}
	}
	needed := make([]manifestRole, 0, len(referenced))
	for _, r := range roles {
		if referenced[r.Role] {
			needed = append(needed, r)
		}
	}
	schemas, err := loadManifestRoleSchemas(needed, layerRoot)
	if err != nil {
		return err
	}
	clusterSetFromRoles(cs, clusters, schemas)
	return nil
}

// composedRole is one role's manifest-declared layer stack and the resolved
// schema that stack composes to. Resolved holds Layers joined under the
// -layer-root, i.e. the dirs actually handed to the loader.
type composedRole struct {
	Role     string
	Layers   []string
	Resolved []string
	Schema   *hclload.Schema
}

// composeManifestRoles loads and resolves each role's composition for the
// selected env (layer paths under layerRoot), preserving manifest order.
func composeManifestRoles(roles []manifestRole, layerRoot string) ([]composedRole, error) {
	composed := make([]composedRole, 0, len(roles))
	for _, r := range roles {
		dirs := make([]string, len(r.Layers))
		for i, l := range r.Layers {
			dirs[i] = filepath.Join(layerRoot, l)
		}
		schema, err := hclload.LoadLayers(dirs)
		if err != nil {
			return nil, fmt.Errorf("role %q: loading %v: %w", r.Role, dirs, err)
		}
		if err := hclload.Resolve(schema); err != nil {
			return nil, fmt.Errorf("role %q: resolving %v: %w", r.Role, dirs, err)
		}
		composed = append(composed, composedRole{
			Role:     r.Role,
			Layers:   r.Layers,
			Resolved: dirs,
			Schema:   schema,
		})
	}
	return composed, nil
}

// loadManifestRoleSchemas loads and resolves each role's composition for the
// selected env (layer paths under layerRoot) into a schema, keyed by role name.
func loadManifestRoleSchemas(roles []manifestRole, layerRoot string) (map[string]*hclload.Schema, error) {
	composed, err := composeManifestRoles(roles, layerRoot)
	if err != nil {
		return nil, err
	}
	schemas := make(map[string]*hclload.Schema, len(composed))
	for _, c := range composed {
		schemas[c.Role] = c.Schema
	}
	return schemas, nil
}

// clusterSetFromRoles registers each cluster's mapping in cs: its schema is the
// union of its member roles' (preloaded) compositions, and each alias maps to
// @alias=cluster. A member role absent from schemas (not deployed in the env)
// contributes nothing.
func clusterSetFromRoles(cs *hclload.ClusterSet, clusters []manifestClusterBlock, schemas map[string]*hclload.Schema) {
	for _, cl := range clusters {
		var dbs []hclload.DatabaseSpec
		if !cl.Absent {
			for _, role := range cl.Roles {
				if s, ok := schemas[role]; ok {
					dbs = append(dbs, s.Databases...)
				}
			}
		}
		// Explicitly absent, or no member role composes this cluster in the
		// selected env: it is modeled elsewhere (env-varying / external), so
		// treat it as absent — proxies into it are satisfied rather than
		// failing against an empty schema.
		if cl.Absent || len(dbs) == 0 {
			cs.AddAbsent(cl.Name)
		} else {
			cs.Add(cl.Name, dbs)
		}
		for _, alias := range cl.Aliases {
			cs.AddAlias(alias, cl.Name)
		}
	}
}

// roleValidation holds the validation errors for one role's composition.
type roleValidation struct {
	Role string
	Errs []hclload.ValidationError
}

// validateManifest validates the manifest's role compositions for env, each
// against the cluster set derived from the whole manifest (member-role unions
// + aliases, with flagEntries applied last). A non-empty role narrows which
// roles are validated, not which compose the cluster set: a single role's
// Distributed proxies still have to resolve against the other roles' storage
// tables. Results are returned per role in manifest order.
func validateManifest(path, env, layerRoot, role string, skip hclload.SkipSet, opts hclload.ValidateOptions, flagEntries []clusterEntry) ([]roleValidation, error) {
	roles, err := parseManifest(path, env)
	if err != nil {
		return nil, err
	}
	clusters, err := parseManifestClusters(path)
	if err != nil {
		return nil, err
	}
	schemas, err := loadManifestRoleSchemas(roles, layerRoot)
	if err != nil {
		return nil, err
	}

	cs := hclload.NewClusterSet()
	clusterSetFromRoles(&cs, clusters, schemas)
	if err := applyClusterEntries(&cs, flagEntries); err != nil {
		return nil, err
	}

	selected, err := filterManifestRoles(roles, role, env)
	if err != nil {
		return nil, err
	}
	results := make([]roleValidation, 0, len(selected))
	for _, r := range selected {
		errs := hclload.ValidateOpts(schemas[r.Role].Databases, skip, cs, opts)
		results = append(results, roleValidation{Role: r.Role, Errs: errs})
	}
	return results, nil
}

// runValidate loads an HCL config (single file or layers), resolves it, and
// checks that every materialized view's source/destination tables and every
// Distributed table's remote table are declared in the loaded schema. It
// exits non-zero when any dependency is unsatisfied. The -skip-validation
// flag takes a comma-separated list of dependent object names (or "*" for
// all) whose dependency checks should be skipped.
//
// A Distributed proxy often forwards to a storage table on another cluster's
// composition. Map those clusters with repeatable -cluster NAME=STACK flags so
// their remotes resolve against the mapped schema; use NAME=@absent for a
// cluster with no local composition, or NAME=@alias=BASE for a remote_servers
// alias that shares BASE's composition. With no -cluster mapping, an off-node
// remote errors (add a mapping, mark it @absent, or -skip-validation it).
//
// Once a remote resolves, the proxy's columns are checked against it: every
// proxy column must exist on the remote with a matching type. -strict-proxy-columns
// additionally requires the remote's columns to all exist on the proxy.
//
// Instead of listing every cluster as a -cluster flag, -manifest/-env derive the
// mappings from the same role manifest `plan` uses: each `cluster` block maps to
// the union of its member roles' composed layer stacks for -env. Explicit
// -cluster flags are applied last, so they override or extend the manifest
// (e.g. NAME=@absent).
//
// With -manifest/-env and no explicit node (-layer/-config), validate runs in
// manifest-driven mode: it validates every role in the manifest, each against
// the cluster set derived from the whole manifest — one command to check a
// whole environment.
func runValidate(args []string) {
	fs := flag.NewFlagSet("hclexp validate", flag.ExitOnError)
	configFlag := fs.String("config", "./cmd/hclexp/node.conf", "path to a single HCL config file (mutually exclusive with -layer)")
	layersFlag := fs.String("layer", "", "comma-separated list of layer directories (loaded in order)")
	skipFlag := fs.String("skip-validation", "", "comma-separated dependent object names to skip, or \"*\" for all")
	strictProxyCols := fs.Bool("strict-proxy-columns", false, "require Distributed proxy and remote to have exactly the same columns (default: proxy columns need only be a subset)")
	strictClusters := fs.Bool("strict-clusters", false, "require every Distributed remote to resolve against a real composition; a remote on an @absent cluster is an error")
	manifestFlag := fs.String("manifest", "", "HCL role manifest to derive -cluster mappings from; requires -env. With no -layer/-config, validates every role in the manifest")
	envFlag := fs.String("env", "", "environment selecting each role's layer stack in -manifest")
	roleFlag := fs.String("role", "", "in manifest-driven mode, validate only this role (clusters are still derived from the whole manifest)")
	layerRootFlag := fs.String("layer-root", ".", "root directory the manifest's layer paths resolve under")
	var clusters clusterFlag
	fs.Var(&clusters, "cluster", "repeatable NAME=STACK external cluster mapping for Distributed remotes; STACK is OS-list-separated (':') layer dirs, @absent, or @alias=BASE")
	_ = fs.Parse(args)

	if (*manifestFlag == "") != (*envFlag == "") {
		slog.Error("-manifest and -env must be used together")
		os.Exit(2)
	}
	if *roleFlag != "" && *manifestFlag == "" {
		slog.Error("-role requires -manifest")
		os.Exit(2)
	}

	// Manifest-driven mode: -manifest/-env with no explicit node (-layer or
	// -config) validates every role in the manifest (or just -role), each
	// against the clusters derived from the whole manifest.
	if *manifestFlag != "" && *layersFlag == "" && !flagWasSet(fs, "config") {
		runValidateManifest(*manifestFlag, *envFlag, *layerRootFlag, *roleFlag,
			hclload.ParseSkipSet(*skipFlag),
			hclload.ValidateOptions{StrictProxyColumns: *strictProxyCols, StrictClusters: *strictClusters},
			clusters.entries)
		return
	}
	if *roleFlag != "" {
		slog.Error("-role requires manifest-driven mode (-manifest/-env without -layer/-config)")
		os.Exit(2)
	}

	schema, err := load(*configFlag, *layersFlag)
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}
	if err := hclload.Resolve(schema); err != nil {
		slog.Error("failed to resolve schema", "err", err)
		os.Exit(1)
	}

	// Manifest-derived cluster mappings first; explicit -cluster flags applied
	// last so they override or extend them (e.g. NAME=@absent).
	clusterSet := hclload.NewClusterSet()
	if *manifestFlag != "" {
		if err := buildManifestClusters(&clusterSet, *manifestFlag, *envFlag, *layerRootFlag); err != nil {
			slog.Error("failed to derive clusters from manifest", "file", *manifestFlag, "env", *envFlag, "err", err)
			os.Exit(1)
		}
	}
	if err := applyClusterEntries(&clusterSet, clusters.entries); err != nil {
		slog.Error("failed to load cluster mapping", "err", err)
		os.Exit(1)
	}

	errs := hclload.ValidateOpts(schema.Databases, hclload.ParseSkipSet(*skipFlag), clusterSet,
		hclload.ValidateOptions{StrictProxyColumns: *strictProxyCols, StrictClusters: *strictClusters})
	if len(errs) > 0 {
		for _, e := range errs {
			fmt.Fprintf(os.Stderr, "validation error: %s\n", e.Error())
		}
		slog.Error("schema validation failed", "errors", len(errs))
		os.Exit(1)
	}

	slog.Info("schema validation passed", "databases", len(schema.Databases))
}

// flagWasSet reports whether the named flag was explicitly provided on the
// command line (as opposed to left at its default).
func flagWasSet(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}

// runValidateManifest validates every role in the manifest for env, each
// against the cluster set derived from the whole manifest. Errors are printed
// per role and it exits non-zero if any role fails.
func runValidateManifest(manifestPath, env, layerRoot, role string, skip hclload.SkipSet, opts hclload.ValidateOptions, flagEntries []clusterEntry) {
	results, err := validateManifest(manifestPath, env, layerRoot, role, skip, opts, flagEntries)
	if err != nil {
		slog.Error("failed to validate manifest", "file", manifestPath, "env", env, "err", err)
		if errors.Is(err, errUnknownRole) {
			os.Exit(2)
		}
		os.Exit(1)
	}

	total := 0
	for _, r := range results {
		for _, e := range r.Errs {
			fmt.Fprintf(os.Stderr, "validation error: [role %s] %s\n", r.Role, e.Error())
		}
		total += len(r.Errs)
	}
	if total > 0 {
		slog.Error("schema validation failed", "env", env, "roles", len(results), "errors", total)
		os.Exit(1)
	}
	slog.Info("schema validation passed", "env", env, "roles", len(results))
}

// runLoad loads an HCL config (single file, layers, or a role manifest),
// resolves it, and optionally writes the resolved schema back out as
// canonical HCL.
//
// With -manifest/-env the layer stack comes from the same role manifest
// `validate` and `plan` consume, so callers never rebuild it by hand. -role
// selects one role; without it every role deployed in -env is composed and
// -out must name a directory.
func runLoad(args []string) {
	fs := flag.NewFlagSet("hclexp", flag.ExitOnError)
	configFlag := fs.String("config", "./cmd/hclexp/node.conf", "path to a single HCL config file (mutually exclusive with -layer)")
	layersFlag := fs.String("layer", "", "comma-separated list of layer directories (loaded in order)")
	outFlag := fs.String("out", "", "if set, write the resolved schema to this file as canonical HCL ('-' for stdout); a directory in manifest mode")
	manifestFlag := fs.String("manifest", "", "HCL role manifest to compose from; requires -env. Mutually exclusive with -layer/-config")
	envFlag := fs.String("env", "", "environment selecting each role's layer stack in -manifest")
	roleFlag := fs.String("role", "", "compose only this role from -manifest (default: every role deployed in -env)")
	layerRootFlag := fs.String("layer-root", ".", "root directory the manifest's layer paths resolve under")
	formatFlag := fs.String("format", "hcl", "output format: hcl (default) or json (the resolved layer stack per role; requires -manifest)")
	_ = fs.Parse(args)

	if err := loadFlagsError(*manifestFlag, *envFlag, *roleFlag, *formatFlag, *layersFlag, flagWasSet(fs, "config")); err != nil {
		slog.Error("invalid flags", "err", err)
		os.Exit(2)
	}
	if *manifestFlag != "" {
		runLoadManifest(*manifestFlag, *envFlag, *roleFlag, *layerRootFlag, *formatFlag, *outFlag)
		return
	}

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

	if *outFlag == "-" {
		if err := hclload.Write(os.Stdout, schema); err != nil {
			slog.Error("failed to write resolved schema", "err", err)
			os.Exit(1)
		}
	} else if *outFlag != "" {
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
	outFlag := fs.String("out", "", "output .hcl file, a directory (one <db>.hcl per database), or '-'/empty for stdout")
	nodeFlag := fs.String("node", "", "node name for the emitted node{} block; defaults to the server's hostName()")
	secure := fs.Bool("secure", cfg.Secure, "connect to ClickHouse over TLS")
	skipVerify := fs.Bool("tls-skip-verify", cfg.TLSSkipVerify, "skip TLS certificate verification (requires -secure)")
	allowRaw := fs.Bool("allow-raw", false, "capture objects whose CREATE DDL cannot be parsed or expressed as a raw{} block instead of failing")
	excludeFlag := fs.String("exclude", "", "HCL exclude config: objects whose name matches a pattern are skipped (see docs)")
	showSecrets := fs.Bool("show-secrets", false, "capture real secret values (passwords, broker lists) instead of '[HIDDEN]'; requires server display_secrets_in_show_and_select=1 and the displaySecretsInShowAndSelect grant")
	_ = fs.Parse(args)

	databases := splitList(*dbFlag)
	if len(databases) == 0 {
		slog.Error("no database specified")
		os.Exit(1)
	}
	exclude := loadExclude(*excludeFlag)

	cfg.Host, cfg.Port, cfg.User, cfg.Password = *host, *port, *user, *password
	cfg.Database = databases[0] // connection requires a database to bind to
	cfg.ShowSecrets = *showSecrets
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
	schema, err := introspectSchema(ctx, conn, databases, *nodeFlag, *allowRaw, exclude)
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
// loadExclude loads an exclude-pattern config from path, exiting on error. An
// empty path returns a nil matcher (excludes nothing).
func loadExclude(path string) *hclload.ExcludeMatcher {
	if path == "" {
		return nil
	}
	m, err := hclload.LoadExcludeConfig(path)
	if err != nil {
		slog.Error("failed to load -exclude config", "path", path, "err", err)
		os.Exit(1)
	}
	return m
}

func introspectSchema(ctx context.Context, conn driver.Conn, databases []string, nodeName string, allowRaw bool, exclude *hclload.ExcludeMatcher) (*hclload.Schema, error) {
	schema := &hclload.Schema{}
	for _, name := range databases {
		spec, err := hclload.IntrospectWithExclude(ctx, conn, name, allowRaw, exclude)
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
	excludeFlag := fs.String("exclude", "", "HCL exclude config: objects whose name matches a pattern are skipped on every node (see docs)")
	_ = fs.Parse(args)

	databases := splitList(*dbFlag)
	if len(databases) == 0 {
		slog.Error("no database specified")
		os.Exit(2)
	}
	exclude := loadExclude(*excludeFlag)
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
		if err := dumpNode(ctx, nodeCfg, databases, *outDirFlag, *allowRaw, exclude); err != nil {
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
func dumpNode(ctx context.Context, cfg config.ClickHouseConfig, databases []string, outDir string, allowRaw bool, exclude *hclload.ExcludeMatcher) error {
	conn, err := config.NewConnection(cfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	// Empty node name: let IntrospectNode use the server's own hostName().
	schema, err := introspectSchema(ctx, conn, databases, "", allowRaw, exclude)
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
	formatFlag := fs.String("format", "text", "output format: text (default) or json (structured, dependency-ordered operations)")
	_ = fs.Parse(args)

	if *leftFlag == "" || *rightFlag == "" {
		slog.Error("both -left and -right are required")
		os.Exit(2)
	}
	if *formatFlag != "text" && *formatFlag != "json" {
		slog.Error("invalid -format (want text or json)", "format", *formatFlag)
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

	if *formatFlag == "json" {
		gen := hclload.GenerateSQL(cs)
		out, err := hclload.RenderDiffJSON(gen, left, right)
		if err != nil {
			slog.Error("failed to render JSON diff", "err", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
		return
	}

	if *asSQL {
		gen := hclload.GenerateSQL(cs)
		for _, u := range gen.Unsafe {
			fmt.Printf("-- UNSAFE: %s.%s: %s\n", u.Database, u.Table, u.Reason)
		}
		for i, stmt := range gen.Statements {
			if gen.Ops[i].Manual {
				fmt.Println("-- MANUAL: " + stmt + ";")
				continue
			}
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
		unsafe := ""
		if c.IsUnsafe() {
			unsafe = " (UNSAFE)"
		}
		fmt.Fprintf(w, "      ~ column %s: %s -> %s%s\n", c.Name, colDesc(c.Old), colDesc(c.New), unsafe)
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

// colDesc renders a compact one-line column descriptor for the diff summary:
// type plus its default form and any codec/comment/ttl markers.
func colDesc(c hclload.ColumnSpec) string {
	t := c.Type
	if c.Nullable {
		t = "Nullable(" + t + ")"
	}
	switch {
	case c.Alias != nil:
		t += " ALIAS " + *c.Alias
	case c.Materialized != nil:
		t += " MATERIALIZED " + *c.Materialized
	case c.Ephemeral != nil:
		t += " EPHEMERAL"
	case c.Default != nil:
		t += " DEFAULT " + *c.Default
	}
	if c.Codec != nil {
		t += " CODEC(" + *c.Codec + ")"
	}
	if c.TTL != nil {
		t += " TTL " + *c.TTL
	}
	if c.Comment != nil {
		t += " COMMENT " + *c.Comment
	}
	return t
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
// stdoutTarget reports whether an -out value means "write to stdout": either
// unset ("") or the conventional dash ("-").
func stdoutTarget(out string) bool { return out == "" || out == "-" }

func writeIntrospected(out string, schema *hclload.Schema) error {
	if stdoutTarget(out) {
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
