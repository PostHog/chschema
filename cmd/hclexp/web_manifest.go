package main

import (
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclparse"
	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

// composition is one (env, role) the manifest declares, with its layer stack.
type composition struct {
	Env    string
	Role   string
	Layers []string
}

// manifestCompositions decodes the plan manifest (role blocks with nested env
// blocks) and returns one composition per (role, env), optionally filtered to
// envFilter. Same format as `hclexp plan`.
func manifestCompositions(path, envFilter string) ([]composition, error) {
	parser := hclparse.NewParser()
	f, diags := parser.ParseHCLFile(path)
	if diags.HasErrors() {
		return nil, fmt.Errorf("%s", diags)
	}
	var m planManifest
	if diags := gohcl.DecodeBody(f.Body, nil, &m); diags.HasErrors() {
		return nil, fmt.Errorf("%s", diags)
	}
	if len(m.Roles) == 0 {
		return nil, fmt.Errorf("manifest declares no roles")
	}

	var out []composition
	seenRole := map[string]bool{}
	for _, rb := range m.Roles {
		if seenRole[rb.Name] {
			return nil, fmt.Errorf("duplicate role %q", rb.Name)
		}
		seenRole[rb.Name] = true

		seenEnv := map[string]bool{}
		for _, eb := range rb.Envs {
			if seenEnv[eb.Name] {
				return nil, fmt.Errorf("role %q: duplicate env %q", rb.Name, eb.Name)
			}
			seenEnv[eb.Name] = true
			if envFilter != "" && eb.Name != envFilter {
				continue
			}
			if len(eb.Layers) == 0 {
				return nil, fmt.Errorf("role %q env %q: layers is empty", rb.Name, eb.Name)
			}
			out = append(out, composition{Env: eb.Name, Role: rb.Name, Layers: eb.Layers})
		}
	}
	if len(out) == 0 {
		if envFilter != "" {
			return nil, fmt.Errorf("no compositions for env %q", envFilter)
		}
		return nil, fmt.Errorf("manifest declares no env blocks")
	}
	return out, nil
}

// schemaLink is one browsable schema in the top-level list.
type schemaLink struct {
	Env  string
	Role string
	Href string
}

type envGroup struct {
	Env     string
	Schemas []schemaLink
}

type schemasData struct {
	Title          string
	Base           string // "" — list page links are absolute schema base paths
	Label          string // "" — list page shows the default crumb
	Heading        string
	Empty          string
	ObjectDiffHref string
	Groups         []envGroup
}

type dumpObjectFilterView struct {
	Label  string
	Href   string
	Count  int
	Active bool
}

type dumpObjectReviewData struct {
	Title             string
	Base              string
	Label             string
	Query             string
	Status            string
	Showing           int
	Filters           []dumpObjectFilterView
	IgnoreColumnOrder bool
	SettingsReturn    string
	dumpObjectReview
}

// multiServer serves a top-level list of composed schemas, each mounted under
// its own /s/<env>/<role>/ prefix.
type multiServer struct {
	tmplSchemas     *template.Template
	tmplLookup      *template.Template
	tmplObjectDiffs *template.Template
	title           string
	heading         string
	empty           string
	groups          []envGroup
	servers         map[string]*webServer // basePath -> server
	dumpContext     *dumpWebContext       // non-nil only for -dump
}

// mux returns this schema's routes (rooted at "/" — mounted under a StripPrefix
// in manifest mode, so handlers parse paths unchanged).
func (s *webServer) mux() *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("/", s.handleIndex)
	m.HandleFunc("/flows", s.handleFlows)
	m.HandleFunc("/lookup", s.handleLookup)
	m.HandleFunc("/compare", s.handleCompare)
	m.HandleFunc("/db/", s.handleObject)
	return m
}

// schemaBasePath is the URL prefix a composition is mounted under.
func schemaBasePath(env, role string) string {
	return "/s/" + url.PathEscape(env) + "/" + url.PathEscape(role)
}

// nodeBasePath is the URL prefix a per-node dump is mounted under.
func nodeBasePath(node string) string {
	return "/n/" + url.PathEscape(node)
}

func newMultiServer(title, heading, empty string) (*multiServer, error) {
	funcs := template.FuncMap{"dict": templateDict}
	tmplSchemas, err := template.New("layout").Funcs(funcs).ParseFS(webFS, "web/layout.html", "web/schemas.html")
	if err != nil {
		return nil, fmt.Errorf("parse schemas template: %w", err)
	}
	tmplLookup, err := template.New("layout").Funcs(funcs).ParseFS(webFS, "web/layout.html", "web/lookup.html")
	if err != nil {
		return nil, fmt.Errorf("parse lookup template: %w", err)
	}
	tmplObjectDiffs, err := template.New("layout").Funcs(funcs).ParseFS(webFS, "web/layout.html", "web/object_diffs.html")
	if err != nil {
		return nil, fmt.Errorf("parse object diff template: %w", err)
	}
	return &multiServer{
		tmplSchemas:     tmplSchemas,
		tmplLookup:      tmplLookup,
		tmplObjectDiffs: tmplObjectDiffs,
		title:           title,
		heading:         heading,
		empty:           empty,
		servers:         map[string]*webServer{},
	}, nil
}

// buildMultiServer composes every manifest schema and builds a webServer for
// each, plus the top-level list. layerRoot prefixes the manifest's layer paths;
// reloadInterval (when > 0) arms per-schema auto-reload.
func buildMultiServer(comps []composition, layerRoot string, reloadInterval time.Duration) (*multiServer, error) {
	ms, err := newMultiServer("Schemas", "Schemas", "No schemas in this manifest.")
	if err != nil {
		return nil, err
	}

	groups := map[string]*envGroup{}
	var envOrder []string

	for _, c := range comps {
		stack := make([]string, len(c.Layers))
		for i, l := range c.Layers {
			stack[i] = filepath.Join(layerRoot, l)
		}
		layers := strings.Join(stack, ",")
		schema, err := loadSide(layers)
		if err != nil {
			return nil, fmt.Errorf("compose %s/%s: %w", c.Env, c.Role, err)
		}
		srv, err := newWebServer(schema)
		if err != nil {
			return nil, fmt.Errorf("build server %s/%s: %w", c.Env, c.Role, err)
		}
		base := schemaBasePath(c.Env, c.Role)
		srv.basePath = base
		srv.label = c.Env + " / " + c.Role
		if reloadInterval > 0 {
			srv.enableReload("", layers, reloadInterval)
		}
		ms.servers[base] = srv

		if _, ok := groups[c.Env]; !ok {
			groups[c.Env] = &envGroup{Env: c.Env}
			envOrder = append(envOrder, c.Env)
		}
		groups[c.Env].Schemas = append(groups[c.Env].Schemas, schemaLink{Env: c.Env, Role: c.Role, Href: base + "/"})
	}

	sort.Strings(envOrder)
	for _, env := range envOrder {
		g := groups[env]
		sort.Slice(g.Schemas, func(i, j int) bool { return g.Schemas[i].Role < g.Schemas[j].Role })
		ms.groups = append(ms.groups, *g)
	}
	return ms, nil
}

// buildDumpMultiServer loads every selected dump independently and mounts each
// node under /n/<node>/. Nodes are grouped on the list page by their cluster
// macro, falling back to hostClusterRole when old dumps have no cluster macro.
func buildDumpMultiServer(dir, glob string, reloadInterval time.Duration) (*multiServer, error) {
	return buildDumpMultiServerWithOptions(dir, glob, reloadInterval, hclload.DiffOptions{})
}

func buildDumpMultiServerWithOptions(
	dir, glob string,
	reloadInterval time.Duration,
	options hclload.DiffOptions,
) (*multiServer, error) {
	nodes, err := loadDriftNodes(dir, glob)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no .hcl files in %s match %q", dir, glob)
	}
	clusters, mappings, _, err := deriveDumpClusterSet(nodes, nil)
	if err != nil {
		return nil, fmt.Errorf("derive dump clusters: %w", err)
	}
	addInferredDumpClusterAliases(&clusters, mappings, dumpClusterReferences(nodes, hclload.ParseSkipSet("")))
	ms, err := newMultiServer("Dump nodes", "Dump nodes", "No node dumps found.")
	if err != nil {
		return nil, err
	}

	groups := map[string]*envGroup{}
	dumpContext := newDumpWebContext(dumpClusterAliases(mappings))
	dumpContext.defaultDiffOptions = options
	ms.dumpContext = dumpContext
	var groupOrder []string
	for _, node := range nodes {
		base := nodeBasePath(node.Name)
		if _, exists := ms.servers[base]; exists {
			return nil, fmt.Errorf("duplicate node name %q in dump directory", node.Name)
		}
		srv, err := newWebServerWithClusters(node.Schema, clusters)
		if err != nil {
			return nil, fmt.Errorf("build server %s: %w", node.Name, err)
		}
		group := dumpNodeGroup(node)
		srv.basePath = base
		srv.label = group + " / " + node.Name
		identity := dumpNodeIdentity{Cluster: group, RoutingCluster: node.Macros["cluster"], Node: node.Name}
		if err := srv.attachDumpContext(dumpContext, identity); err != nil {
			return nil, fmt.Errorf("index tables for %s: %w", node.Name, err)
		}
		if reloadInterval > 0 {
			srv.enableReload("", node.File, reloadInterval)
		}
		ms.servers[base] = srv

		if _, ok := groups[group]; !ok {
			groups[group] = &envGroup{Env: group}
			groupOrder = append(groupOrder, group)
		}
		groups[group].Schemas = append(groups[group].Schemas, schemaLink{
			Env: group, Role: node.Name, Href: base + "/",
		})
	}
	sort.Strings(groupOrder)
	for _, group := range groupOrder {
		g := groups[group]
		sort.Slice(g.Schemas, func(i, j int) bool { return g.Schemas[i].Role < g.Schemas[j].Role })
		ms.groups = append(ms.groups, *g)
	}
	return ms, nil
}

func dumpNodeGroup(node driftNode) string {
	if cluster := node.Macros["cluster"]; cluster != "" {
		return cluster
	}
	if role := node.Macros["hostClusterRole"]; role != "" {
		return role
	}
	return "nodes"
}

// handleSchemas renders the top-level list of composed schemas.
func (ms *multiServer) handleSchemas(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data := schemasData{Title: ms.title, Heading: ms.heading, Empty: ms.empty, Groups: ms.groups}
	if ms.dumpContext != nil {
		data.ObjectDiffHref = "/object-diffs"
	}
	if err := ms.tmplSchemas.ExecuteTemplate(w, "layout", data); err != nil {
		slog.Error("render schemas list", "err", err)
		http.Error(w, "template error", http.StatusInternalServerError)
	}
}

func objectDiffFilterHref(query, status string) string {
	values := url.Values{}
	if query != "" {
		values.Set("q", query)
	}
	if status != "" {
		values.Set("status", status)
	}
	if encoded := values.Encode(); encoded != "" {
		return "/object-diffs?" + encoded
	}
	return "/object-diffs"
}

// handleObjectDiffs renders a dump-wide, human-reviewable inventory. Schema
// variants come directly from dumpWebContext, the same canonical index used by
// per-object markers and comparison pages.
func (ms *multiServer) handleObjectDiffs(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/object-diffs" || ms.dumpContext == nil {
		http.NotFound(w, r)
		return
	}
	ms.dumpContext.maybeReloadAll()
	options := ms.dumpContext.diffOptions(r)
	review := ms.dumpContext.objectReview(options)
	rawQuery := strings.TrimSpace(r.URL.Query().Get("q"))
	query := strings.ToLower(rawQuery)
	status := r.URL.Query().Get("status")
	switch status {
	case "", "different", "uniform", "partial":
	default:
		status = ""
	}

	objects := make([]dumpObjectReviewView, 0, len(review.Objects))
	for _, object := range review.Objects {
		if query != "" && !strings.Contains(object.searchText, query) {
			continue
		}
		switch status {
		case "different":
			if !object.Different {
				continue
			}
		case "uniform":
			if object.Different {
				continue
			}
		case "partial":
			if !object.Partial {
				continue
			}
		}
		objects = append(objects, object)
	}

	data := dumpObjectReviewData{
		Title:             "Object diff summary",
		Query:             rawQuery,
		Status:            status,
		Showing:           len(objects),
		IgnoreColumnOrder: options.IgnoreColumnOrder,
		SettingsReturn:    r.URL.RequestURI(),
		dumpObjectReview:  review,
	}
	data.Objects = objects
	data.Filters = []dumpObjectFilterView{
		{Label: "All", Href: objectDiffFilterHref(rawQuery, ""), Count: review.TotalObjects, Active: status == ""},
		{Label: "Different", Href: objectDiffFilterHref(rawQuery, "different"), Count: review.DifferentObjects, Active: status == "different"},
		{Label: "Uniform", Href: objectDiffFilterHref(rawQuery, "uniform"), Count: review.UniformObjects, Active: status == "uniform"},
		{Label: "Inconsistent presence", Href: objectDiffFilterHref(rawQuery, "partial"), Count: review.PartialObjects, Active: status == "partial"},
	}
	if err := ms.tmplObjectDiffs.ExecuteTemplate(w, "layout", data); err != nil {
		slog.Error("render object diff summary", "err", err)
		http.Error(w, "template error", http.StatusInternalServerError)
	}
}

func (ms *multiServer) handleComparisonSettings(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/comparison-settings" || ms.dumpContext == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid settings", http.StatusBadRequest)
		return
	}
	value := "0"
	if r.FormValue("ignore_column_order") == "1" {
		value = "1"
	}
	// This cookie is a non-sensitive boolean display/comparison preference,
	// not an authentication token. hclexp web serves plain HTTP by default, so
	// Secure cannot be unconditional without disabling the setting; when the
	// handler is served over TLS, protect it automatically.
	http.SetCookie(w, &http.Cookie{
		Name:     columnOrderCookie,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r.TLS != nil,
	})
	http.Redirect(w, r, ms.comparisonSettingsReturn(r.FormValue("return")), http.StatusSeeOther)
}

// comparisonSettingsReturn accepts only destinations emitted by the two
// settings forms. Backslashes are normalized before parsing because browsers
// may treat them as forward slashes; without that step, `/\attacker.example`
// can look local to net/url but become a scheme-relative external redirect in
// a browser. The returned URL is reconstructed from the parsed local path and
// query rather than returning the form value verbatim.
func (ms *multiServer) comparisonSettingsReturn(raw string) string {
	target := strings.ReplaceAll(raw, "\\", "/")
	parsed, err := url.Parse(target)
	if err != nil || parsed.IsAbs() || parsed.Hostname() != "" ||
		!strings.HasPrefix(parsed.Path, "/") || strings.HasPrefix(parsed.Path, "//") {
		return "/object-diffs"
	}
	allowed := parsed.Path == "/object-diffs"
	if !allowed {
		for base := range ms.servers {
			if parsed.Path == base+"/compare" {
				allowed = true
				break
			}
		}
	}
	if !allowed {
		return "/object-diffs"
	}
	return (&url.URL{Path: parsed.Path, RawQuery: parsed.Query().Encode()}).String()
}

// handleLookup searches every mounted schema and reports which composition or
// node owns each match. Mounted schemas also expose their own local /lookup.
func (ms *multiServer) handleLookup(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/lookup" {
		http.NotFound(w, r)
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	var results []lookupResult
	if query != "" {
		for _, srv := range ms.servers {
			srv.maybeReload()
			srv.mu.RLock()
			results = append(results, srv.lookupResults(query, srv.label)...)
			srv.mu.RUnlock()
		}
		sortLookupResults(results)
	}
	data := lookupData{Title: "Lookup", Query: query, Results: results, ShowSchema: true}
	if err := ms.tmplLookup.ExecuteTemplate(w, "layout", data); err != nil {
		slog.Error("render aggregate lookup", "err", err)
		http.Error(w, "template error", http.StatusInternalServerError)
	}
}

// handler builds the top-level mux: the schema list at /, each schema under its
// /s/<env>/<role>/ prefix, shared static assets, and a /flows redirect (the
// list page has no aggregate flows view).
func (ms *multiServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", ms.handleSchemas)
	mux.HandleFunc("/lookup", ms.handleLookup)
	mux.HandleFunc("/object-diffs", ms.handleObjectDiffs)
	mux.HandleFunc("/comparison-settings", ms.handleComparisonSettings)
	mux.HandleFunc("/flows", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/", http.StatusFound)
	})
	for base, srv := range ms.servers {
		mux.Handle(base+"/", http.StripPrefix(base, srv.mux()))
	}
	staticSub, _ := fs.Sub(webFS, "web/static")
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticSub))))
	return mux
}

// runWebManifest composes every schema in the manifest and serves the
// multi-schema browser.
func runWebManifest(manifestPath, env, layerRoot, addr string, reloadInterval time.Duration) {
	comps, err := manifestCompositions(manifestPath, env)
	if err != nil {
		slog.Error("failed to read manifest", "file", manifestPath, "err", err)
		os.Exit(1)
	}
	ms, err := buildMultiServer(comps, layerRoot, reloadInterval)
	if err != nil {
		slog.Error("failed to build schema browser", "err", err)
		os.Exit(1)
	}
	slog.Info("serving multi-schema browser", "addr", addr, "schemas", len(ms.servers), "url", "http://localhost"+addr+"/")
	if err := http.ListenAndServe(addr, ms.handler()); err != nil {
		slog.Error("web server stopped", "err", err)
		os.Exit(1)
	}
}

// runWebDump serves one independently browsable schema per selected node dump.
func runWebDump(dir, glob, addr string, reloadInterval time.Duration, options hclload.DiffOptions) {
	ms, err := buildDumpMultiServerWithOptions(dir, glob, reloadInterval, options)
	if err != nil {
		slog.Error("failed to build dump browser", "dir", dir, "err", err)
		os.Exit(1)
	}
	slog.Info("serving dump browser", "addr", addr, "nodes", len(ms.servers), "url", "http://localhost"+addr+"/")
	if err := http.ListenAndServe(addr, ms.handler()); err != nil {
		slog.Error("web server stopped", "err", err)
		os.Exit(1)
	}
}
