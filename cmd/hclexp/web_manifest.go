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
	Title  string
	Base   string // "" — list page links are absolute schema base paths
	Label  string // "" — list page shows the default crumb
	Groups []envGroup
}

// multiServer serves a top-level list of composed schemas, each mounted under
// its own /s/<env>/<role>/ prefix.
type multiServer struct {
	tmpl    *template.Template
	groups  []envGroup
	servers map[string]*webServer // basePath -> server
}

// mux returns this schema's routes (rooted at "/" — mounted under a StripPrefix
// in manifest mode, so handlers parse paths unchanged).
func (s *webServer) mux() *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("/", s.handleIndex)
	m.HandleFunc("/flows", s.handleFlows)
	m.HandleFunc("/db/", s.handleObject)
	return m
}

// schemaBasePath is the URL prefix a composition is mounted under.
func schemaBasePath(env, role string) string {
	return "/s/" + url.PathEscape(env) + "/" + url.PathEscape(role)
}

// buildMultiServer composes every manifest schema and builds a webServer for
// each, plus the top-level list. layerRoot prefixes the manifest's layer paths;
// reloadInterval (when > 0) arms per-schema auto-reload.
func buildMultiServer(comps []composition, layerRoot string, reloadInterval time.Duration) (*multiServer, error) {
	tmpl, err := template.New("layout").ParseFS(webFS, "web/layout.html", "web/schemas.html")
	if err != nil {
		return nil, fmt.Errorf("parse schemas template: %w", err)
	}

	ms := &multiServer{tmpl: tmpl, servers: map[string]*webServer{}}
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

// handleSchemas renders the top-level list of composed schemas.
func (ms *multiServer) handleSchemas(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if err := ms.tmpl.ExecuteTemplate(w, "layout", schemasData{Title: "Schemas", Groups: ms.groups}); err != nil {
		slog.Error("render schemas list", "err", err)
		http.Error(w, "template error", http.StatusInternalServerError)
	}
}

// handler builds the top-level mux: the schema list at /, each schema under its
// /s/<env>/<role>/ prefix, shared static assets, and a /flows redirect (the
// list page has no aggregate flows view).
func (ms *multiServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", ms.handleSchemas)
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
