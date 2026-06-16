package main

import (
	"embed"
	"flag"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strings"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

//go:embed web/layout.html web/index.html web/object.html web/flows.html web/static/*
var webFS embed.FS

// runWeb loads an HCL config (single file or layers), resolves it, and serves a
// read-only web UI for browsing databases and their objects. It mirrors the
// load/resolve flow of the other subcommands; no ClickHouse connection is made.
func runWeb(args []string) {
	flags := flag.NewFlagSet("hclexp web", flag.ExitOnError)
	configFlag := flags.String("config", "./cmd/hclexp/node.conf", "path to a single HCL config file (mutually exclusive with -layer)")
	layersFlag := flags.String("layer", "", "comma-separated list of layer directories (loaded in order)")
	addrFlag := flags.String("addr", ":8080", "address to listen on (host:port)")
	_ = flags.Parse(args)

	schema, err := load(*configFlag, *layersFlag)
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}
	if err := hclload.Resolve(schema); err != nil {
		slog.Error("failed to resolve schema", "err", err)
		os.Exit(1)
	}

	srv, err := newWebServer(schema)
	if err != nil {
		slog.Error("failed to build web server", "err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleIndex)
	mux.HandleFunc("/flows", srv.handleFlows)
	mux.HandleFunc("/db/", srv.handleObject)
	staticSub, _ := fs.Sub(webFS, "web/static")
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticSub))))

	slog.Info("serving schema browser", "addr", *addrFlag, "url", "http://localhost"+*addrFlag+"/")
	if err := http.ListenAndServe(*addrFlag, mux); err != nil {
		slog.Error("web server stopped", "err", err)
		os.Exit(1)
	}
}

// webServer holds the resolved schema, parsed templates, and the precomputed
// dependency graph + object kind index used for cross-linking.
type webServer struct {
	schema         *hclload.Schema
	tmplIndex      *template.Template
	tmplObject     *template.Template
	tmplFlows      *template.Template
	deps           []hclload.Dependency
	kindIndex      map[string]string // "db\x00name" -> object kind
	flows          []flow
	flowAnchors    map[string]string        // "db\x00name" -> anchor of the first flow it appears in
	problems       map[string][]problemView // "db\x00name" -> validation problems for that object
	globalProblems []problemView            // problems not attributable to a specific object
}

// problemView is a single validation issue rendered in the UI (e.g. a
// materialized view referencing a column its source table does not provide, or
// a dependency target that is not declared).
type problemView struct {
	Kind        string // friendly category, e.g. "undefined column", "missing target"
	Reason      string // full human-readable explanation from the validator
	Missing     string // the offending reference ("db.name" / "db.column")
	MissingHref string // link to the missing object when it is itself declared
}

func newWebServer(schema *hclload.Schema) (*webServer, error) {
	funcs := template.FuncMap{"dict": templateDict}
	tmplIndex, err := template.New("layout").Funcs(funcs).ParseFS(webFS, "web/layout.html", "web/index.html")
	if err != nil {
		return nil, fmt.Errorf("parse index template: %w", err)
	}
	tmplObject, err := template.New("layout").Funcs(funcs).ParseFS(webFS, "web/layout.html", "web/object.html")
	if err != nil {
		return nil, fmt.Errorf("parse object template: %w", err)
	}
	tmplFlows, err := template.New("layout").Funcs(funcs).ParseFS(webFS, "web/layout.html", "web/flows.html")
	if err != nil {
		return nil, fmt.Errorf("parse flows template: %w", err)
	}

	deps, err := hclload.CollectDependencies(schema.Databases)
	if err != nil {
		// A query that won't parse shouldn't take the whole UI down; browsing
		// still works without cross-links.
		slog.Warn("dependency graph unavailable; object cross-links disabled", "err", err)
		deps = nil
	}

	s := &webServer{
		schema:     schema,
		tmplIndex:  tmplIndex,
		tmplObject: tmplObject,
		tmplFlows:  tmplFlows,
		deps:       deps,
		kindIndex:  map[string]string{},
	}
	for i := range schema.Databases {
		db := &schema.Databases[i]
		for _, t := range db.Tables {
			s.kindIndex[indexKey(db.Name, t.Name)] = hclload.KindTable
		}
		for _, mv := range db.MaterializedViews {
			s.kindIndex[indexKey(db.Name, mv.Name)] = hclload.KindMaterializedView
		}
		for _, v := range db.Views {
			s.kindIndex[indexKey(db.Name, v.Name)] = hclload.KindView
		}
		for _, d := range db.Dictionaries {
			s.kindIndex[indexKey(db.Name, d.Name)] = hclload.KindDictionary
		}
		for _, r := range db.Raws {
			s.kindIndex[indexKey(db.Name, r.Name)] = hclload.KindRaw
		}
	}
	s.flows, s.flowAnchors = buildFlows(schema, s.deps, s.kindIndex)
	s.buildProblems()
	return s, nil
}

// buildProblems runs the schema validator and indexes each issue by the object
// it is attributed to, then tags the flow stages whose object has problems.
func (s *webServer) buildProblems() {
	s.problems = map[string][]problemView{}
	for _, e := range hclload.Validate(s.schema.Databases, hclload.ParseSkipSet("")) {
		pv := problemView{
			Kind:    problemKind(e.Kind),
			Reason:  e.Reason,
			Missing: e.Missing.String(),
		}
		if k := s.kindIndex[indexKey(e.Missing.Database, e.Missing.Name)]; k != "" {
			pv.MissingHref = objectHref(e.Missing.Database, k, e.Missing.Name)
		}
		if e.Object.Database == "" && e.Object.Name == "" {
			s.globalProblems = append(s.globalProblems, pv)
			continue
		}
		key := indexKey(e.Object.Database, e.Object.Name)
		s.problems[key] = append(s.problems[key], pv)
	}

	for fi := range s.flows {
		for si := range s.flows[fi].Stages {
			st := &s.flows[fi].Stages[si]
			if p := s.problems[indexKey(st.Database, st.Name)]; len(p) > 0 {
				st.Problems = p
				s.flows[fi].HasProblems = true
			}
		}
	}
}

// problemKind maps a validator dependency kind to a short UI label.
func problemKind(kind string) string {
	switch kind {
	case hclload.KindMVColumn:
		return "undefined column"
	case hclload.DepMVSource, hclload.DepViewSource:
		return "missing source"
	case hclload.DepMVDest, hclload.DepBufferDestination, hclload.DepTimeSeriesTarget:
		return "missing target"
	case hclload.DepDistributedRemote:
		return "missing remote table"
	default:
		return "problem"
	}
}

func indexKey(db, name string) string { return db + "\x00" + name }

// templateDict builds a map from alternating key/value args, letting templates
// pass multiple named values to a sub-template (used by the "objgroup" block).
func templateDict(pairs ...any) (map[string]any, error) {
	if len(pairs)%2 != 0 {
		return nil, fmt.Errorf("dict: odd number of arguments")
	}
	m := make(map[string]any, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		k, ok := pairs[i].(string)
		if !ok {
			return nil, fmt.Errorf("dict: key %d is not a string", i)
		}
		m[k] = pairs[i+1]
	}
	return m, nil
}

// ---- view models ----

type kv struct{ K, V string }

type tableSection struct {
	Title       string
	Headers     []string
	Rows        [][]string
	Collapsible bool // long column lists can be collapsed to the first 10 rows
}

type objLink struct {
	Kind  string
	Name  string
	Label string // human label (e.g. "db.name" for deps)
	Href  string
}

type dbView struct {
	Name              string
	Cluster           string
	Tables            []objLink
	MaterializedViews []objLink
	Views             []objLink
	Dictionaries      []objLink
	Raws              []objLink
}

type indexData struct {
	Title            string
	Databases        []dbView
	NamedCollections []string
	Nodes            []string
}

type objectData struct {
	Title      string
	Database   string
	Kind       string
	KindLabel  string
	Name       string
	View       string // "html" | "ddl" | "hcl"
	HTMLHref   string
	DDLHref    string
	HCLHref    string
	Props      []kv
	Tables     []tableSection
	Query      string
	RawSQL     string
	Code       string // rendered DDL or HCL for those views
	FlowAnchor string // anchor on /flows when this object participates in a flow
	Problems   []problemView
	DependsOn  []objLink
	DependedBy []objLink
}

// columnCollapseLimit is the row count above which a collapsible column list is
// folded by default; it must match the LIMIT constant in web/static/app.js.
const columnCollapseLimit = 10

// ---- handlers ----

func (s *webServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		s.notFound(w)
		return
	}
	data := indexData{Title: "Schema"}
	for i := range s.schema.Databases {
		db := &s.schema.Databases[i]
		dv := dbView{Name: db.Name}
		if db.Cluster != nil {
			dv.Cluster = *db.Cluster
		}
		for _, t := range db.Tables {
			dv.Tables = append(dv.Tables, s.objLink(db.Name, hclload.KindTable, t.Name))
		}
		for _, mv := range db.MaterializedViews {
			dv.MaterializedViews = append(dv.MaterializedViews, s.objLink(db.Name, hclload.KindMaterializedView, mv.Name))
		}
		for _, v := range db.Views {
			dv.Views = append(dv.Views, s.objLink(db.Name, hclload.KindView, v.Name))
		}
		for _, d := range db.Dictionaries {
			dv.Dictionaries = append(dv.Dictionaries, s.objLink(db.Name, hclload.KindDictionary, d.Name))
		}
		for _, raw := range db.Raws {
			dv.Raws = append(dv.Raws, s.objLink(db.Name, hclload.KindRaw, raw.Name))
		}
		data.Databases = append(data.Databases, dv)
	}
	for _, nc := range s.schema.NamedCollections {
		data.NamedCollections = append(data.NamedCollections, nc.Name)
	}
	for _, n := range s.schema.Nodes {
		data.Nodes = append(data.Nodes, n.Name)
	}
	s.render(w, s.tmplIndex, data)
}

func (s *webServer) handleObject(w http.ResponseWriter, r *http.Request) {
	// Path: /db/{database}/{kind}/{name}
	rest := strings.TrimPrefix(r.URL.Path, "/db/")
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		s.notFound(w)
		return
	}
	database := unescape(parts[0])
	kind := unescape(parts[1])
	name := unescape(parts[2])

	db := s.findDatabase(database)
	if db == nil {
		s.notFound(w)
		return
	}

	view := r.URL.Query().Get("view")
	if view != "ddl" && view != "hcl" {
		view = "html"
	}

	data := objectData{
		Title:     name,
		Database:  database,
		Kind:      kind,
		KindLabel: kindLabel(kind),
		Name:      name,
		View:      view,
		HTMLHref:  objectHref(database, kind, name) + "?view=html",
		DDLHref:   objectHref(database, kind, name) + "?view=ddl",
		HCLHref:   objectHref(database, kind, name) + "?view=hcl",
	}

	switch view {
	case "ddl":
		code, err := hclload.RenderObjectSQL(database, kind, name, db)
		if err != nil {
			s.notFound(w)
			return
		}
		data.Code = code
	case "hcl":
		code, err := hclload.RenderObjectHCL(database, kind, name, db)
		if err != nil {
			s.notFound(w)
			return
		}
		data.Code = code
	default:
		if !s.buildHTMLView(&data, db, kind, name) {
			s.notFound(w)
			return
		}
	}

	data.FlowAnchor = s.flowAnchors[indexKey(database, name)]
	data.Problems = s.problems[indexKey(database, name)]
	s.buildDeps(&data, database, name)
	s.render(w, s.tmplObject, data)
}

// buildHTMLView fills the structured (simplified) view for an object. It returns
// false if no object of that kind/name exists.
func (s *webServer) buildHTMLView(data *objectData, db *hclload.DatabaseSpec, kind, name string) bool {
	switch kind {
	case hclload.KindTable:
		t := findTable(db, name)
		if t == nil {
			return false
		}
		data.Props = tableProps(*t)
		data.Tables = append(data.Tables, columnsSection(t.Columns))
		if sec, ok := indexesSection(t.Indexes); ok {
			data.Tables = append(data.Tables, sec)
		}
		if sec, ok := constraintsSection(t.Constraints); ok {
			data.Tables = append(data.Tables, sec)
		}
	case hclload.KindMaterializedView:
		mv := findMaterializedView(db, name)
		if mv == nil {
			return false
		}
		data.Props = mvProps(*mv)
		if len(mv.Columns) > 0 {
			data.Tables = append(data.Tables, columnsSection(mv.Columns))
		}
		data.Query = mv.Query
	case hclload.KindView:
		v := findView(db, name)
		if v == nil {
			return false
		}
		data.Props = viewProps(*v)
		data.Query = v.Query
	case hclload.KindDictionary:
		d := findDictionary(db, name)
		if d == nil {
			return false
		}
		data.Props = dictProps(*d)
		data.Tables = append(data.Tables, dictAttributesSection(d.Attributes))
	case hclload.KindRaw:
		r := findRaw(db, name)
		if r == nil {
			return false
		}
		data.Props = []kv{{"kind", r.Kind}}
		data.RawSQL = r.SQL
	default:
		return false
	}
	return true
}

func (s *webServer) buildDeps(data *objectData, database, name string) {
	self := hclload.ObjectRef{Database: database, Name: name}
	seenOut := map[string]bool{}
	seenIn := map[string]bool{}
	for _, d := range s.deps {
		if d.From == self {
			ref := d.To
			key := ref.String() + "|" + d.Kind
			if !seenOut[key] {
				seenOut[key] = true
				data.DependsOn = append(data.DependsOn, s.depLink(ref, d.Kind))
			}
		}
		if d.To == self {
			ref := d.From
			key := ref.String() + "|" + d.Kind
			if !seenIn[key] {
				seenIn[key] = true
				data.DependedBy = append(data.DependedBy, s.depLink(ref, d.Kind))
			}
		}
	}
}

func (s *webServer) depLink(ref hclload.ObjectRef, depKind string) objLink {
	kind := s.kindIndex[indexKey(ref.Database, ref.Name)]
	if kind == "" {
		kind = hclload.KindTable
	}
	return objLink{
		Kind:  kind,
		Name:  ref.Name,
		Label: ref.String() + " (" + depKind + ")",
		Href:  objectHref(ref.Database, kind, ref.Name),
	}
}

func (s *webServer) objLink(database, kind, name string) objLink {
	return objLink{Kind: kind, Name: name, Label: name, Href: objectHref(database, kind, name)}
}

func (s *webServer) findDatabase(name string) *hclload.DatabaseSpec {
	for i := range s.schema.Databases {
		if s.schema.Databases[i].Name == name {
			return &s.schema.Databases[i]
		}
	}
	return nil
}

func (s *webServer) render(w http.ResponseWriter, t *template.Template, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.ExecuteTemplate(w, "layout", data); err != nil {
		slog.Error("template execution failed", "err", err)
	}
}

func (s *webServer) notFound(w http.ResponseWriter) {
	http.Error(w, "not found", http.StatusNotFound)
}

// ---- property/section builders ----

func tableProps(t hclload.TableSpec) []kv {
	props := engineProps(t.Engine)
	props = appendList(props, "primary_key", t.PrimaryKey)
	props = appendList(props, "order_by", t.OrderBy)
	props = appendPtr(props, "partition_by", t.PartitionBy)
	props = appendPtr(props, "sample_by", t.SampleBy)
	props = appendPtr(props, "ttl", t.TTL)
	props = appendPtr(props, "cluster", t.Cluster)
	props = appendPtr(props, "comment", t.Comment)
	props = appendMap(props, t.Settings)
	return props
}

func mvProps(mv hclload.MaterializedViewSpec) []kv {
	var props []kv
	if mv.ToTable != "" {
		props = append(props, kv{"to_table", mv.ToTable})
	}
	props = appendPtr(props, "cluster", mv.Cluster)
	props = appendPtr(props, "comment", mv.Comment)
	return props
}

func viewProps(v hclload.ViewSpec) []kv {
	var props []kv
	props = appendList(props, "column_aliases", v.ColumnAliases)
	props = appendPtr(props, "sql_security", v.SQLSecurity)
	props = appendPtr(props, "definer", v.Definer)
	props = appendPtr(props, "cluster", v.Cluster)
	props = appendPtr(props, "comment", v.Comment)
	return props
}

func dictProps(d hclload.DictionarySpec) []kv {
	var props []kv
	props = appendList(props, "primary_key", d.PrimaryKey)
	if d.Source != nil {
		props = append(props, kv{"source", d.Source.Kind})
	}
	if d.Layout != nil {
		props = append(props, kv{"layout", d.Layout.Kind})
	}
	props = appendPtr(props, "cluster", d.Cluster)
	props = appendPtr(props, "comment", d.Comment)
	props = appendMap(props, d.Settings)
	return props
}

func columnsSection(cols []hclload.ColumnSpec) tableSection {
	sec := tableSection{
		Title:       "Columns",
		Headers:     []string{"name", "type", "nullable", "default", "codec", "ttl", "comment"},
		Collapsible: len(cols) > columnCollapseLimit,
	}
	for _, c := range cols {
		def := ""
		switch {
		case c.Default != nil:
			def = "DEFAULT " + *c.Default
		case c.Materialized != nil:
			def = "MATERIALIZED " + *c.Materialized
		case c.Ephemeral != nil:
			def = "EPHEMERAL " + *c.Ephemeral
		case c.Alias != nil:
			def = "ALIAS " + *c.Alias
		}
		nullable := ""
		if c.Nullable {
			nullable = "yes"
		}
		sec.Rows = append(sec.Rows, []string{
			c.Name, c.Type, nullable, def, deref(c.Codec), deref(c.TTL), deref(c.Comment),
		})
	}
	return sec
}

func indexesSection(idx []hclload.IndexSpec) (tableSection, bool) {
	if len(idx) == 0 {
		return tableSection{}, false
	}
	sec := tableSection{Title: "Indexes", Headers: []string{"name", "expr", "type", "granularity"}}
	for _, i := range idx {
		gran := ""
		if i.Granularity != 0 {
			gran = fmt.Sprintf("%d", i.Granularity)
		}
		sec.Rows = append(sec.Rows, []string{i.Name, i.Expr, i.Type, gran})
	}
	return sec, true
}

func constraintsSection(cs []hclload.ConstraintSpec) (tableSection, bool) {
	if len(cs) == 0 {
		return tableSection{}, false
	}
	sec := tableSection{Title: "Constraints", Headers: []string{"name", "kind", "expr"}}
	for _, c := range cs {
		kind, expr := "check", deref(c.Check)
		if c.Assume != nil {
			kind, expr = "assume", *c.Assume
		}
		sec.Rows = append(sec.Rows, []string{c.Name, kind, expr})
	}
	return sec, true
}

func dictAttributesSection(attrs []hclload.DictionaryAttribute) tableSection {
	sec := tableSection{Title: "Attributes", Headers: []string{"name", "type", "default", "expression"}}
	for _, a := range attrs {
		sec.Rows = append(sec.Rows, []string{a.Name, a.Type, deref(a.Default), deref(a.Expression)})
	}
	return sec
}

// engineProps renders an engine spec as Kind plus its decoded struct's non-zero
// exported fields, via reflection, so every engine type is covered uniformly.
func engineProps(spec *hclload.EngineSpec) []kv {
	if spec == nil {
		return nil
	}
	out := []kv{{"engine", spec.Kind}}
	if spec.Decoded == nil {
		return out
	}
	v := reflect.ValueOf(spec.Decoded)
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return out
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return out
	}
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" { // unexported
			continue
		}
		fv := v.Field(i)
		if fv.IsZero() {
			continue
		}
		s := formatValue(fv)
		if s == "" {
			continue
		}
		out = append(out, kv{toSnake(f.Name), s})
	}
	return out
}

func formatValue(v reflect.Value) string {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.String:
		return v.String()
	case reflect.Slice:
		var parts []string
		for i := 0; i < v.Len(); i++ {
			parts = append(parts, formatValue(v.Index(i)))
		}
		return strings.Join(parts, ", ")
	case reflect.Map:
		var parts []string
		for _, k := range v.MapKeys() {
			parts = append(parts, fmt.Sprintf("%v=%v", k.Interface(), v.MapIndex(k).Interface()))
		}
		sort.Strings(parts)
		return strings.Join(parts, ", ")
	default:
		return fmt.Sprintf("%v", v.Interface())
	}
}

func appendPtr(props []kv, key string, p *string) []kv {
	if p != nil && *p != "" {
		return append(props, kv{key, *p})
	}
	return props
}

func appendList(props []kv, key string, list []string) []kv {
	if len(list) > 0 {
		return append(props, kv{key, strings.Join(list, ", ")})
	}
	return props
}

func appendMap(props []kv, m map[string]string) []kv {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		props = append(props, kv{k, m[k]})
	}
	return props
}

// ---- small helpers ----

func findTable(db *hclload.DatabaseSpec, name string) *hclload.TableSpec {
	for i := range db.Tables {
		if db.Tables[i].Name == name {
			return &db.Tables[i]
		}
	}
	return nil
}

func findMaterializedView(db *hclload.DatabaseSpec, name string) *hclload.MaterializedViewSpec {
	for i := range db.MaterializedViews {
		if db.MaterializedViews[i].Name == name {
			return &db.MaterializedViews[i]
		}
	}
	return nil
}

func findView(db *hclload.DatabaseSpec, name string) *hclload.ViewSpec {
	for i := range db.Views {
		if db.Views[i].Name == name {
			return &db.Views[i]
		}
	}
	return nil
}

func findDictionary(db *hclload.DatabaseSpec, name string) *hclload.DictionarySpec {
	for i := range db.Dictionaries {
		if db.Dictionaries[i].Name == name {
			return &db.Dictionaries[i]
		}
	}
	return nil
}

func findRaw(db *hclload.DatabaseSpec, name string) *hclload.RawSpec {
	for i := range db.Raws {
		if db.Raws[i].Name == name {
			return &db.Raws[i]
		}
	}
	return nil
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func objectHref(database, kind, name string) string {
	return "/db/" + url.PathEscape(database) + "/" + url.PathEscape(kind) + "/" + url.PathEscape(name)
}

func unescape(s string) string {
	if u, err := url.PathUnescape(s); err == nil {
		return u
	}
	return s
}

func kindLabel(kind string) string {
	switch kind {
	case hclload.KindTable:
		return "Table"
	case hclload.KindMaterializedView:
		return "Materialized View"
	case hclload.KindView:
		return "View"
	case hclload.KindDictionary:
		return "Dictionary"
	case hclload.KindRaw:
		return "Raw"
	default:
		return kind
	}
}

func toSnake(s string) string {
	var b strings.Builder
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r - 'A' + 'a')
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
