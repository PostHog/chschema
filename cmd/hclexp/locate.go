package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

// locateStack is one (role, env) deployment from the manifest and its
// declared layer stack.
type locateStack struct {
	Role   string
	Env    string
	Layers []string
}

type locatePlacement struct {
	Role string `json:"role"`
	Env  string `json:"env"`
}

// locateDecl is one declaration site plus its derived placements: the
// (role, env) stacks whose layer lists include the declaring layer.
type locateDecl struct {
	File       string            `json:"file"`
	Line       int               `json:"line"`
	Layer      string            `json:"layer,omitempty"`
	Type       string            `json:"type"`
	Abstract   bool              `json:"abstract,omitempty"`
	Override   bool              `json:"override,omitempty"`
	Patch      bool              `json:"patch,omitempty"`
	Extends    string            `json:"extends,omitempty"`
	RawKind    string            `json:"raw_kind,omitempty"`
	Placements []locatePlacement `json:"placements,omitempty"`
}

type locateDump struct {
	File string `json:"file"`
	Line int    `json:"line"`
	Node string `json:"node,omitempty"`
	Type string `json:"type"`
}

type locateModelRef struct {
	Source string   `json:"source"`
	File   string   `json:"file,omitempty"`
	Node   string   `json:"node,omitempty"`
	Role   string   `json:"role,omitempty"`
	Env    string   `json:"env,omitempty"`
	Layers []string `json:"layers,omitempty"`
}

type locateColumn struct {
	Database string           `json:"database"`
	Table    string           `json:"table"`
	Name     string           `json:"column"`
	Models   []locateModelRef `json:"models"`
}

// locateColumnDoc is deliberately separate from the existing object-query
// document, keeping its JSON contract unchanged. Columns is always encoded,
// including as [] when a valid selector query finds nothing.
type locateColumnDoc struct {
	TablePatterns  []string       `json:"table_patterns"`
	ColumnPatterns []string       `json:"column_patterns"`
	Columns        []locateColumn `json:"columns"`
}

// locateObject collects every declaration site of one object name. Objects
// are keyed by (database, name) — the namespace ClickHouse object types
// share — so a table and a raw block with the same name land in one entry,
// with Types recording each block type seen. ExtendedBy lists the objects
// whose extend attribute names this one, whether or not they matched the
// query themselves.
type locateObject struct {
	Database     string       `json:"database,omitempty"`
	Name         string       `json:"name"`
	Types        []string     `json:"types"`
	ExtendedBy   []string     `json:"extended_by,omitempty"`
	Declarations []locateDecl `json:"declarations"`
	Dumps        []locateDump `json:"dumps,omitempty"`
}

// locateDoc is the `locate -format json` document. Objects carries the
// pattern query's results; Duplicates carries -duplicates mode's. Exactly
// one of the two is populated (non-nil, so JSON emits [] rather than null).
type locateDoc struct {
	Patterns   []string       `json:"patterns,omitempty"`
	Objects    []locateObject `json:"objects,omitempty"`
	Duplicates []locateObject `json:"duplicates,omitempty"`
}

// locateFlagsError reports the usage error in a locate invocation, if any.
// Pure so the exit-2 paths are testable without a subprocess.
func locateFlagsError(manifest, layers, dump, format string, duplicates bool, patterns, tables, columns []string) error {
	if format != "text" && format != "json" {
		return fmt.Errorf("invalid -format %q (want text or json)", format)
	}
	columnMode := len(tables) > 0 || len(columns) > 0
	if duplicates {
		if columnMode {
			return fmt.Errorf("-duplicates cannot be combined with -tables or -columns")
		}
		if manifest == "" && layers == "" {
			return fmt.Errorf("-duplicates requires -manifest or -layer (it audits authored layers)")
		}
		if dump != "" {
			return fmt.Errorf("-duplicates and -dump are mutually exclusive")
		}
		if len(patterns) != 0 {
			return fmt.Errorf("-duplicates takes no name argument")
		}
		return nil
	}
	if manifest == "" && layers == "" && dump == "" {
		return fmt.Errorf("at least one of -manifest, -layer, or -dump is required")
	}
	if columnMode {
		if len(patterns) != 0 {
			return fmt.Errorf("-tables/-columns cannot be combined with name arguments")
		}
		if len(tables) == 0 {
			return fmt.Errorf("-columns requires -tables")
		}
		if len(columns) == 0 {
			return fmt.Errorf("-tables requires -columns")
		}
		if err := validateLocatePatterns("-tables", tables); err != nil {
			return err
		}
		return validateLocatePatterns("-columns", columns)
	}
	if len(patterns) == 0 {
		return fmt.Errorf("at least one <name-or-glob> argument is required")
	}
	return validateLocatePatterns("name", patterns)
}

func validateLocatePatterns(kind string, patterns []string) error {
	for _, pattern := range patterns {
		if _, err := filepath.Match(pattern, ""); err != nil {
			if kind == "name" {
				return fmt.Errorf("invalid pattern %q: %w", pattern, err)
			}
			return fmt.Errorf("invalid %s pattern %q: %w", kind, pattern, err)
		}
	}
	return nil
}

// parseManifestAllEnvs decodes the manifest into one locateStack per
// (role, env) pair across every environment — unlike parseManifest, which
// selects a single env — with the same duplicate-role/env checks.
func parseManifestAllEnvs(path string) ([]locateStack, error) {
	m, err := decodeManifest(path)
	if err != nil {
		return nil, err
	}
	if len(m.Roles) == 0 {
		return nil, fmt.Errorf("manifest declares no roles")
	}
	var stacks []locateStack
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
			if len(eb.Layers) == 0 {
				return nil, fmt.Errorf("role %q env %q: layers is empty", rb.Name, eb.Name)
			}
			stacks = append(stacks, locateStack{Role: rb.Name, Env: eb.Name, Layers: eb.Layers})
		}
	}
	return stacks, nil
}

// buildLocateDoc scans every layer the manifest references (each unique
// resolved layer once), the ad-hoc extraLayers (deduped against the
// manifest's, without placements), and the dump directory, and groups the
// matching declaration sites by (database, name). Objects match when any
// pattern matches; the second return value lists the patterns that matched
// nothing (the per-pattern existence check exits non-zero on those). With
// duplicates = true the patterns are ignored and the doc's Duplicates side
// is populated instead.
func buildLocateDoc(stacks []locateStack, layerRoot string, extraLayers []string, dumpDir string, patterns []string, duplicates bool) (locateDoc, []string, error) {
	// Index which (role, env) stacks include each resolved layer, keeping
	// first-seen layer order so output is stable.
	stacksByLayer, layerOrder := indexLocateLayers(stacks, layerRoot, extraLayers)

	// Scan each file once; a file reachable through several layers (e.g. a
	// dir layer and the same file listed directly) keeps its first
	// attribution.
	var decls []hclload.Declaration
	layerByFile := map[string]string{}
	for _, layer := range layerOrder {
		files, err := hclload.LayerFiles(layer)
		if err != nil {
			return locateDoc{}, nil, err
		}
		for _, file := range files {
			if _, ok := layerByFile[file]; ok {
				continue
			}
			layerByFile[file] = layer
			fileDecls, err := hclload.ScanDeclarations([]string{file})
			if err != nil {
				return locateDoc{}, nil, err
			}
			decls = append(decls, fileDecls...)
		}
	}

	// Reverse extend edges over every authored declaration — not just the
	// matching ones — so a parent reports its children even when the
	// children don't match the query.
	extendedBy := extendedByIndex(decls)

	if duplicates {
		doc := locateDoc{Duplicates: []locateObject{}}
		for _, g := range hclload.FindDuplicates(decls) {
			obj := locateObject{Database: g.Database, Name: g.Name, ExtendedBy: extendedBy[[2]string{g.Database, g.Name}]}
			for _, d := range g.Declarations {
				obj.Types = appendUniqueString(obj.Types, d.ObjectType)
				obj.Declarations = append(obj.Declarations, toLocateDecl(d, layerByFile, stacksByLayer))
			}
			doc.Duplicates = append(doc.Duplicates, obj)
		}
		return doc, nil, nil
	}

	doc := locateDoc{Patterns: patterns, Objects: []locateObject{}}
	hits := make([]bool, len(patterns))
	type key struct{ db, name string }
	index := map[key]int{}
	upsert := func(db, name string) *locateObject {
		k := key{db, name}
		if i, ok := index[k]; ok {
			return &doc.Objects[i]
		}
		index[k] = len(doc.Objects)
		doc.Objects = append(doc.Objects, locateObject{Database: db, Name: name, Declarations: []locateDecl{}})
		return &doc.Objects[len(doc.Objects)-1]
	}

	for _, d := range decls {
		if !matchesAnyPattern(patterns, hits, d.Database, d.Name) {
			continue
		}
		obj := upsert(d.Database, d.Name)
		obj.Types = appendUniqueString(obj.Types, d.ObjectType)
		obj.Declarations = append(obj.Declarations, toLocateDecl(d, layerByFile, stacksByLayer))
	}

	if dumpDir != "" {
		files, err := filepath.Glob(filepath.Join(dumpDir, "*.hcl"))
		if err != nil {
			return locateDoc{}, nil, fmt.Errorf("dump dir %q: %w", dumpDir, err)
		}
		sort.Strings(files)
		for _, file := range files {
			dumpDecls, node, err := hclload.ScanFileDeclarations(file)
			if err != nil {
				return locateDoc{}, nil, err
			}
			if node == "" {
				// The filename stem, same as drift's fallback identity.
				node = strings.TrimSuffix(filepath.Base(file), ".hcl")
			}
			for _, d := range dumpDecls {
				if !matchesAnyPattern(patterns, hits, d.Database, d.Name) {
					continue
				}
				obj := upsert(d.Database, d.Name)
				obj.Types = appendUniqueString(obj.Types, d.ObjectType)
				obj.Dumps = append(obj.Dumps, locateDump{File: d.File, Line: d.Line, Node: node, Type: d.ObjectType})
			}
		}
	}

	for i := range doc.Objects {
		o := &doc.Objects[i]
		o.ExtendedBy = extendedBy[[2]string{o.Database, o.Name}]
	}

	sort.SliceStable(doc.Objects, func(i, j int) bool {
		if doc.Objects[i].Database != doc.Objects[j].Database {
			return doc.Objects[i].Database < doc.Objects[j].Database
		}
		return doc.Objects[i].Name < doc.Objects[j].Name
	})

	var unmatched []string
	for i, p := range patterns {
		if !hits[i] {
			unmatched = append(unmatched, p)
		}
	}
	return doc, unmatched, nil
}

func indexLocateLayers(stacks []locateStack, layerRoot string, extraLayers []string) (map[string][]locatePlacement, []string) {
	stacksByLayer := map[string][]locatePlacement{}
	var layerOrder []string
	for _, s := range stacks {
		for _, l := range s.Layers {
			resolved := filepath.Join(layerRoot, l)
			if _, ok := stacksByLayer[resolved]; !ok {
				layerOrder = append(layerOrder, resolved)
			}
			stacksByLayer[resolved] = appendUniquePlacement(stacksByLayer[resolved], locatePlacement{Role: s.Role, Env: s.Env})
		}
	}
	// Ad-hoc -layer entries scan after the manifest's layers. They resolve
	// as given (not under -layer-root) and carry no placements.
	for _, l := range extraLayers {
		resolved := filepath.Clean(l)
		if _, ok := stacksByLayer[resolved]; ok {
			continue
		}
		stacksByLayer[resolved] = nil
		layerOrder = append(layerOrder, resolved)
	}
	return stacksByLayer, layerOrder
}

type locateModel struct {
	Ref    locateModelRef
	Schema *hclload.Schema
}

type locateModelTask struct {
	Ref      locateModelRef
	Resolved []string
	Dump     bool
}

const locateLoadParallelism = 32

func buildLocateColumnDoc(stacks []locateStack, layerRoot string, extraLayers []string, dumpDir string, tablePatterns, columnPatterns []string) (locateColumnDoc, error) {
	doc := locateColumnDoc{
		TablePatterns:  tablePatterns,
		ColumnPatterns: columnPatterns,
		Columns:        []locateColumn{},
	}
	type key struct{ db, table, column string }
	index := map[key]int{}
	upsert := func(database, table, column string) *locateColumn {
		k := key{database, table, column}
		if i, ok := index[k]; ok {
			return &doc.Columns[i]
		}
		index[k] = len(doc.Columns)
		doc.Columns = append(doc.Columns, locateColumn{
			Database: database, Table: table, Name: column, Models: []locateModelRef{},
		})
		return &doc.Columns[len(doc.Columns)-1]
	}

	models, err := loadLocateModels(stacks, layerRoot, extraLayers, dumpDir)
	if err != nil {
		return locateColumnDoc{}, err
	}
	for _, model := range models {
		for _, database := range model.Schema.Databases {
			for _, table := range database.Tables {
				if !matchesTablePatterns(tablePatterns, database.Name, table.Name) {
					continue
				}
				for _, column := range table.Columns {
					if !matchesColumnPatterns(columnPatterns, database.Name, table.Name, column.Name) {
						continue
					}
					match := upsert(database.Name, table.Name, column.Name)
					match.Models = append(match.Models, model.Ref)
				}
			}
		}
	}

	sort.SliceStable(doc.Columns, func(i, j int) bool {
		left, right := doc.Columns[i], doc.Columns[j]
		if left.Database != right.Database {
			return left.Database < right.Database
		}
		if left.Table != right.Table {
			return left.Table < right.Table
		}
		return left.Name < right.Name
	})
	return doc, nil
}

func loadLocateModels(stacks []locateStack, layerRoot string, extraLayers []string, dumpDir string) ([]locateModel, error) {
	var tasks []locateModelTask
	for _, stack := range stacks {
		resolved := make([]string, len(stack.Layers))
		for i, layer := range stack.Layers {
			resolved[i] = filepath.Join(layerRoot, layer)
		}
		tasks = append(tasks, locateModelTask{
			Ref: locateModelRef{
				Source: "manifest", Role: stack.Role, Env: stack.Env,
				Layers: append([]string(nil), stack.Layers...),
			},
			Resolved: resolved,
		})
	}
	if len(extraLayers) > 0 {
		tasks = append(tasks, locateModelTask{
			Ref:      locateModelRef{Source: "layer", Layers: append([]string(nil), extraLayers...)},
			Resolved: append([]string(nil), extraLayers...),
		})
	}
	if dumpDir != "" {
		entries, err := os.ReadDir(dumpDir)
		if err != nil {
			return nil, fmt.Errorf("read dump dir %q: %w", dumpDir, err)
		}
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".hcl" {
				continue
			}
			file := filepath.Join(dumpDir, entry.Name())
			tasks = append(tasks, locateModelTask{
				Ref:      locateModelRef{Source: "dump", File: file},
				Resolved: []string{file},
				Dump:     true,
			})
		}
	}
	return loadLocateModelTasks(tasks, locateLoadParallelism, loadLocateModel)
}

func loadLocateModel(task locateModelTask) (locateModel, error) {
	var schema *hclload.Schema
	var err error
	if task.Dump {
		schema, err = loadDumpSchema(task.Ref.File)
	} else {
		schema, err = hclload.LoadLayers(task.Resolved)
		if err == nil {
			err = hclload.Resolve(schema)
		}
	}
	if err != nil {
		return locateModel{}, fmt.Errorf("load %s: %w", locateModelLabel(task.Ref), err)
	}
	ref := task.Ref
	if task.Dump {
		ref.Node = strings.TrimSuffix(filepath.Base(ref.File), ".hcl")
		if len(schema.Nodes) > 0 && schema.Nodes[0].Name != "" {
			ref.Node = schema.Nodes[0].Name
		}
	}
	return locateModel{Ref: ref, Schema: schema}, nil
}

func locateModelLabel(ref locateModelRef) string {
	switch ref.Source {
	case "manifest":
		return fmt.Sprintf("manifest model (%s, %s)", ref.Role, ref.Env)
	case "layer":
		return fmt.Sprintf("layer model %v", ref.Layers)
	default:
		return fmt.Sprintf("dump model %s", ref.File)
	}
}

type locateModelLoader func(locateModelTask) (locateModel, error)

// loadLocateModelTasks bounds parallel parsing while preserving task order in
// the result. Thirty dump files therefore load concurrently, but arbitrarily
// large directories cannot create an unbounded number of goroutines.
func loadLocateModelTasks(tasks []locateModelTask, parallelism int, loader locateModelLoader) ([]locateModel, error) {
	if len(tasks) == 0 {
		return []locateModel{}, nil
	}
	if parallelism < 1 {
		parallelism = 1
	}
	if parallelism > len(tasks) {
		parallelism = len(tasks)
	}

	models := make([]locateModel, len(tasks))
	errs := make([]error, len(tasks))
	jobs := make(chan int, len(tasks))
	for i := range tasks {
		jobs <- i
	}
	close(jobs)

	var wg sync.WaitGroup
	wg.Add(parallelism)
	for range parallelism {
		go func() {
			defer wg.Done()
			for i := range jobs {
				models[i], errs[i] = loader(tasks[i])
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return models, nil
}

func matchesTablePatterns(patterns []string, database, table string) bool {
	for _, pattern := range patterns {
		if hclload.MatchesPattern(pattern, database, table) {
			return true
		}
	}
	return false
}

func matchesColumnPatterns(patterns []string, database, table, column string) bool {
	for _, pattern := range patterns {
		if hclload.MatchesColumnPattern(pattern, database, table, column) {
			return true
		}
	}
	return false
}

// matchesAnyPattern reports whether any pattern matches the object and marks
// every pattern that does in hits, so the caller can tell which patterns
// found nothing across the whole scan.
func matchesAnyPattern(patterns []string, hits []bool, database, name string) bool {
	matched := false
	for i, p := range patterns {
		if hclload.MatchesPattern(p, database, name) {
			hits[i] = true
			matched = true
		}
	}
	return matched
}

// extendedByIndex maps each (database, name) to the qualified names of the
// declarations extending it, in scan order.
func extendedByIndex(decls []hclload.Declaration) map[[2]string][]string {
	idx := map[[2]string][]string{}
	for _, d := range decls {
		if d.Extends == "" {
			continue
		}
		k := [2]string{d.Database, d.Extends}
		idx[k] = appendUniqueString(idx[k], qualifiedName(d.Database, d.Name))
	}
	return idx
}

func toLocateDecl(d hclload.Declaration, layerByFile map[string]string, stacksByLayer map[string][]locatePlacement) locateDecl {
	layer := layerByFile[d.File]
	return locateDecl{
		File:       d.File,
		Line:       d.Line,
		Layer:      layer,
		Type:       d.ObjectType,
		Abstract:   d.Abstract,
		Override:   d.Override,
		Patch:      d.Patch,
		Extends:    d.Extends,
		RawKind:    d.RawKind,
		Placements: stacksByLayer[layer],
	}
}

func appendUniquePlacement(ps []locatePlacement, p locatePlacement) []locatePlacement {
	for _, x := range ps {
		if x == p {
			return ps
		}
	}
	return append(ps, p)
}

func appendUniqueString(ss []string, s string) []string {
	for _, x := range ss {
		if x == s {
			return ss
		}
	}
	return append(ss, s)
}

// declMarkers renders a site's control flags for the text output, e.g.
// " [abstract]" or " extends events_base [override]".
func declMarkers(d locateDecl) string {
	var parts []string
	if d.Extends != "" {
		parts = append(parts, "extends "+d.Extends)
	}
	if d.Abstract {
		parts = append(parts, "[abstract]")
	}
	if d.Override {
		parts = append(parts, "[override]")
	}
	if d.Patch {
		patchKind := map[string]string{
			hclload.KindTable:            "patch_table",
			hclload.KindMaterializedView: "patch_materialized_view",
			hclload.KindView:             "patch_view",
			hclload.KindDictionary:       "patch_dictionary",
		}[d.Type]
		parts = append(parts, "["+patchKind+"]")
	}
	if d.RawKind != "" {
		parts = append(parts, "[raw "+d.RawKind+"]")
	}
	if len(parts) == 0 {
		return ""
	}
	return "  " + strings.Join(parts, " ")
}

func formatPlacements(ps []locatePlacement) string {
	parts := make([]string, 0, len(ps))
	for _, p := range ps {
		parts = append(parts, fmt.Sprintf("(%s, %s)", p.Role, p.Env))
	}
	return strings.Join(parts, ", ")
}

func renderLocateSites(w io.Writer, o locateObject) {
	for _, d := range o.Declarations {
		fmt.Fprintf(w, "  %s:%d%s\n", d.File, d.Line, declMarkers(d))
		if len(d.Placements) > 0 {
			fmt.Fprintf(w, "      %s\n", formatPlacements(d.Placements))
		}
	}
	if len(o.ExtendedBy) > 0 {
		fmt.Fprintf(w, "  extended by: %s\n", strings.Join(o.ExtendedBy, ", "))
	}
	for _, dp := range o.Dumps {
		fmt.Fprintf(w, "  dump: %s:%d  (node %s)\n", dp.File, dp.Line, dp.Node)
	}
}

func renderLocateText(w io.Writer, doc locateDoc) {
	for _, o := range doc.Objects {
		fmt.Fprintf(w, "%s %s\n", strings.Join(o.Types, "|"), qualifiedName(o.Database, o.Name))
		renderLocateSites(w, o)
	}
}

func renderLocateColumnText(w io.Writer, doc locateColumnDoc) {
	for _, column := range doc.Columns {
		fmt.Fprintf(w, "column %s.%s.%s\n", column.Database, column.Table, column.Name)
		for _, model := range column.Models {
			switch model.Source {
			case "manifest":
				fmt.Fprintf(w, "  manifest: (%s, %s)  layers: %s\n", model.Role, model.Env, strings.Join(model.Layers, ","))
			case "layer":
				fmt.Fprintf(w, "  layers: %s\n", strings.Join(model.Layers, ","))
			case "dump":
				fmt.Fprintf(w, "  dump: %s  (node %s)\n", model.File, model.Node)
			}
		}
	}
}

func renderDuplicatesText(w io.Writer, doc locateDoc) {
	if len(doc.Duplicates) == 0 {
		fmt.Fprintln(w, "no duplicate declarations")
		return
	}
	for _, o := range doc.Duplicates {
		fmt.Fprintf(w, "duplicate %s %s (%d sites)\n", strings.Join(o.Types, "|"), qualifiedName(o.Database, o.Name), len(o.Declarations))
		renderLocateSites(w, o)
	}
}

// runLocate answers "where is object X declared?" across a manifest's layer
// tree, ad-hoc -layer entries, and/or a dump directory, or (with
// -duplicates) audits the layer tree for objects defined at more than one
// site. Column selector mode loads resolved models before searching and
// returns success with an empty result. Object pattern mode exits 1 when any
// pattern matches nothing; duplicate mode exits 1 when duplicates exist.
// Usage errors exit 2.
func runLocate(args []string) {
	fs := flag.NewFlagSet("hclexp locate", flag.ExitOnError)
	manifestFlag := fs.String("manifest", "", "HCL manifest: object mode scans its layers; column mode resolves every (role, env) stack")
	layerRootFlag := fs.String("layer-root", ".", "root directory the manifest's layer paths resolve under")
	layersFlag := fs.String("layer", "", "comma-separated ad-hoc layer dirs or .hcl files; column mode resolves them in order as one model")
	dumpFlag := fs.String("dump", "", "directory of per-node .hcl dumps; column mode resolves node models concurrently")
	formatFlag := fs.String("format", "text", "output format: text (default) or json")
	duplicatesFlag := fs.Bool("duplicates", false, "list every object defined at more than one site (patch/override/extend sites refine; abstracts define); takes no name argument")
	tablesFlag := fs.String("tables", "", "comma-separated table names or globs for column lookup; requires -columns and no name argument")
	columnsFlag := fs.String("columns", "", "comma-separated column names or globs to locate within -tables")
	_ = fs.Parse(args)

	patterns := fs.Args()
	tablePatterns := splitList(*tablesFlag)
	columnPatterns := splitList(*columnsFlag)
	if err := locateFlagsError(*manifestFlag, *layersFlag, *dumpFlag, *formatFlag, *duplicatesFlag, patterns, tablePatterns, columnPatterns); err != nil {
		slog.Error("invalid locate invocation", "err", err)
		os.Exit(2)
	}
	if *dumpFlag != "" && !isDir(*dumpFlag) {
		slog.Error("dump directory does not exist", "dir", *dumpFlag)
		os.Exit(1)
	}

	var stacks []locateStack
	if *manifestFlag != "" {
		var err error
		stacks, err = parseManifestAllEnvs(*manifestFlag)
		if err != nil {
			slog.Error("failed to parse manifest", "file", *manifestFlag, "err", err)
			os.Exit(1)
		}
	}

	if len(tablePatterns) > 0 || len(columnPatterns) > 0 {
		doc, err := buildLocateColumnDoc(stacks, *layerRootFlag, splitList(*layersFlag), *dumpFlag, tablePatterns, columnPatterns)
		if err != nil {
			slog.Error("locate failed", "err", err)
			os.Exit(1)
		}
		if *formatFlag == "json" {
			out, err := json.MarshalIndent(doc, "", "  ")
			if err != nil {
				slog.Error("failed to render JSON", "err", err)
				os.Exit(1)
			}
			fmt.Println(string(out))
		} else {
			renderLocateColumnText(os.Stdout, doc)
		}
		return
	}

	doc, unmatched, err := buildLocateDoc(stacks, *layerRootFlag, splitList(*layersFlag), *dumpFlag, patterns, *duplicatesFlag)
	if err != nil {
		slog.Error("locate failed", "err", err)
		os.Exit(1)
	}

	if *formatFlag == "json" {
		out, err := json.MarshalIndent(doc, "", "  ")
		if err != nil {
			slog.Error("failed to render JSON", "err", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	} else if *duplicatesFlag {
		renderDuplicatesText(os.Stdout, doc)
	} else {
		renderLocateText(os.Stdout, doc)
	}

	if *duplicatesFlag {
		if len(doc.Duplicates) > 0 {
			os.Exit(1)
		}
		return
	}
	if len(unmatched) > 0 {
		for _, p := range unmatched {
			fmt.Fprintf(os.Stderr, "locate: no objects match %q\n", p)
		}
		os.Exit(1)
	}
}
