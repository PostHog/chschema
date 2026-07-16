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
func locateFlagsError(manifest, layers, dump, format string, duplicates bool, patterns []string) error {
	if format != "text" && format != "json" {
		return fmt.Errorf("invalid -format %q (want text or json)", format)
	}
	if duplicates {
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
	if len(patterns) == 0 {
		return fmt.Errorf("at least one <name-or-glob> argument is required")
	}
	for _, pattern := range patterns {
		if _, err := filepath.Match(pattern, ""); err != nil {
			return fmt.Errorf("invalid pattern %q: %w", pattern, err)
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
		parts = append(parts, "[patch_table]")
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
// -duplicates) audits the layer tree for objects declared at more than one
// plain site. Read-only; exits 1 when any pattern matches nothing or
// duplicates exist, 2 on usage errors.
func runLocate(args []string) {
	fs := flag.NewFlagSet("hclexp locate", flag.ExitOnError)
	manifestFlag := fs.String("manifest", "", "HCL manifest: role blocks with env blocks; every (role, env) stack is searched")
	layerRootFlag := fs.String("layer-root", ".", "root directory the manifest's layer paths resolve under")
	layersFlag := fs.String("layer", "", "comma-separated ad-hoc layer dirs or .hcl files to search too (or instead of a manifest); no placement info")
	dumpFlag := fs.String("dump", "", "directory of per-node .hcl dumps to search as well")
	formatFlag := fs.String("format", "text", "output format: text (default) or json")
	duplicatesFlag := fs.Bool("duplicates", false, "list every object declared at more than one plain site (override/patch/abstract sites are legitimate); takes no name argument")
	_ = fs.Parse(args)

	patterns := fs.Args()
	if err := locateFlagsError(*manifestFlag, *layersFlag, *dumpFlag, *formatFlag, *duplicatesFlag, patterns); err != nil {
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
