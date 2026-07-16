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
	Type string `json:"type"`
}

// locateObject collects every declaration site of one object name. Objects
// are keyed by (database, name) — the namespace ClickHouse object types
// share — so a table and a raw block with the same name land in one entry,
// with Types recording each block type seen.
type locateObject struct {
	Database     string       `json:"database,omitempty"`
	Name         string       `json:"name"`
	Types        []string     `json:"types"`
	Declarations []locateDecl `json:"declarations"`
	Dumps        []locateDump `json:"dumps,omitempty"`
}

// locateDoc is the `locate -format json` document. Objects carries the
// pattern query's results; Duplicates carries -duplicates mode's. Exactly
// one of the two is populated (non-nil, so JSON emits [] rather than null).
type locateDoc struct {
	Pattern    string         `json:"pattern,omitempty"`
	Objects    []locateObject `json:"objects,omitempty"`
	Duplicates []locateObject `json:"duplicates,omitempty"`
}

// locateFlagsError reports the usage error in a locate invocation, if any.
// Pure so the exit-2 paths are testable without a subprocess.
func locateFlagsError(manifest, dump, format string, duplicates bool, nargs int, pattern string) error {
	if format != "text" && format != "json" {
		return fmt.Errorf("invalid -format %q (want text or json)", format)
	}
	if duplicates {
		if manifest == "" {
			return fmt.Errorf("-duplicates requires -manifest (it audits authored layers)")
		}
		if dump != "" {
			return fmt.Errorf("-duplicates and -dump are mutually exclusive")
		}
		if nargs != 0 {
			return fmt.Errorf("-duplicates takes no name argument")
		}
		return nil
	}
	if manifest == "" && dump == "" {
		return fmt.Errorf("at least one of -manifest or -dump is required")
	}
	if nargs != 1 {
		return fmt.Errorf("exactly one <name-or-glob> argument is required")
	}
	if _, err := filepath.Match(pattern, ""); err != nil {
		return fmt.Errorf("invalid pattern %q: %w", pattern, err)
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
// resolved layer once) plus the dump directory, and groups the matching
// declaration sites by (database, name). With duplicates = true the pattern
// is ignored and the doc's Duplicates side is populated instead.
func buildLocateDoc(stacks []locateStack, layerRoot, dumpDir, pattern string, duplicates bool) (locateDoc, error) {
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

	// Scan each file once; a file reachable through several layers (e.g. a
	// dir layer and the same file listed directly) keeps its first
	// attribution.
	var decls []hclload.Declaration
	layerByFile := map[string]string{}
	for _, layer := range layerOrder {
		files, err := hclload.LayerFiles(layer)
		if err != nil {
			return locateDoc{}, err
		}
		for _, file := range files {
			if _, ok := layerByFile[file]; ok {
				continue
			}
			layerByFile[file] = layer
			fileDecls, err := hclload.ScanDeclarations([]string{file})
			if err != nil {
				return locateDoc{}, err
			}
			decls = append(decls, fileDecls...)
		}
	}

	if duplicates {
		doc := locateDoc{Duplicates: []locateObject{}}
		for _, g := range hclload.FindDuplicates(decls) {
			obj := locateObject{Database: g.Database, Name: g.Name}
			for _, d := range g.Declarations {
				obj.Types = appendUniqueString(obj.Types, d.ObjectType)
				obj.Declarations = append(obj.Declarations, toLocateDecl(d, layerByFile, stacksByLayer))
			}
			doc.Duplicates = append(doc.Duplicates, obj)
		}
		return doc, nil
	}

	doc := locateDoc{Pattern: pattern, Objects: []locateObject{}}
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
		if !hclload.MatchesPattern(pattern, d.Database, d.Name) {
			continue
		}
		obj := upsert(d.Database, d.Name)
		obj.Types = appendUniqueString(obj.Types, d.ObjectType)
		obj.Declarations = append(obj.Declarations, toLocateDecl(d, layerByFile, stacksByLayer))
	}

	if dumpDir != "" {
		files, err := filepath.Glob(filepath.Join(dumpDir, "*.hcl"))
		if err != nil {
			return locateDoc{}, fmt.Errorf("dump dir %q: %w", dumpDir, err)
		}
		sort.Strings(files)
		for _, file := range files {
			dumpDecls, err := hclload.ScanDeclarations([]string{file})
			if err != nil {
				return locateDoc{}, err
			}
			for _, d := range dumpDecls {
				if !hclload.MatchesPattern(pattern, d.Database, d.Name) {
					continue
				}
				obj := upsert(d.Database, d.Name)
				obj.Types = appendUniqueString(obj.Types, d.ObjectType)
				obj.Dumps = append(obj.Dumps, locateDump{File: d.File, Line: d.Line, Type: d.ObjectType})
			}
		}
	}

	sort.SliceStable(doc.Objects, func(i, j int) bool {
		if doc.Objects[i].Database != doc.Objects[j].Database {
			return doc.Objects[i].Database < doc.Objects[j].Database
		}
		return doc.Objects[i].Name < doc.Objects[j].Name
	})
	return doc, nil
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
	for _, dp := range o.Dumps {
		fmt.Fprintf(w, "  dump: %s:%d\n", dp.File, dp.Line)
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
// tree and/or a dump directory, or (with -duplicates) audits the layer tree
// for objects declared at more than one plain site. Read-only; exits 1 when
// nothing matches or duplicates exist, 2 on usage errors.
func runLocate(args []string) {
	fs := flag.NewFlagSet("hclexp locate", flag.ExitOnError)
	manifestFlag := fs.String("manifest", "", "HCL manifest: role blocks with env blocks; every (role, env) stack is searched")
	layerRootFlag := fs.String("layer-root", ".", "root directory the manifest's layer paths resolve under")
	dumpFlag := fs.String("dump", "", "directory of per-node .hcl dumps to search as well")
	formatFlag := fs.String("format", "text", "output format: text (default) or json")
	duplicatesFlag := fs.Bool("duplicates", false, "list every object declared at more than one plain site (override/patch/abstract sites are legitimate); takes no name argument")
	_ = fs.Parse(args)

	pattern := fs.Arg(0)
	if err := locateFlagsError(*manifestFlag, *dumpFlag, *formatFlag, *duplicatesFlag, fs.NArg(), pattern); err != nil {
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

	doc, err := buildLocateDoc(stacks, *layerRootFlag, *dumpFlag, pattern, *duplicatesFlag)
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
	if len(doc.Objects) == 0 {
		fmt.Fprintf(os.Stderr, "locate: no objects match %q\n", pattern)
		os.Exit(1)
	}
}
