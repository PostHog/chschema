package hcl

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// Declaration is one declaration site of a schema object in an HCL source
// file. It comes from a lightweight syntax scan — no gohcl decode, no
// Resolve — which is what preserves the source position and the raw
// inheritance flags (abstract/override/extend/patch_table) that resolution
// consumes and the resolved specs no longer carry.
type Declaration struct {
	ObjectType string // KindTable, KindMaterializedView, KindView, KindDictionary, KindRaw, KindNamedCollection
	Database   string // empty for named collections (cluster-scoped)
	Name       string
	File       string
	Line       int

	Abstract bool
	Override bool
	Patch    bool   // declared via patch_table (strictly additive)
	Extends  string // the extend = "<parent>" attribute, when present
	RawKind  string // raw blocks only: the kind label
}

// ScanDeclarations records every object declaration site in the given files,
// in file order. Files must be native HCL syntax, same as the loader; a
// parse error aborts the scan.
func ScanDeclarations(files []string) ([]Declaration, error) {
	var out []Declaration
	for _, path := range files {
		decls, _, err := ScanFileDeclarations(path)
		if err != nil {
			return nil, err
		}
		out = append(out, decls...)
	}
	return out, nil
}

// ScanFileDeclarations scans one file and additionally returns the label of
// its first node{} block ("" when the file has none). Per-node dumps carry
// exactly one node block, so the name attributes the file's declarations to
// their host.
func ScanFileDeclarations(path string) ([]Declaration, string, error) {
	parser := hclparse.NewParser()
	f, diags := parser.ParseHCLFile(path)
	if diags.HasErrors() {
		return nil, "", formatDiagnostics(parser, diags)
	}
	body, ok := f.Body.(*hclsyntax.Body)
	if !ok {
		return nil, "", fmt.Errorf("%s: not native HCL syntax", path)
	}
	var out []Declaration
	node := ""
	for _, blk := range body.Blocks {
		switch blk.Type {
		case "database":
			if len(blk.Labels) != 1 {
				continue
			}
			for _, obj := range blk.Body.Blocks {
				if d, ok := objectDeclaration(obj, blk.Labels[0], path); ok {
					out = append(out, d)
				}
			}
		case "named_collection":
			if len(blk.Labels) != 1 {
				continue
			}
			out = append(out, Declaration{
				ObjectType: KindNamedCollection,
				Name:       blk.Labels[0],
				File:       path,
				Line:       blk.DefRange().Start.Line,
				Override:   boolAttr(blk.Body, "override"),
			})
		case "node":
			if node == "" && len(blk.Labels) == 1 {
				node = blk.Labels[0]
			}
		}
	}
	return out, node, nil
}

// objectDeclaration converts one block nested in a database{} into a
// Declaration. Blocks that are not object declarations (node, column,
// unknown types) report ok = false.
func objectDeclaration(blk *hclsyntax.Block, database, path string) (Declaration, bool) {
	d := Declaration{Database: database, File: path, Line: blk.DefRange().Start.Line}
	switch blk.Type {
	case "table":
		d.ObjectType = KindTable
	case "patch_table":
		d.ObjectType = KindTable
		d.Patch = true
	case "materialized_view":
		d.ObjectType = KindMaterializedView
	case "view":
		d.ObjectType = KindView
	case "dictionary":
		d.ObjectType = KindDictionary
	case "raw":
		if len(blk.Labels) != 2 {
			return Declaration{}, false
		}
		d.ObjectType = KindRaw
		d.RawKind = blk.Labels[0]
		d.Name = blk.Labels[1]
		return d, true
	default:
		return Declaration{}, false
	}
	if len(blk.Labels) != 1 {
		return Declaration{}, false
	}
	d.Name = blk.Labels[0]
	d.Abstract = boolAttr(blk.Body, "abstract")
	d.Override = boolAttr(blk.Body, "override")
	d.Extends = stringAttr(blk.Body, "extend")
	return d, true
}

// boolAttr reads a literal boolean attribute. Anything the scan cannot
// evaluate statically reads as unset — the full loader is the authority on
// whether the file actually decodes.
func boolAttr(body *hclsyntax.Body, name string) bool {
	attr, ok := body.Attributes[name]
	if !ok {
		return false
	}
	v, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || v.IsNull() || v.Type() != cty.Bool {
		return false
	}
	return v.True()
}

func stringAttr(body *hclsyntax.Body, name string) string {
	attr, ok := body.Attributes[name]
	if !ok {
		return ""
	}
	v, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || v.IsNull() || v.Type() != cty.String {
		return ""
	}
	return v.AsString()
}

// MatchesPattern reports whether a locate pattern (a filepath.Match glob, or
// a plain name) matches an object, trying the bare name and the qualified
// "<database>.<name>" form — the same convention exclude patterns use.
func MatchesPattern(pattern, database, name string) bool {
	if ok, _ := filepath.Match(pattern, name); ok {
		return true
	}
	if database == "" {
		return false
	}
	ok, _ := filepath.Match(pattern, database+"."+name)
	return ok
}

// DuplicateGroup is one object name declared at more than one site without
// the inheritance system explaining the extras: patch_table sites are
// additive, override = true is a deliberate replacement, and abstract
// declarations are dropped at resolve and never materialize. A group
// qualifies when at least two plain declarations remain.
type DuplicateGroup struct {
	Database     string
	Name         string
	Declarations []Declaration // every site for the name, legitimate ones included
}

// FindDuplicates groups declarations by (database, name) — the ClickHouse
// namespace, which object types share — and returns the groups holding two
// or more plain (non-patch, non-override, non-abstract) declarations,
// sorted by database then name.
func FindDuplicates(decls []Declaration) []DuplicateGroup {
	type key struct{ db, name string }
	byKey := map[key][]Declaration{}
	for _, d := range decls {
		k := key{d.Database, d.Name}
		byKey[k] = append(byKey[k], d)
	}
	var out []DuplicateGroup
	for k, group := range byKey {
		plain := 0
		for _, d := range group {
			if !d.Patch && !d.Override && !d.Abstract {
				plain++
			}
		}
		if plain < 2 {
			continue
		}
		sort.Slice(group, func(i, j int) bool {
			if group[i].File != group[j].File {
				return group[i].File < group[j].File
			}
			return group[i].Line < group[j].Line
		})
		out = append(out, DuplicateGroup{Database: k.db, Name: k.name, Declarations: group})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Database != out[j].Database {
			return out[i].Database < out[j].Database
		}
		return out[i].Name < out[j].Name
	})
	return out
}
