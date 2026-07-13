package hcl

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclparse"
)

type fileSpec struct {
	Databases        []DatabaseSpec        `hcl:"database,block"`
	NamedCollections []NamedCollectionSpec `hcl:"named_collection,block"`
	Nodes            []NodeSpec            `hcl:"node,block"`
}

// ParseFile parses a single HCL file and returns the declared schema.
// Diagnostics are formatted into the returned error.
func ParseFile(path string) (*Schema, error) {
	parser := hclparse.NewParser()
	f, diags := parser.ParseHCLFile(path)
	if diags.HasErrors() {
		return nil, formatDiagnostics(parser, diags)
	}

	var spec fileSpec
	if diags := gohcl.DecodeBody(f.Body, evalContextForFile(path), &spec); diags.HasErrors() {
		return nil, formatDiagnostics(parser, diags)
	}

	for di := range spec.Databases {
		db := &spec.Databases[di]
		canonicalize(db)
		for i := range db.Raws {
			r := &db.Raws[i]
			if !rawKinds[r.Kind] {
				return nil, fmt.Errorf("%s: raw %q has unknown kind %q (want one of table, materialized_view, view, dictionary)", db.Name, r.Name, r.Kind)
			}
			r.SQL = normalizeRawSQL(r.SQL)
		}
		for ti := range db.Tables {
			tbl := &db.Tables[ti]
			if tbl.Engine == nil {
				continue
			}
			decoded, err := DecodeEngine(tbl.Engine)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", db.Name, tbl.Name, err)
			}
			tbl.Engine.Decoded = decoded
		}
		for i := range db.Dictionaries {
			d := &db.Dictionaries[i]
			if d.Source != nil {
				decoded, err := DecodeDictionarySource(d.Source)
				if err != nil {
					return nil, fmt.Errorf("%s.%s: %w", db.Name, d.Name, err)
				}
				d.Source.Decoded = decoded
			}
			if d.Layout != nil {
				decoded, err := DecodeDictionaryLayout(d.Layout)
				if err != nil {
					return nil, fmt.Errorf("%s.%s: %w", db.Name, d.Name, err)
				}
				d.Layout.Decoded = decoded
			}
		}
	}
	return &Schema{
		Databases:        spec.Databases,
		NamedCollections: spec.NamedCollections,
		Nodes:            spec.Nodes,
	}, nil
}

func formatDiagnostics(parser *hclparse.Parser, diags hcl.Diagnostics) error {
	var buf bytes.Buffer
	wr := hcl.NewDiagnosticTextWriter(&buf, parser.Files(), 78, false)
	if err := wr.WriteDiagnostics(diags); err != nil {
		return fmt.Errorf("hcl diagnostic: %w (original: %s)", err, diags.Error())
	}
	return errors.New(buf.String())
}
