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
	Databases []DatabaseSpec `hcl:"database,block"`
}

// ParseFile parses a single HCL file and returns the declared databases.
// Diagnostics are formatted into the returned error.
func ParseFile(path string) ([]DatabaseSpec, error) {
	parser := hclparse.NewParser()
	f, diags := parser.ParseHCLFile(path)
	if diags.HasErrors() {
		return nil, formatDiagnostics(parser, diags)
	}

	var spec fileSpec
	if diags := gohcl.DecodeBody(f.Body, nil, &spec); diags.HasErrors() {
		return nil, formatDiagnostics(parser, diags)
	}

	for di := range spec.Databases {
		db := &spec.Databases[di]
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
	}
	return spec.Databases, nil
}

func formatDiagnostics(parser *hclparse.Parser, diags hcl.Diagnostics) error {
	var buf bytes.Buffer
	wr := hcl.NewDiagnosticTextWriter(&buf, parser.Files(), 78, false)
	if err := wr.WriteDiagnostics(diags); err != nil {
		return fmt.Errorf("hcl diagnostic: %w (original: %s)", err, diags.Error())
	}
	return errors.New(buf.String())
}
