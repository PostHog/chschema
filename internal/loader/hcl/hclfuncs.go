package hcl

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/hashicorp/hcl/v2"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
)

// evalContextForFile builds the HCL evaluation context used when decoding a
// file, exposing helper functions whose paths resolve relative to that file's
// directory. Today that is just file(); keeping the construction in one place
// means every parse path (single file or layer stack) gets the same functions.
func evalContextForFile(path string) *hcl.EvalContext {
	return &hcl.EvalContext{
		Functions: map[string]function.Function{
			"file": fileFunc(filepath.Dir(path)),
		},
	}
}

// fileFunc returns the HCL `file(path)` function: it reads the file at path
// (resolved relative to baseDir when not absolute) and returns its contents as
// a string. It lets long values — most usefully a view/MV query — live in a
// formatted, syntax-highlightable .sql file next to the HCL:
//
//	view "custom_metrics" {
//	  query = file("custom_metrics.sql")
//	}
//
// The loaded SQL is normalized like any other query, so external formatting
// never diffs as drift.
func fileFunc(baseDir string) function.Function {
	return function.New(&function.Spec{
		Params: []function.Parameter{{Name: "path", Type: cty.String}},
		Type:   function.StaticReturnType(cty.String),
		Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
			rel := args[0].AsString()
			p := rel
			if !filepath.IsAbs(p) {
				p = filepath.Join(baseDir, p)
			}
			data, err := os.ReadFile(p)
			if err != nil {
				return cty.NilVal, fmt.Errorf("file(%q): %w", rel, err)
			}
			return cty.StringVal(string(data)), nil
		},
	})
}
