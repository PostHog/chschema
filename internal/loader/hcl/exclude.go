package hcl

import (
	"fmt"
	"path/filepath"

	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclparse"
)

// ExcludeMatcher decides whether an object should be skipped during
// introspection. It holds a list of glob patterns (filepath.Match syntax)
// matched against both the bare object name and its database-qualified
// "<database>.<name>" form, so patterns like "tmp_*" or "posthog.*_backup" both
// work. A nil matcher excludes nothing.
type ExcludeMatcher struct {
	patterns []string
}

// excludeFile is the on-disk config: a single `exclude { patterns = [...] }`
// block.
type excludeFile struct {
	Exclude *struct {
		Patterns []string `hcl:"patterns"`
	} `hcl:"exclude,block"`
}

// LoadExcludeConfig parses an HCL exclude config:
//
//	exclude {
//	  patterns = ["tmp_*", "_tmp_replace_*", "*_backup", ...]
//	}
//
// Patterns are validated as globs at load time. A file with no exclude block
// (or no patterns) yields a matcher that excludes nothing.
func LoadExcludeConfig(path string) (*ExcludeMatcher, error) {
	parser := hclparse.NewParser()
	f, diags := parser.ParseHCLFile(path)
	if diags.HasErrors() {
		return nil, fmt.Errorf("%s", diags)
	}
	var cfg excludeFile
	if diags := gohcl.DecodeBody(f.Body, nil, &cfg); diags.HasErrors() {
		return nil, fmt.Errorf("%s", diags)
	}

	m := &ExcludeMatcher{}
	if cfg.Exclude != nil {
		m.patterns = cfg.Exclude.Patterns
	}
	for _, p := range m.patterns {
		if _, err := filepath.Match(p, ""); err != nil {
			return nil, fmt.Errorf("invalid exclude pattern %q: %w", p, err)
		}
	}
	return m, nil
}

// NewExcludeMatcher builds a matcher from explicit patterns (used by tests).
func NewExcludeMatcher(patterns ...string) *ExcludeMatcher {
	return &ExcludeMatcher{patterns: patterns}
}

// Match reports whether an object is excluded and, if so, the pattern that
// matched (for logging). It tries each pattern against the bare name and the
// "<database>.<name>" qualified form.
func (m *ExcludeMatcher) Match(database, name string) (pattern string, ok bool) {
	if m == nil {
		return "", false
	}
	qualified := database + "." + name
	for _, p := range m.patterns {
		if matched, _ := filepath.Match(p, name); matched {
			return p, true
		}
		if matched, _ := filepath.Match(p, qualified); matched {
			return p, true
		}
	}
	return "", false
}

// Matches reports whether an object is excluded.
func (m *ExcludeMatcher) Matches(database, name string) bool {
	_, ok := m.Match(database, name)
	return ok
}

// Empty reports whether the matcher has no patterns (so it excludes nothing).
func (m *ExcludeMatcher) Empty() bool {
	return m == nil || len(m.patterns) == 0
}
