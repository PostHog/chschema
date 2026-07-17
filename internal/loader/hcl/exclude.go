package hcl

import (
	"fmt"
	"path/filepath"

	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclparse"
)

// ExcludeMatcher decides whether an object should be skipped during
// introspection or dropped from a schema before it is compared. It holds a
// list of glob patterns (filepath.Match syntax) matched against both the bare
// object name and its database-qualified "<database>.<name>" form, so patterns
// like "tmp_*" or "posthog.*_backup" both work, plus an optional set of object
// types that are excluded wholesale regardless of name. A nil matcher excludes
// nothing.
type ExcludeMatcher struct {
	patterns    []string
	objectTypes map[string]bool
}

// excludeFile is the on-disk config: a single
// `exclude { patterns = [...] object_types = [...] }` block.
type excludeFile struct {
	Exclude *struct {
		Patterns    []string `hcl:"patterns"`
		ObjectTypes []string `hcl:"object_types,optional"`
	} `hcl:"exclude,block"`
}

// validExcludeObjectTypes are the object_type values `object_types` accepts.
var validExcludeObjectTypes = map[string]bool{
	KindTable: true, KindMaterializedView: true, KindView: true,
	KindDictionary: true, KindRaw: true, KindNamedCollection: true,
}

// LoadExcludeConfig parses an HCL exclude config:
//
//	exclude {
//	  patterns     = ["tmp_*", "_tmp_replace_*", "*_backup", ...]
//	  object_types = ["named_collection"]   # optional: drop a whole class
//	}
//
// Patterns are validated as globs and object types against the known object
// kinds at load time. A file with no exclude block (or no patterns and no
// object types) yields a matcher that excludes nothing.
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
	if cfg.Exclude != nil && len(cfg.Exclude.ObjectTypes) > 0 {
		m.objectTypes = make(map[string]bool, len(cfg.Exclude.ObjectTypes))
		for _, ot := range cfg.Exclude.ObjectTypes {
			if !validExcludeObjectTypes[ot] {
				return nil, fmt.Errorf("invalid exclude object_type %q", ot)
			}
			m.objectTypes[ot] = true
		}
	}
	return m, nil
}

// NewExcludeMatcher builds a matcher from explicit patterns (used by tests).
func NewExcludeMatcher(patterns ...string) *ExcludeMatcher {
	return &ExcludeMatcher{patterns: patterns}
}

// NewExcludeMatcherWithTypes builds a matcher from explicit object types and
// patterns (used by tests and callers that don't load a config file).
func NewExcludeMatcherWithTypes(objectTypes []string, patterns ...string) *ExcludeMatcher {
	m := &ExcludeMatcher{patterns: patterns}
	if len(objectTypes) > 0 {
		m.objectTypes = make(map[string]bool, len(objectTypes))
		for _, ot := range objectTypes {
			m.objectTypes[ot] = true
		}
	}
	return m
}

// MatchesObject reports whether an object is excluded, by its type or by a
// name pattern.
func (m *ExcludeMatcher) MatchesObject(objectType, database, name string) bool {
	if m == nil {
		return false
	}
	if m.objectTypes[objectType] {
		return true
	}
	return m.Matches(database, name)
}

// FilterSchema removes every object the matcher excludes, in place. Both
// sides of a comparison are filtered before Diff so excluded objects appear
// in no output and no count. Databases are kept even when emptied.
func FilterSchema(s *Schema, m *ExcludeMatcher) {
	if s == nil || m.Empty() {
		return
	}
	for di := range s.Databases {
		db := &s.Databases[di]
		db.Tables = filterSlice(db.Tables, func(t TableSpec) bool {
			return m.MatchesObject(KindTable, db.Name, t.Name)
		})
		db.MaterializedViews = filterSlice(db.MaterializedViews, func(v MaterializedViewSpec) bool {
			return m.MatchesObject(KindMaterializedView, db.Name, v.Name)
		})
		db.Views = filterSlice(db.Views, func(v ViewSpec) bool {
			return m.MatchesObject(KindView, db.Name, v.Name)
		})
		db.Dictionaries = filterSlice(db.Dictionaries, func(d DictionarySpec) bool {
			return m.MatchesObject(KindDictionary, db.Name, d.Name)
		})
		db.Raws = filterSlice(db.Raws, func(r RawSpec) bool {
			return m.MatchesObject(KindRaw, db.Name, r.Name)
		})
	}
	s.NamedCollections = filterSlice(s.NamedCollections, func(nc NamedCollectionSpec) bool {
		return m.MatchesObject(KindNamedCollection, "", nc.Name)
	})
}

// SelectSchema keeps only the objects the matcher matches, in place — the
// inverse of FilterSchema, for "emit only these objects" selections (load
// -only). An empty matcher selects nothing to keep... which would erase the
// schema, so it is treated as "no selection" and leaves the schema unchanged;
// callers pass a matcher only when a selector was actually given. Databases
// are kept even when emptied (a split layer still needs the database{}
// wrapper and its cluster default); node blocks are untouched.
func SelectSchema(s *Schema, m *ExcludeMatcher) {
	if s == nil || m.Empty() {
		return
	}
	for di := range s.Databases {
		db := &s.Databases[di]
		db.Tables = filterSlice(db.Tables, func(t TableSpec) bool {
			return !m.MatchesObject(KindTable, db.Name, t.Name)
		})
		db.MaterializedViews = filterSlice(db.MaterializedViews, func(v MaterializedViewSpec) bool {
			return !m.MatchesObject(KindMaterializedView, db.Name, v.Name)
		})
		db.Views = filterSlice(db.Views, func(v ViewSpec) bool {
			return !m.MatchesObject(KindView, db.Name, v.Name)
		})
		db.Dictionaries = filterSlice(db.Dictionaries, func(d DictionarySpec) bool {
			return !m.MatchesObject(KindDictionary, db.Name, d.Name)
		})
		db.Raws = filterSlice(db.Raws, func(r RawSpec) bool {
			return !m.MatchesObject(KindRaw, db.Name, r.Name)
		})
	}
	s.NamedCollections = filterSlice(s.NamedCollections, func(nc NamedCollectionSpec) bool {
		return !m.MatchesObject(KindNamedCollection, "", nc.Name)
	})
}

func filterSlice[T any](in []T, drop func(T) bool) []T {
	out := in[:0]
	for _, v := range in {
		if !drop(v) {
			out = append(out, v)
		}
	}
	return out
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

// Empty reports whether the matcher has no patterns and no object types (so
// it excludes nothing).
func (m *ExcludeMatcher) Empty() bool {
	return m == nil || (len(m.patterns) == 0 && len(m.objectTypes) == 0)
}
