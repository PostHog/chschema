package hcl

import (
	"fmt"
	"sort"
	"strings"
)

// createDictionarySQL renders a CREATE OR REPLACE DICTIONARY statement.
func createDictionarySQL(database string, d DictionarySpec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE OR REPLACE DICTIONARY %s.%s", database, d.Name)
	if d.Cluster != nil {
		fmt.Fprintf(&b, " ON CLUSTER %s", *d.Cluster)
	}
	attrs := make([]string, len(d.Attributes))
	for i, a := range d.Attributes {
		attrs[i] = dictionaryAttributeSQL(a)
	}
	fmt.Fprintf(&b, " (%s)", strings.Join(attrs, ", "))
	if len(d.PrimaryKey) > 0 {
		fmt.Fprintf(&b, " PRIMARY KEY %s", strings.Join(d.PrimaryKey, ", "))
	}
	if d.Source != nil && d.Source.Decoded != nil {
		fmt.Fprintf(&b, " SOURCE(%s)", sourceSQL(d.Source.Decoded))
	}
	if d.Layout != nil && d.Layout.Decoded != nil {
		fmt.Fprintf(&b, " LAYOUT(%s)", layoutSQL(d.Layout.Decoded))
	}
	if d.Lifetime != nil && !lifetimeForbiddenForLayout(d.Layout) {
		fmt.Fprintf(&b, " LIFETIME(%s)", lifetimeSQL(*d.Lifetime))
	}
	if d.Range != nil {
		fmt.Fprintf(&b, " RANGE(MIN %s MAX %s)", d.Range.Min, d.Range.Max)
	}
	if len(d.Settings) > 0 {
		fmt.Fprintf(&b, " SETTINGS(%s)", formatSettingsList(d.Settings))
	}
	if d.Comment != nil {
		fmt.Fprintf(&b, " COMMENT %s", quoteString(*d.Comment))
	}
	return b.String()
}

func dropDictionarySQL(database, name string) string {
	return fmt.Sprintf("DROP DICTIONARY %s.%s", database, name)
}

func dictionaryAttributeSQL(a DictionaryAttribute) string {
	var b strings.Builder
	fmt.Fprintf(&b, "`%s` %s", a.Name, a.Type)
	if a.Default != nil {
		fmt.Fprintf(&b, " DEFAULT %s", *a.Default)
	}
	if a.Expression != nil {
		fmt.Fprintf(&b, " EXPRESSION %s", *a.Expression)
	}
	if a.Hierarchical {
		b.WriteString(" HIERARCHICAL")
	}
	if a.Injective {
		b.WriteString(" INJECTIVE")
	}
	if a.IsObjectID {
		b.WriteString(" IS_OBJECT_ID")
	}
	return b.String()
}

// sourceSQL renders the inside of SOURCE(...): KIND(ARG val ARG val).
func sourceSQL(s DictionarySource) string {
	switch v := s.(type) {
	case SourceClickHouse:
		return "CLICKHOUSE(" + joinSourceArgs([]sourceArg{
			{"HOST", strVal(v.Host)},
			{"PORT", intVal(v.Port)},
			{"USER", strVal(v.User)},
			{"PASSWORD", strVal(v.Password)},
			{"DB", strVal(v.DB)},
			{"TABLE", strVal(v.Table)},
			{"QUERY", strVal(v.Query)},
			{"INVALIDATE_QUERY", strVal(v.InvalidateQuery)},
			{"UPDATE_FIELD", strVal(v.UpdateField)},
			{"UPDATE_LAG", intVal(v.UpdateLag)},
		}) + ")"
	case SourceMySQL:
		return "MYSQL(" + joinSourceArgs([]sourceArg{
			{"HOST", strVal(v.Host)},
			{"PORT", intVal(v.Port)},
			{"USER", strVal(v.User)},
			{"PASSWORD", strVal(v.Password)},
			{"DB", strVal(v.DB)},
			{"TABLE", strVal(v.Table)},
			{"QUERY", strVal(v.Query)},
			{"INVALIDATE_QUERY", strVal(v.InvalidateQuery)},
			{"UPDATE_FIELD", strVal(v.UpdateField)},
			{"UPDATE_LAG", intVal(v.UpdateLag)},
		}) + ")"
	case SourcePostgreSQL:
		return "POSTGRESQL(" + joinSourceArgs([]sourceArg{
			{"HOST", strVal(v.Host)},
			{"PORT", intVal(v.Port)},
			{"USER", strVal(v.User)},
			{"PASSWORD", strVal(v.Password)},
			{"DB", strVal(v.DB)},
			{"TABLE", strVal(v.Table)},
			{"QUERY", strVal(v.Query)},
			{"INVALIDATE_QUERY", strVal(v.InvalidateQuery)},
			{"UPDATE_FIELD", strVal(v.UpdateField)},
			{"UPDATE_LAG", intVal(v.UpdateLag)},
		}) + ")"
	case SourceHTTP:
		return "HTTP(" + joinSourceArgs([]sourceArg{
			{"URL", strVal(&v.URL)},
			{"FORMAT", strVal(&v.Format)},
			{"CREDENTIALS_USER", strVal(v.CredentialsUser)},
			{"CREDENTIALS_PASSWORD", strVal(v.CredentialsPassword)},
		}) + ")"
	case SourceFile:
		return fmt.Sprintf("FILE(PATH '%s' FORMAT '%s')", v.Path, v.Format)
	case SourceExecutable:
		return "EXECUTABLE(" + joinSourceArgs([]sourceArg{
			{"COMMAND", strVal(&v.Command)},
			{"FORMAT", strVal(&v.Format)},
			{"IMPLICIT_KEY", boolVal(v.ImplicitKey)},
		}) + ")"
	case SourceNull:
		return "NULL()"
	}
	return ""
}

func layoutSQL(l DictionaryLayout) string {
	switch v := l.(type) {
	case LayoutFlat:
		return "FLAT()"
	case LayoutHashed:
		return "HASHED()"
	case LayoutSparseHashed:
		return "SPARSE_HASHED()"
	case LayoutComplexKeySparseHashed:
		return "COMPLEX_KEY_SPARSE_HASHED()"
	case LayoutDirect:
		return "DIRECT()"
	case LayoutComplexKeyHashed:
		if v.Preallocate != nil {
			return fmt.Sprintf("COMPLEX_KEY_HASHED(PREALLOCATE %d)", *v.Preallocate)
		}
		return "COMPLEX_KEY_HASHED()"
	case LayoutRangeHashed:
		if v.RangeLookupStrategy != nil {
			return fmt.Sprintf("RANGE_HASHED(RANGE_LOOKUP_STRATEGY '%s')", *v.RangeLookupStrategy)
		}
		return "RANGE_HASHED()"
	case LayoutComplexKeyRangeHashed:
		if v.RangeLookupStrategy != nil {
			return fmt.Sprintf("COMPLEX_KEY_RANGE_HASHED(RANGE_LOOKUP_STRATEGY '%s')", *v.RangeLookupStrategy)
		}
		return "COMPLEX_KEY_RANGE_HASHED()"
	case LayoutCache:
		return fmt.Sprintf("CACHE(SIZE_IN_CELLS %d)", v.SizeInCells)
	case LayoutIPTrie:
		if v.AccessToKeyFromAttributes != nil {
			return fmt.Sprintf("IP_TRIE(ACCESS_TO_KEY_FROM_ATTRIBUTES %t)", *v.AccessToKeyFromAttributes)
		}
		return "IP_TRIE()"
	}
	return ""
}

// lifetimeForbiddenForLayout reports whether ClickHouse rejects a LIFETIME
// clause on the given layout. DIRECT and COMPLEX_KEY_DIRECT load data on
// every lookup; they have no concept of cached lifetime.
func lifetimeForbiddenForLayout(spec *DictionaryLayoutSpec) bool {
	if spec == nil {
		return false
	}
	switch spec.Kind {
	case "direct", "complex_key_direct":
		return true
	}
	return false
}

func lifetimeSQL(lt DictionaryLifetime) string {
	switch {
	case lt.Min != nil && lt.Max != nil:
		return fmt.Sprintf("MIN %d MAX %d", *lt.Min, *lt.Max)
	case lt.Min != nil:
		return fmt.Sprintf("%d", *lt.Min)
	case lt.Max != nil:
		return fmt.Sprintf("MAX %d", *lt.Max)
	}
	return "0"
}

type sourceArg struct {
	k string
	v string // already SQL-encoded ('foo' or 42), or "" when omitted
}

func joinSourceArgs(args []sourceArg) string {
	parts := make([]string, 0, len(args))
	for _, a := range args {
		if a.v == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s %s", a.k, a.v))
	}
	return strings.Join(parts, " ")
}

func strVal(p *string) string {
	if p == nil {
		return ""
	}
	return "'" + strings.ReplaceAll(*p, "'", "''") + "'"
}

func intVal(p *int64) string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("%d", *p)
}

func boolVal(p *bool) string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("%t", *p)
}

// dictionariesByName returns a name-sorted copy of the slice — used by sqlgen
// to emit statements deterministically.
func dictionariesByName(ds []DictionarySpec) []DictionarySpec {
	out := append([]DictionarySpec(nil), ds...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
