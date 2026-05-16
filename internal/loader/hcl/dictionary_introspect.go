package hcl

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	chparser "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// buildDictionaryFromCreateSQL parses a CREATE DICTIONARY statement and
// turns it into a DictionarySpec. The thin parse wrapper is exposed mostly
// for unit tests; production code calls buildDictionaryFromCreateDictionary
// directly from the introspect dispatch.
func buildDictionaryFromCreateSQL(createSQL string) (DictionarySpec, error) {
	stmt, err := parseCreateStatement(createSQL)
	if err != nil {
		return DictionarySpec{}, err
	}
	cd, ok := stmt.(*chparser.CreateDictionary)
	if !ok {
		return DictionarySpec{}, errors.New("no CREATE DICTIONARY statement found")
	}
	return buildDictionaryFromCreateDictionary(cd)
}

// buildDictionaryFromCreateDictionary walks a parsed CREATE DICTIONARY AST
// into a DictionarySpec, including the typed Source/Layout values.
func buildDictionaryFromCreateDictionary(cd *chparser.CreateDictionary) (DictionarySpec, error) {
	out := DictionarySpec{}

	if cd.Schema != nil {
		for _, a := range cd.Schema.Attributes {
			if a == nil {
				continue
			}
			attr := DictionaryAttribute{
				Name:         dictIdent(a.Name),
				Type:         formatNode(a.Type),
				Hierarchical: a.Hierarchical,
				Injective:    a.Injective,
				IsObjectID:   a.IsObjectId,
			}
			if a.Default != nil {
				attr.Default = strPtr(formatNode(a.Default))
			}
			if a.Expression != nil {
				attr.Expression = strPtr(formatNode(a.Expression))
			}
			out.Attributes = append(out.Attributes, attr)
		}
	}

	if cd.OnCluster != nil && cd.OnCluster.Expr != nil {
		out.Cluster = strPtr(formatNode(cd.OnCluster.Expr))
	}
	if cd.Comment != nil {
		out.Comment = strPtr(unquoteString(cd.Comment.Literal))
	}

	if cd.Engine == nil {
		return DictionarySpec{}, errors.New("dictionary has no engine clause")
	}
	eng := cd.Engine

	if eng.PrimaryKey != nil && eng.PrimaryKey.Keys != nil {
		for _, k := range eng.PrimaryKey.Keys.Items {
			out.PrimaryKey = append(out.PrimaryKey, flattenTupleExpr(k)...)
		}
	}

	if eng.Lifetime != nil {
		lt := &DictionaryLifetime{}
		switch {
		case eng.Lifetime.Value != nil:
			n := parseInt64Literal(eng.Lifetime.Value)
			lt.Min = &n
		default:
			if eng.Lifetime.Min != nil {
				n := parseInt64Literal(eng.Lifetime.Min)
				lt.Min = &n
			}
			if eng.Lifetime.Max != nil {
				n := parseInt64Literal(eng.Lifetime.Max)
				lt.Max = &n
			}
		}
		out.Lifetime = lt
	}

	if eng.Range != nil {
		out.Range = &DictionaryRange{
			Min: dictIdent(eng.Range.Min),
			Max: dictIdent(eng.Range.Max),
		}
	}

	if eng.Source != nil {
		src, err := buildDictionarySourceFromAST(eng.Source)
		if err != nil {
			return DictionarySpec{}, err
		}
		out.Source = src
	}
	if eng.Layout != nil {
		lay, err := buildDictionaryLayoutFromAST(eng.Layout)
		if err != nil {
			return DictionarySpec{}, err
		}
		out.Layout = lay
	}

	if eng.Settings != nil {
		out.Settings = map[string]string{}
		for _, s := range eng.Settings.Items {
			if s == nil || s.Name == nil {
				continue
			}
			out.Settings[dictIdent(s.Name)] = formatNode(s.Expr)
		}
	}

	return out, nil
}

// dictIdent returns an *Ident's plain Name, stripping any surrounding
// backticks ClickHouse adds for quoted identifiers.
func dictIdent(i *chparser.Ident) string {
	if i == nil {
		return ""
	}
	s := i.Name
	if len(s) >= 2 && s[0] == '`' && s[len(s)-1] == '`' {
		return s[1 : len(s)-1]
	}
	return s
}

// dictArgsMap turns the parser's []*DictionaryArgExpr into a lower-cased
// name → string-value map. String literals lose their surrounding quotes;
// numbers, identifiers, and nested expressions are rendered via formatNode.
func dictArgsMap(args []*chparser.DictionaryArgExpr) map[string]string {
	out := make(map[string]string, len(args))
	for _, a := range args {
		if a == nil || a.Name == nil {
			continue
		}
		key := strings.ToLower(dictIdent(a.Name))
		out[key] = dictArgValueString(a.Value)
	}
	return out
}

func dictArgValueString(v chparser.Expr) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(*chparser.StringLiteral); ok {
		return s.Literal
	}
	return formatNode(v)
}

func optStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func optInt64(s string) *int64 {
	if s == "" {
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return &n
}

func optBool(s string) *bool {
	if s == "" {
		return nil
	}
	switch strings.ToLower(s) {
	case "1", "true":
		v := true
		return &v
	case "0", "false":
		v := false
		return &v
	}
	return nil
}

func parseInt64Literal(n *chparser.NumberLiteral) int64 {
	if n == nil {
		return 0
	}
	v, _ := strconv.ParseInt(n.Literal, 10, 64)
	return v
}

func buildDictionarySourceFromAST(s *chparser.DictionarySourceClause) (*DictionarySourceSpec, error) {
	if s == nil || s.Source == nil {
		return nil, errors.New("dictionary has no source")
	}
	kind := strings.ToLower(s.Source.Name)
	args := dictArgsMap(s.Args)

	var decoded DictionarySource
	switch kind {
	case "clickhouse":
		decoded = SourceClickHouse{
			Host: optStr(args["host"]), Port: optInt64(args["port"]),
			User: optStr(args["user"]), Password: optStr(args["password"]),
			DB: optStr(args["db"]), Table: optStr(args["table"]),
			Query:           optStr(args["query"]),
			InvalidateQuery: optStr(args["invalidate_query"]),
			UpdateField:     optStr(args["update_field"]),
			UpdateLag:       optInt64(args["update_lag"]),
		}
	case "mysql":
		decoded = SourceMySQL{
			Host: optStr(args["host"]), Port: optInt64(args["port"]),
			User: optStr(args["user"]), Password: optStr(args["password"]),
			DB: optStr(args["db"]), Table: optStr(args["table"]),
			Query:           optStr(args["query"]),
			InvalidateQuery: optStr(args["invalidate_query"]),
			UpdateField:     optStr(args["update_field"]),
			UpdateLag:       optInt64(args["update_lag"]),
		}
	case "postgresql":
		decoded = SourcePostgreSQL{
			Host: optStr(args["host"]), Port: optInt64(args["port"]),
			User: optStr(args["user"]), Password: optStr(args["password"]),
			DB: optStr(args["db"]), Table: optStr(args["table"]),
			Query:           optStr(args["query"]),
			InvalidateQuery: optStr(args["invalidate_query"]),
			UpdateField:     optStr(args["update_field"]),
			UpdateLag:       optInt64(args["update_lag"]),
		}
	case "http":
		decoded = SourceHTTP{
			URL: args["url"], Format: args["format"],
			CredentialsUser:     optStr(args["credentials_user"]),
			CredentialsPassword: optStr(args["credentials_password"]),
		}
	case "file":
		decoded = SourceFile{Path: args["path"], Format: args["format"]}
	case "executable":
		decoded = SourceExecutable{
			Command:     args["command"],
			Format:      args["format"],
			ImplicitKey: optBool(args["implicit_key"]),
		}
	case "null":
		decoded = SourceNull{}
	default:
		return nil, fmt.Errorf("unsupported dictionary source kind %q", kind)
	}
	return &DictionarySourceSpec{Kind: kind, Decoded: decoded}, nil
}

func buildDictionaryLayoutFromAST(l *chparser.DictionaryLayoutClause) (*DictionaryLayoutSpec, error) {
	if l == nil || l.Layout == nil {
		return nil, errors.New("dictionary has no layout")
	}
	kind := strings.ToLower(l.Layout.Name)
	args := dictArgsMap(l.Args)

	var decoded DictionaryLayout
	switch kind {
	case "flat":
		decoded = LayoutFlat{}
	case "hashed":
		decoded = LayoutHashed{}
	case "sparse_hashed":
		decoded = LayoutSparseHashed{}
	case "complex_key_sparse_hashed":
		decoded = LayoutComplexKeySparseHashed{}
	case "direct":
		decoded = LayoutDirect{}
	case "complex_key_hashed":
		decoded = LayoutComplexKeyHashed{Preallocate: optInt64(args["preallocate"])}
	case "range_hashed":
		decoded = LayoutRangeHashed{RangeLookupStrategy: optStr(args["range_lookup_strategy"])}
	case "complex_key_range_hashed":
		decoded = LayoutComplexKeyRangeHashed{RangeLookupStrategy: optStr(args["range_lookup_strategy"])}
	case "cache":
		n := optInt64(args["size_in_cells"])
		if n == nil {
			return nil, errors.New("layout cache: missing size_in_cells")
		}
		decoded = LayoutCache{SizeInCells: *n}
	case "ip_trie":
		decoded = LayoutIPTrie{AccessToKeyFromAttributes: optBool(args["access_to_key_from_attributes"])}
	default:
		return nil, fmt.Errorf("unsupported dictionary layout kind %q", kind)
	}
	return &DictionaryLayoutSpec{Kind: kind, Decoded: decoded}, nil
}
