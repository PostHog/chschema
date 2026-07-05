package hcl

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	chparser "github.com/orian/clickhouse-sql-parser/parser"
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
			n, err := parseInt64Literal(eng.Lifetime.Value)
			if err != nil {
				return DictionarySpec{}, fmt.Errorf("dictionary lifetime: %w", err)
			}
			lt.Min = &n
		default:
			if eng.Lifetime.Min != nil {
				n, err := parseInt64Literal(eng.Lifetime.Min)
				if err != nil {
					return DictionarySpec{}, fmt.Errorf("dictionary lifetime min: %w", err)
				}
				lt.Min = &n
			}
			if eng.Lifetime.Max != nil {
				n, err := parseInt64Literal(eng.Lifetime.Max)
				if err != nil {
					return DictionarySpec{}, fmt.Errorf("dictionary lifetime max: %w", err)
				}
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
		src, err := buildDictionarySourceFromAST(formatNode(cd.Name), eng.Source)
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

func parseInt64Literal(n *chparser.NumberLiteral) (int64, error) {
	if n == nil {
		return 0, nil
	}
	v, err := strconv.ParseInt(n.Literal, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid integer literal %q: %w", n.Literal, err)
	}
	return v, nil
}

// takeArg reads and removes one arg so that, after a builder consumed
// everything it models, leftovers can be rejected loudly — a dropped
// parameter is symmetric between a golden and a fresh dump, so diff
// cannot see the loss (#115, the #108 failure mode).
func takeArg(args map[string]string, key string) string {
	v, ok := args[key]
	if ok {
		delete(args, key)
	}
	return v
}

func takeStr(args map[string]string, key string) *string  { return optStr(takeArg(args, key)) }
func takeInt64(args map[string]string, key string) *int64 { return optInt64(takeArg(args, key)) }
func takeBool(args map[string]string, key string) *bool   { return optBool(takeArg(args, key)) }

func takeFloat64(args map[string]string, key string) *float64 {
	s := takeArg(args, key)
	if s == "" {
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &f
}

func unknownArgsError(clause, kind string, args map[string]string) error {
	if len(args) == 0 {
		return nil
	}
	return fmt.Errorf("%s %s: unknown parameter(s) %v (unmodeled — refusing to drop)", clause, kind, sortedKeys(args))
}

func buildDictionarySourceFromAST(dictName string, s *chparser.DictionarySourceClause) (*DictionarySourceSpec, error) {
	if s == nil || s.Source == nil {
		return nil, errors.New("dictionary has no source")
	}
	kind := strings.ToLower(s.Source.Name)
	args := dictArgsMap(s.Args)

	// optSecret captures a credential, but drops a value ClickHouse redacted to
	// "[HIDDEN]" (returning nil) so it is never re-emitted and applied back —
	// which would overwrite the real secret. See issue #52. To capture real
	// values, grant displaySecretsInShowAndSelect and set
	// display_secrets_in_show_and_select = 1.
	optSecret := func(field string) *string {
		v := takeArg(args, field)
		if v == RedactedValue {
			slog.Warn("dictionary source secret is redacted; dropping it to avoid overwriting the real value on apply",
				"dictionary", dictName, "source", kind, "field", field,
				"hint", "grant displaySecretsInShowAndSelect AND set display_secrets_in_show_and_select=1")
			return nil
		}
		return optStr(v)
	}

	var decoded DictionarySource
	switch kind {
	case "clickhouse":
		decoded = SourceClickHouse{
			Host: takeStr(args, "host"), Port: takeInt64(args, "port"),
			User: takeStr(args, "user"), Password: optSecret("password"),
			DB: takeStr(args, "db"), Table: takeStr(args, "table"),
			Query:           takeStr(args, "query"),
			InvalidateQuery: takeStr(args, "invalidate_query"),
			UpdateField:     takeStr(args, "update_field"),
			UpdateLag:       takeInt64(args, "update_lag"),
		}
	case "mysql":
		decoded = SourceMySQL{
			Host: takeStr(args, "host"), Port: takeInt64(args, "port"),
			User: takeStr(args, "user"), Password: optSecret("password"),
			DB: takeStr(args, "db"), Table: takeStr(args, "table"),
			Query:           takeStr(args, "query"),
			InvalidateQuery: takeStr(args, "invalidate_query"),
			UpdateField:     takeStr(args, "update_field"),
			UpdateLag:       takeInt64(args, "update_lag"),
		}
	case "postgresql":
		decoded = SourcePostgreSQL{
			Host: takeStr(args, "host"), Port: takeInt64(args, "port"),
			User: takeStr(args, "user"), Password: optSecret("password"),
			DB: takeStr(args, "db"), Table: takeStr(args, "table"),
			Query:           takeStr(args, "query"),
			InvalidateQuery: takeStr(args, "invalidate_query"),
			UpdateField:     takeStr(args, "update_field"),
			UpdateLag:       takeInt64(args, "update_lag"),
		}
	case "http":
		decoded = SourceHTTP{
			URL: takeArg(args, "url"), Format: takeArg(args, "format"),
			CredentialsUser:     takeStr(args, "credentials_user"),
			CredentialsPassword: optSecret("credentials_password"),
		}
	case "file":
		decoded = SourceFile{Path: takeArg(args, "path"), Format: takeArg(args, "format")}
	case "executable":
		decoded = SourceExecutable{
			Command:     takeArg(args, "command"),
			Format:      takeArg(args, "format"),
			ImplicitKey: takeBool(args, "implicit_key"),
		}
	case "null":
		decoded = SourceNull{}
	default:
		return nil, fmt.Errorf("unsupported dictionary source kind %q", kind)
	}
	if err := unknownArgsError("source", kind, args); err != nil {
		return nil, err
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
		decoded = LayoutFlat{
			InitialArraySize: takeInt64(args, "initial_array_size"),
			MaxArraySize:     takeInt64(args, "max_array_size"),
		}
	case "hashed":
		decoded = LayoutHashed{
			Shards:                takeInt64(args, "shards"),
			ShardLoadQueueBacklog: takeInt64(args, "shard_load_queue_backlog"),
			MaxLoadFactor:         takeFloat64(args, "max_load_factor"),
		}
	case "sparse_hashed":
		decoded = LayoutSparseHashed{
			Shards:                takeInt64(args, "shards"),
			ShardLoadQueueBacklog: takeInt64(args, "shard_load_queue_backlog"),
			MaxLoadFactor:         takeFloat64(args, "max_load_factor"),
		}
	case "regexp_tree":
		decoded = LayoutRegexpTree{}
	case "complex_key_sparse_hashed":
		decoded = LayoutComplexKeySparseHashed{
			Shards:                takeInt64(args, "shards"),
			ShardLoadQueueBacklog: takeInt64(args, "shard_load_queue_backlog"),
			MaxLoadFactor:         takeFloat64(args, "max_load_factor"),
		}
	case "direct":
		decoded = LayoutDirect{}
	case "complex_key_hashed":
		decoded = LayoutComplexKeyHashed{
			Preallocate:           takeInt64(args, "preallocate"),
			Shards:                takeInt64(args, "shards"),
			ShardLoadQueueBacklog: takeInt64(args, "shard_load_queue_backlog"),
			MaxLoadFactor:         takeFloat64(args, "max_load_factor"),
		}
	case "range_hashed":
		decoded = LayoutRangeHashed{RangeLookupStrategy: takeStr(args, "range_lookup_strategy")}
	case "complex_key_range_hashed":
		decoded = LayoutComplexKeyRangeHashed{RangeLookupStrategy: takeStr(args, "range_lookup_strategy")}
	case "cache":
		n := takeInt64(args, "size_in_cells")
		if n == nil {
			return nil, errors.New("layout cache: missing size_in_cells")
		}
		decoded = LayoutCache{SizeInCells: *n}
	case "complex_key_cache":
		n := takeInt64(args, "size_in_cells")
		if n == nil {
			return nil, errors.New("layout complex_key_cache: missing size_in_cells")
		}
		decoded = LayoutComplexKeyCache{SizeInCells: *n}
	case "complex_key_direct":
		decoded = LayoutComplexKeyDirect{}
	case "hashed_array":
		decoded = LayoutHashedArray{Shards: takeInt64(args, "shards")}
	case "complex_key_hashed_array":
		decoded = LayoutComplexKeyHashedArray{Shards: takeInt64(args, "shards")}
	case "ip_trie":
		decoded = LayoutIPTrie{AccessToKeyFromAttributes: takeBool(args, "access_to_key_from_attributes")}
	default:
		return nil, fmt.Errorf("unsupported dictionary layout kind %q", kind)
	}
	if err := unknownArgsError("layout", kind, args); err != nil {
		return nil, err
	}
	return &DictionaryLayoutSpec{Kind: kind, Decoded: decoded}, nil
}
