package hcl

import (
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
)

// writeDictionary emits a `dictionary` block body. Sub-blocks (lifetime,
// range, attribute, source, layout) are emitted in a consistent order.
func writeDictionary(body *hclwrite.Body, d DictionarySpec) {
	body.SetAttributeValue("primary_key", stringList(d.PrimaryKey))
	if len(d.Settings) > 0 {
		body.SetAttributeValue("settings", stringMap(d.Settings))
	}
	if d.Cluster != nil {
		body.SetAttributeValue("cluster", cty.StringVal(*d.Cluster))
	}
	if d.Comment != nil {
		body.SetAttributeValue("comment", cty.StringVal(*d.Comment))
	}
	if d.Lifetime != nil {
		lt := body.AppendNewBlock("lifetime", nil).Body()
		if d.Lifetime.Min != nil {
			lt.SetAttributeValue("min", cty.NumberIntVal(*d.Lifetime.Min))
		}
		if d.Lifetime.Max != nil {
			lt.SetAttributeValue("max", cty.NumberIntVal(*d.Lifetime.Max))
		}
	}
	if d.Range != nil {
		r := body.AppendNewBlock("range", nil).Body()
		r.SetAttributeValue("min", cty.StringVal(d.Range.Min))
		r.SetAttributeValue("max", cty.StringVal(d.Range.Max))
	}
	for _, a := range d.Attributes {
		ab := body.AppendNewBlock("attribute", []string{a.Name}).Body()
		ab.SetAttributeValue("type", cty.StringVal(a.Type))
		if a.Default != nil {
			ab.SetAttributeValue("default", cty.StringVal(*a.Default))
		}
		if a.Expression != nil {
			ab.SetAttributeValue("expression", cty.StringVal(*a.Expression))
		}
		if a.Hierarchical {
			ab.SetAttributeValue("hierarchical", cty.True)
		}
		if a.Injective {
			ab.SetAttributeValue("injective", cty.True)
		}
		if a.IsObjectID {
			ab.SetAttributeValue("is_object_id", cty.True)
		}
	}
	if d.Source != nil && d.Source.Decoded != nil {
		writeDictionarySource(body, d.Source.Decoded)
	}
	if d.Layout != nil && d.Layout.Decoded != nil {
		writeDictionaryLayout(body, d.Layout.Decoded)
	}
}

func writeDictionarySource(parent *hclwrite.Body, s DictionarySource) {
	block := parent.AppendNewBlock("source", []string{s.Kind()})
	b := block.Body()
	switch v := s.(type) {
	case SourceClickHouse:
		writeOptStr(b, "host", v.Host)
		writeOptInt(b, "port", v.Port)
		writeOptStr(b, "user", v.User)
		writeOptStr(b, "password", v.Password)
		writeOptStr(b, "db", v.DB)
		writeOptStr(b, "table", v.Table)
		writeOptStr(b, "query", v.Query)
		writeOptStr(b, "invalidate_query", v.InvalidateQuery)
		writeOptStr(b, "update_field", v.UpdateField)
		writeOptInt(b, "update_lag", v.UpdateLag)
	case SourceMySQL:
		writeOptStr(b, "host", v.Host)
		writeOptInt(b, "port", v.Port)
		writeOptStr(b, "user", v.User)
		writeOptStr(b, "password", v.Password)
		writeOptStr(b, "db", v.DB)
		writeOptStr(b, "table", v.Table)
		writeOptStr(b, "query", v.Query)
		writeOptStr(b, "invalidate_query", v.InvalidateQuery)
		writeOptStr(b, "update_field", v.UpdateField)
		writeOptInt(b, "update_lag", v.UpdateLag)
	case SourcePostgreSQL:
		writeOptStr(b, "host", v.Host)
		writeOptInt(b, "port", v.Port)
		writeOptStr(b, "user", v.User)
		writeOptStr(b, "password", v.Password)
		writeOptStr(b, "db", v.DB)
		writeOptStr(b, "table", v.Table)
		writeOptStr(b, "query", v.Query)
		writeOptStr(b, "invalidate_query", v.InvalidateQuery)
		writeOptStr(b, "update_field", v.UpdateField)
		writeOptInt(b, "update_lag", v.UpdateLag)
	case SourceHTTP:
		b.SetAttributeValue("url", cty.StringVal(v.URL))
		b.SetAttributeValue("format", cty.StringVal(v.Format))
		writeOptStr(b, "credentials_user", v.CredentialsUser)
		writeOptStr(b, "credentials_password", v.CredentialsPassword)
	case SourceFile:
		b.SetAttributeValue("path", cty.StringVal(v.Path))
		b.SetAttributeValue("format", cty.StringVal(v.Format))
	case SourceExecutable:
		b.SetAttributeValue("command", cty.StringVal(v.Command))
		b.SetAttributeValue("format", cty.StringVal(v.Format))
		writeOptBool(b, "implicit_key", v.ImplicitKey)
	case SourceNull:
		// no fields
	}
}

func writeDictionaryLayout(parent *hclwrite.Body, l DictionaryLayout) {
	block := parent.AppendNewBlock("layout", []string{l.Kind()})
	b := block.Body()
	switch v := l.(type) {
	case LayoutFlat, LayoutHashed, LayoutSparseHashed, LayoutComplexKeySparseHashed, LayoutDirect:
		// no fields
	case LayoutComplexKeyHashed:
		writeOptInt(b, "preallocate", v.Preallocate)
	case LayoutRangeHashed:
		writeOptStr(b, "range_lookup_strategy", v.RangeLookupStrategy)
	case LayoutComplexKeyRangeHashed:
		writeOptStr(b, "range_lookup_strategy", v.RangeLookupStrategy)
	case LayoutCache:
		b.SetAttributeValue("size_in_cells", cty.NumberIntVal(v.SizeInCells))
	case LayoutIPTrie:
		writeOptBool(b, "access_to_key_from_attributes", v.AccessToKeyFromAttributes)
	}
}

func writeOptStr(b *hclwrite.Body, key string, v *string) {
	if v != nil {
		b.SetAttributeValue(key, cty.StringVal(*v))
	}
}

func writeOptInt(b *hclwrite.Body, key string, v *int64) {
	if v != nil {
		b.SetAttributeValue(key, cty.NumberIntVal(*v))
	}
}

func writeOptBool(b *hclwrite.Body, key string, v *bool) {
	if v != nil {
		if *v {
			b.SetAttributeValue(key, cty.True)
		} else {
			b.SetAttributeValue(key, cty.False)
		}
	}
}
