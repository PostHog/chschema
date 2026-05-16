package hcl

import (
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
)

func writeNamedCollection(body *hclwrite.Body, nc NamedCollectionSpec) {
	if nc.External {
		body.SetAttributeValue("external", cty.True)
	}
	if nc.Override {
		body.SetAttributeValue("override", cty.True)
	}
	if nc.Cluster != nil {
		body.SetAttributeValue("cluster", cty.StringVal(*nc.Cluster))
	}
	if nc.Comment != nil {
		body.SetAttributeValue("comment", cty.StringVal(*nc.Comment))
	}
	for _, p := range nc.Params {
		pb := body.AppendNewBlock("param", []string{p.Key}).Body()
		pb.SetAttributeValue("value", cty.StringVal(p.Value))
		if p.Overridable != nil {
			if *p.Overridable {
				pb.SetAttributeValue("overridable", cty.True)
			} else {
				pb.SetAttributeValue("overridable", cty.False)
			}
		}
	}
}
