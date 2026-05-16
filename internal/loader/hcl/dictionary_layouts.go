package hcl

import (
	"fmt"

	"github.com/hashicorp/hcl/v2/gohcl"
)

type LayoutFlat struct{}

func (LayoutFlat) Kind() string { return "flat" }

type LayoutHashed struct{}

func (LayoutHashed) Kind() string { return "hashed" }

type LayoutSparseHashed struct{}

func (LayoutSparseHashed) Kind() string { return "sparse_hashed" }

type LayoutComplexKeyHashed struct {
	Preallocate *int64 `hcl:"preallocate,optional"`
}

func (LayoutComplexKeyHashed) Kind() string { return "complex_key_hashed" }

type LayoutComplexKeySparseHashed struct{}

func (LayoutComplexKeySparseHashed) Kind() string { return "complex_key_sparse_hashed" }

type LayoutRangeHashed struct {
	RangeLookupStrategy *string `hcl:"range_lookup_strategy,optional"`
}

func (LayoutRangeHashed) Kind() string { return "range_hashed" }

type LayoutComplexKeyRangeHashed struct {
	RangeLookupStrategy *string `hcl:"range_lookup_strategy,optional"`
}

func (LayoutComplexKeyRangeHashed) Kind() string { return "complex_key_range_hashed" }

type LayoutCache struct {
	SizeInCells int64 `hcl:"size_in_cells"`
}

func (LayoutCache) Kind() string { return "cache" }

type LayoutIPTrie struct {
	AccessToKeyFromAttributes *bool `hcl:"access_to_key_from_attributes,optional"`
}

func (LayoutIPTrie) Kind() string { return "ip_trie" }

type LayoutDirect struct{}

func (LayoutDirect) Kind() string { return "direct" }

// DecodeDictionaryLayout dispatches on spec.Kind and decodes the body into
// the matching typed layout struct. Returns (nil, nil) when spec is nil.
func DecodeDictionaryLayout(spec *DictionaryLayoutSpec) (DictionaryLayout, error) {
	if spec == nil {
		return nil, nil
	}
	switch spec.Kind {
	case "flat":
		return LayoutFlat{}, nil
	case "hashed":
		return LayoutHashed{}, nil
	case "sparse_hashed":
		return LayoutSparseHashed{}, nil
	case "complex_key_sparse_hashed":
		return LayoutComplexKeySparseHashed{}, nil
	case "direct":
		return LayoutDirect{}, nil
	case "complex_key_hashed":
		var l LayoutComplexKeyHashed
		if d := gohcl.DecodeBody(spec.Body, nil, &l); d.HasErrors() {
			return nil, fmt.Errorf("layout complex_key_hashed: %s", d.Error())
		}
		return l, nil
	case "range_hashed":
		var l LayoutRangeHashed
		if d := gohcl.DecodeBody(spec.Body, nil, &l); d.HasErrors() {
			return nil, fmt.Errorf("layout range_hashed: %s", d.Error())
		}
		return l, nil
	case "complex_key_range_hashed":
		var l LayoutComplexKeyRangeHashed
		if d := gohcl.DecodeBody(spec.Body, nil, &l); d.HasErrors() {
			return nil, fmt.Errorf("layout complex_key_range_hashed: %s", d.Error())
		}
		return l, nil
	case "cache":
		var l LayoutCache
		if d := gohcl.DecodeBody(spec.Body, nil, &l); d.HasErrors() {
			return nil, fmt.Errorf("layout cache: %s", d.Error())
		}
		return l, nil
	case "ip_trie":
		var l LayoutIPTrie
		if d := gohcl.DecodeBody(spec.Body, nil, &l); d.HasErrors() {
			return nil, fmt.Errorf("layout ip_trie: %s", d.Error())
		}
		return l, nil
	default:
		return nil, fmt.Errorf("unsupported dictionary layout kind %q", spec.Kind)
	}
}
