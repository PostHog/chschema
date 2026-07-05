package hcl

import (
	"fmt"

	"github.com/hashicorp/hcl/v2/gohcl"
)

type LayoutFlat struct {
	InitialArraySize *int64 `hcl:"initial_array_size,optional"`
	MaxArraySize     *int64 `hcl:"max_array_size,optional"`
}

func (LayoutFlat) Kind() string { return "flat" }

type LayoutHashed struct {
	Shards                *int64   `hcl:"shards,optional"`
	ShardLoadQueueBacklog *int64   `hcl:"shard_load_queue_backlog,optional"`
	MaxLoadFactor         *float64 `hcl:"max_load_factor,optional"`
}

func (LayoutHashed) Kind() string { return "hashed" }

type LayoutSparseHashed struct {
	Shards                *int64   `hcl:"shards,optional"`
	ShardLoadQueueBacklog *int64   `hcl:"shard_load_queue_backlog,optional"`
	MaxLoadFactor         *float64 `hcl:"max_load_factor,optional"`
}

func (LayoutSparseHashed) Kind() string { return "sparse_hashed" }

// LayoutRegexpTree backs a dictionary that matches keys against an ordered tree
// of regular expressions. The layout itself takes no inner parameters.
type LayoutRegexpTree struct{}

func (LayoutRegexpTree) Kind() string { return "regexp_tree" }

type LayoutComplexKeyHashed struct {
	// Preallocate is accepted by ClickHouse for backwards compatibility
	// but no longer documented; kept so old DDL introspects faithfully.
	Preallocate           *int64   `hcl:"preallocate,optional"`
	Shards                *int64   `hcl:"shards,optional"`
	ShardLoadQueueBacklog *int64   `hcl:"shard_load_queue_backlog,optional"`
	MaxLoadFactor         *float64 `hcl:"max_load_factor,optional"`
}

func (LayoutComplexKeyHashed) Kind() string { return "complex_key_hashed" }

type LayoutComplexKeySparseHashed struct {
	Shards                *int64   `hcl:"shards,optional"`
	ShardLoadQueueBacklog *int64   `hcl:"shard_load_queue_backlog,optional"`
	MaxLoadFactor         *float64 `hcl:"max_load_factor,optional"`
}

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

// LayoutComplexKeyCache is the complex-key (multi-column key) counterpart
// to LayoutCache. Same single required parameter — size_in_cells controls
// the number of cells in the LRU cache.
type LayoutComplexKeyCache struct {
	SizeInCells int64 `hcl:"size_in_cells"`
}

func (LayoutComplexKeyCache) Kind() string { return "complex_key_cache" }

// LayoutComplexKeyDirect is the complex-key counterpart to LayoutDirect:
// every lookup hits the source, no in-process caching. No parameters.
type LayoutComplexKeyDirect struct{}

func (LayoutComplexKeyDirect) Kind() string { return "complex_key_direct" }

// LayoutHashedArray is a memory-efficient hash layout that stores values
// in a packed array. `shards` partitions the dictionary across N parallel
// hash tables for concurrent reads (default 1).
type LayoutHashedArray struct {
	Shards *int64 `hcl:"shards,optional"`
}

func (LayoutHashedArray) Kind() string { return "hashed_array" }

// LayoutComplexKeyHashedArray is the complex-key counterpart to
// LayoutHashedArray. Same optional `shards` parameter.
type LayoutComplexKeyHashedArray struct {
	Shards *int64 `hcl:"shards,optional"`
}

func (LayoutComplexKeyHashedArray) Kind() string { return "complex_key_hashed_array" }

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
		var l LayoutFlat
		if d := gohcl.DecodeBody(spec.Body, nil, &l); d.HasErrors() {
			return nil, fmt.Errorf("layout flat: %s", d.Error())
		}
		return l, nil
	case "hashed":
		var l LayoutHashed
		if d := gohcl.DecodeBody(spec.Body, nil, &l); d.HasErrors() {
			return nil, fmt.Errorf("layout hashed: %s", d.Error())
		}
		return l, nil
	case "sparse_hashed":
		var l LayoutSparseHashed
		if d := gohcl.DecodeBody(spec.Body, nil, &l); d.HasErrors() {
			return nil, fmt.Errorf("layout sparse_hashed: %s", d.Error())
		}
		return l, nil
	case "regexp_tree":
		return LayoutRegexpTree{}, nil
	case "complex_key_sparse_hashed":
		var l LayoutComplexKeySparseHashed
		if d := gohcl.DecodeBody(spec.Body, nil, &l); d.HasErrors() {
			return nil, fmt.Errorf("layout complex_key_sparse_hashed: %s", d.Error())
		}
		return l, nil
	case "direct":
		return LayoutDirect{}, nil
	case "complex_key_direct":
		return LayoutComplexKeyDirect{}, nil
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
	case "complex_key_cache":
		var l LayoutComplexKeyCache
		if d := gohcl.DecodeBody(spec.Body, nil, &l); d.HasErrors() {
			return nil, fmt.Errorf("layout complex_key_cache: %s", d.Error())
		}
		return l, nil
	case "hashed_array":
		var l LayoutHashedArray
		if d := gohcl.DecodeBody(spec.Body, nil, &l); d.HasErrors() {
			return nil, fmt.Errorf("layout hashed_array: %s", d.Error())
		}
		return l, nil
	case "complex_key_hashed_array":
		var l LayoutComplexKeyHashedArray
		if d := gohcl.DecodeBody(spec.Body, nil, &l); d.HasErrors() {
			return nil, fmt.Errorf("layout complex_key_hashed_array: %s", d.Error())
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
