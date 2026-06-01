package hcl

import (
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decodeLayout(t *testing.T, src string) (DictionaryLayout, error) {
	t.Helper()
	parser := hclparse.NewParser()
	f, diags := parser.ParseHCL([]byte(src), "test.hcl")
	require.False(t, diags.HasErrors(), "parse: %s", diags.Error())

	var spec struct {
		Dicts []struct {
			Name   string                `hcl:"name,label"`
			Layout *DictionaryLayoutSpec `hcl:"layout,block"`
			Rest   hcl.Body              `hcl:",remain"`
		} `hcl:"dictionary,block"`
	}
	d := gohcl.DecodeBody(f.Body, nil, &spec)
	require.False(t, d.HasErrors(), "decode: %s", d.Error())
	require.Len(t, spec.Dicts, 1)
	require.NotNil(t, spec.Dicts[0].Layout)
	return DecodeDictionaryLayout(spec.Dicts[0].Layout)
}

func TestDecodeDictionaryLayout_AllSupportedKinds(t *testing.T) {
	cases := []struct {
		name string
		hcl  string
		want DictionaryLayout
	}{
		{"flat", `dictionary "d" {
			layout "flat" {}
		}`, LayoutFlat{}},
		{"hashed", `dictionary "d" {
			layout "hashed" {}
		}`, LayoutHashed{}},
		{"sparse_hashed", `dictionary "d" {
			layout "sparse_hashed" {}
		}`, LayoutSparseHashed{}},
		{"complex_key_hashed", `dictionary "d" {
			layout "complex_key_hashed" {
				preallocate = 1
			}
		}`, LayoutComplexKeyHashed{Preallocate: ptr(int64(1))}},
		{"complex_key_sparse_hashed", `dictionary "d" {
			layout "complex_key_sparse_hashed" {}
		}`, LayoutComplexKeySparseHashed{}},
		{"range_hashed", `dictionary "d" {
			layout "range_hashed" {
				range_lookup_strategy = "max"
			}
		}`, LayoutRangeHashed{RangeLookupStrategy: ptr("max")}},
		{"complex_key_range_hashed", `dictionary "d" {
			layout "complex_key_range_hashed" {
				range_lookup_strategy = "max"
			}
		}`, LayoutComplexKeyRangeHashed{RangeLookupStrategy: ptr("max")}},
		{"cache", `dictionary "d" {
			layout "cache" {
				size_in_cells = 1000
			}
		}`, LayoutCache{SizeInCells: 1000}},
		{"ip_trie", `dictionary "d" {
			layout "ip_trie" {
				access_to_key_from_attributes = true
			}
		}`, LayoutIPTrie{AccessToKeyFromAttributes: ptr(true)}},
		{"direct", `dictionary "d" {
			layout "direct" {}
		}`, LayoutDirect{}},
		{"complex_key_cache", `dictionary "d" {
			layout "complex_key_cache" {
				size_in_cells = 2000
			}
		}`, LayoutComplexKeyCache{SizeInCells: 2000}},
		{"complex_key_direct", `dictionary "d" {
			layout "complex_key_direct" {}
		}`, LayoutComplexKeyDirect{}},
		{"hashed_array_no_shards", `dictionary "d" {
			layout "hashed_array" {}
		}`, LayoutHashedArray{}},
		{"hashed_array_with_shards", `dictionary "d" {
			layout "hashed_array" {
				shards = 4
			}
		}`, LayoutHashedArray{Shards: ptr(int64(4))}},
		{"complex_key_hashed_array", `dictionary "d" {
			layout "complex_key_hashed_array" {
				shards = 8
			}
		}`, LayoutComplexKeyHashedArray{Shards: ptr(int64(8))}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeLayout(t, tc.hcl)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDecodeDictionaryLayout_Unsupported(t *testing.T) {
	src := `dictionary "d" {
		layout "polygon" {}
	}`
	_, err := decodeLayout(t, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported dictionary layout kind")
	assert.Contains(t, err.Error(), "polygon")
}

func TestDecodeDictionaryLayout_NilSpec(t *testing.T) {
	got, err := DecodeDictionaryLayout(nil)
	require.NoError(t, err)
	assert.Nil(t, got)
}
