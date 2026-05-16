package hcl

import (
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decodeSource parses an HCL snippet of the form
//
//	dictionary "x" { source "<kind>" { ... } }
//
// and returns the post-DecodeDictionarySource value, for testing in isolation.
func decodeSource(t *testing.T, src string) (DictionarySource, error) {
	t.Helper()
	parser := hclparse.NewParser()
	f, diags := parser.ParseHCL([]byte(src), "test.hcl")
	require.False(t, diags.HasErrors(), "parse: %s", diags.Error())

	var spec struct {
		Dicts []struct {
			Name   string                `hcl:"name,label"`
			Source *DictionarySourceSpec `hcl:"source,block"`
			Rest   hcl.Body              `hcl:",remain"`
		} `hcl:"dictionary,block"`
	}
	d := gohcl.DecodeBody(f.Body, nil, &spec)
	require.False(t, d.HasErrors(), "decode: %s", d.Error())
	require.Len(t, spec.Dicts, 1)
	require.NotNil(t, spec.Dicts[0].Source)
	return DecodeDictionarySource(spec.Dicts[0].Source)
}

func TestDecodeDictionarySource_AllSupportedKinds(t *testing.T) {
	cases := []struct {
		name string
		hcl  string
		want DictionarySource
	}{
		{
			name: "clickhouse",
			hcl: `dictionary "d" {
				source "clickhouse" {
					host  = "ch.example"
					port  = 9000
					user  = "ro"
					password = "s3cret"
					db    = "default"
					table = "src"
					query = "SELECT * FROM default.src"
					invalidate_query = "SELECT max(ts) FROM default.src"
					update_field = "ts"
					update_lag   = 5
				}
			}`,
			want: SourceClickHouse{
				Host: ptr("ch.example"), Port: ptr(int64(9000)),
				User: ptr("ro"), Password: ptr("s3cret"),
				DB: ptr("default"), Table: ptr("src"),
				Query:           ptr("SELECT * FROM default.src"),
				InvalidateQuery: ptr("SELECT max(ts) FROM default.src"),
				UpdateField:     ptr("ts"), UpdateLag: ptr(int64(5)),
			},
		},
		{
			name: "mysql",
			hcl: `dictionary "d" {
				source "mysql" {
					host = "h"
					port = 3306
					user = "u"
					password = "p"
					db = "d1"
					table = "t1"
				}
			}`,
			want: SourceMySQL{Host: ptr("h"), Port: ptr(int64(3306)), User: ptr("u"), Password: ptr("p"), DB: ptr("d1"), Table: ptr("t1")},
		},
		{
			name: "postgresql",
			hcl: `dictionary "d" {
				source "postgresql" {
					host = "h"
					port = 5432
					user = "u"
					db = "d1"
					table = "t1"
				}
			}`,
			want: SourcePostgreSQL{Host: ptr("h"), Port: ptr(int64(5432)), User: ptr("u"), DB: ptr("d1"), Table: ptr("t1")},
		},
		{
			name: "http",
			hcl: `dictionary "d" {
				source "http" {
					url = "https://x/y"
					format = "JSONEachRow"
					credentials_user = "u"
					credentials_password = "p"
				}
			}`,
			want: SourceHTTP{URL: "https://x/y", Format: "JSONEachRow", CredentialsUser: ptr("u"), CredentialsPassword: ptr("p")},
		},
		{
			name: "file",
			hcl: `dictionary "d" {
				source "file" {
					path = "/data/x.csv"
					format = "CSV"
				}
			}`,
			want: SourceFile{Path: "/data/x.csv", Format: "CSV"},
		},
		{
			name: "executable",
			hcl: `dictionary "d" {
				source "executable" {
					command = "/bin/dump"
					format = "TabSeparated"
					implicit_key = true
				}
			}`,
			want: SourceExecutable{Command: "/bin/dump", Format: "TabSeparated", ImplicitKey: ptr(true)},
		},
		{
			name: "null",
			hcl: `dictionary "d" {
				source "null" {}
			}`,
			want: SourceNull{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeSource(t, tc.hcl)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDecodeDictionarySource_Unsupported(t *testing.T) {
	src := `dictionary "d" {
		source "mongodb" { connection_string = "mongodb://x" }
	}`
	_, err := decodeSource(t, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported dictionary source kind")
	assert.Contains(t, err.Error(), "mongodb")
}

func TestDecodeDictionarySource_NilSpec(t *testing.T) {
	got, err := DecodeDictionarySource(nil)
	require.NoError(t, err)
	assert.Nil(t, got)
}
