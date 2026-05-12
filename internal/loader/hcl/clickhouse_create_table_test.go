// Tests in this file are derived directly from
// clickhouse-docs/en/sql-reference/statements/create/table.md. Each case
// covers one feature documented for CREATE TABLE. The same cases are reused
// by the live test in test/hcl_create_table_live_test.go.
//
// Out-of-scope features (not declarative IaC concerns):
//   - CREATE TABLE AS / CLONE AS / AS table_function() / AS SELECT
//   - TEMPORARY TABLE
//   - REPLACE TABLE
//   - IF NOT EXISTS (handled by the diff/apply layer, not the schema)

package hcl

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clickhouseLive enables tests that execute generated DDL against a real
// ClickHouse instance. Mirrors the flag in test/integration_test.go — both
// are valid in their respective test binaries, neither affects the other.
var clickhouseLive = flag.Bool("clickhouse", false, "run tests that execute against a live ClickHouse")

// createTableCase is one row of the doc-derived test table. Each case maps a
// CREATE TABLE feature from the ClickHouse docs to a piece of HCL and the
// substrings the generated SQL must contain.
//
// skipLive marks cases that can't be executed against a vanilla single-node
// ClickHouse docker container (e.g., ON CLUSTER, which needs a cluster of
// the named topology to exist in the server config).
type createTableCase struct {
	name         string
	hcl          string
	wantContains []string
	skipLive     bool
}

// createTableCases is the shared test table used by both the static unit
// test (TestCH_CreateTable) and the live test
// (TestCHLive_CreateTable).
//
// All HCL fixtures use `database "db" {…}` so the live test can rewrite the
// database name to an isolated temp database via a single string replace.
var createTableCases = []createTableCase{
	// --- Column-level features ---

	// Docs: "NULL Or NOT NULL Modifiers". ClickHouse rejects nullable
	// columns in the sorting key by default (allow_nullable_key = 0), so
	// the nullable column is kept out of order_by.
	{
		name: "Column_NullModifier",
		hcl: `database "db" {
  table "t" {
    column "id" { type = "UInt64" }
    column "x" {
      type     = "UInt64"
      nullable = true
    }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`,
		wantContains: []string{"x Nullable(UInt64)"},
	},

	// Docs: "DEFAULT".
	{
		name: "Column_Default",
		hcl: `database "db" {
  table "t" {
    column "id" {
      type    = "UInt64"
      default = "0"
    }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`,
		wantContains: []string{"id UInt64 DEFAULT 0"},
	},

	// Docs: "MATERIALIZED".
	{
		name: "Column_Materialized",
		hcl: `database "db" {
  table "t" {
    column "id" { type = "UInt64" }
    column "updated_at" {
      type         = "DateTime"
      materialized = "now()"
    }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`,
		wantContains: []string{"updated_at DateTime MATERIALIZED now()"},
	},

	// Docs: "EPHEMERAL". The body expression is optional; an empty string
	// means a bare EPHEMERAL clause.
	{
		name: "Column_Ephemeral",
		hcl: `database "db" {
  table "t" {
    column "id"      { type = "UInt64" }
    column "unhexed" {
      type      = "String"
      ephemeral = ""
    }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`,
		wantContains: []string{"unhexed String EPHEMERAL"},
	},

	// Docs: "ALIAS".
	{
		name: "Column_Alias",
		hcl: `database "db" {
  table "t" {
    column "id"         { type = "UInt64" }
    column "size_bytes" { type = "Int64" }
    column "size" {
      type  = "String"
      alias = "formatReadableSize(size_bytes)"
    }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`,
		wantContains: []string{"size String ALIAS formatReadableSize(size_bytes)"},
	},

	// Docs: per-column COMMENT in the opening syntax form.
	{
		name: "Column_Comment",
		hcl: `database "db" {
  table "t" {
    column "id" {
      type    = "UInt64"
      comment = "Primary key"
    }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`,
		wantContains: []string{"id UInt64 COMMENT 'Primary key'"},
	},

	// Docs: "Column Compression Codecs" — single named codec.
	{
		name: "Column_CodecSimple",
		hcl: `database "db" {
  table "t" {
    column "id" { type = "UInt64" }
    column "ts" {
      type  = "DateTime"
      codec = "ZSTD"
    }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`,
		wantContains: []string{"ts DateTime CODEC(ZSTD)"},
	},

	// Docs: codecs with arguments (LZ4HC, ZSTD with a level).
	{
		name: "Column_CodecWithLevel",
		hcl: `database "db" {
  table "t" {
    column "id" { type = "UInt64" }
    column "v" {
      type  = "Float64"
      codec = "LZ4HC(9)"
    }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`,
		wantContains: []string{"v Float64 CODEC(LZ4HC(9))"},
	},

	// Docs: codec pipelines like CODEC(Delta, ZSTD).
	{
		name: "Column_CodecPipeline",
		hcl: `database "db" {
  table "t" {
    column "id" { type = "UInt64" }
    column "v" {
      type  = "Float32"
      codec = "Delta, ZSTD"
    }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`,
		wantContains: []string{"v Float32 CODEC(Delta, ZSTD)"},
	},

	// Docs: per-column TTL. The referenced column must be in scope; declare
	// it before the TTL-bearing column so the live executor accepts it.
	{
		name: "Column_TTL",
		hcl: `database "db" {
  table "t" {
    column "id" { type = "UInt64" }
    column "ts" { type = "DateTime" }
    column "tmp" {
      type = "String"
      ttl  = "ts + INTERVAL 1 MONTH"
    }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}`,
		wantContains: []string{"tmp String TTL ts + INTERVAL 1 MONTH"},
	},

	// --- Table-level features ---

	// Docs: "Primary Key" — separate from ORDER BY.
	{
		name: "Table_PrimaryKey",
		hcl: `database "db" {
  table "t" {
    column "id" { type = "UInt64" }
    column "ts" { type = "DateTime" }

    primary_key = ["id"]
    order_by    = ["id", "ts"]

    engine "merge_tree" {}
  }
}`,
		wantContains: []string{"PRIMARY KEY (id)"},
	},

	// Docs: "CONSTRAINT" — CHECK boolean expression.
	{
		name: "Table_ConstraintCheck",
		hcl: `database "db" {
  table "t" {
    column "age" { type = "UInt8" }
    constraint "valid_age" {
      check = "age < 150"
    }
    engine "merge_tree" {}
    order_by = ["age"]
  }
}`,
		wantContains: []string{"CONSTRAINT valid_age CHECK age < 150"},
	},

	// Docs: "ASSUME" constraint variant.
	{
		name: "Table_ConstraintAssume",
		hcl: `database "db" {
  table "t" {
    column "name"     { type = "String" }
    column "name_len" {
      type         = "UInt8"
      materialized = "length(name)"
    }
    constraint "name_len_consistent" {
      assume = "length(name) = name_len"
    }
    engine "merge_tree" {}
    order_by = ["name_len", "name"]
  }
}`,
		wantContains: []string{"CONSTRAINT name_len_consistent ASSUME length(name) = name_len"},
	},

	// Docs: "COMMENT Clause" — table-level comment.
	{
		name: "Table_Comment",
		hcl: `database "db" {
  table "t" {
    column "x" { type = "String" }
    engine "merge_tree" {}
    order_by = ["x"]
    comment  = "The temporary table"
  }
}`,
		wantContains: []string{"COMMENT 'The temporary table'"},
	},

	// Docs: opening syntax form mentions `ON CLUSTER cluster`.
	// SkipLive: requires a cluster definition in the server config that
	// vanilla docker-compose doesn't provide.
	{
		name: "Table_OnCluster",
		hcl: `database "db" {
  table "t" {
    column "x" { type = "String" }
    engine "merge_tree" {}
    order_by = ["x"]
    cluster  = "my_cluster"
  }
}`,
		wantContains: []string{"ON CLUSTER my_cluster"},
		skipLive:     true,
	},
}

// parseAndGenerate is an end-to-end helper: HCL source → ParseFile →
// Resolve → Diff(nil, dbs) → GenerateSQL. Returns the single CREATE TABLE
// statement.
func parseAndGenerate(t *testing.T, hclSrc string) string {
	t.Helper()
	tmp := filepath.Join(t.TempDir(), "test.hcl")
	require.NoError(t, os.WriteFile(tmp, []byte(hclSrc), 0o644))

	dbs, err := ParseFile(tmp)
	require.NoError(t, err, "ParseFile failed")
	require.NoError(t, Resolve(dbs), "Resolve failed")

	cs := Diff(nil, dbs)
	out := GenerateSQL(cs)
	require.Len(t, out.Statements, 1, "expected exactly one CREATE TABLE statement")
	return out.Statements[0]
}

// TestCH_CreateTable runs every createTableCase as a subtest, validating
// that the generated SQL contains the documented ClickHouse syntax for the
// feature. This is a static check (no live ClickHouse) and is always
// executed.
func TestCH_CreateTable(t *testing.T) {
	for _, tc := range createTableCases {
		t.Run(tc.name, func(t *testing.T) {
			sql := parseAndGenerate(t, tc.hcl)
			for _, want := range tc.wantContains {
				assert.Contains(t, sql, want)
			}
		})
	}
}
