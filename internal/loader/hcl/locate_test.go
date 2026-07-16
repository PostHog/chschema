package hcl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeHCL(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

func TestScanDeclarations(t *testing.T) {
	dir := t.TempDir()
	path := writeHCL(t, dir, "schema.hcl", `
database "posthog" {
  table "events_base" {
    abstract = true
    column "uuid" { type = "UUID" }
  }

  table "events" {
    extend = "events_base"
    engine "MergeTree" {}
    order_by = ["uuid"]
  }

  table "person" {
    override = true
    engine "MergeTree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }

  patch_table "events" {
    column "extra" { type = "String" }
  }

  materialized_view "events_mv" {
    to_table = "events"
    query    = "SELECT uuid FROM src"
  }

  view "events_view" {
    query = "SELECT 1"
  }

  dictionary "geo" {
    primary_key = ["id"]
  }

  raw "table" "legacy" {
    sql = "CREATE TABLE posthog.legacy (x Int8) ENGINE = TinyLog"
  }

  node "host-1" {}
}

named_collection "kafka_creds" {
  override = true
  param "user" { value = "u" }
}
`)

	decls, err := ScanDeclarations([]string{path})
	require.NoError(t, err)

	want := []Declaration{
		{ObjectType: KindTable, Database: "posthog", Name: "events_base", File: path, Line: 3, Abstract: true},
		{ObjectType: KindTable, Database: "posthog", Name: "events", File: path, Line: 8, Extends: "events_base"},
		{ObjectType: KindTable, Database: "posthog", Name: "person", File: path, Line: 14, Override: true},
		{ObjectType: KindTable, Database: "posthog", Name: "events", File: path, Line: 21, Patch: true},
		{ObjectType: KindMaterializedView, Database: "posthog", Name: "events_mv", File: path, Line: 25},
		{ObjectType: KindView, Database: "posthog", Name: "events_view", File: path, Line: 30},
		{ObjectType: KindDictionary, Database: "posthog", Name: "geo", File: path, Line: 34},
		{ObjectType: KindRaw, Database: "posthog", Name: "legacy", File: path, Line: 38, RawKind: "table"},
		{ObjectType: KindNamedCollection, Name: "kafka_creds", File: path, Line: 45, Override: true},
	}
	assert.Equal(t, want, decls)
}

func TestScanDeclarationsParseError(t *testing.T) {
	dir := t.TempDir()
	path := writeHCL(t, dir, "broken.hcl", `database "posthog" {`)

	_, err := ScanDeclarations([]string{path})
	require.Error(t, err)
}

// ScanFileDeclarations additionally reports the file's node{} identity, so
// dump sites can be attributed to their host; a file without one reports "".
func TestScanFileDeclarationsNode(t *testing.T) {
	dir := t.TempDir()

	withNode := writeHCL(t, dir, "dump.hcl", `
node "prod-ch-1a" { macros = { shard = "1" } }
database "posthog" {
  table "events" {
    column "uuid" { type = "UUID" }
  }
}
`)
	decls, node, err := ScanFileDeclarations(withNode)
	require.NoError(t, err)
	assert.Equal(t, "prod-ch-1a", node)
	require.Len(t, decls, 1)
	assert.Equal(t, "events", decls[0].Name)

	withoutNode := writeHCL(t, dir, "plain.hcl", `
database "posthog" {
  table "person" {
    column "id" { type = "UInt64" }
  }
}
`)
	_, node, err = ScanFileDeclarations(withoutNode)
	require.NoError(t, err)
	assert.Empty(t, node)
}

func TestMatchesPattern(t *testing.T) {
	assert.True(t, MatchesPattern("events", "posthog", "events"))
	assert.True(t, MatchesPattern("events*", "posthog", "events_mv"))
	assert.True(t, MatchesPattern("posthog.events", "posthog", "events"))
	assert.True(t, MatchesPattern("posthog.*", "posthog", "anything"))
	assert.True(t, MatchesPattern("kafka_*", "", "kafka_creds"))
	assert.False(t, MatchesPattern("events", "posthog", "person"))
	assert.False(t, MatchesPattern("other.*", "posthog", "events"))
	assert.False(t, MatchesPattern("*.kafka_creds", "", "kafka_creds"))
}

func TestFindDuplicates(t *testing.T) {
	decls := []Declaration{
		// Plain duplicate: two plain declarations of posthog.person.
		{ObjectType: KindTable, Database: "posthog", Name: "person", File: "b/aux.hcl", Line: 2},
		{ObjectType: KindTable, Database: "posthog", Name: "person", File: "a/shared.hcl", Line: 10},
		// Legitimate: base + override.
		{ObjectType: KindTable, Database: "posthog", Name: "events", File: "a/shared.hcl", Line: 20},
		{ObjectType: KindTable, Database: "posthog", Name: "events", File: "c/prod.hcl", Line: 3, Override: true},
		// Legitimate: base + patch_table.
		{ObjectType: KindTable, Database: "posthog", Name: "groups", File: "a/shared.hcl", Line: 30},
		{ObjectType: KindTable, Database: "posthog", Name: "groups", File: "c/prod.hcl", Line: 9, Patch: true},
		// Legitimate: abstract base + same-named concrete child.
		{ObjectType: KindTable, Database: "posthog", Name: "sessions", File: "a/shared.hcl", Line: 40, Abstract: true},
		{ObjectType: KindTable, Database: "posthog", Name: "sessions", File: "c/prod.hcl", Line: 15, Extends: "sessions"},
		// Cross-type duplicate: table and raw table share the namespace.
		{ObjectType: KindTable, Database: "posthog", Name: "legacy", File: "a/shared.hcl", Line: 50},
		{ObjectType: KindRaw, Database: "posthog", Name: "legacy", File: "b/aux.hcl", Line: 8, RawKind: "table"},
		// Named collections: same name, no database.
		{ObjectType: KindNamedCollection, Name: "kafka_creds", File: "a/shared.hcl", Line: 60},
		{ObjectType: KindNamedCollection, Name: "kafka_creds", File: "b/aux.hcl", Line: 12},
		// Singleton: never reported.
		{ObjectType: KindView, Database: "posthog", Name: "only_once", File: "a/shared.hcl", Line: 70},
	}

	got := FindDuplicates(decls)

	want := []DuplicateGroup{
		{Name: "kafka_creds", Declarations: []Declaration{
			{ObjectType: KindNamedCollection, Name: "kafka_creds", File: "a/shared.hcl", Line: 60},
			{ObjectType: KindNamedCollection, Name: "kafka_creds", File: "b/aux.hcl", Line: 12},
		}},
		{Database: "posthog", Name: "legacy", Declarations: []Declaration{
			{ObjectType: KindTable, Database: "posthog", Name: "legacy", File: "a/shared.hcl", Line: 50},
			{ObjectType: KindRaw, Database: "posthog", Name: "legacy", File: "b/aux.hcl", Line: 8, RawKind: "table"},
		}},
		{Database: "posthog", Name: "person", Declarations: []Declaration{
			{ObjectType: KindTable, Database: "posthog", Name: "person", File: "a/shared.hcl", Line: 10},
			{ObjectType: KindTable, Database: "posthog", Name: "person", File: "b/aux.hcl", Line: 2},
		}},
	}
	assert.Equal(t, want, got)
}
