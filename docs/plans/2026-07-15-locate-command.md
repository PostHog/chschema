# `hclexp locate` Implementation Plan (issue #144)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A read-only `hclexp locate` command that finds every declaration site of an object across manifest layers and dump directories, derives which (role, env) stacks place each site, and (via `-duplicates`) lists objects declared at more than one plain site so CI can enforce the once-only rule even for layers that never co-compose.

**Architecture:** A lightweight declaration scanner in `internal/loader/hcl/locate.go` walks the `hclsyntax` AST of each `.hcl` file directly (no gohcl decode, no `Resolve`) — this is the only way to get `path:line`, because the decoded specs carry no source ranges. The scanner also reads the raw inheritance flags (`abstract`, `override`, `extend`, `patch_table`) that resolution consumes. CLI wiring in `cmd/hclexp/locate.go` parses the manifest across *all* envs (unlike `parseManifest`, which selects one), maps layers → files via the existing `hclload.LayerFiles`, derives placements, scans `-dump` files with the same scanner, and renders text/JSON. Duplicate detection lives in the internal package: group by `(database, name)` and report groups with ≥ 2 plain (non-patch, non-override, non-abstract) declarations.

**Tech Stack:** Go, `hashicorp/hcl/v2` (`hclparse` + `hclsyntax`), `zclconf/go-cty` (both already direct deps in go.mod), `stretchr/testify`.

## Global Constraints

- Never change the module name (`github.com/posthog/chschema`) or the `go` version in `go.mod`.
- Tests use testify `require`/`assert`; compare whole structs, not field-by-field.
- No parallel raw/resolved type hierarchies — one `Declaration` type serves scan, query, and duplicates.
- No comments that restate the code; comment only non-obvious whys.
- Run the full suite with `go test ./...` before every commit (CI runs `./internal/... ./cmd/...` with `-race` plus `./test/...`; `./internal/...` alone is not enough).
- Commit messages: 1-line summary, blank line, detailed description. **No AI attribution of any kind.**
- Commit signing may hit an agent lock ("agent refused operation"): ask for unlock, keep working uncommitted, retry once on go.
- This command is query-only: no diffing, no DDL, no writes.

## Design decisions locked in

- **Object identity for grouping and duplicates is `(database, name)`** — the ClickHouse namespace is shared across object types, so a `table "x"` and a `raw "table" "x"` (or a stray `view "x"`) collide into one entry; `Types` records every block type seen. Named collections use `database == ""`.
- **Duplicate rule:** a group is a duplicate when it has **≥ 2 plain declarations**, where a declaration is *plain* unless it is a `patch_table` (additive by design), has `override = true` (deliberate replacement), or has `abstract = true` (dropped at resolve, never materializes). `extend` children are plain declarations of their *own* name; extend-parent legitimacy falls out of the abstract exclusion. This matches the issue's "ignoring legitimate pairs (override/patch/extend)".
- **Pattern matching** mirrors `ExcludeMatcher.Match`: `filepath.Match` glob tried against the bare name and the `<database>.<name>` qualified form. A plain name with no metacharacters matches itself.
- **Exit codes:** 0 = found / no duplicates; 1 = nothing matched, duplicates found, or runtime error; 2 = usage error. (Same convention as `drift`.)
- **`-duplicates` scans manifest layers only** (requires `-manifest`, rejects `-dump` and a name argument) — dumps describe what a node *has*, not where authors declared it.
- **Out of scope:** `-exclude` support (the pattern argument is already a filter; add later if CI needs it), locating in a bare `-layer` stack without a manifest (the issue's synopsis has only `-manifest` and `-dump` sources), and rewriting anything.

## File Structure

- **Create** `internal/loader/hcl/locate.go` — `Declaration`, `ScanDeclarations`, `MatchesPattern`, `DuplicateGroup`, `FindDuplicates`.
- **Create** `internal/loader/hcl/locate_test.go` — scanner + duplicates unit tests.
- **Create** `cmd/hclexp/locate.go` — flag validation, `parseManifestAllEnvs`, `buildLocateDoc`, JSON doc types, text renderers, `runLocate`.
- **Create** `cmd/hclexp/locate_test.go` — CLI-layer tests over a temp manifest/layer/dump tree.
- **Modify** `cmd/hclexp/hclexp.go` — dispatch case (the `switch os.Args[1]` at `cmd/hclexp/hclexp.go:27`) and the `usage()` command list (~line 100).
- **Modify** `docs/README.hcl.md` — new `## Locating declarations — hclexp locate` section (insert after the `## Cross-role planning — hclexp plan` section).
- **Modify** `CLAUDE.md` — new bullet in Supported Features.

---

### Task 1: Declaration scanner (`ScanDeclarations`, `MatchesPattern`)

**Files:**
- Create: `internal/loader/hcl/locate.go`
- Test: `internal/loader/hcl/locate_test.go`

**Interfaces:**
- Consumes: `formatDiagnostics` (parser.go:79), `KindTable`/`KindMaterializedView`/`KindView`/`KindDictionary`/`KindRaw` (render.go:12–16), `KindNamedCollection` (sqlgen.go:42).
- Produces:
  - `type Declaration struct { ObjectType, Database, Name, File string; Line int; Abstract, Override, Patch bool; Extends, RawKind string }`
  - `func ScanDeclarations(files []string) ([]Declaration, error)`
  - `func MatchesPattern(pattern, database, name string) bool`

- [ ] **Step 1: Write the failing test**

Create `internal/loader/hcl/locate_test.go`:

```go
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
```

Note on line numbers: the heredoc content starts with a newline, so `database` is line 2, the first `table` block line 3, etc. If an assertion fails on lines, recount against the literal — do not loosen the assertion.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/loader/hcl/ -run 'TestScanDeclarations|TestMatchesPattern' -v`
Expected: compile error — `undefined: ScanDeclarations`, `undefined: Declaration`, `undefined: MatchesPattern`.

- [ ] **Step 3: Write the implementation**

Create `internal/loader/hcl/locate.go`:

```go
package hcl

import (
	"fmt"
	"path/filepath"

	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// Declaration is one declaration site of a schema object in an HCL source
// file. It comes from a lightweight syntax scan — no gohcl decode, no
// Resolve — which is what preserves the source position and the raw
// inheritance flags (abstract/override/extend/patch_table) that resolution
// consumes and the resolved specs no longer carry.
type Declaration struct {
	ObjectType string // KindTable, KindMaterializedView, KindView, KindDictionary, KindRaw, KindNamedCollection
	Database   string // empty for named collections (cluster-scoped)
	Name       string
	File       string
	Line       int

	Abstract bool
	Override bool
	Patch    bool   // declared via patch_table (strictly additive)
	Extends  string // the extend = "<parent>" attribute, when present
	RawKind  string // raw blocks only: the kind label
}

// ScanDeclarations records every object declaration site in the given files,
// in file order. Files must be native HCL syntax, same as the loader; a
// parse error aborts the scan.
func ScanDeclarations(files []string) ([]Declaration, error) {
	var out []Declaration
	for _, path := range files {
		decls, err := scanDeclarationFile(path)
		if err != nil {
			return nil, err
		}
		out = append(out, decls...)
	}
	return out, nil
}

func scanDeclarationFile(path string) ([]Declaration, error) {
	parser := hclparse.NewParser()
	f, diags := parser.ParseHCLFile(path)
	if diags.HasErrors() {
		return nil, formatDiagnostics(parser, diags)
	}
	body, ok := f.Body.(*hclsyntax.Body)
	if !ok {
		return nil, fmt.Errorf("%s: not native HCL syntax", path)
	}
	var out []Declaration
	for _, blk := range body.Blocks {
		switch blk.Type {
		case "database":
			if len(blk.Labels) != 1 {
				continue
			}
			for _, obj := range blk.Body.Blocks {
				if d, ok := objectDeclaration(obj, blk.Labels[0], path); ok {
					out = append(out, d)
				}
			}
		case "named_collection":
			if len(blk.Labels) != 1 {
				continue
			}
			out = append(out, Declaration{
				ObjectType: KindNamedCollection,
				Name:       blk.Labels[0],
				File:       path,
				Line:       blk.DefRange().Start.Line,
				Override:   boolAttr(blk.Body, "override"),
			})
		}
	}
	return out, nil
}

// objectDeclaration converts one block nested in a database{} into a
// Declaration. Blocks that are not object declarations (node, column,
// unknown types) report ok = false.
func objectDeclaration(blk *hclsyntax.Block, database, path string) (Declaration, bool) {
	d := Declaration{Database: database, File: path, Line: blk.DefRange().Start.Line}
	switch blk.Type {
	case "table":
		d.ObjectType = KindTable
	case "patch_table":
		d.ObjectType = KindTable
		d.Patch = true
	case "materialized_view":
		d.ObjectType = KindMaterializedView
	case "view":
		d.ObjectType = KindView
	case "dictionary":
		d.ObjectType = KindDictionary
	case "raw":
		if len(blk.Labels) != 2 {
			return Declaration{}, false
		}
		d.ObjectType = KindRaw
		d.RawKind = blk.Labels[0]
		d.Name = blk.Labels[1]
		return d, true
	default:
		return Declaration{}, false
	}
	if len(blk.Labels) != 1 {
		return Declaration{}, false
	}
	d.Name = blk.Labels[0]
	d.Abstract = boolAttr(blk.Body, "abstract")
	d.Override = boolAttr(blk.Body, "override")
	d.Extends = stringAttr(blk.Body, "extend")
	return d, true
}

// boolAttr reads a literal boolean attribute. Anything the scan cannot
// evaluate statically reads as unset — the full loader is the authority on
// whether the file actually decodes.
func boolAttr(body *hclsyntax.Body, name string) bool {
	attr, ok := body.Attributes[name]
	if !ok {
		return false
	}
	v, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || v.IsNull() || v.Type() != cty.Bool {
		return false
	}
	return v.True()
}

func stringAttr(body *hclsyntax.Body, name string) string {
	attr, ok := body.Attributes[name]
	if !ok {
		return ""
	}
	v, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || v.IsNull() || v.Type() != cty.String {
		return ""
	}
	return v.AsString()
}

// MatchesPattern reports whether a locate pattern (a filepath.Match glob, or
// a plain name) matches an object, trying the bare name and the qualified
// "<database>.<name>" form — the same convention exclude patterns use.
func MatchesPattern(pattern, database, name string) bool {
	if ok, _ := filepath.Match(pattern, name); ok {
		return true
	}
	if database == "" {
		return false
	}
	ok, _ := filepath.Match(pattern, database+"."+name)
	return ok
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/loader/hcl/ -run 'TestScanDeclarations|TestMatchesPattern' -v`
Expected: PASS (3 tests). If go-cty was somehow not a direct dep, `go mod tidy` would fix it — go.mod line 12 already lists `github.com/zclconf/go-cty v1.16.3`, so no change expected; **do not touch the module name or go version**.

- [ ] **Step 5: Run the package suite and commit**

Run: `go test ./internal/loader/hcl/`
Expected: ok.

```bash
git add internal/loader/hcl/locate.go internal/loader/hcl/locate_test.go
git commit -m "locate: declaration scanner over raw HCL syntax

Walks hclsyntax blocks directly (no gohcl decode, no Resolve) to record
every object declaration site with file:line plus the raw inheritance
flags (abstract, override, extend, patch_table) — the resolved specs
carry neither positions nor those flags. Adds MatchesPattern, the
name-or-glob matcher locate shares with the exclude convention (bare
name and db.name qualified form). Groundwork for hclexp locate (#144)."
```

---

### Task 2: Duplicate detection (`FindDuplicates`)

**Files:**
- Modify: `internal/loader/hcl/locate.go` (append)
- Test: `internal/loader/hcl/locate_test.go` (append)

**Interfaces:**
- Consumes: `Declaration` from Task 1.
- Produces:
  - `type DuplicateGroup struct { Database, Name string; Declarations []Declaration }`
  - `func FindDuplicates(decls []Declaration) []DuplicateGroup`

- [ ] **Step 1: Write the failing test**

Append to `internal/loader/hcl/locate_test.go`:

```go
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
```

(Expected order: groups sorted by database then name — `kafka_creds` has the empty database, so it sorts first; declarations within a group sorted by file then line.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/loader/hcl/ -run TestFindDuplicates -v`
Expected: compile error — `undefined: FindDuplicates`.

- [ ] **Step 3: Write the implementation**

Append to `internal/loader/hcl/locate.go` (add `"sort"` to the imports):

```go
// DuplicateGroup is one object name declared at more than one site without
// the inheritance system explaining the extras: patch_table sites are
// additive, override = true is a deliberate replacement, and abstract
// declarations are dropped at resolve and never materialize. A group
// qualifies when at least two plain declarations remain.
type DuplicateGroup struct {
	Database     string
	Name         string
	Declarations []Declaration // every site for the name, legitimate ones included
}

// FindDuplicates groups declarations by (database, name) — the ClickHouse
// namespace, which object types share — and returns the groups holding two
// or more plain (non-patch, non-override, non-abstract) declarations,
// sorted by database then name.
func FindDuplicates(decls []Declaration) []DuplicateGroup {
	type key struct{ db, name string }
	byKey := map[key][]Declaration{}
	for _, d := range decls {
		k := key{d.Database, d.Name}
		byKey[k] = append(byKey[k], d)
	}
	var out []DuplicateGroup
	for k, group := range byKey {
		plain := 0
		for _, d := range group {
			if !d.Patch && !d.Override && !d.Abstract {
				plain++
			}
		}
		if plain < 2 {
			continue
		}
		sort.Slice(group, func(i, j int) bool {
			if group[i].File != group[j].File {
				return group[i].File < group[j].File
			}
			return group[i].Line < group[j].Line
		})
		out = append(out, DuplicateGroup{Database: k.db, Name: k.name, Declarations: group})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Database != out[j].Database {
			return out[i].Database < out[j].Database
		}
		return out[i].Name < out[j].Name
	})
	return out
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/loader/hcl/ -run TestFindDuplicates -v`
Expected: PASS.

- [ ] **Step 5: Run the package suite and commit**

Run: `go test ./internal/loader/hcl/`
Expected: ok.

```bash
git add internal/loader/hcl/locate.go internal/loader/hcl/locate_test.go
git commit -m "locate: duplicate declaration detection

Groups declaration sites by (database, name) — the namespace ClickHouse
object types share — and reports every group with two or more plain
declarations. patch_table, override = true, and abstract sites are
legitimate extras: patches are additive, overrides deliberate, abstracts
never materialize. This catches same-name copies in layers that never
co-compose, which LoadLayers alone cannot see (#144)."
```

---

### Task 3: CLI doc building — manifest across all envs, placements, dumps, renderers

**Files:**
- Create: `cmd/hclexp/locate.go`
- Test: `cmd/hclexp/locate_test.go`

**Interfaces:**
- Consumes: `planManifest`/`manifestRoleBlock`/`manifestEnvBlock` (cmd/hclexp/plan.go:27–45), `hclload.LayerFiles`, `hclload.ScanDeclarations`, `hclload.MatchesPattern`, `hclload.FindDuplicates`, `qualifiedName` (cmd/hclexp/hclexp.go:810).
- Produces (all in package `main`):
  - `type locateStack struct { Role, Env string; Layers []string }`
  - `func locateFlagsError(manifest, dump, format string, duplicates bool, nargs int, pattern string) error`
  - `func parseManifestAllEnvs(path string) ([]locateStack, error)`
  - `func buildLocateDoc(stacks []locateStack, layerRoot, dumpDir, pattern string, duplicates bool) (locateDoc, error)`
  - `func renderLocateText(w io.Writer, doc locateDoc)` and `func renderDuplicatesText(w io.Writer, doc locateDoc)`
  - JSON doc types `locateDoc`, `locateObject`, `locateDecl`, `locateDump`, `locatePlacement` (shapes below).

- [ ] **Step 1: Write the failing tests**

Create `cmd/hclexp/locate_test.go`:

```go
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// locateTree writes a small manifest + layer tree + dump dir:
//
//	shared/base.hcl      abstract events_base, plain person
//	ingestion/events.hcl events extending events_base
//	aux/dup.hcl          a second plain person (the accidental duplicate)
//	dumps/node1.hcl      a node dump declaring posthog.events
//
// manifest.hcl deploys (ingestion, prod-us), (ingestion, prod-eu) on
// shared+ingestion and (aux, prod-us) on shared+aux.
func locateTree(t *testing.T) (root string) {
	t.Helper()
	root = t.TempDir()
	mustWrite := func(rel, content string) {
		t.Helper()
		path := filepath.Join(root, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}

	mustWrite("manifest.hcl", `
role "ingestion" {
  env "prod-us" { layers = ["shared", "ingestion"] }
  env "prod-eu" { layers = ["shared", "ingestion"] }
}
role "aux" {
  env "prod-us" { layers = ["shared", "aux"] }
}
`)
	mustWrite("shared/base.hcl", `
database "posthog" {
  table "events_base" {
    abstract = true
    column "uuid" { type = "UUID" }
  }
  table "person" {
    engine "MergeTree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
}
`)
	mustWrite("ingestion/events.hcl", `
database "posthog" {
  table "events" {
    extend   = "events_base"
    engine "MergeTree" {}
    order_by = ["uuid"]
  }
}
`)
	mustWrite("aux/dup.hcl", `
database "posthog" {
  table "person" {
    engine "MergeTree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
}
`)
	mustWrite("dumps/node1.hcl", `
node "node1" {}
database "posthog" {
  table "events" {
    engine "MergeTree" {}
    order_by = ["uuid"]
    column "uuid" { type = "UUID" }
  }
}
`)
	return root
}

func TestLocateFlagsError(t *testing.T) {
	assert.NoError(t, locateFlagsError("m.hcl", "", "text", false, 1, "events"))
	assert.NoError(t, locateFlagsError("", "dumps", "json", false, 1, "events"))
	assert.NoError(t, locateFlagsError("m.hcl", "", "text", true, 0, ""))

	assert.Error(t, locateFlagsError("", "", "text", false, 1, "events"), "needs a source")
	assert.Error(t, locateFlagsError("m.hcl", "", "yaml", false, 1, "events"), "bad format")
	assert.Error(t, locateFlagsError("m.hcl", "", "text", false, 0, ""), "missing name")
	assert.Error(t, locateFlagsError("m.hcl", "", "text", false, 2, "a"), "too many args")
	assert.Error(t, locateFlagsError("m.hcl", "", "text", false, 1, "[bad"), "invalid glob")
	assert.Error(t, locateFlagsError("", "dumps", "text", true, 0, ""), "-duplicates without manifest")
	assert.Error(t, locateFlagsError("m.hcl", "dumps", "text", true, 0, ""), "-duplicates with -dump")
	assert.Error(t, locateFlagsError("m.hcl", "", "text", true, 1, "x"), "-duplicates with name")
}

func TestParseManifestAllEnvs(t *testing.T) {
	root := locateTree(t)

	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	want := []locateStack{
		{Role: "ingestion", Env: "prod-us", Layers: []string{"shared", "ingestion"}},
		{Role: "ingestion", Env: "prod-eu", Layers: []string{"shared", "ingestion"}},
		{Role: "aux", Env: "prod-us", Layers: []string{"shared", "aux"}},
	}
	assert.Equal(t, want, stacks)
}

func TestBuildLocateDocFindsDeclarationsAndPlacements(t *testing.T) {
	root := locateTree(t)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	doc, err := buildLocateDoc(stacks, root, filepath.Join(root, "dumps"), "events", false)
	require.NoError(t, err)

	require.Len(t, doc.Objects, 1)
	obj := doc.Objects[0]
	assert.Equal(t, "posthog", obj.Database)
	assert.Equal(t, "events", obj.Name)
	assert.Equal(t, []string{"table"}, obj.Types)

	require.Len(t, obj.Declarations, 1)
	d := obj.Declarations[0]
	assert.Equal(t, filepath.Join(root, "ingestion", "events.hcl"), d.File)
	assert.Equal(t, 3, d.Line)
	assert.Equal(t, filepath.Join(root, "ingestion"), d.Layer)
	assert.Equal(t, "events_base", d.Extends)
	assert.Equal(t, []locatePlacement{
		{Role: "ingestion", Env: "prod-us"},
		{Role: "ingestion", Env: "prod-eu"},
	}, d.Placements)

	assert.Equal(t, []locateDump{
		{File: filepath.Join(root, "dumps", "node1.hcl"), Line: 4, Type: "table"},
	}, obj.Dumps)
}

func TestBuildLocateDocGlobAndSharedLayerPlacements(t *testing.T) {
	root := locateTree(t)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	doc, err := buildLocateDoc(stacks, root, "", "person", false)
	require.NoError(t, err)

	require.Len(t, doc.Objects, 1)
	obj := doc.Objects[0]
	require.Len(t, obj.Declarations, 2)

	shared, aux := obj.Declarations[0], obj.Declarations[1]
	assert.Equal(t, filepath.Join(root, "shared", "base.hcl"), shared.File)
	assert.Equal(t, []locatePlacement{
		{Role: "ingestion", Env: "prod-us"},
		{Role: "ingestion", Env: "prod-eu"},
		{Role: "aux", Env: "prod-us"},
	}, shared.Placements)
	assert.Equal(t, filepath.Join(root, "aux", "dup.hcl"), aux.File)
	assert.Equal(t, []locatePlacement{{Role: "aux", Env: "prod-us"}}, aux.Placements)
}

func TestBuildLocateDocNoMatch(t *testing.T) {
	root := locateTree(t)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	doc, err := buildLocateDoc(stacks, root, "", "nosuchobject", false)
	require.NoError(t, err)
	assert.Empty(t, doc.Objects)
}

func TestBuildLocateDocDuplicates(t *testing.T) {
	root := locateTree(t)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)

	doc, err := buildLocateDoc(stacks, root, "", "", true)
	require.NoError(t, err)

	// person is declared plainly in shared and aux; events_base (abstract) +
	// events (extend child) must not be flagged.
	require.Len(t, doc.Duplicates, 1)
	dup := doc.Duplicates[0]
	assert.Equal(t, "posthog", dup.Database)
	assert.Equal(t, "person", dup.Name)
	require.Len(t, dup.Declarations, 2)
	assert.Equal(t, filepath.Join(root, "aux", "dup.hcl"), dup.Declarations[0].File)
	assert.Equal(t, filepath.Join(root, "shared", "base.hcl"), dup.Declarations[1].File)
}

func TestRenderLocateText(t *testing.T) {
	root := locateTree(t)
	stacks, err := parseManifestAllEnvs(filepath.Join(root, "manifest.hcl"))
	require.NoError(t, err)
	doc, err := buildLocateDoc(stacks, root, filepath.Join(root, "dumps"), "events", false)
	require.NoError(t, err)

	var buf bytes.Buffer
	renderLocateText(&buf, doc)
	out := buf.String()
	assert.Contains(t, out, "table posthog.events")
	assert.Contains(t, out, "events.hcl:3")
	assert.Contains(t, out, "extends events_base")
	assert.Contains(t, out, "(ingestion, prod-us)")
	assert.Contains(t, out, "dump: ")

	var dupBuf bytes.Buffer
	dupDoc, err := buildLocateDoc(stacks, root, "", "", true)
	require.NoError(t, err)
	renderDuplicatesText(&dupBuf, dupDoc)
	assert.Contains(t, dupBuf.String(), "duplicate table posthog.person")
	assert.Contains(t, dupBuf.String(), "base.hcl:7")
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./cmd/hclexp/ -run 'TestLocate|TestParseManifestAllEnvs|TestBuildLocateDoc|TestRenderLocate' -v`
Expected: compile error — `undefined: locateFlagsError` etc.

- [ ] **Step 3: Write the implementation**

Create `cmd/hclexp/locate.go`:

```go
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclparse"
	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

// locateStack is one (role, env) deployment from the manifest and its
// declared layer stack.
type locateStack struct {
	Role   string
	Env    string
	Layers []string
}

type locatePlacement struct {
	Role string `json:"role"`
	Env  string `json:"env"`
}

// locateDecl is one declaration site plus its derived placements: the
// (role, env) stacks whose layer lists include the declaring layer.
type locateDecl struct {
	File       string            `json:"file"`
	Line       int               `json:"line"`
	Layer      string            `json:"layer,omitempty"`
	Type       string            `json:"type"`
	Abstract   bool              `json:"abstract,omitempty"`
	Override   bool              `json:"override,omitempty"`
	Patch      bool              `json:"patch,omitempty"`
	Extends    string            `json:"extends,omitempty"`
	RawKind    string            `json:"raw_kind,omitempty"`
	Placements []locatePlacement `json:"placements,omitempty"`
}

type locateDump struct {
	File string `json:"file"`
	Line int    `json:"line"`
	Type string `json:"type"`
}

// locateObject collects every declaration site of one object name. Objects
// are keyed by (database, name) — the namespace ClickHouse object types
// share — so a table and a raw block with the same name land in one entry,
// with Types recording each block type seen.
type locateObject struct {
	Database     string       `json:"database,omitempty"`
	Name         string       `json:"name"`
	Types        []string     `json:"types"`
	Declarations []locateDecl `json:"declarations"`
	Dumps        []locateDump `json:"dumps,omitempty"`
}

// locateDoc is the `locate -format json` document. Objects carries the
// pattern query's results; Duplicates carries -duplicates mode's. Exactly
// one of the two is populated (non-nil, so JSON emits [] rather than null).
type locateDoc struct {
	Pattern    string         `json:"pattern,omitempty"`
	Objects    []locateObject `json:"objects,omitempty"`
	Duplicates []locateObject `json:"duplicates,omitempty"`
}

// locateFlagsError reports the usage error in a locate invocation, if any.
// Pure so the exit-2 paths are testable without a subprocess.
func locateFlagsError(manifest, dump, format string, duplicates bool, nargs int, pattern string) error {
	if format != "text" && format != "json" {
		return fmt.Errorf("invalid -format %q (want text or json)", format)
	}
	if duplicates {
		if manifest == "" {
			return fmt.Errorf("-duplicates requires -manifest (it audits authored layers)")
		}
		if dump != "" {
			return fmt.Errorf("-duplicates and -dump are mutually exclusive")
		}
		if nargs != 0 {
			return fmt.Errorf("-duplicates takes no name argument")
		}
		return nil
	}
	if manifest == "" && dump == "" {
		return fmt.Errorf("at least one of -manifest or -dump is required")
	}
	if nargs != 1 {
		return fmt.Errorf("exactly one <name-or-glob> argument is required")
	}
	if _, err := filepath.Match(pattern, ""); err != nil {
		return fmt.Errorf("invalid pattern %q: %w", pattern, err)
	}
	return nil
}

// parseManifestAllEnvs decodes the manifest into one locateStack per
// (role, env) pair across every environment — unlike parseManifest, which
// selects a single env — with the same duplicate-role/env checks.
func parseManifestAllEnvs(path string) ([]locateStack, error) {
	parser := hclparse.NewParser()
	f, diags := parser.ParseHCLFile(path)
	if diags.HasErrors() {
		return nil, fmt.Errorf("%s", diags)
	}
	var m planManifest
	if diags := gohcl.DecodeBody(f.Body, nil, &m); diags.HasErrors() {
		return nil, fmt.Errorf("%s", diags)
	}
	if len(m.Roles) == 0 {
		return nil, fmt.Errorf("manifest declares no roles")
	}
	var stacks []locateStack
	seenRole := map[string]bool{}
	for _, rb := range m.Roles {
		if seenRole[rb.Name] {
			return nil, fmt.Errorf("duplicate role %q", rb.Name)
		}
		seenRole[rb.Name] = true
		seenEnv := map[string]bool{}
		for _, eb := range rb.Envs {
			if seenEnv[eb.Name] {
				return nil, fmt.Errorf("role %q: duplicate env %q", rb.Name, eb.Name)
			}
			seenEnv[eb.Name] = true
			if len(eb.Layers) == 0 {
				return nil, fmt.Errorf("role %q env %q: layers is empty", rb.Name, eb.Name)
			}
			stacks = append(stacks, locateStack{Role: rb.Name, Env: eb.Name, Layers: eb.Layers})
		}
	}
	return stacks, nil
}

// buildLocateDoc scans every layer the manifest references (each unique
// resolved layer once) plus the dump directory, and groups the matching
// declaration sites by (database, name). With duplicates = true the pattern
// is ignored and the doc's Duplicates side is populated instead.
func buildLocateDoc(stacks []locateStack, layerRoot, dumpDir, pattern string, duplicates bool) (locateDoc, error) {
	// Index which (role, env) stacks include each resolved layer, keeping
	// first-seen layer order so output is stable.
	stacksByLayer := map[string][]locatePlacement{}
	var layerOrder []string
	for _, s := range stacks {
		for _, l := range s.Layers {
			resolved := filepath.Join(layerRoot, l)
			if _, ok := stacksByLayer[resolved]; !ok {
				layerOrder = append(layerOrder, resolved)
			}
			stacksByLayer[resolved] = appendUniquePlacement(stacksByLayer[resolved], locatePlacement{Role: s.Role, Env: s.Env})
		}
	}

	// Scan each file once; a file reachable through several layers (e.g. a
	// dir layer and the same file listed directly) keeps its first
	// attribution.
	var decls []hclload.Declaration
	layerByFile := map[string]string{}
	for _, layer := range layerOrder {
		files, err := hclload.LayerFiles(layer)
		if err != nil {
			return locateDoc{}, err
		}
		for _, file := range files {
			if _, ok := layerByFile[file]; ok {
				continue
			}
			layerByFile[file] = layer
			fileDecls, err := hclload.ScanDeclarations([]string{file})
			if err != nil {
				return locateDoc{}, err
			}
			decls = append(decls, fileDecls...)
		}
	}

	if duplicates {
		doc := locateDoc{Duplicates: []locateObject{}}
		for _, g := range hclload.FindDuplicates(decls) {
			obj := locateObject{Database: g.Database, Name: g.Name}
			for _, d := range g.Declarations {
				obj.Types = appendUniqueString(obj.Types, d.ObjectType)
				obj.Declarations = append(obj.Declarations, toLocateDecl(d, layerByFile, stacksByLayer))
			}
			doc.Duplicates = append(doc.Duplicates, obj)
		}
		return doc, nil
	}

	doc := locateDoc{Pattern: pattern, Objects: []locateObject{}}
	type key struct{ db, name string }
	index := map[key]int{}
	upsert := func(db, name string) *locateObject {
		k := key{db, name}
		if i, ok := index[k]; ok {
			return &doc.Objects[i]
		}
		index[k] = len(doc.Objects)
		doc.Objects = append(doc.Objects, locateObject{Database: db, Name: name, Declarations: []locateDecl{}})
		return &doc.Objects[len(doc.Objects)-1]
	}

	for _, d := range decls {
		if !hclload.MatchesPattern(pattern, d.Database, d.Name) {
			continue
		}
		obj := upsert(d.Database, d.Name)
		obj.Types = appendUniqueString(obj.Types, d.ObjectType)
		obj.Declarations = append(obj.Declarations, toLocateDecl(d, layerByFile, stacksByLayer))
	}

	if dumpDir != "" {
		files, err := filepath.Glob(filepath.Join(dumpDir, "*.hcl"))
		if err != nil {
			return locateDoc{}, fmt.Errorf("dump dir %q: %w", dumpDir, err)
		}
		sort.Strings(files)
		for _, file := range files {
			dumpDecls, err := hclload.ScanDeclarations([]string{file})
			if err != nil {
				return locateDoc{}, err
			}
			for _, d := range dumpDecls {
				if !hclload.MatchesPattern(pattern, d.Database, d.Name) {
					continue
				}
				obj := upsert(d.Database, d.Name)
				obj.Types = appendUniqueString(obj.Types, d.ObjectType)
				obj.Dumps = append(obj.Dumps, locateDump{File: d.File, Line: d.Line, Type: d.ObjectType})
			}
		}
	}

	sort.SliceStable(doc.Objects, func(i, j int) bool {
		if doc.Objects[i].Database != doc.Objects[j].Database {
			return doc.Objects[i].Database < doc.Objects[j].Database
		}
		return doc.Objects[i].Name < doc.Objects[j].Name
	})
	return doc, nil
}

func toLocateDecl(d hclload.Declaration, layerByFile map[string]string, stacksByLayer map[string][]locatePlacement) locateDecl {
	layer := layerByFile[d.File]
	return locateDecl{
		File:       d.File,
		Line:       d.Line,
		Layer:      layer,
		Type:       d.ObjectType,
		Abstract:   d.Abstract,
		Override:   d.Override,
		Patch:      d.Patch,
		Extends:    d.Extends,
		RawKind:    d.RawKind,
		Placements: stacksByLayer[layer],
	}
}

func appendUniquePlacement(ps []locatePlacement, p locatePlacement) []locatePlacement {
	for _, x := range ps {
		if x == p {
			return ps
		}
	}
	return append(ps, p)
}

func appendUniqueString(ss []string, s string) []string {
	for _, x := range ss {
		if x == s {
			return ss
		}
	}
	return append(ss, s)
}

// declMarkers renders a site's control flags for the text output, e.g.
// " [abstract]" or " extends events_base [override]".
func declMarkers(d locateDecl) string {
	var parts []string
	if d.Extends != "" {
		parts = append(parts, "extends "+d.Extends)
	}
	if d.Abstract {
		parts = append(parts, "[abstract]")
	}
	if d.Override {
		parts = append(parts, "[override]")
	}
	if d.Patch {
		parts = append(parts, "[patch_table]")
	}
	if d.RawKind != "" {
		parts = append(parts, "[raw "+d.RawKind+"]")
	}
	if len(parts) == 0 {
		return ""
	}
	return "  " + strings.Join(parts, " ")
}

func formatPlacements(ps []locatePlacement) string {
	parts := make([]string, 0, len(ps))
	for _, p := range ps {
		parts = append(parts, fmt.Sprintf("(%s, %s)", p.Role, p.Env))
	}
	return strings.Join(parts, ", ")
}

func renderLocateSites(w io.Writer, o locateObject) {
	for _, d := range o.Declarations {
		fmt.Fprintf(w, "  %s:%d%s\n", d.File, d.Line, declMarkers(d))
		if len(d.Placements) > 0 {
			fmt.Fprintf(w, "      %s\n", formatPlacements(d.Placements))
		}
	}
	for _, dp := range o.Dumps {
		fmt.Fprintf(w, "  dump: %s:%d\n", dp.File, dp.Line)
	}
}

func renderLocateText(w io.Writer, doc locateDoc) {
	for _, o := range doc.Objects {
		fmt.Fprintf(w, "%s %s\n", strings.Join(o.Types, "|"), qualifiedName(o.Database, o.Name))
		renderLocateSites(w, o)
	}
}

func renderDuplicatesText(w io.Writer, doc locateDoc) {
	if len(doc.Duplicates) == 0 {
		fmt.Fprintln(w, "no duplicate declarations")
		return
	}
	for _, o := range doc.Duplicates {
		fmt.Fprintf(w, "duplicate %s %s (%d sites)\n", strings.Join(o.Types, "|"), qualifiedName(o.Database, o.Name), len(o.Declarations))
		renderLocateSites(w, o)
	}
}
```

(`runLocate` comes in Task 4; keeping it out of this task means this task compiles and tests as a unit. `flag`, `json`, `slog`, `os` imports arrive with Task 4 — drop them from the import block here and let Task 4 add them, or add them now and reference them in Task 4; either way `gofmt`/`go vet` must be clean at commit time, so remove any unused import before committing.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./cmd/hclexp/ -run 'TestLocate|TestParseManifestAllEnvs|TestBuildLocateDoc|TestRenderLocate' -v`
Expected: PASS (7 tests). Line-number assertions count lines in the heredoc literals (leading newline makes `database` line 2).

- [ ] **Step 5: Run the package suite and commit**

Run: `go test ./cmd/hclexp/ ./internal/loader/hcl/`
Expected: ok.

```bash
git add cmd/hclexp/locate.go cmd/hclexp/locate_test.go
git commit -m "locate: doc building, placements and renderers

parseManifestAllEnvs walks every (role, env) pair — unlike
parseManifest's single-env selection — so a declaration site can report
every stack whose layer list includes its layer. buildLocateDoc scans
each unique resolved layer once via LayerFiles + ScanDeclarations,
groups matching sites by (database, name), and attaches dump-directory
sites scanned the same way. Text and JSON renderers share the one doc
shape, so the two formats cannot disagree (#144)."
```

---

### Task 4: `runLocate` + dispatch + usage

**Files:**
- Modify: `cmd/hclexp/locate.go` (append `runLocate`)
- Modify: `cmd/hclexp/hclexp.go` (dispatch switch at :27, usage text ~:100)

**Interfaces:**
- Consumes: everything from Task 3; `isDir` (cmd/hclexp/load_manifest.go:197).
- Produces: `func runLocate(args []string)`, `locate` subcommand.

- [ ] **Step 1: Append `runLocate` to `cmd/hclexp/locate.go`**

```go
// runLocate answers "where is object X declared?" across a manifest's layer
// tree and/or a dump directory, or (with -duplicates) audits the layer tree
// for objects declared at more than one plain site. Read-only; exits 1 when
// nothing matches or duplicates exist, 2 on usage errors.
func runLocate(args []string) {
	fs := flag.NewFlagSet("hclexp locate", flag.ExitOnError)
	manifestFlag := fs.String("manifest", "", "HCL manifest: role blocks with env blocks; every (role, env) stack is searched")
	layerRootFlag := fs.String("layer-root", ".", "root directory the manifest's layer paths resolve under")
	dumpFlag := fs.String("dump", "", "directory of per-node .hcl dumps to search as well")
	formatFlag := fs.String("format", "text", "output format: text (default) or json")
	duplicatesFlag := fs.Bool("duplicates", false, "list every object declared at more than one plain site (override/patch/abstract sites are legitimate); takes no name argument")
	_ = fs.Parse(args)

	pattern := fs.Arg(0)
	if err := locateFlagsError(*manifestFlag, *dumpFlag, *formatFlag, *duplicatesFlag, fs.NArg(), pattern); err != nil {
		slog.Error("invalid locate invocation", "err", err)
		os.Exit(2)
	}
	if *dumpFlag != "" && !isDir(*dumpFlag) {
		slog.Error("dump directory does not exist", "dir", *dumpFlag)
		os.Exit(1)
	}

	var stacks []locateStack
	if *manifestFlag != "" {
		var err error
		stacks, err = parseManifestAllEnvs(*manifestFlag)
		if err != nil {
			slog.Error("failed to parse manifest", "file", *manifestFlag, "err", err)
			os.Exit(1)
		}
	}

	doc, err := buildLocateDoc(stacks, *layerRootFlag, *dumpFlag, pattern, *duplicatesFlag)
	if err != nil {
		slog.Error("locate failed", "err", err)
		os.Exit(1)
	}

	if *formatFlag == "json" {
		out, err := json.MarshalIndent(doc, "", "  ")
		if err != nil {
			slog.Error("failed to render JSON", "err", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	} else if *duplicatesFlag {
		renderDuplicatesText(os.Stdout, doc)
	} else {
		renderLocateText(os.Stdout, doc)
	}

	if *duplicatesFlag {
		if len(doc.Duplicates) > 0 {
			os.Exit(1)
		}
		return
	}
	if len(doc.Objects) == 0 {
		fmt.Fprintf(os.Stderr, "locate: no objects match %q\n", pattern)
		os.Exit(1)
	}
}
```

Add the now-needed imports to `cmd/hclexp/locate.go`: `encoding/json`, `flag`, `log/slog`, `os`.

- [ ] **Step 2: Wire the dispatch and usage**

In `cmd/hclexp/hclexp.go`, add to the `switch os.Args[1]` (keep alphabetical-ish grouping; after the `"drift"` case is fine):

```go
	case "locate":
		runLocate(os.Args[2:])
		return
```

In `usage()`, after the `drift` line:

```
  locate       find every declaration site of an object across manifest
               layers and dump directories (-duplicates audits the once-only rule)
```

- [ ] **Step 3: Build and verify against the test tree by hand**

```bash
go build -o hclexp ./cmd/hclexp && go vet ./cmd/... ./internal/...
```
Expected: clean build, no vet findings.

Smoke-run against a throwaway tree (mirror the `locateTree` layout in the scratchpad directory, not in the repo), e.g.:

```bash
./hclexp locate -manifest $SCRATCH/manifest.hcl -layer-root $SCRATCH events
./hclexp locate -manifest $SCRATCH/manifest.hcl -layer-root $SCRATCH -format json 'posthog.*'
./hclexp locate -manifest $SCRATCH/manifest.hcl -layer-root $SCRATCH -duplicates; echo "exit=$?"
./hclexp locate -manifest $SCRATCH/manifest.hcl -layer-root $SCRATCH nosuch; echo "exit=$?"
```
Expected: sites with `file:line`, placements per stack; `-duplicates` prints `duplicate table posthog.person (2 sites)` and exits 1; the no-match run prints `locate: no objects match "nosuch"` and exits 1.

- [ ] **Step 4: Run the full suite**

Run: `go test ./...`
Expected: all packages ok (live tests skip without `-clickhouse`).

- [ ] **Step 5: Commit**

```bash
git add cmd/hclexp/locate.go cmd/hclexp/hclexp.go
git commit -m "locate: wire the subcommand

hclexp locate <name-or-glob> answers where an object is declared across
every (role, env) stack in the manifest, and which node dump files
declare it with -dump. -duplicates flips it into a CI audit: exit 1
when any object has two or more plain declarations, catching same-name
copies in layers that never co-compose. Exit 1 also signals a query
that matched nothing, so existence checks are scriptable (#144)."
```

---

### Task 5: Documentation

**Files:**
- Modify: `docs/README.hcl.md` (insert a new `##` section after `## Cross-role planning — hclexp plan`, before whatever follows it)
- Modify: `CLAUDE.md` (Supported Features)

- [ ] **Step 1: Add the README section**

Insert into `docs/README.hcl.md`:

````markdown
## Locating declarations — `hclexp locate`

`locate` answers two questions the layer tree makes hard to grep for:
*where is object X declared?* and *is X declared more than once?* It is
query-only — no diffing, no DDL.

```bash
# Every declaration site of a table, and which (role, env) stacks place it
hclexp locate -manifest manifest.hcl -layer-root ./schema events

# Globs work like exclude patterns: bare name or db.name qualified
hclexp locate -manifest manifest.hcl -layer-root ./schema 'posthog.person_*'

# Also search per-node dumps (introspect / dump-cluster output)
hclexp locate -manifest manifest.hcl -layer-root ./schema -dump ./dumps events

# CI guard: any object declared at more than one plain site exits 1
hclexp locate -manifest manifest.hcl -layer-root ./schema -duplicates
```

For each matching object (tables, MVs, views, dictionaries, named
collections, raw blocks), `locate` lists every declaration site as
`file:line` plus its control flags (`[abstract]`, `[override]`,
`[patch_table]`, `extends <parent>`, `[raw <kind>]`), and derives the
**placements**: the (role, env) stacks whose manifest layer lists include
the declaring layer. Unlike `plan`/`load`, the manifest is read across
*all* envs. With `-dump DIR`, the per-node `.hcl` dump files declaring the
object are listed too. `-format json` emits the same document
structurally.

Objects are grouped by `(database, name)` — the namespace ClickHouse
object types share — so a stray `view "events"` next to a `table
"events"` shows up as one entry with both types.

`-duplicates` (no name argument) audits the once-only discipline:
`load`/compose only reject a redeclaration when the two layers meet in one
stack, so two layers that never co-compose can silently hold divergent
copies of the same object. A site is a *plain* declaration unless it is a
`patch_table` (additive), has `override = true` (deliberate replacement),
or is `abstract` (dropped at resolve); any object with two or more plain
sites is reported and the command exits 1.

Exit codes: 0 found / no duplicates; 1 no match, duplicates found, or a
load error; 2 usage.
````

- [ ] **Step 2: Add the CLAUDE.md bullet**

In `CLAUDE.md`, add a subsection after `### Cross-role planning (hclexp plan)`:

```markdown
### Locating declarations (`hclexp locate`)
- ✅ `locate <name-or-glob>` lists every declaration site (`file:line` +
  abstract/override/patch/extend flags) of matching objects across all
  manifest layers, with derived (role, env) placements read from the
  manifest across *every* env; `-dump DIR` also lists the per-node dump
  files declaring the object; `-format text|json`
- ✅ `-duplicates` (no name argument): exits 1 when any `(database, name)`
  has two or more plain declarations (patch_table/override/abstract sites
  are legitimate), so CI enforces once-only even for layers that never
  co-compose; a plain query exits 1 on no match (scriptable existence check)
```

- [ ] **Step 3: Final verification and commit**

Run: `go test ./...` and `git status` (per project rule, before writing any PR description).
Expected: all ok; only the intended files changed.

```bash
git add docs/README.hcl.md CLAUDE.md
git commit -m "locate: document the subcommand

README gains the locate section (query semantics, placement derivation,
the plain-declaration rule -duplicates enforces, exit codes); CLAUDE.md
gains the feature bullets."
```

---

## Self-review notes

- **Spec coverage:** issue's object list (tables, MVs, views, dictionaries, named collections, raw blocks) — all handled by the scanner (Task 1). Declaration sites with `path:line` + abstract/override/patch/extend flags — Task 1. Placement derivation from the manifest — Task 3. `-dump` — Task 3/4. Exit non-zero on no match — Task 4. `-duplicates` ignoring override/patch/extend — Task 2 (extend legitimacy via abstract-parent exclusion; extend children are declarations of their own name). Reuses existing loaders (`LayerFiles`, manifest block types) rather than reimplementing parsing; the *scanner* is new because position data simply does not survive gohcl decoding.
- **Known simplifications (documented above):** no `-exclude`, no bare `-layer` mode, duplicates audit layers only.
- **Type consistency check:** `Declaration`/`ScanDeclarations`/`MatchesPattern`/`FindDuplicates` (internal, exported) are consumed with the `hclload.` prefix in cmd; `locateStack.Layers` are manifest-relative and resolved under `-layer-root` only inside `buildLocateDoc`, so tests pass `root` as layerRoot.
