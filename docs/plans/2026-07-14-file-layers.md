# Layers as files: allow a layer stack entry to name a single `.hcl` file

**Status: implemented** (branch `file-layers`). Built as designed — `LayerFiles`
owns the dir/file decision, every caller inherits it, and `loadSide`/`loadLeft`
lost their single-file `ParseFile` branch. All acceptance items below verified
against the built binary.

## Problem

A layer today must be a directory; the loader enumerates `*.hcl` inside it
(`internal/loader/hcl/layers.go`, `hclFilesIn`). Fine-grained composition —
e.g. one node adding a single table, or a manifest role pulling in one shared
definition — currently forces a directory holding one file. That multiplies
tiny directories and makes manifest `layers = [...]` stacks noisier than they
need to be.

Goal: anywhere an ordered layer stack is accepted, an entry may be either a
directory (current behavior) or a single `.hcl` file. A file entry behaves as
a one-file layer with identical merge semantics.

Affected entry points (all funnel into `hclload.LoadLayers`):

- `-layer` flag on `validate`, `load`, `web`
- `-left`/`-right` comma lists on `diff` (`loadSide`) and `-left` on `sql2hcl`
  (`loadLeft`)
- manifest `layers = [...]` stacks (`resolveManifestStacks` →
  `composeManifestRoles`) used by `validate -manifest`, `load -manifest`,
  `plan`, `web -manifest`
- repeatable `-cluster NAME=STACK` mappings (`applyClusterEntries`)

Plus one path that re-derives the file list independently: the `web`
auto-reload fingerprint (`sourceFiles` in `cmd/hclexp/web.go`).

## Approaches considered

1. **Teach `LoadLayers` to accept files (chosen).** Stat each stack entry:
   directory → enumerate `*.hcl` as today; regular file → that one file is
   the layer. Every caller gets the feature for free; one place owns the
   dir/file decision.
2. Expand file entries at each CLI call site before calling `LoadLayers` —
   duplicates the stat/branch in five commands, and the manifest path would
   need it too. Rejected.
3. An HCL-level `include`/import construct — a language change solving a
   packaging problem; far bigger surface. Rejected (YAGNI).

## Design decisions

- **A file layer must have the `.hcl` extension.** Directory enumeration only
  picks up `*.hcl`; an explicit file entry is held to the same rule so a typo
  (pointing at a `.sql` or a dump) fails loudly:
  `layer "x.sql": not an .hcl file`.
- **A missing path stays an error** (today `os.ReadDir` fails; keep an
  equally clear message naming the entry).
- **No new syntax.** A stack entry is just a path; the loader stats it. No
  prefixes, no flags.
- **Merge semantics unchanged.** A file layer is a layer containing exactly
  one file: cross-layer `override`/`patch_table`/duplicate rules apply as-is.
  Listing the same file both directly and via its parent directory yields the
  usual duplicate-declaration error — acceptable, no special-casing.
- `LoadLayers` keeps its signature (`[]string`); rename the parameter
  `layerDirs` → `layerPaths` and update its doc comment.

## Implementation

### 1. Loader (`internal/loader/hcl/layers.go`)

Extract the per-entry file listing into an exported helper so `web` reload
can share it:

```go
// LayerFiles returns the .hcl files a layer path contributes, in load order:
// for a directory, every *.hcl inside it (lexical order); for a regular
// .hcl file, that file itself.
func LayerFiles(path string) ([]string, error)
```

- `os.Stat` the path. Directory → current `hclFilesIn` body. Regular file →
  require `filepath.Ext == ".hcl"`, return `[]string{path}`.
- `LoadLayers` calls `LayerFiles` per entry; the rest of the merge loop is
  untouched. `hclFilesIn` folds into `LayerFiles` (or stays as its
  directory branch).

### 2. `web` auto-reload (`cmd/hclexp/web.go`)

`sourceFiles` re-implements directory enumeration with `os.ReadDir` and
breaks on a file layer (feeds the mod-time fingerprint used by
`enableReload`, including the `web -manifest` servers which reuse it via
`srv.enableReload("", layers, …)`). Replace its per-dir loop with
`hclload.LayerFiles(entry)` so file layers fingerprint and hot-reload
correctly.

### 3. Call-site simplification (small, optional but do it)

`loadSide` (`cmd/hclexp/hclexp.go`) and `loadLeft` (`cmd/hclexp/sql2hcl.go`)
special-case "single path that is a file → `ParseFile`". With file-aware
`LoadLayers` the branch is redundant — always call `LoadLayers(paths)`.
Behavior note: a bare non-`.hcl` file previously parseable via `ParseFile`
would now be rejected; acceptable (all real schemas are `.hcl`), mention in
the commit message. Leave `load()`'s `-config` branch (`ParseFile`) alone —
`-config` is documented as a single-file schema, not a layer stack.

Manifest paths (`resolveManifestStacks`, `composeManifestRoles`,
`applyClusterEntries`) need no code change — they only join paths and pass
them to `LoadLayers`.

### 4. Flag help + docs

- Flag strings saying "comma-separated list of layer directories":
  `validate`/`load` (`cmd/hclexp/hclexp.go:385,508`), `web`
  (`cmd/hclexp/web.go:32`), `diff -left/-right` help, `sql2hcl -left`,
  `-cluster` STACK help → "layer directories or .hcl files".
- `docs/README.hcl.md` "File and layer model": layers are directories **or
  single `.hcl` files**; one sentence + a stack example mixing both. Update
  the manifest section to note `layers = [...]` entries may be files.
- `README.md` flag reference (lines ~143–260) and
  `examples/manifest/README.md` wording.
- `CLAUDE.md` supported-features bullets that say "layer dirs".

## Tests (new feature ⇒ must be covered)

`internal/loader/hcl/layers_test.go` (testify, whole-struct asserts, fixtures
under `testdata/layers/`):

- file-only stack: `LoadLayers([base_dir, extra.hcl])` — file layer's table
  lands after the dir layer's, ordering respected;
- `patch_table` in a file layer patches a dir-layer table;
- `override = true` in a file layer replaces a dir-layer table;
- error: entry names a non-`.hcl` file;
- error: entry does not exist (message names the entry).

`cmd/hclexp`:

- `diff` with `-left` a comma list mixing a dir and a file (`loadSide`);
- `load -layer dir,file.hcl` composes; `load -manifest` where a role's
  `layers` includes a file entry (`load_manifest_test.go` has the harness);
- `web` `sourceFiles`/fingerprint over a stack containing a file layer
  (added/removed sibling files in the dir still register; the file layer's
  mod time change registers).

Run: `go test ./internal/... -v` and `go test ./test -v`.

## Acceptance

- `hclexp validate -layer base,overrides/extra.hcl` works end-to-end.
- A manifest role with `layers = ["base", "roles/ingest/events.hcl"]`
  composes under `validate`, `load`, `plan`, and `web -manifest`.
- `web` hot-reloads when the file layer changes.
- Non-`.hcl` or missing entries fail with messages naming the entry.
- All existing tests pass unchanged (directory layers are untouched).
