# locate follow-ups: -layer mode, dump node attribution, extended-by, multi-pattern

## Why

`hclexp locate` shipped in #145 with four capabilities deliberately left
out (or independently built in the superseded #147 draft). Reintroduce
them on top of the shipped design:

1. **Bare `-layer` mode** — scan ad-hoc layer dirs/files without a
   manifest ("where is X in this stack?" before a manifest exists).
2. **Node attribution on dump sites** — a dump site should say *which
   node* declares the object, from the dump file's `node {}` block
   (filename stem fallback, as `drift` names nodes).
3. **Extended-by cross-links** — an object extended by others lists its
   children, even when the children don't match the pattern.
4. **Multi-pattern support** — accept one or more `<name-or-glob>` args;
   exit 1 when *any* pattern matches nothing (per-pattern existence
   check).

## Design

All additions keep #145's document model (`locateDoc` shared by text and
JSON renderers) and its scanner (`Declaration`).

### Loader (`internal/loader/hcl/locate.go`)

- `scanDeclarationFile` also captures the file's first `node "<name>"`
  block label. New exported `ScanFileDeclarations(path) ([]Declaration,
  string, error)` returns decls + node name; `ScanDeclarations` keeps its
  signature (node discarded).

### CLI (`cmd/hclexp/locate.go`)

- New flag `-layer` (comma-separated dirs or `.hcl` files, like `load`).
  Scanned after the manifest's layers, deduped against them by resolved
  path; sites get `layer` but no placements. Valid alone (no manifest).
  `-duplicates` now requires `-manifest` **or** `-layer` (still excludes
  `-dump`).
- `buildLocateDoc(stacks, layerRoot, extraLayers, dumpDir, patterns,
  duplicates)` returns `(locateDoc, unmatched []string, error)`:
  - matches any-of `patterns` (each tried bare and `db.name` qualified);
    tracks per-pattern hits across layer and dump declarations.
  - dump sites carry `Node` (node block, else filename stem); rendered
    `dump: <file>:<line>  (<node>)`, JSON `node`.
  - `extended_by` computed from **all** layer declarations (not just
    matched ones): children whose `extend` names the object, qualified
    `db.name`, in scan order. Attached in both pattern and duplicates
    modes; rendered `extended by: ...` after the declaration sites.
- JSON envelope: `"pattern"` (string) becomes `"patterns"` (array).

## Tests

- Loader: node capture + no-node file.
- CLI: `-layer` alone (no placements) and combined with `-manifest`
  (dedupe, no double sites); multi-pattern with an unmatched one;
  dump node attribution incl. stem fallback; extended-by on a parent
  whose child doesn't match the pattern; `-duplicates` with `-layer`
  only; flags-error table updated.

## Docs

- README.md "Locate declarations" and docs/README.hcl.md "Locating
  declarations": `-layer`, multi-pattern, node suffix, extended-by,
  `patterns` JSON field.
- CLAUDE.md locate feature bullets.

## Status

- [x] Loader node capture
- [x] `-layer` mode
- [x] Multi-pattern + per-pattern exit rule
- [x] Dump node attribution
- [x] Extended-by cross-links
- [x] Tests
- [x] Docs
