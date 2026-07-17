# load: `-exclude` / `-exclude-objects` / `-only` for layer surgery (issue #149)

## Why

Every other schema-reading command takes `-exclude`; `load` doesn't. So
"emit this layer minus these objects" (or "only these objects") — the
primitive layer factoring needs — pushes consumers into hand-parsing
HCL, which silently drops shapes they hadn't heard of (`raw
"dictionary" "x"` with two labels, heredoc bodies at column 0, …).
hclexp has the real parser; expose the filter on `load`.

## What

Three new `load` flags, all applying to the emitted (resolved) schema:

```bash
# the shared layer: only the objects identical everywhere
hclexp load -layer overrides/data/dev -only "$LIST" -out overrides/data/cloud/tables.hcl

# each env layer: everything except those
hclexp load -layer overrides/data/dev -exclude-objects "$LIST" -out overrides/data/dev/tables.hcl
```

- `-exclude <file>` — the same exclude config `diff`/`drift`/`plan`
  consume (`patterns` + `object_types`); matching objects are dropped.
- `-exclude-objects <glob,...>` — ad-hoc comma-separated name globs
  (matched bare and `db.name`-qualified, like exclude patterns).
- `-only <glob,...>` — keep **only** matching objects; the inverse
  selector, so a layer can be split without leaving hclexp.

Semantics:

- Combinable: an object is kept iff it matches `-only` (when set) and is
  excluded by neither `-exclude` nor `-exclude-objects`.
- Filtering runs after `Resolve()` on the emitted schema — same stage
  the other commands filter at. Databases are kept even when emptied
  (both halves of a split layer still need the `database{}` wrapper and
  its `cluster` default); `node{}` blocks are untouched; named
  collections are filterable, exactly as in `FilterSchema`.
- Works in plain (`-config`/`-layer`) and manifest modes (each composed
  role is filtered before writing).
- Usage errors (exit 2): any filter with `-format json` (stacks aren't
  a schema), malformed globs.

## Implementation

- `internal/loader/hcl/exclude.go`: `SelectSchema(s, m)` — the inverse
  of `FilterSchema` (keeps matches, drops the rest; same object kinds,
  same database/node preservation).
- `cmd/hclexp/load_manifest.go`:
  - `loadFlagsError`'s positional params become a `loadFlags` struct
    (they were getting unwieldy); add the filter rules.
  - `applyLoadFilters(schema, exclude, excludeGlobs, onlyGlobs)` —
    exclusion passes then the `-only` selection.
- `cmd/hclexp/hclexp.go` `runLoad`: the flags; filter after resolve in
  plain mode; `runLoadManifest` filters each composed role.

## Tests

- Loader: `SelectSchema` (raw + named collections selectable, database
  block kept when emptied, nodes untouched).
- CLI end-to-end through real HCL: a layer holding every object shape
  incl. `raw "dictionary"` and a `node{}` block; `-only` and
  `-exclude-objects` splits are complementary and lose nothing (the
  issue's silent-drop failure mode); `-exclude` config file; manifest
  mode; flags table (json rejection, bad globs).

## Docs

README.md load section, CLAUDE.md load bullets + exclude feature entry,
plan doc. (Related: #96 CLI consistency.)
