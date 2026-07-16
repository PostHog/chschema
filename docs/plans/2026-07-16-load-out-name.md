# load: `-out-name` output template for per-env layouts (issue #146)

## Why

In manifest mode `hclexp load -manifest m.hcl -env <env> -out <dir>`
writes one flat `<env>-<role>.hcl` per role. Consumers that want the
per-env tree (`golden/<env>/<role>.hcl`) must `mv`-reshape on the host,
duplicating layout knowledge across every consumer and breaking on edge
cases (stray flat files, role filters).

## What

Issue #146's preferred option 1: an output-name template.

```
hclexp load -manifest m.hcl -env prod-us -layer-root ./schema \
  -out ./golden -out-name '{env}/{role}'     # golden/prod-us/ops.hcl
```

- New `load` flag `-out-name` (default `{env}-{role}`, preserving
  today's layout). Placeholders: `{env}`, `{role}`. `.hcl` is appended.
- Applies when composed roles are written into a `-out` directory
  (manifest mode, hcl format). Parent subdirectories are created.
- Backwards compatible: the default template renders exactly the
  current names.

## Rules

- A non-default `-out-name` requires manifest mode, `-format hcl`, and
  `-out` naming an existing directory (usage errors, exit 2).
- Unknown placeholders (`{foo}`) are usage errors.
- The rendered path must stay inside `-out`: absolute paths and `..`
  escapes are rejected.
- Two roles rendering to the same path is an error naming both roles
  (e.g. `-out-name '{env}/schema'` with two roles) — no silent
  overwrite. A single role with a static template is fine (the
  issue's `-role all -out-name '{env}/schema'` case).

## Implementation

- `cmd/hclexp/hclexp.go` `runLoad`: the flag; pass through.
- `cmd/hclexp/load_manifest.go`:
  - `renderOutName(template, env, role)` — placeholder expansion +
    unknown-placeholder rejection.
  - `outNamePath(dir, rendered)` — join under `-out`, reject escapes.
  - `writeComposedRoles(out, env, outName, composed)` — render per
    role, collision check, `MkdirAll` parent, write.
  - `loadFlagsError` — `-out-name` needs `-manifest`; not with
    `-format json`.
  - `runLoadManifest` — non-default template needs a directory target.

## Tests

`cmd/hclexp/load_manifest_test.go`: template rendering (incl. unknown
placeholder), escape rejection, per-env layout written with subdir
creation, collision error, single role + static template, flags-error
table, default-template behavior unchanged (existing tests).

## Docs

README.md load section, docs/README.hcl.md load-manifest section,
CLAUDE.md load feature bullets.

Relates to #96 (CLI `-out`/`-out-dir` consistency) — flag named
`-out-name` to sit beside `-out`.
