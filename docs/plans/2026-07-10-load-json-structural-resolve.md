# `load -format json`: structural resolve, no composition

Issue: [#132 follow-up comment](https://github.com/PostHog/chschema/issues/132#issuecomment-4935111473)

## Problem

`hclexp load -manifest -env -format json` reports each role's declared and
resolved layer stack — but it computes that document by *composing* every
role (loading and resolving the layer dirs from disk). Downstream,
`vendor-base.sh` in posthog-cloud-infra needs the stacks **before the layers
exist** (fresh clone, or a base-ref bump adding a layer), to answer "what
should I fetch?". Today that fails:

```
ERROR failed to compose manifest roles ... err="role \"ops\": loading [...]:
read layer \"...roles/ops/dev\": no such file or directory"
```

so that one script still parses `manifest.hcl` itself.

## Decision

Make `-format json` purely structural — skip composition entirely — rather
than adding a `-resolve-only` flag. The JSON document (`role`, `layers`,
`resolved_layers`) contains nothing derived from composition; composing was
wasted work and a spurious failure mode. No new flag surface.

Consequence (intended): `load -format json` no longer errors on broken or
missing layers. Validating composition is `validate`'s job.

## Changes

1. `cmd/hclexp/hclexp.go` — extract `resolveManifestStacks(roles, layerRoot)
   []composedRole` (Role/Layers/Resolved only, `Schema` nil): the pure
   path-joining half of `composeManifestRoles`, which then builds on it.
2. `cmd/hclexp/load_manifest.go` — in `runLoadManifest`, move the
   `format == "json"` branch before composition and feed it
   `resolveManifestStacks` output.
3. Tests: `-format json` pipeline (parse → filter → resolve → write)
   succeeds with layer dirs absent; existing stack-shape tests keep passing.
4. Docs: note the structural behavior in `docs/README.hcl.md` and CLAUDE.md's
   `load` bullet.

## Status

- [x] Plan
- [x] RED test (`TestWriteLoadJSON_BeforeLayersExist`)
- [x] Implementation (GREEN)
- [x] Docs
- [x] Full test suite green (`./cmd/hclexp`, `./internal/...`, `./test`)
