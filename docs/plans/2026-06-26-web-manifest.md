# `hclexp web -manifest`: browse a fleet of composed schemas

## Goal

Let `web` load a manifest (the `plan` role/env/layers format) and browse **every
composed schema** in one server, instead of a single `-config`/`-layer` schema.

## Approach

Reuse the plan manifest. For each `(env, role)` it declares, compose the layer
stack into a resolved schema and serve it under its own URL prefix
`/s/<env>/<role>/`, with a top-level schema list at `/`. Single-schema mode
(`-config`/`-layer`) is unchanged.

### Routing — mount-per-schema (`cmd/hclexp/web_manifest.go`)

- `manifestCompositions(path, envFilter)` decodes the manifest (reusing
  `planManifest`) → `[]composition{Env, Role, Layers}`, optionally env-filtered.
- `buildMultiServer` composes each via `loadSide` (LoadLayers + Resolve), builds
  a `webServer` per schema (reusing `newWebServer`), sets its `basePath` /
  `label`, and arms per-schema auto-reload.
- `multiServer.handler()` is the top mux: `/` → schema list; `/static/` → shared
  assets; `/flows` → redirect to `/` (no aggregate flows view); each schema
  mounted as `mux.Handle(base+"/", http.StripPrefix(base, srv.mux()))`. Because
  of `StripPrefix`, the per-schema handlers parse paths exactly as before — no
  handler changes.

### Link prefixing (the only `webServer` change)

Generated links must include the mount prefix. Rather than thread a prefix
through every href builder, each handler now passes `Base` (= `s.basePath`,
empty in single mode) and `Label` into the template data, and the templates
prepend `{{$.Base}}` to generated hrefs (`index`/`object`/`flows`), plus a nav
"all schemas" / current-label crumb (`layout`). Single mode keeps `Base == ""`,
so its URLs are byte-identical and all existing tests pass unchanged.

### Flags (`runWeb`)

`-manifest <file>` switches to manifest mode; `-env <env>` filters; `-layer-root
<dir>` prefixes layer paths. `-reload-interval` applies per schema.

## Verification

- `web_manifest_test.go`: `manifestCompositions` (all / env-filtered / error);
  the multi-server end-to-end via `httptest` — schema list links, per-schema
  browse isolated to its own objects, **prefixed** object links, the nav label,
  object-detail through the prefix, env filtering, and 404 for an unknown schema.
- All existing `TestWeb_*` unchanged (single mode, `Base == ""`). Full suite +
  `-race` + golangci-lint + vet + gofmt green. Live smoke: a 2-role manifest
  served a schema list and per-schema browsing under `/s/<env>/<role>/`.

## Scope notes

- The list page's "data flows" nav link redirects to `/` (per-schema flows live
  at `/s/<env>/<role>/flows`).
- `basePath` segments are `url.PathEscape`d; manifest roles/envs are simple
  identifiers in practice.
