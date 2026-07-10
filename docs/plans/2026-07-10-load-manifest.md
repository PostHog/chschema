# `load -manifest`: compose a node straight from the manifest

Implements [issue #132](https://github.com/PostHog/chschema/issues/132).

## Context

`manifest.hcl` is already the canonical description of what each node
composes, and `validate -manifest -env` and `plan -manifest -env` both consume
it directly. `load` does not — it only accepts `-layer a,b,c`. So anything
that wants to *compose* a node the manifest describes must re-parse the
manifest and rebuild the layer stack by hand.

`PostHog/posthog` and `PostHog/posthog-cloud-infra` both do this today. Each
carried a second, redundant flat `nodes` file purely because it was easier to
parse in shell; cloud-infra has since replaced it with the real
`manifest.hcl`, at the cost of an awk parser for HCL. Parsing HCL with awk to
feed the tool that already parses HCL is the wrong shape, and it breaks
silently on comments containing `]`, heredocs, and single- vs multi-line
arrays.

Outcome: `hclexp load` learns the manifest flags `validate` already has, the
flat `nodes` file disappears from both repos, and `gen-golden.sh` collapses to
one call per env with no shell-side knowledge of layers.

## What already exists (reuse, don't rebuild)

The composition logic is written and tested; `load` simply never calls it.

- `parseManifest(path, env)` — `cmd/hclexp/plan.go:136`. Decodes the manifest,
  resolves each role to its layer stack for `env`, skips roles with no `env`
  block, rejects duplicate role/env names and empty `layers`.
- `loadManifestRoleSchemas(roles, layerRoot)` — `cmd/hclexp/hclexp.go:224`.
  Joins each layer under `layerRoot`, calls `hclload.LoadLayers`, then
  `hclload.Resolve`. Returns schemas keyed by role.
- `flagWasSet(fs, name)` — `cmd/hclexp/hclexp.go:407`. Needed because `-config`
  has a non-empty default, so "was `-config` given?" cannot be tested by
  comparing against `""`. `runValidate` uses it for exactly this.
- `hclload.Write(w, schema)` — `internal/loader/hcl/dump.go:21`. Canonical HCL.
- `stdoutTarget(out)` — `cmd/hclexp/hclexp.go:1134`. `""` or `"-"` means stdout.

## Implementation

### 1. Order-preserving composition helper (`cmd/hclexp/hclexp.go`)

`loadManifestRoleSchemas` returns a `map`, discarding both manifest order and
the layer stack — the JSON output needs the stack, and multi-role file output
wants deterministic order. Introduce the ordered form and rebuild the existing
map from it, so there is one composition path:

```go
// composedRole is one role's manifest-declared layer stack and the resolved
// schema that stack composes to.
type composedRole struct {
    Role     string
    Layers   []string // as declared in the manifest
    Resolved []string // Layers joined under -layer-root
    Schema   *hclload.Schema
}

func composeManifestRoles(roles []manifestRole, layerRoot string) ([]composedRole, error)
```

`loadManifestRoleSchemas` becomes a thin wrapper that calls
`composeManifestRoles` and keys the result by role. No behavior change for
`validate`/`plan`; existing tests must stay green untouched.

### 2. `load` grows the manifest flags (`cmd/hclexp/hclexp.go`, `runLoad`)

```
-manifest <file>   HCL manifest; requires -env. Mutually exclusive with -layer/-config.
-env <name>        selects each role's layer stack (same semantics as validate)
-role <name>       optional; without it, compose every role deployed in -env
-layer-root <dir>  root the manifest's layer paths resolve under (default ".")
-format hcl|json   hcl (default) writes canonical HCL; json emits the resolved stacks
```

Flag validation, all exit 2 (usage errors), matching `runValidate`'s style:

- `-manifest` and `-env` must be given together.
- `-manifest` with `-layer` or an explicitly-set `-config` is an error.
- `-role` without `-manifest` is an error.
- `-format json` without `-manifest` is an error — with `-layer` the stack is
  the flag value, so there is nothing to resolve.
- `-format` accepts only `hcl` or `json`.
- An unknown `-role` is an error naming the roles deployed in that env. It is
  a sentinel (`errUnknownRole`) so that `load` and `validate` both exit 2 on
  it rather than one exiting 1 through its generic failure path — scripts
  should not have to know which command they called to read the exit code.

Without `-manifest`, `runLoad` behaves exactly as today. This keeps the
bare-flags entry point (`hclexp -config …`, dispatched at `hclexp.go:71`)
working.

### 3. Output rules

**`-format hcl` (default)**

| mode | `-out` | behavior |
|---|---|---|
| `-role` given | unset / `-` | canonical HCL to stdout |
| `-role` given | file path | one file |
| `-role` given | existing dir | one `<env>-<role>.hcl` in it |
| no `-role` | **required** | dir; one `<env>-<role>.hcl` per role |
| no `-role` | unset | **error, exit 2** |

Multi-role stdout is refused deliberately: concatenating several composed roles
yields duplicate `database` blocks, so the result is not a loadable single-node
schema. Failing is more honest than emitting something that only looks valid.

**`-format json`** — one document to stdout (or `-out` file), regardless of
`-role`:

```json
{
  "env": "dev",
  "roles": [
    { "role": "ops",
      "layers": ["roles/shared", "roles/ops/shared", "roles/ops/dev"],
      "resolved_layers": ["./roles/shared", "./roles/ops/shared", "./roles/ops/dev"] }
  ]
}
```

`layers` is what the manifest declares; `resolved_layers` is those paths joined
under `-layer-root`. Roles appear in manifest order, filtered by `-role`.

Diagnostics keep going to stderr via `slog`, so `load -format json | jq` stays
clean. The current per-table `slog.Info` spam in `runLoad` (`hclexp.go:464`)
already goes to stderr; leave it.

### 4. `-role` on `validate -manifest` (nice-to-have from the issue)

`runValidateManifest` → `validateManifest` (`hclexp.go:282`) already loops over
`parseManifest`'s roles. Add a `role` parameter that filters *which roles get
validated*, while the cluster set is still derived from the **whole** manifest
— a single role's Distributed proxies must still resolve against the other
roles' compositions, so narrowing the cluster set would produce false failures.
Unknown role: same error as `load`.

### 5. Docs

- `CLAUDE.md`: extend the `hclexp validate` and structured-output bullets.
- `cmd/hclexp/hclexp.go:99` usage text for `load`.
- `docs/README.hcl.md` if it documents the manifest flags.

## Verification

Tests in `cmd/hclexp/hclexp_test.go`, reusing the existing `writeLayer` /
`writeTemp` helpers (`hclexp_test.go:296`, `:315`) and the two-role
`data`/`aux` manifest shape from `TestValidateManifest_AllRoles`
(`hclexp_test.go:717`). testify, whole-struct asserts.

- `composeManifestRoles` returns roles in manifest order with correct
  `Layers` / `Resolved` / composed `Schema`.
- `load -manifest -env -role` → single composed schema; layer precedence
  respected (last layer wins).
- `load -manifest -env` (no `-role`) → one `<env>-<role>.hcl` per deployed
  role in the `-out` dir; a role not deployed in `-env` produces no file.
- `load -manifest -env` with no `-out` → exit 2.
- `-format json` → exact document, whole-struct compare; `-role` filters it.
- `-format json` without `-manifest` → exit 2.
- `-manifest` + `-layer` → exit 2; `-manifest` + explicit `-config` → exit 2.
- Unknown `-role` → error naming available roles.
- `validate -manifest -env -role` validates only that role, but its
  cross-cluster proxy into a *different* role's table still resolves. This is
  the regression test for the cluster-set-stays-whole-manifest decision.
- Round-trip: `load -manifest -env -role X -out f.hcl` then
  `diff -left f.hcl -right <same layers>` reports no differences.

```bash
go test ./internal/... -v
go test ./test -v
go build -o hclexp ./cmd/hclexp
./hclexp load -manifest m.hcl -env dev -layer-root . -format json | jq
```
