# validate: manifest-driven mode (validate a whole environment)

Status: implemented 2026-07-08.
Origin: `validate -manifest` (the prior change) validates **one** node passed via
`-layer`/`-config`, using the manifest only as cross-cluster resolution context.
To check a whole environment you had to loop `validate` per node in a shell. This
adds a first-class mode that validates every role in the manifest in one command.

## Behavior

`hclexp validate -manifest roles.hcl -env prod-us [-layer-root .]` with **no**
`-layer`/`-config`:

- Loads and resolves every role's composition for `-env`.
- Builds the cluster set from the manifest (member-role unions + aliases), plus
  any `-cluster` flags applied last.
- Runs the full validation on **each** role's schema against that cluster set,
  and reports errors prefixed with the role: `[role data] ...`.
- Exits non-zero if any role has errors.

Mode is selected when `-manifest` is set and neither `-layer` nor an explicit
`-config` is given (detected via `flag.FlagSet.Visit`). With `-layer`/`-config`,
the existing single-node behavior (manifest as context) is unchanged.

## Implementation (`cmd/hclexp/hclexp.go`)

Shared helpers factored so both modes load role schemas once:

- `loadManifestRoleSchemas(roles, layerRoot)` — role name → resolved `*Schema`.
- `clusterSetFromRoles(cs, clusters, schemas)` — union member-role databases per
  cluster + aliases. `buildManifestClusters` (single-node) reuses these, loading
  only cluster-referenced roles.
- `validateManifest(...) []roleValidation` — loads all roles, builds the cluster
  set, validates each; returns per-role errors (testable, no `os.Exit`).
- `runValidateManifest(...)` — prints per-role errors and sets the exit code.
- `flagWasSet(fs, name)` — detects an explicit `-config`.

## Tests

- `validateManifest`: every role validated; a clean role passes while a role with
  a broken reference reports it *and* its cross-cluster proxy still resolves.
- all-clean manifest → no errors on any role.
- Binary end-to-end: one command flags `[role data]` for a broken view, then
  passes for the whole env once fixed.

## Downstream

posthog `check.sh` collapses its per-node validate loop into a single
`validate -manifest -env` per environment.
