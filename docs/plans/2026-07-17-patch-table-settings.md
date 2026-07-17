# patch_table: settings (issue #152)

## Why

`patch_table` accepts only `column` blocks, so a table whose envs differ
by **one setting** must be redeclared in full per env — 15 objects in
posthog-cloud-infra are triplicated (~30 lines each) to express a
one-line `default_compression_codec = "lz4"` delta. `extend` doesn't
help (child settings replace, and each env would still *declare* the
table, breaking once-only); `override = true` is the same full
duplication with ceremony.

## What

`patch_table` gains `settings`, merged into the target's map:

```hcl
# overrides/data/cloud — declared once
table "adhoc_events_deletion" { ...; settings = { index_granularity = "8192" } }

# overrides/data/prod-us — the whole env difference
patch_table "adhoc_events_deletion" {
  settings = { default_compression_codec = "lz4" }
}
```

Semantics:

- **Patch wins** on key collision (the issue's preference — an env
  overlay that retunes a base setting is the point). Multiple patches
  apply in accumulated layer order, so a later layer's patch wins over
  an earlier one — the same precedence layers already have.
- Columns keep their existing strictly-additive rule (duplicate column
  still errors); a patch may carry columns, settings, or both.
- Patches still apply before `extend` resolution, so a patched abstract
  base flows into children under the existing inheritance rules.

`index` in patch_table is the natural follow-up (244 index-level deltas
in the same layer pair) — out of scope here, tracked in the issue.

## Implementation

- `types.go`: `PatchTableSpec.Settings map[string]string`
  (`hcl:"settings,optional"`); comment updated.
- `resolver.go` `applyPatches`: merge `patch.Settings` into
  `target.Settings` (allocating the map when nil), after the column
  additions.

## Tests

`internal/loader/hcl/patch_settings_test.go`: the issue's exact
two-layer scenario through LoadLayers+Resolve (base setting kept, patch
key added); patch-wins on collision; two patches in layer order (later
wins); settings-only patch on a table with no settings; column+settings
in one patch; unknown target still errors.

## Docs

docs/README.hcl.md (`patch_table` section, merge-order step, comparison
table), README.md layering section, CLAUDE.md feature bullet.
