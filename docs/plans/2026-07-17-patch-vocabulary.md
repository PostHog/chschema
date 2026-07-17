# Complete the patch vocabulary (issue #154)

## Why

#153's `patch_table` settings collapsed 15 of 85 duplicated objects in
posthog-cloud-infra; the remaining 70 are each blocked on a field no
patch can carry. Profile: ~30 blocked on `engine` alone (Distributed
targets that move with env topology), 6 on views/dictionaries with no
patch form at all, the rest on index/order_by/ttl/column-modify.

## What

### `patch_table` — full field coverage

```hcl
patch_table "raw_sessions" {
  engine "distributed" {            # replace wholesale
    cluster_name    = "posthog"
    remote_database = "posthog"
    remote_table    = "sharded_raw_sessions"
    sharding_key    = "cityHash64(session_id_v7)"
  }
  order_by     = [...]              # replace when set
  partition_by = "..."              # replace when set
  sample_by    = "..."              # replace when set
  ttl          = "..."              # replace when set
  settings     = { ... }            # merge, patch wins (existing)

  column "x" { ... }                # add (existing; must not exist)
  modify_column "y" { ... }         # replace in place (must exist)
  drop_columns = ["z"]              # remove (must exist)

  index "i" { ... }                 # add (must not exist post-drop)
  drop_indexes = ["j"]              # remove (must exist)
}
```

Application order within one patch: `modify_column` → `drop_columns` →
`column` adds; `drop_indexes` → `index` adds (so drop+add in one patch
redefines an index); scalars replace when set; engine replaces
wholesale (merging engine sub-args is not meaningful — per the issue);
settings merge. Patches still accumulate and apply in layer order,
before `extend` resolution.

### `patch_view` / `patch_dictionary`

```hcl
patch_view "user_sessions"       { query  = file("sql/user_sessions_dev.sql") }
patch_dictionary "geoip" {
  source "clickhouse" { ... }       # replace wholesale
  layout "hashed" {}                # replace wholesale
  lifetime { min = 600 }            # replace wholesale
  settings = { ... }                # merge, patch wins
}
```

`patch_view` fields: `query`, `comment` (replace when set).
`patch_dictionary` fields: `source`, `layout`, `lifetime` (replace when
set), `settings` (merge). Unknown targets error, like `patch_table`.

## Canonicalization

Patched values must converge with introspected ones (#136/#137):
`canonicalize` extends to patch columns/modify_columns
(default/materialized/alias/ephemeral), patch index expr/type, and
`patch_view` queries (normalizeQuery). Patch `engine` blocks decode at
parse (`DecodeEngine`), as do patch dictionary source/layout.

## Touch points

- `types.go` — PatchTableSpec fields; PatchViewSpec, PatchDictionarySpec;
  DatabaseSpec.ViewPatches/DictionaryPatches (`diff:"-"`).
- `parser.go` — decode patch engine + dictionary source/layout;
  canonicalize patches.
- `layers.go` — accumulate view/dictionary patches across layers.
- `resolver.go` — applyPatches extended; applyViewPatches /
  applyDictionaryPatches (before resolveDatabase / validations).
- `locate.go` scanner — `patch_view`/`patch_dictionary` recorded as
  patch declaration sites (so `-duplicates` stays correct).

## Tests

- Parse: every new field round-trips; patch engine decoded.
- Apply: engine/scalar replacement; modify/drop/add ordering (add of a
  just-dropped name = redefine); index drop+add; every unknown-target /
  unknown-name error path.
- Layers: the issue's `raw_sessions` scenario — base Distributed('sessions',
  'posthog','raw_sessions'), dev patch Distributed('posthog','posthog',
  'sharded_raw_sessions') → composed engine is the patched one; the
  cross-env diff is exactly the engine change.
- patch_view heredoc query converges with a one-liner (canonicalized).
- patch_dictionary source replace + settings merge.
- locate: patch_view/patch_dictionary sites are patches, exempt from
  `-duplicates`.

## Docs

README.hcl.md (`patch_table` section, new `patch_view`/`patch_dictionary`
sections, top-level block list), FAQ ("Can patch_table change the
engine?" → yes now), README.md layering, CLAUDE.md. Reconcile with #155
(extend/patch explainer) if it merges first.

## Out of scope

- Patching materialized views (no evidence in the profile; `override`
  remains the escape hatch).
- Removing `partition_by`/`ttl` entirely via patch (not expressible with
  optional scalars; separate design if ever needed).
