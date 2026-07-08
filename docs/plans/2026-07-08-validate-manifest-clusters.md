# validate: derive cluster mappings from the plan manifest

Status: design 2026-07-08.
Origin: `validate` takes cross-cluster config only through repeated
`-cluster NAME=STACK` flags (#118/#122). The fleet needs dozens of these, and
the same layer-stack-per-role composition already lives in the `plan`/`web`
manifest. This lets `validate` read that manifest so the cluster config has one
home instead of being duplicated as flag soup.

## The gap

The manifest (`examples/manifest/manifest.hcl`) is keyed by **node role**
(`ops`, `data`) → env → layer stacks; `plan` matches roles to dumps by the
`hostClusterRole` macro. `validate -cluster` is keyed by ClickHouse
**`cluster_name`** (`posthog`, `aux`, …) plus `@absent` / `@alias=BASE`.

Roles and cluster names overlap but differ: `data` role composes the `posthog`
cluster; `ai-events` role ↔ `ai_events` cluster; and the manifest carries no
alias or absent facts. So the manifest can't feed `validate` as-is — it needs
the cluster-name + alias metadata added.

## Design

A ClickHouse cluster is composed of **nodes from one or more roles**, so a
cluster's schema is the **union** of its member roles' compositions — not one
role ↔ one cluster. The manifest gets a cluster-centric block.

### Manifest extension (backward-compatible)

Add an optional top-level `cluster` block; `plan`/`web` ignore it.

```hcl
role "data"             { env "prod-us" { layers = ["layers/base", "layers/env/prod-us"] } }
role "ingestion-events" { env "prod-us" { layers = ["layers/base", "layers/ingestion", "layers/env/prod-us"] } }
role "aux"              { env "prod-us" { layers = ["layers/base", "layers/aux", "layers/env/prod-us"] } }

cluster "posthog" {
  roles   = ["data", "ingestion-events"]      # cluster schema = union of these roles
  aliases = ["posthog_writable", "posthog_single_shard"]
}
cluster "aux" { roles = ["aux"] }
```

- `roles` (required): the roles whose nodes compose this cluster. Each must be a
  declared `role` block (validated). Its schema is the **union** of their
  compositions for the selected `-env`; a role not deployed in that env is
  skipped.
- `aliases` (optional): `remote_servers` aliases sharing this cluster's
  composition → the `@alias=cluster` form.

### validate flags

```
hclexp validate -layer <node-stack> -manifest roles.hcl -env prod-us [-layer-root .]
```

Each cluster block resolves each member role's layer stack for `-env` (paths
under `-layer-root`), unions their databases, and registers that as the cluster's
schema; each alias maps to `@alias=cluster`. Explicit `-cluster` flags are
applied **last**, so they override/extend the manifest (this is how `@absent`
clusters are declared for now — `-cluster NAME=@absent`).

### Absent (deferred)

Declaring `@absent` clusters in the manifest is env-specific and adds schema;
deferred. Until then, an off-node cluster with no composing role errors (the
anti-staleness guard) unless a `-cluster NAME=@absent` flag is passed. Tracked
as a follow-up once the manifest-driven path lands.

## Files

- `cmd/hclexp/plan.go`: `planManifest` gains `Clusters []manifestClusterBlock`
  (`name`, `roles`, `aliases`); `parseManifestClusters` decodes + validates
  (duplicate names, empty roles, unknown role reference). `parseManifest` is
  unchanged and ignores cluster blocks.
- `cmd/hclexp/hclexp.go`: `runValidate` gains `-manifest`, `-env`, `-layer-root`;
  `buildManifestClusters` unions each cluster's member-role schemas into the
  `ClusterSet`; `applyClusterEntries` (factored from `buildClusterSet`) applies
  the `-cluster` flags last. Update `validate` help + doc.
- `examples/manifest/manifest.hcl`: show a `cluster` block.
- `README.md`: document `validate -manifest`.

## Test plan

- cluster parse: `roles`/`aliases` decoded; duplicate cluster, empty roles, and
  unknown-role reference all error.
- `buildManifestClusters`: a cluster spanning two roles resolves remotes from
  **either** role (union); alias resolves via the base; unknown table errors.
- `validate -manifest -env` end-to-end (real HCL, binary path).
- `-manifest` without `-env` (and vice-versa) errors.

## Downstream

posthog `check.sh` replaces its generated `-cluster` flag list with the
manifest it already maintains for `plan`, plus the new `cluster`/`aliases`
metadata — one source of truth for role composition and cluster identity.
