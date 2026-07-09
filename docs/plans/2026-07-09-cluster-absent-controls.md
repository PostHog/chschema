# validate: -strict-clusters and manifest `absent` clusters

Status: implemented 2026-07-09.
Two related controls over cluster absence, following #127 (uncomposed cluster →
absent) and the manifest-cluster work (#125/#126).

## `absent = true` in the manifest

A `cluster` block can now declare `absent = true` (mutually exclusive with
`roles`) for a cluster that has **no composition in the manifest** — modeled
elsewhere / externally. Proxies (and its aliases) into it resolve as satisfied,
independent of env. This is the explicit form of the implicit "member roles not
deployed in this env → absent" behavior from #127.

- `manifestClusterBlock.Roles` becomes optional; `Absent bool` added.
- `parseManifestClusters`: exactly one of `roles` (non-empty) or `absent = true`;
  both → error, neither → error. Unknown-role check skipped for absent clusters.
- `clusterSetFromRoles`: an absent (or empty-union) cluster → `AddAbsent`.

## `-strict-clusters`

Enforce full validation: every Distributed remote must resolve against a real
composition. A remote on an `@absent` cluster becomes an error instead of being
satisfied — the CI gate for a fully-composed fleet, so a stale `@absent` (a
cluster since mapped) cannot silently pass.

- `ValidateOptions.StrictClusters`; threaded into `resolveDistributedRemote`,
  where step 3 (absent) returns an error under strict instead of nil.
- CLI `-strict-clusters`, wired into both the single-node and manifest-driven
  validate paths. Only affects `@absent` remotes; local/mapped resolution and
  MV/View sources are unchanged.

## Tests

- `-strict-clusters`: an `@absent` remote errors under strict, is satisfied
  otherwise; a locally-resolved remote is unaffected.
- manifest `absent`: parse accepts `absent = true`, rejects both/neither; a
  proxy (and its alias) into an absent manifest cluster is satisfied.
- Binary end-to-end: `cluster { absent = true }` passes; `-strict-clusters` turns
  the same remote into an error.
