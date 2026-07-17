# diff: subset-tolerant columns for Distributed proxies over system.* (issue #136, item 4)

## Why

#137 fixed the three canonicalization items of #136 and deliberately left
item 4 open as a policy decision: a Distributed proxy over `system.processes`
declares a curated column subset, but the live proxy (created from a fuller
column set, which grows with every server version) carries columns the layer
intentionally omits — so `diff golden live` reports `ADD COLUMN client_agent`
forever and the convergence gate can never pass. The same asymmetry makes
`drift` flag two nodes whose system proxies were created on different server
versions.

## The decision

**A Distributed proxy whose `remote_database` is `system` compares columns
subset-tolerantly in the diff engine: column *presence* differences (would-be
ADD/DROP COLUMN) are suppressed; columns declared on both sides still compare
fully, so a real type change surfaces as MODIFY.**

Rationale:

- System tables are server-defined and version-evolving; a proxy over one can
  never pin the full column set. Any declared subset is a valid, working
  proxy — the proxy just forwards reads.
- `validate`'s proxy-column check is already subset-tolerant by default
  (`-strict-proxy-columns` opts into exactness); this extends the same policy
  to the comparison engine.
- Scope stays narrow: **non-system proxies keep exact column semantics.**
  Their remotes are layer-managed, so exactness is achievable and an extra
  column there is real drift worth reporting.
- Suppression applies only when **both** sides are Distributed-over-system —
  an engine change (proxy repurposed) still yields the full column diff.
- Symmetric (both directions ignored) because Diff() has no orientation:
  either operand may be the live side, and `drift` compares peer dumps where
  neither side is authoritative.

## Implementation

- `internal/loader/hcl/diff.go`: `isSystemProxy(TableSpec)` (Distributed +
  `RemoteDatabase == "system"`); in `diffTable`, after the virtual-column
  guard, drop the names present on only one side when both sides are system
  proxies. Diff-level only — plan and drift flow through `Diff()`, so all
  three gates converge.

## Tests

- Unit: subset in either direction → empty diff; shared-column type change
  still reported; non-system proxy unchanged (ADD/DROP still reported);
  engine mismatch not suppressed.
- Live (`test/issue136_roundtrip_live_test.go`): the issue's acceptance —
  apply a layer with a Distributed proxy over `system.processes` declaring a
  subset, widen the live proxy with a column the layer omits (simulating the
  version-widened create), introspect, diff → 0 operations.

## Docs

docs/README.hcl.md (Distributed engine notes), CLAUDE.md, README.md diff
section. Close #136 on merge (last open item).
