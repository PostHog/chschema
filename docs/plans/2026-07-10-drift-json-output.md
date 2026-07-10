# Structured output for `hclexp drift`

> **Superseded by [2026-07-10-object-comparison.md](2026-07-10-object-comparison.md).**
> The diff-direction decision and the counting-bug analysis carry over; the
> flat per-drifter operation list is replaced by the shared per-object
> comparison model (`ObjectComparison`) used by `diff`, `plan`, and `drift`.

## Context

Analyzing cross-node schema drift is already a two-command flow that works:

```bash
hclexp dump-cluster -cluster <name> -host <entry> -out-dir /tmp/nodes
hclexp drift -dir /tmp/nodes -details
```

`dump-cluster` enumerates the cluster from `system.clusters` and writes one
`<host>.hcl` per node. `drift` groups nodes by the `hostClusterRole` macro,
diffs each group against its lexically-first member, and exits non-zero on
drift.

The friction is not the invocation — it is the output. `drift` emits only
prose (`✗ prod-eu-fra-ch-10a: ~3 table, +1 mv`) and an exit code. `diff` has
`-format json` and `plan` defaults to JSON; `drift` has neither. An agent
consuming drift today does string surgery on a human-facing format.

We considered exposing `hclexp` as an MCP server so agents could call it.
**Rejected.** MCP's value is reaching capabilities an agent cannot otherwise
reach — remote services, credentialed APIs, stateful sessions. `hclexp` is a
local binary that any shell-capable agent already invokes directly. A server
would add a process, a tool-schema surface, and a second code path to
maintain in exchange for zero new capability and worse ergonomics than
`--help`. Revisit only if a shell-less consumer (hosted agent, Slack bot)
actually appears.

Intended outcome: `hclexp drift -format json` emits a structured document an
agent can consume directly, and the summary counts stop silently ignoring
`raw{}` blocks and named collections.

## Two decisions worth recording

**Diff direction.** `cmd/hclexp/drift.go:95` computes `Diff(ref.Schema,
m.Schema)` — changes turning the *reference* into the *drifter*. Today's
`+1 mv` therefore means "the drifter has an extra MV." The JSON
`operations` keep this same direction and are documented as **descriptive**:
"the DDL that would transform the reference node into this node."

We deliberately do **not** invert them into a reconcile-to-reference fix
script. The reference is merely the lexically-first group member
(`drift.go:90`), not an authority. Authoritative desired state comes from
`plan` / `diff` against HCL. Keeping counts and operations in the same
direction means the two can never contradict each other.

**Counting gap (bug).** `summarizeChanges` (`drift.go:295`) counts tables,
MVs, dictionaries, and views, but not `AddRaws` / `DropRaws` / `AlterRaws`
(`diff.go:35`) or `NamedCollections` (`diff.go:13`). A node drifting *only*
in a `raw{}` block is correctly detected (`IsEmpty()` is false) but prints
the bare fallback `"changed"`. Shipping JSON with this hole would bake it
into a machine-readable contract, so it is fixed here.

## Implementation

### 1. `internal/loader/hcl/render_json.go` — drift document types

Serialization lives beside the existing `DiffJSON`. Reuse `JSONOperation`
(line 13) verbatim — it already carries `engine`, `replicated`, `manual`,
`unsafe`, `unsafe_reason`.

```go
// DriftSummary counts a drifter's changes relative to its group reference.
type DriftSummary struct {
    TablesAdded, TablesDropped, TablesAltered int
    MVsAdded, MVsDropped, MVsAltered          int
    DictsAdded, DictsDropped, DictsAltered    int
    ViewsAdded, ViewsDropped, ViewsAltered    int
    RawsAdded, RawsDropped, RawsAltered       int
    NamedCollectionsChanged                   int
}

type DriftNode struct {
    Node       string            `json:"node"`
    File       string            `json:"file"`
    Macros     map[string]string `json:"macros,omitempty"`
    Summary    DriftSummary      `json:"summary"`
    Operations []JSONOperation   `json:"operations"`
}

type DriftGroup struct {
    Key       string      `json:"key"`
    Reference string      `json:"reference"`
    Nodes     int         `json:"nodes"`
    Drifters  []DriftNode `json:"drifters"`
}

type DriftJSON struct {
    Groups  []DriftGroup     `json:"groups"`
    Summary DriftRunSummary  `json:"summary"` // nodes, groups, groups_with_drift, drifting_nodes
}
```

`RenderDriftJSON` mirrors `RenderDiffJSON` (line 51), building each
drifter's `Operations` from `GenerateSQL(cs)` and reusing `engineFor` /
`unsafeFor` unchanged.

### 2. `cmd/hclexp/drift.go` — `-format text|json`

`runDrift` already computes and retains each drifter's `ChangeSet` in
`changeSets[m.Name]` (line 93) for `-details`. JSON mode feeds those through
the same `hclload.GenerateSQL` path `diff` uses. Validate the flag like
`-zk-paths` does (line 56) — reject anything but `text` / `json` with exit 2.

Text mode stays byte-for-byte identical apart from the corrected counts.
Exit code semantics are unchanged: non-zero when any node drifts. JSON to
stdout, diagnostics to stderr, so `hclexp drift -format json | jq` is clean.

### 3. Extract the shared counting

Split `summarizeChanges` into `driftCounts(cs) DriftSummary` plus a thin
renderer that formats the one-line string from it. Both formats consume the
same function, so the raw / named-collection fix lands in text and JSON at
once.

### 4. Docs

Update the `drift` bullet in `CLAUDE.md` and the command's usage text in
`cmd/hclexp/hclexp.go:97` to mention `-format`.

## Verification

Tests go in the existing `cmd/hclexp/drift_test.go`. Per project convention,
use testify and assert whole structs rather than field-by-field.

- **JSON shape** — fixture dir with one known drifter; unmarshal and compare
  against a fully-constructed expected `DriftJSON`.
- **Raw-only drifter** — a node differing solely in a `raw{}` block yields
  non-zero `RawsAltered` in the JSON summary *and* a non-`"changed"` text
  one-liner. This is the regression test for the counting bug.
- **Named-collection-only drifter** — same, for `NamedCollectionsChanged`.
- **Clean run** — no drift produces `groups` populated, every `drifters`
  empty, exit 0.
- **Direction** — an MV present only on the drifter surfaces as a `CREATE`
  operation, pinning the documented reference → drifter semantics.
- **Bad flag** — `-format yaml` exits 2.

Then end-to-end against real dumps:

```bash
go test ./internal/... -v
go test ./test -v
go build -o hclexp ./cmd/hclexp
./hclexp drift -dir test/testdata/<nodes> -format json | jq '.summary'
./hclexp drift -dir test/testdata/<nodes>            # text unchanged
```
