# Plan: dependency-ordered CREATE generation

## Context

`GenerateSQL` emits CREATE statements in coarse phases (tables → MVs → views →
dictionaries → raws) and only topologically sorts *within* the table phase, using
a single edge type: Distributed → remote table (`orderTablesByDependency`). The
richer dependency graph that `CollectDependencies` already computes
(`mv_source`, `mv_dest`, `buffer_dest`, `ts_target`, `view_source`) is ignored,
so several real cases emit in the wrong order and fail to apply:

- **view → view** (a view selecting from another new view) — emitted in slice order.
- **Buffer → destination table**, **TimeSeries → target table** — same phase, unsorted.
- **dictionary → source table / dictionary** — emitted by name only.
- cross-kind (e.g. an MV whose source is a view) — fixed phase order violates the dependency.

## Approach (unified topological order for CREATEs)

Replace the table-only sort + per-kind create loops with one dependency-respecting
order across **all** added objects (tables, MVs, views, dictionaries):

1. Gather every added object into a node list in the historical stable order
   (tables, then MVs, then views per change-set order, then dictionaries by name),
   pairing each with its CREATE DDL (`createTableSQL`/`createMaterializedViewSQL`/
   `createViewSQL`/`createDictionarySQL`).
2. Build edges:
   - reuse `CollectDependencies` over a synthetic schema of the *added* objects
     (covers tables incl. Distributed/Buffer/TimeSeries, MVs, views), and
   - derive dictionary→source edges from the typed `DictionarySpec.Source`
     (`SourceClickHouse.Table` / `.Query`) — `CollectDependencies` doesn't cover
     dictionaries, and adding them there would change `validate` behaviour.
   Only edges whose both endpoints are in the add-set constrain ordering.
3. Topologically sort (Kahn's, **stable** by input index; a cycle falls back to
   input order) so a dependency is always emitted before its dependent. Raw
   objects stay last (opaque; unchanged).

Stable ordering means objects without a dependency relationship keep their
previous relative order, so existing snapshots are unaffected except where a real
dependency now reorders them.

### Drops
Left as-is for now: dropped views/MVs/dictionaries arrive as names only (no specs
to parse), so they can't be topologically sorted. The existing reverse-by-kind
order (MVs/views/dicts before tables; tables reverse-Distributed) already drops
dependents before dependencies. Documented as a known limitation.

## Files
- `internal/loader/hcl/sqlgen.go` — new `orderedCreates`, `createDependencyEdges`,
  `addedSchema`, `dictionarySourceRefs`, `topoSortNodes`; replace the four create
  loops in `GenerateSQL`; drop the now-unused `addTablesOf`.
- `internal/loader/hcl/sqlgen_test.go` — view→view, Buffer→dest, dict→table,
  dict→dict, cross-kind, and a cycle (no panic); keep existing ordering tests green.

## Verification
- New + existing `internal/...` ordering tests; `cmd/hclexp`; `test` (snapshots).
- `go vet`, gofmt. Update snapshots only if a reorder is verified correct.
