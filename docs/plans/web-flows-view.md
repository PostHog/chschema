# Plan: `hclexp web` data-flow view

## Context

The web browser shows objects in isolation. ClickHouse ingestion is a chain —
Kafka table → materialized view → (writable Distributed) → ReplicatedMergeTree —
and following it by hand across object pages is tedious. Add a `/flows` view that
reconstructs and renders these chains.

## Approach

- **Detection** (`cmd/hclexp/flows.go`): build flows from the existing
  `CollectDependencies` edges + per-table engine kind.
  - `mv_source` → table read by MV; `mv_dest` → MV's target; `distributed_remote`
    → Distributed's storage table.
  - Roots = tables read by some MV but never produced by one (Kafka tables, plain
    sources). DFS forward, enumerating each root→leaf path as one flow; the
    Distributed hop is detected by engine kind and its remote followed; cascading
    MVs continue naturally. Cycle-guarded with a per-path visited set.
  - Each stage: object name, kind, engine, optional subtitle (Kafka
    `topic=`/`collection=`, Distributed `cluster=`), link to detail page (or a
    non-linked "not declared" card for external targets). Arrow labels: `reads`,
    `writes to`, `forwards to`.
- **Caching**: compute flows once in `newWebServer`; store `flows` + a
  `flowAnchorByRef` map (ref → first containing flow's anchor) on `webServer`.
- **Routes/templates**: `GET /flows` (`handleFlows` + `web/flows.html`); a
  `flows` link in the shared header; on each object overview, a link to
  `/flows#<anchor>` rendered next to the Dependencies section when the object
  participates in a flow.
- **CSS**: horizontal card chain with labeled arrows.

## Files

- `cmd/hclexp/flows.go` — new: flow model, `buildFlows`, `handleFlows`.
- `cmd/hclexp/web.go` — wire flows into `webServer`/`newWebServer`/`runWeb`; set
  object-page flow anchor.
- `cmd/hclexp/web/flows.html` — new template; `layout.html` header link;
  `object.html` flow link near Dependencies; `static/style.css` chain styling.

## Verification

- `web_test.go`: Kafka→MV→Distributed→Replicated fixture renders four stages with
  links/labels; fan-out (one source, two MVs → two chains); object overview shows
  the `/flows#` back-link when in a flow and omits it otherwise.
- `go build`, `go test ./cmd/hclexp ./internal/... ./test`, manual smoke test.
