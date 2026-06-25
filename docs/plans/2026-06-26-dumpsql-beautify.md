# #70: offline writers beautify long queries

## Problem

#68 made live `introspect`/`dump-cluster` emit long view/MV queries as readable
heredocs, but the **offline** writers didn't:

- `load -out` (canonical HCL writer)
- `dump-sql` (replayable CREATE-statement seed)

So a layered repo that generates committed goldens offline (`golden/*.hcl` via
`load -out`, `sql/*.sql` via `dump-sql`) couldn't produce reviewable artifacts —
1000+ char query lines.

## What was already done (verify, don't re-fix)

**`load -out` already beautifies on current main.** #68 added `setQueryAttribute`
(heredoc for multi-line queries) to the HCL writer *and* normalizes queries to
the beautified multi-line form on load, so `load -out` emits

```hcl
query = <<SQL
SELECT a, b, c
FROM posthog.events
WHERE ...
SQL
```

The issue's "single-line / `\n`-escaped" observation was on the stale
`sha-08b51f4` image (see #69) — reproduced and confirmed fixed on current source.

## Change: `dump-sql` beautification

`renderDump` emitted ClickHouse's verbatim single-line `create_table_query`. Now,
for **View / MaterializedView** objects whose CREATE exceeds a length threshold,
it re-renders the statement in beautified multi-line form.

- New exported `hclload.BeautifySQL(sql) (string, ok)` — parses a CREATE
  statement and renders it via the parser's `BeautifyVisitor` (the same visitor
  behind #68's query beautification). Unparseable → returns input verbatim,
  `ok=false`, so DDL the parser can't handle still dumps.
- `cmd/hclexp/dumpsql.go`: `renderCreate(o)` beautifies View/MV creates over
  `beautifyThreshold` (120 chars); tables/dictionaries stay verbatim
  (their bodies are already column-structured and beautification is reserved for
  the SELECT-bearing objects #70 targets). Short definitions stay inline to avoid
  churn.

Beautified output is semantically identical valid DDL, so the round-trip
fidelity seed still replays; the round-trip test compares re-introspected
schemas, not SQL text.

## Verification

- `internal/loader/hcl`: `BeautifySQL` renders a CREATE VIEW multi-line and is
  idempotent; unparseable input is kept verbatim.
- `cmd/hclexp`: `renderCreate` beautifies a long view and a long MV, leaves a
  long table verbatim, and leaves a short view inline.
- Full suite + golangci-lint + vet + gofmt green. (Live `dump-sql` smoke was not
  run — no local ClickHouse — but the unit tests exercise the render path.)

## Scope notes

- `load -out` heredoc uses flush-left `<<SQL` (matching the existing `raw{}`
  heredoc style), which satisfies the readability ask; switching to indented
  `<<-SQL` is a separate cosmetic change.

Closes #70.
