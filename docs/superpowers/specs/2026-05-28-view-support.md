# Plain Views Support in hclexp — Design Spec

**Date:** 2026-05-28
**Status:** Approved, ready for implementation
**Reference docs:** `clickhouse-docs/en/sql-reference/statements/create/view.md` (Normal View section, lines 18–58, and SQL security section, lines 115–157)

## Motivation

Introspecting PostHog's production schema with `hclexp introspect` silently skips 17 plain (non-materialized) views. They round-trip nowhere — diff can't enforce their existence, sqlgen can't recreate them, validate can't check their dependencies. Closing this gap is the precondition for using `hclexp` as a complete schema source-of-truth for PostHog deployments.

## Surface (HCL)

A new `view` block inside `database`, sibling to `table`, `materialized_view`, and `dictionary`.

```hcl
database "posthog" {
  view "custom_metrics" {
    query = "SELECT team_id, count() AS n FROM events GROUP BY team_id"

    column_aliases = ["team_id", "n"]   // optional

    sql_security = "definer"            // optional: definer | invoker | none
    definer      = "alice"              // optional; must accompany sql_security="definer"
                                        // with a named user; "current_user" allowed

    cluster = "posthog"                 // optional ON CLUSTER target
    comment = "team-level event counter" // optional
  }
}
```

Field-by-field:

| HCL attribute    | Go type     | Mapped to                                                                                |
| ---------------- | ----------- | ---------------------------------------------------------------------------------------- |
| `name` (label)   | `string`    | `[db.]name` of the view                                                                   |
| `query`          | `string`    | `AS SELECT ...` body (verbatim text)                                                      |
| `column_aliases` | `[]string`  | `CREATE VIEW v (a, b, …) AS …`                                                            |
| `sql_security`   | `*string`   | `SQL SECURITY { DEFINER \| INVOKER \| NONE }`. Lowercase only; validate at parse time.   |
| `definer`        | `*string`   | `DEFINER = …`. Allowed values: any identifier (resolved as user) or literal `current_user`. |
| `cluster`        | `*string`   | `ON CLUSTER`                                                                              |
| `comment`        | `*string`   | `COMMENT '...'`                                                                           |

### Validation at HCL parse time
- `definer != nil` requires `sql_security != nil && *sql_security == "definer"`. Otherwise reject.
- `sql_security` must be one of `"definer" | "invoker" | "none"` (case-insensitive on parse, lowercase canonical).

### Out of scope (rejected during introspection with a clear error)
- Live View
- Refreshable Materialized View
- Window View
- Parameterized View (introspection passes through as a normal view since the parameter syntax is inside the SELECT body, but we don't model the parameter list explicitly; round-trip still works because the body is stored verbatim)

`DatabaseSpec` gains `Views []ViewSpec`. Does **not** participate in `extend` / `abstract` / `patch_table` inheritance — same as `MaterializedViews` and `Dictionaries`.

## Architecture

Six packages touched, each in one consistent slice:

1. **`internal/loader/hcl/types.go`** — new `ViewSpec`, new `DatabaseSpec.Views` field.
2. **`internal/loader/hcl/introspect.go`** — replace the existing skip-view arm with `buildViewFromCreateView`. Reject Live/Refreshable/Window with named errors.
3. **`internal/loader/hcl/render.go`** (or the existing canonical-HCL writer) — emit `view "name" { … }` blocks.
4. **`internal/loader/hcl/diff.go`** — new `ViewChange` / `ViewDiff` types; populate `DatabaseChange.{Add,Drop,Alter}Views`.
5. **`internal/loader/hcl/sqlgen.go`** — emit `CREATE VIEW` / `DROP VIEW` / `ALTER TABLE … MODIFY QUERY` / `ALTER TABLE … MODIFY COMMENT`. Honour view-after-source ordering.
6. **`internal/loader/hcl/validate.go`** — extend the existing loop that walks materialized views: iterate `db.Views`, call `ExtractReferencedTables` on each `query`, fail on undeclared refs. Honour the existing `-skip-validation=<name,…>` flag (views join the same set).

Plus `cmd/hclexp/hclexp.go` — render `+ view`, `- view`, `~ view` lines in the change-set printout.

## Diff semantics

| What changed                                                | Action                                                             |
| ----------------------------------------------------------- | ------------------------------------------------------------------ |
| `query` only                                                | `ALTER TABLE db.name [ON CLUSTER c] MODIFY QUERY <new>`            |
| `comment` only                                              | `ALTER TABLE db.name [ON CLUSTER c] MODIFY COMMENT '<new>'`        |
| `column_aliases` / `sql_security` / `definer` / `cluster`   | `Recreate = true` → `DROP VIEW` + `CREATE VIEW` (no in-place form) |
| Added view                                                  | `CREATE VIEW …`                                                    |
| Dropped view                                                | `DROP VIEW [IF EXISTS] db.name [ON CLUSTER c]`                     |

`ViewDiff` shape (mirrors `MaterializedViewDiff`):

```go
type ViewDiff struct {
    Name        string
    QueryChange *StringChange
    Comment     *StringChange
    Recreate    bool   // true → DROP + CREATE
}

type DatabaseChange struct {
    // …existing fields…
    AddViews    []ViewSpec
    DropViews   []string
    AlterViews  []ViewDiff
}
```

## Validation

Extend `validate.go`'s existing dependency walk. For each `ViewSpec`:

1. Parse `view.Query` with `hclload.ExtractReferencedTables` (already handles `*chparser.CreateView` — added in PR #11; the helper filters CTE names).
2. For each `ObjectRef`, check the referenced table / MV / view is declared in the loaded schema (cross-database refs are looked up by the explicit DB part of the ref).
3. On miss: `validate: view <name> references undeclared table <db.table>`.
4. `-skip-validation=<view_name,…>` skips by name.
5. `hclexp diff -sql` extends its ordering knowledge to views: created after every source, dropped before.

## Round-trip ordering rules

The canonical HCL writer emits, per database:
```
table … { }
materialized_view … { }
view … { }
dictionary … { }
```
Within each block, fields are written in a fixed order (named in the field-by-field table above). This keeps `hclexp introspect` deterministic across runs.

## Introspection: AST walk details

```go
case *chparser.CreateView:
    v, err := buildViewFromCreateView(s)
    if err != nil { return fmt.Errorf("introspect view %s.%s: %w", database, name, err) }
    v.Name = name
    db.Views = append(db.Views, v)
```

`buildViewFromCreateView(*chparser.CreateView) (ViewSpec, error)`:

1. **Reject early**: if the AST exposes `Refresh`, `Engine`, or window-view markers → return a named error (mirrors the MV rejection logic in `buildMaterializedViewFromCreateMV`). The exact field check is impl-time (chparser may surface these as nil-checks or as bool flags; verify at write time).
2. **`Query`** ← `formatNode(s.Select)` (uses `PrintVisitor`, same path the MV introspector uses for its `AS SELECT`).
3. **`ColumnAliases`** ← if the AST has a column list node attached to the view definition, walk it and collect identifier names. If absent → nil.
4. **`Definer` / `SQLSecurity`** ← optional fields on the AST. Lowercase the security value. If `Definer` is present, ensure `SQLSecurity == "definer"` (otherwise the live CREATE shouldn't have parsed; treat as parser bug if it happens).
5. **`Cluster`** ← `s.OnCluster.Expr` text if non-nil.
6. **`Comment`** ← `unquoteString(s.Comment.Literal)` if non-nil.

## SQL Generation forms (canonical)

```sql
-- Add (full form, all clauses optional except CREATE VIEW … AS <query>)
CREATE VIEW [ON CLUSTER cluster] db.name [(alias1, alias2, …)]
  [DEFINER = user] [SQL SECURITY { DEFINER | INVOKER | NONE }]
  AS <query>
  [COMMENT '<comment>']

-- Body-only modify
ALTER TABLE db.name [ON CLUSTER cluster] MODIFY QUERY <query>

-- Comment-only modify
ALTER TABLE db.name [ON CLUSTER cluster] MODIFY COMMENT '<comment>'

-- Recreate (DROP then CREATE — no atomic alternative for column_aliases / security / cluster changes)
DROP VIEW IF EXISTS db.name [ON CLUSTER cluster]
CREATE VIEW … (full form)

-- Drop
DROP VIEW [IF EXISTS] db.name [ON CLUSTER cluster]
```

Quoting: identifiers (`db`, `name`, `definer`) are not quoted unless they require it. Comment literals use ClickHouse single-quote escaping (existing `quoteString` helper).

## Change-set rendering

`renderChangeSet` in `cmd/hclexp/hclexp.go` gains:

```
database "posthog"
  + view my_view
  - view old_view
  ~ view changed_view
      ~ query changed
      ! requires recreation (column_aliases / sql_security / definer / cluster changed)
      ~ comment changed
```

Exactly mirrors the existing materialized-view block layout.

## Testing strategy

| Layer                   | Tests                                                                                                                                      |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| Unit — introspect       | Round-trip parse of every grammar form: bare, `ON CLUSTER`, with column aliases, each `SQL SECURITY` variant, with `DEFINER = current_user`, with `DEFINER = alice`, with comment. Also: reject Live / Refresh / Window with named errors. |
| Unit — HCL parse        | `definer` without `sql_security="definer"` → error. `sql_security` outside the three enum values → error.                                  |
| Unit — diff             | Each row of the diff-semantics table (add, drop, query-only, comment-only, recreate-triggering, no-op).                                    |
| Unit — sqlgen           | Each DDL form, including `ON CLUSTER` propagation and view-after-source ordering vs a dependent table.                                     |
| Unit — validate         | Missing source table → named error; CTE not mistaken for a source; `-skip-validation` honoured.                                            |
| Live (snapshot)         | Round-trip a CREATE VIEW fixture against the docker-compose ClickHouse (the `TestLive_Introspection_AllStatements/View/*` slot enabled by PR #11). Assert introspected `ViewSpec` matches expectations and sqlgen back-out matches the input (after canonicalisation). |

## Cross-cutting

- **Branch**: new branch `feat-view-support` off `main` in a fresh worktree.
- **Spec location**: this file.
- **Plan location**: `docs/superpowers/plans/2026-05-28-view-support.md`.
- **Docs**: update `docs/README.hcl.md` — move views from "Not Yet Supported" to a new `### Views` subsection, then update `README.md` to mention views in the introspection coverage paragraph.
- **CLAUDE.md**: update the Supported Features bullet ("view top-level block — planned, not implemented" → ✅ Views).

## Out of scope (explicitly)

- Live View, Refreshable Materialized View, Window View — rejected at introspection.
- Modeling parameterized-view parameter lists as typed HCL — the SELECT body is stored verbatim, which round-trips fine.
- View grants / `ALTER VIEW … MODIFY SQL SECURITY` migrations triggered by anything other than full recreate.

## Open items (deferred, none blocking)

- Live VIEW MODIFY COMMENT support: docs imply but don't explicitly state ClickHouse supports `ALTER TABLE view MODIFY COMMENT`. Sanity-check during impl against the local ClickHouse; if rejected, fall back to recreate for comment-only changes too.
- Parameterized view validation: ClickHouse runtime, not introspection. Out of scope here.
