# Fix #59: diff ignores column-modifier-only changes

## Problem

`Diff` compared columns by **name + type only**. A column that changed only a
modifier — `ALIAS`/`MATERIALIZED`/`DEFAULT`/`EPHEMERAL`/`CODEC`/`TTL`/`COMMENT`/
nullable, same name and type — was reported as *no differences*, so the drift was
invisible to `diff`/`drift` and the migration DDL omitted the `ALTER`. This is the
diff-side complement to #45 (the dumper dropping the same modifiers).

## The destructive concern

Switching a column's **storage class** is data-affecting. ClickHouse *accepts*
the `MODIFY COLUMN` without error, so the danger is silent data change, not a
rejection. Verified empirically (ClickHouse 26.3, throwaway tables):

| Transition | Result |
|---|---|
| `plain → ALIAS` | **stored value `99` silently became `10`** (the alias expr) — data lost |
| `plain → MATERIALIZED`, `MATERIALIZED → plain`, `ALIAS → plain` | existing rows preserved here, but the column's nature/future-write semantics change |
| `CODEC` / `COMMENT` / `TTL` / nullable / `DEFAULT` add/remove | applied in place, **data preserved** |

So storage-class switches must never be auto-applied.

## Fix

- `ColumnChange` now carries the full old + new `ColumnSpec`. The comparison uses
  `columnsEqual` over every modifier (`diff.go`).
- **Classification** (`ColumnChange.IsUnsafe`): a change is UNSAFE iff it changes
  the column "kind" and either side is `ALIAS`/`MATERIALIZED`/`EPHEMERAL`. Same-kind
  changes and `plain ↔ DEFAULT`, plus codec/comment/ttl/nullable/type, are
  in-place safe.
- **SQL generation** (`sqlgen.go`): safe changes emit `ALTER … MODIFY COLUMN
  <full new definition>` (previously even a type change dropped the modifiers);
  unsafe changes are **flagged via `unsafeReasons` and never auto-emitted**, like
  engine/`ORDER BY` changes. `ADD COLUMN` now also carries modifiers.
- `TableDiff.IsUnsafe` and the `diff` summary learn about column-modifier changes
  (summary marks unsafe ones `(UNSAFE)`).

## Verification

- Unit tests: alias/materialized/ephemeral/alias→materialized (UNSAFE);
  comment/codec/default/nullable (SAFE); identical = no change; SQL gen emits the
  safe `MODIFY COLUMN` with modifiers, flags the unsafe one without DDL, and
  `ADD COLUMN` carries modifiers.
- End-to-end (the issue repro): the alias change now reports
  `~ column y: Int64 -> Int64 ALIAS x (UNSAFE)` and `diff -sql` emits the
  `-- UNSAFE` note with **no auto-applied DDL**; a comment change emits
  `ALTER TABLE … MODIFY COLUMN y Int64 COMMENT 'hi'`.
- Live: the generated combined safe `MODIFY COLUMN y Int64 DEFAULT 5 COMMENT 'c'
  CODEC(...) TTL …` applies cleanly on ClickHouse 26.3.

Closes #59.
