# patch_table: positioned column adds (`after` / `first`) — issue #158

## Why

A patch can append or replace in place, but not insert at a position.
When an env's extra columns interleave mid-table (posthog.person: ~96
`pmat_*` columns after `is_deleted`, before `version`), base+patch
cannot reproduce the env's real column order — and goldens must match
the node's physical order (`SELECT *`, positional `INSERT`, dump
parity). 12 objects, including the largest tables, stay fully
redeclared for this alone.

## What

```hcl
patch_table "person" {
  column "pmat_email" {
    type  = "String"
    after = "is_deleted"   # or first = true
  }
}
```

Semantics:

- `after = "<name>"` inserts immediately after the named column;
  `first = true` inserts at the front; neither = append (unchanged).
  `after` and `first` are mutually exclusive.
- Adds apply in patch order, each resolving against the
  post-previous-op state (the modify → drop → add discipline from
  #156): `after` may name a column added earlier in the same patch;
  naming a dropped or unknown column errors.
- **Patch-only**: `after`/`first` on a declared table/MV column or on a
  `modify_column` is a resolve error. Reordering *base* columns is
  genuine drift and stays a full redeclaration, per the issue.
- Canonicalization: position placement is transient metadata (cleared on
  application, `diff:"-"`), so the composed table is byte-identical to
  the same table declared plainly in that order — goldens converge with
  introspected dumps (#136 discipline).

## Implementation

- `types.go` `ColumnSpec`: `After *string` / `First bool`
  (`hcl:...,optional`, `diff:"-"`), documented as patch-only placement,
  alongside the existing transient `RenamedFrom`.
- `resolver.go` `applyTablePatch`: adds insert at the resolved position
  and clear the placement fields; `modify_column` rejects them.
- `resolver.go` validation: a resolved table column (incl. via
  extend/abstract inheritance) or MV column still carrying
  `after`/`first` errors.

Related: #88 (sqlgen `ADD COLUMN FIRST/AFTER`) stays separate — this is
compose-side only; the transient placement fields are the shared model
plumbing if #88 lands later.

## Tests

The issue's person scenario (interleaved `pmat_*` after `is_deleted`,
`version` following) with **byte-parity**: `Write(base+patch)` equals
`Write(flat declaration)`. Plus: `first`, chained `after` within one
patch, `after` against post-modify/drop state, and every error path
(both set, unknown/dropped target, on `modify_column`, on a declared
table column, inherited from an abstract base, on an MV column).

## Docs

README.hcl.md `patch_table` column bullet + example; FAQ env-column
entry gains the positioned form; README.md patch paragraph; CLAUDE.md.
