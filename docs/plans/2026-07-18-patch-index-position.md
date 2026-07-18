# patch_table: positioned index adds (`after` / `first`) — issue #160

## Why

#159's positioned column adds deduplicated 11 of the 12
column-order-blocked objects; `posthog.sharded_events` (the biggest
table, ~380 columns) remains blocked on **index** order: its per-env
`minmax_mat_*` skip indexes interleave mid-list, and a patch index add
can only append. Goldens must match dumps byte-for-byte.

## What

The exact index analogue of #159:

```hcl
patch_table "sharded_events" {
  index "minmax_mat_x" {
    expr        = "mat_x"
    type        = "minmax"
    granularity = 1
    after       = "minmax_mat_w"   # or first = true
  }
}
```

- `after` / `first` on a patch `index` add; mutually exclusive; neither
  keeps the append default.
- Adds resolve against the post-previous-op state: drops apply first
  (unchanged), so `after` may name an index added earlier in the same
  patch and a dropped/unknown target errors. A drop+add redefine may
  carry placement — drop+add is already the sanctioned redefine path,
  and a redefine lands where you put it.
- Placement is transient (cleared on application, `diff:"-"`), so the
  composed table renders byte-identical to the flat declaration.
- Patch-only: a declared table index carrying `after`/`first` is a
  resolve error (there is no move-only primitive; repositioning base
  indexes stays a full redeclaration).

## Implementation

- `types.go` `IndexSpec`: `After *string` / `First bool`
  (`hcl:...,optional`, `diff:"-"`).
- `resolver.go` `applyTablePatch`: index adds insert at the resolved
  position (shared helper generalized from #159's) and clear placement.
- `resolver.go`: `validateIndexes` alongside `validateColumns` rejects
  placement on declared indexes (incl. inherited via extend).

## Tests

Extend `patch_position_test.go`: interleaved index order with
byte-parity vs the flat declaration; `first`; chained `after`;
positioned drop+add redefine; errors (both set, unknown/dropped target,
declared-index placement, abstract-base inheritance).

## Docs

README.hcl.md patch_table index bullet + example; README.md patch
paragraph; CLAUDE.md bullet.

Related: #158/#159 (columns), #88 (DDL emission of positions).
