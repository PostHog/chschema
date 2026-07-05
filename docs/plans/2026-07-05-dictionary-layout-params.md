# Dictionary layout params (#115) — model the documented set, abort on unknowns

Status: design approved 2026-07-05; inline TDD execution.
Issue: https://github.com/PostHog/chschema/issues/115

## Problem

`buildDictionaryLayoutFromAST` reads known args from `dictArgsMap` and
ignores leftovers: `LAYOUT(FLAT(INITIAL_ARRAY_SIZE 1024 MAX_ARRAY_SIZE
500000))` → `LayoutFlat{}`, no error (verified repro). Same class as
#108/#109/#87. `buildDictionarySourceFromAST` shares the args-map
pattern — same hole for unknown source args.

## Docs-verified parameter gaps

- `flat`: `initial_array_size`, `max_array_size` (int).
- `hashed`, `sparse_hashed`, `complex_key_hashed`,
  `complex_key_sparse_hashed`: `shards`, `shard_load_queue_backlog`
  (int), `max_load_factor` (float — new value type).
- Already correct: cache (size_in_cells), hashed_array (shards),
  range_hashed (strategy), ip_trie, direct, regexp_tree.
- `preallocate` on complex_key_hashed: gone from current docs; kept for
  old-DDL introspection, noted deprecated.

## Design

1. **Model** (`dictionary_layouts.go`): optional fields on the five
   structs; `DecodeDictionaryLayout` gains gohcl arms for flat / hashed
   / sparse_hashed / complex_key_sparse_hashed (now have bodies).
2. **Consuming reads + guard** (`dictionary_introspect.go`): arg
   readers take the map and delete on read (`takeArg(args, key)`
   string; `optInt64`/`optBool`/`optFloat64` wrap it); after each
   builder's switch, leftover keys → error
   `layout %s: unknown parameter(s) %v (unmodeled — refusing to drop)`.
   Same guard in `buildDictionarySourceFromAST` (incl. optSecret path).
3. **Dump** (`dictionary_dump.go`): `writeOptFloat`; new fields in
   `writeDictionaryLayout`.
4. **sqlgen** (`dictionary_sqlgen.go`): `layoutSQL` emits the params
   (uppercase arg names, values unquoted; float via strconv 'g').
5. **Tests** (extend `dictionary_roundtrip_test.go`): fully-loaded
   variants for the five layouts (float round-trip), unknown-arg error
   cases for layout AND source, existing 39 subtests stay green
   (regression: bare variants unchanged).

Out of scope: unmodeled layout *kinds* (ssd_cache, polygon — already
loud), per-kind arg *validation* beyond type parsing (CH validates).

## Execution order (TDD)

1. Failing tests: parameterized five-layout cases + guard cases.
2. Model + decode arms.
3. Consuming helpers + guards.
4. Dump + sqlgen emission.
5. Full sweep (unit, snapshot, live), PR closing #115.
