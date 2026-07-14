# Dictionary changes emit CREATE OR REPLACE (issue #140)

## Problem

A changed `dictionary` block produces no DDL and is reported `UNSAFE`. Run
the migration and the dictionary is silently left as-is.

```
$ hclexp diff -left left.hcl -right right.hcl -sql   # only `lifetime` differs
-- UNSAFE: posthog.exchange_rate_dict: dictionary change requires CREATE OR REPLACE DICTIONARY (changed: lifetime)
-- no changes
```

The diff engine contradicts itself. `DictionaryDiff` (`diff.go`) documents:

> ClickHouse has no useful in-place ALTER DICTIONARY, so any non-empty diff is
> materialized as a `CREATE OR REPLACE DICTIONARY` statement — safe, not
> flagged as unsafe.

and `DictionaryDiff.IsUnsafe()` returns `false`. But the `AlterDictionaries`
loop in `sqlgen.go` appends an `UnsafeChange` for every altered dictionary and
emits no statement.

The cause is a missing struct field, not a real hazard: `DictionaryDiff` is
`{Name string; Changed []string}` — it records *which fields* changed but not
the *target spec*, so `sqlgen` has no `DictionarySpec` to hand to
`createDictionarySQL` (which already renders a complete
`CREATE OR REPLACE DICTIONARY`). `diffDictionary(from, to *DictionarySpec)`
has `to` in hand and drops it. A dictionary holds no persistent data — it
reloads from its source — so the recreate genuinely is safe.

## The complication: redacted source credentials

`optSecret` (`dictionary_introspect.go`) drops a source credential that
ClickHouse redacted to `[HIDDEN]`, so it is never re-emitted and applied back
over the real secret (#52). A live-introspected dictionary therefore has
`password = nil` where the authored HCL has a real value — and
`dictSourceEqual` is a plain `reflect.DeepEqual` on the decoded source, so
that registers as a `source` change.

Today that is inert (it only prints an unsafe line). The moment we emit DDL it
is not:

- **Perpetual drift.** The phantom `source` change would rewrite the
  dictionary on *every* run, forever.
- **Credential loss.** If the *right* side of the diff is the introspected one
  — `drift` always puts the drifter on the right; so does
  `diff -left ./schema -right clickhouse://` — the generated
  `CREATE OR REPLACE` is built from a spec with `password = nil`, i.e. it
  writes a dictionary with **no password at all**.

The same hole already exists on the add path: `diff -left <empty> -right
clickhouse://` emits a `CREATE DICTIONARY` with the password silently missing.

## Design

One invariant, from which everything else follows:

> **Never emit dictionary DDL built from a spec whose source secret the server
> redacted.**

### 1. The source spec records what was hidden from it

`types.go`:

```go
type DictionarySourceSpec struct {
	Kind    string   `hcl:"kind,label" diff:"-"`
	Body    hcl.Body `hcl:",remain"    diff:"-"`
	Decoded DictionarySource

	// RedactedSecrets names the source fields (e.g. "password") whose value
	// the server returned as "[HIDDEN]" during introspection. The value itself
	// is dropped (see optSecret / #52); this records only that we could not
	// see it. No hcl tag: it is introspection metadata and never round-trips
	// to HCL.
	RedactedSecrets []string
}
```

`optSecret` keeps returning `nil` for a redacted value — so neither the secret
nor the literal `[HIDDEN]` ever reaches an HCL dump, exactly as #52 requires —
but now also appends the field name to `RedactedSecrets`. Field names are the
DDL argument names: `password`, `credentials_password`.

### 2. The diff stops hallucinating a `source` change

`dictSourceEqual` masks, on *both* sides, every field either side flagged
redacted, then compares:

```go
func dictSourceEqual(a, b *DictionarySourceSpec) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Kind != b.Kind {
		return false
	}
	mask := unionSecrets(a.RedactedSecrets, b.RedactedSecrets)
	return reflect.DeepEqual(maskSecrets(a.Decoded, mask), maskSecrets(b.Decoded, mask))
}
```

`maskSecrets` zeroes the named fields on a copy of the decoded source, matching
struct fields by their `hcl` tag name (reflection; ~15 lines, works for all
seven source kinds without touching the `DictionarySource` interface).

Consequences:

- A password the introspector never saw can no longer read as a difference.
- When **neither** side is redacted, a genuine password change still diffs and
  still applies. Rolling a credential through hclexp keeps working.

`DictionaryDiff` grows two fields:

```go
type DictionaryDiff struct {
	Name    string
	Changed []string

	// New is the target spec — the CREATE OR REPLACE DICTIONARY that
	// reconciles the change is rendered from it.
	New DictionarySpec

	// SkippedRedactedSecrets names source fields whose comparison was
	// suppressed because either side's value was redacted. Mirrors
	// NamedCollectionChange.SkippedRedactedParams: hclexp could not verify
	// equality, and says so rather than guessing.
	SkippedRedactedSecrets []string
}

func (d DictionaryDiff) IsEmpty() bool {
	return len(d.Changed) == 0 && len(d.SkippedRedactedSecrets) == 0
}
func (d DictionaryDiff) IsUnsafe() bool { return false }
```

`IsEmpty` counting the skipped list mirrors `NamedCollectionChange.IsEmpty`: a
dictionary whose *only* difference is an unverifiable secret surfaces as an
altered object carrying zero operations. It is permanent noise on a cluster
that redacts secrets, and that is the point — it is the tool declining to claim
equality it cannot establish.

### 3. sqlgen emits the statement diff.go always promised

The `AlterDictionaries` loop in `GenerateSQL`:

```go
for _, dd := range dc.AlterDictionaries {
	if len(dd.Changed) == 0 {
		continue // skipped-secret-only: nothing to reconcile
	}
	if reason := redactedSecretBlock(dd.New); reason != "" {
		out.Unsafe = append(out.Unsafe, UnsafeChange{Database: dc.Database, Table: dd.Name, Reason: reason})
		continue
	}
	emit(OpAlter, KindDictionary, dc.Database, dd.Name, createDictionarySQL(dc.Database, dd.New))
}
```

It stays where it already sits in the emission order — after ALTER TABLE — so a
dictionary sourcing from a just-widened table is replaced against the new shape.

### 4. The guard, on both paths

`redactedSecretBlock(d DictionarySpec) string` returns `""` when the spec's
source has no redacted secrets, otherwise a reason naming the fields and the
remedy:

```
dictionary source secret(s) [password] were redacted by the server, so
CREATE OR REPLACE DICTIONARY would write the dictionary without them; grant
displaySecretsInShowAndSelect AND set display_secrets_in_show_and_select=1
```

Applied in two places:

- `orderedCreates` skips an added dictionary that fails the guard, and
  `GenerateSQL` records the `UnsafeChange` for it.
- The alter loop, as above.

Routing through `out.Unsafe` is what `MaterializedViewDiff.Recreate` already
does, so `unsafeFor` in `compare.go` lights up `Unsafe`/`UnsafeReason` on the
`ObjectComparison` with no new plumbing.

The guard fires exactly when emitting would be wrong (introspected schema on
the right: `drift`, or `diff -left ./schema -right clickhouse://`). The normal
direction — desired HCL on the right — has real secrets and emits cleanly.

### 5. Reporting

`fieldChangesForDictionary` appends one entry per skipped secret, the shape
`fieldChangesForNamedCollection` already uses for `param:`:

```go
FieldChange{Field: "source.password", Change: "modify", Old: RedactedValue, New: RedactedValue}
```

Text, JSON, and counts all stay derived from the one `[]ObjectComparison`
model, so they cannot contradict each other.

## Behaviour change

| Case | Before | After |
|---|---|---|
| `lifetime` changed | `-- UNSAFE`, no DDL | `CREATE OR REPLACE DICTIONARY`, safe |
| HCL password vs redacted live | phantom `source` change, `-- UNSAFE` | not a change; object notes the unverifiable secret, no DDL |
| any change, target redacted | `-- UNSAFE` (right, by accident) | `-- UNSAFE` naming the field and the fix |
| fresh add, source redacted | `CREATE DICTIONARY`, password silently missing | blocked, `-- UNSAFE` |
| genuine password change, neither side redacted | `-- UNSAFE`, no DDL | `CREATE OR REPLACE DICTIONARY`, applies |

`DictionaryDiff.IsUnsafe()` stays `false` and finally agrees with the generated
output.

## Testing

Unit (`internal/loader/hcl/`):

- `diffDictionary`: a redacted secret on one side produces no `source` change
  and populates `SkippedRedactedSecrets`.
- `diffDictionary`: a genuine password change with neither side redacted does
  produce a `source` change.
- `GenerateSQL`: an altered dictionary emits `CREATE OR REPLACE DICTIONARY`
  and records **no** `UnsafeChange` (this is the #140 regression test).
- `GenerateSQL`: an altered dictionary whose target has a redacted secret emits
  no statement and records an `UnsafeChange` naming the field.
- `GenerateSQL`: an added dictionary whose source has a redacted secret emits
  no `CREATE DICTIONARY` and records an `UnsafeChange`.
- `BuildObjectComparisons`: an altered dictionary carries its
  `CREATE OR REPLACE` op; a skipped-secret-only one carries a
  `source.password` `FieldChange` and zero ops.
- `buildDictionarySourceFromAST`: a `[HIDDEN]` password yields
  `Password == nil` **and** `RedactedSecrets == ["password"]`.

Snapshot (`test/`): a dictionary `lifetime` change renders the expected DDL.

Live (`-clickhouse`): create a dictionary, change its `lifetime`, apply the
generated DDL, introspect, and assert the round-trip matches — proving the
emitted `CREATE OR REPLACE` is accepted by ClickHouse and takes effect.

Existing test to update: `sqlgen_test.go:673` currently asserts the
unsafe-and-no-DDL behaviour this change removes.

## Docs

- `docs/README.hcl.md`: add `source.<secret>` to the `FieldChange.field`
  vocabulary; document that a dictionary change reconciles via
  `CREATE OR REPLACE DICTIONARY` and when it is blocked.
- `CLAUDE.md`: the dictionary bullet under Supported Features.

## Out of scope

- Resolving secrets from anywhere but the authored HCL (no vault/env lookup).
- Any change to `optSecret`'s refusal to re-emit a redacted value.
