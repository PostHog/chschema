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
- **Credential loss.** If the *right* side of the diff is the redacted one —
  `drift` always puts the drifter on the right; so does
  `diff -left ./schema -right clickhouse://` — the generated
  `CREATE OR REPLACE` is built from a spec with `password = nil`, i.e. it
  writes a dictionary with **no password at all**.

The same hole already exists on the add path: `diff -left <empty> -right
clickhouse://` emits a `CREATE DICTIONARY` with the password silently missing.

**And the drop itself does not survive a dump.** Almost every comparison in
the ecosystem consumes dump *files*, not live introspection: `drift` loads
per-node HCL dumps (`loadDriftNodes` → `loadSide` → `LoadLayers` — a pure HCL
parse), and the golden ↔ dump gate diffs against a file `dump-live.sh` wrote
earlier. Once the redacted value is dropped, the dump merely has *no*
`password` attribute — indistinguishable from a dictionary that genuinely has
none. Any redaction bookkeeping that lives only in memory during
introspection is gone after the round trip, so a guard built on it protects
exactly one path (`-right clickhouse://`) and is inert on the paths the tool
is actually built around:

- golden (`password = "x"`) vs dump file (`password` absent) → phantom
  `source` change **forever** on the primary gate;
- descriptive direction (dump on the right) builds passwordless DDL with no
  guard at all.

Named collections already solved this: introspection keeps the literal
`[HIDDEN]` as the param *value* (`IntrospectNamedCollections`), it
round-trips through HCL like any string, and `diffOneNamedCollection` treats
it as "unknown — don't generate a change". Dictionaries diverged (#52 chose
to drop) because nothing guarded the emit path back then. The guard this plan
adds is that missing piece — which lets dictionaries adopt the same
convention.

## Design

One invariant, from which everything else follows:

> **Never write the `[HIDDEN]` placeholder to a cluster, and never emit
> dictionary DDL built from a spec whose source secret is unknown.**

### 1. Redaction is carried in-band, like named collections

`optSecret` stops dropping: a source field the server returned as `[HIDDEN]`
is kept literally, so the spec (and any dump written from it) carries
`password = "[HIDDEN]"` — exactly the `named_collection` convention and the
same `RedactedValue` constant (whose doc comment gains the dictionary case).
The slog warning stays.

This needs **no new struct field, no writer change, no loader change**:
`writeDictionarySource` already emits `Decoded`'s fields
(`writeOptStr(b, "password", v.Password)`, dictionary_dump.go:70 — today it
skips the field only because the value is nil), and loading decodes the
literal back like any string. The marker now survives
introspect → dump → load → diff, which is what makes §2 and §4 work on every
path instead of only against a live URL.

#52's protection is preserved but moves: the value was dropped at
introspection so it could never be applied; it is now kept for comparison and
**blocked at emission** (§4). It still never reaches a cluster.

Authored HCL may also write `password = "[HIDDEN]"` deliberately. It means
"this dictionary has a secret that is not managed here" and behaves
identically: compared as unknown, DDL from that spec blocked.

Groundwork facts: the seven source kinds are value types
(`SourceClickHouse`, `SourceMySQL`, `SourcePostgreSQL`, `SourceHTTP`,
`SourceFile`, `SourceExecutable`, `SourceNull`), and at most one field per
kind is secret — `password` (clickhouse/mysql/postgresql),
`credentials_password` (http), none (file/executable/null).

*Rejected — out-of-band metadata* (a `RedactedSecrets []string` on
`DictionarySourceSpec`): it either doesn't round-trip (introspection-only
metadata dies at the dump, recreating the hole above) or it costs
writer+loader plumbing and can desync from the value it describes
(hand-edited dump with a real password but stale `redacted_secrets`). The
in-band marker cannot desync — it *is* the value — and matches the existing
NC convention.

### 2. The comparison: three verdicts per secret field

`dictSourceEqual` becomes
`compareDictSources(a, b *DictionarySourceSpec) (equal bool, unverifiable []string)`.
For each source field where either side holds `RedactedValue`:

| left | right | verdict |
|---|---|---|
| `[HIDDEN]` | `[HIDDEN]` | **equal, silent** — both observers are equally blind; no skip entry |
| `[HIDDEN]` | real value | **unverifiable** — the field is masked out of the equality check and recorded |
| `[HIDDEN]` | absent (`nil`) | **a real difference** — present-vs-absent is visible even when the value isn't |

All other fields (and the no-redaction case) compare exactly as before. The
masking helper zeroes the one affected field on copies of both sides — a
small per-kind switch mirroring `writeDictionarySource`, or ~15 lines of
reflection; sources are small value structs, so copies are assignments.

Consequences:

- The phantom `source` change is gone on **every** path: live URL, dump file,
  drift.
- Dump ↔ dump comparisons with identically-redacted secrets (the normal
  `drift` case — every node dumped by the same user) are **clean**. This is a
  deliberate divergence from `diffOneNamedCollection`, which records a skip
  even when both sides are `[HIDDEN]` and thereby keeps NC-bearing node pairs
  permanently non-empty; aligning NC to the quieter rule is a follow-up.
- Known blind spot, documented not solved: two nodes whose *hidden* passwords
  genuinely differ read as equal — no observer without
  `displaySecretsInShowAndSelect` can do better.
- A golden with **no** password vs a live dictionary that has one (hidden) is
  a genuine `source` change; the prescriptive `CREATE OR REPLACE` built from
  the golden legitimately removes the password, and the next run converges.
- A genuine rotation with both sides visible still diffs and still applies.
- An unverifiable secret does ride along when *another* change legitimately
  triggers the `CREATE OR REPLACE`: the statement is built from the target
  spec, so the target's (known, real) value is written then. Only a
  secret-*only* difference is unactionable.

`DictionaryDiff` grows two fields:

```go
type DictionaryDiff struct {
	Name    string
	Changed []string

	// New is the target spec — the CREATE OR REPLACE DICTIONARY that
	// reconciles the change is rendered from it.
	New DictionarySpec

	// SkippedRedactedSecrets names source fields whose comparison was
	// suppressed because one side is redacted and the other holds a real
	// value (the asymmetric case only — see compareDictSources). hclexp
	// could not verify equality, and says so rather than guessing.
	SkippedRedactedSecrets []string
}

func (d DictionaryDiff) IsEmpty() bool {
	return len(d.Changed) == 0 && len(d.SkippedRedactedSecrets) == 0
}
func (d DictionaryDiff) IsUnsafe() bool { return false }
```

`IsEmpty` counting the skipped list mirrors `NamedCollectionChange.IsEmpty`: a
dictionary whose only difference is an unverifiable secret surfaces as an
altered object carrying zero operations — the tool declining to claim
equality it cannot establish. Because the skip fires only on the asymmetric
case, this noise appears exactly where it is meaningful (an authored secret
the tool cannot confirm) and *not* on symmetric dump↔dump drift.

### 3. sqlgen emits the statement diff.go always promised

The `AlterDictionaries` loop in `GenerateSQL`:

```go
for _, dd := range dc.AlterDictionaries {
	// Not dead code: a skipped-secret-only diff has empty Changed but is
	// non-IsEmpty, reaches this loop, and must emit nothing.
	if len(dd.Changed) == 0 {
		continue
	}
	if reason := redactedSecretBlock(dd.New); reason != "" {
		out.Unsafe = append(out.Unsafe, UnsafeChange{Database: dc.Database, Table: dd.Name, Reason: reason})
		continue
	}
	emit(OpCreate, KindDictionary, dc.Database, dd.Name, createDictionarySQL(dc.Database, dd.New))
}
```

It stays where it already sits in the emission order — after ALTER TABLE — so
a dictionary sourcing from a just-widened table is replaced against the new
shape.

**Op kind is `OpCreate`, not `OpAlter`.** The op kind describes the statement
(`CREATE OR REPLACE …`), matching added dictionaries, which already emit
CREATE OR REPLACE under `OpCreate` via `orderedCreates`; nothing in the
codebase tags a CREATE-verb statement `OpAlter`, and keeping the verb honest
lets a cross-role `plan` dedupe an add on one role against an
alter-to-the-same-target on another (same kind + SQL → one merged op).
"Altered vs added" stays visible where it belongs: `ObjectComparison.Status`.
(Rejected: a DROP+CREATE pair as NC/raw recreates use — CREATE OR REPLACE is
atomic, and a drop window would fail concurrent readers for no benefit.)

**Add-path mechanics.** `orderedCreates(cs) []Operation` has no channel to
record an `UnsafeChange`, so the guard for added dictionaries lives in
`GenerateSQL`: pre-scan `AddDictionaries`, record an `UnsafeChange` per guard
failure and collect a `blocked` set of `(database, name)`; the existing
`for _, op := range orderedCreates(cs) { emit(...) }` loop skips dictionary
ops in `blocked`. Dependency ordering is unaffected — the op is simply
omitted.

### 4. The guard, and a backstop at emit

`redactedSecretBlock(d DictionarySpec) string` returns `""` when the spec's
decoded source holds no `RedactedValue` in any field, otherwise a reason
naming the fields and the remedies:

```
dictionary source secret(s) [password] are unknown to hclexp ([HIDDEN]:
redacted by the server at introspection, or declared unmanaged in HCL);
CREATE OR REPLACE DICTIONARY would write the dictionary without the real
value. Grant displaySecretsInShowAndSelect + set
display_secrets_in_show_and_select=1, or apply manually with the real secret
```

Applied in two places: the alter loop and the add-path pre-scan (§3). Routing
through `out.Unsafe` is what the table paths already do, so `unsafeFor` in
`compare.go` lights up `Unsafe`/`UnsafeReason` on the `ObjectComparison` with
no new plumbing.

**Backstop:** `emit` refuses any statement containing the `[HIDDEN]` literal —
the statement is dropped and an `UnsafeChange` recorded (`statement contains
the [HIDDEN] redaction placeholder; refusing to write it to a cluster`). This
is defense-in-depth for dictionaries and incidentally closes two live
named-collection holes, where the literal is emitted today: an *added* NC
whose params are redacted, and a redacted param present on only one side
(`diffOneNamedCollection`'s skip requires the key on both sides; otherwise
the `[HIDDEN]` value lands in `SetParams`/`Add` and flows into DDL). The
backstop blocks the whole statement; precise per-param NC handling is a
follow-up (#141).

### 5. Reporting

`fieldChangesForDictionary` appends one entry per skipped secret, the shape
`fieldChangesForNamedCollection` already uses for `param:`:

```go
FieldChange{Field: "source.password", Change: "modify", Old: RedactedValue, New: RedactedValue}
```

Both sides render `[HIDDEN]` even when one side's real value is known — diff
output lands in CI logs, and a real secret must never be printed. Through the
existing renderer this comes out as `~ source.password: [HIDDEN] -> [HIDDEN]`,
identical to the `param:` treatment. `source.<field>` (the DDL argument name:
`source.password`, `source.credentials_password`) joins the `field`
vocabulary.

Text, JSON, and counts all stay derived from the one `[]ObjectComparison`
model, so they cannot contradict each other.

## Behaviour change

| Case | Before | After |
|---|---|---|
| `lifetime` changed | `-- UNSAFE`, no DDL | `CREATE OR REPLACE DICTIONARY`, safe |
| authored password vs live `[HIDDEN]` | phantom `source` change, `-- UNSAFE` | not a change; `source.password` reported unverifiable, no DDL |
| same comparison via a dump **file** (golden ↔ dump gate) | phantom change, forever | same as live: unverifiable note, no phantom change |
| dump ↔ dump, both sides `[HIDDEN]` (drift) | equal (both values dropped) | equal (both values kept and identical) — drift stays quiet |
| authored has no password, live has one (`[HIDDEN]`) | phantom `source` change, inert | genuine `source` change; prescriptive DDL removes the password and converges |
| any change, target spec redacted | `-- UNSAFE` (right, by accident) | `-- UNSAFE` naming the field and the remedies |
| fresh add, source redacted | `CREATE DICTIONARY` with password silently missing | blocked, `-- UNSAFE` |
| genuine password change, both sides visible | `-- UNSAFE`, no DDL | `CREATE OR REPLACE DICTIONARY`, applies |
| any generated statement containing `[HIDDEN]` (incl. named collections) | emitted — writes the literal over the real secret | blocked at emit, `-- UNSAFE` |

`DictionaryDiff.IsUnsafe()` stays `false` and finally agrees with the
generated output.

## Gates, CI, and migration

An **asymmetric** unverifiable secret (authored real value vs live
`[HIDDEN]`) keeps the dictionary in `objects` with a `source.password` entry
on every run — `jq -e '.objects == []'` gates stay red, deliberately: the
tool will not certify a comparison it cannot make. Remedies, with their
trade-offs:

1. **Grant** `displaySecretsInShowAndSelect` + set
   `display_secrets_in_show_and_select=1` for the introspecting user:
   everything becomes verifiable and credential rotation through hclexp
   works. Caution: dumps then contain real secrets in plaintext — treat dump
   artifacts accordingly.
2. **Declare the secret unmanaged** in the authored HCL:
   `password = "[HIDDEN]"`. Both sides hidden → compares clean. Trade-off:
   any other change to that dictionary is guard-blocked (a CREATE OR REPLACE
   cannot preserve a secret it doesn't know), so those changes are applied
   manually.
3. **`-exclude`** the dictionary — removes the whole object from comparison;
   the bluntest tool.

`drift` specifically: symmetric redaction (every node dumped by the same
user) compares clean, so the CI drift guard does **not** go permanently red.
Credential rotation on a redacting cluster cannot be driven by hclexp — a
secret-only difference emits no DDL — rotate out of band or use remedy 1.

Migration notes (one-time, when this lands):

- **Regenerate dumps.** Old dumps carry no `password` at all (the drop); new
  introspection keeps `[HIDDEN]`. An old dump compared against a new one
  reads absent-vs-hidden = a genuine change until dumps are refreshed.
- **Audit authored dictionaries for omitted secrets.** Under the old
  behaviour, omitting `password` in a golden was inert. Now a passwordless
  golden vs a live passworded dictionary is a *genuine* diff whose
  prescriptive DDL **removes the live password**. Goldens that omitted the
  secret because "it never mattered" must either add the real value or
  declare `password = "[HIDDEN]"` (remedy 2).

## Testing

Unit (`internal/loader/hcl/`):

- `compareDictSources` matrix: both-hidden → equal, no skip; hidden-vs-real →
  equal (masked), skip recorded; hidden-vs-absent → unequal, no skip;
  both-real rotation → unequal, no skip.
- `diffDictionary`: `SkippedRedactedSecrets` populated only in the asymmetric
  case; `IsEmpty` counts it; `New` carries the target spec.
- `GenerateSQL`: an altered dictionary emits `CREATE OR REPLACE DICTIONARY`
  under `OpCreate` and records **no** `UnsafeChange` (the #140 regression
  test).
- `GenerateSQL`: altered dictionary whose target has a redacted secret emits
  no statement and records an `UnsafeChange` naming the field; same for an
  added dictionary (the `blocked` set path); a skipped-secret-only diff emits
  nothing and records nothing unsafe.
- `emit` backstop: a change set that would write `[HIDDEN]` (e.g. NC
  `SetParams` carrying the literal) emits no statement and records the
  backstop `UnsafeChange`.
- `BuildObjectComparisons`: an altered dictionary carries its
  `CREATE OR REPLACE` op; a skipped-secret-only one carries the
  `source.password` `FieldChange` and zero ops.
- `buildDictionarySourceFromAST`: a `[HIDDEN]` password now yields
  `Password == strPtr("[HIDDEN]")` (was `nil`) — update the #52-era tests.
- **Round-trip regression (the dump-file hole):** an introspected-style spec
  with `[HIDDEN]` → `hclload.Write` → load → diff against an authored spec
  with a real password → no `source` change, one skip entry. The same pair
  dump↔dump (both `[HIDDEN]`) → `IsEmpty()`.

Snapshot (`test/`): a dictionary `lifetime` change renders the expected DDL.

Live (`-clickhouse`): create a dictionary, change its `lifetime`, apply the
generated DDL, introspect, and assert the round-trip matches — proving the
emitted `CREATE OR REPLACE` is accepted by ClickHouse and takes effect.

Existing test to update: `sqlgen_test.go:673`
(`TestSQLGen_AlterDictionary_EmitsUnsafe`) asserts the unsafe-and-no-DDL
behaviour this change removes.

## Docs

- `docs/README.hcl.md`: add `source.<secret>` to the `FieldChange.field`
  vocabulary; document that a dictionary change reconciles via
  `CREATE OR REPLACE DICTIONARY` and when it is blocked; document the
  `[HIDDEN]` convention now shared by dictionaries and named collections
  (round-trips through dumps, compares as unknown, blocked at emission,
  authored `password = "[HIDDEN]"` = unmanaged declaration); the gate
  remedies above.
- `CLAUDE.md`: the dictionary bullet under Supported Features.

## Out of scope / follow-ups

- Resolving secrets from anywhere but the authored HCL (no vault/env lookup).
- The redacted *value* still never reaches a cluster — but enforcement moves
  from drop-at-introspection (#52's mechanism, retired here) to
  block-at-emission, which also covers values arriving via dumps and authored
  HCL that the drop never protected.
- Precise named-collection redaction handling — align NC's both-hidden case
  to the silent rule (today it keeps dump↔dump NC drift permanently
  non-empty) and replace the emit backstop with per-param guards. Filed as
  #141; the backstop here already stops the corruption.
- `hclexp web` renders `[HIDDEN]` as an ordinary value — fine as-is.
