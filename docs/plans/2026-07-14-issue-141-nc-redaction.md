# Precise Named-Collection Redaction Handling Implementation Plan (issue #141)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Named-collection diffs stop reporting identically-redacted params as
permanent drift, never route a `[HIDDEN]` value into `SetParams`, and sqlgen
blocks CREATE/recreate statements built from redacted specs with a precise
unsafe reason — per issue #141's verdict table.

**Architecture:** Two small changes on existing seams. (1) The param loop in
`diffOneNamedCollection` gets three verdicts per param (both-hidden-silent /
unverifiable / normal compare) instead of the current two. (2) `GenerateSQL`
gains a `redactedNCBlock` guard on the NC add and recreate paths, mirroring
`redactedSecretBlock` for dictionaries (#140). The emit-level `[HIDDEN]`
backstop already exists and stays as the last line of defence; its test moves
to a vehicle the new guards don't intercept.

**Tech Stack:** Go, testify.

## Global Constraints

- **Precondition:** the #140 dictionary implementation is in flight and, at
  plan time, UNCOMMITTED — in the same files this plan edits
  (`named_collection_diff.go`, `sqlgen.go`, `sqlgen_test.go`, `diff_test.go`,
  `compare.go`). Task 1 Step 1 verifies the tree; if those files are still
  dirty with foreign changes, STOP and ask the user to land #140 first.
- Never change the module name or go version in `go.mod`.
- Tests: testify `assert`/`require`; compare whole structs, not
  field-by-field. Run `go test ./internal/... -v` and `go test ./test -v`.
- Commit style: 1-line summary, blank line, detailed description. NO AI
  attribution of any kind (no Co-Authored-By, no "Generated with" footers).
- Stage ONLY the files you touched, by exact path. Never `git add -A`, `-u`,
  or `.`.
- If `git commit` fails with "agent refused operation", commit signing is
  locked: ask the user to unlock, keep working uncommitted, retry once when
  they say go.
- Line numbers below are anchors from the tree at plan time; re-locate by
  symbol name or the quoted comment markers if they've shifted.
- `RedactedValue` (`"[HIDDEN]"`, `named_collection_diff.go`) is the shared
  redaction marker; never introduce a second constant or a bare literal.

---

### Task 1: Three-verdict param loop in `diffOneNamedCollection`

**Files:**
- Modify: `internal/loader/hcl/named_collection_diff.go` (struct doc comment
  ~line 35; param loop in `diffOneNamedCollection`, ~lines 136–158)
- Test: `internal/loader/hcl/diff_test.go` (append near
  `TestDiff_NamedCollections_HIDDEN_OnTargetSide`, ~line 914)

**Interfaces:**
- Produces: no signature changes — `diffOneNamedCollection(name string, f,
  ft *NamedCollectionSpec) NamedCollectionChange` keeps its shape; only the
  verdict logic changes. Task 2 and the compare layer consume
  `SkippedRedactedParams`/`SetParams` exactly as today.
- Consumes: `RedactedValue`, `ncPtrBoolEqual` (both already in
  `named_collection_diff.go`).

Existing tests that must KEEP passing unchanged (both are asymmetric cases,
whose semantics this task does not alter):
`TestDiff_NamedCollections_HIDDEN_SkipsValueChange` (diff_test.go:872) and
`TestDiff_NamedCollections_HIDDEN_OnTargetSide` (diff_test.go:899).

- [ ] **Step 1: Verify the precondition**

Run: `git status --short`
Expected: `internal/loader/hcl/*.go` files clean (untracked/modified entries
under `docs/plans/` are fine). If `named_collection_diff.go`, `sqlgen.go`,
`diff_test.go`, `sqlgen_test.go`, or `compare_test.go` show as modified, STOP
and ask the user to commit the in-flight #140 work first.

- [ ] **Step 2: Write the failing tests** (append to
  `internal/loader/hcl/diff_test.go` after
  `TestDiff_NamedCollections_HIDDEN_OnTargetSide`)

```go
func TestDiff_NamedCollections_BothHIDDEN_Silent(t *testing.T) {
	// Two dumps captured by a user without displaySecretsInShowAndSelect:
	// byte-identical, secrets redacted on both sides. Every observer sees
	// the same thing — this must NOT be a difference (issue #141: it
	// previously reported SkippedRedactedParams forever, so dump↔dump
	// drift of any secret-bearing collection never went green).
	spec := func() *Schema {
		return &Schema{NamedCollections: []NamedCollectionSpec{{
			Name: "s3",
			Params: []NamedCollectionParam{
				{Key: "url", Value: "https://bucket"},
				{Key: "password", Value: RedactedValue},
			},
		}}}
	}
	assert.True(t, Diff(spec(), spec()).IsEmpty())
}

func TestDiff_NamedCollections_BothHIDDEN_OverridableDiffers_Unverifiable(t *testing.T) {
	// Both values hidden but the OVERRIDABLE flag differs: the difference
	// is visible, the value needed to reconcile it is not — unverifiable.
	over := true
	from := &Schema{NamedCollections: []NamedCollectionSpec{{
		Name:   "s3",
		Params: []NamedCollectionParam{{Key: "password", Value: RedactedValue}},
	}}}
	to := &Schema{NamedCollections: []NamedCollectionSpec{{
		Name:   "s3",
		Params: []NamedCollectionParam{{Key: "password", Value: RedactedValue, Overridable: &over}},
	}}}
	cs := Diff(from, to)
	require.Len(t, cs.NamedCollections, 1)
	assert.Equal(t, NamedCollectionChange{
		Name:                  "s3",
		SkippedRedactedParams: []string{"password"},
	}, cs.NamedCollections[0])
}

func TestDiff_NamedCollections_HIDDENOnlyOnRight_NoSetOfLiteral(t *testing.T) {
	// The param exists only on the right, redacted. Today this falls
	// through into SetParams and generates `SET password = '[HIDDEN]'`,
	// overwriting the real secret with the placeholder (issue #141). It
	// must be unverifiable instead: present-vs-absent is visible, the
	// value is not.
	from := &Schema{NamedCollections: []NamedCollectionSpec{{
		Name:   "s3",
		Params: []NamedCollectionParam{{Key: "url", Value: "https://b"}},
	}}}
	to := &Schema{NamedCollections: []NamedCollectionSpec{{
		Name: "s3",
		Params: []NamedCollectionParam{
			{Key: "url", Value: "https://b"},
			{Key: "password", Value: RedactedValue},
		},
	}}}
	cs := Diff(from, to)
	require.Len(t, cs.NamedCollections, 1)
	assert.Equal(t, NamedCollectionChange{
		Name:                  "s3",
		SkippedRedactedParams: []string{"password"},
	}, cs.NamedCollections[0])
}

func TestDiff_NamedCollections_HIDDENDropped_PlainDelete(t *testing.T) {
	// Hidden on the left, absent on the right: deleting needs no value —
	// a plain DELETE, no skip entry, no literal anywhere.
	from := &Schema{NamedCollections: []NamedCollectionSpec{{
		Name:   "s3",
		Params: []NamedCollectionParam{{Key: "password", Value: RedactedValue}},
	}}}
	to := &Schema{NamedCollections: []NamedCollectionSpec{{Name: "s3"}}}
	cs := Diff(from, to)
	require.Len(t, cs.NamedCollections, 1)
	assert.Equal(t, NamedCollectionChange{
		Name:         "s3",
		DeleteParams: []string{"password"},
	}, cs.NamedCollections[0])
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test ./internal/loader/hcl/ -run 'TestDiff_NamedCollections' -v`
Expected: the two existing HIDDEN tests PASS;
`BothHIDDEN_Silent` FAILS (IsEmpty false — skip entry recorded),
`BothHIDDEN_OverridableDiffers` PASSES already or FAILS only on struct
equality (either is fine — it pins the corner),
`HIDDENOnlyOnRight_NoSetOfLiteral` FAILS (SetParams carries the literal),
`HIDDENDropped_PlainDelete` PASSES already (pins existing behaviour).

- [ ] **Step 4: Implement the three verdicts**

In `internal/loader/hcl/named_collection_diff.go`, replace the param loop and
its lead comment (currently "Param SET / DELETE. Params whose value is
\"[HIDDEN]\" on EITHER side are skipped …"):

```go
	// Param SET / DELETE. A value of "[HIDDEN]" (RedactedValue) means the
	// real value is unknown to hclexp. Three verdicts per param:
	//   - hidden on BOTH sides with equal flags: every observer sees the
	//     same thing — not a difference (identically-redacted dumps must
	//     not read as drift);
	//   - hidden on either side otherwise (including a hidden param the
	//     other side lacks entirely): unverifiable — recorded in
	//     SkippedRedactedParams and never SET, because writing the literal
	//     "[HIDDEN]" would clobber the real secret;
	//   - visible on both sides: compared normally.
	fromParams := map[string]NamedCollectionParam{}
	for _, p := range f.Params {
		fromParams[p.Key] = p
	}
	toParams := map[string]NamedCollectionParam{}
	for _, p := range ft.Params {
		toParams[p.Key] = p
	}
	for _, p := range ft.Params {
		fp, present := fromParams[p.Key]
		toHidden := p.Value == RedactedValue
		fromHidden := present && fp.Value == RedactedValue
		switch {
		case toHidden && fromHidden && ncPtrBoolEqual(fp.Overridable, p.Overridable):
			// equally blind on both sides: silent
		case toHidden || fromHidden:
			change.SkippedRedactedParams = append(change.SkippedRedactedParams, p.Key)
		case !present || fp.Value != p.Value || !ncPtrBoolEqual(fp.Overridable, p.Overridable):
			change.SetParams = append(change.SetParams, p)
		}
	}
```

(The `fromParams`/`toParams` construction and the DELETE loop below it stay
exactly as they are.)

Update the `SkippedRedactedParams` doc comment on the struct (~line 35) to
match:

```go
	// SkippedRedactedParams lists param keys whose diff was suppressed
	// because the value is the redacted "[HIDDEN]" placeholder on one side
	// while the other side holds a real value, differs in flags, or lacks
	// the param entirely. Identically-redacted params (hidden on both
	// sides, flags equal) are not a difference and are not listed. The CLI
	// surfaces these so operators know hclexp couldn't verify equality.
```

- [ ] **Step 5: Run the package tests**

Run: `go test ./internal/loader/hcl/ -v`
Expected: PASS, including the two pre-existing HIDDEN tests (asymmetric
semantics are unchanged, so they must not need edits — if they fail, the
switch logic is wrong; fix the code, not the tests).

- [ ] **Step 6: Commit**

```bash
git add internal/loader/hcl/named_collection_diff.go internal/loader/hcl/diff_test.go
git commit -m "nc diff: three-verdict redaction handling

Identically-redacted params (hidden on both sides, flags equal) are no
longer a difference — dump-to-dump drift of any secret-bearing collection
was permanently non-empty. A hidden param the other side lacks is now
unverifiable instead of falling through into SetParams, which generated
ALTER ... SET password = '[HIDDEN]' and overwrote the real secret.
Asymmetric hidden-vs-real keeps its existing skip semantics."
```

---

### Task 2: `redactedNCBlock` guard on NC add and recreate

**Files:**
- Modify: `internal/loader/hcl/named_collection_sqlgen.go` (new func)
- Modify: `internal/loader/hcl/sqlgen.go` (the loops under the comments
  `// 1. Named-collection recreates` and `// 2. Fresh NC adds`)
- Test: `internal/loader/hcl/sqlgen_test.go` (new tests; rewrite
  `TestSQLGen_EmitRefusesStatementCarryingRedactionMarker`, ~line 756),
  `internal/loader/hcl/compare_test.go` (one surfacing test)

**Interfaces:**
- Produces: `redactedNCBlock(nc NamedCollectionSpec) string` (unexported;
  returns `""` when emission is allowed, else the unsafe reason).
- Consumes: `RedactedParamKeys(nc NamedCollectionSpec) []string`
  (`named_collection_introspect.go:83` — already exists, do NOT write a new
  scanner), `RedactedValue`, `UnsafeChange`, and the emit backstop added by
  #140 (in `GenerateSQL`'s `emit` closure — it stays untouched).

- [ ] **Step 1: Write the failing tests**

Append to `internal/loader/hcl/sqlgen_test.go`:

```go
func TestSQLGen_NamedCollectionAdd_RedactedParamBlocked(t *testing.T) {
	cs := ChangeSet{NamedCollections: []NamedCollectionChange{{
		Name: "nc",
		Add: &NamedCollectionSpec{
			Name: "nc",
			Params: []NamedCollectionParam{
				{Key: "url", Value: "https://b"},
				{Key: "password", Value: RedactedValue},
			},
		},
	}}}
	out := GenerateSQL(cs)
	assert.Empty(t, out.Statements)
	require.Len(t, out.Unsafe, 1)
	assert.Equal(t, "nc", out.Unsafe[0].Table)
	assert.Contains(t, out.Unsafe[0].Reason, "named collection param(s) [password]")
	assert.Contains(t, out.Unsafe[0].Reason, "displaySecretsInShowAndSelect")
}

func TestSQLGen_NamedCollectionRecreate_RedactedParamBlocksPair(t *testing.T) {
	cluster := "main"
	cs := ChangeSet{NamedCollections: []NamedCollectionChange{{
		Name: "nc", Recreate: true, Drop: true,
		Add: &NamedCollectionSpec{
			Name:    "nc",
			Cluster: &cluster,
			Params:  []NamedCollectionParam{{Key: "password", Value: RedactedValue}},
		},
	}}}
	out := GenerateSQL(cs)
	assert.Empty(t, out.Statements, "no DROP either — dropping without recreating destroys the collection")
	require.Len(t, out.Unsafe, 1)
	assert.Contains(t, out.Unsafe[0].Reason, "named collection param(s) [password]")
}
```

Rewrite `TestSQLGen_EmitRefusesStatementCarryingRedactionMarker`
(sqlgen_test.go:756): its current vehicle — an NC add with a redacted param —
will now be intercepted by the precise guard before `emit` ever sees a
statement, so the backstop assertion (`"refusing to write it to a cluster"`)
would fail. Re-arm it with a hand-built SET, the one remaining path that can
carry the marker (the Task 1 diff logic never produces this; that is exactly
what a backstop is for):

```go
// Backstop: whatever path builds a statement, the redaction marker never
// leaves hclexp. The NC diff (three-verdict handling) and the CREATE
// guards stop it upstream, so drive emit() directly with a hand-built SET —
// the backstop is the last line of defence for paths no guard anticipated.
func TestSQLGen_EmitRefusesStatementCarryingRedactionMarker(t *testing.T) {
	cs := ChangeSet{NamedCollections: []NamedCollectionChange{{
		Name:      "nc",
		SetParams: []NamedCollectionParam{{Key: "password", Value: RedactedValue}},
	}}}
	out := GenerateSQL(cs)
	assert.Empty(t, out.Statements, "writing the literal [HIDDEN] would clobber the real secret")
	require.Len(t, out.Unsafe, 1)
	assert.Equal(t, "nc", out.Unsafe[0].Table)
	assert.Contains(t, out.Unsafe[0].Reason, "refusing to write it to a cluster")
}
```

Append to `internal/loader/hcl/compare_test.go`:

```go
// A blocked NC add still surfaces in the object view: status added, zero
// operations, unsafe with the precise reason — same wiring dictionaries use.
func TestBuildObjectComparisons_NamedCollectionRedactedAddBlocked(t *testing.T) {
	cs := ChangeSet{NamedCollections: []NamedCollectionChange{{
		Name: "nc",
		Add: &NamedCollectionSpec{
			Name:   "nc",
			Params: []NamedCollectionParam{{Key: "password", Value: RedactedValue}},
		},
	}}}
	gen := GenerateSQL(cs)
	objs := BuildObjectComparisons(cs, gen, nil, nil)
	require.Len(t, objs, 1)
	assert.Equal(t, StatusAdded, objs[0].Status)
	assert.Empty(t, objs[0].Operations)
	assert.True(t, objs[0].Unsafe)
	assert.Contains(t, objs[0].UnsafeReason, "param(s) [password]")
}
```

- [ ] **Step 2: Run to verify failures**

Run: `go test ./internal/loader/hcl/ -run 'TestSQLGen_NamedCollection|TestSQLGen_EmitRefuses|TestBuildObjectComparisons_NamedCollection' -v`
Expected: the two new SQLGen tests FAIL on the reason assertions (the
backstop fires with its generic reason instead of the precise one); the
rewritten backstop test PASSES (the SET path is already backstopped); the
compare test FAILS on `UnsafeReason` content.

- [ ] **Step 3: Implement the guard**

Append to `internal/loader/hcl/named_collection_sqlgen.go`:

```go
// redactedNCBlock reports why no CREATE may be generated from nc, or ""
// when there is no obstacle. A CREATE NAMED COLLECTION writes every param,
// so a spec whose values carry the RedactedValue marker cannot be emitted:
// hclexp does not know the real values, and the literal placeholder would
// overwrite them. Mirrors redactedSecretBlock for dictionaries.
func redactedNCBlock(nc NamedCollectionSpec) string {
	keys := RedactedParamKeys(nc)
	if len(keys) == 0 {
		return ""
	}
	return fmt.Sprintf("named collection param(s) [%s] are unknown to hclexp (%s: redacted by the server at introspection, "+
		"or declared unmanaged in HCL); CREATE NAMED COLLECTION would write the literal placeholder over the real value. "+
		"Grant displaySecretsInShowAndSelect AND set display_secrets_in_show_and_select=1, or apply this change manually",
		strings.Join(keys, ", "), RedactedValue)
}
```

In `internal/loader/hcl/sqlgen.go`, the recreate loop (locate by the comment
`// 1. Named-collection recreates`) becomes:

```go
	for _, ncc := range cs.NamedCollections {
		if ncc.Recreate && ncc.Add != nil {
			if reason := redactedNCBlock(*ncc.Add); reason != "" {
				// Block the whole pair: dropping without being able to
				// recreate would destroy the collection.
				out.Unsafe = append(out.Unsafe, UnsafeChange{Database: "", Table: ncc.Name, Reason: reason})
			} else {
				emit(OpDrop, KindNamedCollection, "", ncc.Name, dropNamedCollectionSQL(ncc.Name))
				emit(OpCreate, KindNamedCollection, "", ncc.Name, createNamedCollectionSQL(*ncc.Add))
			}
		}
		if ncc.Error != "" {
			out.Unsafe = append(out.Unsafe, UnsafeChange{
				Database: "",
				Table:    ncc.Name,
				Reason:   "named collection: " + ncc.Error,
			})
		}
	}
```

The add loop (locate by `// 2. Fresh NC adds`) becomes:

```go
	for _, ncc := range cs.NamedCollections {
		if ncc.Add != nil && !ncc.Recreate {
			if reason := redactedNCBlock(*ncc.Add); reason != "" {
				out.Unsafe = append(out.Unsafe, UnsafeChange{Database: "", Table: ncc.Name, Reason: reason})
				continue
			}
			emit(OpCreate, KindNamedCollection, "", ncc.Name, createNamedCollectionSQL(*ncc.Add))
		}
	}
```

The SET/DELETE loop (`// 8. ALTER NAMED COLLECTION`) and the emit backstop
stay untouched.

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/loader/hcl/ -v && go test ./test -v && go build ./...`
Expected: PASS. If any `test/` snapshot covering NC DDL changes, inspect the
diff — only redacted-spec scenarios may change (statement replaced by an
unsafe entry); anything else is a regression.

- [ ] **Step 5: Commit**

```bash
git add internal/loader/hcl/named_collection_sqlgen.go internal/loader/hcl/sqlgen.go \
  internal/loader/hcl/sqlgen_test.go internal/loader/hcl/compare_test.go
git commit -m "sqlgen: block NC CREATE/recreate built from redacted params

Mirrors redactedSecretBlock for dictionaries: an added or ON-CLUSTER
recreated collection whose target params carry [HIDDEN] emits nothing and
records an UnsafeChange naming the params. A recreate blocks the whole
DROP+CREATE pair — dropping without recreating would destroy the
collection. The emit backstop remains the last line of defence; its test
now drives a hand-built SET, the only path left that can carry the marker.

Closes #141"
```

---

### Task 3: Documentation

**Files:**
- Modify: `docs/README.hcl.md` (the `[HIDDEN]` sentence in the field-
  vocabulary notes, ~line 671 — locate with `grep -n 'HIDDEN' docs/README.hcl.md`)

**Interfaces:** none (docs only). `CLAUDE.md` needs no change — verified: its
only named-collection mentions are the exclude `object_types` example and the
field-vocabulary bullet, neither of which describes redaction semantics.

- [ ] **Step 1: Update the README sentence**

Current text (one sentence, ends the field-vocabulary notes):

> A param whose value is redacted on either side reports `[HIDDEN]` on both —
> hclexp could not verify equality (grant `displaySecretsInShowAndSelect` to
> compare).

Replace with:

> A param whose value is redacted (`[HIDDEN]`) on one side while the other
> side holds a real value, differs in flags, or lacks the param entirely
> reports `[HIDDEN]` on both sides — hclexp could not verify equality and
> emits no DDL for it. Identically-redacted params (hidden on both sides,
> flags equal) are not a difference, so identically-redacted dumps compare
> clean. An added or ON-CLUSTER-recreated collection whose params are
> redacted is not emitted at all; it is flagged unsafe with a reason naming
> the params. Grant `displaySecretsInShowAndSelect` AND set
> `display_secrets_in_show_and_select=1` to compare and emit real values.

- [ ] **Step 2: Verify nothing else references the old wording**

Run: `grep -rn "redacted on either side" docs/ CLAUDE.md`
Expected: no matches (the README sentence was the only occurrence).

- [ ] **Step 3: Build + full test sweep**

Run: `go build ./... && go test ./internal/... -v && go test ./test -v`
Expected: PASS (docs-only change; this is the final gate).

- [ ] **Step 4: Commit**

```bash
git add docs/README.hcl.md
git commit -m "docs: precise named-collection redaction semantics (#141)

The [HIDDEN] paragraph in README.hcl.md now states the three verdicts:
identically-redacted params are not a difference, one-sided redaction is
unverifiable and emits no DDL, and redacted adds/recreates are blocked
with an unsafe reason."
```

---

## Final verification (after all tasks)

```bash
go build -o hclexp ./cmd/hclexp
go test ./internal/... -v
go test ./cmd/hclexp/ -v
go test ./test -v
```

End-to-end spot-check of the issue's headline repro (two byte-identical
redacted dumps must not drift):

```bash
mkdir -p /tmp/nc-drift && cat > /tmp/nc-drift/node-a.hcl <<'EOF'
named_collection "s3" {
  param "url"      { value = "https://bucket" }
  param "password" { value = "[HIDDEN]" }
}
EOF
cp /tmp/nc-drift/node-a.hcl /tmp/nc-drift/node-b.hcl
./hclexp drift -dir /tmp/nc-drift; echo "exit: $?"
# -> OK (all identical); exit: 0    (was: drifting + exit 1)
```

Out of scope (recorded in issue #141's context): dictionary-side redaction
(#140, its own plan), resolving secrets from vaults/env, and NC introspection
capturing OVERRIDABLE flags.
