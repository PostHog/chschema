package hcl

import "sort"

// RedactedValue is the literal string ClickHouse substitutes for a secret when
// the connecting user lacks displaySecretsInShowAndSelect / the server lacks
// display_secrets_in_show_and_select / a query omits
// format_display_secrets_in_show_and_select. It shows up in
// system.named_collections values and in a dictionary's create_table_query
// SOURCE(...) credentials.
//
// It is kept in-band: introspection stores the marker as the value, so it
// round-trips through an HCL dump and every comparison — live, dump file, or
// drift — can tell "this secret is unknown to hclexp" apart from "this object
// has no secret". The diff layer treats it as unknown ("don't generate a
// change") rather than as a literal target, and sqlgen refuses to emit any
// statement containing it: writing the string "[HIDDEN]" over a real cluster
// credential would be actively destructive.
//
// Authored HCL may also use it deliberately — `password = "[HIDDEN]"` declares
// a secret that is managed outside hclexp.
const RedactedValue = "[HIDDEN]"

// NamedCollectionChange describes a planned change to a named collection.
type NamedCollectionChange struct {
	Name string

	// Add is set for fresh adds AND for the create-half of a recreate
	// (so the create has the full target spec).
	Add *NamedCollectionSpec

	// Drop is true for pure drops AND for the drop-half of a recreate.
	Drop bool

	// Recreate is true when ON CLUSTER changed; sqlgen emits
	// DROP then CREATE adjacently.
	Recreate bool

	// Surgical (non-recreate) changes:
	SetParams     []NamedCollectionParam
	DeleteParams  []string
	CommentChange *StringChange

	// SkippedRedactedParams lists param keys whose diff was suppressed
	// because the value is the redacted "[HIDDEN]" placeholder on one side
	// while the other side holds a real value, differs in flags, or lacks
	// the param entirely. Identically-redacted params (hidden on both
	// sides, flags equal) are not a difference and are not listed. The CLI
	// surfaces these so operators know hclexp couldn't verify equality.
	SkippedRedactedParams []string

	// Error is non-empty when the diff describes an unsupported transition
	// (e.g. external↔managed). sqlgen emits no DDL; the CLI surfaces it.
	Error string
}

func (c NamedCollectionChange) IsEmpty() bool {
	return c.Add == nil && !c.Drop && !c.Recreate &&
		len(c.SetParams) == 0 && len(c.DeleteParams) == 0 &&
		c.CommentChange == nil && c.Error == "" &&
		len(c.SkippedRedactedParams) == 0
}

func (c NamedCollectionChange) IsUnsafe() bool { return false }

// diffNamedCollections returns the per-collection changes between two
// schemas. External-on-both-sides changes are omitted. External-on-one-side
// transitions surface as Error entries.
func diffNamedCollections(from, to []NamedCollectionSpec) []NamedCollectionChange {
	fromIdx := map[string]*NamedCollectionSpec{}
	for i := range from {
		fromIdx[from[i].Name] = &from[i]
	}
	toIdx := map[string]*NamedCollectionSpec{}
	for i := range to {
		toIdx[to[i].Name] = &to[i]
	}

	names := map[string]bool{}
	for n := range fromIdx {
		names[n] = true
	}
	for n := range toIdx {
		names[n] = true
	}
	sorted := make([]string, 0, len(names))
	for n := range names {
		sorted = append(sorted, n)
	}
	sort.Strings(sorted)

	var out []NamedCollectionChange
	for _, n := range sorted {
		f, ft := fromIdx[n], toIdx[n]
		switch {
		case f == nil && ft != nil:
			if ft.External {
				continue
			}
			toCopy := *ft
			out = append(out, NamedCollectionChange{Name: n, Add: &toCopy})
		case f != nil && ft == nil:
			if f.External {
				continue
			}
			out = append(out, NamedCollectionChange{Name: n, Drop: true})
		default:
			if f.External && ft.External {
				continue
			}
			if f.External != ft.External {
				out = append(out, NamedCollectionChange{
					Name:  n,
					Error: "external↔managed migration not supported; promote/demote manually",
				})
				continue
			}
			change := diffOneNamedCollection(n, f, ft)
			if !change.IsEmpty() {
				out = append(out, change)
			}
		}
	}
	return out
}

func diffOneNamedCollection(name string, f, ft *NamedCollectionSpec) NamedCollectionChange {
	change := NamedCollectionChange{Name: name}

	// ON CLUSTER mismatch → recreate.
	if !ncPtrStringEqual(f.Cluster, ft.Cluster) {
		toCopy := *ft
		change.Recreate = true
		change.Drop = true
		change.Add = &toCopy
		return change
	}

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
	for _, p := range f.Params {
		if _, present := toParams[p.Key]; !present {
			change.DeleteParams = append(change.DeleteParams, p.Key)
		}
	}

	if !ncPtrStringEqual(f.Comment, ft.Comment) {
		change.CommentChange = &StringChange{Old: f.Comment, New: ft.Comment}
	}

	return change
}

func ncPtrStringEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func ncPtrBoolEqual(a, b *bool) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
