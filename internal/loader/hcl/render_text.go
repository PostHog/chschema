package hcl

import (
	"fmt"
	"io"
	"strings"
)

// RenderObjectComparisons prints comparisons as the indented, +/-/~ marked
// summary used by `diff` (text mode) and `drift -details`. Objects render in
// input order under a header per database; named collections (empty
// database) render under a "named_collections" header. It consumes the same
// []ObjectComparison the JSON emits, so text and JSON cannot disagree.
func RenderObjectComparisons(w io.Writer, objs []ObjectComparison) {
	statusMark := map[string]string{StatusAdded: "+", StatusDropped: "-", StatusAltered: "~"}
	header := ""
	printed := false
	for _, o := range objs {
		h := fmt.Sprintf("database %q", o.Database)
		if o.ObjectType == KindNamedCollection {
			h = "named_collections"
		}
		if !printed || h != header {
			fmt.Fprintln(w, h)
			header, printed = h, true
		}
		// A raw block's inner kind is part of its identity: only a raw table
		// holds rows, so "raw table r" and "raw view r" recreate differently.
		objType := o.ObjectType
		if o.RawKind != "" {
			objType += " " + o.RawKind
		}
		if o.Error != "" {
			fmt.Fprintf(w, "  ! %s %s: %s\n", objType, o.Object, o.Error)
			continue
		}
		suffix := ""
		if o.Unsafe {
			suffix = " (UNSAFE: " + o.UnsafeReason + ")"
		}
		fmt.Fprintf(w, "  %s %s %s%s\n", statusMark[o.Status], objType, o.Object, suffix)
		for _, fc := range o.Changes {
			renderFieldChange(w, fc)
		}
	}
}

func renderFieldChange(w io.Writer, fc FieldChange) {
	field := strings.Replace(fc.Field, ":", " ", 1)
	switch fc.Change {
	case "add":
		if fc.New != "" {
			fmt.Fprintf(w, "      + %s = %s\n", field, fc.New)
		} else {
			fmt.Fprintf(w, "      + %s\n", field)
		}
	case "drop":
		fmt.Fprintf(w, "      - %s\n", field)
	case "rename":
		fmt.Fprintf(w, "      ~ %s (renamed from %s)\n", field, fc.Old)
	default: // modify
		switch {
		case fc.Field == "query" || strings.ContainsRune(fc.Old+fc.New, '\n'):
			// Canonical queries and raw DDL are long and multi-line; inlining
			// them would break the one-line-per-change layout. Presence is the
			// signal here — the JSON carries the full old/new text.
			fmt.Fprintf(w, "      ~ %s changed\n", field)
		case fc.Old == "" && fc.New == "":
			fmt.Fprintf(w, "      ~ %s changed\n", field)
		case fc.Old == "":
			fmt.Fprintf(w, "      ~ %s = %s\n", field, fc.New)
		case fc.New == "":
			fmt.Fprintf(w, "      ~ %s: %s -> (unset)\n", field, fc.Old)
		default:
			fmt.Fprintf(w, "      ~ %s: %s -> %s\n", field, fc.Old, fc.New)
		}
	}
}
