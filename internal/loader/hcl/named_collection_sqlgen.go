package hcl

import (
	"fmt"
	"strings"
)

func createNamedCollectionSQL(nc NamedCollectionSpec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE NAMED COLLECTION %s", nc.Name)
	if nc.Cluster != nil {
		fmt.Fprintf(&b, " ON CLUSTER %s", *nc.Cluster)
	}
	b.WriteString(" AS ")
	parts := make([]string, len(nc.Params))
	for i, p := range nc.Params {
		parts[i] = formatNCParam(p)
	}
	b.WriteString(strings.Join(parts, ", "))
	return b.String()
}

func dropNamedCollectionSQL(name string) string {
	return fmt.Sprintf("DROP NAMED COLLECTION %s", name)
}

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

func alterNamedCollectionSetSQL(name string, params []NamedCollectionParam) string {
	if len(params) == 0 {
		return ""
	}
	parts := make([]string, len(params))
	for i, p := range params {
		parts[i] = formatNCParam(p)
	}
	return fmt.Sprintf("ALTER NAMED COLLECTION %s SET %s", name, strings.Join(parts, ", "))
}

func alterNamedCollectionDeleteSQL(name string, keys []string) string {
	if len(keys) == 0 {
		return ""
	}
	return fmt.Sprintf("ALTER NAMED COLLECTION %s DELETE %s", name, strings.Join(keys, ", "))
}

func formatNCParam(p NamedCollectionParam) string {
	val := "'" + strings.ReplaceAll(p.Value, "'", "''") + "'"
	suffix := ""
	if p.Overridable != nil {
		if *p.Overridable {
			suffix = " OVERRIDABLE"
		} else {
			suffix = " NOT OVERRIDABLE"
		}
	}
	return fmt.Sprintf("%s = %s%s", p.Key, val, suffix)
}
