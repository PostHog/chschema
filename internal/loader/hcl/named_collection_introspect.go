package hcl

import (
	"context"
	"errors"
	"fmt"

	chparser "github.com/AfterShip/clickhouse-sql-parser/parser"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// IntrospectNamedCollections returns every named collection the live
// ClickHouse cluster exposes via system.named_collections.
//
// The cluster's system.named_collections schema varies by ClickHouse
// version. Newer versions expose `source` (DDL vs. config file) and
// `create_query` (the full CREATE statement, parseable for ON CLUSTER,
// comments, and OVERRIDABLE flags). Older versions expose only `name`
// and `collection` (a Map of key→value pairs).
//
// This implementation reads only the columns guaranteed across versions
// — `name` and `collection`. As a consequence:
//   - Config-file (XML) collections are NOT filtered out at introspection;
//     the operator-side `external = true` declaration is the way to keep
//     hclexp from trying to manage them.
//   - ON CLUSTER, comment, and OVERRIDABLE/NOT OVERRIDABLE flags are not
//     captured (the `collection` map doesn't carry them). Round-tripping
//     these requires a newer ClickHouse with `create_query` exposed —
//     extending the introspection then is a follow-up.
//
// The query also enables `format_display_secrets_in_show_and_select` so
// values come back as their real strings rather than the redacted
// `[HIDDEN]` placeholder. The connecting user must have the
// `displaySecretsInShowAndSelect` access (granted via SQL `GRANT
// displaySecretsInShowAndSelect ON *.* TO <user>`, or in users.xml via
// `<show_named_collections_secrets>1</show_named_collections_secrets>`)
// for the setting to actually take effect — otherwise ClickHouse keeps
// returning `[HIDDEN]`.
func IntrospectNamedCollections(ctx context.Context, conn driver.Conn) ([]NamedCollectionSpec, error) {
	const q = `SELECT name, collection FROM system.named_collections ORDER BY name SETTINGS format_display_secrets_in_show_and_select = 1`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query system.named_collections: %w", err)
	}
	defer rows.Close()

	var out []NamedCollectionSpec
	for rows.Next() {
		var name string
		var collection map[string]string
		if err := rows.Scan(&name, &collection); err != nil {
			return nil, fmt.Errorf("scan system.named_collections: %w", err)
		}
		nc := NamedCollectionSpec{Name: name}
		// Sort keys for stable output.
		keys := make([]string, 0, len(collection))
		for k := range collection {
			keys = append(keys, k)
		}
		sortStrings(keys)
		for _, k := range keys {
			nc.Params = append(nc.Params, NamedCollectionParam{Key: k, Value: collection[k]})
		}
		out = append(out, nc)
	}
	return out, rows.Err()
}

func sortStrings(ss []string) {
	for i := 1; i < len(ss); i++ {
		for j := i; j > 0 && ss[j-1] > ss[j]; j-- {
			ss[j-1], ss[j] = ss[j], ss[j-1]
		}
	}
}

// RedactedParamKeys returns the param keys whose introspected value is
// the literal "[HIDDEN]" placeholder ClickHouse returns when the user
// lacks displaySecretsInShowAndSelect / the server-side
// display_secrets_in_show_and_select is off. Returns an empty slice when
// the spec is fully un-redacted. Callers use this to warn operators that
// the diff cannot be trusted for those specific params.
func RedactedParamKeys(nc NamedCollectionSpec) []string {
	var keys []string
	for _, p := range nc.Params {
		if p.Value == RedactedValue {
			keys = append(keys, p.Key)
		}
	}
	return keys
}

// buildNamedCollectionFromCreateSQL parses a CREATE NAMED COLLECTION
// statement and returns the corresponding NamedCollectionSpec.
func buildNamedCollectionFromCreateSQL(createSQL string) (NamedCollectionSpec, error) {
	stmt, err := parseCreateStatement(createSQL)
	if err != nil {
		return NamedCollectionSpec{}, err
	}
	cnc, ok := stmt.(*chparser.CreateNamedCollection)
	if !ok {
		return NamedCollectionSpec{}, errors.New("no CREATE NAMED COLLECTION statement found")
	}
	return buildNamedCollectionFromAST(cnc)
}

func buildNamedCollectionFromAST(cnc *chparser.CreateNamedCollection) (NamedCollectionSpec, error) {
	out := NamedCollectionSpec{}
	if cnc.Name != nil {
		out.Name = dictIdent(cnc.Name)
	}
	if cnc.OnCluster != nil && cnc.OnCluster.Expr != nil {
		out.Cluster = strPtr(formatNode(cnc.OnCluster.Expr))
	}
	for _, p := range cnc.Params {
		if p == nil || p.Name == nil {
			continue
		}
		key := dictIdent(p.Name)
		val := ""
		if p.Value != nil {
			val = dictArgValueString(p.Value)
		}
		ncp := NamedCollectionParam{Key: key, Value: val}
		switch {
		case p.Overridable:
			v := true
			ncp.Overridable = &v
		case p.NotOverridable:
			v := false
			ncp.Overridable = &v
		}
		out.Params = append(out.Params, ncp)
	}
	return out, nil
}
