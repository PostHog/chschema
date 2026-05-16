package hcl

import (
	"context"
	"errors"
	"fmt"

	chparser "github.com/AfterShip/clickhouse-sql-parser/parser"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// IntrospectNamedCollections returns every DDL-managed named collection
// the live ClickHouse cluster knows about. Config-file collections
// (source != 'DDL') are filtered out — they're not ours to manage.
func IntrospectNamedCollections(ctx context.Context, conn driver.Conn) ([]NamedCollectionSpec, error) {
	const q = `SELECT name, create_query FROM system.named_collections WHERE source = 'DDL' ORDER BY name`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query system.named_collections: %w", err)
	}
	defer rows.Close()

	var out []NamedCollectionSpec
	for rows.Next() {
		var name, createSQL string
		if err := rows.Scan(&name, &createSQL); err != nil {
			return nil, fmt.Errorf("scan system.named_collections: %w", err)
		}
		nc, err := buildNamedCollectionFromCreateSQL(createSQL)
		if err != nil {
			return nil, fmt.Errorf("parse create_query for %s: %w", name, err)
		}
		if nc.Name == "" {
			nc.Name = name
		}
		out = append(out, nc)
	}
	return out, rows.Err()
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
