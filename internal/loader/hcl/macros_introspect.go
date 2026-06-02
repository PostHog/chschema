package hcl

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// IntrospectMacros returns the ClickHouse macros configured on the
// connected node, read from system.macros. Macros carry a node's
// identity within the cluster — shard, replica, hostClusterRole,
// hostClusterType — substituted into ON CLUSTER DDL and ReplicatedMergeTree
// zoo paths. The returned map is keyed by macro name.
func IntrospectMacros(ctx context.Context, conn driver.Conn) (map[string]string, error) {
	const q = `SELECT macro, substitution FROM system.macros ORDER BY macro`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query system.macros: %w", err)
	}
	defer rows.Close()

	out := map[string]string{}
	for rows.Next() {
		var macro, substitution string
		if err := rows.Scan(&macro, &substitution); err != nil {
			return nil, fmt.Errorf("scan system.macros: %w", err)
		}
		out[macro] = substitution
	}
	return out, rows.Err()
}

// IntrospectNode builds a NodeSpec for the connected node: its macros plus
// a name. When nameOverride is non-empty it is used as the node name;
// otherwise the name is read from ClickHouse via hostName().
func IntrospectNode(ctx context.Context, conn driver.Conn, nameOverride string) (NodeSpec, error) {
	macros, err := IntrospectMacros(ctx, conn)
	if err != nil {
		return NodeSpec{}, err
	}

	name := nameOverride
	if name == "" {
		host, err := introspectHostName(ctx, conn)
		if err != nil {
			return NodeSpec{}, err
		}
		name = host
	}

	return NodeSpec{Name: name, Macros: macros}, nil
}

// introspectHostName returns the server-reported hostname via hostName().
func introspectHostName(ctx context.Context, conn driver.Conn) (string, error) {
	rows, err := conn.Query(ctx, `SELECT hostName()`)
	if err != nil {
		return "", fmt.Errorf("query hostName(): %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", fmt.Errorf("query hostName(): %w", err)
		}
		return "", fmt.Errorf("query hostName(): no rows returned")
	}
	var host string
	if err := rows.Scan(&host); err != nil {
		return "", fmt.Errorf("scan hostName(): %w", err)
	}
	return host, nil
}
