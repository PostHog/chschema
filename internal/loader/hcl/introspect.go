package hcl

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Introspect builds a DatabaseSpec from a live ClickHouse database.
// The returned spec is "flat" — no extend/abstract/patch_table — and every
// TableSpec has its Engine.Decoded populated. Order of tables is the
// declaration order ClickHouse reports; column order matches positions in
// system.columns.
func Introspect(ctx context.Context, conn driver.Conn, database string) (*DatabaseSpec, error) {
	db := &DatabaseSpec{Name: database}

	tables, err := introspectTables(ctx, conn, database)
	if err != nil {
		return nil, err
	}
	for i := range tables {
		cols, err := introspectColumns(ctx, conn, database, tables[i].Name)
		if err != nil {
			return nil, fmt.Errorf("columns for %s.%s: %w", database, tables[i].Name, err)
		}
		tables[i].Columns = cols

		idx, err := introspectIndexes(ctx, conn, database, tables[i].Name)
		if err != nil {
			return nil, fmt.Errorf("indexes for %s.%s: %w", database, tables[i].Name, err)
		}
		tables[i].Indexes = idx
	}
	db.Tables = tables
	return db, nil
}

func introspectTables(ctx context.Context, conn driver.Conn, database string) ([]TableSpec, error) {
	const q = `SELECT name, engine_full, create_table_query, sorting_key, partition_key, sampling_key, primary_key, comment
		FROM system.tables
		WHERE database = ? AND NOT is_temporary
		ORDER BY name`
	rows, err := conn.Query(ctx, q, database)
	if err != nil {
		return nil, fmt.Errorf("query system.tables: %w", err)
	}
	defer rows.Close()

	var out []TableSpec
	for rows.Next() {
		var (
			name, engineFull, createSQL                                string
			sortingKey, partitionKey, samplingKey, primaryKey, comment string
		)
		if err := rows.Scan(&name, &engineFull, &createSQL, &sortingKey, &partitionKey, &samplingKey, &primaryKey, &comment); err != nil {
			return nil, fmt.Errorf("scan system.tables: %w", err)
		}

		eng, err := ParseEngineString(engineFull)
		if err != nil {
			return nil, fmt.Errorf("parse engine %q for table %s.%s: %w", engineFull, database, name, err)
		}
		t := TableSpec{
			Name: name,
			Engine: &EngineSpec{
				Kind:    eng.Kind(),
				Decoded: eng,
			},
		}
		t.OrderBy = splitKeyList(sortingKey)
		t.PartitionBy = nilIfEmpty(partitionKey)
		t.SampleBy = nilIfEmpty(samplingKey)
		if pk := splitKeyList(primaryKey); len(pk) > 0 && !stringSliceEqual(pk, t.OrderBy) {
			t.PrimaryKey = pk
		}
		t.Comment = nilIfEmpty(comment)
		t.TTL = extractTableTTL(engineFull)
		t.Settings = extractTableSettings(engineFull, eng)
		t.createSQL = createSQL
		out = append(out, t)
	}
	return out, rows.Err()
}

func introspectColumns(ctx context.Context, conn driver.Conn, database, table string) ([]ColumnSpec, error) {
	// Note: per-column TTL is not in system.columns on all supported CH
	// versions; recovering it requires parsing engine_full or
	// create_table_query. Left as future work.
	const q = `SELECT name, type, default_kind, default_expression, comment, compression_codec
		FROM system.columns
		WHERE database = ? AND table = ?
		ORDER BY position`
	rows, err := conn.Query(ctx, q, database, table)
	if err != nil {
		return nil, fmt.Errorf("query system.columns: %w", err)
	}
	defer rows.Close()

	var out []ColumnSpec
	for rows.Next() {
		var name, typ, defaultKind, defaultExpr, comment, codec string
		if err := rows.Scan(&name, &typ, &defaultKind, &defaultExpr, &comment, &codec); err != nil {
			return nil, fmt.Errorf("scan system.columns: %w", err)
		}
		c := ColumnSpec{Name: name, Type: typ}
		if defaultExpr != "" {
			switch defaultKind {
			case "DEFAULT":
				v := defaultExpr
				c.Default = &v
			case "MATERIALIZED":
				v := defaultExpr
				c.Materialized = &v
			case "EPHEMERAL":
				v := defaultExpr
				c.Ephemeral = &v
			case "ALIAS":
				v := defaultExpr
				c.Alias = &v
			}
		} else if defaultKind == "EPHEMERAL" {
			empty := ""
			c.Ephemeral = &empty
		}
		c.Comment = nilIfEmpty(comment)
		c.Codec = parseCodecExpression(codec)
		out = append(out, c)
	}
	return out, rows.Err()
}

func introspectIndexes(ctx context.Context, conn driver.Conn, database, table string) ([]IndexSpec, error) {
	const q = `SELECT name, expr, type, granularity
		FROM system.data_skipping_indices
		WHERE database = ? AND table = ?
		ORDER BY name`
	rows, err := conn.Query(ctx, q, database, table)
	if err != nil {
		return nil, fmt.Errorf("query system.data_skipping_indices: %w", err)
	}
	defer rows.Close()

	var out []IndexSpec
	for rows.Next() {
		var name, expr, typ string
		var gran uint64
		if err := rows.Scan(&name, &expr, &typ, &gran); err != nil {
			return nil, fmt.Errorf("scan system.data_skipping_indices: %w", err)
		}
		out = append(out, IndexSpec{
			Name:        name,
			Expr:        expr,
			Type:        typ,
			Granularity: int(gran),
		})
	}
	return out, rows.Err()
}

// ParseEngineString parses a ClickHouse engine_full value into a typed
// HCL Engine. Strips the trailing ORDER BY / PARTITION BY / SAMPLE BY / TTL
// / SETTINGS / COMMENT clauses, then dispatches on the engine name.
func ParseEngineString(engineFull string) (Engine, error) {
	decl := extractEngineDeclaration(engineFull)
	switch {
	case strings.HasPrefix(decl, "ReplicatedMergeTree"):
		p, err := extractEngineParams(decl)
		if err != nil {
			return nil, err
		}
		if len(p) < 2 {
			return nil, fmt.Errorf("ReplicatedMergeTree needs (zoo_path, replica_name); got %v", p)
		}
		return EngineReplicatedMergeTree{ZooPath: p[0], ReplicaName: p[1]}, nil
	case strings.HasPrefix(decl, "ReplicatedReplacingMergeTree"):
		p, err := extractEngineParams(decl)
		if err != nil {
			return nil, err
		}
		if len(p) < 2 {
			return nil, fmt.Errorf("ReplicatedReplacingMergeTree needs (zoo_path, replica_name[, version_column]); got %v", p)
		}
		e := EngineReplicatedReplacingMergeTree{ZooPath: p[0], ReplicaName: p[1]}
		if len(p) > 2 {
			e.VersionColumn = &p[2]
		}
		return e, nil
	case strings.HasPrefix(decl, "ReplicatedCollapsingMergeTree"):
		p, err := extractEngineParams(decl)
		if err != nil {
			return nil, err
		}
		if len(p) != 3 {
			return nil, fmt.Errorf("ReplicatedCollapsingMergeTree needs (zoo_path, replica_name, sign_column); got %v", p)
		}
		return EngineReplicatedCollapsingMergeTree{ZooPath: p[0], ReplicaName: p[1], SignColumn: p[2]}, nil
	case strings.HasPrefix(decl, "ReplicatedAggregatingMergeTree"):
		p, err := extractEngineParams(decl)
		if err != nil {
			return nil, err
		}
		if len(p) < 2 {
			return nil, fmt.Errorf("ReplicatedAggregatingMergeTree needs (zoo_path, replica_name); got %v", p)
		}
		return EngineReplicatedAggregatingMergeTree{ZooPath: p[0], ReplicaName: p[1]}, nil
	case strings.HasPrefix(decl, "ReplacingMergeTree"):
		p, err := extractEngineParams(decl)
		if err != nil {
			return nil, err
		}
		e := EngineReplacingMergeTree{}
		if len(p) > 0 && p[0] != "" {
			e.VersionColumn = &p[0]
		}
		return e, nil
	case strings.HasPrefix(decl, "SummingMergeTree"):
		return parseSummingMergeTreeHCL(decl)
	case strings.HasPrefix(decl, "CollapsingMergeTree"):
		p, err := extractEngineParams(decl)
		if err != nil {
			return nil, err
		}
		if len(p) != 1 {
			return nil, fmt.Errorf("CollapsingMergeTree needs (sign_column); got %v", p)
		}
		return EngineCollapsingMergeTree{SignColumn: p[0]}, nil
	case strings.HasPrefix(decl, "AggregatingMergeTree"):
		return EngineAggregatingMergeTree{}, nil
	case strings.HasPrefix(decl, "MergeTree"):
		return EngineMergeTree{}, nil
	case strings.HasPrefix(decl, "Distributed"):
		p, err := extractEngineParams(decl)
		if err != nil {
			return nil, err
		}
		if len(p) < 3 {
			return nil, fmt.Errorf("Distributed needs (cluster, db, table[, sharding_key]); got %v", p)
		}
		e := EngineDistributed{ClusterName: p[0], RemoteDatabase: p[1], RemoteTable: p[2]}
		if len(p) > 3 {
			e.ShardingKey = &p[3]
		}
		return e, nil
	case strings.HasPrefix(decl, "Log"):
		return EngineLog{}, nil
	case strings.HasPrefix(decl, "Kafka"):
		return parseKafkaEngine(engineFull, decl)
	}
	return nil, fmt.Errorf("unsupported engine: %q", decl)
}

// extractEngineDeclaration trims the trailing storage clauses off engine_full.
func extractEngineDeclaration(engineFull string) string {
	keywords := []string{" ORDER BY", " PARTITION BY", " SAMPLE BY", " TTL", " SETTINGS", " COMMENT", " PRIMARY KEY"}
	end := len(engineFull)
	for _, kw := range keywords {
		if i := strings.Index(engineFull, kw); i != -1 && i < end {
			end = i
		}
	}
	return strings.TrimSpace(engineFull[:end])
}

// extractEngineParams pulls comma-separated args from "Name(arg1, arg2)".
// Quotes are stripped after splitting; commas inside quotes are preserved.
func extractEngineParams(decl string) ([]string, error) {
	open := strings.Index(decl, "(")
	close := strings.LastIndex(decl, ")")
	if open == -1 || close == -1 || open >= close {
		return nil, nil
	}
	inner := strings.TrimSpace(decl[open+1 : close])
	if inner == "" {
		return nil, nil
	}

	var parts []string
	var b strings.Builder
	inQuote := false
	var quote rune
	for _, r := range inner {
		switch {
		case (r == '\'' || r == '"') && !inQuote:
			inQuote = true
			quote = r
			b.WriteRune(r)
		case r == quote && inQuote:
			inQuote = false
			quote = 0
			b.WriteRune(r)
		case r == ',' && !inQuote:
			parts = append(parts, strings.TrimSpace(b.String()))
			b.Reset()
		default:
			b.WriteRune(r)
		}
	}
	if b.Len() > 0 {
		parts = append(parts, strings.TrimSpace(b.String()))
	}
	for i, p := range parts {
		parts[i] = strings.Trim(p, `'"`)
	}
	return parts, nil
}

func parseSummingMergeTreeHCL(decl string) (Engine, error) {
	// SummingMergeTree wraps its column list in an extra pair of parens:
	//   SummingMergeTree((col1, col2))
	// while the no-arg form is just SummingMergeTree() or SummingMergeTree.
	re := regexp.MustCompile(`SummingMergeTree\(\((.*?)\)\)`)
	m := re.FindStringSubmatch(decl)
	e := EngineSummingMergeTree{}
	if len(m) > 1 && strings.TrimSpace(m[1]) != "" {
		for _, c := range strings.Split(m[1], ",") {
			e.SumColumns = append(e.SumColumns, strings.TrimSpace(c))
		}
	}
	return e, nil
}

// parseKafkaEngine handles both legacy positional Kafka(a,b,c,d) and the
// modern Kafka SETTINGS kafka_broker_list=... form. The settings form is
// preferred by modern ClickHouse, so check it first.
func parseKafkaEngine(engineFull, decl string) (Engine, error) {
	settings := extractEngineSettings(engineFull)
	if len(settings) > 0 {
		return EngineKafka{
			BrokerList:    strings.Split(settings["kafka_broker_list"], ","),
			Topic:         settings["kafka_topic_list"],
			ConsumerGroup: settings["kafka_group_name"],
			Format:        settings["kafka_format"],
		}, nil
	}
	p, err := extractEngineParams(decl)
	if err != nil {
		return nil, err
	}
	if len(p) < 4 {
		return nil, fmt.Errorf("Kafka needs (broker_list, topic, group, format) or SETTINGS form; got %v", p)
	}
	return EngineKafka{
		BrokerList:    strings.Split(p[0], ","),
		Topic:         p[1],
		ConsumerGroup: p[2],
		Format:        p[3],
	}, nil
}

// extractEngineSettings pulls the SETTINGS clause from engine_full into a map.
func extractEngineSettings(engineFull string) map[string]string {
	i := strings.Index(engineFull, " SETTINGS ")
	if i == -1 {
		return nil
	}
	tail := engineFull[i+len(" SETTINGS "):]
	// Truncate at COMMENT if present.
	if j := strings.Index(tail, " COMMENT"); j != -1 {
		tail = tail[:j]
	}
	out := map[string]string{}
	// Naive parse: split by top-level commas, then "k = 'v'" or "k = v".
	for _, part := range splitSettingsList(tail) {
		eq := strings.Index(part, "=")
		if eq == -1 {
			continue
		}
		k := strings.TrimSpace(part[:eq])
		v := strings.TrimSpace(part[eq+1:])
		v = strings.Trim(v, `'"`)
		out[k] = v
	}
	return out
}

func splitSettingsList(s string) []string {
	var parts []string
	var b strings.Builder
	inQuote := false
	var quote rune
	for _, r := range s {
		switch {
		case (r == '\'' || r == '"') && !inQuote:
			inQuote = true
			quote = r
			b.WriteRune(r)
		case r == quote && inQuote:
			inQuote = false
			quote = 0
			b.WriteRune(r)
		case r == ',' && !inQuote:
			parts = append(parts, strings.TrimSpace(b.String()))
			b.Reset()
		default:
			b.WriteRune(r)
		}
	}
	if b.Len() > 0 {
		parts = append(parts, strings.TrimSpace(b.String()))
	}
	return parts
}

// parseCodecExpression turns ClickHouse's "CODEC(LZ4)" wrapping into the
// inner argument list. Returns nil for empty / default codec.
func parseCodecExpression(s string) *string {
	if s == "" {
		return nil
	}
	if strings.HasPrefix(s, "CODEC(") && strings.HasSuffix(s, ")") {
		inner := s[len("CODEC(") : len(s)-1]
		return &inner
	}
	return &s
}

// splitKeyList parses ClickHouse-formatted key lists. Single-column keys
// come back as "id"; multi-column as "id, ts" or "(id, ts)".
func splitKeyList(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	s = strings.TrimPrefix(s, "(")
	s = strings.TrimSuffix(s, ")")
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, strings.TrimSpace(p))
	}
	return out
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
