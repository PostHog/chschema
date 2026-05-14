package introspection

import (
	"fmt"
	"strings"

	chparser "github.com/AfterShip/clickhouse-sql-parser/parser"
	"github.com/posthog/chschema/gen/chschema_v1"
)

// ParseEngine parses a ClickHouse engine string into a protobuf Engine message.
//
// It accepts the values ClickHouse reports in system.tables: the bare engine
// name (e.g. "MergeTree") and the engine_full clause (e.g.
// "ReplicatedMergeTree('/path', 'replica') ORDER BY id SETTINGS ..."). The
// engine_full clause is parsed by wrapping it in a synthetic CREATE TABLE and
// running it through the ClickHouse SQL parser, then walking the EngineExpr
// AST — no hand-rolled string parsing.
func ParseEngine(engineName, engineFull string) (*chschema_v1.Engine, error) {
	e, err := parseEngineExpr(engineName, engineFull)
	if err != nil {
		return nil, err
	}
	return engineFromAST(e)
}

// parseEngineExpr parses the engine clause into a chparser.EngineExpr by
// embedding it in a minimal CREATE TABLE statement.
func parseEngineExpr(engineName, engineFull string) (*chparser.EngineExpr, error) {
	raw := strings.TrimSpace(engineFull)
	if raw == "" {
		raw = strings.TrimSpace(engineName)
	}
	if raw == "" {
		return nil, fmt.Errorf("empty engine declaration")
	}

	// engine_full carries trailing ORDER BY / PARTITION BY / TTL / SETTINGS
	// clauses that aren't part of the engine itself and that the parser may
	// not accept (e.g. `TTL ... WHERE ...`). Isolate just `Name(args)`.
	decl := engineDeclaration(raw)

	sql := "CREATE TABLE __chschema_engine_probe__ (__c Int8) ENGINE = " + decl
	stmts, err := chparser.NewParser(sql).ParseStmts()
	if err != nil {
		return nil, fmt.Errorf("failed to parse engine declaration %q: %w", decl, err)
	}
	for _, s := range stmts {
		if ct, ok := s.(*chparser.CreateTable); ok && ct.Engine != nil {
			return ct.Engine, nil
		}
	}
	return nil, fmt.Errorf("no engine found in declaration %q", decl)
}

// engineDeclaration isolates the `EngineName(args)` portion of an engine
// string, dropping any trailing ORDER BY / PARTITION BY / TTL / SETTINGS
// clauses. The engine name is the leading identifier; if a `(` immediately
// follows it, the balanced (quote-aware) parenthesized argument list is
// included.
func engineDeclaration(s string) string {
	s = strings.TrimSpace(s)

	i := 0
	for i < len(s) && (s[i] == '_' ||
		(s[i] >= 'a' && s[i] <= 'z') ||
		(s[i] >= 'A' && s[i] <= 'Z') ||
		(s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	name := s[:i]

	rest := strings.TrimLeft(s[i:], " \t\n")
	if !strings.HasPrefix(rest, "(") {
		return name
	}

	open := strings.IndexByte(s, '(')
	depth := 0
	inQuote := false
	var quote byte
	for j := open; j < len(s); j++ {
		c := s[j]
		switch {
		case inQuote:
			if c == quote {
				inQuote = false
			}
		case c == '\'' || c == '"' || c == '`':
			inQuote = true
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 {
				return s[:j+1]
			}
		}
	}
	return s // unbalanced; let the parser surface the error
}

// ParseEngineSettings extracts the SETTINGS clause from a ClickHouse
// engine_full string and returns it as a key/value map. Values keep their
// SQL form (string literals stay quoted) so they can be emitted verbatim.
// Returns nil when engine_full has no SETTINGS clause.
func ParseEngineSettings(engineFull string) (map[string]string, error) {
	ef := strings.TrimSpace(engineFull)
	if ef == "" {
		return nil, nil
	}
	decl := engineDeclaration(ef)
	tail := strings.TrimPrefix(ef, decl)

	const kw = " SETTINGS "
	idx := strings.Index(tail, kw)
	if idx == -1 {
		return nil, nil
	}
	settingsClause := strings.TrimSpace(tail[idx+len(kw):])
	if settingsClause == "" {
		return nil, nil
	}

	sql := "CREATE TABLE __chschema_settings_probe__ (__c Int8) ENGINE = " + decl + " SETTINGS " + settingsClause
	stmts, err := chparser.NewParser(sql).ParseStmts()
	if err != nil {
		return nil, fmt.Errorf("failed to parse engine settings %q: %w", settingsClause, err)
	}
	for _, s := range stmts {
		ct, ok := s.(*chparser.CreateTable)
		if !ok || ct.Engine == nil || ct.Engine.Settings == nil {
			continue
		}
		out := make(map[string]string, len(ct.Engine.Settings.Items))
		for _, it := range ct.Engine.Settings.Items {
			out[it.Name.Name] = formatNode(it.Expr)
		}
		if len(out) == 0 {
			return nil, nil
		}
		return out, nil
	}
	return nil, nil
}

// ParseEngineTTL extracts the table TTL expression from a ClickHouse
// engine_full string (the part between ` TTL ` and ` SETTINGS `). It is
// returned as a raw expression string — the parser can't always handle the
// `TTL <expr> WHERE/GROUP BY ...` forms, and round-tripping only needs the
// verbatim text. Returns "" when there is no TTL clause.
func ParseEngineTTL(engineFull string) string {
	ef := strings.TrimSpace(engineFull)
	if ef == "" {
		return ""
	}
	decl := engineDeclaration(ef)
	tail := strings.TrimPrefix(ef, decl)

	const kw = " TTL "
	i := strings.Index(tail, kw)
	if i == -1 {
		return ""
	}
	ttl := tail[i+len(kw):]
	if j := strings.Index(ttl, " SETTINGS "); j != -1 {
		ttl = ttl[:j]
	}
	return strings.TrimSpace(ttl)
}

// ParseEngineSampleBy extracts the SAMPLE BY expression from a ClickHouse
// engine_full string (the part between ` SAMPLE BY ` and the following ` TTL `
// or ` SETTINGS ` clause). Returns "" when there is no SAMPLE BY clause.
func ParseEngineSampleBy(engineFull string) string {
	ef := strings.TrimSpace(engineFull)
	if ef == "" {
		return ""
	}
	decl := engineDeclaration(ef)
	tail := strings.TrimPrefix(ef, decl)

	const kw = " SAMPLE BY "
	i := strings.Index(tail, kw)
	if i == -1 {
		return ""
	}
	sample := tail[i+len(kw):]
	for _, end := range []string{" TTL ", " SETTINGS "} {
		if j := strings.Index(sample, end); j != -1 {
			sample = sample[:j]
		}
	}
	return strings.TrimSpace(sample)
}

// engineFromAST dispatches on the parsed engine name and builds the typed
// protobuf Engine message.
func engineFromAST(e *chparser.EngineExpr) (*chschema_v1.Engine, error) {
	params := engineParams(e)

	switch e.Name {
	case "MergeTree":
		return &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_MergeTree{MergeTree: &chschema_v1.MergeTree{}},
		}, nil

	case "ReplicatedMergeTree":
		if len(params) < 2 {
			return nil, fmt.Errorf("ReplicatedMergeTree requires 2 parameters (zoo_path, replica_name), got %d", len(params))
		}
		return &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_ReplicatedMergeTree{
				ReplicatedMergeTree: &chschema_v1.ReplicatedMergeTree{
					ZooPath:     params[0],
					ReplicaName: params[1],
				},
			},
		}, nil

	case "ReplacingMergeTree":
		// ReplacingMergeTree([version [, is_deleted]])
		engine := &chschema_v1.ReplacingMergeTree{}
		if len(params) > 0 {
			engine.VersionColumn = &params[0]
		}
		if len(params) > 1 {
			engine.IsDeletedColumn = &params[1]
		}
		return &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_ReplacingMergeTree{ReplacingMergeTree: engine},
		}, nil

	case "ReplicatedReplacingMergeTree":
		// ReplicatedReplacingMergeTree(zoo_path, replica[, version [, is_deleted]])
		if len(params) < 2 {
			return nil, fmt.Errorf("ReplicatedReplacingMergeTree requires at least 2 parameters, got %d", len(params))
		}
		engine := &chschema_v1.ReplicatedReplacingMergeTree{
			ZooPath:     params[0],
			ReplicaName: params[1],
		}
		if len(params) > 2 {
			engine.VersionColumn = &params[2]
		}
		if len(params) > 3 {
			engine.IsDeletedColumn = &params[3]
		}
		return &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_ReplicatedReplacingMergeTree{ReplicatedReplacingMergeTree: engine},
		}, nil

	case "SummingMergeTree":
		engine := &chschema_v1.SummingMergeTree{}
		// SummingMergeTree takes an optional tuple of columns to sum:
		// SummingMergeTree((col1, col2)) — a single tuple parameter.
		for _, p := range params {
			engine.SumColumns = append(engine.SumColumns, flattenTuple(p)...)
		}
		return &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_SummingMergeTree{SummingMergeTree: engine},
		}, nil

	case "CollapsingMergeTree":
		if len(params) != 1 {
			return nil, fmt.Errorf("CollapsingMergeTree requires 1 parameter (sign column), got %d", len(params))
		}
		return &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_CollapsingMergeTree{
				CollapsingMergeTree: &chschema_v1.CollapsingMergeTree{SignColumn: params[0]},
			},
		}, nil

	case "ReplicatedCollapsingMergeTree":
		if len(params) != 3 {
			return nil, fmt.Errorf("ReplicatedCollapsingMergeTree requires 3 parameters, got %d", len(params))
		}
		return &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_ReplicatedCollapsingMergeTree{
				ReplicatedCollapsingMergeTree: &chschema_v1.ReplicatedCollapsingMergeTree{
					ZooPath:     params[0],
					ReplicaName: params[1],
					SignColumn:  params[2],
				},
			},
		}, nil

	case "AggregatingMergeTree":
		return &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_AggregatingMergeTree{AggregatingMergeTree: &chschema_v1.AggregatingMergeTree{}},
		}, nil

	case "ReplicatedAggregatingMergeTree":
		if len(params) < 2 {
			return nil, fmt.Errorf("ReplicatedAggregatingMergeTree requires 2 parameters (zoo_path, replica_name), got %d", len(params))
		}
		return &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_ReplicatedAggregatingMergeTree{
				ReplicatedAggregatingMergeTree: &chschema_v1.ReplicatedAggregatingMergeTree{
					ZooPath:     params[0],
					ReplicaName: params[1],
				},
			},
		}, nil

	case "Distributed":
		if len(params) < 3 {
			return nil, fmt.Errorf("Distributed requires at least 3 parameters, got %d", len(params))
		}
		engine := &chschema_v1.Distributed{
			ClusterName:    params[0],
			RemoteDatabase: params[1],
			RemoteTable:    params[2],
		}
		if len(params) > 3 {
			engine.ShardingKey = &params[3]
		}
		return &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_Distributed{Distributed: engine},
		}, nil

	case "Log":
		return &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_Log{Log: &chschema_v1.Log{}},
		}, nil

	case "Kafka":
		if len(params) < 4 {
			return nil, fmt.Errorf("Kafka requires 4 parameters (broker_list, topic, consumer_group, format), got %d", len(params))
		}
		brokerList := strings.Split(params[0], ",")
		for i := range brokerList {
			brokerList[i] = strings.TrimSpace(brokerList[i])
		}
		return &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_Kafka{
				Kafka: &chschema_v1.Kafka{
					BrokerList:    brokerList,
					Topic:         params[1],
					ConsumerGroup: params[2],
					Format:        params[3],
				},
			},
		}, nil

	default:
		return nil, fmt.Errorf("unsupported engine type: %s", e.Name)
	}
}

// engineParams renders each engine parameter expression to its string form,
// stripping surrounding SQL quotes so callers see the underlying value.
func engineParams(e *chparser.EngineExpr) []string {
	if e.Params == nil || e.Params.Items == nil {
		return nil
	}
	out := make([]string, 0, len(e.Params.Items.Items))
	for _, it := range e.Params.Items.Items {
		out = append(out, unquoteString(formatNode(it)))
	}
	return out
}

// extractParameters parses an engine declaration (e.g. "ReplicatedMergeTree('/path', 'replica')")
// and returns its comma-separated parameters with SQL quoting stripped.
func extractParameters(engineDecl string) ([]string, error) {
	e, err := parseEngineExpr("", engineDecl)
	if err != nil {
		return nil, err
	}
	params := engineParams(e)
	if params == nil {
		return []string{}, nil
	}
	return params, nil
}

// formatNode renders an AST node back to compact SQL text via the fork's
// visitor-based printer.
func formatNode(n chparser.Expr) string {
	if n == nil {
		return ""
	}
	v := chparser.NewPrintVisitor()
	if err := n.Accept(v); err != nil {
		return ""
	}
	return v.String()
}

// unquoteString strips matching surrounding single, double, or backtick quotes.
func unquoteString(s string) string {
	if len(s) >= 2 {
		first, last := s[0], s[len(s)-1]
		if (first == '\'' || first == '"' || first == '`') && first == last {
			inner := s[1 : len(s)-1]
			inner = strings.ReplaceAll(inner, "\\'", "'")
			inner = strings.ReplaceAll(inner, "\\\"", "\"")
			return inner
		}
	}
	return s
}

// flattenTuple turns a parenthesized tuple expression like "(a, b)" into its
// members ["a", "b"]. A non-tuple value is returned as a single-element slice.
func flattenTuple(s string) []string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "(") || !strings.HasSuffix(s, ")") {
		if s == "" {
			return nil
		}
		return []string{s}
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return nil
	}
	parts := strings.Split(inner, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, strings.TrimSpace(p))
	}
	return out
}
