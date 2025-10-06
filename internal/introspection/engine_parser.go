package introspection

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/posthog/chschema/gen/chschema_v1"
)

// ParseEngine parses a ClickHouse engine string into a protobuf Engine message
// Handles engine strings from system.tables.engine column like:
//   - "MergeTree"
//   - "ReplicatedMergeTree"
//   - "ReplacingMergeTree"
//   - etc.
//
// And from system.tables.engine_full column like:
//   - "ReplacingMergeTree(version) ORDER BY ..."
//   - "SummingMergeTree((col1, col2)) ORDER BY ..."
//   - "ReplicatedMergeTree('/path', 'replica') ORDER BY ..."
func ParseEngine(engineName, engineFull string) (*chschema_v1.Engine, error) {
	// Extract just the engine declaration part (before ORDER BY or PARTITION BY)
	engineDecl := extractEngineDeclaration(engineFull)
	if engineDecl == "" {
		engineDecl = engineName
	}

	switch {
	case strings.HasPrefix(engineDecl, "MergeTree"):
		return parseMergeTree(engineDecl)
	case strings.HasPrefix(engineDecl, "ReplicatedMergeTree"):
		return parseReplicatedMergeTree(engineDecl)
	case strings.HasPrefix(engineDecl, "ReplacingMergeTree"):
		return parseReplacingMergeTree(engineDecl)
	case strings.HasPrefix(engineDecl, "ReplicatedReplacingMergeTree"):
		return parseReplicatedReplacingMergeTree(engineDecl)
	case strings.HasPrefix(engineDecl, "SummingMergeTree"):
		return parseSummingMergeTree(engineDecl)
	case strings.HasPrefix(engineDecl, "ReplicatedCollapsingMergeTree"):
		return parseReplicatedCollapsingMergeTree(engineDecl)
	case strings.HasPrefix(engineDecl, "CollapsingMergeTree"):
		return parseCollapsingMergeTree(engineDecl)
	case strings.HasPrefix(engineDecl, "Distributed"):
		return parseDistributed(engineDecl)
	case strings.HasPrefix(engineDecl, "Log"):
		return &chschema_v1.Engine{
			EngineType: &chschema_v1.Engine_Log{
				Log: &chschema_v1.Log{},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported engine type: %s", engineName)
	}
}

// extractEngineDeclaration extracts the engine declaration from engine_full
// "ReplacingMergeTree(version) ORDER BY id SETTINGS ..." -> "ReplacingMergeTree(version)"
func extractEngineDeclaration(engineFull string) string {
	// Find the position of ORDER BY, PARTITION BY, or SETTINGS
	keywords := []string{" ORDER BY", " PARTITION BY", " SETTINGS"}
	minPos := len(engineFull)

	for _, keyword := range keywords {
		if pos := strings.Index(engineFull, keyword); pos != -1 && pos < minPos {
			minPos = pos
		}
	}

	if minPos < len(engineFull) {
		return strings.TrimSpace(engineFull[:minPos])
	}
	return strings.TrimSpace(engineFull)
}

// parseMergeTree parses "MergeTree" or "MergeTree()"
func parseMergeTree(engineDecl string) (*chschema_v1.Engine, error) {
	return &chschema_v1.Engine{
		EngineType: &chschema_v1.Engine_MergeTree{
			MergeTree: &chschema_v1.MergeTree{},
		},
	}, nil
}

// parseReplicatedMergeTree parses "ReplicatedMergeTree('/path', 'replica')"
func parseReplicatedMergeTree(engineDecl string) (*chschema_v1.Engine, error) {
	params, err := extractParameters(engineDecl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ReplicatedMergeTree parameters: %w", err)
	}

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
}

// parseReplacingMergeTree parses "ReplacingMergeTree" or "ReplacingMergeTree(version)"
func parseReplacingMergeTree(engineDecl string) (*chschema_v1.Engine, error) {
	params, err := extractParameters(engineDecl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ReplacingMergeTree parameters: %w", err)
	}

	engine := &chschema_v1.ReplacingMergeTree{}
	if len(params) > 0 {
		engine.VersionColumn = &params[0]
	}

	return &chschema_v1.Engine{
		EngineType: &chschema_v1.Engine_ReplacingMergeTree{
			ReplacingMergeTree: engine,
		},
	}, nil
}

// parseReplicatedReplacingMergeTree parses "ReplicatedReplacingMergeTree('/path', 'replica'[, version])"
func parseReplicatedReplacingMergeTree(engineDecl string) (*chschema_v1.Engine, error) {
	params, err := extractParameters(engineDecl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ReplicatedReplacingMergeTree parameters: %w", err)
	}

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

	return &chschema_v1.Engine{
		EngineType: &chschema_v1.Engine_ReplicatedReplacingMergeTree{
			ReplicatedReplacingMergeTree: engine,
		},
	}, nil
}

// parseSummingMergeTree parses "SummingMergeTree" or "SummingMergeTree((col1, col2))"
func parseSummingMergeTree(engineDecl string) (*chschema_v1.Engine, error) {
	// Extract content between outer parentheses
	re := regexp.MustCompile(`SummingMergeTree\(\((.*?)\)\)`)
	matches := re.FindStringSubmatch(engineDecl)

	engine := &chschema_v1.SummingMergeTree{}

	if len(matches) > 1 {
		// Parse comma-separated column list
		columnsStr := strings.TrimSpace(matches[1])
		if columnsStr != "" {
			columns := strings.Split(columnsStr, ",")
			for _, col := range columns {
				engine.SumColumns = append(engine.SumColumns, strings.TrimSpace(col))
			}
		}
	}

	return &chschema_v1.Engine{
		EngineType: &chschema_v1.Engine_SummingMergeTree{
			SummingMergeTree: engine,
		},
	}, nil
}

// parseCollapsingMergeTree parses "CollapsingMergeTree(sign_column)"
func parseCollapsingMergeTree(engineDecl string) (*chschema_v1.Engine, error) {
	params, err := extractParameters(engineDecl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CollapsingMergeTree parameters: %w", err)
	}

	if len(params) != 1 {
		return nil, fmt.Errorf("CollapsingMergeTree requires 1 parameter (sign column), got %d", len(params))
	}

	return &chschema_v1.Engine{
		EngineType: &chschema_v1.Engine_CollapsingMergeTree{
			CollapsingMergeTree: &chschema_v1.CollapsingMergeTree{
				SignColumn: params[0],
			},
		},
	}, nil
}

// parseReplicatedCollapsingMergeTree parses "ReplicatedCollapsingMergeTree('/path', 'replica', sign_column)"
func parseReplicatedCollapsingMergeTree(engineDecl string) (*chschema_v1.Engine, error) {
	params, err := extractParameters(engineDecl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ReplicatedCollapsingMergeTree parameters: %w", err)
	}

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
}

// parseDistributed parses "Distributed(cluster, database, table[, sharding_key])"
func parseDistributed(engineDecl string) (*chschema_v1.Engine, error) {
	params, err := extractParameters(engineDecl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Distributed parameters: %w", err)
	}

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
		EngineType: &chschema_v1.Engine_Distributed{
			Distributed: engine,
		},
	}, nil
}

// extractParameters extracts comma-separated parameters from engine declaration
// "ReplicatedMergeTree('/path', 'replica')" -> ["/path", "replica"]
// Handles quoted strings with commas inside them
func extractParameters(engineDecl string) ([]string, error) {
	// Find content between parentheses
	start := strings.Index(engineDecl, "(")
	end := strings.LastIndex(engineDecl, ")")

	if start == -1 || end == -1 || start >= end {
		// No parameters
		return []string{}, nil
	}

	content := strings.TrimSpace(engineDecl[start+1 : end])
	if content == "" {
		return []string{}, nil
	}

	// Split by comma, but respect quotes
	var params []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for _, ch := range content {
		switch {
		case (ch == '\'' || ch == '"') && !inQuote:
			inQuote = true
			quoteChar = ch
		case ch == quoteChar && inQuote:
			inQuote = false
			quoteChar = 0
		case ch == ',' && !inQuote:
			params = append(params, strings.TrimSpace(current.String()))
			current.Reset()
			continue
		}
		current.WriteRune(ch)
	}

	// Add last parameter
	if current.Len() > 0 {
		params = append(params, strings.TrimSpace(current.String()))
	}

	// Remove quotes from parameters
	for i, param := range params {
		params[i] = strings.Trim(param, "'\"")
	}

	return params, nil
}
