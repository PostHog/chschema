package dumper

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/posthog/chschema/gen/chschema_v1"
	"github.com/posthog/chschema/internal/introspection"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// Dumper handles exporting database schema to YAML files
type Dumper struct {
	conn clickhouse.Conn
}

// NewDumper creates a new Dumper instance
func NewDumper(conn clickhouse.Conn) *Dumper {
	return &Dumper{conn: conn}
}

// DumpOptions configures the dump operation
type DumpOptions struct {
	OutputDir  string
	Database   string
	TablesOnly bool
	Overwrite  bool
}

// Dump extracts database schema and writes YAML files
func (d *Dumper) Dump(ctx context.Context, opts DumpOptions) error {
	// 1. Introspect current database state
	introspector := introspection.NewIntrospector(d.conn)
	currentState, err := introspector.GetCurrentState(ctx)
	if err != nil {
		return fmt.Errorf("failed to introspect database: %w", err)
	}

	// 2. Create output directory structure
	if err := d.createDirectoryStructure(opts.OutputDir); err != nil {
		return fmt.Errorf("failed to create directory structure: %w", err)
	}

	// 3. Dump tables
	if err := d.dumpTables(currentState.Tables, opts); err != nil {
		return fmt.Errorf("failed to dump tables: %w", err)
	}

	// 4. Dump clusters (if not tables-only)
	if !opts.TablesOnly {
		if err := d.dumpClusters(currentState.Clusters, opts); err != nil {
			return fmt.Errorf("failed to dump clusters: %w", err)
		}
	}

	fmt.Printf("Schema dump completed successfully to %s\n", opts.OutputDir)
	return nil
}

// createDirectoryStructure creates the necessary directories
func (d *Dumper) createDirectoryStructure(outputDir string) error {
	dirs := []string{
		filepath.Join(outputDir, "tables"),
		filepath.Join(outputDir, "clusters"),
		filepath.Join(outputDir, "views"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return nil
}

// dumpTables writes table definitions to YAML files
func (d *Dumper) dumpTables(tables map[string]*chschema_v1.Table, opts DumpOptions) error {
	for tableName, table := range tables {
		// Filter by database if specified
		if opts.Database != "" && table.Database != nil && *table.Database != opts.Database {
			continue
		}

		// Write protobuf table directly to YAML
		filename := filepath.Join(opts.OutputDir, "tables", tableName+".yaml")
		if err := d.writeYAMLFile(filename, table, opts.Overwrite); err != nil {
			return fmt.Errorf("failed to write table %s: %w", tableName, err)
		}

		fmt.Printf("Dumped table: %s\n", tableName)
	}

	return nil
}

// dumpClusters writes cluster definitions to YAML files
func (d *Dumper) dumpClusters(clusters map[string]*chschema_v1.Cluster, opts DumpOptions) error {
	for clusterName, cluster := range clusters {
		// Write protobuf cluster directly to YAML
		filename := filepath.Join(opts.OutputDir, "clusters", clusterName+".yaml")
		if err := d.writeYAMLFile(filename, cluster, opts.Overwrite); err != nil {
			return fmt.Errorf("failed to write cluster %s: %w", clusterName, err)
		}

		fmt.Printf("Dumped cluster: %s\n", clusterName)
	}

	return nil
}

// writeYAMLFile writes protobuf data to a YAML file in a format compatible with the loader
func (d *Dumper) writeYAMLFile(filename string, data interface{}, overwrite bool) error {
	// Check if file exists and overwrite is false
	if !overwrite {
		if _, err := os.Stat(filename); err == nil {
			return fmt.Errorf("file %s already exists (use --overwrite to replace)", filename)
		}
	}

	// Convert protobuf to JSON first (to get proper field names), then to YAML
	var yamlData interface{}
	if protoMsg, ok := data.(proto.Message); ok {
		// Convert protobuf to JSON
		jsonBytes, err := protojson.Marshal(protoMsg)
		if err != nil {
			return fmt.Errorf("failed to marshal protobuf to JSON: %w", err)
		}

		// Convert JSON to Go object for YAML encoding
		if err := json.Unmarshal(jsonBytes, &yamlData); err != nil {
			return fmt.Errorf("failed to unmarshal JSON: %w", err)
		}
	} else {
		yamlData = data
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filename, err)
	}
	defer file.Close()

	encoder := yaml.NewEncoder(file)
	defer encoder.Close()

	if err := encoder.Encode(yamlData); err != nil {
		return fmt.Errorf("failed to encode YAML: %w", err)
	}

	return nil
}
