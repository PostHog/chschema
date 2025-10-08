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

	// 4. Dump materialized views
	if err := d.dumpMaterializedViews(currentState.MaterializedViews, opts); err != nil {
		return fmt.Errorf("failed to dump materialized views: %w", err)
	}

	// 5. Dump views
	if err := d.dumpViews(currentState.Views, opts); err != nil {
		return fmt.Errorf("failed to dump views: %w", err)
	}

	// 6. Dump dictionaries
	if err := d.dumpDictionaries(currentState.Dictionaries, opts); err != nil {
		return fmt.Errorf("failed to dump dictionaries: %w", err)
	}

	// 7. Dump clusters (if not tables-only)
	if !opts.TablesOnly {
		if err := d.dumpClusters(currentState.Clusters, opts); err != nil {
			return fmt.Errorf("failed to dump clusters: %w", err)
		}
	}

	// 8. Print statistics
	d.printStatistics(introspector)

	fmt.Printf("\nSchema dump completed successfully to %s\n", opts.OutputDir)
	return nil
}

// printStatistics prints introspection statistics
func (d *Dumper) printStatistics(introspector *introspection.Introspector) {
	fmt.Println("\n--- Introspection Statistics ---")

	// Print dumped engines
	if len(introspector.DumpedEngines) > 0 {
		fmt.Println("\nDumped engines:")
		for engine, count := range introspector.DumpedEngines {
			fmt.Printf("  ✓ %s: %d\n", engine, count)
		}
	}

	// Print skipped engines (those with count > 0)
	skippedCount := 0
	for _, count := range introspector.SkippedEngines {
		if count > 0 {
			skippedCount += count
		}
	}

	if skippedCount > 0 {
		fmt.Println("\nSkipped engines:")
		for engine, count := range introspector.SkippedEngines {
			if count > 0 {
				fmt.Printf("  ✗ %s: %d\n", engine, count)
			}
		}
	}

	fmt.Println("--------------------------------")
}

// createDirectoryStructure creates the necessary directories
func (d *Dumper) createDirectoryStructure(outputDir string) error {
	dirs := []string{
		filepath.Join(outputDir, "tables"),
		filepath.Join(outputDir, "clusters"),
		filepath.Join(outputDir, "views"),
		filepath.Join(outputDir, "materialized_views"),
		filepath.Join(outputDir, "dictionaries"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return nil
}

// dumpTables writes table definitions to YAML files
func (d *Dumper) dumpTables(tables []*chschema_v1.Table, opts DumpOptions) error {
	for _, table := range tables {
		// Filter by database if specified
		if opts.Database != "" && table.Database != nil && *table.Database != opts.Database {
			continue
		}

		// Write protobuf table directly to YAML
		filename := filepath.Join(opts.OutputDir, "tables", table.Name+".yaml")
		if err := WriteYAMLFile(filename, table, opts.Overwrite); err != nil {
			return fmt.Errorf("failed to write table %s: %w", table.Name, err)
		}

		fmt.Printf("Dumped table: %s\n", table.Name)
	}

	return nil
}

// dumpClusters writes cluster definitions to YAML files
func (d *Dumper) dumpClusters(clusters []*chschema_v1.Cluster, opts DumpOptions) error {
	for _, cluster := range clusters {
		// Write protobuf cluster directly to YAML
		filename := filepath.Join(opts.OutputDir, "clusters", cluster.Name+".yaml")
		if err := WriteYAMLFile(filename, cluster, opts.Overwrite); err != nil {
			return fmt.Errorf("failed to write cluster %s: %w", cluster.Name, err)
		}

		fmt.Printf("Dumped cluster: %s\n", cluster.Name)
	}

	return nil
}

// dumpMaterializedViews writes materialized view definitions to YAML files
func (d *Dumper) dumpMaterializedViews(views []*chschema_v1.MaterializedView, opts DumpOptions) error {
	for _, view := range views {
		// Filter by database if specified
		if opts.Database != "" && view.Database != nil && *view.Database != opts.Database {
			continue
		}

		// Write protobuf materialized view directly to YAML
		filename := filepath.Join(opts.OutputDir, "materialized_views", view.Name+".yaml")
		if err := WriteYAMLFile(filename, view, opts.Overwrite); err != nil {
			return fmt.Errorf("failed to write materialized view %s: %w", view.Name, err)
		}

		fmt.Printf("Dumped materialized view: %s\n", view.Name)
	}

	return nil
}

// dumpViews writes regular view definitions to YAML files
func (d *Dumper) dumpViews(views []*chschema_v1.View, opts DumpOptions) error {
	for _, view := range views {
		// Filter by database if specified
		if opts.Database != "" && view.Database != nil && *view.Database != opts.Database {
			continue
		}

		// Write protobuf view directly to YAML
		filename := filepath.Join(opts.OutputDir, "views", view.Name+".yaml")
		if err := WriteYAMLFile(filename, view, opts.Overwrite); err != nil {
			return fmt.Errorf("failed to write view %s: %w", view.Name, err)
		}

		fmt.Printf("Dumped view: %s\n", view.Name)
	}

	return nil
}

// dumpDictionaries writes dictionary definitions to YAML files
func (d *Dumper) dumpDictionaries(dictionaries []*chschema_v1.Dictionary, opts DumpOptions) error {
	for _, dict := range dictionaries {
		// Filter by database if specified
		if opts.Database != "" && dict.Database != nil && *dict.Database != opts.Database {
			continue
		}

		// Write protobuf dictionary directly to YAML
		filename := filepath.Join(opts.OutputDir, "dictionaries", dict.Name+".yaml")
		if err := WriteYAMLFile(filename, dict, opts.Overwrite); err != nil {
			return fmt.Errorf("failed to write dictionary %s: %w", dict.Name, err)
		}

		fmt.Printf("Dumped dictionary: %s\n", dict.Name)
	}

	return nil
}

// WriteYAMLFile writes protobuf data to a YAML file in a format compatible with the loader
func WriteYAMLFile(filename string, data interface{}, overwrite bool) error {
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
