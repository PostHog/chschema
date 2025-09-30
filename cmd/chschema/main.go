package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/posthog/chschema/internal/diff"
	"github.com/posthog/chschema/internal/dumper"
	"github.com/posthog/chschema/internal/executor"
	"github.com/posthog/chschema/internal/introspection"
	"github.com/posthog/chschema/internal/loader"
	"github.com/spf13/cobra"
)

var (
	configDir   string
	connection  string
	autoApprove bool
	dryRun      bool
	outputFile  string

	// Dump command flags
	dumpOutputDir  string
	dumpDatabase   string
	dumpTablesOnly bool
	dumpOverwrite  bool
)

func migrateCmdFunc(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	// 1. Load the desired schema from YAML files
	fmt.Printf("Using config directory: %s\n", configDir)
	schemaLoader := loader.NewSchemaLoader(configDir)
	desiredState, err := schemaLoader.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading schema: %s\n", err)
		os.Exit(1)
	}

	// 2. Establish connection to ClickHouse
	fmt.Printf("Connecting to %s...\n", connection)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{connection},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to ClickHouse: %s\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	// 3. Introspect the current state from the live cluster
	introspector := introspection.NewIntrospector(conn)
	currentState, err := introspector.GetCurrentState(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error introspecting schema: %s\n", err)
		os.Exit(1)
	}

	// 4. Compare the states and generate a plan
	differ := diff.NewDiffer()
	plan, err := differ.Plan(desiredState, currentState)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating execution plan: %s\n", err)
		os.Exit(1)
	}

	// 5. Display the plan
	if err := printPlan(plan, outputFile); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing plan: %s\n", err)
		os.Exit(1)
	}

	// 6. Execute the plan if approved
	if autoApprove {
		fmt.Println("\nAuto-approving and applying changes...")
		exec := executor.NewExecutor(conn)
		if err := exec.Execute(ctx, plan); err != nil {
			fmt.Fprintf(os.Stderr, "Error applying schema changes: %s\n", err)
			os.Exit(1)
		}
	} else if len(plan.Actions) > 0 {
		fmt.Println("\nRun with --auto-approve to apply these changes.")
	}
}

var rootCmd = &cobra.Command{
	Use:   "chschema",
	Short: "A declarative schema management tool for ClickHouse",
	Long: `chschema is a tool to manage ClickHouse schemas using a declarative
approach, inspired by Infrastructure-as-Code (IaC) principles.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("chschema is a tool to manage ClickHouse schemas using a declarative approach, inspired by Infrastructure-as-Code (IaC) principles.")
	},
}

func printPlan(plan *diff.Plan, outputFile string) error {
	var writer io.Writer = os.Stdout

	if outputFile != "" {
		file, err := os.Create(outputFile)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		defer file.Close()
		writer = file
		fmt.Printf("Writing execution plan to %s\n", outputFile)
	}

	fmt.Fprintln(writer, "\n--- Execution Plan ---")
	if len(plan.Actions) == 0 {
		fmt.Fprintln(writer, "No changes detected. The schema is up-to-date.")
		return nil
	}

	for i, action := range plan.Actions {
		fmt.Fprintf(writer, "%d. [%s] %s\n", i+1, action.Type, action.Reason)
	}
	fmt.Fprintln(writer, "----------------------")

	if outputFile != "" {
		fmt.Printf("Execution plan written to %s\n", outputFile)
	}

	return nil
}

var (
	versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Print the version number of chschema",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("chschema v0.1.0")
		},
	}
	migrateCmd = &cobra.Command{
		Use:   "migrate",
		Short: "Migrates the database schema to match the desired state",
		Run:   migrateCmdFunc,
	}
)

func dumpCmdFunc(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	// Establish connection to ClickHouse
	fmt.Printf("Connecting to %s...\n", connection)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{connection},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to ClickHouse: %s\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	// Create dumper and configure options
	d := dumper.NewDumper(conn)
	opts := dumper.DumpOptions{
		OutputDir:  dumpOutputDir,
		Database:   dumpDatabase,
		TablesOnly: dumpTablesOnly,
		Overwrite:  dumpOverwrite,
	}

	// Perform the dump
	if err := d.Dump(ctx, opts); err != nil {
		fmt.Fprintf(os.Stderr, "Error dumping schema: %s\n", err)
		os.Exit(1)
	}
}

var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Export database schema to YAML files",
	Long: `Export the current ClickHouse database schema to YAML files that can be
used with chschema for declarative schema management.`,
	Run: dumpCmdFunc,
}

func init() {
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(dumpCmd)

	rootCmd.PersistentFlags().StringVarP(&configDir, "config", "c", "schema", "Directory containing schema definition files")
	rootCmd.PersistentFlags().StringVar(&connection, "connect", "localhost:9000", "ClickHouse connection string")

	migrateCmd.Flags().BoolVar(&autoApprove, "auto-approve", false, "Automatically approve and apply changes")
	migrateCmd.Flags().BoolVar(&dryRun, "dry-run", true, "Show planned changes without applying them (default behavior)")
	migrateCmd.Flags().StringVarP(&outputFile, "output", "o", "", "Write execution plan to file instead of stdout")

	// Dump command flags
	dumpCmd.Flags().StringVarP(&dumpOutputDir, "output-dir", "o", "./schema-dump", "Target directory for YAML files")
	dumpCmd.Flags().StringVarP(&dumpDatabase, "database", "d", "", "Specific database to dump (default: all non-system databases)")
	dumpCmd.Flags().BoolVar(&dumpTablesOnly, "tables-only", false, "Only dump table definitions, skip clusters/views")
	dumpCmd.Flags().BoolVar(&dumpOverwrite, "overwrite", false, "Overwrite existing files without prompting")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Whoops. There was an error while executing your CLI: %s", err)
		os.Exit(1)
	}
}
