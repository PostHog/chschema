package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/posthog/chschema/config"
	"github.com/posthog/chschema/internal/diff"
	"github.com/posthog/chschema/internal/dumper"
	"github.com/posthog/chschema/internal/executor"
	"github.com/posthog/chschema/internal/introspection"
	"github.com/posthog/chschema/internal/loader"
	"github.com/posthog/chschema/internal/logger"
	"github.com/rs/zerolog/log"
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

	// Logger flags
	logLevel   string
	logFormat  string
	logFile    string
	logNoColor bool

	clickhouseConfig config.ClickHouseConfig
)

func migrateCmdFunc(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	// 1. Load the desired schema from YAML files
	log.Info().Str("config_dir", configDir).Msg("Loading schema configuration")
	schemaLoader := loader.NewSchemaLoader(configDir)
	desiredState, err := schemaLoader.Load()
	if err != nil {
		log.Error().Err(err).Str("config_dir", configDir).Msg("Failed to load schema")
		os.Exit(1)
	}

	// 2. Establish connection to ClickHouse
	cfg := clickhouseConfig
	log.Info().Str("host", cfg.Host).Int("port", cfg.Port).Str("database", cfg.Database).Msg("Connecting to ClickHouse")
	conn, err := config.NewConnection(cfg)
	if err != nil {
		log.Error().Err(err).Str("host", cfg.Host).Int("port", cfg.Port).Msg("Failed to connect to ClickHouse")
		os.Exit(1)
	}
	defer conn.Close()

	// 3. Introspect the current state from the live cluster
	log.Info().Msg("Introspecting current state")
	introspector := introspection.NewIntrospector(conn)
	currentState, err := introspector.GetCurrentState(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to introspect schema")
		os.Exit(1)
	}

	// 4. Compare the states and generate a plan
	log.Info().Msg("Comparing desired and current states")
	differ := diff.NewDiffer()
	plan, err := differ.Plan(desiredState, currentState)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create execution plan")
		os.Exit(1)
	}

	// 5. Display the plan
	if err := printPlan(plan, outputFile); err != nil {
		log.Error().Err(err).Msg("Failed to write plan")
		os.Exit(1)
	}

	// 6. Execute the plan if approved
	if autoApprove {
		log.Info().Msg("Auto-approving and applying changes")
		exec := executor.NewExecutor(conn)
		if err := exec.Execute(ctx, plan); err != nil {
			log.Error().Err(err).Msg("Failed to apply schema changes")
			os.Exit(1)
		}
	} else if len(plan.Actions) > 0 {
		log.Info().Msg("Run with --auto-approve to apply these changes")
	}
}

var rootCmd = &cobra.Command{
	Use:   "chschema",
	Short: "A declarative schema management tool for ClickHouse",
	Long: `chschema is a tool to manage ClickHouse schemas using a declarative
approach, inspired by Infrastructure-as-Code (IaC) principles.`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// Initialize logger after flags are parsed
		if err := logger.Init(logLevel, logFormat, logFile, logNoColor); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to initialize logger: %s\n", err)
			os.Exit(1)
		}
	},
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
		log.Info().Str("file", outputFile).Msg("Writing execution plan to file")
	}

	// Plan output goes to stdout (program output, not logging)
	fmt.Fprintln(writer, "\n--- Execution Plan ---")
	if len(plan.Actions) == 0 {
		fmt.Fprintln(writer, "No changes detected. The schema is up-to-date.")
		log.Info().Msg("No schema changes detected")
		return nil
	}

	for i, action := range plan.Actions {
		fmt.Fprintf(writer, "%d. [%s] %s\n", i+1, action.Type, action.Reason)
	}
	fmt.Fprintln(writer, "----------------------")

	log.Info().Int("action_count", len(plan.Actions)).Msg("Execution plan generated")

	if outputFile != "" {
		log.Info().Str("file", outputFile).Msg("Execution plan written successfully")
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
	cfg := clickhouseConfig
	log.Info().Str("host", cfg.Host).Int("port", cfg.Port).Str("database", cfg.Database).Msg("Connecting to ClickHouse")
	conn, err := config.NewConnection(cfg)
	if err != nil {
		log.Error().Err(err).Str("host", cfg.Host).Int("port", cfg.Port).Msg("Failed to connect to ClickHouse")
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

	log.Info().Str("output_dir", dumpOutputDir).Bool("tables_only", dumpTablesOnly).Msg("Starting schema dump")

	// Perform the dump
	if err := d.Dump(ctx, opts); err != nil {
		log.Error().Err(err).Msg("Failed to dump schema")
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

func validateCmdFunc(cmd *cobra.Command, args []string) {
	// Load the desired schema from YAML files
	log.Info().Str("config_dir", configDir).Msg("Loading schema configuration")
	schemaLoader := loader.NewSchemaLoader(configDir)
	desiredState, err := schemaLoader.Load()
	if err != nil {
		log.Error().Err(err).Str("config_dir", configDir).Msg("Failed to load schema")
		os.Exit(1)
	}

	// Display summary statistics
	fmt.Println("\n--- Schema Validation Summary ---")
	fmt.Printf("✓ Tables: %d\n", len(desiredState.Tables))
	fmt.Printf("✓ Materialized Views: %d\n", len(desiredState.MaterializedViews))
	fmt.Printf("✓ Views: %d\n", len(desiredState.Views))
	fmt.Printf("✓ Dictionaries: %d\n", len(desiredState.Dictionaries))
	fmt.Println("----------------------------------")
	fmt.Println("Schema loaded successfully!")

	log.Info().
		Int("tables", len(desiredState.Tables)).
		Int("materialized_views", len(desiredState.MaterializedViews)).
		Int("views", len(desiredState.Views)).
		Int("dictionaries", len(desiredState.Dictionaries)).
		Msg("Schema validation completed successfully")
}

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate schema YAML files without connecting to database",
	Long: `Load and validate schema definition files from the specified directory.
This command checks that all YAML files can be parsed correctly and reports
any errors. No database connection is required.`,
	Run: validateCmdFunc,
}

func init() {
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(dumpCmd)
	rootCmd.AddCommand(validateCmd)

	rootCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", true, "Show planned changes without applying them (default behavior)")
	rootCmd.PersistentFlags().StringVarP(&configDir, "config", "c", "schema", "Directory containing schema definition files")
	rootCmd.PersistentFlags().StringVar(&connection, "connect", "localhost:9000", "ClickHouse connection string")
	rootCmd.PersistentFlags().StringVar(&clickhouseConfig.Host, "host", "localhost", "Host to use for schema definition files")
	rootCmd.PersistentFlags().IntVar(&clickhouseConfig.Port, "port", 9000, "Port to use for schema definition files")
	rootCmd.PersistentFlags().StringVar(&clickhouseConfig.Database, "database", "default", "Database to use for schema definition files")
	rootCmd.PersistentFlags().StringVar(&clickhouseConfig.User, "user", "default", "")
	rootCmd.PersistentFlags().StringVar(&clickhouseConfig.Password, "password", "default", "")

	// Logger flags
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "auto", "Log format (console, json, auto)")
	rootCmd.PersistentFlags().StringVar(&logFile, "log-file", "", "Log file path (default: stderr)")
	rootCmd.PersistentFlags().BoolVar(&logNoColor, "log-no-color", false, "Disable color output in console format")

	migrateCmd.Flags().BoolVar(&autoApprove, "auto-approve", false, "Automatically approve and apply changes")
	migrateCmd.Flags().StringVarP(&outputFile, "output", "o", "", "Write execution plan to file instead of stdout")

	// Dump command flags
	dumpCmd.Flags().StringVarP(&dumpOutputDir, "output-dir", "o", "./schema-dump", "Target directory for YAML files")
	//dumpCmd.Flags().StringVarP(&dumpDatabase, "database", "d", "", "Specific database to dump (default: all non-system databases)")
	dumpCmd.Flags().BoolVar(&dumpTablesOnly, "tables-only", false, "Only dump table definitions, skip clusters/views")
	dumpCmd.Flags().BoolVar(&dumpOverwrite, "overwrite", false, "Overwrite existing files without prompting")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		// Logger may not be initialized yet if error happens during flag parsing
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}
