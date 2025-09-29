package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/posthog/chschema/internal/diff"
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
)

var rootCmd = &cobra.Command{
	Use:   "chschema",
	Short: "A declarative schema management tool for ClickHouse",
	Long: `chschema is a tool to manage ClickHouse schemas using a declarative
approach, inspired by Infrastructure-as-Code (IaC) principles.`,
	Run: func(cmd *cobra.Command, args []string) {
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
		printPlan(plan)

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
	},
}

func printPlan(plan *diff.Plan) {
	fmt.Println("\n--- Execution Plan ---")
	if len(plan.Actions) == 0 {
		fmt.Println("No changes detected. The schema is up-to-date.")
		return
	}

	for i, action := range plan.Actions {
		fmt.Printf("%d. [%s] %s\n", i+1, action.Type, action.Reason)
	}
	fmt.Println("----------------------")
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of chschema",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("chschema v0.1.0")
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
	rootCmd.PersistentFlags().StringVarP(&configDir, "config", "c", "schema", "Directory containing schema definition files")
	rootCmd.PersistentFlags().StringVar(&connection, "connect", "localhost:9000", "ClickHouse connection string")

	rootCmd.Flags().BoolVar(&autoApprove, "auto-approve", false, "Automatically approve and apply changes")
	rootCmd.Flags().BoolVar(&dryRun, "dry-run", true, "Show planned changes without applying them (default behavior)")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Whoops. There was an error while executing your CLI: %s", err)
		os.Exit(1)
	}
}
