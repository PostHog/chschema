# Zerolog Integration Plan

## Overview
This document outlines the plan for integrating zerolog into the chschema project to provide structured, high-performance logging throughout the application.

## Goals
1. Replace all `fmt.Print*` statements with structured logging
2. Provide configurable log levels (debug, info, warn, error)
3. Add contextual logging for operations (table names, databases, operations)
4. Maintain zero-allocation performance for production use
5. Support both human-readable (console) and JSON output formats

## Architecture

### Package Structure
```
internal/logger/
├── logger.go          # Core logger initialization and configuration
├── context.go         # Context-aware logging helpers
└── logger_test.go     # Tests for logger package
```

### Logger Configuration
- **Default Level**: Info (configurable via `--log-level` flag)
- **Default Format**: Console for TTY, JSON for non-TTY
- **Output**: stderr (to keep stdout clean for SQL output)
- **Timestamp Format**: RFC3339 for console, Unix for JSON

## Implementation Phases

### Phase 1: Core Setup
1. Add zerolog dependency
2. Create `internal/logger` package with initialization
3. Add command-line flags for log configuration
4. Setup global logger instance

### Phase 2: Command Line Integration
Replace logging in `cmd/chschema/main.go`:
- Connection status → Info level
- Command execution → Debug level
- Errors → Error level with stack traces
- Dry-run output → Keep as stdout (not logged)

### Phase 3: Package Integration

#### Introspection Package (`internal/introspection`)
```go
log.Info().
    Str("database", database).
    Str("table", tableName).
    Msg("Introspecting table")

log.Debug().
    Str("engine", engine).
    Str("create_table_query", query).
    Msg("Parsed table definition")
```

#### Diff Package (`internal/diff`)
```go
log.Info().
    Int("tables_to_create", len(creates)).
    Int("tables_to_alter", len(alters)).
    Int("tables_to_drop", len(drops)).
    Msg("Diff calculation complete")

log.Debug().
    Str("table", tableName).
    Interface("changes", changes).
    Msg("Table differences found")
```

#### Executor Package (`internal/executor`)
```go
log.Info().
    Str("operation", "CREATE TABLE").
    Str("table", tableName).
    Str("cluster", cluster).
    Msg("Executing DDL")

log.Error().
    Err(err).
    Str("sql", statement).
    Msg("DDL execution failed")
```

#### Loader Package (`internal/loader`)
```go
log.Debug().
    Str("file", filepath).
    Msg("Loading YAML configuration")

log.Warn().
    Str("file", filepath).
    Err(err).
    Msg("Failed to load configuration file")
```

#### Dumper Package (`internal/dumper`)
```go
log.Info().
    Str("database", database).
    Int("tables_dumped", count).
    Msg("Schema dump complete")

log.Debug().
    Str("engine", engine).
    Bool("supported", supported).
    Msg("Engine support check")
```

### Phase 4: Testing & Documentation
1. Update tests to handle structured logging
2. Add logging level tests
3. Document logging conventions
4. Add examples to README

## Log Levels Guidelines

### Debug
- SQL queries being executed
- Detailed configuration loading
- File I/O operations
- Parser internals

### Info
- Major operations (introspection, diff, apply)
- Connection establishment
- Summary statistics
- Command completion

### Warn
- Unsupported engines skipped
- Missing optional configuration
- Non-critical errors that don't stop execution

### Error
- Connection failures
- SQL execution errors
- Parse errors
- Fatal configuration problems

## Structured Fields Convention

### Common Fields
- `database`: Database name
- `table`: Table name
- `column`: Column name
- `engine`: Table engine type
- `cluster`: Cluster name
- `operation`: Operation type (CREATE, ALTER, DROP)
- `duration`: Operation duration
- `error`: Error message

### Operation-Specific Fields
- Introspection: `rows_examined`, `tables_found`
- Diff: `changes_count`, `change_type`
- Executor: `sql`, `affected_rows`
- Loader: `file_path`, `config_type`
- Dumper: `output_dir`, `files_written`

## Command Line Flags

```bash
# New flags to add
--log-level string     Log level (debug, info, warn, error) (default "info")
--log-format string    Log format (console, json) (default "auto")
--log-file string      Log file path (default stderr)
--log-no-color         Disable color output in console format
```

## Example Usage

```bash
# Debug mode with console output
chschema --log-level debug --log-format console dump

# Production mode with JSON logs
chschema --log-format json --log-file /var/log/chschema.log apply

# Quiet mode (errors only)
chschema --log-level error diff
```

## Code Examples

### Logger Initialization
```go
package logger

import (
    "os"
    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
)

var Logger zerolog.Logger

func Init(level string, format string, noColor bool) {
    // Set log level
    lvl, err := zerolog.ParseLevel(level)
    if err != nil {
        lvl = zerolog.InfoLevel
    }
    zerolog.SetGlobalLevel(lvl)

    // Configure output format
    if format == "console" || (format == "auto" && isTerminal()) {
        output := zerolog.ConsoleWriter{
            Out: os.Stderr,
            TimeFormat: "15:04:05",
            NoColor: noColor,
        }
        Logger = zerolog.New(output).With().Timestamp().Logger()
    } else {
        Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
    }

    // Set global logger
    log.Logger = Logger
}
```

### Usage in Packages
```go
package introspection

import "github.com/rs/zerolog/log"

func (i *Introspector) IntrospectTables(database string) error {
    log.Info().
        Str("database", database).
        Msg("Starting table introspection")

    defer func(start time.Time) {
        log.Debug().
            Str("database", database).
            Dur("duration", time.Since(start)).
            Msg("Introspection completed")
    }(time.Now())

    // ... introspection logic ...
}
```

## Migration Checklist

- [x] Add zerolog to go.mod
- [x] Create internal/logger package
- [x] Add CLI flags for logging configuration
- [x] Replace fmt.Printf in cmd/chschema/main.go
- [x] Add logging to introspection package
- [x] Add logging to diff package
- [x] Add logging to executor package
- [x] Add logging to loader package
- [x] Add logging to dumper package
- [x] Add logging to sqlgen package (SQL generation uses fmt.Sprintf for string formatting, which is appropriate)
- [x] Update config/clickhouse.go connection logging (no changes needed - connection logging handled in main.go)
- [x] Update test helpers to suppress logs in tests
- [x] Add logger tests
- [ ] Update README with logging documentation
- [x] Test all log levels and formats

## Performance Considerations

1. Use `Logger.With()` to create sub-loggers with common fields
2. Avoid logging in hot paths (use Debug level)
3. Use field methods instead of `Interface()` when possible
4. Disable debug logging in production builds
5. Consider sampling for high-frequency operations

## Testing Strategy

1. **Unit Tests**: Mock logger for testing log output
2. **Integration Tests**: Set log level to ERROR to reduce noise
3. **Benchmarks**: Measure logging overhead
4. **E2E Tests**: Verify log output in different scenarios

## Success Metrics

1. No performance regression (measured by benchmarks)
2. All fmt.Print* statements replaced
3. Consistent structured logging across packages
4. Improved debuggability with contextual information
5. Clean separation of program output and logs

## References
- [Zerolog Documentation](https://github.com/rs/zerolog)
- [Zerolog Best Practices](https://betterstack.com/community/guides/logging/zerolog/)
- [Go Logging Benchmarks](https://github.com/betterstack-community/go-logging-benchmarks)