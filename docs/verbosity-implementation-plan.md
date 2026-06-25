# Verbosity Implementation Plan

## Goal
Add verbosity level control to the application to enable query logging during ClickHouse introspection.

## Proposed Solutions

### Option 1: Simple Boolean Debug Flag
**Approach**: Add a single `--debug` or `--verbose` flag that enables query logging.

**Pros**:
- Simple to implement and use
- Clear on/off behavior
- Minimal code changes

**Cons**:
- Not granular - can't control different levels of verbosity
- All-or-nothing approach

**Implementation**:
```bash
chschema dump --debug  # Enables all debug output including SQL queries
```

---

### Option 2: Integer Verbosity Levels ⭐ **RECOMMENDED**
**Approach**: Add `--verbosity` or `-v` flag with numeric levels (0-3).

**Levels**:
- **0**: Silent (errors only)
- **1**: Normal (default - current behavior)
- **2**: Verbose (show queries)
- **3**: Debug (show queries + responses/details)

**Pros**:
- Fine-grained control
- Industry standard (similar to -v, -vv, -vvv pattern)
- Extensible for future logging needs

**Cons**:
- More complex to implement
- Users need to remember what each level means

**Implementation**:
```bash
chschema dump --verbosity=2    # or -v 2
```

---

### Option 3: Multiple -v Flags (Kubernetes/Git style)
**Approach**: Allow stacking `-v` flags for increased verbosity.

**Levels**:
- No flag: Normal
- `-v`: Verbose (show queries)
- `-vv`: Very verbose (queries + responses)
- `-vvv`: Debug (everything)

**Pros**:
- Intuitive UX (more v's = more verbose)
- Common pattern in CLI tools
- Easy to type

**Cons**:
- Slightly more complex flag parsing
- Can be unclear what each level does

**Implementation**:
```bash
chschema dump -v    # verbose
chschema dump -vv   # very verbose
chschema dump -vvv  # debug
```

---

### Option 4: Named Verbosity Levels
**Approach**: Use named levels similar to logging frameworks.

**Levels**:
```bash
--log-level=error|warn|info|debug|trace
```

**Pros**:
- Self-documenting
- Familiar to developers
- Clear semantics

**Cons**:
- More typing required
- Overkill for this use case

---

## Recommended Approach

**Option 2 (Integer Verbosity Levels)** is recommended for:

1. **Balance**: Provides granularity without being overly complex
2. **Extensibility**: Easy to add more levels later if needed
3. **Integration**: Works well with Go's flag package
4. **Clarity**: Numeric levels are straightforward and we can document them in `--help`

---

## Implementation Plan

### Phase 1: Add Verbosity Infrastructure
**Files to modify/create**:
- `cmd/chschema/main.go`
- `internal/utils/verbosity.go` (new file)

**Tasks**:
1. Add global `--verbosity` flag to root command in `cmd/chschema/main.go`
2. Create `internal/utils/verbosity.go` with verbosity constants:
   ```go
   package utils

   const (
       VerbositySilent  = 0  // Errors only
       VerbosityNormal  = 1  // Default behavior
       VerbosityVerbose = 2  // Show SQL queries
       VerbosityDebug   = 3  // Show queries + debug info
   )
   ```
3. Add helper function to check verbosity level

### Phase 2: Add Query Logging Helper
**Files to modify/create**:
- `config/clickhouse.go` or `internal/utils/query_logger.go` (new file)

**Tasks**:
1. Create query logger function:
   ```go
   func LogQuery(verbosity int, query string, args ...interface{}) {
       if verbosity >= VerbosityVerbose {
           fmt.Printf("[SQL] %s\n", cleanupQuery(query))
           if len(args) > 0 && verbosity >= VerbosityDebug {
               fmt.Printf("[ARGS] %v\n", args)
           }
       }
   }
   ```
2. Implement `cleanupQuery()` to format multi-line queries for readable output

### Phase 3: Integrate with Introspector
**Files to modify**:
- `internal/introspection/introspector.go`

**Tasks**:
1. Add `verbosity int` field to `Introspector` struct:
   ```go
   type Introspector struct {
       conn      clickhouse.Conn
       Databases []string
       Tables    []string
       verbosity int  // New field

       // Statistics
       DumpedEngines  map[string]int
       SkippedEngines map[string]int
   }
   ```
2. Update `NewIntrospector()` to accept verbosity parameter:
   ```go
   func NewIntrospector(conn clickhouse.Conn, verbosity int) *Introspector
   ```
3. Add query logging before each `conn.Query()` call:
   - `introspectAllTables()`
   - `introspectTables()`
   - `introspectColumns()`
   - `introspectMaterializedViews()`
   - `introspectViews()`
   - `introspectDictionaries()`

### Phase 4: Update CLI and Dumper
**Files to modify**:
- `cmd/chschema/main.go`
- `internal/dumper/dumper.go`

**Tasks**:
1. Add `--verbosity` flag to main command with default value 1
2. Add short flag `-v` as alias
3. Pass verbosity to dumper
4. Update dumper to pass verbosity to introspector
5. Update help text to document verbosity levels

### Phase 5: Testing & Documentation
**Files to modify**:
- `README.md`
- `CLAUDE.md`

**Tasks**:
1. Test with different verbosity levels (0, 1, 2, 3)
2. Verify query output formatting is readable
3. Add verbosity examples to README.md
4. Update CLAUDE.md with verbosity flag info
5. Add usage examples showing different verbosity levels

---

## Example Output

### Verbosity 0 (Silent)
```bash
$ chschema dump --verbosity=0

Schema dump completed successfully to ./schema-dump
```

### Verbosity 1 (Normal - Default)
```bash
$ chschema dump

Connecting to localhost:9000...
Dumped table: users
Dumped table: events
Dumped materialized view: users_mv
Dumped view: active_users
Dumped dictionary: user_dict

--- Introspection Statistics ---

Dumped engines:
  ✓ ReplicatedReplacingMergeTree: 38
  ✓ Dictionary: 9
--------------------------------

Schema dump completed successfully to ./schema-dump
```

### Verbosity 2 (Verbose - Show Queries)
```bash
$ chschema dump --verbosity=2

Connecting to localhost:9000...
[SQL] SELECT name, engine FROM system.tables WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') ORDER BY engine, name
[SQL] SELECT database, name, engine, engine_full, sorting_key, partition_key, primary_key, total_rows, total_bytes FROM system.tables WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') AND engine IN ('MergeTree', 'ReplicatedMergeTree', ...) ORDER BY database, name
Dumped table: users
[SQL] SELECT name, type, default_expression, compression_codec, comment FROM system.columns WHERE database = 'default' AND table = 'users'
Dumped table: events
[SQL] SELECT database, name, engine_full, as_select FROM system.tables WHERE engine = 'MaterializedView' AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') ORDER BY database, name
Dumped materialized view: users_mv
...

Schema dump completed successfully to ./schema-dump
```

### Verbosity 3 (Debug - Show Everything)
```bash
$ chschema dump --verbosity=3

Connecting to localhost:9000...
[DEBUG] Starting introspection for all tables
[SQL] SELECT name, engine FROM system.tables WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') ORDER BY engine, name
[DEBUG] Found 150 total objects across all engines
[DEBUG] Skipped engines initialized: {Dictionary: 9, Kafka: 27}
[DEBUG] Starting table introspection
[SQL] SELECT database, name, engine, engine_full, sorting_key, partition_key, primary_key, total_rows, total_bytes FROM system.tables WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') AND engine IN ('MergeTree', 'ReplicatedMergeTree', ...) ORDER BY database, name
[DEBUG] Processing table: users (database: default, engine: ReplicatedReplacingMergeTree)
Dumped table: users
[SQL] SELECT name, type, default_expression, compression_codec, comment FROM system.columns WHERE database = 'default' AND table = 'users'
[ARGS] [default users]
[DEBUG] Table 'users' has 5 columns
[DEBUG] Parsing engine: ReplicatedReplacingMergeTree('/clickhouse/tables/users', '{replica}', 'version')
...

Schema dump completed successfully to ./schema-dump
```

---

## Implementation Checklist

- [ ] Phase 1: Add verbosity infrastructure
  - [ ] Create `internal/utils/verbosity.go` with constants
  - [ ] Add `--verbosity` flag to `cmd/chschema/main.go`

- [ ] Phase 2: Add query logging helper
  - [ ] Create `LogQuery()` function
  - [ ] Create `cleanupQuery()` formatter

- [ ] Phase 3: Integrate with introspector
  - [ ] Add `verbosity` field to `Introspector` struct
  - [ ] Update `NewIntrospector()` signature
  - [ ] Add logging to `introspectAllTables()`
  - [ ] Add logging to `introspectTables()`
  - [ ] Add logging to `introspectColumns()`
  - [ ] Add logging to `introspectMaterializedViews()`
  - [ ] Add logging to `introspectViews()`
  - [ ] Add logging to `introspectDictionaries()`

- [ ] Phase 4: Update CLI and dumper
  - [ ] Add flag to main command
  - [ ] Pass verbosity to dumper
  - [ ] Update dumper to pass to introspector
  - [ ] Update help text

- [ ] Phase 5: Testing & documentation
  - [ ] Test verbosity level 0
  - [ ] Test verbosity level 1 (default)
  - [ ] Test verbosity level 2
  - [ ] Test verbosity level 3
  - [ ] Update README.md
  - [ ] Update CLAUDE.md

---

## Future Enhancements

- Add verbosity to other operations (diff, apply)
- Add timestamp to log messages
- Add color coding for different log levels
- Add option to log to file instead of stdout
- Add JSON output format for structured logging
