# Integration Testing Framework

End-to-end testing of the YAML-to-SQL pipeline with snapshot-based regression testing.

## Core Function

```go
func AssertSQLDiff(t *testing.T, current, wanted *loader.DesiredState, snapshotPath string)
```

Compares database states, generates SQL, and validates against snapshots.

## Usage

```go
// Prepare states
current := loader.NewDesiredState()  // Empty database
wanted := loader.NewDesiredState()   // Database with events table
wanted.Tables["events"] = eventsTable

// Test SQL generation
AssertSQLDiff(t, current, wanted, "testdata/snapshots/events_create.sql")
```

## Test Scenarios

- **`TestIntegration_EventsTable_CreateSQL`** - Create single table
- **`TestIntegration_BothTables_CreateSQL`** - Create multiple tables
- **`TestIntegration_DropTable_SQL`** - Drop table

## Running Tests

```bash
# Run tests
go test ./test -v

# Update snapshots when SQL generation changes
go test ./test -update-snapshots
```

## Snapshots

Located in `testdata/snapshots/`:
- `events_create.sql` - Single table creation
- `both_tables_create.sql` - Multiple tables
- `events_drop.sql` - Table deletion

Snapshots contain expected SQL output and serve as regression protection.