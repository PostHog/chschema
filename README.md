# chschema - Declarative ClickHouse Schema Management

A declarative Infrastructure-as-Code (IaC) tool for managing ClickHouse schemas using Go and Protobuf. Inspired by Terraform and Kubernetes, `chschema` abandons traditional sequential migrations in favor of state reconciliation.

## Features

- **Declarative Configuration**: Define your schema in YAML files as the single source of truth
- **Bidirectional Operations**: Both apply schemas to databases AND extract schemas from databases
- **State Reconciliation**: Compares desired vs current state and generates execution plans
- **Dry-Run Mode**: Preview changes before applying them
- **GitOps Ready**: Version-controlled schemas with CI/CD integration
- **Cluster-Native**: Built for ClickHouse's distributed architecture with `ON CLUSTER` support

## Installation

### Build from Source

```bash
go install github.com/posthog/chschema@latest
```

## Quick Start

### 1. Extract Existing Schema

Start by dumping your current ClickHouse schema:

```bash
# Dump all tables to YAML files
./chschema dump --output-dir ./schema

# Or dump only tables (skip clusters/views)
./chschema dump --tables-only --output-dir ./schema
```

This creates a directory structure:
```
schema/
├── tables/              # Table definitions
│   ├── users.yaml
│   ├── events.yaml
│   └── ...
├── materialized_views/  # Materialized view definitions
│   ├── users_mv.yaml
│   └── ...
├── views/              # Regular view definitions
│   ├── active_users.yaml
│   └── ...
├── dictionaries/       # Dictionary definitions
│   ├── user_dict.yaml
│   └── ...
└── clusters/           # Cluster configurations (future)
    └── production.yaml
```

The dump shows statistics about what was exported:
```
--- Introspection Statistics ---

Dumped engines:
  ✓ ReplicatedReplacingMergeTree: 38
  ✓ Distributed: 51
  ✓ MaterializedView: 33
  ✓ Dictionary: 9
  ✓ Kafka: 27

Skipped engines:
  (none)
--------------------------------
```

### 2. Review and Modify Schema

Edit the generated YAML files to match your desired schema:

**Example: `schema/tables/users.yaml`**
```yaml
name: users
database: myapp
order_by: [user_id, created_at]
partition_by: toYYYYMM(created_at)
engine:
  merge_tree: {}
columns:
  - name: user_id
    type: UInt64
    comment: "Unique user identifier"
  - name: email
    type: String
  - name: metadata
    type: String
    codec: "CODEC(ZSTD(3))"
  - name: created_at
    type: DateTime
    defaultexpression: now()
```

### 3. Plan Changes

Preview what changes will be applied:

```bash
# Show execution plan
./chschema --dry-run

# Save plan to file
./chschema --dry-run --output plan.txt
```

### 4. Apply Changes

Apply the schema changes:

```bash
# Apply changes (requires confirmation)
./chschema --auto-approve
```

## Command Reference

### Main Commands

```bash
# Show execution plan (default behavior)
chschema [--dry-run]

# Apply changes automatically
chschema --auto-approve

# Export database schema to YAML
chschema dump [options]

# Show version
chschema version
```

### Global Flags

- `--config`, `-c`: Directory containing schema files (default: `schema`)
- `--connect`: ClickHouse connection string (default: `localhost:9000`)
- `--output`, `-o`: Write execution plan to file

### Dump Command

Extract database schema to YAML files:

```bash
# Basic usage
chschema dump

# Advanced options
chschema dump \
  --output-dir ./my-schema \
  --database myapp \
  --tables-only \
  --overwrite
```

**Dump Flags:**
- `--output-dir`, `-o`: Target directory (default: `./schema-dump`)
- `--database`, `-d`: Specific database to dump
- `--tables-only`: Only dump tables, skip clusters/views
- `--overwrite`: Overwrite existing files

## Configuration

### Connection

Specify ClickHouse connection:

```bash
# Default local connection
chschema --connect localhost:9000

# Remote connection
chschema --connect clickhouse.example.com:9000
```

### Schema Directory Structure

Organize your schema files:

```
schema/
├── tables/              # Table definitions
│   ├── users.yaml
│   └── events.yaml
├── materialized_views/  # Materialized view definitions
│   ├── users_mv.yaml
│   └── events_mv.yaml
├── views/              # Regular view definitions
│   └── active_users.yaml
├── dictionaries/       # Dictionary definitions
│   └── user_dict.yaml
└── clusters/           # Cluster configurations (future)
    └── production.yaml
```

## Supported Engines

### Table Engines
- ✅ MergeTree
- ✅ ReplicatedMergeTree
- ✅ ReplacingMergeTree
- ✅ ReplicatedReplacingMergeTree
- ✅ SummingMergeTree
- ✅ CollapsingMergeTree
- ✅ ReplicatedCollapsingMergeTree
- ✅ AggregatingMergeTree
- ✅ ReplicatedAggregatingMergeTree
- ✅ Distributed
- ✅ Log
- ✅ Kafka

### Views
- ✅ MaterializedView
- ✅ View

### Dictionaries
- ✅ Dictionary

## YAML Schema Format

### Table Definition

```yaml
name: table_name
database: database_name        # optional
order_by: [col1, col2]        # ORDER BY clause
partition_by: toYYYYMM(date)  # PARTITION BY clause
engine:
  merge_tree: {}               # or replicated_merge_tree, replacing_merge_tree, etc.
columns:
  - name: id
    type: UInt64
    comment: "Unique identifier"  # optional, column comment
  - name: name
    type: String
    defaultexpression: ''         # optional, DEFAULT expression
  - name: data
    type: String
    codec: "CODEC(ZSTD(3))"      # optional, compression codec
  - name: created_at
    type: DateTime
    defaultexpression: now()
```

### Materialized View Definition

```yaml
name: users_mv
database: myapp
destinationTable: users_aggregated  # optional, uses .inner if not specified
selectQuery: SELECT user_id, count() as cnt FROM users GROUP BY user_id
```

### View Definition

```yaml
name: active_users
database: myapp
selectQuery: SELECT user_id, email FROM users WHERE active = 1
```

### Dictionary Definition

```yaml
name: user_dict
database: myapp
primaryKey:
  - user_id
source:
  sourceType: clickhouse
  sourceConfig: "SELECT user_id, email FROM users"
layout:
  layoutType: flat
lifetime:
  minSeconds: 300
  maxSeconds: 360
attributes:
  - name: user_id
    type: UInt64
    isKey: true
  - name: email
    type: String
    isKey: false
```

### Kafka Table Definition

```yaml
name: kafka_events
database: myapp
columns:
  - name: event_id
    type: UUID
  - name: event_data
    type: String
  - name: timestamp
    type: DateTime
engine:
  kafka:
    broker_list:
      - "localhost:9092"
      - "broker2:9092"
    topic: "events"
    consumer_group: "consumer_group1"
    format: "JSONEachRow"
```

### Cluster Definition

```yaml
name: production
nodes:
  - host: clickhouse-1.example.com
    port: 9000
    shard: 1
    replica: 1
    database: myapp           # optional
  - host: clickhouse-2.example.com
    port: 9000
    shard: 1
    replica: 2
```

## Workflow Examples

### Development Workflow

1. **Extract Current Schema**:
   ```bash
   chschema dump --output-dir ./schema
   ```

2. **Make Changes**: Edit YAML files

3. **Preview Changes**:
   ```bash
   chschema --dry-run
   ```

4. **Apply Changes**:
   ```bash
   chschema --auto-approve
   ```

### CI/CD Integration

```yaml
# .github/workflows/schema.yml
name: Schema Management
on:
  pull_request:
    paths: ['schema/**']

jobs:
  plan:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Plan Schema Changes
        run: |
          ./chschema --dry-run --output plan.txt
          # Post plan as PR comment

  apply:
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v2
      - name: Apply Schema Changes
        run: ./chschema --auto-approve
```

### Migration from Existing Setup

1. **Dump Current Schema**:
   ```bash
   chschema dump --overwrite
   ```

2. **Review Generated Files**: Check that all tables are captured correctly

3. **Commit to Git**:
   ```bash
   git add schema/
   git commit -m "Initial schema dump"
   ```

4. **Test Round-Trip**: Ensure dumped schema can be applied cleanly

## Best Practices

### Schema Organization

- **One file per table**: Keep table definitions in separate files
- **Consistent naming**: Use clear, descriptive names
- **Version control**: Always commit schema changes
- **Environment separation**: Use different directories for different environments

### Safety

- **Always dry-run first**: Review changes before applying
- **Backup before changes**: Take database backups before major changes
- **Test in staging**: Validate changes in non-production environments
- **Monitor deployments**: Watch for errors during schema application

### Team Workflow

- **Code reviews**: Review schema changes like code
- **Documentation**: Comment complex schema decisions
- **Breaking changes**: Plan and communicate schema breaking changes
- **Rollback plan**: Have a rollback strategy for schema changes

## Troubleshooting

### Common Issues

**Connection Failed**:
```bash
# Check ClickHouse is running
clickhouse client --query "SELECT 1"

# Verify connection string
chschema --connect your-host:9000 version
```

**Permission Denied**:
- Ensure user has DDL permissions
- Check cluster permissions for distributed operations

**Schema Conflicts**:
- Run `chschema --dry-run` to see conflicts
- Manually resolve differences in YAML files

### Getting Help

```bash
# Show help
chschema --help

# Show command-specific help
chschema dump --help
```

## Contributing

See [PROJECT.md](PROJECT.md) for development information and project structure.

## License

TBD