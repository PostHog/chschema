# chschema Deficiency Analysis & QA Strategy

## Architectural Overview
The project is transitioning from a legacy YAML/Protobuf system (`chschema`) to a modern HCL-based tool (`hclexp`). Core phases include:
1. **Parsing & Resolution**: HCL -> Go structs with inheritance/patching support.
2. **Introspection**: Live cluster -> Internal state via ClickHouse SQL parser.
3. **Diffing**: Logical comparison of states.
4. **SQL Generation**: State changes -> ClickHouse DDL (identifies unsafe operations).

## Identified Deficiencies
- **Fail-Late Validation**: Semantic validation (types, engine settings) is deferred to ClickHouse server during DDL execution.
- **Limited Materialized View Support**: Only "TO-form" MVs are supported; "inner-engine" MVs are rejected.
- **Secret Handling**: Introspection redacts passwords as `[HIDDEN]`, breaking "dump and apply" workflows.
- **SQL Parser Maintenance**: Dependency on a private fork of `clickhouse-sql-parser` creates a high maintenance burden.
- **Unstructured Safety Gates**: "Unsafe" changes are flagged via text comments only; no structured machine-readable safety policies.
- **Statelessness vs. Drift**: Unmanaged settings/columns might be ignored rather than flagged as drift.

## Proposed Quality Assurance Strategy
- **Round-Trip Property-Based Testing**: Generate random schemas, apply, introspect, and verify consistency.
- **Structured Migration Plans**: Refactor `GenerateSQL` to return machine-readable plans (MetadataOnly, DataMutation, Blocking, Destructive).
- **Semantic Linter**: Pre-cluster validation of data types, `ORDER BY` expressions, and engine parameters.
- **Compatibility Matrix Testing**: Test against multiple ClickHouse versions (23.x - 25.x).
- **Drift Policy Enforcement**: Add policies (`Ignore`, `Strict`, `Prune`) for handling unmanaged attributes in live clusters.
