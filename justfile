update-sql-parser:
    go mod edit -replace github.com/AfterShip/clickhouse-sql-parser=github.com/orian/clickhouse-sql-parser@refactor-visitor && go mod tidy

# Run unit and snapshot tests
test:
    go test ./internal/... -v
    go test ./test -v

# Run unit tests only
test-unit:
    go test ./internal/... -v

# Run snapshot/integration tests only
test-integration:
    go test ./test -v

# Run live ClickHouse integration tests (requires: docker compose up -d)
test-live:
    go test ./test -v -clickhouse

# Update SQL snapshot fixtures
test-update-snapshots:
    go test ./test -update-snapshots