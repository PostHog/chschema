update-sql-parser:
    go mod edit -replace github.com/AfterShip/clickhouse-sql-parser=github.com/orian/clickhouse-sql-parser@refactor-visitor && go mod tidy

# Install the repo's pre-commit hook (gofmt + go vet). One-time per checkout.
setup-hooks:
    git config core.hooksPath .githooks
    @echo "pre-commit hook installed (.githooks/pre-commit)."

# Format all Go code in place (gofmt -s), matching what CI checks
fmt:
    gofmt -s -w .

# Run the same lint gate as CI: gofmt -s check + golangci-lint
lint:
    test -z "$(gofmt -s -l .)" || { echo "Not gofmt-formatted (run 'just fmt'):"; gofmt -s -l .; exit 1; }
    golangci-lint run

# Verify module deps and run go vet, matching CI's vet job
vet:
    go mod verify
    go vet ./...

# Build all packages and the hclexp binary, matching CI's build job
build:
    go build ./...
    go build -o hclexp ./cmd/hclexp

# Run unit tests with race + coverage, matching CI's test/coverage job
coverage:
    go test -race -coverprofile=coverage.out ./internal/... ./cmd/...
    go tool cover -func=coverage.out

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