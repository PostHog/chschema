#!/usr/bin/env sh

docker run --rm -v "$(pwd):/app" proto-builder:latest \
  protoc --proto_path=/app/ \
         --go_out=/app/gen --go_opt=module=github.com/posthog/chschema/gen \
         --go-grpc_out=/app/gen --go-grpc_opt=paths=source_relative \
         /app/proto/cluster.proto \
         /app/proto/column.proto \
         /app/proto/dictionary.proto \
         /app/proto/engine.proto \
         /app/proto/index.proto \
         /app/proto/schema_state.proto \
         /app/proto/table.proto \
         /app/proto/view.proto
