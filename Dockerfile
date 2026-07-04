# syntax=docker/dockerfile:1.7

# ---- Build stage ----
FROM golang:1.26-alpine AS build
WORKDIR /src

# Cache deps independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Version metadata injected by publish.yml; empty defaults let a bare
# `docker build .` degrade to the binary's "dev" report.
ARG VERSION
ARG COMMIT
ARG BUILD_TIME

# Static binary (no CGO) so it runs on distroless without a libc.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.buildTime=${BUILD_TIME}" \
    -o /out/hclexp ./cmd/hclexp

# ---- Ops runtime (shell-capable: sh + git + curl) ----
# Built only on demand with `docker build --target ops .`. Intended as a
# deployment-time schema-dump hook: a Kubernetes Job runs `hclexp introspect`
# per ClickHouse node and git-commits the output, so it needs a shell, git,
# and curl that the distroless image deliberately lacks.
FROM alpine:3.20.3 AS ops
RUN apk add --no-cache git curl ca-certificates \
    && addgroup -S hclexp \
    && adduser -S -G hclexp -h /home/hclexp -s /bin/sh hclexp
COPY --from=build /out/hclexp /usr/local/bin/hclexp
USER hclexp
ENTRYPOINT ["/usr/local/bin/hclexp"]

# ---- Runtime stage (distroless; default) ----
# Kept LAST on purpose: a plain `docker build .` (no --target) resolves to the
# final stage, so this remains the default image — minimal, no shell, just the
# static binary. Do not move the ops stage below this one.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/hclexp /hclexp
USER nonroot:nonroot
ENTRYPOINT ["/hclexp"]
