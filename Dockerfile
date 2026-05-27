# syntax=docker/dockerfile:1.7

# ---- Build stage ----
FROM golang:1.26-alpine AS build
WORKDIR /src

# Cache deps independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binary (no CGO) so it runs on distroless without a libc.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/hclexp ./cmd/hclexp

# ---- Runtime stage ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/hclexp /hclexp
USER nonroot:nonroot
ENTRYPOINT ["/hclexp"]
