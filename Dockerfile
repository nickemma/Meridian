# --- Build Stage ---
# We build the binary in a full Go environment
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Copy dependency files first — Docker caches this layer.
# If only source code changes, this layer is not rebuilt.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
RUN go build -o bin/meridian ./cmd/meridian

# --- Runtime Stage ---
# The final image has only the binary — no Go toolchain, no source.
# This keeps the image small and reduces attack surface.
FROM alpine:3.19

RUN apk add --no-cache iptables

WORKDIR /app

# Create the data directory the node will write its WAL to
RUN mkdir -p /var/lib/meridian

COPY --from=builder /app/bin/meridian .

ENTRYPOINT ["./meridian"]
