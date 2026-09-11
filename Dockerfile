# --- Storage Build Stage ---
FROM rust:1.87-alpine AS storage-builder

RUN apk add --no-cache build-base musl-dev
WORKDIR /app/storage-engine
COPY storage-engine/Cargo.toml storage-engine/Cargo.lock ./
COPY storage-engine/src ./src
RUN cargo build --release

# --- Go Build Stage ---
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache build-base musl-dev
WORKDIR /app

# Copy dependency files first — Docker caches this layer.
# If only source code changes, this layer is not rebuilt.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
COPY --from=storage-builder /app/storage-engine/target/release/libmeridian_storage.a storage-engine/target/release/libmeridian_storage.a
RUN CGO_ENABLED=1 go build -tags storageffi -o bin/meridian ./cmd/meridian

# --- Runtime Stage ---
# The final image has only the binary — no Go toolchain, no source.
# This keeps the image small and reduces attack surface.
FROM alpine:3.19

RUN apk add --no-cache iptables libgcc

WORKDIR /app

# Create the data directory the node will write its WAL to
RUN mkdir -p /var/lib/meridian

COPY --from=builder /app/bin/meridian .

ENTRYPOINT ["./meridian"]
