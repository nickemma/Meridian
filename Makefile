.PHONY: all build build-storageffi build-load build-openload build-check build-staleness run test storage-build test-storage-ffi test-strong-integration check-history proto cluster-up cluster-down baselines-up baselines-down clean fmt

# Build the main node binary
build:
	go build -o bin/meridian ./cmd/meridian

# Build a runnable node linked to the Rust LSM state machine.
build-storageffi: storage-build
	go build -tags storageffi -o bin/meridian ./cmd/meridian

# Build the development workload driver and bounded history checker.
build-load:
	go build -o bin/meridian-load ./cmd/meridian-load

# Build the open-loop driver used for latency-under-offered-load trials.
build-openload:
	go build -o bin/meridian-openload ./cmd/meridian-openload

build-check:
	go build -o bin/meridian-check ./cmd/meridian-check

# Build the conservative client-observable staleness analyzer.
build-staleness:
	go build -o bin/meridian-staleness ./cmd/meridian-staleness

# Verify a small successful strong-operation history emitted by meridian-load.
# Usage: make check-history HISTORY=results/trial-1.jsonl
check-history:
	@test -n "$(HISTORY)" || (echo "HISTORY is required" >&2; exit 2)
	go run ./cmd/meridian-check -history "$(HISTORY)"

# Run the node locally (single node, no cluster)
run:
	go run ./cmd/meridian

# Run all tests
test:
	go test ./... -v -race

# Build the Rust static library consumed by the optional Go storage adapter.
storage-build:
	cargo build --manifest-path storage-engine/Cargo.toml --release

# Run the real Go-to-Rust storage integration tests.
test-storage-ffi: storage-build
	go test -tags storageffi ./internal/storage -v -race

# Exercise leader election, client RPCs, Raft replication, and Rust state
# machine application in one live three-node process test.
test-strong-integration: storage-build
	go test -tags storageffi ./internal/server -v -race -run TestStrongKVOverLiveThreeNodeCluster

# Generate Go code from all .proto file
proto:
	protoc \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		$(shell find proto -name "*.proto")

# Start a 3-node local cluster via docker-compose
cluster-up:
	docker compose -f infra/docker-compose.yml up --build --force-recreate -d

# Tear down the cluster
cluster-down:
	docker compose -f infra/docker-compose.yml down -v

baselines-up:
	docker compose -f infra/baselines-compose.yml up -d

baselines-down:
	docker compose -f infra/baselines-compose.yml down -v

# Stream logs from all 3 nodes
cluster-logs:
	docker compose -f infra/docker-compose.yml logs -f

# Run the full chaos suite against the live cluster
chaos-run:
	make cluster-up
	sleep 5
	cd chaos && source venv/bin/activate && python3 run_chaos.py
	make cluster-down

# Clean built binaries
clean:
	rm -rf bin/
