.PHONY: all build run test proto cluster-up cluster-down clean

# Build the main node binary
build:
	go build -o bin/meridian ./cmd/meridian

# Build the CLI binary
build-cli:
	go build -o bin/meridian-cli ./cmd/meridian-cli

# Run the node locally (single node, no cluster)
run:
	go run ./cmd/meridian

# Run all tests
test:
	go test ./... -v -race

# Generate Go code from all .proto files
# proto:
#	protoc \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		proto/*.proto

proto:
	protoc \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		$(shell find proto -name "*.proto")

# Start a 3-node local cluster via docker-compose
cluster-up:
	docker compose -f infra/docker-compose.yml up --build -d

# Tear down the cluster
cluster-down:
	docker compose -f infra/docker-compose.yml down -v

# Clean built binaries
clean:
	rm -rf bin/
