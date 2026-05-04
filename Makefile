# .PHONY: all build build-cli run test proto cluster-up cluster-down clean fmt

# # Build the main node binary
# build:
# 	go build -o bin/meridian ./cmd/meridian

# # Build the CLI binary
# build-cli:
# 	go build -o bin/meridian-cli ./cmd/meridian-cli

# # Run the node locally (single node, no cluster)
# run:
# 	go run ./cmd/meridian

# # Run all tests
# test:
# 	go test ./... -v -race

# # Generate Go code from all .proto file
# proto:
# 	protoc \
# 		--go_out=. \
# 		--go_opt=paths=source_relative \
# 		--go-grpc_out=. \
# 		--go-grpc_opt=paths=source_relative \
# 		$(shell find proto -name "*.proto")

# # Format all Go code
# fmt:
# 	gofmt -w .

# # Start a 3-node local cluster via docker-compose
# cluster-up:
# 	docker compose -f infra/docker-compose.yml up --build -d

# # Tear down the cluster
# cluster-down:
# 	docker compose -f infra/docker-compose.yml down -v

# # Stream logs from all 3 nodes
# cluster-logs:
# 	docker compose -f infra/docker-compose.yml logs -f

# # Clean built binaries
# clean:
# 	rm -rf bin/
