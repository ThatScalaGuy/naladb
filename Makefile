.PHONY: build build-cli build-sim test test-int lint bench proto docker docker-cli docker-push single cluster clean

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags="-X main.version=$(VERSION)"

build: ## Build the NalaDB server binary
	go build $(LDFLAGS) -o bin/naladb ./cmd/naladb

build-cli: ## Build the NalaDB CLI binary
	go build $(LDFLAGS) -o bin/naladb-cli ./cmd/naladb-cli

build-sim: ## Build the production line simulator (source: _internal/simulator/)
	go build -o bin/naladb-sim ./cmd/naladb-sim

test: ## Run all tests with race detection
	go test -race -count=1 ./...

test-int: ## Run integration tests
	go test -race -tags=integration ./tests/integration/...

lint: ## Run golangci-lint
	golangci-lint run ./...

bench: ## Run benchmarks
	go test -bench=. -benchmem ./benchmarks/...

proto: ## Generate Go stubs from protobuf definitions
	@mkdir -p api/gen
	protoc --proto_path=api/proto \
		--go_out=api/gen --go_opt=paths=source_relative \
		--go-grpc_out=api/gen --go-grpc_opt=paths=source_relative \
		api/proto/naladb/v1/*.proto

docker: ## Build Docker image (server)
	docker build --build-arg VERSION=$(VERSION) -t naladb:latest -f docker/Dockerfile .

docker-cli: ## Build Docker image (CLI)
	docker build --build-arg VERSION=$(VERSION) -t naladb-cli:latest -f docker/Dockerfile.cli .

docker-push: docker docker-cli ## Build and push Docker images to Docker Hub
	docker tag naladb:latest thatscalaguy/naladb:latest
	docker tag naladb-cli:latest thatscalaguy/naladb-cli:latest
	docker push thatscalaguy/naladb:latest
	docker push thatscalaguy/naladb-cli:latest

single: docker ## Start a single-node NalaDB with Docker Compose
	docker compose -f docker/docker-compose.single.yml up

cluster: docker ## Start a 3-node cluster with Docker Compose
	docker compose -f docker/docker-compose.cluster.yml up

clean: ## Remove build artifacts
	rm -rf bin/ api/gen/

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
