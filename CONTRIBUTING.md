# Contributing to NalaDB

Thank you for your interest in contributing to NalaDB! This guide will help you get started.

## Development Setup

### Prerequisites

- Go 1.24+
- `protoc` (Protocol Buffers compiler) -- only needed if modifying `.proto` files
- `golangci-lint` -- for linting
- Docker -- for running cluster tests

### Building

```bash
git clone https://github.com/thatscalaguy/naladb.git
cd naladb
make build       # Build server binary
make build-cli   # Build CLI binary
```

### Running Tests

```bash
make test        # Unit tests with race detection
make test-int    # Integration tests (requires Docker)
make lint        # Linting
make bench       # Benchmarks
```

All tests must pass before submitting a pull request.

## How to Contribute

### Reporting Bugs

Open an issue on GitHub with:
- A clear description of the bug
- Steps to reproduce
- Expected vs actual behavior
- NalaDB version (`naladb -v`)
- OS and Go version

### Suggesting Features

Open an issue with the `enhancement` label. Describe the use case and why existing features don't cover it.

### Submitting Code

1. Fork the repository
2. Create a feature branch from `main`: `git checkout -b feat/my-feature`
3. Make your changes
4. Run `make test && make lint` -- both must pass
5. Commit using conventional commits (see below)
6. Push and open a pull request against `main`

### Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add TTL support for graph edges
fix: prevent WAL corruption on unclean shutdown
docs: update clustering guide with TLS example
test: add benchmark for causal traversal depth=10
refactor: simplify segment merge logic
```

Scope is optional but encouraged:

```
feat(query): add EXPLAIN statement to NalaQL
fix(raft): handle split-brain during network partition
```

## Project Structure

```
cmd/naladb/        Server entry point
cmd/naladb-cli/    CLI entry point
internal/          Core packages (not importable externally)
  hlc/             Hybrid Logical Clock
  wal/             Write-Ahead Log
  store/           Temporal KV store
  graph/           Graph operations
  query/           NalaQL lexer, parser, executor
  raft/            RAFT consensus
  grpc/            gRPC service implementations
  segment/         Cold storage segments
  ...
api/proto/         Protobuf definitions
api/gen/           Generated Go stubs (do not edit)
docker/            Docker and Docker Compose files
docs/              Documentation
examples/          Runnable use-case examples
benchmarks/        Benchmark suites
```

## Code Style

- Follow standard Go conventions (`gofmt`, `go vet`)
- The project uses `golangci-lint` with the config in `.golangci.yml`
- Keep functions focused and small
- Write table-driven tests where applicable
- Add benchmarks for performance-sensitive code

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
