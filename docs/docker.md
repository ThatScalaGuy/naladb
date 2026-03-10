# Running NalaDB in Docker

> How to configure and run NalaDB in containers.

## Quick Start

```bash
docker run -d \
  --name naladb \
  -p 7301:7301 \
  -p 9090:9090 \
  -v naladb-data:/data \
  thatscalaguy/naladb:latest \
  --wal-dir=/data/wal \
  --segment-dir=/data/segments
```

## Configuration Methods

NalaDB supports two ways to configure the server. Both can be combined — CLI flags always take precedence over the config file.

| Method | Best for |
|--------|----------|
| **CLI flags** | Simple setups, overriding a single value |
| **Config file** (`--config`) | Complex setups, version-controlled configuration |

### Method 1: CLI Flags

Pass flags directly as the container command:

```bash
docker run -d \
  --name naladb \
  -p 7301:7301 \
  -v naladb-data:/data \
  thatscalaguy/naladb:latest \
  --addr=:7301 \
  --metrics-addr=:9090 \
  --node-id=0 \
  --wal-dir=/data/wal \
  --segment-dir=/data/segments
```

Or in `docker-compose.yml`:

```yaml
services:
  naladb:
    image: thatscalaguy/naladb:latest
    command:
      - --addr=:7301
      - --metrics-addr=:9090
      - --node-id=0
      - --wal-dir=/data/wal
      - --segment-dir=/data/segments
    ports:
      - "7301:7301"
      - "9090:9090"
    volumes:
      - naladb-data:/data
```

### Method 2: Config File (Recommended for Production)

Create a `naladb.yml` (see `docker/naladb.example.yml` for a template) and mount it into the container:

```yaml
# naladb.yml
cluster:
  listen_addr: ":7301"
hlc:
  node_id: 0
  max_clock_skew: "1s"
storage:
  wal_dir: "/data/wal"
  segment_dir: "/data/segments"
metrics:
  addr: ":9090"
```

```bash
docker run -d \
  --name naladb \
  -p 7301:7301 \
  -p 9090:9090 \
  -v naladb-data:/data \
  -v ./naladb.yml:/etc/naladb/naladb.yml:ro \
  thatscalaguy/naladb:latest \
  --config=/etc/naladb/naladb.yml
```

In `docker-compose.yml`:

```yaml
services:
  naladb:
    image: thatscalaguy/naladb:latest
    command: ["--config", "/etc/naladb/naladb.yml"]
    ports:
      - "7301:7301"
      - "9090:9090"
    volumes:
      - naladb-data:/data
      - ./naladb.yml:/etc/naladb/naladb.yml:ro
```

### Mixing Both Methods

CLI flags override config file values. This is useful for keeping a shared base config and overriding per-node settings:

```yaml
services:
  naladb-0:
    image: thatscalaguy/naladb:latest
    command:
      - --config=/etc/naladb/naladb.yml
      - --node-id=0                        # overrides hlc.node_id from file
      - --raft-node-id=node-0              # overrides raft.node_id from file
    volumes:
      - naladb-data:/data
      - ./naladb.yml:/etc/naladb/naladb.yml:ro
```

## Data Persistence

The NalaDB image defines a `/data` volume. Always mount a named volume or host directory to persist data across container restarts:

```yaml
volumes:
  - naladb-data:/data       # named volume (recommended)
  # - ./data:/data           # host bind mount (alternative)
```

All data paths in the config should be under `/data`:

| Setting | Recommended path |
|---------|-----------------|
| `storage.wal_dir` | `/data/wal` |
| `storage.segment_dir` | `/data/segments` |
| `raft.data_dir` | `/data/raft` |

## Exposed Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 7301 | gRPC | Client API |
| 7400 | TCP | RAFT transport (cluster mode only) |
| 9090 | HTTP | Prometheus metrics (`/metrics`) |

## RAFT Cluster Setup

For a 3-node cluster, create a shared config file with all peer addresses and override the per-node values via CLI flags:

**`naladb-cluster.yml`** (shared by all nodes):

```yaml
cluster:
  listen_addr: ":7301"
storage:
  wal_dir: "/data/wal"
  segment_dir: "/data/segments"
metrics:
  addr: ":9090"
raft:
  enabled: true
  data_dir: "/data/raft"
  bind_addr: "0.0.0.0:7400"
  bootstrap: true
  peers:
    - id: "node-0"
      address: "naladb-0:7400"
    - id: "node-1"
      address: "naladb-1:7400"
    - id: "node-2"
      address: "naladb-2:7400"
  grpc_peers:
    - id: "node-0"
      address: "naladb-0:7301"
    - id: "node-1"
      address: "naladb-1:7301"
    - id: "node-2"
      address: "naladb-2:7301"
```

**`docker-compose.yml`**:

```yaml
x-naladb: &naladb-common
  image: thatscalaguy/naladb:latest
  restart: unless-stopped
  networks: [naladb-net]
  volumes:
    - ./naladb-cluster.yml:/etc/naladb/naladb.yml:ro

services:
  naladb-0:
    <<: *naladb-common
    command:
      - --config=/etc/naladb/naladb.yml
      - --node-id=0
      - --raft-node-id=node-0
      - --raft-advertise=naladb-0:7400
    ports: ["7301:7301", "9090:9090"]
    volumes:
      - naladb-0-data:/data
      - ./naladb-cluster.yml:/etc/naladb/naladb.yml:ro

  naladb-1:
    <<: *naladb-common
    command:
      - --config=/etc/naladb/naladb.yml
      - --node-id=1
      - --raft-node-id=node-1
      - --raft-advertise=naladb-1:7400
    ports: ["7302:7301", "9091:9090"]
    volumes:
      - naladb-1-data:/data
      - ./naladb-cluster.yml:/etc/naladb/naladb.yml:ro

  naladb-2:
    <<: *naladb-common
    command:
      - --config=/etc/naladb/naladb.yml
      - --node-id=2
      - --raft-node-id=node-2
      - --raft-advertise=naladb-2:7400
    ports: ["7303:7301", "9092:9090"]
    volumes:
      - naladb-2-data:/data
      - ./naladb-cluster.yml:/etc/naladb/naladb.yml:ro
```

This pattern keeps all shared configuration (peers, ports, storage paths) in a single file and only varies the per-node identity via flags.

## Health Checks

The Prometheus metrics endpoint doubles as a health check:

```yaml
healthcheck:
  test: ["CMD", "wget", "--spider", "-q", "http://localhost:9090/metrics"]
  interval: 10s
  timeout: 3s
  retries: 3
```

## Pre-built Compose Files

The `docker/` directory includes ready-to-use setups:

| File | Description |
|------|-------------|
| `docker-compose.single.yml` | Single node + Prometheus + Grafana |
| `docker-compose.cluster.yml` | 3-node RAFT cluster + Prometheus + Grafana |
| `naladb.example.yml` | Example config file template |

```bash
# Single node with monitoring
docker compose -f docker/docker-compose.single.yml up -d

# 3-node cluster with monitoring
docker compose -f docker/docker-compose.cluster.yml up -d
```

## Reference: All CLI Flags

| Flag | Default | Config file key |
|------|---------|-----------------|
| `--config` | — | *(N/A — specifies the file itself)* |
| `--addr` | `:7301` | `cluster.listen_addr` |
| `--metrics-addr` | `:9090` | `metrics.addr` |
| `--node-id` | `0` | `hlc.node_id` |
| `--max-clock-skew` | `1s` | `hlc.max_clock_skew` |
| `--wal-dir` | `data/wal` | `storage.wal_dir` |
| `--segment-dir` | `data/segments` | `storage.segment_dir` |
| `--raft` | `false` | `raft.enabled` |
| `--raft-node-id` | `""` | `raft.node_id` |
| `--raft-addr` | `:7400` | `raft.bind_addr` |
| `--raft-advertise` | `""` | `raft.advertise_addr` |
| `--raft-dir` | `data/raft` | `raft.data_dir` |
| `--raft-bootstrap` | `false` | `raft.bootstrap` |
| `--raft-peers` | `""` | `raft.peers` |
| `--grpc-peers` | `""` | `raft.grpc_peers` |
