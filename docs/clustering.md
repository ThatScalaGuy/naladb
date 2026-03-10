# NalaDB Cluster Operations

> This document describes how to set up and operate a NalaDB cluster using RAFT consensus.

## Overview

NalaDB uses the RAFT consensus protocol (via [hashicorp/raft](https://github.com/hashicorp/raft)) for cluster replication. A minimum of 3 nodes is recommended for production deployments to tolerate a single node failure.

### Key Properties

- **Consistency:** Linearizable writes through RAFT leader. Eventual or linearizable reads.
- **RAFT Log = WAL:** The RAFT log serves as the Write-Ahead Log (no double-write). When a command is committed by RAFT, the FSM applies it directly to the in-memory store.
- **Fault Tolerance:** A 3-node cluster tolerates 1 node failure. A 5-node cluster tolerates 2 failures.
- **Snapshots:** Periodic snapshots capture the full store state (version log, index, graph data) for efficient new-node catchup.

## Architecture

```
Client
  │
  ▼
┌─────────────────────────────┐
│  Leader Node                │
│  ┌───────┐   ┌───────────┐ │
│  │ gRPC  │──▶│  Cluster  │ │
│  │ Server│   │  .Set()   │ │
│  └───────┘   └─────┬─────┘ │
│                    │       │
│              ┌─────▼─────┐ │
│              │ RAFT      │ │
│              │ Consensus │ │
│              └──┬──┬──┬──┘ │
│                 │  │  │    │
│              ┌──▼──┘  │    │
│              │ FSM    │    │
│              │ Apply  │    │
│              └──┬─────┘    │
│              ┌──▼────────┐ │
│              │  Store    │ │
│              │ (in-mem)  │ │
│              └───────────┘ │
└─────────────────────────────┘
          │         │
     Replicate  Replicate
          │         │
          ▼         ▼
    ┌──────────┐  ┌──────────┐
    │ Follower │  │ Follower │
    │  Node 2  │  │  Node 3  │
    └──────────┘  └──────────┘
```

## Setup

### Single-Node (Development)

Start a single-node cluster for development and testing:

```bash
./bin/naladb \
  --addr :7301 \
  --node-id 0 \
  --raft-dir data/raft \
  --raft-bind :7400 \
  --bootstrap
```

The `--bootstrap` flag initializes this node as the sole member of a new cluster. It immediately becomes the leader.

### 3-Node Cluster

#### Node 1 (Bootstrap)

```bash
./bin/naladb \
  --addr :7301 \
  --node-id 0 \
  --raft-dir data/raft-0 \
  --raft-bind :7400 \
  --bootstrap \
  --raft-peers "node-1=host2:7400,node-2=host3:7400"
```

#### Node 2

```bash
./bin/naladb \
  --addr :7302 \
  --node-id 1 \
  --raft-dir data/raft-1 \
  --raft-bind :7400
```

#### Node 3

```bash
./bin/naladb \
  --addr :7303 \
  --node-id 2 \
  --raft-dir data/raft-2 \
  --raft-bind :7400
```

### Configuration (YAML)

NalaDB cluster nodes can also be configured via YAML:

```yaml
node:
  id: "node-0"
  addr: ":7301"

hlc:
  node_id: 0

raft:
  enabled: true
  data_dir: "data/raft"
  bind_addr: ":7400"
  bootstrap: true
  snapshot_retain: 2
  snapshot_threshold: 8192
  apply_timeout: 5s
  peers:
    - id: "node-1"
      address: "host2:7400"
    - id: "node-2"
      address: "host3:7400"
```

## Leader Election

RAFT uses randomized election timeouts to elect a leader. When the cluster starts:

1. All nodes begin as **Followers**.
2. After an election timeout (150ms default), a node becomes a **Candidate** and requests votes.
3. If it receives a majority of votes, it becomes the **Leader**.
4. The leader sends periodic heartbeats to maintain authority.

### Failover

If the leader fails:

1. Followers detect the absence of heartbeats after the election timeout.
2. One follower becomes a candidate and starts a new election.
3. After receiving a majority vote, the new leader begins accepting writes.
4. All committed data survives the failover (guaranteed by RAFT's log replication).

Typical failover time: 150-500ms (configurable via election timeout).

## Consistency Levels

NalaDB supports three consistency levels for reads:

| Level | Behavior | Latency | Use Case |
|-------|----------|---------|----------|
| **EVENTUAL** | Read from any node (may be slightly stale) | Lowest (~0 extra) | Dashboards, monitoring, analytics |
| **BOUNDED_STALE** | Read locally if within `max_stale_ms` of leader, otherwise forward | Low (~0 if fresh) | Dashboard queries requiring bounded freshness |
| **LINEARIZABLE** | ReadIndex protocol: leader verifies quorum + barrier | ~1 RTT extra | Critical reads after writes, transactions |

Writes always go through the leader and are linearizable by default.

### Leader Routing

All write operations are automatically forwarded to the RAFT leader:

1. Client sends Set/Delete to any node.
2. If the node is not leader, the request is transparently forwarded via gRPC.
3. The response includes `x-naladb-leader` metadata header with the current leader endpoint.
4. No client-side retry or redirection required.

```
Client ──Set("k","v")──▶ Follower ──forward──▶ Leader
                              ◀── response ─────┘
                              ◀── response (+ x-naladb-leader header)
```

### ReadIndex Protocol

LINEARIZABLE reads use the ReadIndex optimization from the Raft paper:

1. **VerifyLeader** — Leader confirms it still holds a quorum via heartbeat responses.
2. **Barrier** — Leader issues a no-op log entry; once committed and applied, guarantees the FSM is fully caught up (`appliedIndex >= commitIndex`).
3. **Local Read** — Leader reads from the local state machine, which now reflects all committed writes.

This avoids routing every read through the Raft log while still providing linearizable guarantees.

### BOUNDED_STALE Reads

BOUNDED_STALE provides a middle ground between EVENTUAL and LINEARIZABLE:

- Controlled by the `max_stale_ms` parameter (default: 5000ms).
- The follower checks `time.Since(lastLeaderContact)` against `max_stale_ms`.
- If within bounds, the read is served locally (zero extra network hops).
- If stale beyond bounds, the request is forwarded to the leader.

```
Follower receives BOUNDED_STALE read:
  if lastContact < max_stale_ms → serve locally
  else → forward to leader
```

### Consistency Level Recommendations

| Use Case | Recommended Level | Why |
|----------|-------------------|-----|
| Monitoring dashboards | EVENTUAL | Minimal latency, slight staleness acceptable |
| Sensor data display | BOUNDED_STALE (5s) | Recent data required but not real-time |
| Read-after-write | LINEARIZABLE | Must see the write that was just performed |
| Financial transactions | LINEARIZABLE | Correctness is critical |
| Anomaly detection | BOUNDED_STALE (1s) | Near-real-time with low latency |

## Clock Synchronization

NalaDB uses a Hybrid Logical Clock (HLC) to timestamp every write. The HLC combines the node's physical wall-clock time with a logical counter, ensuring causal ordering even when clocks are not perfectly synchronized. However, there are limits to how much clock drift the system can safely absorb.

### Clock Skew Protection

When a node receives an HLC timestamp from another node (e.g., during RAFT replication), it checks whether the remote wall time is too far ahead of its own physical clock. If the difference exceeds `max_clock_skew`, the update is **rejected** to prevent a single misconfigured node from corrupting the cluster's time basis.

```
                                          max_clock_skew (default: 1s)
                                    ◄────────────────────────►
Local physical time ────────────────┤                         │
                                    │      Acceptable         │  Rejected
                                    │      remote range       │  (ErrClockSkew)
                                    ├─────────────────────────┤──────────────▶
```

**Default tolerance:** 1 second. This is deliberately generous — NTP-synchronized hosts typically drift by less than 10ms.

### Configuration

```bash
# Command-line flag
./bin/naladb --max-clock-skew 1s

# Disable the check (not recommended)
./bin/naladb --max-clock-skew 0
```

```yaml
# YAML configuration
hlc:
  max_clock_skew: "1s"   # accepts Go duration strings: 500ms, 1s, 2s, etc.
```

### Recommendations

| Environment | `max_clock_skew` | Notes |
|-------------|------------------|-------|
| Production (NTP) | `1s` (default) | Standard NTP keeps drift well under 100ms |
| Production (PTP) | `500ms` | Precision Time Protocol provides tighter sync |
| Cloud / multi-region | `2s` | Cross-region NTP may have higher variance |
| Development / testing | `0` (disabled) | Useful when running nodes without NTP |

### What Happens on Rejection

When a clock skew violation is detected:

1. The local HLC state is **not modified** — the cluster continues with its current timestamps.
2. The operation that triggered the update fails with an error.
3. The error is logged, allowing operators to identify and fix the misconfigured node.

**Resolution:** Ensure NTP (or chrony) is running on all cluster nodes and that the system clock is not manually set to the future.

### Timezone Independence

All HLC wall times are stored as **microseconds since the NalaDB epoch** (January 1, 2025 UTC), derived from `time.Now().UnixMicro()`. This is timezone-independent — a node in UTC+9 and a node in UTC-5 produce identical wall-time values (assuming synchronized clocks). No timezone configuration is needed.

## Snapshot & Recovery

### Automatic Snapshots

RAFT automatically takes snapshots after a configurable number of log entries (default: 8192). Snapshots capture:

- **Memory-Index:** Current value for every key (256 shards)
- **Version Log:** Complete version history for all keys
- **Graph-Index:** Node metadata, edge metadata, and adjacency lists (stored as KV entries)

### Snapshot Lifecycle

1. Snapshot is triggered (automatic or manual via `Cluster.Snapshot()`).
2. The FSM exports the store's version log (point-in-time copy).
3. Data is gob-encoded and persisted to the snapshot store.
4. Old log entries up to the snapshot index are compacted.

### New Node Catchup

When a new node joins the cluster:

1. The leader sends the latest snapshot to the new node.
2. The new node restores its store state from the snapshot.
3. Any log entries after the snapshot are replayed through the FSM.
4. The new node is now fully caught up and can serve reads.

## Membership Changes

### Adding a Node

```go
cluster.AddVoter("node-3", "host4:7400")
```

The leader handles membership changes through RAFT consensus, ensuring the configuration is safely replicated before taking effect.

### Removing a Node

```go
cluster.RemoveServer("node-2")
```

## Docker Deployment

### Prerequisites

- Docker 20.10+
- Docker Compose v2+

### Quick Start

A pre-configured 3-node cluster with monitoring can be started with:

```bash
make cluster
```

This runs `docker-compose -f docker/docker-compose.yml up -d`, which starts:

- **naladb-0, naladb-1, naladb-2** — 3 NalaDB nodes with RAFT consensus
- **prometheus** — Metrics collection (scrapes all 3 nodes)
- **grafana** — Dashboard visualization (auto-provisioned)

### Service Ports

| Service | Port | Description |
|---------|------|-------------|
| naladb-0 | 7301 (gRPC), 9090 (metrics) | Bootstrap node |
| naladb-1 | 7302 (gRPC), 9091 (metrics) | Follower |
| naladb-2 | 7303 (gRPC), 9092 (metrics) | Follower |
| Prometheus | 9093 | Prometheus UI |
| Grafana | 3000 | Dashboards (admin/naladb) |

### Volumes

Each NalaDB node persists data in a named Docker volume:

- `naladb-0-data`, `naladb-1-data`, `naladb-2-data` — WAL and RAFT data
- `prometheus-data` — Prometheus TSDB
- `grafana-data` — Grafana state

### Network

All services share the `naladb-net` bridge network. NalaDB nodes communicate via their container names (`naladb-0:7301`, etc.).

### Custom Configuration

Override node settings via the `command` section in `docker-compose.yml`:

```yaml
naladb-0:
  command:
    - --addr=:7301
    - --metrics-addr=:9090
    - --node-id=0
    - --wal-dir=/data/wal
```

### Building the Image

The Dockerfile uses a multi-stage build:

1. **Build stage** — `golang:1.23-alpine`: compiles the binary via `make build`
2. **Runtime stage** — `alpine:3.20`: minimal image with the binary

```bash
make docker   # builds naladb:latest
```

### Verifying the Cluster

After starting, verify the cluster is healthy:

```bash
# Check all containers are running
docker compose -f docker/docker-compose.yml ps

# Check Prometheus targets
curl http://localhost:9093/api/v1/targets

# Access Grafana dashboard
open http://localhost:3000  # admin / naladb
```

### Data Persistence

Data survives container restarts because each node uses a named volume. To fully reset:

```bash
docker compose -f docker/docker-compose.yml down -v
```

## Monitoring

### Cluster Status

The `ClusterStatus` struct provides:

- Current leader ID and address
- All nodes with their roles (Leader/Follower/Candidate)

### Health Checks

Each node exposes a gRPC health service for load balancer integration. The health check reports:

- `SERVING` when the node is operational
- `NOT_SERVING` during initialization or shutdown

## Troubleshooting

### No Leader Elected

- Ensure all nodes can reach each other on the RAFT bind address.
- Check that at least a majority of nodes are running (2 of 3, 3 of 5).
- Verify the bootstrap node was started with `--bootstrap` exactly once.

### Stale Reads on Follower

- This is expected with `EVENTUAL` consistency. Use `LINEARIZABLE` for strong reads.
- Replication lag is typically sub-millisecond in healthy clusters.

### Snapshot Failures

- Ensure sufficient disk space in `raft-dir`.
- Check file permissions on the snapshot directory.
- Monitor snapshot size — very large stores may need tuned `snapshot_threshold`.
