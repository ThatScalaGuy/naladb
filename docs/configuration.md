# NalaDB Configuration Reference

> This document describes all configuration options for NalaDB.

## Configuration File

NalaDB uses a YAML configuration file. By default, it looks for `naladb.yml` in the current directory. All settings can also be set via command-line flags (flags take precedence over the config file).

## Full YAML Reference

```yaml
# ─── Cluster ────────────────────────────────────────────────────────────────
cluster:
  node_id: "node-0"              # Unique node identifier in the cluster
  listen_addr: ":7301"           # gRPC listen address (host:port)
  grpc_addr: "0.0.0.0:7301"     # Advertised gRPC address for peers
  data_dir: "/var/lib/naladb"    # Root directory for all persistent data

# ─── RAFT Consensus ─────────────────────────────────────────────────────────
raft:
  enabled: true                  # Enable RAFT clustering (false = single-node)
  data_dir: "/var/lib/naladb/raft"  # RAFT log and snapshot directory
  bind_addr: ":7400"             # RAFT transport bind address
  bootstrap: false               # Bootstrap as initial cluster leader (first node only)
  election_timeout: "150ms"      # Leader election timeout
  heartbeat_interval: "100ms"    # Leader heartbeat interval
  snapshot_retain: 2             # Number of snapshots to retain
  snapshot_threshold: 8192       # Log entries before triggering a snapshot
  apply_timeout: "5s"            # Timeout for RAFT Apply operations
  peers:                         # Initial cluster peers
    - id: "node-1"
      address: "host2:7400"
    - id: "node-2"
      address: "host3:7400"

# ─── HLC ────────────────────────────────────────────────────────────────────
hlc:
  node_id: 0                     # HLC node ID (0-15), must be unique per node
  max_clock_skew: "1s"           # Max tolerated clock skew between nodes (0 = disabled)

# ─── Storage ────────────────────────────────────────────────────────────────
storage:
  wal_dir: "/var/lib/naladb/wal"          # WAL file directory
  wal_sync_interval: "0"                  # WAL fsync interval (0 = sync every write)
  segment_dir: "/var/lib/naladb/segments" # Segment storage directory
  segment_max_bytes: 67108864             # Max segment size before rotation (64 MiB)
  compaction_strategy: "level"            # Compaction strategy: "level"
  compaction_max_l0: 4                    # Trigger compaction when L0 segments exceed this

  # ─── Tiered Storage (Memory Retention) ──────────────────────────────────
  max_memory_versions: 0                  # Versions per key in RAM (0 = unlimited/pure in-memory)
  eviction_interval: "30s"                # Background evictor cycle (0 = disable)
  eviction_batch_size: 10000              # Max keys per eviction cycle

# ─── Blob Store ─────────────────────────────────────────────────────────────
blob_store:
  enabled: true                           # Enable blob store for values > 64 KiB
  dir: "/var/lib/naladb/blobs"            # Blob storage directory
  gc_interval: "24h"                      # GC sweep interval
  gc_min_age: "24h"                       # Minimum age before GC can reclaim a blob
  max_blob_size: 104857600                # Maximum blob size (100 MiB)

# ─── Retention & TTL ────────────────────────────────────────────────────────
retention:
  policies:
    - prefix: ""                          # Global default (empty = matches all keys)
      ttl: "90d"                          # Delete data older than 90 days
      downsample_after: "30d"             # Begin downsampling after 30 days
      downsample_strategy: "avg"          # Strategy: avg, minmax, or lttb
      downsample_interval: "1h"           # Aggregate to 1-hour buckets

    - prefix: "sensors:"                  # Per-prefix policy for sensor data
      ttl: "365d"
      downsample_after: "7d"
      downsample_strategy: "minmax"
      downsample_interval: "5m"

    - prefix: "logs:"                     # Short retention, no downsampling
      ttl: "30d"

ttl:
  wheel_size: 65536                       # Timing wheel slots
  wheel_resolution: "100ms"               # Time per slot
  tick_interval: "100ms"                  # Wheel advance interval
  scan_interval: "1m"                     # Segment expiry scan interval

# ─── Graph Index ─────────────────────────────────────────────────────────────
graph_index:
  snapshot_interval: "5m"                 # How often graph index is snapshotted
  format: "json"                          # Snapshot format: "json"
  compression: "snappy"                   # Snapshot compression: "none", "snappy"

# ─── Tenants (Multi-Tenancy) ────────────────────────────────────────────────
tenants:
  defaults:                               # Default tenant configuration
    max_nodes: 10000                      # Max graph nodes (0 = unlimited)
    max_edges: 50000                      # Max graph edges (0 = unlimited)
    rate_limit: 1000                      # Max writes/second (0 = unlimited)
    retention_days: 90                    # Data retention in days (0 = unlimited)

  overrides:                              # Per-tenant overrides
    - id: "enterprise-tier"
      max_nodes: 0
      max_edges: 0
      rate_limit: 0
      retention_days: 365

    - id: "free-tier"
      max_nodes: 100
      max_edges: 500
      rate_limit: 10
      retention_days: 7

# ─── Replication ─────────────────────────────────────────────────────────────
replication:
  default_consistency: "eventual"         # Default read consistency: eventual, linearizable, bounded_stale
  max_stale_ms: 5000                      # Max staleness for BOUNDED_STALE reads (ms)

# ─── Metrics (Prometheus) ───────────────────────────────────────────────────
metrics:
  enabled: true                           # Enable Prometheus metrics
  addr: ":9090"                           # Metrics HTTP listen address
  collect_interval: "5s"                  # Gauge metric refresh interval
```

## Multi-Tenancy

NalaDB supports multi-tenant deployments with key-prefix isolation, per-tenant quotas, and rate limiting.

### Tenant Identification

The tenant ID is extracted from the `x-tenant-id` gRPC metadata header on each request. If no header is present, the request is assigned to the `default` tenant.

```bash
# Set tenant via gRPC metadata
grpcurl -H "x-tenant-id: acme-corp" localhost:7233 naladb.v1.KVService/Set ...
```

### Key-Prefix Isolation

All keys are automatically prefixed with `{tenant_id}:` in storage. Tenants cannot access each other's data. The prefix is transparent to clients — keys in requests and responses appear without the prefix.

```
# Internal storage layout
acme-corp:node:n1:meta          → Tenant "acme-corp" node metadata
other-co:node:n1:meta           → Tenant "other-co" node metadata (separate)
default:node:n1:meta            → Default tenant node metadata
```

### Tenant Configuration

Tenants can be registered with resource limits and policies:

```yaml
tenants:
  defaults:
    max_nodes: 10000
    max_edges: 50000
    rate_limit: 1000        # writes per second (0 = unlimited)
    retention_days: 90      # data retention in days (0 = unlimited)

  overrides:
    - id: "enterprise-tier"
      max_nodes: 0           # unlimited
      max_edges: 0           # unlimited
      rate_limit: 0          # unlimited
      retention_days: 365

    - id: "free-tier"
      max_nodes: 100
      max_edges: 500
      rate_limit: 10
      retention_days: 7
```

### Quota Enforcement

When a tenant exceeds its configured `max_nodes` or `max_edges` limit, further creation requests return a `RESOURCE_EXHAUSTED` gRPC error with the message `"tenant quota exceeded"`.

### Rate Limiting

Write operations (Set, Delete, CreateNode, UpdateNode, DeleteNode, CreateEdge, UpdateEdge, DeleteEdge) are rate-limited per tenant using a token bucket algorithm. When the limit is exceeded, requests return a `RESOURCE_EXHAUSTED` gRPC error with the message `"rate limit exceeded"`.

### Single-Tenant Deployments

For single-tenant deployments, no configuration is needed. All requests without an `x-tenant-id` header are assigned to the `default` tenant with no quotas or rate limits.

## Retention & TTL

NalaDB provides eager TTL management with timing-wheel-based expiry, segment-level retention scanning, and data downsampling before deletion.

### TTL Configuration

```yaml
ttl:
  wheel_size: 65536            # Number of slots in the timing wheel
  wheel_resolution: "100ms"    # Time resolution per slot
  tick_interval: "100ms"       # How often the wheel is advanced
  scan_interval: "1m"          # How often the segment expiry scanner runs
```

### Retention Policies

Retention policies define how long data is kept and how it is downsampled before expiry. Policies can be set globally or per key prefix.

```yaml
retention:
  policies:
    - prefix: ""               # Global default (empty prefix matches all)
      ttl: "90d"               # Delete data older than 90 days
      downsample_after: "30d"  # Begin downsampling after 30 days
      downsample_strategy: avg # Aggregation strategy: avg, minmax, or lttb
      downsample_interval: "1h" # Aggregate to 1-hour buckets

    - prefix: "sensors:"
      ttl: "365d"
      downsample_after: "7d"
      downsample_strategy: minmax
      downsample_interval: "5m"

    - prefix: "logs:"
      ttl: "30d"               # Short retention, no downsampling
```

### Downsample Strategies

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `avg`    | Average of values per interval | General time-series metrics |
| `minmax` | Preserves min and max per interval (2 points per bucket) | Anomaly detection, range analysis |
| `lttb`   | Largest Triangle Three Buckets — visually accurate downsampling | Dashboard visualizations |

### Per-Key TTL

Individual keys can be assigned a TTL at write time. When the TTL expires, the timing wheel triggers a tombstone write:

```bash
# Set a key with 5-second TTL
grpcurl localhost:7233 naladb.v1.KVService/SetWithTTL \
  -d '{"key": "session:abc", "value": "...", "ttl_seconds": 5}'
```

The timing wheel processes expirations at millisecond granularity. Keys scheduled with TTLs exceeding the wheel capacity (`wheel_size × wheel_resolution`) are clamped to the maximum supported TTL.

### Segment Expiry Scanner

The scanner runs periodically (controlled by `scan_interval`) and checks each segment's maximum timestamp against the configured retention policies. If a segment's newest record is older than the shortest applicable TTL, the entire segment is deleted without per-record tombstones.

### Leader-Only Execution

TTL management (both the timing wheel and the expiry scanner) runs only on the RAFT leader node. This prevents duplicate tombstone writes and ensures consistent expiration across the cluster.

## Compaction

NalaDB uses level-based compaction to merge segments and reclaim space from deleted data.

### Compaction Configuration

```yaml
compaction:
  max_l0_segments: 4           # Trigger compaction when L0 count exceeds this
```

### How Compaction Works

1. **Level-0 Overflow**: When the number of L0 segments exceeds `max_l0_segments`, all L0 segments are merged into a single L1 segment using merge-sort.
2. **Tombstone Pair Removal**: During compaction, write+delete pairs for the same key are identified and both records are removed. If the latest version of a key is a tombstone, all versions of that key are dropped.
3. **Background Execution**: Compaction runs in the background without blocking reads or writes. The segment list is atomically swapped after the new merged segment is built.

### Compaction Behavior

| Scenario | Result |
|----------|--------|
| 5 L0 segments, no tombstones | 1 L1 segment with all records |
| 10,000 records with 2,000 tombstone pairs | 6,000 records in compacted segment |
| All records are tombstone pairs | Segments removed, no output segment |

## Tiered Storage (Memory Retention)

NalaDB supports configurable memory retention to control the trade-off between RAM usage and disk I/O. By default, all version history is kept in RAM. When `max_memory_versions` is set to a positive value, older versions are evicted from RAM and served from on-disk segments.

### Configuration

```yaml
storage:
  max_memory_versions: 100     # Keep last 100 versions per key in RAM
  eviction_interval: "30s"     # Background eviction cycle
  eviction_batch_size: 10000   # Max keys per eviction cycle
  segment_dir: "/var/lib/naladb/segments"  # Required when eviction is enabled
```

### Settings Reference

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `max_memory_versions` | int | 0 | Max versions per key in RAM. 0 = unlimited (pure in-memory). |
| `eviction_interval` | duration | 30s | How often the background evictor runs. 0 = manual eviction only. |
| `eviction_batch_size` | int | 10000 | Max keys processed per eviction cycle to limit lock hold time. |

### Presets

| Preset | Setting | Use Case |
|--------|---------|----------|
| Pure in-memory | `max_memory_versions: 0` | Small datasets, lowest latency |
| Minimal RAM | `max_memory_versions: 1` | Large datasets, most history served from disk |
| Balanced | `max_memory_versions: 100` | Recent history in RAM, older history on disk |
| Near in-memory | `max_memory_versions: 1000000` | Effectively in-memory for most workloads |

### Requirements

- **Segments must be enabled**: `segment_dir` must be set when `max_memory_versions > 0`. The store will fail to start if eviction is enabled without segments.
- **WAL or RAFT log**: Required for crash recovery. On restart, the WAL is replayed and then immediately trimmed by the evictor.

### Behavior

- The current value (`Get`) is always served from RAM regardless of this setting.
- `GetAt` and `History` transparently fall through to disk segments when the requested data has been evicted.
- The evictor only removes versions confirmed to be in finalized segments — data in the active segment is never evicted.
- Eviction runs in the background and does not block reads or writes.

See [docs/storage-model.md](storage-model.md) for detailed capacity planning and architecture.

## Observability

NalaDB exposes Prometheus metrics for monitoring performance and cluster health.

### Prometheus Endpoint

The metrics HTTP server runs on a separate port (default `:9090`):

```bash
./bin/naladb --addr :7301 --metrics-addr :9090
```

Metrics are available at `GET /metrics` in Prometheus exposition format.

### Configuration

```yaml
metrics:
  addr: ":9090"       # Prometheus HTTP listen address
  collect_interval: 5s # How often gauge metrics are refreshed
```

### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `naladb_writes_total` | counter | Total write operations |
| `naladb_reads_total` | counter | Total read operations |
| `naladb_write_duration_seconds` | histogram | Write operation latency |
| `naladb_read_duration_seconds` | histogram | Read operation latency |
| `naladb_keys_total` | gauge | Keys in the in-memory index |
| `naladb_segments_total` | gauge | Finalized segment count |
| `naladb_segment_bytes` | gauge | Total bytes in segments |
| `naladb_raft_term` | gauge | Current RAFT term |
| `naladb_raft_commit_index` | gauge | Current RAFT commit index |
| `naladb_raft_is_leader` | gauge | 1 if leader, 0 otherwise |
| `naladb_grpc_requests_total` | counter | gRPC requests by method |
| `naladb_ttl_expired_total` | counter | Keys expired by TTL |
| `naladb_compaction_duration_seconds` | histogram | Compaction duration |
| `naladb_blob_store_bytes` | gauge | Blob store disk usage |

### Grafana Dashboard

A pre-built Grafana dashboard is included at `docker/grafana/dashboard.json`. When using Docker Compose, Grafana is auto-provisioned with the dashboard at `http://localhost:3000` (user: `admin`, password: `naladb`).

The dashboard includes panels for:
- Write/read rate and latency (p50, p99)
- Key count, segment count, and storage size
- RAFT cluster state (leader, term, commit index)
- gRPC request rate by method
- TTL expiry rate and compaction duration
