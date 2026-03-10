# NalaDB Storage Model: Tiered Memory & Disk

> This document explains how NalaDB uses memory and disk, the tiered storage model, eviction behavior, and how to plan capacity.

## Overview

NalaDB uses a **tiered storage model** with configurable memory retention. Recent versions of each key are kept in RAM (hot tier) for fast access, while older versions are served from on-disk segments (cold tier). The `MaxMemoryVersions` setting controls the boundary:

- **`MaxMemoryVersions = 0`** (default): Pure in-memory mode. All versions stay in RAM. This is the original behavior — you cannot store more data than fits in memory.
- **`MaxMemoryVersions > 0`**: Tiered mode. Only the last N versions per key stay in RAM. Older versions are evicted and served from disk segments. The database can hold datasets larger than RAM.

The current value (via `Get`) is **always** served from the in-memory index regardless of this setting.

## Tiered Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    HOT TIER (RAM)                        │
│                                                         │
│  vlog[key] = last N versions per key (configurable)     │
│  idx       = current value per key (always in RAM)      │
│  sortedKeys = key index (always in RAM)                 │
│  bloom/sparse per segment (always in RAM, small)        │
│  meta registry = per-key statistics (always in RAM)     │
│                                                         │
│  Access: O(1) Get, O(log n) GetAt, O(log n + m) History │
├─────────────────────────────────────────────────────────┤
│                   COLD TIER (Disk)                       │
│                                                         │
│  Finalized segments: sorted .log + .idx + .bloom + .meta│
│  Full version history for all keys                      │
│  Access: O(log n) GetAt via bloom + sparse index + seek │
│                                                         │
│  Compacted segments: L0 → L1 (merged, deduped)         │
└─────────────────────────────────────────────────────────┘
```

### What Lives in Memory

| Component | Data Structure | Growth Pattern | Per-Entry Cost |
|-----------|---------------|----------------|----------------|
| **Version Log** | `map[string][]version` | Capped at `MaxMemoryVersions` per key (or unlimited if 0) | ~50 bytes + value size (up to 64 KiB inline, or 40 bytes if blob ref) |
| **Current-Value Index** | 256-shard map | One entry per unique key | ~50 bytes + value size |
| **Meta Registry** | `map[string]*KeyMeta` | One entry per unique key | ~700 bytes (includes HyperLogLog sketch) |
| **Sorted Keys Index** | `[]string` | One entry per unique key | Key length + slice overhead |
| **Bloom Filters** | Per finalized segment | One per segment | Depends on unique key count (~1% FPR) |
| **Sparse Indices** | Per finalized segment | One entry per 4 KiB of segment data | ~20 bytes per entry |
| **Blob References** | `map[[32]byte]*blobEntry` | One per unique blob hash | ~60 bytes |

### What Lives on Disk

| Component | Location | Purpose | Growth Pattern |
|-----------|----------|---------|----------------|
| **WAL** | `wal_dir` | Crash recovery (replay on startup) | Append-only, one file |
| **Segments** | `segment_dir` | Historical persistence, point-in-time queries | Rotated at `segment_max_bytes` (default 64 MiB) |
| **Blobs** | `blob_store.dir` | Content-addressed storage for values > 64 KiB | One file per unique content hash |
| **RAFT log** | `raft.data_dir` | Consensus replication (replaces WAL in cluster mode) | Managed by hashicorp/raft |

## Configuration

```yaml
storage:
  max_memory_versions: 100     # Keep last 100 versions per key in RAM
  eviction_interval: "30s"     # Background eviction cycle frequency
  eviction_batch_size: 10000   # Max keys processed per eviction cycle
  segment_dir: "/var/lib/naladb/segments"  # Required when max_memory_versions > 0
```

| Setting | Default | Description |
|---------|---------|-------------|
| `max_memory_versions` | 0 (unlimited) | Versions per key retained in RAM. 0 = keep all (pure in-memory). |
| `eviction_interval` | 30s | How often the background evictor runs. 0 = disable background eviction. |
| `eviction_batch_size` | 10000 | Max keys processed per eviction cycle to avoid long pauses. |

### Choosing a Value for `max_memory_versions`

| Setting | RAM Usage | Disk Reads | Best For |
|---------|-----------|------------|----------|
| `0` | All versions in RAM | None for GetAt/History | Small datasets, lowest latency |
| `1` | Minimal (current + 1 version per key) | Most GetAt/History queries hit disk | Large datasets, minimal RAM |
| `100` | Last 100 versions per key | Only queries older than ~100 writes ago | Balanced: covers most temporal queries in RAM |
| `10000+` | Effectively all versions for most keys | Rare | High-write keys with deep history queries |

## Query Paths with Tiered Storage

### `Get(key)` — Current Value

```
idx.Get(key) → O(1), always in RAM, no change regardless of MaxMemoryVersions
```

### `GetAt(key, ts)` — Point-in-Time

```
1. Binary search vlog[key] (in-memory)
2. If found and ts >= oldest in-memory version → return (hot path, no disk I/O)
3. If ts < oldest in-memory version → segments.GetAt(key, ts) (cold path, disk)
```

### `History(key, opts)` — Version Range

```
1. Slice vlog[key] for in-memory portion of [from, to]
2. If from < oldest in-memory version → segments.HistoryRange(key, from, oldestInMem)
3. Merge disk + memory results (both sorted by HLC)
4. Apply reverse/limit
```

### `ScanPrefix(prefix)` — Key Enumeration

```
Binary search on sortedKeys → no change (key index always in RAM)
```

## Eviction

### How It Works

The background evictor trims the in-memory version log (`vlog`) to `MaxMemoryVersions` per key. It runs on a timer (`EvictionInterval`) and also triggers immediately after each segment rotation.

### Safety Invariant

Versions are only evicted from RAM after they are confirmed to be persisted in **finalized** (not active) segments:

1. Query `LatestFinalizedMaxTS()` — the max HLC of the newest finalized segment.
2. For each key where `len(versions) > MaxMemoryVersions`:
   - Only evict versions with HLC <= the finalized timestamp.
   - Always keep at least `MaxMemoryVersions` most recent versions.
3. Active segment data (not yet sorted/indexed) is never considered safe for eviction.

This guarantees zero data loss — every evicted version is recoverable from disk.

### Eviction Triggers

- **Background ticker**: Runs every `EvictionInterval` (default 30s).
- **Segment rotation callback**: Immediate eviction pass after each segment rotation.

### What Eviction Does NOT Do

- Does not affect the current-value index (`idx`) — `Get` is always fast.
- Does not affect the sorted keys index — `ScanPrefix` is always fast.
- Does not delete data from disk — it only removes copies from RAM.
- Does not block reads or writes — eviction holds the vlog lock briefly per batch.

## Startup Recovery

### With `MaxMemoryVersions = 0` (Default)

Standard behavior: replay WAL/RAFT log, rebuild full version log and index in memory. Startup memory equals steady-state memory.

### With `MaxMemoryVersions > 0` (Tiered Mode)

1. Load finalized segments (bloom filters + sparse indices only — small footprint).
2. Replay WAL/RAFT log into the version log (rebuilds full history temporarily).
3. Run an immediate eviction pass — trims vlog to `MaxMemoryVersions` per key.
4. Old versions are already in segments from before the restart.

This means startup still replays the full WAL for correctness, but immediately reclaims memory. The WAL can be truncated after segments are confirmed — entries with HLC <= the latest finalized segment are redundant.

## Capacity Planning

### Memory Sizing — Pure In-Memory (`max_memory_versions = 0`)

```
Memory ≈ (total_versions × avg_version_cost) + (unique_keys × 800 bytes)
```

Where:
- `total_versions` = sum of all writes across all keys (including deletes)
- `avg_version_cost` = 10 bytes overhead + average value size (capped at 64 KiB for inline, 40 bytes for blob refs)
- `800 bytes per unique key` = index entry + meta registry + sorted keys overhead

### Memory Sizing — Tiered Mode (`max_memory_versions = N`)

```
Memory ≈ (unique_keys × N × avg_version_cost) + (unique_keys × 800 bytes)
```

The version log is bounded: at most `N` versions per key. For example, 100K keys with `max_memory_versions=100` and 200-byte average values:

```
Version log:  100,000 keys × 100 versions × 210 bytes  ≈ 2.0 GiB
Fixed cost:   100,000 keys × 800 bytes                  ≈  76 MiB
─────────────────────────────────────────────────────────────────
Total                                                    ≈ 2.1 GiB
```

Compare with unlimited mode at 1,000 versions per key: ~20 GiB.

### Disk Sizing

```
Disk ≈ WAL_size + segment_total_size + blob_total_size
```

Disk usage grows with total write volume regardless of `MaxMemoryVersions`. Compaction reclaims space from tombstone pairs, but all non-deleted versions remain on disk.

### Monitoring

| Metric | What It Tells You |
|--------|-------------------|
| `naladb_keys_total` | Number of unique keys in the index |
| `naladb_segments_total` | Number of finalized segments on disk |
| `naladb_segment_bytes` | Total disk usage for segments |
| `naladb_blob_store_bytes` | Total disk usage for blobs |

For memory monitoring, use Go runtime metrics or the process RSS from your OS/container.

## Recommendations

1. **Start with `max_memory_versions = 0`** if your dataset fits in RAM — it gives the lowest latency for all query types.
2. **Switch to tiered mode** when the version log approaches your memory limit. Set `max_memory_versions` high enough to cover your most common temporal query ranges.
3. **Use blob store** for values > 64 KiB — reduces per-version in-memory cost to 40 bytes.
4. **Monitor process RSS** — in tiered mode, memory should stabilize at `unique_keys × MaxMemoryVersions × avg_version_cost`.
5. **Enable segments** — `MaxMemoryVersions > 0` requires segments to be configured. The store will fail fast if eviction is enabled without segments.
6. **Use TTL and retention policies** to bound disk growth. In tiered mode, TTL also reduces the number of versions that need to be evicted.

## Disk vs. Memory Relationship

```
┌──────────────────────────────────────────────────────┐
│                      MEMORY                           │
│                                                       │
│  ┌──────────────┐  ┌───────────┐  ┌──────────────┐   │
│  │ Version Log  │  │ Index     │  │ Meta Registry│   │
│  │ (last N per  │  │ (current  │  │ (stats per   │   │
│  │  key, or all │  │  values)  │  │  key)        │   │
│  │  if N = 0)   │  │           │  │              │   │
│  └──────┬───────┘  └───────────┘  └──────────────┘   │
│         │ eviction                                    │
│         ▼                                             │
│  ┌──────────────┐  ┌───────────┐                      │
│  │ Bloom Filters│  │ Sparse    │                      │
│  │ (per segment)│  │ Indices   │                      │
│  └──────────────┘  └───────────┘                      │
│                                                       │
├───────────────────────────────────────────────────────┤
│                       DISK                            │
│                                                       │
│  ┌──────────────┐  ┌───────────┐  ┌──────────────┐   │
│  │ WAL          │  │ Segments  │  │ Blobs        │   │
│  │ (durability) │  │ (full     │  │ (values      │   │
│  │              │  │  history) │  │  > 64 KiB)   │   │
│  └──────────────┘  └───────────┘  └──────────────┘   │
│                                                       │
└───────────────────────────────────────────────────────┘
```
