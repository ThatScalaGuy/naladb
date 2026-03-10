# NalaDB Architecture

> This document describes the high-level architecture of NalaDB.

## Overview

NalaDB is a temporal key-value graph database built as a layered system. Each layer builds on the one below it, providing increasing levels of abstraction.

## Layers

### Hybrid Logical Clock (HLC)

The foundation of NalaDB's temporal capabilities. Every write is stamped with a Hybrid Logical Clock that combines wall-clock time with a logical counter, enabling causal ordering across distributed nodes.

#### Packed 8-Byte Layout

```
 63                  16 15    12 11          0
┌──────────────────────┬────────┬─────────────┐
│  Wall-Time (48 bit)  │Node(4) │ Logical(12) │
│  µs since NalaDB     │  ID    │  Counter    │
│  epoch               │ 0–15   │  0–4095     │
└──────────────────────┴────────┴─────────────┘
```

- **Wall-Time (bits 63–16):** 48-bit microsecond counter since the NalaDB epoch (January 1, 2025 UTC). Provides ~8.9 years of range.
- **Node ID (bits 15–12):** 4-bit identifier supporting up to 16 nodes in a cluster.
- **Logical Counter (bits 11–0):** 12-bit counter (0–4095) that increments when the physical clock has not advanced, preserving happens-before ordering.

The natural `uint64` ordering of packed HLC values matches the tuple ordering `(WallTime, NodeID, Logical)`, enabling efficient comparisons and index lookups with a single integer comparison.

#### Ordering Relation

For two HLC values `a` and `b`, `a < b` iff:
1. `a.WallTime < b.WallTime`, or
2. `a.WallTime == b.WallTime` and `a.NodeID < b.NodeID`, or
3. `a.WallTime == b.WallTime` and `a.NodeID == b.NodeID` and `a.Logical < b.Logical`

#### Clock Rules

**Local event (`Clock.Now()`):**
1. Read physical time `pt` (µs since NalaDB epoch).
2. If `pt > last.WallTime`: emit `(pt, nodeID, 0)`.
3. Otherwise: emit `(last.WallTime, nodeID, last.Logical + 1)`.

**Remote receive (`Clock.Update(remote)`):**
1. Read physical time `pt`.
2. Set `newWall = max(pt, last.WallTime, remote.WallTime)`.
3. Determine logical counter based on which wall times tied the max.
4. Emit `(newWall, nodeID, logical)`.

Both operations guarantee strict monotonicity: every emitted timestamp is greater than all previously emitted timestamps.

#### Clock Skew Protection

In a distributed cluster, nodes may have slightly different system clocks due to NTP drift or misconfiguration. The HLC algorithm naturally handles small forward skew by adopting the remote wall time via the `max()` operation. However, **unbounded** skew is dangerous: a single node whose clock is far in the future can permanently push the entire cluster's timestamps forward, wasting the 48-bit wall-time range and corrupting temporal ordering.

To prevent this, `Clock.Update()` enforces a **maximum clock skew** bound. If the remote HLC's wall time exceeds the local physical clock by more than the configured tolerance, the update is **rejected** with `ErrClockSkew` and the local clock state remains unchanged.

```
Remote wall time ─────────────────────────────┐
Local physical time ──────────┐               │
                              │               │
                              ▼               ▼
                         ├─────────────────────┤
                              skew = remote - local

  If skew > max_clock_skew → reject (ErrClockSkew)
  If skew ≤ max_clock_skew → accept, proceed with HLC merge
```

**Default:** 1 second (1 000 000 µs). Configurable via `--max-clock-skew` flag or `hlc.max_clock_skew` in the YAML config. Set to `0` to disable the check (not recommended in production).

**Operational requirement:** All cluster nodes should run NTP (or a similar time synchronization service) to keep clock drift well below the configured tolerance. A typical NTP-synchronized host has drift under 10ms, far within the 1-second default.

### Write-Ahead Log (WAL)

All writes are first persisted to a WAL for crash recovery. The WAL uses a binary record format with CRC32 checksums for integrity verification.

#### Record Layout

```
 0         4                  12    13      15         19
┌──────────┬──────────────────┬─────┬───────┬──────────┬──────────────────────────┐
│ CRC32    │ HLC Timestamp    │Flags│KeyLen │ ValLen   │ Key        │ Value       │
│ (4 B)    │ (8 B)            │(1 B)│(2 B)  │ (4 B)    │ (variable) │ (variable)  │
└──────────┴──────────────────┴─────┴───────┴──────────┴──────────────────────────┘
           |<─────────── CRC32 covers this range ───────────────────────────────>|
```

- **CRC32 (4 bytes):** IEEE CRC32 checksum over all subsequent bytes (HLC through end of Value). Little-endian.
- **HLC (8 bytes):** Hybrid Logical Clock timestamp (see HLC section). Little-endian uint64.
- **Flags (1 byte):** Bit flags controlling record interpretation:
  - Bit 0 (`0x01`): **Tombstone** — marks a key deletion.
  - Bit 1 (`0x02`): **Compressed** — value is Snappy-compressed.
  - Bit 2 (`0x04`): **BlobRef** — value contains a 40-byte blob reference (SHA-256 hash + size) instead of inline data.
- **KeyLen (2 bytes):** Length of the key in bytes (max 512). Little-endian.
- **ValLen (4 bytes):** Length of the value in bytes. Little-endian.
- **Key (variable):** The key bytes.
- **Value (variable):** The value bytes (or compressed value, or blob reference).

#### Compression

Values above 256 bytes are automatically Snappy-compressed when using `CompressionAuto` mode. Compression is only applied if it actually reduces size. The `FlagCompressed` bit is set on disk; the Reader transparently decompresses on read.

#### Crash Recovery

The WAL is append-only. On crash recovery, the Reader reads records sequentially:

1. Complete records with valid CRC32 are accepted.
2. A truncated record at the end of the file (incomplete header or payload) is treated as a normal end-of-file condition — not an error.
3. A record with a CRC32 mismatch anywhere in the file is reported as `ErrCorruptRecord`.

This design means the WAL can tolerate crashes mid-write: any partial record at the tail is simply discarded on recovery.

#### Batch fsync

The Writer supports configurable `SyncInterval` for batching fsync calls:

- **SyncInterval = 0:** Every `Append` call triggers an immediate `flush + fsync` (safest, highest latency).
- **SyncInterval > 0:** A background goroutine calls `flush + fsync` at the configured interval. Writes between syncs are buffered in a 64 KiB `bufio.Writer`. `Close()` always performs a final sync.

### In-Memory Index

A sharded in-memory index (256 virtual shards) provides O(1) current-value lookups. The index maps keys to their latest HLC timestamp and value.

#### Shard Architecture

```
Key ──► FNV-1a hash ──► hash % 256 ──► Shard[n]
                                         │
                                    ┌────┴────┐
                                    │ RWMutex │
                                    │ map[key] │──► Entry{HLC, Value, Tombstone}
                                    └─────────┘
```

- **256 virtual shards** reduce lock contention; concurrent readers/writers typically hit different shards.
- Each shard uses `sync.RWMutex` — reads are non-blocking when no write is in progress on the same shard.
- **FNV-1a** hashing provides uniform key distribution (verified: stddev < 10% of mean for 100k random keys).
- **Put** only applies if the new HLC >= existing HLC, preventing out-of-order overwrites.

### Segment Storage

Immutable segment files store historical data with efficient point-in-time lookups. The segment subsystem consists of a **Manager** that handles automatic rotation and multi-segment queries, and individual **Segment** files with associated indices.

#### File Layout

Each finalized segment produces four files:

```
data/
├── seg-000001.log      # Sorted WAL records (by key, then HLC)
├── seg-000001.idx      # Sparse index (binary: key → byte offset)
├── seg-000001.bloom    # Bloom filter (probabilistic key membership)
├── seg-000001.meta     # JSON metadata (min_ts, max_ts, record_count, size_bytes)
├── seg-000002.log
├── seg-000002.idx
├── seg-000002.bloom
├── seg-000002.meta
├── seg-current.log     # Active writable segment (append-only, time-ordered)
└── seg-current.idx     # Placeholder for active segment
```

#### Rotation

The Manager writes records to `seg-current.log` in arrival order. When the active segment exceeds `maxBytes` (configurable, default 1 MiB for tests):

1. Flush and close the active segment file.
2. Read all records from the active log.
3. Sort records by `(key, HLC ascending)` — producing an SSTable-like sorted run.
4. Write sorted records to `seg-NNNNNN.log`.
5. Build a **sparse index** (one entry every 4 KiB of log data) and write to `.idx`.
6. Build a **bloom filter** from all unique keys and write to `.bloom`.
7. Write segment **metadata** (min/max HLC, record count, size) to `.meta`.
8. Remove old `seg-current.*` files and open a fresh active segment.

#### Sparse Index

The sparse index samples the first record at every 4 KiB block boundary in the sorted log:

```
┌─────────────────────────────────────────────┐
│ NumEntries (uint32)                         │
├──────────┬────────────┬─────────────────────┤
│ KeyLen   │ Key        │ Offset (int64)      │
│ (uint16) │ (variable) │                     │
├──────────┼────────────┼─────────────────────┤
│ ...      │ ...        │ ...                 │
└──────────┴────────────┴─────────────────────┘
```

**Lookup:** Binary search over sorted entries finds the largest entry with `Key <= target`, giving the byte offset to start scanning. Complexity: O(log n) where n = number of index entries (in-memory binary search, no disk I/O for the index itself).

#### Bloom Filter

Each segment has a bloom filter (using `bits-and-blooms/bloom/v3`) with a configurable false positive rate (default 1%). The filter enables fast negative lookups — if `Test(key)` returns false, the key is guaranteed not to be in the segment.

#### Point-in-Time Query (GetAt)

```
GetAt("key", ts)
  │
  └──► For each segment (newest → oldest):
         │
         ├── min_ts > ts?  → skip (time range filter)
         │
         ├── bloom.Test("key") == false?  → skip (bloom filter)
         │
         └── sparse.Lookup("key") → offset
               │
               └── Seek to offset, scan sorted records:
                     • key < target → continue
                     • key == target && HLC <= ts → candidate (keep latest)
                     • key == target && HLC > ts → break
                     • key > target → break
```

Since segments have non-overlapping, ordered time ranges, the first match found (searching newest to oldest) is the globally latest version ≤ ts.

### Temporal KV Store

The core storage engine combining WAL, index, and an in-memory version log to provide Set, Get, GetAt, Delete, History, and BatchSet operations.

> **Note:** NalaDB uses a tiered storage model — recent versions are held in RAM for fast access while older versions can be evicted to on-disk segments. The `MaxMemoryVersions` setting controls the trade-off: set it to 0 (default) for pure in-memory behavior, or to a positive value to cap per-key RAM usage and serve historical data from disk. See [docs/storage-model.md](storage-model.md) for details on tiered storage, capacity planning, and eviction behavior.

#### Write Path

```
Set("key", value)
  │
  ├──1──► Clock.Now() → generate HLC timestamp
  │
  ├──2──► WAL.Append(ts, flags, key, value) → persist to disk
  │         │
  │         └──► Buffered write + fsync (batch or immediate)
  │
  ├──3──► Index.Put(key, ts, value) → update current-value lookup
  │
  └──4──► VersionLog.append(key, ts, value) → append to history
```

1. **Clock.Now()** generates a monotonically increasing HLC timestamp.
2. **WAL.Append** persists the record to disk with CRC32 integrity. This is the durability guarantee — if the process crashes after this step, the write can be recovered.
3. **Index.Put** updates the sharded in-memory map so subsequent `Get` calls return the new value. The update is only applied if the HLC is >= the existing entry.
4. **VersionLog.append** adds the entry to the per-key version history for `GetAt` and `History` queries.

#### Read Path — Current Value (O(1))

```
Get("key")
  │
  └──► Index.Get(key)
         │
         ├── found & not tombstoned → return Entry{Value, HLC}
         └── not found or tombstoned → return {Found: false}
```

A single hash + map lookup in the appropriate shard. No disk I/O required.

#### Read Path — Point-in-Time (GetAt)

```
GetAt("key", at_hlc)
  │
  ├──1──► VersionLog[key] (in-memory)
  │         │
  │         └──► Binary search for latest version where ts <= at_hlc
  │                │
  │                ├── found & ts >= oldest in-memory version → return (hot path)
  │                └── not found or ts < oldest in-memory version → continue
  │
  └──2──► Segment Manager.GetAt(key, at_hlc) (disk fallthrough)
            │
            ├── Search segments newest → oldest (bloom + sparse index)
            ├── found → return value from disk (cold path)
            └── not found → return {Found: false}
```

The hot path (version still in RAM) uses `sort.Search` — O(log n) with no disk I/O. When `MaxMemoryVersions` is set and the requested timestamp is older than the oldest in-memory version, the query falls through to on-disk segments using bloom filters and sparse indices.

#### Read Path — History

```
History("key", from, to, limit, reverse)
  │
  ├──1──► VersionLog[key] (in-memory portion)
  │         │
  │         ├──► Binary search for start index (from)
  │         ├──► Binary search for end index (to)
  │         └──► Slice in-memory results in [from, to]
  │
  ├──2──► If MaxMemoryVersions > 0 and from < oldest in-memory version:
  │         │
  │         └──► Segment Manager.HistoryRange(key, from, oldestInMem)
  │               └──► Scan segments for versions in the evicted range
  │
  ├──3──► Merge disk + memory results (both sorted by HLC)
  │
  └──4──► Apply reverse and limit on merged result
```

When all versions are in RAM (default), this is a pure in-memory range scan. When `MaxMemoryVersions` is set, the query merges in-memory versions with on-disk segment data to produce the complete history.

#### BatchSet

BatchSet writes multiple key-value pairs with consecutive HLC timestamps from the same clock, guaranteeing ascending temporal order across all entries in the batch.

### Tiered Storage & Memory Eviction

NalaDB uses a two-tier storage model: a hot tier (RAM) for recent data and a cold tier (disk segments) for historical data. The boundary between tiers is controlled by `MaxMemoryVersions`.

#### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    HOT TIER (RAM)                        │
│                                                         │
│  vlog[key] = last N versions (configurable)             │
│  idx       = current value per key (always in RAM)      │
│  sortedKeys = key index (always in RAM)                 │
│  bloom/sparse per segment (always in RAM, small)        │
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

#### Configuration

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `MaxMemoryVersions` | int | 0 (unlimited) | Max versions per key in RAM. 0 = keep all (pure in-memory). |
| `EvictionInterval` | duration | 30s | Background evictor cycle frequency. 0 = disable background eviction. |
| `EvictionBatchSize` | int | 10000 | Max keys processed per eviction cycle. |

#### Eviction Safety

The background evictor only removes versions from RAM that are confirmed to be persisted in finalized (not active) segments. The safety invariant:

1. Query `LatestFinalizedMaxTS()` — the max HLC of the newest finalized segment.
2. Only evict versions with HLC <= that timestamp.
3. Keep at least `MaxMemoryVersions` most recent versions per key, regardless.

This guarantees that evicted data is always recoverable from disk. The active segment (which has not yet been sorted and indexed) is never considered safe for eviction.

#### Eviction Triggers

- **Background ticker**: Runs every `EvictionInterval` (default 30s).
- **Segment rotation callback**: An immediate eviction pass runs after each segment rotation, ensuring memory is reclaimed promptly when new segments are finalized.

#### Startup Behavior

When `MaxMemoryVersions > 0`, the startup sequence is:

1. Load finalized segments (bloom filters + sparse indices only — small footprint).
2. Replay WAL (or RAFT log) into the version log as normal.
3. Run an immediate eviction pass to trim the version log down to `MaxMemoryVersions` per key.
4. Optionally truncate the WAL — entries with HLC <= the latest finalized segment are redundant and can be removed to speed up future restarts.

#### Behavioral Guarantees

- **Write path is unchanged**: Every write still goes to WAL + segments + vlog + idx. Eviction is a separate background process.
- **Read consistency**: `Get` always returns the current value from the in-memory index. `GetAt` and `History` merge RAM + disk data transparently.
- **No data loss**: Versions are only evicted after confirmation in finalized segments.
- **Graceful degradation**: Only historical queries (`GetAt`, `History`) pay a disk I/O penalty for evicted data. Current-value reads are unaffected.
- **Backward compatible**: `MaxMemoryVersions = 0` (default) preserves the original pure in-memory behavior exactly.

### KeyMeta Statistics

Every write to the store updates per-key inline statistics, enabling META queries without a separate aggregation job. The `meta.Registry` manages a `KeyMeta` struct per key, updated atomically on each `Set` or `Delete`.

#### Welford's Online Algorithm

Running mean, variance, and standard deviation are computed incrementally using Welford's online algorithm. For each new numeric value `x`:

```
n++
delta  = x - mean
mean  += delta / n
delta2 = x - mean
M2    += delta * delta2
stddev = sqrt(M2 / n)
```

This is numerically stable even for large sample sizes. Non-numeric values (those that do not parse as `float64`) are skipped for statistical updates but still counted in `TotalWrites`.

#### EWMA Write Rate

The write frequency (Hz) is estimated using an Exponentially Weighted Moving Average with smoothing factor alpha = 0.05:

```
interval_us = current_wall_us - last_wall_us
instant_hz  = 1,000,000 / interval_us
rate_hz     = alpha * instant_hz + (1 - alpha) * rate_hz
```

The first interval initializes the rate directly. Subsequent writes blend the instantaneous rate into the running average.

#### HyperLogLog Cardinality

Distinct value cardinality is estimated using HyperLogLog (via `axiomhq/hyperloglog`). Each value's bytes are inserted into a per-key sketch, providing ~1.6% standard error. The `Cardinality` field exposes the current estimate.

#### KeyMeta Fields

| Field | Type | Description |
|-------|------|-------------|
| TotalWrites | uint64 | Total number of writes to this key |
| FirstSeenUs | int64 | Wall-time (µs) of the first write |
| LastSeenUs | int64 | Wall-time (µs) of the most recent write |
| WriteRateHz | float64 | EWMA write frequency (writes/second) |
| MinValue | float64 | Minimum numeric value observed |
| MaxValue | float64 | Maximum numeric value observed |
| AvgValue | float64 | Running mean (Welford) |
| StdDevValue | float64 | Population standard deviation (Welford) |
| Cardinality | uint32 | Estimated distinct values (HyperLogLog) |
| SizeBytes | uint64 | Cumulative bytes written |

### Blob Store

Values exceeding 64 KiB are transparently stored in a content-addressed blob store, keeping the WAL compact. The blob store uses SHA-256 hashing for content addressing and deduplication.

#### Architecture

```
Set("key", 100KB_value)
  │
  ├── len(value) > 64 KiB?
  │     │
  │     ├── Yes: blob.Put(value) → Ref{SHA-256, Size, Flags}
  │     │         │
  │     │         ├── File exists? → increment ref count (dedup)
  │     │         └── New? → write to {dir}/{hash[:2]}/{hash}.blob
  │     │
  │     │   WAL.Append(ts, FlagBlobRef, key, ref_bytes)  // 40-byte ref
  │     │
  │     └── No: WAL.Append(ts, 0, key, value)  // inline
  │
  └── Get("key")
        │
        └── entry.BlobRef? → blob.Get(ref) → original value
```

#### BlobRef Format (40 bytes)

```
┌──────────────────────────────┬──────────────┬──────────────┐
│ SHA-256 Hash (32 bytes)      │ Size (4B LE) │ Flags (4B LE)│
└──────────────────────────────┴──────────────┴──────────────┘
```

#### Content-Addressed Deduplication

When two keys reference identical content, only one blob file exists on disk. The blob store maintains an in-memory reference count per hash. `Put` increments the count; `Deref` (called on `Delete`) decrements it.

#### Garbage Collection Lifecycle

1. Key is deleted → `Deref(ref)` decrements the blob's reference count.
2. GC sweep runs periodically with a configurable `minAge` (default 24h).
3. Blobs with `refcount == 0` and `age > minAge` are removed from disk.
4. The `minAge` safety window prevents deletion of blobs that may still be needed by in-flight reads.

### Graph Layer

Nodes and edges are projected onto the temporal KV store using a namespace convention. The graph layer provides CRUD operations with full temporal versioning, BFS/DFS traversal, and causal queries.

#### Namespace Convention

All graph data is stored as regular keys in the temporal KV store:

```
node:{node_id}:meta          → Node metadata (JSON: id, type, valid_from, valid_to, deleted)
node:{node_id}:prop:{name}   → Node property (raw bytes)
edge:{edge_id}:meta          → Edge metadata (JSON: id, from, to, relation, valid_from, valid_to, deleted)
edge:{edge_id}:prop:{name}   → Edge property (raw bytes)
graph:adj:{node_id}:out      → Outgoing edge IDs (JSON: edge_ids array)
graph:adj:{node_id}:in       → Incoming edge IDs (JSON: edge_ids array)
```

This design means graph data inherits all temporal capabilities (History, GetAt, point-in-time queries) from the underlying KV store without any additional versioning infrastructure.

#### Node/Edge Lifecycle

- **Create:** Generates a UUID v7 ID, stores metadata with `valid=[now, MaxHLC)`, initializes empty adjacency lists.
- **Update:** Writes new property values, which the store automatically versions with ascending HLC timestamps.
- **Delete:** Soft-delete — sets `deleted=true` in metadata, preserving full history. Node deletion cascades to all connected edges.

#### Adjacency List Versioning

Adjacency lists are stored as JSON arrays of edge IDs. Each mutation (add or remove an edge) writes the complete updated list, creating a new version in the store's version log. This enables point-in-time adjacency queries:

```
GetOutgoingEdgesAt("node_id", hlc_timestamp)
```

Returns the exact set of edges that existed at that timestamp, leveraging the store's `GetAt` binary search over the version log.

#### Edge Validity Invariants

- An edge's validity range `[validFrom, validTo)` must be contained within both endpoint nodes' validity ranges.
- Multiple edges of the same relation type between the same nodes are allowed (multi-graph).
- Edge IDs are always UUID v7, ensuring temporal ordering by creation time.

### Graph Traversal

The graph layer provides temporal-aware BFS traversal and shortest-path queries, enabling queries like "what did the graph look like at time T?"

#### TraverseAt (BFS)

```
TraverseAt(start="pump_3", at=HLC(T), max_depth=3, direction=OUTGOING)
  │
  └──► BFS from start node:
         │
         For each level (up to max_depth):
           │
           ├── Get adjacency list at time T (outgoing/incoming/both)
           │
           ├── For each edge ID in adjacency list:
           │     │
           │     ├── GetEdgeAt(id, T) → EdgeMeta
           │     │
           │     ├── Valid at T? → edge.ValidFrom <= T < edge.ValidTo
           │     │     │
           │     │     ├── No  → skip
           │     │     └── Yes → check relation filter (if set)
           │     │
           │     └── Add neighbor to results, enqueue for next level
           │
           └── Skip already-visited nodes (cycle prevention)
```

**Key features:**
- **Temporal filtering:** Only edges valid at the queried timestamp are followed. Both the adjacency list version and the edge metadata validity are checked.
- **Direction:** `Outgoing`, `Incoming`, or `Both`.
- **Relation filter:** Optional list of relation types to restrict traversal.
- **Max depth:** Limits BFS depth (0 = unlimited).
- **Node properties:** Optionally loads property values at the queried timestamp into results.

#### PathQuery (Shortest Path)

Uses BFS to find the shortest (fewest hops) path between two nodes at a given timestamp. Same temporal filtering as TraverseAt. Returns the ordered list of node IDs and path length, or `ErrNoPath` if no path exists.

#### Performance

| Operation | Target | Measured |
|-----------|--------|----------|
| 3-Hop Traversal (10k nodes, 50k edges) | < 50 ms | ~25 µs |

The in-memory adjacency list design enables sub-millisecond traversals even for moderately sized graphs.

### Causal Traversal

NalaDB's differentiating feature: causal chain tracing using HLC-ordered property changes, graph topology, and exponential confidence decay. Supports both **forward impact analysis** (what was affected?) and **backward root-cause analysis** (what caused this?).

#### Algorithm (BFS + Per-Hop Property Change Detection)

```
CausalTraverse(trigger="redis_main", at=HLC(T), depth=5, window=15min, direction=FORWARD)
  │
  └──► BFS from trigger node:
         │
         Root: {nodeID=trigger, depth=0, changeTime=T, confidence=1.0}
         │
         For each dequeued entry (if depth < maxDepth):
           │
           ├── Get neighbors via neighborsAt(nodeID, T, direction)
           │     └── Edge validity always checked at original trigger time T
           │
           ├── Apply RelationFilter (if set)
           │
           ├── For each unvisited neighbor:
           │     │
           │     ├── detectPropertyChange(neighbor, parent.changeTime, window, dir)
           │     │     │
           │     │     ├── ScanPrefix("node:{id}:prop:") → discover all properties
           │     │     │
           │     │     ├── For each property: History(key, {From, To})
           │     │     │     Forward window: [parent.changeTime, parent.changeTime + window]
           │     │     │     Backward window: [parent.changeTime - window, parent.changeTime]
           │     │     │
           │     │     └── Return earliest (fwd) or latest (bwd) change time
           │     │
           │     ├── Compute confidence:
           │     │     timeFactor = exp(-0.25 × deltaMicros / windowMicros)
           │     │     edgeWeight = edge "weight" property (default 1.0)
           │     │     confidence = parent.confidence × timeFactor × edgeWeight
           │     │
           │     ├── Apply MinConfidence filter
           │     │
           │     └── Add to results, enqueue for next BFS level
           │
           └── Skip already-visited nodes (cycle prevention)
```

#### Confidence Formula

The confidence of a causal link is the product of three factors:

```
confidence = parent_confidence × time_factor × edge_weight
```

Where:
- **time_factor** = `exp(-k × delta/window)` with decay constant `k = 0.25`
- **edge_weight** = custom weight property on the edge (default 1.0)
- **parent_confidence** = cumulative confidence from the trigger node (root = 1.0)

This exponential decay ensures that events closer in time to their parent receive higher confidence scores, while events at the edge of the window approach zero confidence.

| Example | Delta/Window | Time Factor |
|---------|-------------|-------------|
| Pump → Press (22min/60min) | 0.367 | 0.912 |
| Redis → Checkout (3min/15min) | 0.200 | 0.951 |
| Near-instant (0/any) | 0.000 | 1.000 |
| At window edge (1.0) | 1.000 | 0.779 |

#### Per-Hop Windowing

Each BFS hop evaluates property changes relative to the **parent node's change time**, not the original trigger. This enables multi-hop causal chains where each propagation takes time:

```
Trigger (t=0) ──22min──► Node A (t=22min) ──3min──► Node B (t=25min)
                         │                          │
                    window=[0, 30min]          window=[22min, 52min]
```

#### Forward vs Backward Direction

- **Forward (CausalForward):** Follows outgoing edges. Detects property changes *after* the parent's change. Used for impact analysis: "What was affected by this event?"
- **Backward (CausalBackward):** Follows incoming edges. Detects property changes *before* the parent's change. Used for root-cause analysis: "What caused this event?"

### NalaQL Engine

A Cypher-inspired query language with temporal extensions. The engine consists of a lexer, Pratt parser, AST, and executor.

See [docs/nalaql.md](nalaql.md) for the full language specification.

### gRPC API

External interface for clients. Provides all KV and graph operations, plus server-streaming watch/subscription support.

See [docs/api.md](api.md) for the full API reference.

### RAFT Consensus

Cluster replication using [hashicorp/raft](https://github.com/hashicorp/raft). The RAFT log doubles as the WAL to avoid double-writes.

#### RAFT-Log = WAL (No Double-Write)

A key design decision: in clustered mode, the RAFT log **replaces** the separate WAL file. Every write is serialized as a `RaftCommand`, submitted through RAFT consensus, and applied to the in-memory store by the FSM. This eliminates the overhead of writing to both a WAL and the RAFT log.

```
Client Write: Set("key", "value")
  │
  ├──1──► Leader: Clock.Now() → pre-assign HLC timestamp
  │
  ├──2──► Leader: Create RaftCommand{CmdSet, [{key, value, HLC}]}
  │
  ├──3──► Leader: raft.Apply(command) → replicate to followers
  │         │
  │         ├── Follower 1: AppendEntries → persist to RAFT log
  │         └── Follower 2: AppendEntries → persist to RAFT log
  │
  ├──4──► Majority ACK → committed
  │
  └──5──► All Nodes: FSM.Apply(log) → store.SetWithHLC(key, HLC, value)
                                        │
                                        ├── Update in-memory index
                                        └── Append to version log
```

The Store is created with `NewWithoutWAL()` in clustered mode, since the RAFT log provides the durability guarantee.

#### FSM Apply Flow

The FSM processes committed RAFT log entries by deserializing the command and applying each KV operation to the store:

```go
func (f *FSM) Apply(l *raft.Log) interface{} {
    cmd := UnmarshalCommand(l.Data)
    for _, op := range cmd.Ops {
        f.store.SetWithHLCAndFlags(op.Key, op.HLC, op.Value, op.Tombstone, op.BlobRef)
    }
    return &ApplyResponse{}
}
```

All command types (Set, Delete, CreateNode, CreateEdge, BatchSet) use the same mechanism: a list of pre-computed KV operations with pre-assigned HLC timestamps.

#### Command Types

| CommandType | Description | KV Operations |
|-------------|-------------|---------------|
| `CmdSet` | Write a single key-value pair | 1 op |
| `CmdDelete` | Tombstone a key | 1 op (tombstone flag) |
| `CmdCreateNode` | Create a graph node | N ops (meta + props + adj lists) |
| `CmdCreateEdge` | Create a graph edge | N ops (meta + props + adj updates) |
| `CmdBatchSet` | Atomic multi-key write | N ops |

#### Consistency Levels

| Level | Behavior |
|-------|----------|
| `Eventual` | Read from local store on any node (may be stale) |
| `Linearizable` | Read from leader with `VerifyLeader()` check |

#### Snapshot Contents

Snapshots capture the complete in-memory state using gob encoding:

- **Version log:** All keys with their full version history (`map[string][]VersionExport`)
- **Index state:** Rebuilt from the version log during restore (latest version per key)
- **Graph state:** Included automatically (nodes, edges, adjacency lists are KV entries)

The snapshot format enables efficient new-node catchup without replaying the entire RAFT log.

### Blob Store

Values exceeding 64 KiB are transparently offloaded to a content-addressed blob store. See [Blob Store](#blob-store) above for the full architecture and GC lifecycle.

### TTL Management

NalaDB enforces eager key expiration using a two-tier approach:

#### Timing Wheel

A circular-buffer timing wheel provides O(1) scheduling and expiry at millisecond granularity:

```
Schedule("session:abc", 5s)
  │
  └──► slot = (currentSlot + ceil(5s / resolution)) % wheelSize
         │
         └──► Insert key into slot's linked list
               │
               └──► Tick loop advances currentSlot every TickInterval (100ms default)
                      │
                      └──► All keys in current slot → DeleteFunc(key) writes tombstone
```

- **Wheel size:** 65536 slots (configurable)
- **Resolution:** 100ms per slot (configurable)
- **Max TTL:** `wheelSize × resolution` = ~109 minutes at defaults
- **Cancel:** O(1) removal via key→slot map

#### Expiry Scanner

A background scanner runs at `ScanInterval` (default 1 minute), checking each segment's `maxTimestamp` against configured retention policies. If a segment's newest record is older than the applicable TTL, the entire segment is removed without per-record tombstone writes.

#### Leader-Only Execution

Both the timing wheel and the expiry scanner run exclusively on the RAFT leader node. This prevents duplicate tombstone writes and ensures consistent expiration behavior across the cluster.

### Multi-Tenancy

NalaDB supports multi-tenant deployments with key-prefix isolation, per-tenant quotas, and token-bucket rate limiting.

#### Key-Prefix Schema

All keys are automatically prefixed with `{tenant_id}:` in storage:

```
{tenant}:node:{node_id}:meta          → Node metadata
{tenant}:node:{node_id}:prop:{name}   → Node property
{tenant}:edge:{edge_id}:meta          → Edge metadata
{tenant}:edge:{edge_id}:prop:{name}   → Edge property
{tenant}:graph:adj:{node_id}:out      → Outgoing edge IDs
{tenant}:graph:adj:{node_id}:in       → Incoming edge IDs
```

The prefix is injected transparently by a gRPC interceptor based on the `x-tenant-id` metadata header. Clients see unprefixed keys in requests and responses. If no header is provided, the `default` tenant is used.

#### Per-Tenant Resource Management

Each tenant has an independent:

- **Graph instance** created on-demand with the tenant's key prefix
- **QuotaTracker** enforcing `MaxNodes` and `MaxEdges` limits
- **RateLimiter** using a token-bucket algorithm for write rate limiting

Exceeding quotas or rate limits returns a `RESOURCE_EXHAUSTED` gRPC status.

See [docs/configuration.md](configuration.md) for tenant configuration options.

## Component Interaction Diagram

```
┌──────────────────────────────────────────────────────────────┐
│                       gRPC Server                            │
│  ┌──────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │KVService │  │ GraphService │  │ WatchService │           │
│  └────┬─────┘  └──────┬───────┘  └──────┬───────┘           │
│       │                │                 │                    │
│  ┌────▼────────────────▼─────────────────▼───────┐           │
│  │              Tenant Manager                    │           │
│  │    (Key-Prefix Isolation, Quota, Rate Limit)   │           │
│  └────┬───────────────────────────────────┬──────┘           │
│       │                                   │                  │
│  ┌────▼──────┐                     ┌──────▼──────┐           │
│  │  Store    │◄────────────────────│   Graph     │           │
│  │ (KV CRUD) │                     │ (Node/Edge) │           │
│  └──┬──┬──┬──┘                     └─────────────┘           │
│     │  │  │                                                  │
│  ┌──▼┐ │ ┌▼───────┐  ┌──────────┐  ┌────────┐              │
│  │WAL│ │ │Version  │  │  Blob    │  │ KeyMeta│              │
│  │   │ │ │  Log    │  │  Store   │  │Registry│              │
│  └───┘ │ └────┬────┘  └──────────┘  └────────┘              │
│     ┌──▼──────┤──┐                                           │
│     │ Sharded │  │    ┌────────────┐   ┌──────────┐          │
│     │  Index  │  │    │  Segment   │   │   TTL    │          │
│     │(256 shd)│  │    │  Manager   │   │ Manager  │          │
│     └─────────┘  │    └─────┬──────┘   └──────────┘          │
│                  │          │                                 │
│            ┌─────▼──────────▼─────┐                           │
│            │   Evictor (Tiered)   │                           │
│            │  hot RAM ↔ cold disk │                           │
│            └──────────────────────┘                           │
│                                                              │
│  ┌───────────────────────────────────────────────┐           │
│  │          RAFT Consensus (hashicorp/raft)       │           │
│  │   FSM │ Snapshots │ Leader Election            │           │
│  └───────────────────────────────────────────────┘           │
│                                                              │
│  ┌───────────────────┐  ┌────────────────────────┐           │
│  │  NalaQL Engine    │  │  Prometheus Metrics     │           │
│  │ Lexer→Parser→Exec │  │  Counters/Histograms   │           │
│  └───────────────────┘  └────────────────────────┘           │
│                                                              │
│  ┌───────────────────────────────────────────────┐           │
│  │           Hybrid Logical Clock                 │           │
│  │  48-bit Wall-Time │ 4-bit NodeID │ 12-bit Log  │           │
│  └───────────────────────────────────────────────┘           │
└──────────────────────────────────────────────────────────────┘
```
