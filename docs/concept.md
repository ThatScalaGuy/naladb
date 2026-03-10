# Why KV + Graph + Temporal in One Database

> This document explains the design philosophy behind NalaDB: why combining key-value storage, graph relationships, and temporal versioning in a single engine produces capabilities that are impossible with any one paradigm alone.

## The Problem With Specialized Databases

Modern data infrastructure tends to push teams toward purpose-built databases. Need fast lookups? Redis. Time-series? InfluxDB. Relationships? Neo4j. Each tool is excellent at its job. But real-world problems don't respect category boundaries.

Consider a manufacturing plant with 500 sensors monitoring 80 machines across 12 production lines. Every second, sensors emit readings. Machines depend on each other -- the extruder feeds the dryer, the dryer feeds the fryer, the fryer feeds the packaging line. When the packaging line rejects product, the root cause might be a humidity spike in the raw material silo three hours ago. To find that root cause, you need:

1. **The current reading** of each sensor (key-value)
2. **The history** of readings over the past hours (time-series)
3. **The dependency graph** between machines and sensors (graph)
4. **The causal chain** showing which change propagated to which downstream effect (temporal ordering + graph traversal)

With separate databases, you'd query InfluxDB for the time-series, Neo4j for the dependency graph, and then manually correlate timestamps in application code to reconstruct the causal chain. Three databases. Three query languages. Three consistency models. Application-level joins. And the causal chain? You're guessing, because InfluxDB's wall-clock timestamps and Neo4j's commit timestamps have no formal ordering relationship.

NalaDB solves this by putting all three paradigms in a single engine with a single clock.

## The Three Paradigms

### Key-Value: The Foundation

At its core, NalaDB is a key-value store. Every piece of data -- sensor readings, node metadata, edge properties, adjacency lists -- is a key mapping to a value. This gives:

- **O(1) current-value lookups** via a sharded in-memory index (256 shards, FNV-1a hashing)
- **Simple data model**: no schema migrations, no column definitions, no ORM mapping
- **Flexible value types**: strings, numbers, JSON, binary blobs up to 100 MiB

A pure KV store like Redis gives you this. It's fast and simple. But it answers exactly one question: "What is the value of key X right now?" It doesn't tell you what the value was yesterday, what other keys are related to it, or why it changed.

### Temporal: Everything Has a History

Every write in NalaDB creates a new version stamped with a Hybrid Logical Clock (HLC) timestamp. The previous value isn't overwritten -- it's preserved in a version log. This gives:

- **Point-in-time queries**: "What was the temperature at 3pm?" (`GetAt`)
- **Full history**: "Show me all temperature readings this week" (`History`)
- **Time-range queries**: "What happened between midnight and 6am?" (`DURING`)
- **Statistical metadata**: per-key write rate, mean, standard deviation, cardinality
- **Downsampling**: LTTB, min-max, and average strategies for dashboard-friendly results

A pure time-series database like InfluxDB gives you this (and often with more sophisticated time-series aggregations). But it treats each metric as an independent stream. There's no notion of "this metric is related to that metric" or "a change in metric A caused a change in metric B."

### Graph: Everything Is Connected

NalaDB models entities as nodes and their relationships as directed edges. The graph layer supports:

- **Typed nodes and edges**: `(sensor)-[:monitors]->(machine)`
- **BFS/DFS traversal**: find all connected entities within N hops
- **Shortest path**: find the shortest route between two nodes
- **Temporal filtering**: only follow edges that were valid at a given timestamp
- **Multi-hop pattern matching**: `(a)-[r1]->(b)-[r2]->(c)` in a single query

A pure graph database like Neo4j gives you this. But it stores the "current" state of the graph. If you want to know what the graph looked like last week, you need to implement your own versioning. If you want to know what caused a node's property to change, you need to correlate with an external time-series system.

## What the Combination Enables

When you fuse all three paradigms in a single engine, you get capabilities that are fundamentally impossible with any one paradigm alone -- and genuinely hard to achieve even with all three as separate systems.

### 1. Time-Travel Graph Queries

"What did the service dependency graph look like before last Friday's deployment?"

```sql
MATCH (s:service)-[d:depends_on]->(dep)
AT "2025-03-07T00:00:00Z"
RETURN s.id, dep.id
```

This isn't just querying the graph. It's querying **the graph as it existed at a specific point in time**. Every edge and node in NalaDB has a validity range (`valid_from`, `valid_to`). Traversals respect these ranges. If a service dependency was added on Thursday and you query for Wednesday's state, that edge doesn't exist in the result.

**Why this requires all three paradigms:**
- KV provides the storage substrate (edges are keys)
- Temporal provides the versioning (each edge has timestamped versions)
- Graph provides the traversal semantics (BFS with temporal filters)

### 2. Causal Traversal With Confidence Scoring

"What caused the API gateway to go down?"

```sql
CAUSAL FROM api_gateway
AT "2025-03-08T14:32:05Z"
DEPTH 5 WINDOW 5m
WHERE confidence > 0.5
RETURN path, delta, confidence
ORDER BY confidence DESC
```

This is NalaDB's signature capability. It combines:

1. **Graph topology**: follow dependency edges to find connected services
2. **Temporal evidence**: check if each connected service actually changed within the time window
3. **Causal ordering**: use HLC timestamps to determine the direction of causation (A changed before B, not after)
4. **Confidence scoring**: weight by time proximity (changes closer in time are more likely causal) and domain-encoded edge weights

The result is a ranked list of causal chains with confidence scores. Not "here are all the services that are down" (monitoring can tell you that), but "here is the sequence of failures, ordered by likelihood, with the root cause at the top."

**Why this is impossible with separate databases:**
- A graph DB knows the topology but not the timing
- A time-series DB knows the timing but not the topology
- Even with both, application-level correlation can't use HLC ordering to establish happens-before relationships, because the two databases use independent clocks

### 3. Temporal Diff on Graph Structure

"What supplier relationships changed between Q1 and Q2?"

```sql
DIFF (m:material)-[s:supplied_by]->(sup:supplier)
BETWEEN "2025-01-01" AND "2025-04-01"
RETURN edge_id, from, to, change
```

This compares the entire graph topology at two points in time and returns the structural differences -- edges added, removed, or changed. It's like `git diff` for your graph.

**Why this requires the combination:**
- Graph provides the structural semantics (nodes, edges, patterns)
- Temporal provides the two snapshots being compared
- KV provides efficient storage of both snapshots (they're just versioned keys)

### 4. Live KV Data Enriched by Graph Context

"Show me the current temperature for every sensor on machine M03."

```sql
MATCH (s:Sensor)-[:BELONGS_TO]->(m:Machine)
WHERE m.machineId = "M03"
FETCH latest(s.readingKey) AS s.value
RETURN s.sensorId, s.metric, s.unit, s.value
```

This query uses the graph to find which sensors belong to a machine, then reaches into the KV store to fetch each sensor's latest reading. The `FETCH` clause bridges graph and KV in a single query -- no application-level join.

### 5. Anomaly Detection via Write Statistics

"Which sensors are behaving abnormally?"

```sql
META "node:sensor_*:prop:*"
WHERE stddev_value > 5.0 AND write_rate > 10.0
RETURN key, write_rate, avg_value, stddev_value
```

Every write updates per-key statistics (Welford's algorithm for running mean/stddev, EWMA for write rate, HyperLogLog for cardinality). These statistics are maintained inline -- no separate aggregation job. Combined with graph context, you can ask "which sensors on machine M03 have anomalous readings" in a single query pipeline.

## How the Integration Works Technically

The key insight: **the graph layer is projected onto the KV store**. Nodes and edges aren't stored in a separate graph engine. They're stored as regular keys:

```
node:{id}:meta              -> JSON metadata
node:{id}:prop:{name}       -> property value (versioned)
edge:{id}:meta              -> JSON metadata
edge:{id}:prop:{name}       -> property value (versioned)
graph:adj:{node_id}:out     -> JSON array of outgoing edge IDs (versioned)
graph:adj:{node_id}:in      -> JSON array of incoming edge IDs (versioned)
```

This is not a limitation -- it's a deliberate design choice that creates a force multiplier:

| Inherited Capability | How |
|---|---|
| Every node property has full version history | It's a KV key -> automatic versioning |
| Point-in-time graph queries | `GetAt` on adjacency lists and edge metadata |
| Graph statistics (write rate, stddev) | KeyMeta registry tracks all keys, including graph keys |
| Tiered storage for graph data | Old graph versions evict to disk like any other key |
| WAL durability for graph mutations | Graph writes go through the same WAL |
| RAFT replication for graph data | Graph operations are RAFT commands like any KV write |
| Multi-tenant graph isolation | Key-prefix partitioning applies to graph keys too |
| TTL on graph data | Retention policies apply to graph keys |
| Blob store for large properties | Node properties > 64 KiB transparently use blob store |

None of these capabilities required separate implementation for the graph layer. They came for free from the KV substrate.

## Comparison With Alternatives

### NalaDB vs. Neo4j + InfluxDB

| Aspect | Neo4j + InfluxDB | NalaDB |
|---|---|---|
| Time-travel graph queries | Manual versioning or separate snapshots | Built-in (every edge/node is versioned) |
| Causal chain analysis | Application-level correlation | Native CAUSAL query with confidence scoring |
| Consistency between layers | Eventual (two separate systems) | Strong (single HLC clock, single WAL) |
| Query language | Cypher + InfluxQL/Flux | NalaQL (one language for both) |
| Operational complexity | Two clusters, two backup strategies | One cluster, one binary |
| Graph query maturity | Cypher is industry-standard, highly optimized | NalaQL is younger, supports core patterns |
| Time-series aggregations | Extensive (moving averages, derivatives, etc.) | Basic (LTTB, min-max, avg downsampling) |
| Scale | Neo4j supports billions of nodes | Better suited for millions of nodes |

**Choose NalaDB when**: your primary need is understanding causality and temporal relationships across a connected system -- and operational simplicity matters.

**Choose Neo4j + InfluxDB when**: you need highly sophisticated graph algorithms (PageRank, community detection) or advanced time-series analytics (forecasting, anomaly detection) that go beyond what a single engine can optimize.

### NalaDB vs. TimescaleDB With Foreign Tables

| Aspect | TimescaleDB + foreign tables | NalaDB |
|---|---|---|
| Time-series features | Continuous aggregates, compression, hyperfunctions | History, downsampling, KeyMeta statistics |
| Graph traversal | Not native (recursive CTEs, limited) | Native BFS, shortest path, causal traversal |
| Causal ordering | Wall-clock timestamps only | HLC with happens-before guarantees |
| Schema flexibility | Requires table definitions | Schema-free KV (any key, any value) |
| SQL compatibility | Full PostgreSQL SQL | NalaQL (Cypher-inspired, purpose-built) |
| Ecosystem | pgvector, PostGIS, hundreds of extensions | Focused: temporal + graph + causality |

**Choose NalaDB when**: your system is inherently a graph (services depending on services, sensors monitoring machines) and you need causal chain analysis.

**Choose TimescaleDB when**: your primary workload is analytical SQL over time-series data with occasional relationship lookups.

### NalaDB vs. Custom Solution (Redis + PostgreSQL + Application Logic)

This is how NalaDB started. The original system used Ruby + Redis + PostgreSQL. Here's what drove the rewrite:

| Aspect | Redis + PostgreSQL + App Code | NalaDB |
|---|---|---|
| Versioning | Manual (append to history table) | Automatic (every write creates a version) |
| Causal ordering | Wall-clock correlation (unreliable across machines) | HLC with formal happens-before ordering |
| Graph traversal | Recursive SQL queries (slow, hard to maintain) | Native BFS with temporal filters |
| Data consistency | Two-phase commit or eventual consistency | Single-writer with RAFT replication |
| Root cause analysis | Hours of manual investigation | One CAUSAL query, 30 seconds |
| Maintenance | Three systems to upgrade, monitor, backup | One binary |

## When NalaDB Is NOT the Right Choice

Honest assessment of where NalaDB shouldn't be used:

- **Pure analytical workloads**: If you need GROUP BY, window functions, CTEs, or materialized views over terabytes of data, use a columnar database (ClickHouse, DuckDB, BigQuery).
- **OLTP with complex transactions**: If you need multi-table transactions with ACID guarantees, use PostgreSQL.
- **Social network-scale graphs**: If your graph has billions of nodes and you need PageRank or community detection, use a dedicated graph platform (Neo4j, TigerGraph).
- **Simple caching**: If you just need a fast cache with TTL, use Redis. NalaDB's versioning overhead is unnecessary.
- **Log aggregation**: If you need full-text search over logs, use Elasticsearch or Loki.

NalaDB shines in the intersection: systems where **entities are connected**, **state changes over time**, and **understanding causality matters**. Manufacturing floors. Service meshes. Supply chains. Building automation. Financial transaction networks. Anywhere the question isn't just "what happened?" but "why did it happen?"

## The Hybrid Logical Clock: Why It Matters

A subtle but critical aspect of NalaDB's design: every event is stamped with a Hybrid Logical Clock, not a wall-clock timestamp.

Wall-clock timestamps are unreliable in distributed systems. NTP can drift. Clocks can jump. Two events on different machines might get the same millisecond timestamp but occur in a definite order. Wall-clock correlation is the reason most "root cause analysis" tools give inconclusive results -- when your Redis timestamp says 14:32:05.123 and your checkout service timestamp says 14:32:05.124, you can't be sure which happened first, because the clocks might differ by 100ms.

NalaDB's HLC combines wall-clock time with a logical counter and a node ID:

```
┌──────────────────────┬────────┬─────────────┐
│  Wall-Time (48 bit)  │Node(4) │ Logical(12) │
│  us since epoch      │  ID    │  Counter    │
└──────────────────────┴────────┴─────────────┘
```

The HLC maintains the **happens-before** property: if event A causally precedes event B, then `HLC(A) < HLC(B)`. When a node receives a message from another node, it updates its clock to be at least as recent as the sender's clock plus one. This means:

- If Redis writes a value at HLC=1000 and sends a message to the checkout service, the checkout service's next write will have HLC >= 1001
- If the checkout service then sends a message to the API gateway, the API gateway's next write will have HLC >= 1002
- The causal chain Redis -> Checkout -> Gateway is preserved in the HLC ordering

This is what makes CAUSAL queries work. They don't just look for "events close in time." They look for events that respect the happens-before ordering, weighted by temporal proximity. The HLC is the formal mechanism that makes "A caused B" a meaningful statement, not just "A and B happened around the same time."

## Summary

NalaDB exists because the most interesting questions span multiple paradigms:

- "What is connected to what?" (graph)
- "What was the state at time T?" (temporal)
- "What caused this?" (graph + temporal + causal ordering)

By storing graph data as versioned KV entries under a unified HLC clock, NalaDB makes these cross-paradigm queries native, consistent, and fast. The trade-off is generality: NalaDB doesn't try to replace your analytical warehouse, your full-text search engine, or your OLTP database. It occupies a specific niche -- temporal-causal-graph workloads -- and executes well within that niche.
