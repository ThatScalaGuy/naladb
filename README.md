# NalaDB

**The database that remembers everything -- and knows what caused what.**

NalaDB is a temporal key-value graph database with causal ordering. It stores every version of every value, models relationships between entities as a graph, and uses Hybrid Logical Clocks to track what caused what. One query can time-travel to last Tuesday, follow dependency chains, and tell you _why_ your pump failed -- with a confidence score.

```sql
-- "The API gateway is down. What broke it?"
CAUSAL FROM api_gateway
AT "2025-03-08T14:32:05Z"
DEPTH 5 WINDOW 5m
WHERE confidence > 0.5
RETURN path, delta, confidence
ORDER BY confidence DESC
```

```
| confidence | delta | path                                         |
+------------+-------+----------------------------------------------+
| 0.92       | 4.2s  | redis_main -> checkout_svc -> api_gateway     |
| 0.78       | 8.1s  | redis_main -> session_svc -> api_gateway      |
```

Redis went down first. The checkout service felt it 4 seconds later. The API gateway followed. One query. No log-grepping. No Slack threads.

## Why NalaDB Exists

Databases make you choose:

| You need...                | Tool                  | But you lose...                 |
| -------------------------- | --------------------- | ------------------------------- |
| Time-series data           | InfluxDB, TimescaleDB | Relationships between things    |
| Relationships & traversals | Neo4j, DGraph         | Version history and time-travel |
| Fast key-value lookups     | Redis, etcd           | Both of the above               |

Real systems don't fit in one box. A temperature sensor has readings (time-series), belongs to a machine (graph), and its spike 20 minutes ago _caused_ the pump to overheat (causality). To answer "why did the pump fail?", you currently need three databases, a data pipeline, and a prayer.

NalaDB puts all three in one engine. Every write is versioned. Every entity can have relationships. Every change has a timestamp that preserves causal ordering. When something goes wrong, you ask the database directly.

## The Idea: KV + Graph + Temporal in One Engine

This isn't three databases duct-taped together. The graph layer is _projected onto_ the temporal KV store -- nodes and edges are just keys with a naming convention:

```
node:pump_01:meta              -> {"type": "pump", "id": "pump_01"}
node:pump_01:prop:temperature  -> 85.3   (version 1, t=100)
                               -> 87.1   (version 2, t=200)
                               -> 91.7   (version 3, t=300)
edge:e001:meta                 -> {"from": "sensor_01", "to": "pump_01", "relation": "monitors"}
graph:adj:pump_01:in           -> ["e001", "e002"]
```

This means graph data automatically inherits every temporal capability -- versioning, time-travel, causal ordering, downsampling -- for free. There's no separate graph storage engine. No sync jobs. No eventual consistency between layers.

### What Each Concept Brings to the Table

| Capability                                             | KV alone         | Graph alone | Temporal alone      | NalaDB (all three)               |
| ------------------------------------------------------ | ---------------- | ----------- | ------------------- | -------------------------------- |
| "What is the current temperature?"                     | Fast O(1) lookup | --          | --                  | Fast O(1) lookup                 |
| "What was it yesterday at 3pm?"                        | --               | --          | Point-in-time query | Point-in-time query              |
| "What does this sensor monitor?"                       | --               | Traversal   | --                  | Traversal with temporal filter   |
| "What did the topology look like last week?"           | --               | --          | --                  | Time-travel graph query          |
| "Why did the pump fail?"                               | --               | --          | --                  | Causal traversal with confidence |
| "What changed between deployments?"                    | --               | --          | --                  | DIFF query on graph state        |
| "Show me 24h of readings, downsampled for a dashboard" | --               | --          | History + LTTB      | History + LTTB                   |
| "Which sensors have anomalous write patterns?"         | --               | --          | --                  | META query with statistics       |

The "--" cells aren't just missing features. They're _impossible_ without combining all three paradigms. A pure graph database can tell you the pump is connected to the sensor -- but not what the sensor reading was at the time of failure. A pure time-series database can show you the temperature spike -- but not which downstream equipment was affected. Only the combination answers the full question.

### Why Not Just Use Separate Databases?

You can. Many teams do. Here's what it costs:

1. **Data synchronization**: You need ETL pipelines to keep the graph DB and time-series DB in sync. Every pipeline is a potential point of failure, delay, and inconsistency.
2. **Query federation**: Your "root cause analysis" query becomes three separate queries across three databases, joined in application code. Latency compounds. Bugs multiply.
3. **Causal ordering is lost**: When you write to Redis and Neo4j separately, you lose the happens-before relationship between events. You can't definitively say "A caused B" because the timestamps come from different clocks.
4. **Operational overhead**: Three databases means three backup strategies, three monitoring dashboards, three failure modes, three teams to train.

NalaDB eliminates all of this with a single binary, a single query language, and a single source of truth for both structure and history.

### Honest Trade-offs

NalaDB isn't a drop-in replacement for every database. Here's what you're signing up for:

**Pros:**

- One system for temporal data, relationships, and causality
- Every write is automatically versioned -- no schema migration needed for "add history"
- Causal ordering via HLC means "what caused what" is answerable, not just "what happened when"
- Sub-microsecond current-value reads, sub-millisecond graph traversals
- Tiered storage: hot data in RAM, cold data on disk, transparent to queries
- RAFT consensus for high availability with automatic leader election
- Multi-tenant by design (key-prefix isolation, per-tenant quotas and rate limits)
- Single binary deployment, Docker Compose for clusters

**Cons:**

- Not a general-purpose OLTP database -- no SQL, no joins in the relational sense, no transactions across arbitrary keys
- Query language (NalaQL) is Cypher-inspired but purpose-built -- teams familiar with SQL will need to learn new syntax
- 16-node cluster limit (4-bit node ID in the HLC) -- designed for small-to-medium clusters, not planet-scale
- The graph layer stores adjacency lists as JSON arrays -- mutation-heavy graphs with thousands of edges per node will see write amplification
- Young project with a single maintainer -- battle-tested in one production environment, not yet in hundreds
- No built-in TLS (planned), no authentication layer (use a sidecar or service mesh)

## Quick Start

### Docker Compose (Recommended)

Start a 3-node cluster with Prometheus + Grafana:

```bash
make cluster
```

Check that everything is up:

```bash
docker compose -f docker/docker-compose.yml ps
```

**Service endpoints:**

| Service                | Endpoint                               |
| ---------------------- | -------------------------------------- |
| NalaDB node 0 (leader) | `localhost:7301` (gRPC)                |
| NalaDB node 1          | `localhost:7302` (gRPC)                |
| NalaDB node 2          | `localhost:7303` (gRPC)                |
| Grafana dashboard      | `http://localhost:3000` (admin/naladb) |
| Prometheus             | `http://localhost:9093`                |

### Write Your First Data

```bash
# Store a sensor reading
grpcurl -plaintext -d '{"key":"sensor:temp_1","value":"MjUuMA=="}' \
  localhost:7301 naladb.v1.KVService/Set

# Read it back
grpcurl -plaintext -d '{"key":"sensor:temp_1"}' \
  localhost:7301 naladb.v1.KVService/Get

# Create a graph node
grpcurl -plaintext -d '{"type":"sensor","properties":{"location":"cGxhbnQtQQ=="}}' \
  localhost:7301 naladb.v1.GraphService/CreateNode
```

### Use the CLI

```bash
# Build the CLI
go build -o bin/naladb-cli ./cmd/naladb-cli

# Connect to NalaDB
./bin/naladb-cli -addr localhost:7301
```

Interactive session:

```
NalaDB CLI (connected to localhost:7301)
Type a NalaQL query and press Enter. Use ';' to end multi-line queries.
Commands: \q quit  \h help

naladb> MATCH (s:sensor)-[r:monitors]->(m:machine)
   ...> WHERE m.type = "hydraulic_press"
   ...> RETURN s.id, m.id, r.weight
   ...> LIMIT 5;
| m.id            | r.weight | s.id            |
+-----------------+----------+-----------------+
| hydraulic_001   | 0.95     | temp_sensor_01  |
| hydraulic_001   | 0.87     | vibr_sensor_03  |
| hydraulic_002   | 0.91     | temp_sensor_07  |
(3 rows, 1.204ms)

naladb> CAUSAL FROM temp_sensor_01
   ...> AT "2025-03-08T14:32:05Z"
   ...> DEPTH 3 WINDOW 5m
   ...> WHERE confidence > 0.5
   ...> RETURN path, delta, confidence
   ...> ORDER BY confidence DESC;
| confidence | delta | path                                      |
+------------+-------+-------------------------------------------+
| 0.92       | 4.2s  | temp_sensor_01 -> hydraulic_001 -> alert_7 |
| 0.78       | 8.1s  | temp_sensor_01 -> hydraulic_001            |
(2 rows, 3.817ms)

naladb> GET history("node:temp_sensor_01:prop:temperature")
   ...> FROM "2025-03-08" TO "2025-03-09"
   ...> DOWNSAMPLE LTTB(5);
| hlc                | key                                  | value |
+--------------------+--------------------------------------+-------+
| 1741392000000000   | node:temp_sensor_01:prop:temperature | 21.3  |
| 1741413600000000   | node:temp_sensor_01:prop:temperature | 24.7  |
| 1741435200000000   | node:temp_sensor_01:prop:temperature | 28.1  |
| 1741456800000000   | node:temp_sensor_01:prop:temperature | 25.4  |
| 1741478400000000   | node:temp_sensor_01:prop:temperature | 22.0  |
(5 rows, 2.561ms)

naladb> DIFF (m:material)-[s:supplied_by]->(sup:supplier)
   ...> BETWEEN "2025-01-01" AND "2025-03-01"
   ...> RETURN edge_id, from, to, change;
| change  | edge_id | from         | to           |
+---------+---------+--------------+--------------+
| added   | e_1042  | steel_batch3 | supplier_new |
| removed | e_0871  | steel_batch1 | supplier_old |
(2 rows, 5.102ms)

naladb> META "node:sensor_*:prop:temperature"
   ...> WHERE write_rate > 10.0
   ...> RETURN key, write_rate, avg_interval, stddev_value;
| avg_interval | key                                  | stddev_value | write_rate |
+--------------+--------------------------------------+--------------+------------+
| 1.2s         | node:sensor_01:prop:temperature      | 3.42         | 50.5       |
| 0.8s         | node:sensor_07:prop:temperature      | 5.17         | 75.2       |
(2 rows, 0.893ms)

naladb> SHOW NODES WHERE type = "sensor" LIMIT 3;
| id              | properties       | type   |
+-----------------+------------------+--------+
| temp_sensor_01  | prop_value, unit | sensor |
| vibr_sensor_03  | prop_value       | sensor |
| temp_sensor_07  | prop_value, unit | sensor |
(3 rows, 0.287ms)

naladb> \q
Bye!
```

Run a one-off query from the shell:

```bash
./bin/naladb-cli -e 'MATCH (n:sensor) RETURN n.id LIMIT 10'
```

## NalaQL at a Glance

NalaQL is a Cypher-inspired query language with temporal extensions. Here's one query per capability:

```sql
-- Graph pattern matching with temporal filter
MATCH (s:sensor)-[r:monitors]->(m:machine)
AT "2025-03-08T14:32:05Z"
WHERE r.weight > 0.5
RETURN s.id, m.id

-- Causal root-cause analysis
CAUSAL FROM pump_01
AT "2025-03-08T14:32:05Z"
DEPTH 5 WINDOW 30m
WHERE confidence > 0.5
RETURN path, delta, confidence

-- Graph with live KV data (bridging graph and time-series)
MATCH (s:Sensor)-[:BELONGS_TO]->(m:Machine)
WHERE m.criticality = "critical"
FETCH latest(s.readingKey) AS s.value
RETURN s.sensorId, s.metric, s.value

-- Temporal diff between two points in time
DIFF (m:material)-[s:supplied_by]->(sup:material)
BETWEEN "2025-01-01" AND "2025-03-01"
RETURN edge_id, from, to, change

-- Time-series history with visual downsampling
GET history("node:temp_sensor_305a:prop:temperature")
FROM "2025-03-08" TO "2025-03-09"
DOWNSAMPLE LTTB(288)

-- Key statistics (write rate, stddev, cardinality)
META "node:sensor_*:prop:temperature"
WHERE write_rate > 10.0
RETURN key, write_rate, stddev_value
```

See [docs/nalaql.md](docs/nalaql.md) for the full language specification.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                       gRPC API                           │
│  KVService │ GraphService │ WatchService │ Health         │
├──────────────────────────────────────────────────────────┤
│                     NalaQL Engine                        │
│              Lexer -> Parser -> Executor                  │
├────────────────────┬─────────────────────────────────────┤
│    Graph Layer     │       Temporal KV Store              │
│  (Node/Edge CRUD,  │  (Set, Get, GetAt, Delete,          │
│   Traverse, Causal) │   History, BatchSet)                │
├────────────────────┴─────────────────────────────────────┤
│           Tiered Storage (Hot RAM + Cold Disk)            │
│                                                          │
│  HOT:  vlog (last N versions) | Index | Sorted Keys      │
│  COLD: Segments (Bloom+Sparse) | WAL  | Blob Store       │
│        Background Evictor (configurable retention)       │
├──────────────────────────────────────────────────────────┤
│  RAFT Consensus | TTL Manager | Multi-Tenancy | Metrics  │
├──────────────────────────────────────────────────────────┤
│              Hybrid Logical Clock (HLC)                  │
│       48-bit Wall-Time | 4-bit NodeID | 12-bit Logical   │
└──────────────────────────────────────────────────────────┘
```

For details, see [docs/architecture.md](docs/architecture.md).

## Use Cases

Each use case includes a documentation page and a runnable example:

| Use Case                      | Documentation                                                                      | Example                                          |
| ----------------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------ |
| Predictive Maintenance        | [docs/usecases/predictive-maintenance.md](docs/usecases/predictive-maintenance.md) | `go run examples/predictive-maintenance/main.go` |
| Fraud Detection               | [docs/usecases/fraud-detection.md](docs/usecases/fraud-detection.md)               | `go run examples/fraud-detection/main.go`        |
| Supply Chain Transparency     | [docs/usecases/supply-chain.md](docs/usecases/supply-chain.md)                     | `go run examples/supply-chain/main.go`           |
| Smart Building / Digital Twin | [docs/usecases/smart-building.md](docs/usecases/smart-building.md)                 | `go run examples/smart-building/main.go`         |
| IT Infrastructure             | [docs/usecases/it-infrastructure.md](docs/usecases/it-infrastructure.md)           | `go run examples/it-infrastructure/main.go`      |

## Performance

| Operation            | Target            | Notes                                       |
| -------------------- | ----------------- | ------------------------------------------- |
| Current-Value Get    | < 1 us            | In-memory O(1) via sharded index            |
| Point-in-Time (hot)  | < 1 us            | In-memory binary search                     |
| Point-in-Time (cold) | < 100 us          | Bloom + Sparse Index + disk seek            |
| Write Throughput     | > 500k/s per node | Batch fsync, lock-free sharded index        |
| History (last=100)   | < 10 ms           | In-memory or merged RAM + segment streaming |
| 3-Hop Traversal      | < 50 ms           | In-memory adjacency lists                   |
| Causal (depth=5)     | < 100 ms          | HLC ordering + BFS                          |

```bash
make bench
```

## Installation

### From Source

Requires Go 1.24+ and `protoc` (for proto generation).

```bash
git clone https://github.com/thatscalaguy/naladb.git
cd naladb
make build
./bin/naladb
```

### Docker

```bash
# Server
docker run -p 7301:7301 -p 9090:9090 thatscalaguy/naladb:latest

# CLI (connect to a running NalaDB instance)
docker run --rm -it --network host thatscalaguy/naladb-cli:latest -addr localhost:7301
```

### Binary

Download pre-built binaries from the [releases page](https://github.com/thatscalaguy/naladb/releases).

```bash
./naladb -addr :7301 -wal-dir data/wal -node-id 0
```

## Configuration

See [docs/configuration.md](docs/configuration.md) for the full YAML reference covering:

- Cluster settings (node ID, addresses, data directories)
- RAFT consensus (peers, election timeout, snapshots)
- HLC clock skew protection (`max_clock_skew`)
- Storage (WAL, segments, compaction, memory retention)
- Blob store (content-addressed storage, GC)
- Retention policies (per-prefix TTL, downsampling)
- Multi-tenancy (quotas, rate limiting)
- Prometheus metrics

### Memory Retention (Tiered Storage)

NalaDB supports configurable memory retention via `MaxMemoryVersions`. This controls how many versions per key are kept in RAM -- older versions are served from on-disk segments.

```yaml
storage:
  max_memory_versions: 100 # Keep last 100 versions per key in RAM
  eviction_interval: "30s" # Background eviction cycle
  eviction_batch_size: 10000 # Keys processed per eviction cycle
```

| Setting                        | Behavior                                               |
| ------------------------------ | ------------------------------------------------------ |
| `max_memory_versions: 0`       | Pure in-memory (default, all versions in RAM)          |
| `max_memory_versions: 1`       | Minimal RAM: only latest version in vlog, rest on disk |
| `max_memory_versions: 100`     | Balanced: recent history in RAM, older on disk         |
| `max_memory_versions: 1000000` | Effectively in-memory for most workloads               |

See [docs/storage-model.md](docs/storage-model.md) for capacity planning and the tiered storage architecture.

## Documentation

| Document                                              | Description                                                                             |
| ----------------------------------------------------- | --------------------------------------------------------------------------------------- |
| [Integration Guide](docs/integration.md)              | How to write data and run queries from any language (Go examples, gRPC code generation) |
| [Architecture](docs/architecture.md)                  | How the layers fit together (HLC, WAL, Index, Segments, Graph, Causal, RAFT)            |
| [NalaQL](docs/nalaql.md)                              | Full query language spec with EBNF grammar                                              |
| [API Reference](docs/api.md)                          | gRPC API with grpcurl and Go examples                                                   |
| [Causal Analysis](docs/causal-analysis.md)            | Deep dive on the causal traversal algorithm                                             |
| [Clustering](docs/clustering.md)                      | RAFT setup, consistency levels, clock skew protection, Docker deployment                |
| [Configuration](docs/configuration.md)                | Full YAML reference                                                                     |
| [Storage Model](docs/storage-model.md)                | Tiered storage, eviction, capacity planning                                             |
| [Concept: Why KV + Graph + Temporal](docs/concept.md) | The design philosophy and comparison with alternatives                                  |

## Background

NalaDB started over 10 years ago as a freelance project -- a predictive maintenance system built for a manufacturing customer. The first version used Ruby, Redis, and PostgreSQL. As the requirements grew, it was rewritten in Scala with Actors for better concurrency. The final rewrite in Go distilled the core idea into what NalaDB is today: a temporal-graph-kv store purpose-built for tracking causality over time.

The original customer recently allowed the project to be open-sourced. All customer-specific code has been removed and the git history was reset for a clean start. **NalaDB is actively used in production**, though development happens at a project-maintainer pace rather than a full-time team.

The original domain was industrial predictive maintenance (sensors, machines, failure chains), and that remains a strong use case. However, the current focus is shifting toward **root cause analysis for software cloud infrastructure** -- tracing cascading failures across services, dependencies, and infrastructure components.

## Contributing

```bash
make build          # Build
make test           # Run tests with race detection
make lint           # Run linter
make bench          # Run benchmarks
make proto          # Generate protobuf stubs
make cluster        # Start 3-node cluster
```

1. Fork the repository
2. Create a feature branch
3. Run `make test && make lint` before submitting
4. Open a pull request

## License

[MIT](LICENSE)
