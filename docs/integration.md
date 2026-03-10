# Integrating With NalaDB

> How to write data, run queries, and build applications on top of NalaDB from any language.

## Two Interfaces, One Database

NalaDB exposes everything through **gRPC**. There is no HTTP/REST API, no embedded library mode, and no native driver SDK. This is intentional -- gRPC gives you type-safe contracts, server-side streaming, and automatic code generation for every major language.

There are two ways to interact with NalaDB:

| Interface | Good for | How |
|---|---|---|
| **gRPC API** (typed RPCs) | Writing data, creating graph nodes/edges, reading KV values, streaming traversals | Call individual RPC methods like `KVService/Set`, `GraphService/CreateNode` |
| **NalaQL** (query language) | Pattern matching, causal analysis, time-travel, diffs, aggregations | Send a query string via `QueryService/Query` or use the CLI |

In practice, most applications use **gRPC for writes** and **NalaQL for reads**. The gRPC API gives you fine-grained control over individual operations. NalaQL gives you expressive, declarative queries that would require many RPC calls to compose manually.

```
Your Application
  │
  ├── Writes ───► gRPC API (KVService, GraphService)
  │                 Set, CreateNode, CreateEdge, UpdateNode, ...
  │
  └── Reads ────► NalaQL via gRPC (QueryService/Query)
                    MATCH, CAUSAL, DIFF, GET history, META, ...

                  -- or --

                  NalaQL via CLI (naladb-cli)
                    Interactive REPL or one-shot queries
```

## Generating a Client in Any Language

NalaDB's proto files are at `api/proto/naladb/v1/`. Use `protoc` (or `buf`) to generate a client in your language:

```
api/proto/naladb/v1/
├── naladb.proto    # All service and message definitions
└── types.proto     # Shared enums (ConsistencyLevel, Direction, CausalDirection)
```

### Example: Generate a Python Client

```bash
pip install grpcio grpcio-tools

python -m grpc_tools.protoc \
  -I api/proto \
  --python_out=./gen \
  --grpc_python_out=./gen \
  api/proto/naladb/v1/naladb.proto \
  api/proto/naladb/v1/types.proto
```

### Example: Generate a TypeScript Client

```bash
npm install @grpc/grpc-js @grpc/proto-loader
# or use buf, protoc-gen-ts, or connect-es
```

### Example: Generate a Rust Client

```bash
# Using tonic-build in build.rs
tonic_build::configure()
    .compile(&["api/proto/naladb/v1/naladb.proto"], &["api/proto"])
    .unwrap();
```

### Supported Languages

gRPC has official support for: **Go, Python, Java, C++, C#, Ruby, PHP, Dart, Kotlin, Swift, Objective-C**. Community implementations exist for Rust (tonic), TypeScript (nice-grpc, connect-es), Elixir, Haskell, and others.

If your language supports gRPC, it can talk to NalaDB. No NalaDB-specific SDK required.

## Quick Reference: What Goes Where

| Task | Use | Example |
|---|---|---|
| Write a sensor reading | `KVService/Set` | `Set("sensor:temp_1", "25.0")` |
| Read the current value | `KVService/Get` | `Get("sensor:temp_1")` |
| Read a value at a past time | `KVService/GetAt` | `GetAt("sensor:temp_1", hlc)` |
| Delete a key | `KVService/Delete` | `Delete("sensor:temp_1")` |
| Stream version history | `KVService/History` | `History("sensor:temp_1", from, to)` |
| Create a graph node | `GraphService/CreateNode` | `CreateNode("sensor", {name: "temp_1"})` |
| Create a graph edge | `GraphService/CreateEdge` | `CreateEdge(nodeA, nodeB, "monitors")` |
| Update node properties | `GraphService/UpdateNode` | `UpdateNode(id, {status: "critical"})` |
| BFS traversal | `GraphService/Traverse` | `Traverse(startID, at, depth=3)` |
| Causal analysis | `GraphService/Causal` | `Causal(triggerID, at, depth=5, window=5m)` |
| Subscribe to live changes | `WatchService/Watch` | `Watch(["sensor:temp_1"])` |
| Pattern matching query | `QueryService/Query` | `MATCH (s:sensor)-[r]->(m:machine) ...` |
| Causal query | `QueryService/Query` | `CAUSAL FROM pump_01 DEPTH 5 WINDOW 30m ...` |
| Time-travel diff | `QueryService/Query` | `DIFF (a)-[r]->(b) BETWEEN ... AND ...` |
| History with downsampling | `QueryService/Query` | `GET history("key") DOWNSAMPLE LTTB(100)` |
| Key statistics | `QueryService/Query` | `META "sensor_*" WHERE write_rate > 10` |

## Full Go Example: Building a Sensor Monitoring Application

This example shows a complete integration: connecting, creating a graph, writing time-series data, and querying with both gRPC and NalaQL.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "math"
    "time"

    pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func main() {
    // --- Connect -----------------------------------------------------------
    conn, err := grpc.NewClient("localhost:7301",
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    )
    if err != nil {
        panic(err)
    }
    defer conn.Close()

    kv := pb.NewKVServiceClient(conn)
    graph := pb.NewGraphServiceClient(conn)
    query := pb.NewQueryServiceClient(conn)
    ctx := context.Background()

    // --- Step 1: Build the graph (gRPC) ------------------------------------
    // Create nodes
    machine, _ := graph.CreateNode(ctx, &pb.CreateNodeRequest{
        Type: "machine",
        Properties: map[string][]byte{
            "machineId": []byte("press-01"),
            "type":      []byte("hydraulic_press"),
        },
    })
    fmt.Printf("Created machine: %s\n", machine.Id)

    sensor, _ := graph.CreateNode(ctx, &pb.CreateNodeRequest{
        Type: "sensor",
        Properties: map[string][]byte{
            "sensorId":   []byte("temp-01"),
            "unit":       []byte("celsius"),
            "readingKey": []byte("sensor:temp-01:reading"),
        },
    })
    fmt.Printf("Created sensor: %s\n", sensor.Id)

    // Create edge: sensor monitors machine
    edge, _ := graph.CreateEdge(ctx, &pb.CreateEdgeRequest{
        From:     sensor.Id,
        To:       machine.Id,
        Relation: "MONITORS",
        ValidTo:  math.MaxUint64,
        Properties: map[string][]byte{
            "weight": []byte("0.95"),
        },
    })
    fmt.Printf("Created edge: %s\n", edge.Id)

    // --- Step 2: Write sensor readings (gRPC) ------------------------------
    readings := []string{"22.1", "22.3", "23.5", "28.7", "45.2", "78.9"}
    for _, val := range readings {
        resp, _ := kv.Set(ctx, &pb.SetRequest{
            Key:   "sensor:temp-01:reading",
            Value: []byte(val),
        })
        fmt.Printf("Wrote %s at HLC=%d\n", val, resp.Timestamp)
    }

    // --- Step 3: Query with NalaQL (gRPC QueryService) ---------------------

    // Find all sensors monitoring hydraulic presses
    fmt.Println("\n--- MATCH query ---")
    runQuery(ctx, query, `
        MATCH (s:sensor)-[r:MONITORS]->(m:machine)
        WHERE m.type = "hydraulic_press"
        RETURN s.sensorId, m.machineId, r.weight
    `)

    // Get reading history with downsampling
    fmt.Println("\n--- History query ---")
    runQuery(ctx, query, `
        GET history("sensor:temp-01:reading")
        LAST 10
    `)

    // Check key statistics
    fmt.Println("\n--- META query ---")
    runQuery(ctx, query, `
        META "sensor:*:reading"
        RETURN key, write_rate, avg_value, stddev_value
    `)
}

// runQuery executes a NalaQL query via gRPC and prints the results.
func runQuery(ctx context.Context, client pb.QueryServiceClient, nalaql string) {
    stream, err := client.Query(ctx, &pb.QueryRequest{Query: nalaql})
    if err != nil {
        fmt.Printf("Query error: %v\n", err)
        return
    }
    rowCount := 0
    for {
        row, err := stream.Recv()
        if err == io.EOF {
            break
        }
        if err != nil {
            fmt.Printf("Stream error: %v\n", err)
            break
        }
        rowCount++
        fmt.Printf("  Row %d: %v\n", rowCount, row.Columns)
    }
    fmt.Printf("(%d rows)\n", rowCount)
}
```

### Key points in this example:

1. **Connection**: Standard gRPC client. No NalaDB-specific SDK needed.
2. **Graph setup via gRPC**: `CreateNode` and `CreateEdge` give you back IDs and HLC timestamps.
3. **Data ingestion via gRPC**: `KVService/Set` writes versioned values. Each call returns the HLC timestamp.
4. **Queries via NalaQL**: `QueryService/Query` sends a NalaQL string and streams back rows as `map<string, string>`.

## Writing Data (gRPC)

All writes go through typed gRPC RPCs. This gives you compile-time type safety, explicit error handling, and the HLC timestamp of every write.

### KV Writes

```go
// Simple key-value write
resp, err := kv.Set(ctx, &pb.SetRequest{
    Key:   "sensor:temp-01:reading",
    Value: []byte("25.3"),
})
// resp.Timestamp = HLC timestamp of this write

// Delete (writes a tombstone -- history is preserved)
delResp, err := kv.Delete(ctx, &pb.DeleteRequest{
    Key: "sensor:temp-01:reading",
})
```

### Graph Writes

```go
// Create a node (ID is auto-generated as UUID v7)
node, err := graph.CreateNode(ctx, &pb.CreateNodeRequest{
    Type: "sensor",
    Properties: map[string][]byte{
        "sensorId":   []byte("temp-01"),
        "readingKey": []byte("sensor:temp-01:reading"),
    },
})

// Update node properties (creates a new version, old values preserved)
_, err = graph.UpdateNode(ctx, &pb.UpdateNodeRequest{
    Id: node.Id,
    Properties: map[string][]byte{
        "status": []byte("warning"),
    },
})

// Create an edge between two nodes
edge, err := graph.CreateEdge(ctx, &pb.CreateEdgeRequest{
    From:     sensorID,
    To:       machineID,
    Relation: "MONITORS",
    ValidTo:  math.MaxUint64, // open-ended validity
    Properties: map[string][]byte{
        "weight": []byte("0.9"),
    },
})
```

### Multi-Tenancy

Pass the tenant ID as gRPC metadata. The server transparently prefixes all keys:

```go
import "google.golang.org/grpc/metadata"

// All operations in this context are scoped to "acme-corp"
md := metadata.Pairs("x-tenant-id", "acme-corp")
tenantCtx := metadata.NewOutgoingContext(ctx, md)

kv.Set(tenantCtx, &pb.SetRequest{
    Key:   "sensor:temp-01:reading",  // stored as "acme-corp:sensor:temp-01:reading"
    Value: []byte("25.0"),
})
```

## Querying Data (NalaQL)

For reads, NalaQL is almost always more convenient than raw gRPC calls. A single NalaQL query can express what would take 10+ individual RPC calls.

### Via gRPC (QueryService)

Every NalaQL query goes through a single RPC:

```protobuf
service QueryService {
  rpc Query(QueryRequest) returns (stream QueryRow);
}
```

The request is a query string. The response is a stream of rows, where each row is a `map<string, string>` of column name to value.

```go
stream, err := query.Query(ctx, &pb.QueryRequest{
    Query: `MATCH (s:sensor)-[:MONITORS]->(m:machine)
            WHERE m.type = "hydraulic_press"
            RETURN s.sensorId, m.machineId`,
})
for {
    row, err := stream.Recv()
    if err == io.EOF { break }
    if err != nil { /* handle error */ }

    fmt.Println(row.Columns["s.sensorId"], row.Columns["m.machineId"])
}
```

### Via CLI (naladb-cli)

The CLI is useful for exploration, debugging, and ad-hoc queries:

```bash
# Interactive REPL
./bin/naladb-cli -addr localhost:7301

# One-shot query from the shell
./bin/naladb-cli -addr localhost:7301 -e 'SHOW NODES LIMIT 10'

# Pipe queries
echo 'MATCH (n:sensor) RETURN n.id LIMIT 5' | ./bin/naladb-cli
```

### Common Query Patterns

**Graph pattern matching** -- find entities by type and relationship:

```sql
MATCH (s:sensor)-[r:MONITORS]->(m:machine)
WHERE m.type = "hydraulic_press"
RETURN s.sensorId, m.machineId, r.weight
LIMIT 20
```

**Graph + live KV data** -- enrich graph results with current readings:

```sql
MATCH (s:Sensor)-[:BELONGS_TO]->(m:Machine)
WHERE m.machineId = "M03"
FETCH latest(s.readingKey) AS s.value
RETURN s.sensorId, s.metric, s.value, s.unit
```

**Causal root-cause analysis** -- trace what caused a failure:

```sql
CAUSAL FROM "pump-01"
AT "2025-03-08T14:32:05Z"
DEPTH 5 WINDOW 30m
WHERE confidence > 0.5
RETURN path, delta, confidence
ORDER BY confidence DESC
```

**Time-travel** -- query the graph at a past point in time:

```sql
MATCH (s:service)-[d:DEPENDS_ON]->(dep)
AT "2025-03-07T00:00:00Z"
RETURN s.id, dep.id
```

**Temporal diff** -- what changed between two points:

```sql
DIFF (m:material)-[s:supplied_by]->(sup:supplier)
BETWEEN "2025-01-01" AND "2025-03-01"
RETURN edge_id, from, to, change
```

**History with downsampling** -- time-series for dashboards:

```sql
GET history("sensor:temp-01:reading")
FROM "2025-03-08" TO "2025-03-09"
DOWNSAMPLE LTTB(288)
```

**Key statistics** -- anomaly detection via inline stats:

```sql
META "sensor:*:reading"
WHERE stddev_value > 5.0
RETURN key, write_rate, avg_value, stddev_value
```

## Streaming: Watch for Live Updates

The WatchService provides real-time push notifications when keys change:

```go
stream, err := watch.Watch(ctx, &pb.WatchRequest{
    Keys: []string{
        "sensor:temp-01:reading",
        "sensor:vibr-01:reading",
    },
})
for {
    event, err := stream.Recv()
    if err != nil { break }

    fmt.Printf("Key %s changed to %s (deleted=%v)\n",
        event.Key, string(event.Value), event.Deleted)
}
```

This is a long-lived server-side stream. The connection stays open and NalaDB pushes events as they happen. Use this for:

- Real-time dashboards
- Alerting pipelines
- Event-driven architectures (react to state changes)

## Typical Application Architecture

### Sensor Data Pipeline

```
IoT Sensors / PLCs
  │
  │  gRPC Set (per reading)
  ▼
NalaDB Cluster (3 nodes, RAFT)
  │
  ├── Dashboard App ──── QueryService/Query ──── NalaQL
  │     "Show me sensor history for machine M03"
  │
  ├── Alerting Service ── WatchService/Watch ──── streaming
  │     "Notify me when temperature > 80"
  │
  └── Investigation Tool ── QueryService/Query ── NalaQL
        "What caused the pump failure at 14:32?"
```

### Microservice Dependency Tracker

```
Service Mesh / Kubernetes
  │
  │  gRPC CreateNode, CreateEdge, Set (health status)
  ▼
NalaDB Cluster
  │
  ├── Incident Dashboard ── QueryService/Query ──── NalaQL
  │     CAUSAL FROM api_gateway DEPTH 5 WINDOW 5m
  │
  ├── Topology Viewer ──── QueryService/Query ──── NalaQL
  │     MATCH (s:service)-[d:DEPENDS_ON]->(dep) RETURN ...
  │
  └── Change Auditor ───── QueryService/Query ──── NalaQL
        DIFF (s:service)-[d]->(dep) BETWEEN ... AND ...
```

## Error Handling

NalaDB uses standard gRPC status codes:

| Code | Meaning | Your action |
|---|---|---|
| `OK` | Success | -- |
| `INVALID_ARGUMENT` | Bad request (missing key, malformed query) | Fix the request |
| `NOT_FOUND` | Node/edge doesn't exist | Check the ID |
| `RESOURCE_EXHAUSTED` | Tenant quota or rate limit exceeded | Back off or upgrade tenant tier |
| `FAILED_PRECONDITION` | Not the RAFT leader | Automatic: request is transparently forwarded |
| `UNAVAILABLE` | No leader available | Retry after backoff (cluster might be electing) |
| `INTERNAL` | Server bug | Report the error |

NalaQL parse errors are returned as `INVALID_ARGUMENT` with the error position:

```
parse error at line 1, column 17: unexpected token WHERE (expected ), got WHERE)
```

## Consistency Levels for Reads

When reading via `KVService/Get`, you can control the freshness guarantee:

```go
// Fast but potentially stale (default)
kv.Get(ctx, &pb.GetRequest{
    Key: "sensor:temp-01:reading",
    Consistency: pb.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL,
})

// Guaranteed latest value (extra round-trip to leader)
kv.Get(ctx, &pb.GetRequest{
    Key: "sensor:temp-01:reading",
    Consistency: pb.ConsistencyLevel_CONSISTENCY_LEVEL_LINEARIZABLE,
})

// Read locally if within 5 seconds of leader, else forward
kv.Get(ctx, &pb.GetRequest{
    Key: "sensor:temp-01:reading",
    Consistency: pb.ConsistencyLevel_CONSISTENCY_LEVEL_BOUNDED_STALE,
    MaxStaleMs: 5000,
})
```

| Level | Latency | Guarantee | Use for |
|---|---|---|---|
| EVENTUAL | Lowest | May be slightly stale | Dashboards, monitoring |
| BOUNDED_STALE | Low (usually local) | Stale by at most N ms | Sensor displays, near-real-time |
| LINEARIZABLE | +1 RTT | Guaranteed latest | Read-after-write, financial operations |

NalaQL queries currently use eventual consistency. For linearizable reads, use the typed gRPC API.

## gRPC Reflection

NalaDB has gRPC server reflection enabled. You can explore the API without proto files:

```bash
# List all services
grpcurl -plaintext localhost:7301 list

# Describe a service
grpcurl -plaintext localhost:7301 describe naladb.v1.KVService

# Describe a message
grpcurl -plaintext localhost:7301 describe naladb.v1.CreateNodeRequest
```

This also means tools like **grpcui** (web-based gRPC client) work out of the box:

```bash
grpcui -plaintext localhost:7301
```

## See Also

- [API Reference](api.md) -- full proto message definitions with grpcurl examples
- [NalaQL Specification](nalaql.md) -- complete query language grammar
- [Causal Analysis](causal-analysis.md) -- how CAUSAL queries work internally
- [Configuration](configuration.md) -- YAML reference for server settings
- [Clustering](clustering.md) -- RAFT setup, consistency levels, Docker deployment
