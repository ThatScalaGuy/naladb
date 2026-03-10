# NalaDB gRPC API Reference

> This document describes the gRPC API for NalaDB. All services are defined in `api/proto/naladb/v1/`.

## Connection

NalaDB listens on port `7301` by default. All services use plaintext gRPC (TLS will be added with clustering support).

```bash
# Start the server
./bin/naladb -addr :7301
```

## Services Overview

| Service | Description |
| ------------- | ----------------------------------------- |
| KVService | Temporal key-value CRUD + History streaming |
| GraphService | Graph node/edge CRUD + Traverse/Causal streaming |
| WatchService | Live subscription on key changes |
| Health | Standard gRPC health checking |

---

## KVService

### Set

Writes a key-value pair. Returns the assigned HLC timestamp.

```protobuf
rpc Set(SetRequest) returns (SetResponse);
```

**Request:**
| Field | Type | Description |
| ----- | ----- | ---------------------- |
| key | string | The key to write |
| value | bytes | The value to store |

**Response:**
| Field | Type | Description |
| --------- | ------ | ---------------------------------- |
| timestamp | uint64 | HLC timestamp assigned to the write |

**Example (grpcurl):**
```bash
grpcurl -plaintext -d '{"key":"sensor:temp_1","value":"MjUuMA=="}' \
  localhost:7301 naladb.v1.KVService/Set
```

**Example (Go):**
```go
resp, err := kvClient.Set(ctx, &pb.SetRequest{
    Key:   "sensor:temp_1",
    Value: []byte("25.0"),
})
fmt.Printf("Written at HLC: %d\n", resp.Timestamp)
```

### Get

Reads the current value of a key.

```protobuf
rpc Get(GetRequest) returns (GetResponse);
```

**Request:**
| Field | Type | Description |
| ------------- | ---------------- | ------------------------------------------------- |
| key | string | The key to read |
| consistency | ConsistencyLevel | EVENTUAL (default), LINEARIZABLE, or BOUNDED_STALE |
| max_stale_ms | int64 | For BOUNDED_STALE: max staleness in ms (default: 5000) |

**Response:**
| Field | Type | Description |
| --------- | ------ | --------------------------------- |
| key | string | The requested key |
| value | bytes | The current value |
| timestamp | uint64 | HLC timestamp of the value |
| found | bool | Whether the key exists |

**Consistency Levels:**
- `CONSISTENCY_LEVEL_EVENTUAL` (0) -- Read from any node (default). Lowest latency, may be slightly stale.
- `CONSISTENCY_LEVEL_LINEARIZABLE` (1) -- Read through RAFT leader with ReadIndex protocol (VerifyLeader + Barrier). Guarantees the most recent committed value.
- `CONSISTENCY_LEVEL_BOUNDED_STALE` (2) -- Read locally if the follower's last leader contact is within `max_stale_ms`, otherwise forward to leader. Good for near-real-time reads with low latency.

**Leader Routing:**

All requests (reads and writes) that cannot be served locally are transparently forwarded to the RAFT leader. Every response includes `x-naladb-leader` metadata header with the current leader's gRPC address. If the request was forwarded, `x-naladb-forwarded: true` is also included.

**Consistency Level Recommendations:**
| Use Case | Level | max_stale_ms |
|----------|-------|-------------|
| Dashboards, monitoring | EVENTUAL | -- |
| Sensor data queries | BOUNDED_STALE | 5000 |
| Read-after-write | LINEARIZABLE | -- |
| Financial operations | LINEARIZABLE | -- |

### GetAt

Reads the value of a key at a specific point in time using binary search over the version log.

```protobuf
rpc GetAt(GetAtRequest) returns (GetAtResponse);
```

**Request:**
| Field | Type | Description |
| ----- | ------ | -------------------------------- |
| key | string | The key to read |
| at | uint64 | HLC timestamp for point-in-time lookup |

**Response:** Same as `GetResponse`.

**Example (grpcurl):**
```bash
grpcurl -plaintext -d '{"key":"sensor:temp_1","at":1234567890}' \
  localhost:7301 naladb.v1.KVService/GetAt
```

**Example (Go):**
```go
resp, err := kvClient.GetAt(ctx, &pb.GetAtRequest{
    Key: "sensor:temp_1",
    At:  prevTimestamp,
})
if resp.Found {
    fmt.Printf("Value at %d: %s\n", resp.Timestamp, resp.Value)
}
```

### Delete

Marks a key as deleted by writing a tombstone.

```protobuf
rpc Delete(DeleteRequest) returns (DeleteResponse);
```

**Request:**
| Field | Type | Description |
| ----- | ------ | --------------- |
| key | string | The key to delete |

**Response:**
| Field | Type | Description |
| --------- | ------ | ----------------------------------- |
| timestamp | uint64 | HLC timestamp of the tombstone |

**Example (grpcurl):**
```bash
grpcurl -plaintext -d '{"key":"sensor:temp_1"}' \
  localhost:7301 naladb.v1.KVService/Delete
```

**Example (Go):**
```go
resp, err := kvClient.Delete(ctx, &pb.DeleteRequest{
    Key: "sensor:temp_1",
})
fmt.Printf("Deleted at HLC: %d\n", resp.Timestamp)
```

### History (Server-Side Streaming)

Streams the version history of a key. Results are sent as individual messages.

```protobuf
rpc History(HistoryRequest) returns (stream HistoryEntry);
```

**Request:**
| Field | Type | Description |
| ------- | ------ | ------------------------------------------ |
| key | string | The key to query |
| from | uint64 | Start of HLC range (0 = no lower bound) |
| to | uint64 | End of HLC range (0 = no upper bound) |
| limit | int32 | Maximum entries to return (0 = unlimited) |
| reverse | bool | Return entries in reverse chronological order |

**Stream Response (per entry):**
| Field | Type | Description |
| --------- | ------ | -------------------------------- |
| timestamp | uint64 | HLC timestamp of this version |
| value | bytes | The value at this version |
| tombstone | bool | Whether this is a deletion marker |

**Example (grpcurl):**
```bash
grpcurl -plaintext -d '{"key":"sensor:temp_1","limit":10}' \
  localhost:7301 naladb.v1.KVService/History
```

---

## GraphService

### CreateNode

Creates a new graph node with an auto-generated UUID v7 identifier.

```protobuf
rpc CreateNode(CreateNodeRequest) returns (CreateNodeResponse);
```

**Request:**
| Field | Type | Description |
| ---------- | --------------- | ---------------------- |
| type | string | Node type label |
| properties | map<string, bytes> | Initial properties |

**Response:**
| Field | Type | Description |
| ---------- | ------ | ------------------------------ |
| id | string | Generated UUID v7 |
| valid_from | uint64 | HLC timestamp of creation |
| valid_to | uint64 | End of validity (MaxHLC = open) |

**Example (grpcurl):**
```bash
grpcurl -plaintext -d '{"type":"sensor","properties":{"location":"cGxhbnQtQQ=="}}' \
  localhost:7301 naladb.v1.GraphService/CreateNode
```

**Example (Go):**
```go
resp, err := graphClient.CreateNode(ctx, &pb.CreateNodeRequest{
    Type: "sensor",
    Properties: map[string][]byte{
        "location": []byte("plant-A"),
        "model":    []byte("TMP-3000"),
    },
})
fmt.Printf("Created node: %s\n", resp.Id)
```

### GetNode

Retrieves the current metadata for a node.

```protobuf
rpc GetNode(GetNodeRequest) returns (GetNodeResponse);
```

**Example (grpcurl):**
```bash
grpcurl -plaintext -d '{"id":"01234567-89ab-cdef-0123-456789abcdef"}' \
  localhost:7301 naladb.v1.GraphService/GetNode
```

**Example (Go):**
```go
resp, err := graphClient.GetNode(ctx, &pb.GetNodeRequest{
    Id: nodeID,
})
fmt.Printf("Node type: %s, deleted: %v\n", resp.Type, resp.Deleted)
```

### UpdateNode

Updates properties of an existing node.

```protobuf
rpc UpdateNode(UpdateNodeRequest) returns (UpdateNodeResponse);
```

**Example (grpcurl):**
```bash
grpcurl -plaintext -d '{"id":"<node-id>","properties":{"status":"Y3JpdGljYWw="}}' \
  localhost:7301 naladb.v1.GraphService/UpdateNode
```

**Example (Go):**
```go
_, err := graphClient.UpdateNode(ctx, &pb.UpdateNodeRequest{
    Id: nodeID,
    Properties: map[string][]byte{
        "status": []byte("critical"),
    },
})
```

### DeleteNode

Soft-deletes a node and cascades to all connected edges.

```protobuf
rpc DeleteNode(DeleteNodeRequest) returns (DeleteNodeResponse);
```

**Example (grpcurl):**
```bash
grpcurl -plaintext -d '{"id":"<node-id>"}' \
  localhost:7301 naladb.v1.GraphService/DeleteNode
```

### CreateEdge

Creates an edge between two nodes with temporal validity constraints.

```protobuf
rpc CreateEdge(CreateEdgeRequest) returns (CreateEdgeResponse);
```

**Request:**
| Field | Type | Description |
| ---------- | --------------- | ----------------------------------------- |
| from | string | Source node ID |
| to | string | Target node ID |
| relation | string | Edge relation type (e.g., "feeds", "monitors") |
| valid_from | uint64 | Start of edge validity (must be within both nodes' ranges) |
| valid_to | uint64 | End of edge validity |
| properties | map<string, bytes> | Edge properties |

**Response:**
| Field | Type | Description |
| ---------- | ------ | ------------------------------ |
| id | string | Generated edge UUID v7 |
| valid_from | uint64 | Start of validity |
| valid_to | uint64 | End of validity |

**Example (grpcurl):**
```bash
grpcurl -plaintext -d '{
  "from":"<node-id-1>","to":"<node-id-2>",
  "relation":"monitors","valid_from":0,"valid_to":18446744073709551615
}' localhost:7301 naladb.v1.GraphService/CreateEdge
```

**Example (Go):**
```go
resp, err := graphClient.CreateEdge(ctx, &pb.CreateEdgeRequest{
    From:      sourceNodeID,
    To:        targetNodeID,
    Relation:  "monitors",
    ValidFrom: 0,
    ValidTo:   math.MaxUint64,
    Properties: map[string][]byte{
        "weight": []byte("0.9"),
    },
})
fmt.Printf("Created edge: %s\n", resp.Id)
```

### GetEdge / UpdateEdge / DeleteEdge

Standard CRUD operations for edges. See proto definitions for details.

**Example (grpcurl):**
```bash
# Get edge
grpcurl -plaintext -d '{"id":"<edge-id>"}' \
  localhost:7301 naladb.v1.GraphService/GetEdge

# Update edge properties
grpcurl -plaintext -d '{"id":"<edge-id>","properties":{"weight":"MC45"}}' \
  localhost:7301 naladb.v1.GraphService/UpdateEdge

# Delete edge
grpcurl -plaintext -d '{"id":"<edge-id>"}' \
  localhost:7301 naladb.v1.GraphService/DeleteEdge
```

### Traverse (Server-Side Streaming)

Performs a BFS traversal with temporal filtering. Only edges valid at the specified HLC timestamp are followed.

```protobuf
rpc Traverse(TraverseRequest) returns (stream TraverseResult);
```

**Request:**
| Field | Type | Description |
| ------------------- | --------- | --------------------------------------------- |
| start | string | Starting node ID |
| at | uint64 | HLC timestamp for temporal filtering |
| max_depth | int32 | Maximum traversal depth (0 = unlimited) |
| direction | Direction | OUTGOING (default), INCOMING, or BOTH |
| relation_filter | []string | Restrict to these edge relations |
| include_properties | bool | Include node properties in results |

**Stream Response:**
| Field | Type | Description |
| ------------ | --------------- | ------------------------------ |
| node_id | string | Reached node ID |
| depth | int32 | Hops from start node |
| via_edge | string | Edge ID used to reach this node |
| via_relation | string | Edge relation type |
| properties | map<string, bytes> | Node properties (if requested) |

**Example (grpcurl):**
```bash
grpcurl -plaintext -d '{
  "start":"<node-id>","at":0,"max_depth":3,
  "direction":0,"include_properties":true
}' localhost:7301 naladb.v1.GraphService/Traverse
```

**Example (Go):**
```go
stream, err := graphClient.Traverse(ctx, &pb.TraverseRequest{
    Start:             startNodeID,
    At:                0, // current time
    MaxDepth:          3,
    Direction:         pb.Direction_DIRECTION_OUTGOING,
    IncludeProperties: true,
})
for {
    result, err := stream.Recv()
    if err == io.EOF {
        break
    }
    fmt.Printf("Node: %s (depth=%d, via=%s)\n",
        result.NodeId, result.Depth, result.ViaRelation)
}
```

### Causal (Server-Side Streaming)

Performs causal dependency traversal, detecting property changes within time windows and computing confidence scores.

```protobuf
rpc Causal(CausalRequest) returns (stream CausalResult);
```

**Request:**
| Field | Type | Description |
| --------------- | --------------- | ------------------------------------------------ |
| trigger | string | Trigger node ID |
| at | uint64 | HLC timestamp of the trigger event |
| max_depth | int32 | Maximum causal chain depth |
| window_micros | int64 | Per-hop time window in microseconds |
| direction | CausalDirection | FORWARD (impact) or BACKWARD (root cause) |
| min_confidence | double | Minimum confidence threshold (0 = no filter) |
| relation_filter | []string | Restrict to these edge relations |

**Stream Response:**
| Field | Type | Description |
| ------------ | -------- | ------------------------------------------- |
| node_id | string | Causally linked node ID |
| depth | int32 | Hops from trigger |
| delta_micros | int64 | Time difference from trigger (microseconds) |
| confidence | double | Cumulative causal confidence (0.0 to 1.0) |
| causal_path | []string | Ordered node IDs from trigger to this node |
| via_edge | string | Edge ID used |
| via_relation | string | Edge relation type |
| change_time | uint64 | HLC timestamp of detected property change |

**Example (grpcurl):**
```bash
grpcurl -plaintext -d '{
  "trigger":"<node-id>","at":0,"max_depth":5,
  "window_micros":900000000,"direction":0,"min_confidence":0.5
}' localhost:7301 naladb.v1.GraphService/Causal
```

**Example (Go):**
```go
stream, err := graphClient.Causal(ctx, &pb.CausalRequest{
    Trigger:       triggerNodeID,
    At:            triggerTimestamp,
    MaxDepth:      5,
    WindowMicros:  15 * 60 * 1_000_000, // 15 minutes in microseconds
    Direction:     pb.CausalDirection_CAUSAL_DIRECTION_FORWARD,
    MinConfidence: 0.5,
})
for {
    result, err := stream.Recv()
    if err == io.EOF {
        break
    }
    fmt.Printf("Node: %s, confidence=%.3f, depth=%d\n",
        result.NodeId, result.Confidence, result.Depth)
}
```

---

## WatchService

### Watch (Server-Side Streaming)

Subscribes to live updates on specified keys. The stream remains open until the client disconnects.

```protobuf
rpc Watch(WatchRequest) returns (stream WatchEvent);
```

**Request:**
| Field | Type | Description |
| ----- | -------- | ---------------------- |
| keys | []string | Keys to watch |

**Stream Response:**
| Field | Type | Description |
| --------- | ------ | ---------------------------------- |
| key | string | The changed key |
| value | bytes | New value (nil for deletions) |
| timestamp | uint64 | HLC timestamp of the change |
| deleted | bool | Whether this is a deletion |

**Example (grpcurl):**
```bash
grpcurl -plaintext -d '{"keys":["sensor:temp_1:prop:value"]}' \
  localhost:7301 naladb.v1.WatchService/Watch
```

**Example (Go):**
```go
stream, err := watchClient.Watch(ctx, &pb.WatchRequest{
    Keys: []string{"sensor:temp_1:prop:value"},
})
for {
    event, err := stream.Recv()
    if err != nil {
        break
    }
    fmt.Printf("Key: %s, Value: %s, Deleted: %v\n",
        event.Key, event.Value, event.Deleted)
}
```

---

## Streaming Protocol

All streaming RPCs use **server-side streaming**: the client sends a single request and receives a stream of responses.

- **History**: Streams all matching version entries, then closes.
- **Traverse**: Streams all reachable nodes, then closes.
- **Causal**: Streams all causal results, then closes.
- **Watch**: Keeps the stream open indefinitely, sending events as they occur.

## Error Handling

All errors follow standard gRPC status codes:

| Code | Usage |
| ---------------------- | ----------------------------------------- |
| `INVALID_ARGUMENT` | Missing or invalid request fields |
| `NOT_FOUND` | Node or edge does not exist |
| `FAILED_PRECONDITION` | Not the RAFT leader (triggers transparent forwarding) |
| `UNAVAILABLE` | No leader available for forwarding |
| `INTERNAL` | Unexpected server error |

## Health Checking

NalaDB implements the standard [gRPC Health Checking Protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md).

```bash
grpcurl -plaintext localhost:7301 grpc.health.v1.Health/Check
# {"status":"SERVING"}
```

## Reflection

gRPC server reflection is enabled, allowing tools like `grpcurl` and `grpcui` to discover services without proto files.

```bash
# List all services
grpcurl -plaintext localhost:7301 list

# Describe a service
grpcurl -plaintext localhost:7301 describe naladb.v1.KVService
```
