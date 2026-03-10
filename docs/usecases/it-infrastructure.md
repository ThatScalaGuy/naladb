# Use Case: IT Infrastructure / Cascading Failure Analysis

> Model service dependencies, detect cascading failures, and perform root cause analysis.

## Overview

An operations team manages a microservices architecture. Services, databases, and load balancers are modeled as graph nodes. Dependencies are edges. When a failure occurs, NalaDB's causal traversal identifies the cascade path -- which upstream service failure caused downstream outages.

**NalaDB solves this** by modeling the service mesh as a temporal graph, storing health status changes as versioned KV entries, and using backward causal traversal to automatically trace root causes through the dependency chain.

## Data Model

### Nodes

| Node Type | Description | Properties |
|-----------|-------------|------------|
| `service` | Microservice | version, port, replicas |
| `database` | Database instance | engine, max_connections |
| `cache` | Cache layer | eviction_policy, max_memory |
| `loadbalancer` | Load balancer | algorithm, max_rate |

### Edges

| Relation | From | To | Description |
|----------|------|----|-------------|
| `DEPENDS_ON` | service | service/database/cache | Runtime dependency |
| `ROUTES_TO` | loadbalancer | service | Traffic routing |
| `READS_FROM` | service | database/cache | Data source |
| `WRITES_TO` | service | database | Data sink |

### Properties (Time-Series)

```
node:{service_id}:prop:status        → string (healthy/degraded/down)
node:{service_id}:prop:latency_p99   → float (ms)
node:{service_id}:prop:error_rate    → float (%)
node:{service_id}:prop:cpu_usage     → float (%)
```

## NalaQL Queries

### 1. Find All Dependencies of a Service

```sql
MATCH (s:service)-[d:depends_on]->(dep)
WHERE s.id = "api_gateway"
RETURN s.id, dep.id, d.weight
```

### 2. Root Cause Analysis (Backward Causal)

When the API gateway goes down, trace back through dependencies to find the root cause:

```sql
CAUSAL FROM api_gateway
AT "2025-03-08T14:32:05Z"
DEPTH 5
WINDOW 5m
WHERE confidence > 0.5
RETURN path, delta, confidence
ORDER BY confidence DESC
```

### 3. Forward Impact Analysis

When Redis goes down, find all services that will be affected:

```sql
CAUSAL FROM redis_cache
AT "2025-03-08T14:30:00Z"
DEPTH 5
WINDOW 10m
WHERE confidence > 0.3
RETURN path, delta, confidence
ORDER BY delta ASC
```

### 4. Service Health History

Track the health status of a service over the last hour:

```sql
GET history("node:auth_service:prop:status")
FROM "2025-03-08T13:30:00Z" TO "2025-03-08T14:30:00Z"
```

### 5. Detect Services with High Error Rates

Find services with anomalous write patterns in their error_rate metric:

```sql
META "node:*:prop:error_rate"
WHERE avg_value > 5.0
RETURN key, avg_value, stddev_value, write_rate
LIMIT 20
```

### 6. Compare Service Topology Between Deployments

Detect dependency changes after a deployment:

```sql
DIFF (s:service)-[d:depends_on]->(dep)
BETWEEN "2025-03-07" AND "2025-03-08"
RETURN edge_id, from, to, change
```

### 7. Multi-Hop Dependency Chain

Find all transitive dependencies of the API gateway (3 hops deep):

```sql
MATCH (gw:service)-[d1:depends_on]->(s1:service)-[d2:depends_on]->(s2)
WHERE gw.id = "api_gateway"
RETURN gw.id, s1.id, s2.id
```

## Architecture Integration

```
┌──────────────────────────────────────────┐
│       Kubernetes / Cloud Platform        │
│  (Service Discovery, Health Probes)      │
└───────────────┬──────────────────────────┘
                │ gRPC Set (status changes)
                ▼
┌──────────────────────────────────────────┐
│              NalaDB Cluster              │
│  ┌────────────┐  ┌───────────────────┐   │
│  │ Temporal KV │  │  Graph Layer     │   │
│  │ (health)   │  │  (dependencies)   │   │
│  └────────────┘  └───────────────────┘   │
└───────────────┬──────────────────────────┘
                │ NalaQL queries
                ▼
┌──────────────────────────────────────────┐
│         Incident Management              │
│  (PagerDuty, Grafana OnCall, custom)     │
└──────────────────────────────────────────┘
```

## Quick Start

Run the IT infrastructure example:

```bash
# Start NalaDB server
./bin/naladb

# Run the example
go run examples/it-infrastructure/main.go
```

## See Also

- [Full Use Case Description](UseCase-05-IT-Infrastruktur.md)
