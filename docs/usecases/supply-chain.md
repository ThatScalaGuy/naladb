# Use Case: Supply Chain Transparency

> Track goods through supply chain stages with full audit trail and provenance tracking.

## Overview

A manufacturer tracks products from raw materials through production, shipping, and delivery. Each stage transition is recorded with temporal metadata. The supply chain is modeled as a directed graph where causal queries can trace the full provenance of any product.

**NalaDB solves this** by modeling the entire supply chain as a temporal graph where every stage transition is versioned. Impact analysis uses graph traversal to trace affected products downstream, while DIFF queries detect supply chain topology changes over time.

## Data Model

### Nodes

| Node Type | Description | Properties |
|-----------|-------------|------------|
| `material` | Raw material batch | origin, grade, supplier |
| `production` | Production lot | quantity, quality_score |
| `shipment` | Shipping container | carrier, route, eta |
| `warehouse` | Storage facility | location, capacity |
| `delivery` | Final delivery | destination, status |

### Edges

| Relation | From | To | Description |
|----------|------|----|-------------|
| `USED_IN` | material | production | Raw material used in production |
| `PRODUCED` | production | shipment | Production lot shipped |
| `SHIPPED_VIA` | shipment | warehouse | Shipment routed to warehouse |
| `DELIVERED_TO` | warehouse | delivery | Warehouse dispatches delivery |
| `SUPPLIED_BY` | material | material | Material sourced from sub-supplier |

## NalaQL Queries

### 1. Trace All Products Affected by a Material Recall

```sql
MATCH (m:material)-[u:used_in]->(p:production)
AT "2025-03-01T00:00:00Z"
WHERE m.id = "batch_001"
RETURN m.id, p.id, u.quantity
```

### 2. Multi-Tier Impact Analysis via Causal Chain

Trace how a contaminated material batch propagates through the supply chain:

```sql
CAUSAL FROM batch_001
AT "2025-03-01T00:00:00Z"
DEPTH 5
WINDOW 7d
WHERE confidence > 0.3
RETURN path, delta, confidence
ORDER BY depth ASC
```

### 3. Full Provenance Tracking (Backward)

Given a final product, trace back to raw materials:

```sql
MATCH (d:delivery)<-[s:delivered_to]-(w:warehouse)<-[v:shipped_via]-(sh:shipment)
AT "2025-03-08T12:00:00Z"
WHERE d.id = "delivery_456"
RETURN d.id, w.id, sh.id
```

### 4. Detect Supply Chain Topology Changes

Identify new or removed supplier relationships over a period:

```sql
DIFF (m:material)-[s:supplied_by]->(sup:material)
BETWEEN "2025-01-01" AND "2025-03-01"
RETURN edge_id, from, to, change
```

### 5. Monitor Shipment Status Changes

Track status history of a specific shipment with downsampling:

```sql
GET history("node:shipment_789:prop:status")
FROM "2025-02-15" TO "2025-03-08"
DOWNSAMPLE LTTB(100)
```

### 6. Find High-Volume Supply Chains

Identify material batches with high transaction volumes:

```sql
META "node:material_*:prop:quantity"
WHERE total_writes > 100
RETURN key, total_writes, avg_value
LIMIT 20
```

## Architecture Integration

```
┌──────────────────────────────────────────┐
│         Supplier ERP Systems             │
│  (SAP, Oracle, custom APIs)              │
└───────────────┬──────────────────────────┘
                │ gRPC Set (stage transitions)
                ▼
┌──────────────────────────────────────────┐
│              NalaDB Cluster              │
│  ┌────────────┐  ┌───────────────────┐   │
│  │ Temporal KV │  │  Graph Layer     │   │
│  │ (metadata) │  │  (supply chain)   │   │
│  └────────────┘  └───────────────────┘   │
└───────────────┬──────────────────────────┘
                │ NalaQL queries
                ▼
┌──────────────────────────────────────────┐
│     Supply Chain Management Dashboard    │
│  (Provenance tracking, recall mgmt)      │
└──────────────────────────────────────────┘
```

## Quick Start

Run the supply chain example:

```bash
# Start NalaDB server
./bin/naladb

# Run the example
go run examples/supply-chain/main.go
```

## See Also

- [Full Use Case Description](UseCase-03-Supply-Chain.md)
