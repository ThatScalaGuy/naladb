# Use Case: Predictive Maintenance

> Track sensor data over time, detect anomalies, and trace causal chains to predict equipment failures.

## Overview

An industrial plant monitors pumps, motors, and valves via IoT sensors. Each sensor reading is stored as a temporal key-value pair. Equipment relationships are modeled as a graph. When an anomaly is detected, NalaDB's causal traversal identifies the root cause across the machine dependency chain.

**NalaDB solves this** by combining temporal KV storage (every sensor reading versioned) with a graph layer (equipment relationships) and causal ordering (tracing anomaly propagation through HLC timestamps).

## Data Model

### Nodes

| Node Type | Description | Properties |
|-----------|-------------|------------|
| `pump` | Hydraulic/centrifugal pump | model, capacity, install_date |
| `motor` | Electric motor | power_rating, rpm |
| `valve` | Control valve | type, position |
| `sensor` | IoT sensor | sensor_type, unit, location |

### Edges

| Relation | From | To | Description |
|----------|------|----|-------------|
| `MONITORS` | sensor | pump/motor/valve | Sensor observes equipment |
| `DRIVES` | pump | motor | Pump drives motor |
| `FEEDS` | valve | pump | Valve feeds material to pump |

### Properties (Time-Series)

```
node:{sensor_id}:prop:temperature    → float (°C)
node:{sensor_id}:prop:vibration      → float (mm/s)
node:{sensor_id}:prop:pressure       → float (bar)
node:{sensor_id}:prop:status         → string (healthy/warning/critical)
```

## NalaQL Queries

### 1. Find All Sensors Monitoring a Pump

```sql
MATCH (s:sensor)-[r:monitors]->(p:pump)
WHERE p.id = "pump_01"
RETURN s.id, s.type, r.weight
```

### 2. Causal Root-Cause Analysis

When pump_01 fails, trace what caused it by following causal chains backward:

```sql
CAUSAL FROM pump_01
AT "2025-03-08T14:32:05Z"
DEPTH 5
WINDOW 30m
WHERE confidence > 0.5
RETURN path, delta, confidence
ORDER BY confidence DESC
```

### 3. Equipment Dependency Traversal

Traverse all downstream equipment connected to pump_01 at the time of failure:

```sql
MATCH (p:pump)-[d:drives]->(m:motor)
AT "2025-03-08T14:32:05Z"
WHERE p.id = "pump_01"
RETURN p.id, m.id, d.weight
```

### 4. Sensor History with Downsampling

View temperature readings over the last 24 hours with visual downsampling:

```sql
GET history("node:sensor_temp_01:prop:temperature")
FROM "2025-03-07" TO "2025-03-08"
DOWNSAMPLE LTTB(500)
```

### 5. Detect Anomalous Sensor Behavior

Find sensors with unusually high write rates or standard deviation:

```sql
META "node:sensor_*:prop:vibration"
WHERE stddev_value > 5.0
RETURN key, write_rate, avg_value, stddev_value
LIMIT 20
```

### 6. Compare Equipment Graph Between Maintenance Windows

Detect topology changes (e.g., a valve was disconnected between maintenance windows):

```sql
DIFF (s:sensor)-[r:monitors]->(p:pump)
BETWEEN "2025-02-01" AND "2025-03-01"
RETURN edge_id, from, to, change
```

## Architecture Integration

```
┌──────────────────────────────────────────┐
│            SCADA / PLC System            │
│  (Sensors, Actuators, Controllers)       │
└───────────────┬──────────────────────────┘
                │ gRPC Set (sensor readings)
                ▼
┌──────────────────────────────────────────┐
│              NalaDB Cluster              │
│  ┌────────────┐  ┌───────────────────┐   │
│  │ Temporal KV │  │  Graph Layer     │   │
│  │ (readings) │  │  (dependencies)   │   │
│  └────────────┘  └───────────────────┘   │
└───────────────┬──────────────────────────┘
                │ NalaQL queries
                ▼
┌──────────────────────────────────────────┐
│          Analytics Dashboard             │
│  (Grafana, custom alerting system)       │
└──────────────────────────────────────────┘
```

## Quick Start

Run the predictive maintenance example:

```bash
# Start NalaDB server
./bin/naladb

# Run the example
go run examples/predictive-maintenance/main.go
```

## See Also

- [Full Use Case Description](UseCase-01-Predictive-Maintenance.md)
