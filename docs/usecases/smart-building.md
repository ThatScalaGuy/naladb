# Use Case: Smart Building / Digital Twin

> Model building systems as a graph, track state changes over time, and simulate "what-if" scenarios.

## Overview

A smart building platform maintains a digital twin of all building systems (HVAC, lighting, security). Sensors feed real-time data into the temporal store. The building topology is modeled as a graph, enabling operators to query historical states and understand how system changes propagate.

**NalaDB solves this** by representing the building as a temporal graph (floors, rooms, HVAC, sensors), storing sensor readings as versioned KV entries, and using CAUSAL queries to trace how HVAC changes propagate through comfort conditions.

## Data Model

### Nodes

| Node Type | Description | Properties |
|-----------|-------------|------------|
| `building` | Building structure | name, address, floors |
| `floor` | Building floor | level, area_sqm |
| `room` | Individual room | name, capacity, zone |
| `hvac` | HVAC unit | model, capacity_kw, mode |
| `sensor` | IoT sensor | type, unit, location |

### Edges

| Relation | From | To | Description |
|----------|------|----|-------------|
| `CONTAINS` | building/floor | floor/room | Spatial hierarchy |
| `SERVES` | hvac | room | HVAC serves room |
| `MONITORS` | sensor | room | Sensor monitors room |

### Properties (Time-Series)

```
node:{sensor_id}:prop:temperature    → float (°C)
node:{sensor_id}:prop:humidity       → float (%)
node:{hvac_id}:prop:setpoint         → float (°C)
node:{hvac_id}:prop:mode             → string (cooling/heating/off)
```

## NalaQL Queries for Comfort Analysis

### 1. Find All Sensors Monitoring a Specific Room

```sql
MATCH (s:sensor)-[r:monitors]->(room:room)
WHERE room.name = "305"
RETURN s.id, s.type, room.name
```

### 2. HVAC Impact Analysis

Trace how an HVAC unit change propagates through the building:

```sql
CAUSAL FROM hvac_unit_12
AT "2025-03-08T14:00:00Z"
DEPTH 3
WINDOW 30m
WHERE confidence > 0.4
RETURN path, delta, confidence
ORDER BY delta ASC
```

### 3. Temperature History with Downsampling

View a day's temperature readings for a room sensor:

```sql
GET history("node:temp_sensor_305a:prop:temperature")
FROM "2025-03-08" TO "2025-03-09"
DOWNSAMPLE LTTB(288)
```

This returns 288 points (one every 5 minutes) from potentially thousands of raw readings.

### 4. Detect Building Topology Changes

Compare the building graph between two maintenance windows:

```sql
DIFF (a)-[r:contains]->(b)
BETWEEN "2025-01-01" AND "2025-03-01"
RETURN edge_id, from, to, change
```

### 5. Find Overactive Sensors

Identify sensors writing data too frequently (potential malfunction):

```sql
META "node:*sensor*:prop:*"
WHERE write_rate > 50.0
RETURN key, write_rate, total_writes
LIMIT 10
```

### 6. Multi-Hop Building Traversal

Find all sensors connected to a building through floors and rooms:

```sql
MATCH (b:building)-[c1:contains]->(f:floor)-[c2:contains]->(r:room)<-[m:monitors]-(s:sensor)
WHERE b.name = "Building A"
RETURN b.name, f.id, r.id, s.id
ORDER BY f.id ASC
```

## Architecture Integration

```
┌──────────────────────────────────────────┐
│       Building Management System         │
│  (BACnet, KNX, Modbus gateways)          │
└───────────────┬──────────────────────────┘
                │ gRPC Set (sensor readings)
                ▼
┌──────────────────────────────────────────┐
│              NalaDB Cluster              │
│  ┌────────────┐  ┌───────────────────┐   │
│  │ Temporal KV │  │  Graph Layer     │   │
│  │ (readings) │  │  (building topo)  │   │
│  └────────────┘  └───────────────────┘   │
└───────────────┬──────────────────────────┘
                │ NalaQL queries
                ▼
┌──────────────────────────────────────────┐
│       Digital Twin Dashboard             │
│  (3D visualization, comfort analytics)   │
└──────────────────────────────────────────┘
```

## Quick Start

Run the smart building example:

```bash
# Start NalaDB server
./bin/naladb

# Run the example
go run examples/smart-building/main.go
```

## See Also

- [Full Use Case Description](UseCase-04-Smart-Building.md)
