# Causal Analysis

> Automatically trace cause-and-effect chains across your graph by combining topology, temporal evidence, and domain-encoded confidence.

## What It Does

CAUSAL traverses dependency edges in the graph and checks whether connected nodes show **correlated data changes** within a time window. It returns ranked causal chains with confidence scores.

Instead of "sensor X is above threshold", CAUSAL tells you "sensor X is high **because** sensor Y changed 15 minutes ago, which was caused by sensor Z changing 2 hours ago" -- with a confidence score for each link.

## Quick Start

```sql
CAUSAL FROM "S-EXT-01-VIBR"
DEPTH 4
WINDOW 3h
RETURN path, confidence, via_rel
ORDER BY confidence DESC
```

This says: starting from the extruder vibration sensor, follow causal edges up to 4 hops, looking for correlated changes within a 3-hour window per hop.

## Setting Up Causal Edges

CAUSAL works on any graph edge. You model domain knowledge by creating edges between nodes that have a cause-and-effect relationship.

### Minimum Setup

```
Sensor A  --[AFFECTS]--> Sensor B
```

Create the edge via gRPC or any client:

```go
CreateEdge(from: sensorA_ID, to: sensorB_ID, relation: "AFFECTS")
```

That's it. The relation name is **freely chosen** -- `AFFECTS`, `CAUSES_DEGRADATION`, `TRIGGERS`, `DEPENDS_ON`, whatever fits your domain. CAUSAL follows all outgoing edges from the trigger node regardless of name.

### Adding Confidence Weights

To tune the confidence scoring, add a `weight` or `confidence` property (float, 0.0 to 1.0):

```go
CreateEdge(
    from: sensorA_ID,
    to:   sensorB_ID,
    relation: "CAUSES_DEGRADATION",
    properties: {
        "weight": "0.75",  // or "confidence": "0.75"
    },
)
```

- `1.0` = strong causal link (default if omitted)
- `0.5` = moderate link
- `0.1` = weak link

### Optional Metadata

You can store any additional properties on edges for documentation. CAUSAL doesn't use them, but they're useful for humans and for MATCH queries:

```go
properties: {
    "weight":             "0.75",
    "mechanism":          "Gearbox vibration causes torque rise",
    "typicalLagSeconds":  "7200",
}
```

## How the Algorithm Works

### Step 1: Find the Trigger Event

CAUSAL locates the trigger node (by ID or property lookup) and finds its **most recent data change** within `[now - window, now]`. This is the causal origin -- "event zero".

If you provide an explicit `AT` timestamp, that timestamp is used instead.

### Step 2: BFS Along Edges

Starting from the trigger, CAUSAL performs a breadth-first traversal following outgoing edges (for forward analysis) or incoming edges (for backward/root-cause analysis).

```
Trigger: VIBR (changed at t=0)
  ├── TORQ (edge: CAUSES_DEGRADATION, weight=0.75)
  │     └── TMP2 (edge: CAUSES_DEGRADATION, weight=0.85)
  │           └── PRES3 (edge: CAUSES_DEGRADATION, weight=0.80)
  └── AMPS (edge: CORRELATES_WITH, weight=0.95)
```

### Step 3: Evidence Check

At each neighbor, CAUSAL checks: **did this node's data actually change within the time window?**

It looks at:
1. **Node properties** -- any `node:<id>:prop:*` key history
2. **KV time-series** -- if the node has a `readingKey` property, the referenced KV key's history is checked too

No data change in the window = no causal evidence = branch is pruned. This prevents false chains -- CAUSAL only reports links where the data supports the connection.

### Step 4: Confidence Scoring

Each result gets a confidence score computed from three factors:

```
confidence = parent_confidence * time_factor * edge_weight
```

| Factor | Formula | Meaning |
|--------|---------|---------|
| `parent_confidence` | inherited | Confidence of the upstream node (starts at 1.0) |
| `time_factor` | `exp(-0.25 * delta/window)` | Exponential decay based on time distance |
| `edge_weight` | from `weight` or `confidence` property | Domain-encoded strength (default 1.0) |

**Time factor examples** (with 1h window):

| Time between changes | Time factor |
|---------------------|-------------|
| 0 min (simultaneous) | 1.000 |
| 5 min | 0.979 |
| 15 min | 0.939 |
| 30 min | 0.882 |
| 60 min (= window) | 0.779 |

Confidence naturally decreases with each hop and with temporal distance, so deep chains and slow propagations rank lower.

## Query Syntax

```sql
CAUSAL FROM <trigger>
[AT <timestamp>]
[DEPTH <n>]
[WINDOW <duration>]
[WHERE <expression>]
[RETURN <projection>, ...]
[ORDER BY <expr> [ASC|DESC], ...]
[LIMIT <n>]
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `FROM` | required | Trigger node -- graph node ID or a property value like a sensorId |
| `AT` | `now()` | Point in time for edge validity and trigger event |
| `DEPTH` | 5 | Maximum hops from trigger |
| `WINDOW` | 30s | Per-hop time window for change detection |
| `WHERE` | -- | Filter results (e.g., `confidence > 0.5`) |

### Trigger Node Resolution

The `FROM` value is resolved in order:

1. **Direct node ID** -- if a graph node with that ID exists, use it
2. **Property lookup** -- scan nodes for a matching `sensorId`, `machineId`, `zoneId`, or `id` property

This means you can write `CAUSAL FROM "S-EXT-01-VIBR"` even though the internal node ID is a UUID.

### Result Columns

| Column | Type | Description |
|--------|------|-------------|
| `node_id` | string | Human-readable name of the reached node (resolved from `sensorId`, `machineId`, `zoneId`, `name`, or `id` property; falls back to UUID if none found) |
| `depth` | number | Hops from trigger (1 = direct neighbor) |
| `delta` | number | Time difference from trigger event (microseconds) |
| `confidence` | number | Cumulative confidence score (0.0 to 1.0) |
| `path` | string | Resolved node names from trigger to this node, joined with ` -> ` |
| `via_edge` | string | Edge ID through which this node was reached |
| `via_rel` | string | Relation type of that edge |

Node names in `node_id` and `path` are automatically resolved by checking node properties in this order: `sensorId`, `machineId`, `zoneId`, `name`, `id`. The first non-empty match is used. If no property is found, the raw graph UUID is shown.

## Use Cases

### Forward Impact Analysis

*"Vibration is rising on the extruder. What else will be affected?"*

```sql
CAUSAL FROM "S-EXT-01-VIBR"
DEPTH 5
WINDOW 4h
RETURN path, confidence, via_rel
ORDER BY confidence DESC
```

### Root-Cause Investigation

*"The packaging weight is off. What's upstream?"*

Start from the symptom, follow causal chains. The graph encodes which sensors affect which:

```sql
CAUSAL FROM "S-PKG-01-WGHT"
DEPTH 5
WINDOW 8h
RETURN path, confidence
ORDER BY depth ASC
```

### Time-Travel Analysis

*"The morning shift was fine, the afternoon shift had rejects. What changed?"*

```sql
CAUSAL FROM "S-EXT-01-TORQ"
AT "2026-03-10T14:00:00Z"
DEPTH 4
WINDOW 8h
RETURN path, confidence, delta
ORDER BY confidence DESC
```

### Filtering Weak Links

Only show high-confidence chains:

```sql
CAUSAL FROM "S-EXT-01-VIBR"
DEPTH 4
WINDOW 3h
WHERE confidence > 0.5
RETURN path, confidence
```

## Example: Erdnussflips Production Line

The simulation models 5 failure scenarios with causal edges:

```
S-EXT-01-VIBR -[CAUSES_DEGRADATION {weight: 0.75}]-> S-EXT-01-TORQ
S-EXT-01-TORQ -[CAUSES_DEGRADATION {weight: 0.85}]-> S-EXT-01-TMP2
S-EXT-01-TMP3 -[CAUSES_DEGRADATION {weight: 0.80}]-> S-EXT-01-PRES3
S-SIL-02-HUMD -[CAUSES_DEGRADATION {weight: 0.70}]-> S-EXT-01-TORQ
S-DRY-02-VIBR -[CAUSES_DEGRADATION {weight: 0.80}]-> S-DRY-02-RPM
S-EXT-02-VIBR -[CAUSES_DEGRADATION {weight: 0.85}]-> S-EXT-02-AMPS
...
```

### Scenario A: Extruder Screw Wear

Vibration rises over weeks, then torque, then pressure. CAUSAL traces the full chain:

```sql
CAUSAL FROM "S-EXT-01-VIBR" DEPTH 4 WINDOW 84h
```

Returns: `VIBR -> TORQ -> TMP2` with decreasing confidence at each hop.

### Scenario D: Wet Raw Material

Humidity spike in the silo cascades through the extruder within minutes:

```sql
CAUSAL FROM "S-SIL-02-HUMD" DEPTH 4 WINDOW 1h
```

Returns: `HUMD -> TORQ -> TMP2 -> PRES3` -- a fast cascade with high confidence due to short time deltas.

## Bridging Graph and KV Data

CAUSAL connects two data layers:

- **Graph layer**: nodes, edges, topology (who affects whom)
- **KV layer**: time-series sensor readings (`sensor:S-EXT-01-VIBR:reading`)

The bridge is the `readingKey` property on graph nodes. When a sensor node has:

```
readingKey = "sensor:S-EXT-01-VIBR:reading"
```

CAUSAL checks that KV key's write history for evidence of change. Without `readingKey`, it only checks graph node property changes (`node:<id>:prop:*`).

**Both work.** If your data lives in node properties (direct graph writes), CAUSAL finds it there. If your data lives in the KV store (time-series via `Set`), add `readingKey` to bridge the gap.

## Architecture

```
                    CAUSAL FROM "S-EXT-01-VIBR" WINDOW 3h
                                   |
                    +--------------v--------------+
                    |         Query Planner        |
                    |  1. Resolve "S-EXT-01-VIBR"  |
                    |     -> UUID node lookup      |
                    |  2. Build CausalQuery        |
                    +--------------+--------------+
                                   |
                    +--------------v--------------+
                    |      CausalTraverse (BFS)    |
                    |                              |
                    |  For each hop:               |
                    |   - neighborsAt(node, now)   |
                    |   - detectPropertyChange()   |
                    |   - score confidence          |
                    +--------------+--------------+
                                   |
          +------------------------+------------------------+
          |                        |                        |
   +------v------+         +------v------+          +------v------+
   |  Graph Store |         |  KV Store   |          | Edge Props  |
   | adjacency    |         | reading     |          | weight /    |
   | lists        |         | history     |          | confidence  |
   +--------------+         +-------------+          +-------------+
```

## See Also

- [NalaQL Reference](nalaql.md) -- full query syntax including CAUSAL
- [Predictive Maintenance Use Case](usecases/predictive-maintenance.md)
- [Architecture](architecture.md) -- HLC and temporal store internals
