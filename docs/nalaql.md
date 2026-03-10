# NalaQL -- Query Language Specification

> NalaQL is a Cypher-inspired query language with temporal extensions for NalaDB.

## Overview

NalaQL provides a declarative way to interact with NalaDB's temporal key-value store and graph layer. It extends familiar Cypher-like syntax with temporal operators for point-in-time queries, range queries, causal dependency traversal, and temporal diff comparisons.

Keywords are **case-insensitive** (`MATCH`, `match`, and `Match` are equivalent).

## Temporal Extensions

| Operator  | Description                              | Example                                          |
| --------- | ---------------------------------------- | ------------------------------------------------ |
| `AT`      | Point-in-time query                      | `AT "2024-06-01T14:32:05Z"`                      |
| `DURING`  | Range query over a time interval         | `DURING "2024-01-01" TO "2024-06-01"`            |
| `CAUSAL`  | Follow causal dependency chains          | `CAUSAL FROM sensor_A DEPTH 5`                   |
| `DIFF`    | Compare graph state between two points   | `DIFF ... BETWEEN "2024-01-01" AND "2024-06-01"` |

## Token Types

### Keywords

```
MATCH  AT  DURING  WHERE  RETURN  CAUSAL  DIFF  GET  META  SHOW  DESCRIBE
FROM  DEPTH  WINDOW  LIMIT  SET  DELETE  HISTORY  TRAVERSE
LAST  DOWNSAMPLE  LTTB  ORDER  BY  ASC  DESC  BETWEEN  TO
AND  OR  NOT  TRUE  FALSE  NODE  NODES  EDGE  EDGES  KEYS
FETCH  AS  LATEST  OFFSET
```

### Operators

| Token | Meaning          |
| ----- | ---------------- |
| `=`   | Equal            |
| `!=`  | Not equal        |
| `>`   | Greater than     |
| `<`   | Less than        |
| `>=`  | Greater or equal |
| `<=`  | Less or equal    |
| `+`   | String concat    |

### Graph Punctuation

| Token | Meaning                 |
| ----- | ----------------------- |
| `(`   | Node pattern open       |
| `)`   | Node pattern close      |
| `[`   | Edge pattern open       |
| `]`   | Edge pattern close      |
| `:`   | Type separator          |
| `->`  | Outgoing edge direction |
| `<-`  | Incoming edge direction |
| `-`   | Edge connector          |
| `.`   | Property access         |
| `,`   | List separator          |
| `*`   | Wildcard                |

### Literals

| Type     | Examples                              |
| -------- | ------------------------------------- |
| IDENT    | `sensor_A`, `myVar`, `_private`       |
| STRING   | `"hello"`, `"2024-06-01T14:32:05Z"`  |
| INT      | `42`, `500`, `0`                      |
| FLOAT    | `0.5`, `3.14`, `10.0`                |
| BOOL     | `TRUE`, `FALSE`                       |

## Operator Precedence (Expression Parser)

The expression parser uses Pratt parsing with the following binding powers (highest binds tightest):

| Precedence | Operators       | Associativity |
| ---------- | --------------- | ------------- |
| 70         | `.` (property)  | Left          |
| 40         | `+` (concat)    | Left          |
| 30         | `= != > < >= <=`| Left          |
| 20         | `AND`           | Left          |
| 10         | `OR`            | Left          |

`NOT` is a prefix operator with precedence above comparisons.

Parentheses `(expr)` can override precedence in expressions.

## Statements

### MATCH -- Graph Pattern Query

Query the graph with optional temporal and filter clauses.

```sql
MATCH <pattern>
[AT <timestamp>]
[DURING <start> TO <end>]
[WHERE <expression>]
[FETCH latest(<key_expr>) AS <alias>, ...]
[FETCH history(<key_expr>) [FROM <ts> TO <ts>] [LAST <n>] AS <alias>, ...]
[RETURN <projection>, ...]
[ORDER BY <expr> [ASC|DESC], ...]
[LIMIT <n>]
```

#### Graph Pattern Syntax

Nodes: `(variable)` or `(variable:type)`

Edges with direction:
- Outgoing: `-[variable:relation]->`
- Incoming: `<-[variable:relation]-`
- Both/undirected: `-[variable:relation]-`

Variable and relation are optional. Patterns chain: `(a)-[r1]->(b)-[r2]->(c)`

#### FETCH Clause -- Bridging Graph and KV

The `FETCH` clause enriches graph query results with data from the KV store. This bridges the gap between graph topology (nodes/edges) and time-series data (sensor readings stored as KV keys).

Two modes are supported:

- **`latest(key_expr)`** — fetches the current value of a KV key
- **`history(key_expr)`** — fetches the time-series history of a KV key

The `key_expr` can be a string literal or an expression using `+` for string concatenation, including references to node properties.

##### Convention: `readingKey` Property

Nodes that produce time-series data should store an explicit `readingKey` property pointing to their KV key. This makes the binding between graph and KV explicit and queryable.

For example, a Sensor node with `readingKey = "sensor:S-EXT-01-TORQ:reading"` maps directly to the KV key where its readings are stored.

##### Example: Current sensor values for a machine

```sql
MATCH (s:Sensor)-[:BELONGS_TO]->(m:Machine)
WHERE m.machineId = "M03"
FETCH latest(s.readingKey) AS s.value
RETURN s.sensorId, s.metric, s.unit, s.value
```

##### Example: Sensor history with time range

```sql
MATCH (s:Sensor)-[:BELONGS_TO]->(m:Machine)
WHERE m.machineId = "M03"
FETCH history(s.readingKey) FROM "2026-03-09T08:00:00Z" TO "2026-03-09T09:00:00Z" AS s.history
RETURN s.sensorId, s.history
```

##### Example: Key expression with concatenation

```sql
MATCH (s:Sensor)-[:BELONGS_TO]->(m:Machine)
FETCH latest("sensor:" + s.sensorId + ":reading") AS s.value
RETURN s.sensorId, s.value
```

#### Inline `latest()` Function

As an alternative to `FETCH`, `latest()` can be used directly in `RETURN` expressions:

```sql
MATCH (s:Sensor)-[:BELONGS_TO]->(m:Machine)
WHERE m.machineId = "M03"
RETURN s.sensorId, s.unit, latest(s.readingKey)
```

The inline form is more concise but doesn't support history retrieval or aliasing.

#### Example: Predictive Maintenance

```sql
MATCH (a:sensor)-[r:triggers]->(b)
AT "2024-06-01T14:32:05Z"
WHERE r.weight > 0.5
RETURN a.id, b.id, r.weight
```

#### Example: Fraud Detection (multi-hop)

```sql
MATCH (a:account)-[t:transfer]->(b:account)-[t2:transfer]->(c:account)
AT "2024-03-15T10:00:00Z"
WHERE t.amount > 10000 AND t2.amount > 10000
RETURN a.id, b.id, c.id, t.amount, t2.amount
LIMIT 100
```

### CAUSAL -- Causal Dependency Traversal

Traverse causal dependency chains from a trigger node.

```sql
CAUSAL FROM <trigger_node>
[AT <timestamp>]
[DEPTH <n>]
[WINDOW <duration>]
[WHERE <expression>]
[RETURN <projection>, ...]
[ORDER BY <expr> [ASC|DESC], ...]
[LIMIT <n>]
```

The `WINDOW` clause accepts duration values like `30s`, `5m`, `1h`.

#### Example: IT Infrastructure Cascading Failure

```sql
CAUSAL FROM sensor_A
AT "2024-06-01T14:32:05Z"
DEPTH 5
WINDOW 30s
WHERE confidence > 0.7
RETURN path, delta, confidence
ORDER BY delta ASC
```

### DIFF -- Temporal Graph Comparison

Compare graph state between two points in time.

```sql
DIFF <pattern>
BETWEEN <timestamp1> AND <timestamp2>
[WHERE <expression>]
[RETURN <projection>, ...]
[LIMIT <n>]
```

#### Example: Supply Chain Changes

```sql
DIFF (a:device)-[r]->(b)
BETWEEN "2024-01-01" AND "2024-06-01"
RETURN added_edges, removed_edges, changed_weights
```

### GET history() -- Key History Retrieval

Retrieve the temporal history of a key with optional downsampling.

```sql
GET history(<key>)
[FROM <start> TO <end>]
[LAST <n>]
[DOWNSAMPLE <strategy>(<buckets>)]
```

Supported downsampling strategies: `LTTB` (Largest Triangle Three Buckets).

#### Example: Smart Building Temperature

```sql
GET history("node:sensor_42:prop:temperature")
FROM "2024-06-01" TO "2024-06-02"
DOWNSAMPLE LTTB(500)
```

### META -- Key Statistics Query

Query statistical metadata about keys matching a pattern.

```sql
META <key_pattern>
[WHERE <expression>]
[RETURN <projection>, ...]
[LIMIT <n>]
```

Key patterns support wildcards via `*` inside the string literal.

#### Example: Sensor Monitoring

```sql
META "node:sensor_*:prop:temperature"
WHERE write_rate > 10.0
RETURN key, write_rate, avg_interval, stddev_value
```

## Built-in Functions

NalaQL supports the following built-in functions for use in WHERE and RETURN clauses:

| Function | Description | Example |
|----------|-------------|---------|
| `now()` | Returns the current HLC timestamp | `AT now()` |
| `trend(key)` | Returns the write rate trend for a key | `WHERE trend(key) > 0` |
| `stddev(key)` | Returns the standard deviation for a key | `WHERE stddev(key) > 5.0` |
| `avg(key)` | Returns the running average for a key | `RETURN avg(key)` |
| `count(*)` | Counts matching results | `RETURN count(*)` |
| `latest(key)` | Returns the current value for a KV key | `RETURN latest(s.readingKey)` |

These functions are resolved at query execution time against the KeyMeta registry or the store.

The `latest()` function can be used inline in `RETURN` clauses or as a `FETCH` mode. When used inline, the key expression is evaluated per row, and the current value is fetched from the KV store.

## Use Case Examples

### Predictive Maintenance

```sql
-- Find sensors monitoring a failing pump with live readings
MATCH (s:Sensor)-[r:BELONGS_TO]->(m:Machine)
WHERE m.criticality = "critical"
FETCH latest(s.readingKey) AS s.value
RETURN s.sensorId, s.metric, s.unit, s.value

-- Same query using inline latest()
MATCH (s:Sensor)-[r:BELONGS_TO]->(m:Machine)
WHERE m.criticality = "critical"
RETURN s.sensorId, s.unit, latest(s.readingKey)

-- Trace causal chain from anomaly
CAUSAL FROM pump_01
AT "2025-03-08T14:32:05Z"
DEPTH 5
WINDOW 30m
RETURN path, confidence
ORDER BY confidence DESC
```

### Fraud Detection

```sql
-- Detect high-value transfer chains
MATCH (a:account)-[t1:transfer]->(b:account)-[t2:transfer]->(c:account)
AT "2025-03-15T10:00:00Z"
WHERE t1.amount > 10000 AND t2.amount > 10000
RETURN a.id, b.id, c.id

-- Monitor accounts with unusual activity
META "node:account_*:prop:balance"
WHERE write_rate > 100.0
RETURN key, write_rate, total_writes
```

### Supply Chain

```sql
-- Track material provenance
MATCH (m:material)-[u:used_in]->(p:production)
WHERE m.grade = "A"
RETURN m.id, p.id, u.quantity

-- Detect supply chain topology changes
DIFF (m:material)-[s:supplied_by]->(sup:material)
BETWEEN "2025-01-01" AND "2025-03-01"
RETURN edge_id, from, to, change
```

### Smart Building

```sql
-- Find all sensors in a building
MATCH (b:building)-[c1:contains]->(f:floor)-[c2:contains]->(r:room)<-[m:monitors]-(s:sensor)
WHERE b.name = "Building A"
RETURN f.id, r.id, s.id
ORDER BY f.id ASC

-- HVAC impact on room temperature
CAUSAL FROM hvac_unit_12
AT "2025-03-08T14:00:00Z"
DEPTH 3
WINDOW 30m
RETURN path, delta, confidence
```

### Erdnussflips Production Line

```sql
-- All sensor values for the Extruder zone
MATCH (s:Sensor)-[:BELONGS_TO]->(m:Machine)-[:LOCATED_IN]->(z:Zone)
WHERE z.zoneId = "Z3"
FETCH latest(s.readingKey) AS s.value
RETURN s.sensorId, s.metric, s.value, s.unit

-- Sensor history for extruder torque during a shift
MATCH (s:Sensor)-[:BELONGS_TO]->(m:Machine)
WHERE s.sensorId = "S-EXT-01-TORQ"
FETCH history(s.readingKey) FROM "2026-03-09T06:00:00Z" TO "2026-03-09T14:00:00Z" AS s.history
RETURN s.sensorId, s.history

-- Critical machine overview with live readings
MATCH (s:Sensor)-[:BELONGS_TO]->(m:Machine)
WHERE m.criticality = "critical"
FETCH latest(s.readingKey) AS s.value
RETURN m.machineId, s.sensorId, s.metric, s.value, s.unit
ORDER BY m.machineId ASC
```

### IT Infrastructure

```sql
-- Root cause analysis for cascading failure
CAUSAL FROM api_gateway
AT "2025-03-08T14:32:05Z"
DEPTH 5
WINDOW 5m
WHERE confidence > 0.5
RETURN path, delta, confidence

-- Service health monitoring
GET history("node:auth_service:prop:status")
FROM "2025-03-08T13:30:00Z" TO "2025-03-08T14:30:00Z"
```

### DESCRIBE -- Inspect Nodes and Edges

Retrieve full details of nodes or edges, including all property values.

Unlike `SHOW` (which lists property names), `DESCRIBE` returns actual property values.

```sql
DESCRIBE NODE <id>           -- single node by ID
DESCRIBE EDGE <id>           -- single edge by ID
DESCRIBE NODES               -- all nodes with property values
DESCRIBE EDGES               -- all edges with property values
[AT <timestamp>]
[WHERE <expression>]
[LIMIT <n>]
[OFFSET <n>]
```

#### Example: Inspect a Specific Node

```sql
DESCRIBE NODE "sensor_42"
```

Returns columns: `id`, `type`, and all property key-value pairs (e.g. `temperature`, `status`, `location`).

#### Example: Inspect a Node at a Point in Time

```sql
DESCRIBE NODE "sensor_42" AT "2024-06-01T14:00:00Z"
```

#### Example: List All Nodes with Property Values

```sql
DESCRIBE NODES WHERE type = "sensor" LIMIT 10
```

Returns one row per node with `id`, `type`, and all property values as columns.

#### Example: Inspect a Specific Edge

```sql
DESCRIBE EDGE "edge_abc123"
```

Returns columns: `id`, `from`, `to`, `relation`, and all edge property values (e.g. `weight`, `label`).

#### Example: List All Edges with Properties

```sql
DESCRIBE EDGES LIMIT 20
```

### SHOW -- List Available Nodes, Edges, or Keys

Discover what data is available to query.

```sql
SHOW NODES|EDGES|KEYS
[WHERE <expression>]
[LIMIT <n>]
```

#### Example: List All Nodes

```sql
SHOW NODES
```

Returns columns: `id`, `type`, `properties` (comma-separated property names).

#### Example: List Nodes Filtered by Type

```sql
SHOW NODES WHERE type = "sensor" LIMIT 10
```

#### Example: List All Edges

```sql
SHOW EDGES
```

Returns columns: `id`, `from`, `to`, `relation`.

#### Example: List All Store Keys

```sql
SHOW KEYS
```

Returns column: `key`.

## Error Handling

Parse errors include source position (line and column) and indicate the expected vs. actual token:

```
parse error at line 1, column 17: unexpected token WHERE (expected ), got WHERE)
```

## Query Execution

### Architecture

NalaQL queries are executed through a three-stage pipeline:

1. **Parse**: The Pratt parser transforms NalaQL text into an AST (Abstract Syntax Tree).
2. **Plan**: The `Planner` transforms the AST into a tree of physical operators.
3. **Execute**: The `Executor` drives the operator tree using a pull-based iterator model.

```
NalaQL Text → [Lexer] → Tokens → [Parser] → AST → [Planner] → Operator Tree → [Executor] → Rows
```

### Iterator Model

All operators implement the `Operator` interface:

```go
type Operator interface {
    Next() (Row, bool, error)
    Close()
}
```

This pull-based model enables **lazy evaluation**: downstream operators (e.g., `LIMIT`) can stop pulling from upstream operators early, avoiding unnecessary computation. For a query returning 100,000 matches with `LIMIT 10`, only 10 rows are materialized.

### Operator Pipeline

| Statement | Pipeline |
|-----------|----------|
| `MATCH`   | NodeScan → EdgeJoin(s) → Filter → Fetch → LatestResolve → Sort → Limit → Project |
| `CAUSAL`  | CausalScan → Filter → Sort → Limit → Project |
| `DIFF`    | DiffScan → Filter → Limit → Project |
| `META`    | MetaScan → Filter → Limit → Project |
| `GET history()` | HistoryScan (with optional downsampling) |
| `DESCRIBE` | DescribeNodeScan/DescribeEdgeScan → Filter → Offset → Limit |
| `SHOW`    | ShowNodesScan/ShowEdgesScan/ShowKeysScan → Filter → Limit |

### Available Operators

| Operator | Description |
|----------|-------------|
| `NodeScanOp` | Scans all nodes of a given type from the store |
| `EdgeJoinOp` | Joins nodes through edges following graph patterns |
| `FilterOp` | Applies WHERE expressions to filter rows |
| `ProjectOp` | Selects columns from RETURN clause |
| `SortOp` | Materializing sort for ORDER BY |
| `LimitOp` | Stops after N rows (lazy) |
| `CausalScanOp` | Wraps `CausalTraverse` as an iterator |
| `DiffScanOp` | Compares edge state between two timestamps |
| `MetaScanOp` | Scans KeyMeta entries matching a pattern |
| `HistoryScanOp` | Scans key history with optional downsampling |
| `DescribeNodeScanOp` | Returns nodes with all property values (single or all) |
| `DescribeEdgeScanOp` | Returns edges with all property values (single or all) |
| `ShowNodesScanOp` | Lists all active graph nodes with type and properties |
| `ShowEdgesScanOp` | Lists all active graph edges with relation info |
| `ShowKeysScanOp` | Lists all keys in the store |
| `FetchOp` | Enriches rows with KV data from FETCH clause |
| `LatestResolveOp` | Resolves inline `latest()` function calls |

### Downsampling Strategies

When retrieving large time-series histories, downsampling reduces the result set to a manageable number of representative points.

| Strategy | Description |
|----------|-------------|
| `LTTB` | Largest Triangle Three Buckets — preserves visual shape by selecting the point in each bucket that forms the largest triangle with adjacent selected points. Best for visualization. |
| `MINMAX` | Selects min and max per bucket — preserves extremes. Returns up to 2× bucket count. |
| `AVG` | Averages each bucket — smooths noise. Returns exactly N points. |

Example:

```sql
GET history("sensor:temp")
FROM "2025-06-01" TO "2025-06-02"
DOWNSAMPLE LTTB(100)
```

This returns exactly 100 representative data points from potentially thousands of stored values.

### Usage

```go
exec := query.NewExecutor(store, graph, metaRegistry, clock)
rows, err := exec.Execute(`
    MATCH (s:sensor)-[r:monitors]->(m:machine)
    WHERE m.type = "hydraulic_press"
    RETURN s.id, m.id
    LIMIT 10
`)
```

## Grammar Summary (EBNF)

```ebnf
statement     = match_stmt | causal_stmt | diff_stmt | get_stmt | meta_stmt
              | describe_stmt | show_stmt ;

match_stmt    = "MATCH" graph_pattern
                { at_clause | during_clause | where_clause
                | fetch_clause | return_clause | order_clause | limit_clause } ;

causal_stmt   = "CAUSAL" "FROM" (IDENT | STRING)
                { at_clause | depth_clause | window_clause | where_clause
                | return_clause | order_clause | limit_clause } ;

diff_stmt     = "DIFF" graph_pattern
                { between_clause | where_clause | return_clause | limit_clause } ;

get_stmt      = "GET" history_call
                { from_to_clause | last_clause | downsample_clause } ;

meta_stmt     = "META" STRING
                { where_clause | return_clause | limit_clause } ;

describe_stmt = "DESCRIBE" ( "NODE" STRING | "EDGE" STRING
              | "NODES" | "EDGES" )
                { at_clause | where_clause | limit_clause | offset_clause } ;

show_stmt     = "SHOW" ("NODES" | "EDGES" | "KEYS")
                { where_clause | limit_clause } ;

graph_pattern = node_pattern { edge_pattern node_pattern } ;
node_pattern  = "(" [IDENT] [":" IDENT] ")" ;
edge_pattern  = ("-" | "<-") "[" [IDENT] [":" IDENT] "]" ("->" | "-") ;

at_clause     = "AT" STRING ;
during_clause = "DURING" STRING "TO" STRING ;
where_clause  = "WHERE" expression ;
return_clause = "RETURN" expression { "," expression } ;
order_clause  = "ORDER" "BY" order_item { "," order_item } ;
order_item    = expression ["ASC" | "DESC"] ;
limit_clause  = "LIMIT" INT ;
offset_clause = "OFFSET" INT ;
between_clause= "BETWEEN" STRING "AND" STRING ;
depth_clause  = "DEPTH" INT ;
window_clause = "WINDOW" (INT [IDENT] | STRING | IDENT) ;
from_to_clause= "FROM" STRING "TO" STRING ;
last_clause   = "LAST" INT ;
downsample_clause = "DOWNSAMPLE" (IDENT | LTTB) "(" INT ")" ;
history_call  = "HISTORY" "(" STRING ")" ;
fetch_clause  = "FETCH" fetch_item { "," fetch_item } ;
fetch_item    = ("LATEST" | "HISTORY") "(" expression ")"
                ["FROM" STRING "TO" STRING] ["LAST" INT]
                "AS" dotted_ident ;
dotted_ident  = IDENT { "." IDENT } ;

expression    = prefix { infix_op expression } ;
prefix        = IDENT ["(" [expression {"," expression}] ")"]
              | STRING | INT | FLOAT | "TRUE" | "FALSE"
              | "NOT" expression
              | "(" expression ")"
              | "*" ;
infix_op      = "." | "+" | "=" | "!=" | ">" | "<" | ">=" | "<="
              | "AND" | "OR" ;
```
