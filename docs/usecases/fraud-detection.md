# Use Case: Real-Time Fraud Detection

> Model transaction graphs, detect suspicious patterns, and trace money flows in real time.

## Overview

A financial services company monitors transactions between accounts. Each transaction is stored with full temporal history. Account relationships form a graph that can be traversed to detect fraud rings and suspicious patterns.

**NalaDB solves this** by storing every transaction as a temporal KV entry, modeling account relationships as a graph, and using CAUSAL queries to trace money flows and MATCH queries to detect suspicious multi-hop transfer patterns.

## Data Model

### Nodes

| Node Type | Description | Properties |
|-----------|-------------|------------|
| `account` | Bank account | name, type, risk_score |
| `merchant` | Merchant account | category, mcc_code |

### Edges

| Relation | From | To | Description |
|----------|------|----|-------------|
| `TRANSFER` | account | account | Inter-account transfer |
| `PAYMENT` | account | merchant | Payment to merchant |

### Properties (Time-Series)

```
node:{account_id}:prop:balance       → float (currency amount)
node:{account_id}:prop:risk_score    → float (0.0-1.0)
node:{account_id}:prop:status        → string (active/frozen/closed)
```

## NalaQL Queries for Fraud Detection

### 1. Detect High-Value Transfer Chains

Find accounts involved in multi-hop high-value transfers:

```sql
MATCH (a:account)-[t1:transfer]->(b:account)-[t2:transfer]->(c:account)
AT "2025-03-15T10:00:00Z"
WHERE t1.amount > 10000 AND t2.amount > 10000
RETURN a.id, b.id, c.id, t1.amount, t2.amount
LIMIT 100
```

### 2. Trace Money Flow from Suspicious Account

Follow the causal chain of transfers from a flagged account:

```sql
CAUSAL FROM account_suspect_42
AT "2025-03-15T12:00:00Z"
DEPTH 5
WINDOW 1h
WHERE confidence > 0.5
RETURN path, delta, confidence
ORDER BY confidence DESC
LIMIT 20
```

### 3. Detect Circular Transfer Patterns (Layering)

Find accounts that send and receive within a short window:

```sql
MATCH (a:account)-[t1:transfer]->(b:account)-[t2:transfer]->(a:account)
AT "2025-03-15T10:00:00Z"
WHERE t1.amount > 5000 AND t2.amount > 5000
RETURN a.id, b.id, t1.amount, t2.amount
```

### 4. Compare Account Relationships Over Time

Detect new connections established in a suspicious period:

```sql
DIFF (a:account)-[t:transfer]->(b:account)
BETWEEN "2025-03-01" AND "2025-03-15"
RETURN edge_id, from, to, change
LIMIT 50
```

### 5. Monitor High-Frequency Trading Accounts

Find accounts with unusually high write rates (many transactions):

```sql
META "node:account_*:prop:balance"
WHERE write_rate > 100.0
RETURN key, write_rate, total_writes, avg_value
LIMIT 20
```

### 6. Historical Balance Analysis

Track balance changes for a specific account with downsampling:

```sql
GET history("node:account_42:prop:balance")
FROM "2025-01-01" TO "2025-03-15"
DOWNSAMPLE LTTB(200)
```

## Architecture Integration

```
┌──────────────────────────────────────────┐
│        Transaction Processing            │
│  (Core Banking, Payment Gateway)         │
└───────────────┬──────────────────────────┘
                │ gRPC Set (transactions)
                ▼
┌──────────────────────────────────────────┐
│              NalaDB Cluster              │
│  ┌────────────┐  ┌───────────────────┐   │
│  │ Temporal KV │  │  Graph Layer     │   │
│  │ (balances) │  │  (account graph)  │   │
│  └────────────┘  └───────────────────┘   │
└───────────────┬──────────────────────────┘
                │ NalaQL queries
                ▼
┌──────────────────────────────────────────┐
│        Fraud Detection Engine            │
│  (Rule-based + ML scoring, alerting)     │
└──────────────────────────────────────────┘
```

## Quick Start

Run the fraud detection example:

```bash
# Start NalaDB server
./bin/naladb

# Run the example
go run examples/fraud-detection/main.go
```

## See Also

- [Full Use Case Description](UseCase-02-Betrugserkennung.md)
