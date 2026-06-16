# Solace Platform Integration for Materialize

> **Fork note**: This directory and its contents are specific to the
> `SolaceDev/materialize-solace` fork and must be removed before merging
> upstream to `MaterializeInc/materialize`.

This fork adds a native Solace Platform connector to Materialize, enabling
Materialize to ingest messages from Solace queues (SOURCE) and publish query
results back to Solace topics (SINK).

## Architecture

```
Solace Platform broker
    │  SMF / persistent queues
    ▼
CREATE SOURCE ... FROM SOLACE   ← mz-storage / src/storage/src/source/solace.rs
    │  exactly-once via RGMID watermark
    ▼
Materialized views, indexes, sinks
    │
    ▼
CREATE SINK ... INTO SOLACE     ← mz-storage / src/storage/src/sink/solace.rs
    │  per-row topic template rendering
    ▼
Solace Platform broker
```

The source uses Solace's `solace-rs` async SDK with an exactly-once protocol:
rows are emitted and timestamped using the broker-assigned
`ReplicationGroupMessageId` (RGMID); broker acknowledgement only fires after
Materialize's persist layer has durably committed the data.

The sink publishes rows as JSON to a topic derived from a template string
(e.g. `mz/airspace/{lat_cell}/{lon_cell}`), with optional dedup suppression.

## SQL syntax

### Connection

```sql
CREATE SECRET solace_pw AS '...';

CREATE CONNECTION my_conn TO SOLACE (
  HOST     'tcp://broker.example.com:55555',
  MESSAGE VPN 'my-vpn',
  USERNAME 'mz-user',
  PASSWORD SECRET solace_pw
);
```

### Source

```sql
CREATE SOURCE my_source
  IN CLUSTER my_cluster
  FROM SOLACE CONNECTION my_conn (
    QUEUE 'my-queue'
  )
  INCLUDE TOPIC, BROKER TIMESTAMP
  FORMAT BYTES;
```

### Sink

```sql
-- Static topic
CREATE SINK my_sink
  IN CLUSTER my_cluster
  FROM my_view
  INTO SOLACE CONNECTION my_conn (
    TOPIC 'mz/output/events'
  );

-- Dynamic topic (column values substituted at publish time)
CREATE SINK density_sink
  IN CLUSTER my_cluster
  FROM airspace_density
  INTO SOLACE CONNECTION my_conn (
    TOPIC      'mz/airspace-density/{lat_cell}/{lon_cell}',
    DEDUP WINDOW '5s'   -- suppress duplicate topic publishes within window
  );
```

## Key source files

| File | Purpose |
|------|---------|
| `src/storage/src/source/solace.rs` | Source dataflow: SMF session, RGMID-based exactly-once |
| `src/storage/src/sink/solace.rs` | Sink dataflow: topic template rendering, JSON publish, dedup |
| `src/storage-types/src/sources/solace.rs` | `SolaceSourceConnection` type |
| `src/storage-types/src/sinks.rs` | `SolaceSinkConnection` type |
| `src/storage-types/src/connections.rs` | `SolaceConnection` type (host, VPN, credentials) |
| `src/sql-parser/src/ast/defs/ddl.rs` | AST nodes: `CreateSinkConnection::Solace`, etc. |
| `src/sql-parser/src/parser.rs` | `SOLACE` keyword parsing for all DDL statements |
| `src/sql/src/plan/statement/ddl.rs` | Planning: `CREATE SINK INTO SOLACE` |
| `src/sql/src/plan/statement/ddl/connection.rs` | Planning: `CREATE CONNECTION TO SOLACE` |
| `src/sql/src/solace_util.rs` | Shared planning helpers (topic template parsing, option extraction) |
| `src/sql/src/pure.rs` | Purification: queue existence checks for `CREATE SOURCE` |
| `misc/python/materialize/mzbuild.py` | Fingerprint fix: Solace crate sources included so testdrive image rebuilds on parser changes |

## Tests

Tests live in `test/solace/`. Run with mzcompose:

```bash
# Prerequisites (WSL2 / Linux — Solace broker requires high vm.max_map_count)
sudo sysctl -w vm.max_map_count=512000

# Run all sink tests
./bin/mzcompose --find solace --dev \
  --image-registry ghcr.io/jessemenning/materialize-solace \
  run sink-round-trip

# Run source round-trip
./bin/mzcompose --find solace --dev \
  --image-registry ghcr.io/jessemenning/materialize-solace \
  run round-trip

# Run exactly-once crash/restart test
./bin/mzcompose --find solace --dev \
  --image-registry ghcr.io/jessemenning/materialize-solace \
  run exactly-once
```

Individual testdrive files under `test/solace/`:

| File | What it tests |
|------|--------------|
| `round-trip.td` | Source: publish via REST → read via Materialize SELECT |
| `sink-basic.td` | Sink: INSERT into table → messages appear on Solace queue |
| `sink-dynamic-topic.td` | Sink: `{column}` placeholders render correctly per row |
| `sink-dedup.td` | Sink: `DEDUP WINDOW` suppresses repeated topic publishes |
| `sink-reconnect-*.td` | Sink: broker disconnect/reconnect resilience |
| `exactly-once-*.td` | Source: exactly-once protocol survives process restart |

## Image registry

Pre-built images are pushed to `ghcr.io/jessemenning/materialize-solace`.
mzbuild hashes are computed from source content; the mzbuild fingerprint in
this fork includes Solace crate sources so that parser and sink changes force
testdrive image rebuilds (see `misc/python/materialize/mzbuild.py`).

To trigger a CI image build after source changes:

```bash
gh workflow run CI --repo SolaceDev/materialize-solace --ref main
```

## Dependency

The Solace SDK is `solace-rs` (async wrapper over the C `libsolclient`).
Dependency declared in root `Cargo.toml` under `[workspace.dependencies]`.
