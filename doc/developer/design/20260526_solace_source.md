# Implementation Prompt: Solace Source Connector for Materialize

## Context

You are implementing a new streaming source connector for Materialize that reads from Solace PubSub+ brokers using guaranteed (persistent) messaging. This is a net-new source type, alongside the existing Kafka, PostgreSQL, MySQL, SQL Server, and Webhook sources.

The Solace source is differentiated from the Kafka source in one important way: it provides **exactly-once ingestion into Materialize's persist layer** by leveraging Solace's broker-side acknowledgment protocol and the broker-assigned, monotonic `ReplicationGroupMessageId` (RGMID). Users should not need to write `DISTINCT ON` views to deduplicate after restart-induced redeliveries — the source handles this transparently.

Before writing any code, read the existing Kafka source implementation end-to-end. It is the closest structural precedent (CONNECTION + endpoint + FORMAT + INCLUDE + ENVELOPE + WITH options) and the new source should follow its conventions for everything that isn't Solace-specific. Look in particular at how the Kafka source:

- Defines the SQL surface (parser, planner, catalog entries)
- Bridges from SQL options to the runtime ingestion dataflow
- Commits progress to persist
- Exposes metadata columns via `INCLUDE`
- Handles connection objects and secrets

Mirror these patterns. Diverge only where Solace's semantics genuinely require it.

## Background reading

Familiarize yourself with these concepts before starting:

1. **Solace guaranteed messaging** — durable queues, durable topic endpoints (DTEs), client acknowledgment mode, ack windows, flow control. The Solace JCSMP and PubSub+ Messaging API for C docs are the canonical references; the JCSMP guide is easier to read.
2. **ReplicationGroupMessageId (RGMID)** — broker-assigned, monotonic per queue, stable across HA failover and DR replication. This is the modern replacement for ADMessageID. Critically: RGMIDs are *comparable* (one is less than, equal to, or greater than another within a queue), which is what enables the dedup-by-comparison approach below.
3. **Materialize's persist layer** — how the Kafka source writes batches and commits resumption upper. The Solace source needs to write the highest RGMID seen so far *atomically with the data batch*, so restart can resume correctly.
4. **Topic wildcards** — Solace supports `*` (single-level) and `>` (multi-level, terminal-only) wildcards in topic subscriptions. The DTE flow needs to handle these.

## SQL surface

### Connection

```sql
CREATE CONNECTION <name> TO SOLACE (
    HOST '<url>',                          -- e.g. 'tcps://broker.example.com:55443'
    MSG_VPN '<vpn>',                       -- Solace's tenant concept; required
    -- Auth (choose one):
    USERNAME '<user>', PASSWORD SECRET <secret>,
    -- OR:
    CLIENT CERTIFICATE = SECRET <cert>, CLIENT KEY = SECRET <key>,
    -- OR:
    OAUTH TOKEN = SECRET <token>,
    -- Optional:
    SSH TUNNEL <tunnel_connection>          -- match Kafka source's SSH tunnel support
);
```

Validation: `VALIDATE CONNECTION` should attempt a connect + bind probe and report failure modes (bad host, bad auth, missing VPN, etc.) cleanly.

### Source

```sql
CREATE SOURCE <name>
  [IN CLUSTER <cluster>]
  FROM SOLACE CONNECTION <connection> (
    -- Choose one of:
    QUEUE '<queue_name>'
    -- OR:
    DURABLE TOPIC ENDPOINT '<dte_name>',
    TOPIC SUBSCRIPTION '<topic_pattern>'   -- supports * and > wildcards
  )
  FORMAT <AVRO | JSON | PROTOBUF | TEXT | BYTES | CSV | REGEX> [...]
  [INCLUDE
      REPLICATION GROUP MESSAGE ID [AS <alias>]
    | BROKER TIMESTAMP             [AS <alias>]
    | SENDER TIMESTAMP             [AS <alias>]
    | APPLICATION MESSAGE ID       [AS <alias>]
    | CORRELATION ID               [AS <alias>]
    | TOPIC                        [AS <alias>]
    | TOPIC LEVELS                 [AS <prefix>]    -- see "Topic level decomposition" below
    | PARTITION                    [AS <alias>]    -- partition ID; NULL for non-partitioned queues
    | USER PROPERTIES              [AS <alias>]
    [, ...]
  ]
  [ENVELOPE NONE | UPSERT (KEY (<column>))]
  [WITH (
      ACK WINDOW SIZE = <int>,            -- default 255 (Solace default)
      FLOW MAX UNACKED = <int>,           -- default 10000
      DEDUPLICATE = <bool>,               -- default true; see semantics below
      PARALLELISM = <int>                 -- default: broker partition count; see "Parallel consumers"
  )]
```

Notes on the surface:

- **No `START OFFSET` / `START TIMESTAMP`.** The queue/DTE itself is the cursor. Document this explicitly in error messages if a user tries to specify one.
- **No `KEY FORMAT` / `VALUE FORMAT` split.** Solace messages don't have a separate key field; UPSERT keys come from `INCLUDE`-d metadata columns.
- **`USER PROPERTIES`** should be exposed as `map[text => text]` (matching how Kafka headers are exposed).
- **`TOPIC`** is the *resolved* topic the message was published to, which matters for DTE sources with wildcard subscriptions.

### Topic level decomposition

Solace topics are hierarchical, slash-delimited strings (e.g. `acme/orders/us-east/created`). Each level carries semantic meaning that users almost always want to filter, join, or group by. Requiring users to `split_part(topic, '/', N)` in every downstream view is a tax that the source can eliminate.

**Requirement:** `INCLUDE TOPIC LEVELS` decomposes the resolved topic into one column per level, exposed as `text` columns. The columns are named `<prefix>_1`, `<prefix>_2`, …, `<prefix>_N` where `<prefix>` defaults to `topic_level` and can be overridden via `AS <prefix>`. Examples:

```sql
-- Default naming: topic_level_1, topic_level_2, ...
INCLUDE TOPIC LEVELS

-- Custom prefix: tl_1, tl_2, ...
INCLUDE TOPIC LEVELS AS tl
```

For a message published to `acme/orders/us-east/created`, this produces:

| topic_level_1 | topic_level_2 | topic_level_3 | topic_level_4 |
|---|---|---|---|
| `acme` | `orders` | `us-east` | `created` |

**Column count determination.** The number of columns is fixed at source creation time and must be specified or inferred:

- For sources with a concrete `TOPIC SUBSCRIPTION` containing no wildcards (e.g. `'acme/orders/us-east/created'`), infer the level count from the subscription.
- For sources with wildcard subscriptions (`*`, `>`) or for queue sources (where subscriptions live broker-side and aren't visible in the SQL), the user must specify the column count explicitly:

  ```sql
  INCLUDE TOPIC LEVELS (COUNT = 8) AS tl
  ```

  Messages with fewer levels than `COUNT` get `NULL` in the trailing columns. Messages with *more* levels than `COUNT` get the last column populated with the remaining levels joined back together (preserving the original topic suffix without loss). Document this behavior clearly — users debugging a missing level need to know where it went.

- If `COUNT` is omitted on a wildcard or queue source, the source should fail at `CREATE SOURCE` time with a clear error directing the user to specify it.

**Why `text` and not `text[]`?** A flat-column representation lets users filter and index individual levels directly (`WHERE topic_level_2 = 'orders'`) and is more discoverable in tools that introspect schemas. An array column would force `topic_level_arr[2]` everywhere, which is awkward and harder to optimize. If users want the array form, they can construct it in a view.

**Interaction with `INCLUDE TOPIC`.** `TOPIC` and `TOPIC LEVELS` are independent; users can include either, both, or neither. `TOPIC` gives the original slash-joined string (useful for logging and exact-match cases); `TOPIC LEVELS` gives the decomposition (useful for filtering and joining).

**Implementation note.** The decomposition happens once per message in the source operator, before the row is staged for persist. Don't push it into a downstream view — that defeats the indexing and pushdown benefits of having flat columns.

## Runtime semantics (the important part)

### Connection lifecycle

1. Source startup: open a Solace session using the connection, bind to the queue or DTE, set client-ack mode, set the ack window size.
2. Read the resume RGMID from persist (see "Exactly-once protocol" below).
3. Begin consuming messages.
4. On disconnect: reconnect with exponential backoff, rebind, resume. The broker will redeliver anything unacked.

### Exactly-once protocol

This is the central piece of new logic. Do not skip or simplify.

**What "persist commit" actually means.** Before reading the protocol below, understand the physical reality of a persist commit, because it affects every batching and latency decision in the source:

- Materialize's Persist layer stores all durable data in **S3** (or an S3-compatible blob store: MinIO, GCS, R2, on-prem object stores in self-managed deployments). There is no local-disk durability tier; S3 is the only durable surface.
- A "commit" is two things happening together: (a) a PUT of the batch data to S3, and (b) an update to a distributed transactional database (CockroachDB in Materialize Cloud; Postgres in self-managed) that records the batch's existence and advances the source's frontier. The consensus update is what makes the batch *visible* to readers; the S3 PUT is what makes it *durable*.
- Both must succeed before the source can ack to Solace. If the S3 PUT succeeds but the consensus update fails, the batch is orphaned (and garbage-collected later) — from the source's perspective, the commit failed and the messages must not be acked.
- A persist commit takes tens to hundreds of milliseconds typically, dominated by S3 latency. **Per-message commits are not viable.** The source must batch messages aggressively and amortize the commit cost across many messages.
- Self-managed deployments may use slower or less-reliable blob stores than AWS S3. The source's batching and retry behavior must be robust to commit latencies in the seconds range and to transient S3 errors (5xx, throttling). The Kafka source has battle-tested patterns here — reuse them.

The implication for the protocol: `last_persisted_rgmid` lives *inside* the Persist batch (in S3) and becomes durable atomically with the data. There is no separate "RGMID watermark" storage — it's a column in the source's relation, or a piece of source-specific metadata stored alongside the batch. On restart, recovering it means reading from S3 via Persist's normal read path, not a separate lookup.

**Write path:**

1. Receive a message from Solace. Extract RGMID, payload, metadata.
2. If `DEDUPLICATE = true` (default) and `rgmid <= last_persisted_rgmid`: drop the message and ack it to Solace immediately. This is the post-restart redelivery case.
3. Otherwise, decode the message according to FORMAT, apply ENVELOPE, and stage it for the next persist batch.
4. When the batch is ready to commit: write the data **and** the new `last_persisted_rgmid` (the max RGMID in the batch) atomically as part of the same persist write. Physically: a single S3 PUT for the batch contents (including the watermark) followed by a consensus update that makes it visible.
5. After persist confirms the write is durable (S3 PUT acknowledged *and* consensus update committed): ack the corresponding RGMID range to Solace.

**Restart path:**

1. Read `last_persisted_rgmid` from persist before binding to the queue. This is an S3 read via Persist's normal read path — no special recovery mechanism needed.
2. Bind to the queue. Solace will redeliver everything it considers unacked.
3. For each redelivered message, compare its RGMID against `last_persisted_rgmid`. If `<=`, it's a duplicate from a crash between persist-commit and Solace-ack — drop it and ack it immediately. If `>`, process it normally.
4. Steady-state resumes.

**Why this works:** RGMIDs are monotonic per queue (or per partition, for partitioned queues), so a single "highest seen" watermark per queue/partition is sufficient — no set membership data structure needed. The watermark moves forward with each persist commit. Any RGMID at or below the watermark has, by construction, already been durably written to S3 *and* recorded in consensus.

For partitioned queues, the watermark is a map `partition_id -> max_rgmid` rather than a single scalar; see "Parallel consumers" below for the details.

**Failure modes to handle:**

- Crash between message receipt and persist commit → broker redelivers → dedup check catches it.
- Crash between persist commit and Solace ack → broker redelivers → dedup check catches it.
- S3 PUT succeeds but consensus update fails → batch is orphaned, source treats commit as failed, does not ack to Solace, retries with a fresh batch. Garbage collection will eventually clean up the orphaned S3 object.
- S3 5xx or throttling → retry the PUT with backoff; do not ack to Solace until success.
- Network partition between source and broker → session drops → reconnect, rebind, broker redelivers unacked → dedup catches anything already persisted.
- Network partition between Materialize and S3 → commits stall, ack window fills, Solace flow-controls the source, no data loss.
- Broker HA failover → RGMID remains valid (this is the whole point of RGMID vs the older ADMessageID) → resume cleanly.
- DR replication failover → RGMID still valid (same reason) → resume cleanly.

### Batching strategy

Because each commit is an S3 round-trip, batch size directly trades latency against cost and throughput. Guidelines:

- **Target batch size:** match the Solace `ACK WINDOW SIZE`. A natural rhythm is "fill a window, commit the batch, ack the window, repeat."
- **Time-based flush:** commit any partially-filled batch after a configurable timeout (default ~1 second) to bound end-to-end latency under low message rates. Otherwise a slow trickle of messages would never become visible.
- **Size-based flush:** also commit when the batch reaches a byte threshold, to bound memory and prevent oversized S3 PUTs.
- **Adaptive sizing:** if S3 latency is high, allow batches to grow larger (capped) rather than queuing multiple slow commits in flight. Persist likely already has tuning for this — reuse it rather than inventing source-specific logic.

The user-facing knobs (`ACK WINDOW SIZE`, `FLOW MAX UNACKED`) should compose cleanly with whatever batching Persist already does; do not expose a separate "persist batch size" option on the Solace source.

### When `DEDUPLICATE = false`

The source still acks after persist commit (so the broker isn't holding state forever), but it does *not* compare incoming RGMID against the watermark, so post-restart duplicates flow through to the user. This mode exists for debugging and for users who explicitly want at-least-once semantics. Document it as "escape hatch, not recommended."

### Acking

Acks are issued per message id after persist commit completes, in RGMID order. The Solace C API has no cumulative ack, but the SDK batches the actual transport acks internally (controlled by the flow's ack threshold and ack timer), so the wire cost of per-message `sendAck` calls is already amortized. What the source bounds instead is the FFI cost: the receive loop drains at most a fixed budget of pending acks per iteration, so a large persist commit never stalls message intake behind thousands of synchronous ack calls. The `ACK WINDOW SIZE` option controls the broker's in-flight transport window.

### Backpressure

Solace's `FLOW MAX UNACKED` is the broker's lever for backpressure: if the source has that many messages outstanding, the broker stops delivering. Because every client-side buffer entry (the receive channel and the pending-ack queue) is a delivered-but-unacked message, `FLOW MAX UNACKED` also bounds source memory — if persist is slow, unacked grows, the broker slows down, and no unbounded growth occurs. The exception is `FLOW MAX UNACKED = -1` (delegate to the queue's `max-delivered-unacked-msgs-per-flow`), which is unbounded if the broker-side limit is unlimited; the source warns at flow creation in that configuration.

### Parallel consumers

Solace offers two distinct parallelism stories, and the source supports both — but with materially different guarantees. Be precise about which is which in documentation and error messages; they look similar but have different ordering and exactly-once properties.

**1. Non-partitioned queue with competing consumers (weaker semantics).** Multiple consumers bind to the same queue; the broker fans out messages across them round-robin. No per-consumer ordering is guaranteed (the broker may send message 5 to consumer A and message 6 to consumer B), and RGMIDs are queue-global rather than per-consumer. This is *load sharing*, not parallel ordered processing.

This mode breaks the RGMID watermark invariant: no individual consumer sees a monotonic RGMID sequence, so the "highest seen" watermark would incorrectly skip messages that other consumers are still processing. The source supports this mode anyway because users have legitimate operational reasons to need it (existing non-partitioned queues they can't restructure, broker-version constraints, ops policies), but with explicitly weakened guarantees:

- **Exactly-once is disabled.** `DEDUPLICATE = true` is rejected at `CREATE SOURCE` time when `PARALLELISM > 1` is specified on a non-partitioned queue. The user must explicitly set `DEDUPLICATE = false` to opt into this mode, making the at-least-once semantics a deliberate choice rather than a silent downgrade.
- **No watermark is maintained.** The source still acks after persist commit (so the broker doesn't hold messages forever), but no `last_persisted_rgmid` is tracked or compared. On restart, the broker redelivers unacked messages and they flow through to persist without dedup. Users will see duplicates after any crash or rebalance.
- **No ordering guarantees.** Downstream views cannot assume any RGMID monotonicity. If users need ordering for their use case, they must derive it from message contents (e.g. a sender-side sequence number in the payload) and dedup in SQL with `DISTINCT ON`.
- **Clear error messages.** If a user specifies `PARALLELISM > 1` and `DEDUPLICATE = true` together on a non-partitioned queue, the error must explain both the underlying constraint (RGMID is queue-global, not per-consumer) and the fix (use a partitioned queue for exactly-once parallelism, or set `DEDUPLICATE = false` to accept at-least-once).
- **Documentation must warn prominently.** The reference page should call out this mode as "weaker semantics, opt-in" with a comparison table against the partitioned-queue mode. Users reading the SQL syntax alone should never end up here by accident.

**2. Partitioned queue (full exactly-once semantics).** A single logical queue with N internal partitions. Each partition has independent ordering and its own RGMID sequence. The broker assigns partitions to consumers (similar to Kafka consumer-group partition assignment), so each consumer owns a stable subset of partitions and sees monotonic RGMIDs within each owned partition. This is the recommended parallelism model and preserves the exactly-once guarantee.

**Where parallelism is defined:**

The source operator runs with N workers. Each worker is one Solace consumer; the broker handles partition assignment (for partitioned queues) or round-robin distribution (for non-partitioned queues) via its standard protocols. Worker count is determined as follows:

- **Default behavior for partitioned queues:** at source startup, query the broker's metadata for the partitioned queue and create one worker per partition. This is the right answer for most users.
- **Default behavior for non-partitioned queues:** `PARALLELISM = 1` (single consumer, full exactly-once). Users must explicitly set `PARALLELISM > 1` to opt into competing-consumer mode.
- **Explicit override** via a `WITH` option:

  ```sql
  WITH (PARALLELISM = 4)
  ```

  For partitioned queues, allows binding fewer workers than partitions exist (the broker will assign multiple partitions to each worker, reducing parallelism but saving cluster resources). Binding *more* workers than partitions is rejected with a clear error — the extra workers would sit idle.

  For non-partitioned queues, `PARALLELISM > 1` activates competing-consumer mode and requires `DEDUPLICATE = false` per the rules above.

**Decision matrix (put this in the user docs):**

| Queue type | `PARALLELISM` | `DEDUPLICATE` | Result |
|---|---|---|---|
| Non-partitioned | 1 (default) | true (default) | Single consumer, exactly-once. Recommended. |
| Non-partitioned | 1 | false | Single consumer, at-least-once. Rarely useful. |
| Non-partitioned | > 1 | true | **Rejected at `CREATE SOURCE`.** Error directs user to partitioned queue. |
| Non-partitioned | > 1 | false | Competing consumers, at-least-once, no ordering. Weaker mode. |
| Partitioned | default (= partition count) | true (default) | Full parallelism, exactly-once. Recommended for high throughput. |
| Partitioned | < partition count | true | Reduced parallelism, exactly-once. |
| Partitioned | > partition count | any | **Rejected at `CREATE SOURCE`.** Error directs user to lower parallelism or repartition. |
| Partitioned | any valid | false | Parallelism with at-least-once. Escape hatch for debugging. |

**Impact on the exactly-once protocol:**

Per-partition RGMID sequences mean the watermark for partitioned queues is no longer a single value but a vector — one watermark per partition. Specifically:

- Each persist batch records the max RGMID seen *per partition* in that batch.
- The watermark stored alongside the data is a map `partition_id -> max_rgmid`.
- On restart, each worker reads the watermark for its assigned partitions and uses per-partition comparison for the dedup check.
- Partition assignment can change across restarts (the broker may rebalance). The watermark map is keyed by partition ID, not worker ID, so a worker that picks up a new partition correctly reads that partition's prior watermark.

For non-partitioned queues with `PARALLELISM = 1`, the watermark is a single scalar as originally described. For non-partitioned queues with `PARALLELISM > 1`, no watermark is maintained (because exactly-once is disabled).

**Worker coordination:**

Workers are independent — each owns its partitions (or competes for messages on a non-partitioned queue), manages its own Solace session, commits its own persist batches, and acks to Solace independently. There is no cross-worker coordination needed. The only shared state is the per-partition watermark map in Persist (for partitioned queues in exactly-once mode), updated atomically per worker per commit.

**Cluster replicas vs. parallelism:**

Note for documentation: Materialize cluster replicas are for HA, not parallel ingestion. Only one replica actively ingests; others stand by. Users should not confuse `REPLICATION FACTOR` (HA) with `PARALLELISM` (worker count within a source). Spell this out in the user-facing docs to avoid the confusion.

**Repartitioning on the broker side:**

Solace partitioned queues can be reconfigured to change partition count. The source must handle this gracefully:

- If partition count increases, new partitions appear with no prior watermark — treated as starting from "no prior state."
- If partition count decreases, watermarks for removed partitions become stale but harmless (they're just unused map entries; garbage-collect them on the next commit).
- The source should detect partition count changes at reconnect time and log a warning if the configured `PARALLELISM` no longer matches.

## Component breakdown

Roughly, the work decomposes into:

1. **SQL parser and AST** — `CREATE CONNECTION ... TO SOLACE`, `CREATE SOURCE ... FROM SOLACE`. Add new AST nodes; extend the parser.
2. **Catalog and planner** — connection validation, source planning, `INCLUDE` column expansion, `ENVELOPE UPSERT` key validation against `INCLUDE`-d columns.
3. **Solace client integration** — pick a Rust Solace client. Options: the official `solace-rs` bindings to the PubSub+ Messaging API for C (the `solclient` library), or write a thin FFI wrapper if `solace-rs` is insufficient. The C library is the canonical client; the Java JCSMP API is reference for semantics but not usable from Rust. Avoid pure-Rust reimplementations; SMF protocol details and ack semantics are too easy to get wrong.
4. **Ingestion dataflow operator** — the new source operator: bind, consume, decode, dedup-check, hand off to persist sink, ack on commit. Follow the Kafka source operator's structure.
5. **Persist integration** — extend the source's commit protocol to write `last_persisted_rgmid` atomically with the data. This may require touching shared persist write paths; coordinate with whoever owns persist.
6. **Metadata exposure** — implement each `INCLUDE` column. RGMID needs a representation (probably `bytea` since it's broker-opaque, or a struct if you want users to compare them — recommend `bytea` and provide a comparison function). `TOPIC LEVELS` needs schema-time column expansion (the column count is part of the source's relation type, fixed at `CREATE SOURCE` time) plus runtime splitting on `/` with the trailing-suffix-preservation behavior described in the surface section.
7. **Error handling** — bind failures, auth failures, message decode errors, persist write failures, broker disconnects. Each needs a clear error path and recovery behavior.
8. **Testing** (see below).

## Testing

1. **Unit tests** — parser, planner, individual operator pieces, RGMID comparison.
2. **Integration tests against a real broker** — use the Solace PubSub+ Standard Docker image (free, no license needed for non-prod). Cover:
   - Basic queue consumption with each FORMAT.
   - DTE consumption with `*` and `>` wildcards.
   - All `INCLUDE` columns return correct values.
   - `INCLUDE TOPIC LEVELS` with both inferred count (concrete subscription) and explicit `COUNT` (wildcard / queue source), including the edge cases: message with exactly `COUNT` levels, fewer levels (NULL padding), and more levels (trailing suffix preserved in last column).
   - `ENVELOPE UPSERT` deduplicates by application message ID.
3. **Crash/restart tests** — the critical ones. For each scenario, verify no data loss and no duplicates in the output:
   - Kill the source process mid-batch (between receipt and persist commit).
   - Kill the source process between persist commit and Solace ack.
   - Force a broker HA failover mid-stream (Docker image supports this with a two-node config).
   - Network partition between source and broker, then heal.
   - Restart with `DEDUPLICATE = false` and verify duplicates *are* visible (negative test).
4. **Partitioned queue tests (exactly-once parallelism):**
   - Basic consumption from a partitioned queue with N partitions and N workers; verify per-partition ordering is preserved in the output.
   - `PARALLELISM` set lower than partition count (e.g. 8 partitions, 4 workers); verify all partitions are consumed and per-partition ordering holds.
   - `PARALLELISM` set higher than partition count → reject at `CREATE SOURCE` time.
   - Per-partition watermarks survive restart: kill the source mid-stream, restart, verify no duplicates and no loss *per partition*.
   - Broker-side partition assignment rebalance (e.g. add a new worker mid-stream) handled without loss or duplicates.
   - Repartitioning on the broker (changing partition count while running): verify graceful behavior described in "Parallel consumers."
5. **Competing-consumer mode tests (weaker semantics):**
   - `PARALLELISM > 1` on a non-partitioned queue with `DEDUPLICATE = true` → reject at `CREATE SOURCE` time with a clear error message.
   - `PARALLELISM > 1` on a non-partitioned queue with `DEDUPLICATE = false` → accepted; verify messages are consumed across workers and total throughput scales.
   - Crash/restart in competing-consumer mode: verify duplicates *are* visible after restart (expected behavior, not a bug) and no data is lost.
   - Verify ordering is *not* preserved across workers in this mode (negative test — important to confirm we're not accidentally providing stronger guarantees than documented).
   - Documentation rendering: confirm the decision matrix and warnings appear correctly in the generated reference docs.
6. **Throughput / backpressure** — verify that with a slow downstream, the source doesn't OOM and the broker's unacked count is bounded by `FLOW MAX UNACKED`.
7. **S3 fault injection** — simulate S3 5xx errors, throttling, and elevated latency. Verify the source backs off and retries cleanly, that Solace is not acked until S3 succeeds, and that no data is lost or duplicated. A local MinIO instance with a chaos-injection proxy (e.g. toxiproxy) is sufficient for this.
8. **Long-running soak** — 24h+ run with a steady message stream and periodic random restarts. Verify final row count matches expected exactly.

## Documentation deliverables

Alongside the code, produce:

1. **`CREATE SOURCE: Solace`** reference page following the same structure as the Kafka reference page.
2. **`Ingest data from Solace`** how-to guide covering queue setup, DTE setup, auth options, and the exactly-once guarantee.
3. **Architecture doc** (internal) explaining the RGMID-watermark exactly-once protocol, so future maintainers don't break it.

## Open questions to resolve before or during implementation

These were flagged in design discussion and need decisions, not just code:

1. **Queue provisioning.** Source assumes queue/DTE exists, or provisions on first use? Recommend "assumes exists" for v1 to avoid owning broker policy decisions (redelivery limits, DMQ targets, etc.).
2. **Non-durable subscriptions.** Should there be a `TOPIC` (no durable endpoint) variant for direct/non-guaranteed delivery? This would be closer to the webhook source's semantics — no exactly-once, no RGMID. Recommend punting to a follow-up; out of scope for the guaranteed-messaging source.
3. **Schema registry.** Solace has its own schema registry product. Decide whether to support it as a `CONNECTION TO SOLACE SCHEMA REGISTRY` or only support Confluent Schema Registry (which is what Kafka uses and what users will likely already have).

Resolve these by writing a short design note for each and getting sign-off before locking the surface.

## Out of scope for this work

- Solace as a *sink* (separate work item).
- Non-guaranteed (direct) messaging.
- Solace event broker management API integration (queue creation, ACL management, etc.).
- Migration tooling from existing Kafka sources to Solace sources.

## Definition of done

- All SQL surface implemented and parseable.
- All integration tests pass against a real Solace broker.
- All crash/restart tests pass with zero duplicates and zero loss under `DEDUPLICATE = true`.
- Soak test passes.
- Reference and how-to docs published.
- Internal architecture doc reviewed by at least one persist maintainer and one source-system maintainer.

Start by reading the Kafka source code and the Solace JCSMP guarantees documentation, then write a short design note (one page) describing how you intend to plug the exactly-once protocol into Materialize's existing persist commit path. Get that reviewed before writing implementation code.
