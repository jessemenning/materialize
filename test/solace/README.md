# Solace connector — end-to-end tests

Runs a `solace/solace-pubsub-standard` broker alongside `materialized` and
exercises both `CREATE SOURCE … FROM SOLACE` and `CREATE SINK … INTO SOLACE`.

## Workflows

### Source workflows

| Workflow | What it covers |
|---|---|
| `round-trip` (default) | Publish 5 messages, verify they all flow through; verify `INCLUDE REPLICATION GROUP MESSAGE ID` and `INCLUDE BROKER TIMESTAMP` are populated. |
| `exactly-once` | Publish 5 messages, wait for them to land, kill `materialized`, restart, publish 5 more. Verify the final count is exactly 10 distinct rows — i.e., no duplicates from dedup-on-restart, no loss from the persisted-but-not-yet-acked window. |

### Sink workflows

Sink-side verification uses **stm** (solace-tryme-cli), a Node.js CLI that
consumes from a Solace queue via WebSocket (`ws://solace:8008`). stm is
installed from npm into a `node:20-alpine` container at workflow start
(first-run cost ~60 s; subsequent runs use Docker layer cache).

Each sink test pre-creates a durable queue with a topic subscription that
intercepts the sink's topic-published messages, then uses stm to consume and
assert on count and payload.

| Workflow | What it covers |
|---|---|
| `sink-round-trip` | Creates a sink writing to a static topic, inserts 5 rows, verifies all 5 JSON payloads arrive via stm. |
| `sink-dedup` | Creates a sink with `DEDUP WINDOW '3s'`, inserts 3 rapid rows to the same topic (only 1 published), waits for window expiry, inserts 1 more (published). Verifies exactly 2 messages. |
| `sink-dynamic-topic` | Sink TOPIC uses `{region}` placeholder. Inserts rows with different regions + 1 NULL (silently dropped). Verifies 4 messages each on the correct per-region topic. |
| `sink-reconnect` | Publishes pre-crash rows, kills Solace, inserts rows during outage (lost — Direct delivery), restarts broker, verifies sink reconnects and post-restart rows publish normally. |

## Running locally

```bash
./bin/mzcompose --find solace down -v          # tear down anything still up

# Source tests
./bin/mzcompose --find solace run round-trip
./bin/mzcompose --find solace run exactly-once

# Sink tests
./bin/mzcompose --find solace run sink-round-trip
./bin/mzcompose --find solace run sink-dedup
./bin/mzcompose --find solace run sink-dynamic-topic
./bin/mzcompose --find solace run sink-reconnect
```

First-run cost includes pulling the Solace image (≈600 MB). Boot time is
~30–40 s on a laptop; the `Solace` service definition's healthcheck has a
300 s `start_period` to absorb that on both laptops and slower CI runners
(which can take 3–5 minutes on a cold pull), followed by 30 × 2 s retries.

## Resource requirements

The Solace Standard image is heavier than most Materialize test
dependencies:

* **Shared memory:** 1 GB (set via `shm_size` on the service).
* **File descriptors:** soft 2448 / hard 6592 (set via `ulimits` on the
  service).
* **Memory:** ~512 MB resident at idle.
* **Boot time:** 25–40 s before SEMP/SMF accept connections.

If Docker on your machine has lower defaults you may need to bump shm_size
in `~/.docker/daemon.json` and / or restart the Docker engine.

## Troubleshooting

### Broker crashes immediately — "Unable to raise event; rc(would block)"

The Solace broker's internal consul process requires a high
`vm.max_map_count`. The Linux default (65530) is too low; the broker exits
with code 2 within seconds of starting.

Fix (takes effect immediately, lost on reboot):
```bash
sudo sysctl -w vm.max_map_count=512000
```

Fix permanently (survives reboots):
```bash
echo "vm.max_map_count=512000" | sudo tee /etc/sysctl.d/99-solace.conf
sudo sysctl --system
```

On **WSL2** you must set this inside the WSL2 kernel, not the Windows host.
The commands above work inside a WSL2 terminal. To persist across WSL2
restarts, add `vm.max_map_count=512000` to `/etc/sysctl.d/99-solace.conf`
inside WSL2 and run `sudo sysctl --system`.

The mzcompose workflows attempt to set this automatically via `sudo sysctl`
before starting the broker. If that fails (no sudo) a warning is printed.

### Broker stays in `(health: starting)` for more than 6 minutes

Check the container logs for errors:
```bash
docker logs solace-solace-1 2>&1 | tail -30
```
The most common causes are `vm.max_map_count` too low (see above) or
insufficient shared memory (`shm_size`). The service definition sets 1 GB;
verify Docker is not capping it lower.

## Broker provisioning

The workflow provisions the broker before running testdrive scripts:

1. Waits for SEMP to become healthy (`/SEMP/v2/__about` returns 200).
2. Creates client-username `mz_user` with password `changeme` via
   `POST /SEMP/v2/config/msgVpns/default/clientUsernames`.
3. Creates durable queue `mz_test_q` (exclusive, ingress+egress enabled) via
   `POST /SEMP/v2/config/msgVpns/default/queues`.

Both calls are idempotent; rerunning the workflow against a still-warm
broker is supported.

## Message ingestion

The testdrive scripts publish messages via the broker's REST messaging
endpoint (port 9000), which uses the same client-username authentication as
SMF — meaning the same `mz_user` we created for Materialize works for
testdrive publishing too.

```
POST http://mz_user:changeme@solace:9000/QUEUE/mz_test_q
Content-Type: text/plain

hello-1
```

This is a guaranteed-messaging publish (REST → SMF translation inside the
broker); the broker stages the message on the queue's persistent spool, and
Materialize's source consumes it via the SMF flow it has bound to that
queue.
