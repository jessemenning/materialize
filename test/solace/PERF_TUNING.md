# Solace Source Connector — Performance Tuning Reference

**Branch:** `fix/solace-probe-frontier`
**Test workload:** 500 msg/sec × 600s = 300,000 JSON messages, 3 cascading MVs, 1 Solace sink
**Hardware:** WSL2 on local dev machine (not CI-representative for absolute numbers)

---

## Latency model

End-to-end lag (wall-clock minus `send_ts_ms` visible in mv3) decomposes into three
independent terms:

```
lag_ms ≈ broker_lag + probe_wait + mv_cascade
```

| Term | Typical value | What drives it |
|------|--------------|----------------|
| `broker_lag` | +2–5ms | Time between message publish and broker timestamp assignment. Near-zero means the broker stamps on receipt. |
| `probe_wait` (`src_ms`) | ≈ probe_interval / 2 | A `SELECT` against the source blocks until the next frontier advance (probe). On average, a message waits half the probe interval. |
| `mv_cascade` (`mv3_ms`) | 190–300ms | Time for mv1→mv2→mv3 to recompute after a frontier advance. Grows with probe frequency (more re-evaluations per second = more contention). |

**Key insight:** reducing probe interval reduces `probe_wait` linearly, but increases
`mv_cascade` because compute recomputes more often. There is a crossover point where
more-frequent probing makes lag worse, not better.

---

## Configuration knobs

All three are read **once at source startup** and cached for the lifetime of the source
worker. `ALTER SYSTEM SET` must be issued **before `CREATE SOURCE`**; changes after the
source is running have no effect until the source is dropped and recreated.

| Dyncfg | Default | Effect |
|--------|---------|--------|
| `solace_probe_interval` | 1s | Timer-based probe cadence. Controls `probe_wait` in steady state. |
| `solace_event_probe_min_gap` | 0 (disabled) | Minimum gap between event-driven probes emitted after each message batch. Enables probing at message-arrival rate rather than fixed timer rate. |
| `solace_catchup_probe_enabled` | true | When a full batch (≥1000 msgs) is drained, emit a probe immediately. Useful for backlog catch-up on restart. No effect in steady state at 500 msg/sec. |

Source-level options (set in `CREATE SOURCE ... FROM SOLACE`):

| Option | Throughput goal | Latency goal | Effect |
|--------|----------------|--------------|--------|
| `ACK MODE` | `auto` | `client` | `client` = ack after persist-commit (exactly-once); `auto` = ack on delivery (at-most-once, higher throughput) |
| `DEDUPLICATE` | `false` | `true` | Watermark-based dedup on restart; adds ~208ms of persist latency per message batch when true |
| `PARALLELISM` | 4 | 1 | Worker count. More workers increase throughput but require a non-exclusive queue. |
| `ACK WINDOW SIZE` | 4096 | 255 | Broker-side max unacked messages before flow control. |
| `FLOW MAX UNACKED` | -1 (broker default) | 10000 | Cap on unacked messages in flight. |

---

## Test results

All runs: 500 msg/sec, 300k messages, `--goal throughput` unless noted.

| Run | `probe_interval` | `event_probe_min_gap` | avg lag | `src_ms` | `mv3_ms` | q_backlog (peak) | Notes |
|-----|-----------------|----------------------|---------|----------|----------|-----------------|-------|
| Baseline (pre-fix) | 500ms | off | ~1900ms | ~950ms | ~190ms | — | Probe carried stale frontier; doubled minimum latency |
| Post probe-fix, 500ms | 500ms | off | 712ms | 454ms | 190ms | 300k (broken) | `q_backlog` metric was broken (used wrong SEMP fields) |
| Post probe-fix, 200ms | 200ms | off | 653ms | 184ms | 205ms | 0–153 | First clean run with working `q_backlog` |
| Latency goal, 200ms | 200ms | off | ~645ms | ~240ms | ~200ms | 0–50 | `ack_mode=client` + `deduplicate=true` added ~208ms persist latency, mostly cancelling the probe gain |
| Event probe, 50ms gap | 200ms | 50ms | **984ms** | 224ms | **294ms** | 0–135 | Worse than no event probing — compute saturated, cascade ~45% slower |

### q_backlog behaviour

`q_backlog = lastSpooledMsgId − highestAckedMsgId` (SEMP v2 monitor endpoint).

This is the reliable pending-message count: it measures messages published minus the
highest application-level ACK watermark. `spooledMsgCount` is **not** reliable with
`ack_mode=auto` — it is cumulative (total ever spooled) and does not decrease as
messages are consumed.

In all tested configurations the queue **did not back up**. Materialize consumed at
publish rate; backlog stayed 0–153 messages throughout 300k-message runs.

---

## Probe/frontier fix (the main latency improvement)

**Root cause of the baseline ~1900ms lag:**

The source loop had two separate timers — `probe_interval` (priority 2) and
`frontier_tick` (priority 3). In the biased select, `probe_interval` fired first,
emitting a probe carrying the stale `data_cap.time() = max_ts`. The actual committed
boundary (`max_ts.next()`) was still pending in `frontier_tick`, which fired one cycle
later. Result: each probe was one cycle stale, doubling minimum query latency.

**Fix:** merged `frontier_tick` into `probe_interval`. Before emitting the probe, flush
`pending_frontier` into `data_cap`. This ensures every probe carries the current
committed boundary. Implemented in `src/storage/src/source/solace.rs`.

**Measured improvement:** ~1900ms → 712ms at 500ms probe interval (~63% reduction).

---

## Event-driven probing — findings

**Hypothesis:** emitting a probe after each message batch (rather than waiting for the
timer) should reduce `probe_wait` from `probe_interval/2` to `event_probe_min_gap/2`.
At 50ms gap and 500ms interval, expected `src_ms` reduction: 184ms → ~25ms.

**Result:** lag increased from 653ms to 984ms.

**Why it made things worse:**

At 500 msg/sec with 50ms min gap, event-driven probing fires ~20 probes/sec instead of
~5/sec. Each probe triggers a full mv1→mv2→mv3 cascade recomputation. `mv3_ms`
increased from 205ms to 294ms — the compute overhead from 4× more frequent
recomputation exceeded the `probe_wait` savings.

**The crossover point** depends on hardware (compute throughput per re-evaluation).
On this dev machine, the threshold is somewhere between 5 and 20 probes/sec.

**The queue never backed up** — the bottleneck was compute re-evaluation speed, not
ingest capacity.

**Next steps for event-driven probing:**

1. Try `--event-probe-min-gap 100ms` (10 probes/sec) — likely still above the crossover
   on this hardware, but worth measuring.
2. Try on production-grade hardware where the cascade recomputation is faster.
3. Consider making event-driven probing conditional on `mv3_ms < threshold` so it
   backs off automatically when compute is saturated.

---

## Lag budget at current best config (200ms timer, no event probe)

```
broker_lag   +3ms    (broker stamps on receipt — no room to improve)
probe_wait   184ms   (≈ probe_interval/2; set probe_interval lower to reduce)
mv_cascade   205ms   (dominant term; limited by compute throughput on this hardware)
─────────────────
floor        392ms   (sum of averages; actual avg 653ms due to query timing jitter)
```

The gap between the floor (392ms) and the measured avg (653ms) reflects polling
alignment: when a query lands just after a frontier advance it sees a near-zero wait;
when it lands just before the advance it waits nearly the full probe interval. The
reported `lag_ms` is wall-clock at poll time, not at frontier-advance time, so it
includes this jitter.

---

## How to run the perf benchmark

```bash
# Tear down any previous run
bin/mzcompose --find solace down

# Run via the wrapper script — handles Docker cache pre-warm automatically.
# Pulls ghcr tag derived from current git branch (or :latest on main).
test/solace/run-perf.sh \
  --goal throughput \
  --probe-interval 200ms \
  --rate 500 \
  --duration 600 \
  --poll-interval 60

# Override the ghcr tag if needed (e.g. to test a specific branch build):
# test/solace/run-perf.sh --ghcr-tag fix-solace-probe-frontier --goal throughput ...

# Optional: event-driven probing (currently makes things worse at 50ms on dev hardware)
# --event-probe-min-gap 50ms
```

**Goal presets** (`--goal`):

| Goal | `ack_mode` | `deduplicate` | `parallelism` | Use for |
|------|-----------|--------------|--------------|---------|
| `throughput` | auto | false | 4 | Max throughput, at-most-once |
| `balanced` | auto | false | 1 | Single-worker baseline |
| `latency` | client | true | 1 | Exactly-once, latency measurement |

---

## Files changed on this branch

| File | Change |
|------|--------|
| `src/storage/src/source/solace.rs` | Probe/frontier fix; event-driven probe implementation |
| `src/storage-types/src/dyncfgs.rs` | Added `SOLACE_EVENT_PROBE_MIN_GAP`, `SOLACE_CATCHUP_PROBE_ENABLED` |
| `src/storage/src/sink/solace.rs` | Fixed clippy: `HashMap→BTreeMap`, `.zip→.zip_eq` |
| `test/solace/mzcompose.py` | Full perf workflow: background publisher, poll loop, lag decomposition report, SEMP q_backlog metric |
| `test/solace/perf-setup.td` | DDL for perf source, MVs, sink |
