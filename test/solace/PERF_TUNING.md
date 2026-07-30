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
| `mv_cascade` (`mv3_ms`) | 190–300ms | Time for mv1→mv2→mv3 to recompute after a frontier advance. One recomputation per distinct downstream timestamp, which is now capped at one per probe interval. |

**Key insight (updated 2026-07-30):** probing now goes through `probe::Ticker`
(`src/storage/src/source/probe.rs`), which rounds probe timestamps down to multiples
of `solace_probe_interval`. The remap operator only mints a binding for a strictly
newer probe timestamp, so remap minting (one persist compare-and-append plus a listen
read-back) happens exactly once per interval, idle or busy, regardless of message
rate. `probe_wait ≈ probe_interval/2` is the only interval-dependent term left.
Downstream dataflows see one distinct timestamp per interval, so shrinking the
interval should no longer inflate `mv_cascade` as severely as before. The old
crossover behaviour documented below (653ms → 984ms under 50ms event probing) was
measuring timestamp churn from the event-probe mechanism, which has been deleted.
The interval matrix still needs re-measurement under the new machinery.

---

## Configuration knobs

`solace_probe_interval` is re-read by the probe ticker after every tick, so
`ALTER SYSTEM SET` applies to **running** sources. Setting it before `CREATE SOURCE`
is no longer required, though doing so avoids one interval of mixed cadence while the
new value takes effect.

| Dyncfg | Default | Effect |
|--------|---------|--------|
| `solace_probe_interval` | 200ms | Probe cadence and the quantum probe timestamps are rounded down to. Controls `probe_wait` and caps remap minting at one binding per interval. Live-tunable. |
| `solace_rgmid_order_validation` | false | Diagnostic: cross-checks byte-wise RGMID ordering against the C SDK comparator per message pair. Costs one FFI call per message, leave off in perf runs. |

**Removed knobs (2026-07-30):** `solace_event_probe_min_gap` and
`solace_catchup_probe_enabled` no longer exist. Under probe rounding, minting is
hard-capped at one binding per interval regardless of message rate, which made both
the event-driven and catch-up probe paths dead code. They were deleted along with
their dyncfgs. Historical measurements of those paths are preserved below.

Source-level options (set in `CREATE SOURCE ... FROM SOLACE`):

| Option | Throughput goal | Latency goal | Effect |
|--------|----------------|--------------|--------|
| `ACK MODE` | `auto` | `client` | `client` = ack after persist-commit (exactly-once); `auto` = ack on delivery (at-most-once, higher throughput) |
| `DEDUPLICATE` | `false` | `true` | Watermark-based dedup on restart; adds ~208ms of persist latency per message batch when true |
| `PARALLELISM` | 1 | 1 | The reader is now clamped to a single hash-chosen worker. Values > 1 log a warning and have no effect. Multiple probe loops used to stall remap minting via the last-writer-wins probe slot. True parallelism needs partitioned queues (future work). |
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

> **Note (2026-07-30):** all runs above predate the `probe::Ticker` rounding change.
> The event-probe row measured a mechanism that has since been deleted. The other
> rows remain the best available baselines but the interval matrix should be re-run
> under the new machinery.

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

## Event-driven probing — findings (historical, mechanism removed 2026-07-30)

> The event-driven probe path and its dyncfg were deleted when probing moved to
> `probe::Ticker` rounding, which caps minting at one binding per interval and made
> the path dead code. The findings below are kept because they motivated the change.

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

**Resolution (2026-07-30):** rather than tuning the event-probe gap or adding
adaptive backoff, the mechanism was removed. Probe rounding through `probe::Ticker`
gives the intended outcome directly, one distinct downstream timestamp per interval,
without a probe rate that scales with message rate. The remaining tuning question is
the plain interval matrix (100ms/200ms/500ms/1s), to be re-measured.

---

## Lag budget at current best config (200ms timer, no event probe)

> Measured before the 2026-07-30 `probe::Ticker` change. Steady-state behaviour at
> 200ms is expected to be roughly unchanged, but the budget should be re-measured.

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
```

The `--event-probe-min-gap` flag was removed from `mzcompose.py` along with the
event-driven probe path (2026-07-30). `--probe-interval` can also be changed on a
running source via `ALTER SYSTEM SET solace_probe_interval`.

**Goal presets** (`--goal`):

| Goal | `ack_mode` | `deduplicate` | `parallelism` | Use for |
|------|-----------|--------------|--------------|---------|
| `throughput` | auto | false | 1 | Max throughput, at-most-once |
| `balanced` | auto | false | 1 | Single-worker baseline |
| `latency` | client | true | 1 | Exactly-once, latency measurement |

All presets now use `parallelism 1`. The source is clamped to a single worker, so
higher values only produce a warning.

---

## Files changed on this branch

| File | Change |
|------|--------|
| `src/storage/src/source/solace.rs` | Probe/frontier fix; event-driven probe implementation (event probing since removed, 2026-07-30) |
| `src/storage-types/src/dyncfgs.rs` | Added `SOLACE_EVENT_PROBE_MIN_GAP`, `SOLACE_CATCHUP_PROBE_ENABLED` (both since removed, 2026-07-30) |
| `src/storage/src/sink/solace.rs` | Fixed clippy: `HashMap→BTreeMap`, `.zip→.zip_eq` |
| `test/solace/mzcompose.py` | Full perf workflow: background publisher, poll loop, lag decomposition report, SEMP q_backlog metric |
| `test/solace/perf-setup.td` | DDL for perf source, MVs, sink |
