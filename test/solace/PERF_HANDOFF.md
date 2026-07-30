# Solace Source Connector — Performance Testing Handoff

**Branch:** `fix/solace-probe-frontier` (merged to fork `main` 2026-06-08)
**Repo:** `SolaceDev/materialize-solace` (fork of `MaterializeInc/materialize`)
**Pre-built images:** `ghcr.io/jessemenning/materialize-solace/{materialized,testdrive}:latest`

---

## What was done

### 1. Root-cause investigation and fix — probe/frontier ordering bug

Identified and fixed the dominant latency bug in `src/storage/src/source/solace.rs`.

**Root cause:** Two separate timers governed frontier advancement (`frontier_tick`,
priority 3) and probe emission (`probe_interval`, priority 2). In Timely's biased
`select!`, `probe_interval` always fired before `frontier_tick`. Every probe therefore
carried a stale frontier (the previous batch's timestamp), causing each message to wait
an extra probe interval before becoming visible to queries.

**Fix:** Merged `frontier_tick` into `probe_interval`. The probe now flushes
`pending_frontier` into `data_cap` before emitting, so every probe carries the current
committed boundary.

**Measured impact:** avg end-to-end lag dropped from ~1900ms to ~650ms at 500ms probe
interval — a 63% reduction.

Relevant commit: `f2b35003510`

### 2. Configurable probe interval (`SOLACE_PROBE_INTERVAL`)

The probe cadence was hardcoded at 1s. Added a dyncfg so operators can tune the
probe_wait / compute-overhead tradeoff without recompiling.

```sql
ALTER SYSTEM SET solace_probe_interval = '200ms';
```

### 3. Event-driven probing infrastructure (`SOLACE_EVENT_PROBE_MIN_GAP`) — since removed

Added a dyncfg to emit probes inline after each message batch, at a minimum gap rate,
rather than waiting for the fixed timer. Intended to reduce `probe_wait` at the cost of
more frequent compute re-evaluations.

```sql
ALTER SYSTEM SET solace_event_probe_min_gap = '50ms';
```

**Finding:** At 50ms gap (20 probes/sec) on dev hardware, `mv_cascade` increased from
205ms to 294ms. Compute saturation exceeded the `probe_wait` savings. Net lag increased
from 653ms to 984ms. See [PERF_TUNING.md](PERF_TUNING.md) for full analysis.

Relevant commit: `2b2b7775e78`

**Update 2026-07-30:** this path and its dyncfg were deleted. See the probe machinery
overhaul section at the bottom of this document.

### 4. Full performance benchmark workflow

Built out `workflow_perf` in `mzcompose.py` with:

- Background SMF publisher (configurable rate, duration, message size)
- Repeating lag-decomposition poll loop: reports `src_ms`, `mv3_ms`, `q_backlog`
- SEMP v2 queue depth via `lastSpooledMsgId − highestAckedMsgId` (reliable across all
  ack modes — see [QUEUE_DEPTH_MEASUREMENT.md](QUEUE_DEPTH_MEASUREMENT.md))
- Goal presets (`--goal throughput | balanced | latency`) that configure ack mode,
  deduplication, and parallelism in one flag
- `--event-probe-min-gap` arg wired to `ALTER SYSTEM SET` before source creation
  (removed 2026-07-30 along with the event-probe path)
- Dynamic ghcr tag derivation from current git branch

### 5. Docker cache pre-warm workaround (`run-perf.sh`)

mzbuild computes content-addressed fingerprint hashes before any Python workflow code
runs. Changed source files → new hash → mzbuild falls back to local Rust recompile
(multi-hour) instead of pulling the pre-built ghcr image.

`test/solace/run-perf.sh` runs the ghcr pull and retag step via `bin/pyactivate`
before invoking `bin/mzcompose`. This ensures images are in the local Docker cache with
the correct fingerprint hash before mzbuild's acquisition phase.

Relevant commit: `c4195f56b30`

---

## Environment and prerequisites

| Requirement | Notes |
|-------------|-------|
| Docker (with ghcr.io pull access) | `docker login ghcr.io` with a PAT that has `read:packages` |
| Materialize repo tools | `bin/pyactivate`, `bin/mzcompose` — both available in repo root |
| Solace Platform broker | Started automatically by mzcompose as a Docker container |
| `GHCR_TOKEN` env var | Optional; only needed if `docker login` is not persistent |

The test stack spins up everything (broker, materialized, testdrive) in Docker
containers. No external Solace instance is required.

---

## Running the benchmark

```bash
# Tear down any previous run first.
bin/mzcompose --find solace down

# Standard throughput run — pulls images from ghcr automatically.
test/solace/run-perf.sh \
  --goal throughput \
  --probe-interval 200ms \
  --rate 500 \
  --duration 600 \
  --poll-interval 60

# Override the ghcr tag (e.g. to test a specific feature-branch build):
test/solace/run-perf.sh --ghcr-tag fix-solace-probe-frontier --goal throughput ...
```

The `--event-probe-min-gap` flag no longer exists (removed 2026-07-30 with the
event-probe path). `--probe-interval` is now also live-tunable on a running source
via `ALTER SYSTEM SET solace_probe_interval`.

**Goal presets:**

| `--goal` | `ack_mode` | `deduplicate` | `parallelism` |
|----------|-----------|--------------|--------------|
| `throughput` | auto | false | 1 |
| `balanced` | auto | false | 1 |
| `latency` | client | true | 1 |

All presets use `parallelism 1` since 2026-07-30. The source is clamped to a single
hash-chosen worker and higher values only produce a warning.

---

## Key findings

| Run | probe_interval | event_probe_min_gap | avg lag | src_ms | mv3_ms | q_backlog |
|-----|---------------|---------------------|---------|--------|--------|-----------|
| Pre-fix baseline | 500ms | off | ~1900ms | ~950ms | ~190ms | — |
| Post-fix | 500ms | off | 712ms | 454ms | 190ms | 0–300k* |
| Post-fix | 200ms | off | 653ms | 184ms | 205ms | 0–153 |
| Latency goal | 200ms | off | ~645ms | ~240ms | ~200ms | 0–50 |
| Event-driven | 200ms | 50ms | 984ms | 224ms | 294ms | 0–135 |

*q_backlog metric was broken in early runs (used wrong SEMP fields).

All rows predate the 2026-07-30 probe machinery overhaul. The event-driven row
measured a mechanism that has since been deleted.

**Latency floor at best config (200ms probe, no event probe):**

```
broker_lag   +3ms    (broker stamps on receipt)
probe_wait   184ms   (≈ probe_interval / 2)
mv_cascade   205ms   (dominant — compute throughput bound on this hardware)
─────────────────────
floor        392ms   (actual avg 653ms due to poll-time jitter)
```

---

## Open investigations

### A. Interval matrix re-run under probe::Ticker rounding

The event-driven probing crossover investigation and the adaptive probe rate idea
that previously occupied this section are obsolete. Both paths were deleted in the
2026-07-30 probe machinery overhaul (see below), because probe rounding gives one
distinct downstream timestamp per interval without a probe rate that scales with
message rate. The open item now is a plain re-run of the interval matrix (100ms,
200ms, 500ms, 1s at rate 500) under the new machinery, plus the catch-up and restart
measurements listed in that section.

### B. Multi-worker / partitioned-queue parallelism (future work)

The source is now clamped to a single hash-chosen worker, so parallelism scaling as
previously framed no longer applies. Historical context: all pre-overhaul throughput
runs used `PARALLELISM 4`, but only one flow delivers on an exclusive queue and the
extra workers' stale probes stalled remap minting roughly half of all intervals.
Queue depth never exceeded 153 messages at 500 msg/sec, so compute, not ingest, was
the bottleneck anyway. True parallelism would need partitioned queues on the broker
side, a partitioned `FromTime`, and probe unioning across workers.

### C. Upstream PR to MaterializeInc/materialize

The probe/frontier fix is the primary candidate for upstreaming. The performance
benchmark workflow is Solace-specific and would stay in this fork.

Before upstreaming:
1. Confirm the fix does not regress any existing Solace testdrive tests
   (`bin/mzcompose --find testdrive run default -- solace*.td`)
2. Add a unit test in `src/storage/tests/` covering the frontier ordering invariant
3. Open a PR against `MaterializeInc/materialize:main` from a fresh branch

---

## Key files

| File | Purpose |
|------|---------|
| `src/storage/src/source/solace.rs` | Source implementation — probe/frontier fix, probing via `probe::Ticker` (event probing removed 2026-07-30) |
| `src/storage-types/src/dyncfgs.rs` | Dyncfg declarations: `SOLACE_PROBE_INTERVAL`, `SOLACE_RGMID_ORDER_VALIDATION` (`SOLACE_EVENT_PROBE_MIN_GAP` and `SOLACE_CATCHUP_PROBE_ENABLED` removed 2026-07-30) |
| `src/storage/src/sink/solace.rs` | Sink implementation (clippy fixes applied) |
| `test/solace/mzcompose.py` | Full perf workflow, SEMP polling, goal presets |
| `test/solace/perf-setup.td` | DDL: source, MVs, sink for perf runs |
| `test/solace/run-perf.sh` | Entry point — pre-warms Docker cache, then invokes mzcompose |
| `test/solace/PERF_TUNING.md` | Configuration knobs reference, all test result data |
| `test/solace/QUEUE_DEPTH_MEASUREMENT.md` | SEMP v2 queue depth measurement — why `lastSpooledMsgId − highestAckedMsgId` is correct |
| `ci/solace/build.py` | CI build script — builds materialized + testdrive and pushes to ghcr |
| `.github/workflows/ci.yml` | GHA workflow — build triggered by `workflow_dispatch` only |

---

## CI / ghcr

Images are at `ghcr.io/jessemenning/materialize-solace/{materialized,testdrive}`.

- **`:latest`** → `main` branch build (current: includes all fixes above)
- **`:fix-solace-probe-frontier`** → feature branch build (now merged to main)
- **Branch builds** → tag is sanitized branch name (e.g. `my-feature` → `my-feature`)

To trigger a CI build after pushing new commits:

```bash
gh workflow run ci.yml \
  --repo SolaceDev/materialize-solace \
  --ref <branch-name>
```

Build takes ~25–40 minutes. Monitor with:

```bash
gh run list --repo SolaceDev/materialize-solace --limit 5
gh run watch <RUN_ID> --repo SolaceDev/materialize-solace
```

---

## 2026-07-30 — Probe machinery overhaul (probe::Ticker rounding)

Commits `d5277367893`, `ad54c5855eb`, `caf74d4d802`, `af899c8d733` on `main`.

### What changed

1. Probing now goes through `probe::Ticker` (`src/storage/src/source/probe.rs`).
   Probe timestamps are rounded down to multiples of `solace_probe_interval`. The
   remap operator only mints a binding for a strictly newer probe timestamp, so
   minting is hard-capped at one binding per interval regardless of message rate.
2. The inline catch-up probe path and the event-driven probe path were deleted as
   dead code under rounding. Dyncfgs `solace_catchup_probe_enabled` and
   `solace_event_probe_min_gap` no longer exist, and `mzcompose.py` no longer has
   `--event-probe-min-gap`.
3. `solace_probe_interval` (still default 200ms) is re-read by the ticker after
   every tick, so `ALTER SYSTEM SET solace_probe_interval` applies to running
   sources. The old "read once at source startup" caveat is gone.
4. `probe_cap` is never downgraded anymore (Kafka pattern). The old synthetic
   `.next()` advancement and its divergence bug are gone.
5. The 5×100ms startup probe burst was removed. The pipeline's synthetic seed probe
   covers startup.
6. The reader is clamped to a single hash-chosen worker. `PARALLELISM > 1` warns
   and no longer runs multiple probe loops, which used to stall remap minting via
   the last-writer-wins probe slot.
7. Acks are drained with a budget (1024 per iteration) outside the select arms. The
   resume_uppers arm only records the commit boundary.
8. Per-message RGMID ordering validation is gated behind dyncfg
   `solace_rgmid_order_validation` (default false), and metadata extraction is
   skipped when no INCLUDE columns are requested.

### Updated cost model

`lag_ms ≈ broker_lag + probe_wait + mv_cascade`, where `probe_wait ≈
probe_interval/2` is now the only interval-dependent term. Remap minting (persist
compare-and-append plus read-back) happens exactly once per interval, idle or busy.
During backlog replay the mint rate no longer scales with message rate. Previously
it was roughly one mint per 1000-message batch via the catch-up path. Distinct
downstream timestamps are capped at one per interval, so `mv_cascade` should no
longer grow when the interval shrinks as severely as before. The old event-probing
experiment that regressed 653ms to 984ms was measuring exactly that timestamp
churn, and that mechanism is deleted.

### Expected effects

- Catch-up drain time should improve sharply, since the mint rate drops from
  roughly batch rate to one per interval.
- Steady state at 200ms should be roughly unchanged.
- `mv_cascade` should not regress, and smaller intervals should now be viable
  where they previously saturated compute.

### To be measured

No new numbers exist yet. All of the following are to be measured:

- Interval matrix (100ms, 200ms, 500ms, 1s) at rate 500.
- Catch-up profile at rate 5000 (backlog drain under the one-mint-per-interval
  regime).
- Restart-to-first-row latency after the startup-burst removal.
