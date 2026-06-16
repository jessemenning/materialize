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

### 3. Event-driven probing infrastructure (`SOLACE_EVENT_PROBE_MIN_GAP`)

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

### 4. Full performance benchmark workflow

Built out `workflow_perf` in `mzcompose.py` with:

- Background SMF publisher (configurable rate, duration, message size)
- Repeating lag-decomposition poll loop: reports `src_ms`, `mv3_ms`, `q_backlog`
- SEMP v2 queue depth via `lastSpooledMsgId − highestAckedMsgId` (reliable across all
  ack modes — see [QUEUE_DEPTH_MEASUREMENT.md](QUEUE_DEPTH_MEASUREMENT.md))
- Goal presets (`--goal throughput | balanced | latency`) that configure ack mode,
  deduplication, and parallelism in one flag
- `--event-probe-min-gap` arg wired to `ALTER SYSTEM SET` before source creation
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

# With event-driven probing enabled:
test/solace/run-perf.sh \
  --goal throughput \
  --probe-interval 200ms \
  --event-probe-min-gap 100ms \
  --rate 500 \
  --duration 600

# Override the ghcr tag (e.g. to test a specific feature-branch build):
test/solace/run-perf.sh --ghcr-tag fix-solace-probe-frontier --goal throughput ...
```

**Goal presets:**

| `--goal` | `ack_mode` | `deduplicate` | `parallelism` |
|----------|-----------|--------------|--------------|
| `throughput` | auto | false | 4 |
| `balanced` | auto | false | 1 |
| `latency` | client | true | 1 |

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

### A. Event-driven probing crossover point

Event-driven probing is expected to help on production hardware where the MV cascade
recomputes faster. Hypothesis: `mv_cascade` on production is ~30–50ms vs. 205ms on this
dev machine, putting the crossover above 20 probes/sec.

**How to test:**
1. Run `--event-probe-min-gap 100ms` (10 probes/sec) on dev hardware — if mv3_ms <
   250ms it's below the crossover here too; if mv3_ms ≥ 250ms the crossover is below 10.
2. Run the same configuration on a production-grade machine and compare.
3. Target: confirm that some `event_probe_min_gap` value lowers avg lag below 392ms on
   production hardware.

### B. Adaptive probe rate (future feature)

Rather than a fixed min gap, a smarter policy would back off probe rate when `mv3_ms`
exceeds a configurable threshold. This would give the latency benefit when compute has
headroom and automatically retreat when saturated. Not implemented yet.

Implementation sketch: track a rolling `mv3_ms` estimate in the source loop
(available in the probe response message) and gate inline probes on
`mv3_ms < solace_event_probe_max_cascade_ms`.

### C. Parallelism scaling

All tests used `PARALLELISM 4` for the throughput goal. Queue depth never exceeded 153
messages even at 500 msg/sec — the bottleneck is compute, not ingest. Higher parallelism
(8, 16) may help if compute is parallelizable, but requires a non-exclusive queue on the
broker side.

### D. Upstream PR to MaterializeInc/materialize

The probe/frontier fix is the primary candidate for upstreaming. Event-driven probing
and the performance benchmark workflow are Solace-specific and would stay in this fork.

Before upstreaming:
1. Confirm the fix does not regress any existing Solace testdrive tests
   (`bin/mzcompose --find testdrive run default -- solace*.td`)
2. Add a unit test in `src/storage/tests/` covering the frontier ordering invariant
3. Open a PR against `MaterializeInc/materialize:main` from a fresh branch

---

## Key files

| File | Purpose |
|------|---------|
| `src/storage/src/source/solace.rs` | Source implementation — probe/frontier fix, event-driven probing |
| `src/storage-types/src/dyncfgs.rs` | Dyncfg declarations: `SOLACE_PROBE_INTERVAL`, `SOLACE_EVENT_PROBE_MIN_GAP`, `SOLACE_CATCHUP_PROBE_ENABLED` |
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
