// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code to render the ingestion dataflow of a [`SolaceSourceConnection`].
//!
//! # Phase 3b — exactly-once protocol
//!
//! The reader runs a single async loop that multiplexes three sources of work:
//!
//! 1. **`flow.recv()`** — the broker delivers a new message. The reader
//!    extracts the broker-assigned `ReplicationGroupMessageId` (RGMID), checks
//!    it against the persisted watermark for dedup, and either drops + acks the
//!    message (if it is a post-restart redelivery of an already-committed
//!    record) or emits a `SourceMessage` timestamped by the RGMID and buffers
//!    the Solace message-id for later acknowledgement.
//!
//! 2. **`resume_uppers`** — Materialize's reclock layer reports that data up to
//!    some frontier has been durably written to persist. The reader drains its
//!    pending-ack buffer of every `(rgmid, msg_id)` whose `rgmid` is strictly
//!    below the new frontier and calls `flow.ack(msg_id)`. This is the
//!    Solace-side commit that lets the broker remove the messages from the
//!    spool — and is the load-bearing piece of the exactly-once protocol: data
//!    is durable in persist *before* the broker is told to forget it.
//!
//! 3. **Probe tick** — every second the reader emits a `Probe` carrying the
//!    current data-frontier so that reclock mints fresh bindings even when the
//!    queue is idle. Without this, an idle queue never advances the source
//!    frontier, `resume_uppers` never moves, acks never fire, and the broker
//!    keeps the spool growing.
//!
//! Phase 3c follow-ups: `INCLUDE`-metadata column population (currently the
//! metadata row is empty; the planner sets up the column shape but the
//! runtime does not yet fill it), `DURABLE TOPIC ENDPOINT` bind, format
//! decoding verification through the existing decoder pipeline, and a
//! mzcompose testdrive against a live `solace/solace-pubsub-standard`
//! container.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime};
use differential_dataflow::AsCollection;
use futures::StreamExt;
use itertools::Itertools as _;
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::solace::{
    SolaceBindEntity, SolaceMetadataKind, SolaceSourceConnection, SolaceTimestamp,
};
use mz_storage_types::sources::{SourceExportDetails, SourceTimestamp};
use mz_timely_util::builder_async::{
    AsyncOutputHandle, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use mz_timely_util::containers::stack::FueledBuilder;
use solace_rs::SolaceLogLevel;
use solace_rs::async_support::AsyncSessionBuilder;
use solace_rs::context::Context;
use solace_rs::flow::AckMode;
use solace_rs::message::Message;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::core::Partition;
use timely::dataflow::{Scope, StreamVec};
use timely::progress::Antichain;
use tokio::time::interval;
use tracing::{info, warn};

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::source::types::{FuelSize, Probe, SignaledFuture, SourceRender, StackedCollection};
use crate::source::{RawSourceCreationConfig, SourceMessage};

/// Number of rapid probes emitted at startup before entering the steady-state
/// probe loop. Spaced `STARTUP_PROBE_INTERVAL` apart, giving the Timely
/// scheduler multiple opportunities to process them before the first user
/// query arrives.
const STARTUP_PROBE_COUNT: u32 = 5;
/// Spacing between startup burst probes. Short enough to prime the reclock
/// within ~500ms; long enough for each probe to be scheduled and processed.
const STARTUP_PROBE_INTERVAL: Duration = Duration::from_millis(100);
/// Maximum number of messages drained from `flow.try_recv()` per select! arm
/// firing. Batching amortises Timely capability downgrades (which trigger
/// progress-tracking messages across all workers) from O(msg_rate) to
/// O(batch_rate), reducing coordination overhead at high ingest rates.
const MAX_BATCH_SIZE: usize = 1_000;

impl SourceRender for SolaceSourceConnection {
    type Time = SolaceTimestamp;

    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::Solace;

    fn render<'scope>(
        self,
        scope: Scope<'scope, SolaceTimestamp>,
        config: &RawSourceCreationConfig,
        resume_uppers: impl futures::Stream<Item = Antichain<SolaceTimestamp>> + 'static,
        _start_signal: impl std::future::Future<Output = ()> + 'static,
    ) -> (
        BTreeMap<
            GlobalId,
            StackedCollection<'scope, SolaceTimestamp, Result<SourceMessage, DataflowError>>,
        >,
        StreamVec<'scope, SolaceTimestamp, HealthStatusMessage>,
        StreamVec<'scope, SolaceTimestamp, Probe<SolaceTimestamp>>,
        Vec<PressOnDropButton>,
    ) {
        let (stream, health_stream, probe_stream, button) =
            render_reader(scope.clone(), self, config.clone(), resume_uppers);

        let export_count = u64::cast_from(config.source_exports.len());
        let data_streams: Vec<_> = stream.inner.partition::<CapacityContainerBuilder<_>, _, _>(
            export_count,
            |((output, data), time, diff)| {
                let output = u64::cast_from(output);
                (output, (data, time, diff))
            },
        );
        let mut data_collections = BTreeMap::new();
        for (id, data_stream) in config.source_exports.keys().zip_eq(data_streams) {
            data_collections.insert(*id, data_stream.as_collection());
        }

        (data_collections, health_stream, probe_stream, vec![button])
    }
}

fn render_reader<'scope>(
    scope: Scope<'scope, SolaceTimestamp>,
    connection: SolaceSourceConnection,
    config: RawSourceCreationConfig,
    resume_uppers: impl futures::Stream<Item = Antichain<SolaceTimestamp>> + 'static,
) -> (
    StackedCollection<'scope, SolaceTimestamp, (usize, Result<SourceMessage, DataflowError>)>,
    StreamVec<'scope, SolaceTimestamp, HealthStatusMessage>,
    StreamVec<'scope, SolaceTimestamp, Probe<SolaceTimestamp>>,
    PressOnDropButton,
) {
    let name = format!("SolaceReader({})", config.id);
    let mut builder = AsyncOperatorBuilder::new(name, scope.clone());

    let (data_output, stream) = builder.new_output::<FueledBuilder<
        CapacityContainerBuilder<
            Vec<(
                (usize, Result<SourceMessage, DataflowError>),
                SolaceTimestamp,
                Diff,
            )>,
        >,
    >>();
    let (health_output, health_stream) = builder.new_output::<CapacityContainerBuilder<Vec<_>>>();
    let (probe_output, probe_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<Probe<SolaceTimestamp>>>>();

    let busy_signal = Arc::clone(&config.busy_signal);
    let parallelism = usize::cast_from(connection.parallelism);
    let is_active_worker = config.worker_id < parallelism.min(config.worker_count);
    let export_ids: Vec<_> = config.source_exports.keys().copied().collect();

    // Per-export metadata-column specs. For the OLD-syntax MVP path there is
    // exactly one export (the legacy primary), and every export gets the same
    // `SolaceSourceExportDetails`. Multi-output sources (NEW-syntax `CREATE
    // TABLE … FROM SOURCE`, Phase 4+) will land different INCLUDE specs per
    // export here, but the per-export iteration in the recv loop already
    // handles that.
    let export_metadata_cols: Vec<Vec<(String, SolaceMetadataKind)>> = config
        .source_exports
        .values()
        .map(|export| match &export.details {
            SourceExportDetails::Solace(d) => d.metadata_columns.clone(),
            SourceExportDetails::None => Vec::new(),
            other => {
                warn!(
                    source_id = %config.id,
                    "unexpected SourceExportDetails variant on Solace export: {other:?}; \
                     emitting no metadata columns"
                );
                Vec::new()
            }
        })
        .collect();

    let button = builder.build(move |caps| {
        SignaledFuture::new(busy_signal, async move {
            let [mut data_cap, health_cap, mut probe_cap] = caps.try_into().unwrap();
            let mut health_cap = Some(health_cap);

            if !is_active_worker {
                return;
            }

            // Initial RGMID watermark: the smallest source-time NOT yet
            // committed to persist. Everything strictly less than this is
            // already durable and must be dropped + acked when the broker
            // redelivers it.
            let initial_watermark = compute_initial_watermark(&config.source_resume_uppers);
            info!(
                source_id = %config.id,
                ?initial_watermark,
                "Solace source resuming from persisted watermark"
            );

            // Resolve the PASSWORD secret to a plaintext byte string.
            let password = match config
                .config
                .connection_context
                .secrets_reader
                .read_string(connection.connection.password)
                .await
            {
                Ok(p) => p,
                Err(err) => {
                    emit_halting(
                        &health_output,
                        &mut health_cap,
                        &export_ids,
                        format!(
                            "failed to read PASSWORD secret: {}",
                            err.display_with_causes()
                        ),
                    );
                    std::future::pending::<()>().await;
                    unreachable!("pending future never returns");
                }
            };

            // Initialize the Solace client context. Each source operator owns
            // its own context for now; multi-source coalescing into a shared
            // context is a follow-up optimization.
            let context = match Context::new(SolaceLogLevel::Error) {
                Ok(ctx) => ctx,
                Err(err) => {
                    emit_halting(
                        &health_output,
                        &mut health_cap,
                        &export_ids,
                        format!(
                            "failed to initialize Solace context: {}",
                            err.display_with_causes()
                        ),
                    );
                    std::future::pending::<()>().await;
                    unreachable!("pending future never returns");
                }
            };

            let host = connection.connection.host.clone();
            let msg_vpn = connection.connection.msg_vpn.clone();
            let username = connection.connection.username.clone();

            let session = AsyncSessionBuilder::new(&context)
                .host_name(host.clone())
                .vpn_name(msg_vpn.clone())
                .username(username.clone())
                .password(password.into_bytes())
                .reconnect_retries(-1)
                .reconnect_retry_wait_ms(1_000)
                .reapply_subscriptions(true)
                .generate_rcv_timestamps(true)
                .build();
            let session = match session {
                Ok(s) => s,
                Err(err) => {
                    emit_halting(
                        &health_output,
                        &mut health_cap,
                        &export_ids,
                        format!(
                            "failed to open Solace session to {host} (vpn {msg_vpn}): {}",
                            err.display_with_causes()
                        ),
                    );
                    std::future::pending::<()>().await;
                    unreachable!("pending future never returns");
                }
            };

            let queue_name = match &connection.bind_entity {
                SolaceBindEntity::Queue { name } => name.clone(),
                SolaceBindEntity::TopicEndpoint { .. } => {
                    emit_halting(
                        &health_output,
                        &mut health_cap,
                        &export_ids,
                        "DURABLE TOPIC ENDPOINT bind is not yet supported in the runtime (Phase 6)"
                            .to_owned(),
                    );
                    std::future::pending::<()>().await;
                    unreachable!("pending future never returns");
                }
            };

            let mut flow = match session.create_flow(
                &queue_name,
                if connection.auto_ack {
                    AckMode::Auto
                } else {
                    AckMode::Client
                },
                connection.ack_window_size,
                Some(connection.flow_max_unacked),
            ) {
                Ok(f) => f,
                Err(err) => {
                    emit_halting(
                        &health_output,
                        &mut health_cap,
                        &export_ids,
                        format!(
                            "failed to bind to queue '{queue_name}': {}",
                            err.display_with_causes()
                        ),
                    );
                    std::future::pending::<()>().await;
                    unreachable!("pending future never returns");
                }
            };

            if let Err(err) = flow.start() {
                emit_halting(
                    &health_output,
                    &mut health_cap,
                    &export_ids,
                    format!(
                        "failed to start Solace flow on '{queue_name}': {}",
                        err.display_with_causes()
                    ),
                );
                std::future::pending::<()>().await;
                unreachable!("pending future never returns");
            }

            info!(
                source_id = %config.id,
                worker_id = config.worker_id,
                parallelism,
                host = %host,
                msg_vpn = %msg_vpn,
                queue = %queue_name,
                "Solace source bound; entering receive loop"
            );

            emit_running(&health_output, &mut health_cap, &export_ids);

            // (rgmid, msg_id) pairs awaiting persist-commit before they can be
            // acked to the broker. Ordered by RGMID within this flow.
            let mut pending_acks: VecDeque<(SolaceTimestamp, u64)> =
                VecDeque::with_capacity(MAX_BATCH_SIZE);
            // Tracks the previous message's raw RGMID bytes for per-flow ordering validation.
            let mut prev_rgmid: Option<[u8; 16]> = None;

            // Reusable heap buffers. Declared outside the select! loop so
            // their allocated capacity persists across iterations, eliminating
            // per-batch malloc/free churn at high message rates.
            //
            // raw_batch: inbound messages drained from the flow each cycle.
            let mut raw_batch: Vec<solace_rs::message::InboundMessage> =
                Vec::with_capacity(MAX_BATCH_SIZE);
            // emit_batch: one entry per (message × export) pair, built flat
            // (no inner Vec) to avoid per-message inner-Vec allocations.
            let n_exports = export_metadata_cols.len();
            let mut emit_batch: Vec<(SolaceTimestamp, usize, SourceMessage)> =
                Vec::with_capacity(MAX_BATCH_SIZE * n_exports.max(1));
            // ack_batch: one entry per message (not per export) for
            // pending_acks population after capability downgrade.
            let mut ack_batch: Vec<(SolaceTimestamp, u64)> = Vec::with_capacity(MAX_BATCH_SIZE);

            let mut resume_uppers = std::pin::pin!(resume_uppers);
            let probe_interval_duration =
                mz_storage_types::dyncfgs::SOLACE_PROBE_INTERVAL.get(config.config.config_set());
            let mut probe_interval = interval(probe_interval_duration);
            // Skip the immediate first tick; we want to wait probe_interval_duration
            // between emissions.
            probe_interval.tick().await;

            let catchup_probe_enabled = mz_storage_types::dyncfgs::SOLACE_CATCHUP_PROBE_ENABLED
                .get(config.config.config_set());

            let event_probe_min_gap = mz_storage_types::dyncfgs::SOLACE_EVENT_PROBE_MIN_GAP
                .get(config.config.config_set());
            let event_probe_enabled = !event_probe_min_gap.is_zero();
            // Tracks when any probe was last emitted (timer or event) so the
            // event path can rate-limit itself to event_probe_min_gap.
            let mut last_probe_emitted = tokio::time::Instant::now();

            // After a restart the caps open at SolaceTimestamp::minimum() (None).
            // Advance them to the persisted watermark so the reclocker can
            // immediately advance its output frontier past already-committed data,
            // allowing queries at "now" to return without waiting for a new message.
            if data_cap.time() < &initial_watermark {
                data_cap.downgrade(&initial_watermark);
                let _ = probe_cap.try_downgrade(&initial_watermark);
            }

            // Emit an initial probe at the resume point so reclock can mint a
            // binding even before the first message arrives.
            emit_probe(&probe_output, &probe_cap, &config, data_cap.time());

            // Startup burst: emit STARTUP_PROBE_COUNT rapid probes spaced
            // STARTUP_PROBE_INTERVAL apart. Gives the Timely scheduler multiple
            // scheduling cycles to propagate the initial probe through remap
            // before any user query arrives — avoiding a multi-second stall on
            // schema_ok / populate_view_registry on fresh starts.
            //
            // NOTE: probe_cap is intentionally NOT advanced here (no synthetic
            // RGMID increments). Advancing probe_cap with synthetic .next()
            // values can place it ahead of the first real broker RGMIDs; the
            // subsequent probe_cap.try_downgrade(&real_ts) calls then fail
            // silently, leaving probe_cap and data_cap diverged until they
            // re-sync via natural message flow. The probe PAYLOAD
            // (upstream_frontier = data_cap.time()) carries the semantic
            // content; probe_cap's position only affects Timely GC, which
            // will catch up when probe_cap is advanced to the first real RGMID.
            for _ in 0..STARTUP_PROBE_COUNT {
                tokio::time::sleep(STARTUP_PROBE_INTERVAL).await;
                emit_probe(&probe_output, &probe_cap, &config, data_cap.time());
            }

            // Accumulates the highest ts_next seen across batches. Flushed into
            // data_cap within the probe_interval tick (before the probe is emitted)
            // so the probe always carries the current committed boundary, and
            // compute re-evaluates at most once per probe_interval instead of
            // once per batch.
            let mut pending_frontier: Option<SolaceTimestamp> = None;

            loop {
                tokio::select! {
                    biased;

                    // Persist has committed up to a new frontier; ack everything
                    // below it.
                    Some(frontier) = resume_uppers.next() => {
                        drain_pending_acks(&mut pending_acks, &frontier, &flow, config.id);
                    }

                    // Heartbeat: flush any pending frontier then emit a probe so
                    // reclock advances even when the broker is idle.
                    //
                    // Flushing pending_frontier BEFORE emitting ensures the probe
                    // always carries max_ts.next() (the real committed boundary),
                    // not the stale max_ts. With two separate ticks the probe arm
                    // (higher biased-select priority) would have fired first,
                    // emitting the old data_cap and leaving the correct frontier
                    // pending for one more second — doubling query latency.
                    _ = probe_interval.tick() => {
                        if let Some(ts) = pending_frontier.take() {
                            if data_cap.time() < &ts {
                                data_cap.downgrade(&ts);
                                let _ = probe_cap.try_downgrade(&ts);
                            }
                        }
                        emit_probe(&probe_output, &probe_cap, &config, data_cap.time());
                        // Advance probe_cap by one step so Timely can immediately
                        // GC this probe record rather than accumulating records at
                        // the same SolaceTimestamp indefinitely. The probe PAYLOAD
                        // carries data_cap.time() (the real source frontier), so
                        // reclock still mints accurate bindings regardless of
                        // probe_cap's position.
                        if let Some(next) = probe_cap.time().next() {
                            let _ = probe_cap.try_downgrade(&next);
                        }
                        last_probe_emitted = tokio::time::Instant::now();
                    }

                    // A new message has arrived. Drain all currently-queued
                    // messages into a batch so capability downgrades happen
                    // once per batch rather than twice per message, reducing
                    // Timely progress-tracking overhead from O(msg_rate) to
                    // O(batch_rate).
                    msg = flow.recv() => {
                        let Some(first) = msg else {
                            warn!(
                                source_id = %config.id,
                                "Solace flow recv() returned None; ending source"
                            );
                            return;
                        };

                        // Drain all currently-available messages into the
                        // pre-allocated buffer without blocking.
                        raw_batch.clear();
                        raw_batch.push(first);
                        while let Ok(m) = flow.try_recv() {
                            raw_batch.push(m);
                            if raw_batch.len() >= MAX_BATCH_SIZE {
                                break;
                            }
                        }
                        // True if the broker queue had ≥ MAX_BATCH_SIZE messages
                        // ready — used below as a catch-up indicator.
                        let batch_was_full = raw_batch.len() >= MAX_BATCH_SIZE;

                        emit_batch.clear();
                        ack_batch.clear();
                        let mut max_ts: Option<SolaceTimestamp> = None;

                        for msg in raw_batch.drain(..) {
                            let rgmid_bytes =
                                match msg.get_replication_group_message_id_raw() {
                                    Ok(Some(b)) => b,
                                    Ok(None) => {
                                        warn!(
                                            source_id = %config.id,
                                            "Solace message has no RGMID; skipping \
                                             (unexpected on a guaranteed-messaging queue)"
                                        );
                                        continue;
                                    }
                                    Err(err) => {
                                        warn!(
                                            source_id = %config.id,
                                            error = %err.display_with_causes(),
                                            "failed to read RGMID; skipping message"
                                        );
                                        continue;
                                    }
                                };
                            let ts = SolaceTimestamp(Some(rgmid_bytes));

                            // Validate that byte-wise Ord matches the C SDK comparator
                            // for consecutive RGMIDs. The design at
                            // src/storage-types/src/sources/solace.rs:350-356 asserts
                            // these agree within a single broker/HA pair; this check
                            // produces empirical evidence for or against that assertion.
                            if let Some(prev) = prev_rgmid {
                                use solace_rs::message::compare_replication_group_message_ids;
                                match compare_replication_group_message_ids(
                                    &prev,
                                    &rgmid_bytes,
                                ) {
                                    Ok(sdk_ordering) => {
                                        let byte_ordering = prev.cmp(&rgmid_bytes);
                                        if byte_ordering != sdk_ordering {
                                            warn!(
                                                source_id = %config.id,
                                                prev = ?prev,
                                                curr = ?rgmid_bytes,
                                                ?byte_ordering,
                                                ?sdk_ordering,
                                                "RGMID byte-wise Ord disagrees with C \
                                                 SDK comparator — ordering invariant \
                                                 violated; exactly-once semantics may \
                                                 be unsound"
                                            );
                                        }
                                    }
                                    Err(err) => {
                                        warn!(
                                            source_id = %config.id,
                                            error = %err.display_with_causes(),
                                            "RGMID comparison returned error \
                                             (cross-broker messages? IDs not comparable)"
                                        );
                                    }
                                }
                            }
                            prev_rgmid = Some(rgmid_bytes);

                            let msg_id = match msg.get_msg_id() {
                                Ok(Some(id)) => id,
                                Ok(None) | Err(_) => {
                                    warn!(
                                        source_id = %config.id,
                                        "Solace message has no msg_id; skipping \
                                         (cannot ack such messages to the broker)"
                                    );
                                    continue;
                                }
                            };

                            // Dedup-on-restart: broker redelivery of an already-
                            // committed RGMID. Ack and skip.
                            if ts < initial_watermark {
                                if let Err(err) = flow.ack(msg_id) {
                                    warn!(
                                        source_id = %config.id,
                                        error = %err.display_with_causes(),
                                        "failed to ack dedup-skipped Solace message"
                                    );
                                }
                                continue;
                            }

                            // Try the string variant first so the Solace C SDK
                            // strips the SDT container header that the Python
                            // Messaging API adds to string payloads. Fall back to
                            // the raw binary getter for non-string attachments.
                            let payload = msg
                                .get_payload_as_string()
                                .ok()
                                .flatten()
                                .map(|s| s.into_bytes())
                                .or_else(|| {
                                    msg.get_payload().ok().flatten().map(|p| p.to_vec())
                                })
                                .unwrap_or_default();

                            let mut key_row = {
                                let mut row = Row::default();
                                row.packer().push(Datum::Null);
                                row
                            };
                            let mut value_row = {
                                let mut row = Row::default();
                                row.packer().push(Datum::Bytes(&payload));
                                row
                            };

                            // Extract all metadata fields once per message so
                            // each per-export build_metadata_row call below never
                            // re-invokes the Solace C SDK.
                            let extracted = extract_msg_fields(&msg, rgmid_bytes);

                            // Push one flat emit entry per export. On the last
                            // (or only) export, move key/value instead of
                            // cloning — eliminates all Row copies for the common
                            // single-export MVP case.
                            for (export_idx, cols) in
                                export_metadata_cols.iter().enumerate()
                            {
                                let metadata = build_metadata_row(&extracted, cols);
                                let is_last = export_idx + 1 == n_exports;
                                let key = if is_last {
                                    std::mem::take(&mut key_row)
                                } else {
                                    key_row.clone()
                                };
                                let value = if is_last {
                                    std::mem::take(&mut value_row)
                                } else {
                                    value_row.clone()
                                };
                                emit_batch.push((
                                    ts,
                                    export_idx,
                                    SourceMessage { key, value, metadata },
                                ));
                            }

                            max_ts = Some(match max_ts.take() {
                                None => ts.clone(),
                                Some(prev_max) => std::cmp::max(prev_max, ts.clone()),
                            });
                            // One ack entry per message regardless of export count.
                            ack_batch.push((ts, msg_id));
                        }

                        // Nothing to emit — all messages were invalid or deduped.
                        let Some(max_ts) = max_ts else {
                            continue;
                        };

                        // ONE capability downgrade covers every timestamp in the batch.
                        if data_cap.time() < &max_ts {
                            data_cap.downgrade(&max_ts);
                            let _ = probe_cap.try_downgrade(&max_ts);
                        }

                        for (ts, export_idx, source_message) in emit_batch.drain(..) {
                            let update = (
                                (export_idx, Ok::<_, DataflowError>(source_message)),
                                ts,
                                Diff::ONE,
                            );
                            let size = update.fuel_size();
                            data_output.give_fueled(&data_cap, update, size).await;
                        }

                        // Pending acks are populated after capability downgrade so
                        // the frontier-ordering invariant is preserved. ack_batch is
                        // RGMID-ascending because raw_batch preserves broker order.
                        pending_acks.extend(ack_batch.drain(..));

                        // Accumulate the post-batch advance; flushed into
                        // data_cap within the probe_interval tick so compute
                        // re-evaluates at most once per probe_interval instead of
                        // once per batch — except during catch-up (see below).
                        if let Some(ts_next) = max_ts.next() {
                            pending_frontier = Some(match pending_frontier.take() {
                                Some(prev) => prev.max(ts_next),
                                None => ts_next,
                            });
                        }

                        // Emit an inline probe when warranted, without waiting
                        // for the next probe_interval tick:
                        //
                        // Event-driven mode (SOLACE_EVENT_PROBE_MIN_GAP > 0):
                        //   Probe after any batch once min_gap has elapsed since
                        //   the last probe (timer or event). Reduces steady-state
                        //   query latency from ~probe_interval/2 to ~min_gap/2 at
                        //   the cost of more frequent MV re-evaluations.
                        //   Supersedes the catch-up path when enabled.
                        //
                        // Catch-up mode (SOLACE_CATCHUP_PROBE_ENABLED, default on):
                        //   Probe after full batches (broker had ≥ MAX_BATCH_SIZE
                        //   messages queued). Drives reclock at batch rate during
                        //   backlog replay without permanently increasing probe
                        //   frequency in steady state.
                        let should_probe_inline = if event_probe_enabled {
                            last_probe_emitted.elapsed() >= event_probe_min_gap
                        } else {
                            catchup_probe_enabled && batch_was_full
                        };
                        if should_probe_inline {
                            if let Some(ts) = pending_frontier.take() {
                                if data_cap.time() < &ts {
                                    data_cap.downgrade(&ts);
                                    let _ = probe_cap.try_downgrade(&ts);
                                }
                            }
                            emit_probe(&probe_output, &probe_cap, &config, data_cap.time());
                            if let Some(next) = probe_cap.time().next() {
                                let _ = probe_cap.try_downgrade(&next);
                            }
                            last_probe_emitted = tokio::time::Instant::now();
                        }
                    }
                }
            }
        })
    });

    (
        stream.as_collection(),
        health_stream,
        probe_stream,
        button.press_on_drop(),
    )
}

/// All fields extracted from a single inbound Solace message. Populated once
/// per message via [`extract_msg_fields`] and passed to [`build_metadata_row`]
/// for every export, eliminating repeated SDK calls when multiple exports share
/// the same underlying message.
struct ExtractedMsgFields {
    /// Already-verified RGMID bytes, passed in from the main batch loop.
    rgmid_bytes: [u8; 16],
    topic_owned: Option<String>,
    /// Topic split on `/`, computed once for all `TopicLevels` metadata columns.
    topic_levels: Vec<String>,
    rcv_timestamp: Option<std::time::SystemTime>,
    sender_timestamp: Option<std::time::SystemTime>,
    app_msg_id: Option<String>,
    correlation_id: Option<String>,
    /// Key-sorted user property pairs, ready for deterministic dict packing.
    user_properties: Vec<(String, String)>,
}

/// Extract all metadata fields from `msg` into an owned struct so that
/// [`build_metadata_row`] can be called once per export without re-invoking the
/// Solace C SDK for each one.
fn extract_msg_fields(
    msg: &solace_rs::message::InboundMessage,
    rgmid_bytes: [u8; 16],
) -> ExtractedMsgFields {
    let topic_owned: Option<String> = msg
        .get_destination()
        .ok()
        .flatten()
        .and_then(|d| d.dest.into_string().ok());

    let topic_levels: Vec<String> = topic_owned
        .as_deref()
        .map(|t| t.split('/').map(str::to_owned).collect())
        .unwrap_or_default();

    let rcv_timestamp = msg.get_rcv_timestamp().ok().flatten();
    let sender_timestamp = msg.get_sender_timestamp().ok().flatten();
    let app_msg_id = msg.get_application_message_id().map(|s| s.to_owned());
    let correlation_id = msg
        .get_correlation_id()
        .ok()
        .flatten()
        .map(|s| s.to_owned());

    // Collect and sort once; all exports that request UserProperties share the
    // result without re-sorting per export.
    let mut user_properties: Vec<(String, String)> = msg
        .get_user_properties()
        .unwrap_or_default()
        .into_iter()
        .collect();
    user_properties.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    ExtractedMsgFields {
        rgmid_bytes,
        topic_owned,
        topic_levels,
        rcv_timestamp,
        sender_timestamp,
        app_msg_id,
        correlation_id,
        user_properties,
    }
}

/// Build a metadata `Row` for one Solace message export, packing one (or more)
/// Datums per entry in `kinds` in declaration order. `TopicLevels { count }`
/// expands to `count` `text NULLABLE` columns. All fields come from
/// `ExtractedMsgFields`, which is computed once per message before iterating
/// over exports.
fn build_metadata_row(fields: &ExtractedMsgFields, kinds: &[(String, SolaceMetadataKind)]) -> Row {
    let mut row = Row::default();
    let mut packer = row.packer();
    for (_alias, kind) in kinds {
        match kind {
            SolaceMetadataKind::ReplicationGroupMessageId => {
                packer.push(Datum::Bytes(&fields.rgmid_bytes));
            }
            SolaceMetadataKind::BrokerTimestamp => {
                packer.push(system_time_to_datum(fields.rcv_timestamp));
            }
            SolaceMetadataKind::SenderTimestamp => {
                packer.push(system_time_to_datum(fields.sender_timestamp));
            }
            SolaceMetadataKind::ApplicationMessageId => {
                packer.push(match fields.app_msg_id {
                    Some(ref s) => Datum::String(s),
                    None => Datum::Null,
                });
            }
            SolaceMetadataKind::CorrelationId => {
                packer.push(match fields.correlation_id {
                    Some(ref s) => Datum::String(s),
                    None => Datum::Null,
                });
            }
            SolaceMetadataKind::Topic => {
                packer.push(match fields.topic_owned {
                    Some(ref s) => Datum::String(s),
                    None => Datum::Null,
                });
            }
            SolaceMetadataKind::TopicLevels { count } => {
                let count = usize::cast_from(*count);
                let levels = &fields.topic_levels;
                for i in 0..count {
                    if levels.is_empty() {
                        packer.push(Datum::Null);
                    } else if i + 1 < count {
                        // Pre-last column: emit the level if it exists, else NULL.
                        match levels.get(i) {
                            Some(s) => packer.push(Datum::String(s)),
                            None => packer.push(Datum::Null),
                        }
                    } else {
                        // Last column: glom remaining levels so the topic suffix
                        // is never lost when the message has more levels than COUNT.
                        if levels.len() > count {
                            let suffix = levels[i..].join("/");
                            packer.push(Datum::String(&suffix));
                        } else {
                            match levels.get(i) {
                                Some(s) => packer.push(Datum::String(s)),
                                None => packer.push(Datum::Null),
                            }
                        }
                    }
                }
            }
            SolaceMetadataKind::Partition => {
                // Non-partitioned MVP: always NULL. Phase 8 will populate this
                // from the broker-assigned partition id.
                packer.push(Datum::Null);
            }
            SolaceMetadataKind::UserProperties => {
                // user_properties is already sorted by key at extraction time.
                packer.push_dict_with(|dict_packer| {
                    for (k, v) in &fields.user_properties {
                        dict_packer.push(Datum::String(k));
                        dict_packer.push(Datum::String(v));
                    }
                });
            }
        }
    }
    row
}

/// Convert a Solace `SystemTime` accessor result to a `Datum`. Returns
/// `Datum::Null` when the timestamp is absent or falls outside the range
/// representable by Materialize's `CheckedTimestamp`.
fn system_time_to_datum<'a>(ts: Option<std::time::SystemTime>) -> Datum<'a> {
    let Some(ts) = ts else {
        return Datum::Null;
    };
    let Ok(dur) = ts.duration_since(std::time::UNIX_EPOCH) else {
        return Datum::Null;
    };
    // SystemTime fits in i64 ms for any plausible broker clock.
    let millis = match i64::try_from(dur.as_millis()) {
        Ok(m) => m,
        Err(_) => return Datum::Null,
    };
    let Some(dt) = DateTime::from_timestamp_millis(millis) else {
        return Datum::Null;
    };
    let naive: NaiveDateTime = dt.naive_utc();
    match CheckedTimestamp::try_from(naive) {
        Ok(ct) => Datum::from(ct),
        Err(_) => Datum::Null,
    }
}

/// Compute the source's initial watermark from the persisted resume frontiers
/// across all exports. Returns `SolaceTimestamp(None)` when the source is
/// starting fresh (no rows ever written to persist).
fn compute_initial_watermark(
    source_resume_uppers: &BTreeMap<GlobalId, Vec<Row>>,
) -> SolaceTimestamp {
    let mut watermark = SolaceTimestamp(None);
    for rows in source_resume_uppers.values() {
        for row in rows {
            let t = SolaceTimestamp::decode_row(row);
            if t > watermark {
                watermark = t;
            }
        }
    }
    watermark
}

/// Pop every pending `(rgmid, msg_id)` whose RGMID is strictly below the new
/// frontier and call `flow.ack(msg_id)`. Per [`pending_acks`] invariants the
/// queue is ordered by RGMID, so the drain stops as soon as the head element
/// is at or beyond the frontier.
fn drain_pending_acks(
    pending_acks: &mut VecDeque<(SolaceTimestamp, u64)>,
    frontier: &Antichain<SolaceTimestamp>,
    flow: &solace_rs::async_support::OwnedAsyncFlow,
    source_id: GlobalId,
) {
    let Some(boundary) = frontier.as_option().cloned() else {
        // Empty antichain means the source is shutting down — nothing to ack.
        return;
    };
    while let Some(&(rgmid, _)) = pending_acks.front() {
        if rgmid < boundary {
            let (_, msg_id) = pending_acks.pop_front().expect("front just observed");
            if let Err(err) = flow.ack(msg_id) {
                warn!(
                    source_id = %source_id,
                    error = %err.display_with_causes(),
                    "failed to ack Solace message after persist commit"
                );
            }
        } else {
            break;
        }
    }
}

/// Emit a heartbeat `Probe` so reclock can mint a binding even when the
/// broker is quiet. The probe carries the current source-frontier so the
/// reclock layer knows the source is alive and has advanced (or is stuck) at
/// the reported time.
fn emit_probe(
    probe_output: &AsyncOutputHandle<
        SolaceTimestamp,
        CapacityContainerBuilder<Vec<Probe<SolaceTimestamp>>>,
    >,
    probe_cap: &Capability<SolaceTimestamp>,
    config: &RawSourceCreationConfig,
    current_time: &SolaceTimestamp,
) {
    let probe_ts: mz_repr::Timestamp = (config.now_fn)().into();
    let upstream_frontier = Antichain::from_elem(current_time.clone());
    probe_output.give(
        probe_cap,
        Probe {
            probe_ts,
            upstream_frontier,
        },
    );
}

/// Emit a `Running` health status across all exports plus the global slot.
fn emit_running(
    health_output: &AsyncOutputHandle<
        SolaceTimestamp,
        CapacityContainerBuilder<Vec<HealthStatusMessage>>,
    >,
    health_cap: &mut Option<Capability<SolaceTimestamp>>,
    export_ids: &[GlobalId],
) {
    let Some(cap) = health_cap.take() else {
        return;
    };
    for id in export_ids
        .iter()
        .map(|id| Some(*id))
        .chain(std::iter::once(None))
    {
        health_output.give(
            &cap,
            HealthStatusMessage {
                id,
                namespace: StatusNamespace::Solace,
                update: HealthStatusUpdate::running(),
            },
        );
    }
}

/// Emit a `Halting` health status across all exports plus the global slot
/// and consume the capability — the source has hit a fatal initialization
/// error and will not produce more updates.
fn emit_halting(
    health_output: &AsyncOutputHandle<
        SolaceTimestamp,
        CapacityContainerBuilder<Vec<HealthStatusMessage>>,
    >,
    health_cap: &mut Option<Capability<SolaceTimestamp>>,
    export_ids: &[GlobalId],
    error: String,
) {
    let Some(cap) = health_cap.take() else {
        return;
    };
    let update = HealthStatusUpdate::halting(error, None);
    for id in export_ids
        .iter()
        .map(|id| Some(*id))
        .chain(std::iter::once(None))
    {
        health_output.give(
            &cap,
            HealthStatusMessage {
                id,
                namespace: StatusNamespace::Solace,
                update: update.clone(),
            },
        );
    }
}
