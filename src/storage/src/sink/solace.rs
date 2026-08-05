// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Solace Platform sink implementation.
//!
//! Publishes rows as JSON messages to a Solace topic derived from a per-row
//! template. Only insertions (`diff_pair.after.is_some()`) are published;
//! retractions are counted and dropped. An optional DEDUP WINDOW throttles
//! publishes per rendered topic: at most one message per topic per window.
//! Rows suppressed by the window are conflated (the latest payload per topic
//! wins) and flushed once the window elapses, so subscribers always converge
//! to the newest value even if the input then goes quiet.
//!
//! # Delivery semantics
//!
//! `DELIVERY MODE` selects the guarantee. In `direct` (the default) delivery
//! is fire-and-forget Direct messaging: the write frontier follows the input
//! frontier once batches have been handed to the Solace session, whether or
//! not the broker received them, so the sink is at-most-once. In `persistent`
//! mode each message is published with a broker acknowledgement, and the
//! frontier (and durable shard upper) advance only past data the broker has
//! acked. A restart resumes from that confirmed upper and re-publishes
//! anything unacked, so the sink is at-least-once. An ack failure halts the
//! sink so the dataflow restarts and replays.
//!
//! The following caveats apply to direct mode (persistent mode confirms
//! delivery, so they do not):
//!
//! * **Restart duplicate window.** On restart the sink resumes from its
//!   durable shard upper, but the dataflow as_of trails it (it is the sink's
//!   since), and there is no resume-time filtering, so updates between the
//!   as_of and the pre-crash upper are published again. Every clusterd
//!   restart can therefore re-publish a small window of recent updates.
//!
//! * **Per-worker ordering.** Rows are distributed across timely workers by
//!   hash of the whole row, so two updates rendering to the same topic can
//!   land on different workers, each with its own session, and reach the
//!   broker in either order. Within one worker, publishes are ordered by
//!   input timestamp. Dedup state is likewise per worker: a topic whose rows
//!   are spread across N workers can publish up to N messages per window.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use itertools::Itertools as _;

use differential_dataflow::{Hashable, VecCollection};
use futures::StreamExt;
use mz_interchange::avro::DiffPair;
use mz_interchange::envelopes::for_each_diff_pair;
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, Diff, GlobalId, RelationDesc, Row, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::{SolaceDeliveryMode, SolaceSinkConnection, StorageSinkDesc};
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{
    AsyncOutputHandle, Event, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use serde_json::Value;
use solace_rs::SessionError;
use solace_rs::SolaceLogLevel;
use solace_rs::async_support::{AsyncSession, AsyncSessionBuilder};
use solace_rs::context::Context;
use solace_rs::message::{
    DeliveryMode, DestinationType, MessageDestination, OutboundMessage, OutboundMessageBuilder,
};
use solace_rs::session::SessionEvent;
use timely::PartialOrder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::StreamVec;
use timely::dataflow::operators::Capability;
use timely::progress::{Antichain, Timestamp as _};
use tokio::sync::oneshot;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::render::sinks::{SinkBatchStream, SinkRender};
use crate::statistics::SinkStatistics;
use crate::storage_state::StorageState;

impl<'scope> SinkRender<'scope> for SolaceSinkConnection {
    fn get_key_indices(&self) -> Option<&[usize]> {
        None
    }

    fn get_relation_key_indices(&self) -> Option<&[usize]> {
        None
    }

    fn render_sink(
        &self,
        storage_state: &mut StorageState,
        sink: &StorageSinkDesc<CollectionMetadata, Timestamp>,
        sink_id: GlobalId,
        batches: SinkBatchStream<'scope>,
        _key_is_synthetic: bool,
        _err_collection: VecCollection<'scope, Timestamp, DataflowError, Diff>,
    ) -> (
        StreamVec<'scope, Timestamp, HealthStatusMessage>,
        Vec<PressOnDropButton>,
    ) {
        let connection = self.clone();
        let secrets_reader = Arc::clone(
            &storage_state
                .storage_configuration
                .connection_context
                .secrets_reader,
        );

        // Durable progress lives in the sink's (data-less) persist shard: one
        // worker advances the shard upper with empty appends as input progress
        // arrives, mirroring the Kafka sink. `StorageCollections` watches the
        // shard upper, which is what `mz_frontiers` reports and what restart
        // as_of derivation reads, so without these appends the sink appears
        // frozen at 0 and re-snapshots from scratch on every restart. The
        // shared in-memory frontier below is NOT sufficient for this: it only
        // feeds the controller's read-hold downgrades and dies with the
        // process.
        let progress_leader =
            usize::cast_from(sink_id.hashed()) % batches.scope().peers() == batches.scope().index();
        let progress_handle = progress_leader.then(|| {
            let persist = Arc::clone(&storage_state.persist_clients);
            let shard_meta = sink.to_storage_metadata.clone();
            async move {
                let client = persist.open(shard_meta.persist_location).await?;
                let handle = client
                    .open_writer::<SourceData, (), Timestamp, StorageDiff>(
                        shard_meta.data_shard,
                        Arc::new(shard_meta.relation_desc),
                        Arc::new(UnitSchema),
                        Diagnostics::from_purpose("sink handle"),
                    )
                    .await?;
                Ok::<_, anyhow::Error>(handle)
            }
        });

        // Replace the placeholder write frontier registered by the storage
        // state with one this operator drives. Without this the frontier
        // stays at `minimum()` forever, which pins the input's read hold at
        // the creation as_of, blocks upstream compaction, and forces a full
        // re-snapshot on every restart.
        let write_frontier = Rc::new(RefCell::new(Antichain::from_elem(Timestamp::minimum())));
        storage_state
            .sink_write_frontiers
            .insert(sink_id, Rc::clone(&write_frontier));

        let statistics = storage_state
            .aggregated_statistics
            .get_sink(&sink_id)
            .expect("statistics initialized")
            .clone();

        let mut builder =
            AsyncOperatorBuilder::new(format!("{sink_id}-solace-sink"), batches.scope());
        let (health_output, health_stream) =
            builder.new_output::<CapacityContainerBuilder<Vec<_>>>();
        let mut input = builder.new_input_for(
            batches,
            timely::dataflow::channels::pact::Pipeline,
            &health_output,
        );

        let button = builder.build(move |caps| async move {
            // Compile the topic template once here so the hot publish loop can
            // render topics without any per-message string cloning or replace().
            let compiled_template =
                compile_template(&connection.topic, &connection.topic_column_indices);

            let health_cap = caps.into_iter().next();
            let password = match secrets_reader
                .read_string(connection.connection.password)
                .await
            {
                Ok(p) => p,
                Err(err) => {
                    // NOTE: startup failures must emit a halting status, not
                    // return. Returning would drop our capabilities and leave
                    // the write frontier behind, either pinning the sink's
                    // reported upper at 0 (read hold held forever) or, if
                    // cleared, reporting the empty antichain, which the
                    // controller reads as "sink complete". Halting makes the
                    // health operator suspend and restart the dataflow, which
                    // is the retry.
                    emit_sink_health(
                        &health_output,
                        &health_cap,
                        sink_id,
                        HealthStatusUpdate::halting(
                            format!(
                                "failed to read PASSWORD secret: {}",
                                err.display_with_causes()
                            ),
                            None,
                        ),
                    );
                    std::future::pending::<()>().await;
                    unreachable!("pending future never returns");
                }
            };

            let context = match Context::new(SolaceLogLevel::Error) {
                Ok(ctx) => ctx,
                Err(err) => {
                    emit_sink_health(
                        &health_output,
                        &health_cap,
                        sink_id,
                        HealthStatusUpdate::halting(
                            format!(
                                "failed to initialize Solace context: {}",
                                err.display_with_causes()
                            ),
                            None,
                        ),
                    );
                    std::future::pending::<()>().await;
                    unreachable!("pending future never returns");
                }
            };

            let host = connection.connection.host.clone();
            let msg_vpn = connection.connection.msg_vpn.clone();
            let username = connection.connection.username.clone();

            let mut session_builder = AsyncSessionBuilder::new(&context)
                .host_name(host.clone())
                .vpn_name(msg_vpn.clone())
                .username(username.clone())
                .password(password.into_bytes())
                .reconnect_retries(-1)
                .reconnect_retry_wait_ms(1_000);

            // Enable the TLS trust store for secure schemes (tcps:// SMF, wss://
            // WebSocket). Public-CA brokers (e.g. Solace Cloud) validate against the
            // OS CA bundle at /etc/ssl/certs. Override with SOLACE_SSL_TRUST_STORE_DIR
            // for a private CA.
            if host.starts_with("tcps://") || host.starts_with("wss://") {
                let dir = std::env::var("SOLACE_SSL_TRUST_STORE_DIR")
                    .unwrap_or_else(|_| "/etc/ssl/certs".to_string());
                session_builder = session_builder.ssl_trust_store_dir(dir);
            }

            // Session connect is a blocking FFI exchange (an unreachable
            // broker holds the C SDK's blocking connect for tens of seconds),
            // so run it on a blocking thread rather than stalling this timely
            // worker and every dataflow sharing it.
            let session = mz_ore::task::spawn_blocking(
                || format!("solace_sink_connect({sink_id})"),
                move || session_builder.build(),
            )
            .await;
            let mut session = match session {
                Ok(s) => s,
                Err(err) => {
                    emit_sink_health(
                        &health_output,
                        &health_cap,
                        sink_id,
                        HealthStatusUpdate::halting(
                            format!(
                                "failed to open Solace session to {host} (vpn {msg_vpn}): {}",
                                err.display_with_causes()
                            ),
                            None,
                        ),
                    );
                    std::future::pending::<()>().await;
                    unreachable!("pending future never returns");
                }
            };

            tracing::info!(sink_id = %sink_id, "Solace sink connected to {host} (vpn {msg_vpn})");

            // Session events (connectivity) drive the stalled/running health
            // status. Taking the receiver also prevents the otherwise-
            // undrained unbounded channel from accumulating events.
            let mut session_events = session.take_event_receiver();
            // Shared so publish calls can move to a blocking thread while the
            // operator keeps its own handle.
            let session = Arc::new(session);

            // The progress leader opens its persist writer only after the
            // broker session is up, so a sink that cannot connect never
            // advances durable progress past data it has not published.
            let mut progress_handle = match progress_handle {
                Some(open) => match open.await {
                    Ok(handle) => Some(handle),
                    Err(err) => {
                        emit_sink_health(
                            &health_output,
                            &health_cap,
                            sink_id,
                            HealthStatusUpdate::halting(
                                format!(
                                    "failed to open persist progress handle: {}",
                                    err.display_with_causes()
                                ),
                                None,
                            ),
                        );
                        std::future::pending::<()>().await;
                        unreachable!("pending future never returns");
                    }
                },
                None => None,
            };

            // Signal running so Materialize transitions out of `starting`.
            emit_sink_health(
                &health_output,
                &health_cap,
                sink_id,
                HealthStatusUpdate::running(),
            );

            // Dedup state: rendered topic -> time of the last actual publish.
            let mut dedup_cache: BTreeMap<String, Instant> = BTreeMap::new();
            // Latest (input time, payload) per topic suppressed by the dedup
            // window, awaiting flush. Newer suppressed payloads replace older
            // ones (conflation), so at most one payload per topic is pending.
            let mut pending: BTreeMap<String, (Timestamp, Vec<u8>)> = BTreeMap::new();

            let mut drop_stats = DropStats::new();

            let mut publisher = Publisher {
                session,
                sink_id,
                statistics,
                mode: connection.delivery_mode,
                msgs: Vec::new(),
                staged_bytes: 0,
                pending_acks: Vec::new(),
                consecutive_failures: 0,
                fatal: None,
            };

            // Reusable buffers declared outside the event loop so their heap
            // capacity persists across batches, avoiding per-batch allocations.
            let mut topic_buf = String::new();
            let mut to_publish: Vec<(Timestamp, String, Vec<u8>)> = Vec::new();

            // Health state: `None` while the session is up, the stall reason
            // while it is down. `reported_healthy` tracks the last emitted
            // status so transitions are emitted exactly once.
            let mut session_stall: Option<String> = None;
            let mut reported_healthy = true;
            let mut session_events_open = true;

            let mut input_closed = false;
            while !input_closed {
                // Await the next input event, a session connectivity event, or
                // the earliest pending flush deadline when the dedup window
                // has payloads waiting. `Some(ev)` is an input event, `None`
                // means the flush timer fired.
                let flush_deadline = if pending.is_empty() {
                    None
                } else {
                    let window = connection
                        .dedup_window
                        .expect("pending is only populated when a dedup window is set");
                    let now = Instant::now();
                    let deadline = pending
                        .keys()
                        .map(|topic| {
                            // A topic evicted from the dedup cache is due
                            // immediately.
                            dedup_cache.get(topic).map_or(now, |last| *last + window)
                        })
                        .min()
                        .expect("pending is non-empty");
                    Some(deadline.saturating_duration_since(now))
                };
                let wake = tokio::select! {
                    ev = input.next() => Some(ev),
                    ev = session_events.recv(), if session_events_open => {
                        match ev {
                            Some(event) => {
                                match event {
                                    SessionEvent::UpNotice
                                    | SessionEvent::ReconnectedNotice => session_stall = None,
                                    SessionEvent::DownError
                                    | SessionEvent::ConnectFailedError
                                    | SessionEvent::ReconnectingNotice => {
                                        session_stall =
                                            Some(format!("Solace session event: {event}"));
                                    }
                                    _ => {}
                                }
                                report_health(
                                    &health_output,
                                    &health_cap,
                                    sink_id,
                                    &mut reported_healthy,
                                    &session_stall,
                                    publisher.consecutive_failures,
                                );
                            }
                            None => session_events_open = false,
                        }
                        continue;
                    }
                    _ = async {
                        match flush_deadline {
                            Some(d) => tokio::time::sleep(d).await,
                            None => std::future::pending::<()>().await,
                        }
                    } => None,
                };

                match wake {
                    // Flush timer fired; fall through to the pending flush.
                    None => {}
                    Some(None) => input_closed = true,
                    Some(Some(Event::Progress(frontier))) => {
                        // In persistent mode, wait for the broker to ack every
                        // message below the new frontier before advancing. On
                        // an ack failure this sets `publisher.fatal` and leaves
                        // the frontier where it was, so the halt below restarts
                        // the dataflow and replays from the last confirmed
                        // upper (at-least-once). Direct mode is a no-op here:
                        // input progress is an honest at-most-once frontier
                        // since delivery is fire-and-forget.
                        publisher.confirm_through(&frontier).await;

                        // Advance only when delivery is confirmed. On a fatal
                        // persistent-mode failure the frontier stays put and
                        // the handler below halts, so the restart replays from
                        // the last confirmed upper (at-least-once).
                        if publisher.fatal.is_none() {
                            // Reporting the frontier lets the controller
                            // downgrade the sink's read hold on its input,
                            // unblocking upstream compaction. In direct mode,
                            // payloads still buffered in `pending` are
                            // deliberately not accounted for: losing them on
                            // restart is within the at-most-once contract.
                            write_frontier.borrow_mut().clone_from(&frontier);

                            // The progress leader also records progress durably
                            // by advancing the sink's persist shard upper with
                            // empty appends, mirroring the Kafka sink. This is
                            // what `mz_frontiers` reports (`StorageCollections`
                            // watches the shard upper, not the in-memory
                            // frontier above) and what restart as_of derivation
                            // reads, so it is what prevents a full re-snapshot
                            // on every restart. Input timestamps tick at the 1s
                            // timestamp_interval, so this costs about one
                            // consensus write per second.
                            //
                            // NOTE: the leader's input frontier is not a
                            // fleet-wide lower bound: a slower peer worker can
                            // still hold unpublished updates below it, which a
                            // crash at the wrong moment then skips on restart.
                            // In direct mode that widens the at-most-once loss
                            // window beyond in-flight messages.
                            if let Some(handle) = progress_handle.as_mut() {
                                let mut expect_upper = handle.shared_upper();
                                while PartialOrder::less_than(&expect_upper, &frontier) {
                                    const EMPTY: &[((SourceData, ()), Timestamp, StorageDiff)] =
                                        &[];
                                    match handle
                                        .compare_and_append(EMPTY, expect_upper, frontier.clone())
                                        .await
                                        .expect("valid usage")
                                    {
                                        Ok(()) => break,
                                        Err(mismatch) => expect_upper = mismatch.current,
                                    }
                                }
                            }
                        }
                    }
                    Some(Some(Event::Data(_cap, mut batches))) => {
                        to_publish.clear();

                        for batch in batches.drain(..) {
                            for_each_diff_pair(&batch, |_key, time, diff_pair: DiffPair<Row>| {
                                let Some(row) = diff_pair.after else {
                                    drop_stats.retractions += 1;
                                    return;
                                };
                                let datums: Vec<Datum> = row.iter().collect();
                                if !render_compiled_into(
                                    &compiled_template,
                                    &datums,
                                    &mut topic_buf,
                                ) {
                                    drop_stats.null_topic += 1;
                                    return;
                                }
                                if topic_buf.is_empty() {
                                    drop_stats.empty_topic += 1;
                                    return;
                                }
                                let payload = row_to_json(&row, &connection.value_desc);
                                to_publish.push((time, topic_buf.clone(), payload));
                            });
                        }

                        // `for_each_diff_pair` iterates in key order and only
                        // guarantees time order within a key, but with
                        // synthetic whole-row keys two versions of the same
                        // logical entity are different keys. Publishing (and
                        // conflating) in key order could then converge a topic
                        // to a stale value whenever a batch spans multiple
                        // timestamps. Sort by time so later updates win; the
                        // sort is stable, preserving within-time order.
                        to_publish.sort_by_key(|(time, _, _)| *time);

                        for (time, topic, payload) in to_publish.drain(..) {
                            let Some(window) = connection.dedup_window else {
                                publisher.stage(time, &topic, payload).await;
                                continue;
                            };
                            let now = Instant::now();
                            let window_elapsed = match dedup_cache.get(&topic) {
                                Some(last) => now.duration_since(*last) >= window,
                                None => true,
                            };
                            if window_elapsed {
                                // Any older pending payload for this topic is
                                // superseded by the row published now.
                                if pending.remove(&topic).is_some() {
                                    drop_stats.conflated += 1;
                                }
                                note_published(&mut dedup_cache, &topic, now, window);
                                publisher.stage(time, &topic, payload).await;
                            } else {
                                // Keep the latest payload (and its time) per
                                // topic. Rows arrive time-sorted, so a later
                                // insert never lowers the recorded time.
                                if pending.insert(topic, (time, payload)).is_some() {
                                    drop_stats.conflated += 1;
                                }
                                // Bound memory on high-cardinality topic
                                // spaces by flushing everything early rather
                                // than dropping payloads.
                                if pending.len() >= MAX_DEDUP_CACHE_ENTRIES {
                                    for (topic, (time, payload)) in std::mem::take(&mut pending) {
                                        note_published(&mut dedup_cache, &topic, now, window);
                                        publisher.stage(time, &topic, payload).await;
                                    }
                                }
                            }
                        }
                    }
                }

                // Flush pending payloads whose window has elapsed. When the
                // input has closed, flush everything for a best-effort final
                // delivery before shutdown.
                if !pending.is_empty() {
                    let window = connection
                        .dedup_window
                        .expect("pending is only populated when a dedup window is set");
                    let now = Instant::now();
                    let due: Vec<String> = pending
                        .keys()
                        .filter(|topic| {
                            input_closed
                                || match dedup_cache.get(*topic) {
                                    Some(last) => now.duration_since(*last) >= window,
                                    None => true,
                                }
                        })
                        .cloned()
                        .collect();
                    for topic in due {
                        let (time, payload) = pending.remove(&topic).expect("due topic is pending");
                        note_published(&mut dedup_cache, &topic, now, window);
                        publisher.stage(time, &topic, payload).await;
                    }
                }

                // Publish anything staged below the chunk boundary.
                publisher.flush().await;

                // An unrecoverable persistent-mode failure halts the sink so
                // the dataflow restarts and replays from the last confirmed
                // upper. The frontier was not advanced past the failed data.
                if let Some(error) = publisher.fatal.take() {
                    emit_sink_health(
                        &health_output,
                        &health_cap,
                        sink_id,
                        HealthStatusUpdate::halting(error, None),
                    );
                    std::future::pending::<()>().await;
                    unreachable!("pending future never returns");
                }

                // Persistent publish failure (with a live session) surfaces as
                // a stalled status; recovery flips back to running.
                report_health(
                    &health_output,
                    &health_cap,
                    sink_id,
                    &mut reported_healthy,
                    &session_stall,
                    publisher.consecutive_failures,
                );

                drop_stats.maybe_log(sink_id);
            }

            // The input is closed: the sink is complete (or being torn down).
            // Report the empty frontier, mirroring the Kafka sink.
            write_frontier.borrow_mut().clear();
        });

        (health_stream, vec![button.press_on_drop()])
    }
}

/// Hard cap on the number of entries in the dedup cache and pending buffer.
/// When the cache is full, expired entries are evicted first; if still full
/// (all within the window), the cache is cleared to prevent OOM on
/// high-cardinality topic spaces.
const MAX_DEDUP_CACHE_ENTRIES: usize = 100_000;

/// Messages staged before a publish is forced. Matches the vendored crate's
/// internal `publish_multiple` chunk size, so one flush is one FFI chunk:
/// a failure loses at most one chunk rather than an arbitrarily large staged
/// batch, and a whole-relation snapshot is never materialized as C-heap
/// messages all at once.
const PUBLISH_CHUNK: usize = 50;

/// Consecutive failed publish calls before the sink reports itself stalled.
const PUBLISH_STALL_THRESHOLD: u64 = 3;

/// Health output handle for the sink operator.
type SinkHealthOutput =
    AsyncOutputHandle<Timestamp, CapacityContainerBuilder<Vec<HealthStatusMessage>>>;

/// Emit a health status for the sink's export slot plus the global slot. The
/// capability is borrowed, not consumed, so the sink can keep cycling between
/// `stalled` and `running` as broker connectivity comes and goes.
fn emit_sink_health(
    health_output: &SinkHealthOutput,
    health_cap: &Option<Capability<Timestamp>>,
    sink_id: GlobalId,
    update: HealthStatusUpdate,
) {
    let Some(cap) = health_cap else {
        return;
    };
    for id in [Some(sink_id), None] {
        health_output.give(
            cap,
            HealthStatusMessage {
                id,
                namespace: StatusNamespace::Solace,
                update: update.clone(),
            },
        );
    }
}

/// Emit a stalled/running transition if the desired state (derived from
/// session connectivity and consecutive publish failures) differs from the
/// last reported one.
fn report_health(
    health_output: &SinkHealthOutput,
    health_cap: &Option<Capability<Timestamp>>,
    sink_id: GlobalId,
    reported_healthy: &mut bool,
    session_stall: &Option<String>,
    consecutive_publish_failures: u64,
) {
    let desired_stall: Option<String> = if let Some(reason) = session_stall {
        Some(reason.clone())
    } else if consecutive_publish_failures >= PUBLISH_STALL_THRESHOLD {
        Some(format!(
            "{consecutive_publish_failures} consecutive Solace publish failures"
        ))
    } else {
        None
    };
    match desired_stall {
        None if !*reported_healthy => {
            *reported_healthy = true;
            tracing::info!(sink_id = %sink_id, "Solace sink recovered");
            emit_sink_health(
                health_output,
                health_cap,
                sink_id,
                HealthStatusUpdate::running(),
            );
        }
        Some(reason) if *reported_healthy => {
            *reported_healthy = false;
            tracing::warn!(sink_id = %sink_id, %reason, "Solace sink stalled");
            emit_sink_health(
                health_output,
                health_cap,
                sink_id,
                HealthStatusUpdate::stalled(reason, None),
            );
        }
        _ => {}
    }
}

/// Publishes staged rows to the broker. In `Direct` mode messages are batched
/// and sent fire-and-forget in bounded chunks. In `Persistent` mode each
/// message is published with a broker acknowledgement, and the acks gate how
/// far the sink's frontier may advance (see [`Publisher::confirm_through`]).
struct Publisher {
    session: Arc<AsyncSession>,
    sink_id: GlobalId,
    statistics: SinkStatistics,
    mode: SolaceDeliveryMode,
    /// Direct-mode staging buffer, flushed in [`PUBLISH_CHUNK`]-sized chunks.
    msgs: Vec<OutboundMessage>,
    staged_bytes: u64,
    /// Persistent-mode outstanding acks, tagged with the input timestamp of
    /// the row that produced them. Ordered by insertion (broker order).
    pending_acks: Vec<(Timestamp, oneshot::Receiver<Result<(), SessionError>>)>,
    /// Consecutive failed Direct `publish_multiple` calls, reset on success.
    /// Drives the stalled health status via [`report_health`]. Persistent-mode
    /// failures halt instead (see [`Publisher::fatal`]).
    consecutive_failures: u64,
    /// Set when a persistent publish or ack fails unrecoverably. The event
    /// loop halts on this, so the dataflow restarts and replays from the last
    /// confirmed (durable) upper.
    fatal: Option<String>,
}

impl Publisher {
    /// Build a message for `topic` and stage (Direct) or publish-with-ack
    /// (Persistent) it. `time` is the input timestamp of the row, used in
    /// persistent mode to gate the frontier. Rows whose topic or message
    /// cannot be built are dropped with a warning.
    async fn stage(&mut self, time: Timestamp, topic: &str, payload: Vec<u8>) {
        let payload_len = u64::cast_from(payload.len());
        let delivery_mode = match self.mode {
            SolaceDeliveryMode::Direct => DeliveryMode::Direct,
            SolaceDeliveryMode::Persistent => DeliveryMode::Persistent,
        };
        let dest = match MessageDestination::new(DestinationType::Topic, topic) {
            Ok(d) => d,
            Err(err) => {
                tracing::warn!(
                    sink_id = %self.sink_id,
                    "Solace sink: invalid topic '{}': {}", topic, err
                );
                return;
            }
        };
        let message = match OutboundMessageBuilder::new()
            .delivery_mode(delivery_mode)
            .destination(dest)
            .payload(payload)
            .build()
        {
            Ok(m) => m,
            Err(err) => {
                tracing::warn!(
                    sink_id = %self.sink_id,
                    "Solace sink: failed to build message for topic '{}': {}",
                    topic,
                    err
                );
                return;
            }
        };

        match self.mode {
            SolaceDeliveryMode::Direct => {
                self.msgs.push(message);
                self.staged_bytes += payload_len;
                if self.msgs.len() >= PUBLISH_CHUNK {
                    self.flush().await;
                }
            }
            SolaceDeliveryMode::Persistent => {
                self.statistics.inc_messages_staged_by(1);
                self.statistics.inc_bytes_staged_by(payload_len);
                // publish_with_ack registers the correlation before the
                // blocking C publish, so run the whole call off-thread.
                let session = Arc::clone(&self.session);
                let sink_id = self.sink_id;
                let result = mz_ore::task::spawn_blocking(
                    || format!("solace_sink_publish({sink_id})"),
                    move || session.publish_with_ack(message),
                )
                .await;
                match result {
                    Ok(rx) => self.pending_acks.push((time, rx)),
                    Err(err) => {
                        // The message never left; block the frontier at its
                        // time by halting so the restart replays it.
                        self.fatal = Some(format!("persistent publish failed: {err}"));
                    }
                }
            }
        }
    }

    /// Publish all staged Direct messages. A failure drops the staged chunk
    /// (Direct delivery has no confirmation to retry against) and is reflected
    /// in the staged-vs-committed statistics gap and `consecutive_failures`.
    /// No-op in persistent mode, which publishes eagerly in [`Self::stage`].
    async fn flush(&mut self) {
        if self.msgs.is_empty() {
            return;
        }
        let staged_msgs = u64::cast_from(self.msgs.len());
        let staged_bytes = self.staged_bytes;
        self.statistics.inc_messages_staged_by(staged_msgs);
        self.statistics.inc_bytes_staged_by(staged_bytes);
        let msgs = std::mem::take(&mut self.msgs);
        self.staged_bytes = 0;
        // The C API publish is blocking (and serialized behind the session
        // mutex), so run it on a blocking thread rather than stalling the
        // timely worker. The message vector round-trips to preserve its
        // allocated capacity.
        let session = Arc::clone(&self.session);
        let sink_id = self.sink_id;
        let (result, mut msgs) = mz_ore::task::spawn_blocking(
            || format!("solace_sink_publish({sink_id})"),
            move || {
                let result = session.publish_multiple(&msgs);
                (result, msgs)
            },
        )
        .await;
        msgs.clear();
        self.msgs = msgs;
        match result {
            Ok(()) => {
                self.consecutive_failures = 0;
                self.statistics.inc_messages_committed_by(staged_msgs);
                self.statistics.inc_bytes_committed_by(staged_bytes);
            }
            Err(err) => {
                self.consecutive_failures += 1;
                tracing::warn!(
                    sink_id = %self.sink_id,
                    "Solace sink: publish_multiple failed: {}", err
                );
            }
        }
    }

    /// Persistent mode only: await the broker acks for every outstanding
    /// message whose input timestamp is below `frontier`, so the caller may
    /// advance the durable upper to `frontier` only once that data is
    /// confirmed delivered. A rejected ack or dropped sender sets [`fatal`],
    /// which halts the sink so the restart replays from the last confirmed
    /// upper. Direct mode is a no-op (delivery is not confirmed).
    async fn confirm_through(&mut self, frontier: &Antichain<Timestamp>) {
        if self.mode != SolaceDeliveryMode::Persistent {
            return;
        }
        // A message at `time` is below the frontier when the frontier is not
        // less-or-equal to it, i.e. every frontier element is strictly
        // greater. For the closed (empty) frontier this is true of every time.
        let below = |time: &Timestamp| !frontier.less_equal(time);
        let mut still_pending = Vec::with_capacity(self.pending_acks.len());
        // Draining in insertion (broker) order keeps confirmation monotonic.
        for (time, rx) in std::mem::take(&mut self.pending_acks) {
            if !below(&time) {
                still_pending.push((time, rx));
                continue;
            }
            match rx.await {
                Ok(Ok(())) => {
                    self.statistics.inc_messages_committed_by(1);
                }
                Ok(Err(err)) => {
                    self.fatal = Some(format!("broker rejected a persistent message: {err}"));
                }
                Err(_) => {
                    self.fatal = Some("persistent ack channel closed before delivery".to_string());
                }
            }
        }
        self.pending_acks = still_pending;
    }
}

/// Record an actual publish to `topic` at `now`, evicting expired entries
/// when the cache reaches its cap.
fn note_published(
    cache: &mut BTreeMap<String, Instant>,
    topic: &str,
    now: Instant,
    window: Duration,
) {
    if cache.len() >= MAX_DEDUP_CACHE_ENTRIES && !cache.contains_key(topic) {
        // Evict expired entries first to reclaim space.
        cache.retain(|_, last| now.duration_since(*last) < window);
        // If all entries are within the window (e.g. burst of unique topics),
        // clear completely to avoid OOM. A cleared cache may re-publish once
        // per topic, which is acceptable given the alternative of unbounded
        // growth.
        if cache.len() >= MAX_DEDUP_CACHE_ENTRIES {
            cache.clear();
        }
    }
    cache.insert(topic.to_string(), now);
}

/// Interval between rate-limited log lines summarizing unpublished rows.
const DROP_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Cumulative counters for rows the sink does not publish, logged at most
/// once per [`DROP_LOG_INTERVAL`]. Without these a sink that suppresses or
/// drops 100% of its input is indistinguishable from a healthy quiet one.
struct DropStats {
    /// Retraction-only diff pairs. Deletes are never published by design.
    retractions: u64,
    /// Rows whose topic template rendered a NULL column datum.
    null_topic: u64,
    /// Rows whose rendered topic was the empty string.
    empty_topic: u64,
    /// Payloads superseded by a newer payload for the same topic while
    /// suppressed by the dedup window.
    conflated: u64,
    /// Counter values as of the last emitted log line.
    logged: [u64; 4],
    last_log: Option<Instant>,
}

impl DropStats {
    fn new() -> Self {
        DropStats {
            retractions: 0,
            null_topic: 0,
            empty_topic: 0,
            conflated: 0,
            logged: [0; 4],
            last_log: None,
        }
    }

    fn maybe_log(&mut self, sink_id: GlobalId) {
        let totals = [
            self.retractions,
            self.null_topic,
            self.empty_topic,
            self.conflated,
        ];
        if totals == self.logged {
            return;
        }
        if let Some(last) = self.last_log {
            if last.elapsed() < DROP_LOG_INTERVAL {
                return;
            }
        }
        // NULL or empty rendered topics indicate a data problem the user
        // should fix, so those warrant `warn`. Retractions and conflation are
        // by-design behavior, reported at `info` for visibility.
        let data_problem = self.null_topic > self.logged[1] || self.empty_topic > self.logged[2];
        if data_problem {
            tracing::warn!(
                sink_id = %sink_id,
                retractions = self.retractions,
                null_topic = self.null_topic,
                empty_topic = self.empty_topic,
                conflated = self.conflated,
                "Solace sink: rows dropped due to NULL or empty rendered topic \
                 (cumulative unpublished-row counts)",
            );
        } else {
            tracing::info!(
                sink_id = %sink_id,
                retractions = self.retractions,
                null_topic = self.null_topic,
                empty_topic = self.empty_topic,
                conflated = self.conflated,
                "Solace sink: cumulative unpublished-row counts",
            );
        }
        self.logged = totals;
        self.last_log = Some(Instant::now());
    }
}

/// A pre-compiled piece of a topic template. Built once at sink initialization
/// so that the hot path avoids per-message string cloning and `.replace()` calls.
enum TemplatePart {
    Literal(String),
    /// Index of the column whose datum supplies this segment of the topic.
    Column(usize),
}

/// Compile a topic template string into a `Vec<TemplatePart>` at sink
/// initialization time. The resulting slice is passed to [`render_compiled_into`]
/// on each row without any further string allocation.
fn compile_template(template: &str, indices: &[(String, usize)]) -> Vec<TemplatePart> {
    // Build placeholder strings once so we don't format!() inside the loop.
    let placeholders: Vec<(String, usize)> = indices
        .iter()
        .map(|(name, idx)| (format!("{{{}}}", name), *idx))
        .collect();

    let mut parts: Vec<TemplatePart> = Vec::new();
    let mut remaining = template;
    loop {
        // Find the left-most placeholder in the remaining slice.
        let earliest = placeholders
            .iter()
            .filter_map(|(ph, idx)| remaining.find(ph.as_str()).map(|pos| (pos, ph.len(), *idx)))
            .min_by_key(|(pos, _, _)| *pos);

        let Some((pos, ph_len, col_idx)) = earliest else {
            break;
        };
        if pos > 0 {
            parts.push(TemplatePart::Literal(remaining[..pos].to_owned()));
        }
        parts.push(TemplatePart::Column(col_idx));
        remaining = &remaining[pos + ph_len..];
    }
    if !remaining.is_empty() {
        parts.push(TemplatePart::Literal(remaining.to_owned()));
    }
    parts
}

/// Write one datum into `buf` for topic rendering. Returns `false` if the
/// datum is NULL (caller should abort topic construction for this row).
/// All other variants write directly into `buf` without an intermediate String.
fn push_datum_to(datum: &Datum, buf: &mut String) -> bool {
    match datum {
        Datum::Null => return false,
        Datum::True => buf.push_str("true"),
        Datum::False => buf.push_str("false"),
        Datum::String(s) => buf.push_str(s),
        Datum::Int16(n) => {
            let _ = write!(buf, "{n}");
        }
        Datum::Int32(n) => {
            let _ = write!(buf, "{n}");
        }
        Datum::Int64(n) => {
            let _ = write!(buf, "{n}");
        }
        Datum::UInt8(n) => {
            let _ = write!(buf, "{n}");
        }
        Datum::UInt16(n) => {
            let _ = write!(buf, "{n}");
        }
        Datum::UInt32(n) => {
            let _ = write!(buf, "{n}");
        }
        Datum::UInt64(n) => {
            let _ = write!(buf, "{n}");
        }
        Datum::Float32(f) => {
            let _ = write!(buf, "{f}");
        }
        Datum::Float64(f) => {
            let _ = write!(buf, "{f}");
        }
        other => {
            let _ = write!(buf, "{other}");
        }
    }
    true
}

/// Render a pre-compiled template into `buf`, clearing it first. Returns
/// `false` if any column datum is NULL (caller should skip this row).
/// Reusing the same `buf` across calls avoids a heap allocation per message.
fn render_compiled_into(parts: &[TemplatePart], datums: &[Datum<'_>], buf: &mut String) -> bool {
    buf.clear();
    for part in parts {
        match part {
            TemplatePart::Literal(s) => buf.push_str(s),
            TemplatePart::Column(idx) => {
                if !push_datum_to(datums.get(*idx).unwrap_or(&Datum::Null), buf) {
                    return false;
                }
            }
        }
    }
    true
}

fn row_to_json(row: &Row, desc: &RelationDesc) -> Vec<u8> {
    let obj: serde_json::Map<String, Value> = desc
        .iter_names()
        .zip_eq(row.iter())
        .map(|(name, datum)| (name.to_string(), datum_to_json(&datum)))
        .collect();
    serde_json::to_vec(&obj).unwrap_or_else(|_| b"{}".to_vec())
}

fn datum_to_json(datum: &Datum) -> Value {
    match datum {
        Datum::Null => Value::Null,
        Datum::True => Value::Bool(true),
        Datum::False => Value::Bool(false),
        Datum::Int16(n) => Value::Number((*n).into()),
        Datum::Int32(n) => Value::Number((*n).into()),
        Datum::Int64(n) => Value::Number((*n).into()),
        Datum::UInt8(n) => Value::Number((*n).into()),
        Datum::UInt16(n) => Value::Number((*n).into()),
        Datum::UInt32(n) => Value::Number((*n).into()),
        Datum::UInt64(n) => Value::Number((*n).into()),
        Datum::Float32(f) => serde_json::Number::from_f64(f64::from(**f))
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Datum::Float64(f) => serde_json::Number::from_f64(**f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Datum::String(s) => Value::String(s.to_string()),
        other => Value::String(format!("{}", other)),
    }
}
