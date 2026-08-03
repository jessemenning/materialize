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
//! Delivery is fire-and-forget Direct messaging with no durable output shard,
//! so the sink reports at-most-once progress: the write frontier follows the
//! input frontier once batches have been handed to the Solace session.
//!
//! NOTE: dedup state is per timely worker. Each worker throttles the topics
//! it renders independently, so a topic whose rows are spread across N
//! workers can publish up to N messages per window.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;
use std::rc::Rc;
use std::time::{Duration, Instant};

use itertools::Itertools as _;

use differential_dataflow::VecCollection;
use futures::StreamExt;
use mz_interchange::avro::DiffPair;
use mz_interchange::envelopes::for_each_diff_pair;
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_repr::{Datum, Diff, GlobalId, RelationDesc, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::{SolaceSinkConnection, StorageSinkDesc};
use mz_timely_util::builder_async::{
    Event, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use serde_json::Value;
use solace_rs::SolaceLogLevel;
use solace_rs::async_support::AsyncSessionBuilder;
use solace_rs::context::Context;
use solace_rs::message::{
    DeliveryMode, DestinationType, MessageDestination, OutboundMessage, OutboundMessageBuilder,
};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::StreamVec;
use timely::progress::{Antichain, Timestamp as _};

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::render::sinks::{SinkBatchStream, SinkRender};
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
        _sink: &StorageSinkDesc<CollectionMetadata, Timestamp>,
        sink_id: GlobalId,
        batches: SinkBatchStream<'scope>,
        _key_is_synthetic: bool,
        _err_collection: VecCollection<'scope, Timestamp, DataflowError, Diff>,
    ) -> (
        StreamVec<'scope, Timestamp, HealthStatusMessage>,
        Vec<PressOnDropButton>,
    ) {
        let connection = self.clone();
        let secrets_reader = std::sync::Arc::clone(
            &storage_state
                .storage_configuration
                .connection_context
                .secrets_reader,
        );

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
                    tracing::warn!(
                        sink_id = %sink_id,
                        "Solace sink: failed to read PASSWORD secret: {}",
                        err.display_with_causes()
                    );
                    return;
                }
            };

            let context = match Context::new(SolaceLogLevel::Error) {
                Ok(ctx) => ctx,
                Err(err) => {
                    tracing::warn!(
                        sink_id = %sink_id,
                        "Solace sink: failed to initialize context: {}",
                        err.display_with_causes()
                    );
                    return;
                }
            };

            let host = connection.connection.host.clone();
            let msg_vpn = connection.connection.msg_vpn.clone();
            let username = connection.connection.username.clone();

            let mut builder = AsyncSessionBuilder::new(&context)
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
                builder = builder.ssl_trust_store_dir(dir);
            }

            let session = builder.build();
            let session = match session {
                Ok(s) => s,
                Err(err) => {
                    tracing::warn!(
                        sink_id = %sink_id,
                        "Solace sink: failed to open session to {host} (vpn {msg_vpn}): {}",
                        err.display_with_causes()
                    );
                    return;
                }
            };

            tracing::info!(sink_id = %sink_id, "Solace sink connected to {host} (vpn {msg_vpn})");

            // Signal running so Materialize transitions out of `starting`.
            if let Some(ref cap) = health_cap {
                for id in [Some(sink_id), None] {
                    health_output.give(
                        cap,
                        HealthStatusMessage {
                            id,
                            namespace: StatusNamespace::Solace,
                            update: HealthStatusUpdate::running(),
                        },
                    );
                }
            }

            // Dedup state: rendered topic -> time of the last actual publish.
            let mut dedup_cache: BTreeMap<String, Instant> = BTreeMap::new();
            // Latest payload per topic suppressed by the dedup window,
            // awaiting flush. Newer suppressed payloads replace older ones
            // (conflation), so at most one payload per topic is pending.
            let mut pending: BTreeMap<String, Vec<u8>> = BTreeMap::new();

            let mut drop_stats = DropStats::new();

            // Reusable buffers declared outside the event loop so their heap
            // capacity persists across batches, avoiding per-batch allocations.
            let mut topic_buf = String::new();
            let mut to_publish: Vec<(String, Vec<u8>)> = Vec::new();
            let mut msgs_to_send: Vec<OutboundMessage> = Vec::new();

            let mut input_closed = false;
            while !input_closed {
                // Await the next input event, or the earliest pending flush
                // deadline when the dedup window has payloads waiting.
                // `Some(ev)` is an input event, `None` means the timer fired.
                let wake = if pending.is_empty() {
                    Some(input.next().await)
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
                    tokio::select! {
                        ev = input.next() => Some(ev),
                        _ = tokio::time::sleep(deadline.saturating_duration_since(now)) => None,
                    }
                };

                msgs_to_send.clear();
                let mut staged_bytes = 0;

                match wake {
                    // Flush timer fired; fall through to the pending flush.
                    None => {}
                    Some(None) => input_closed = true,
                    Some(Some(Event::Progress(frontier))) => {
                        // Direct delivery is fire-and-forget with no durable
                        // output, so input progress is an honest at-most-once
                        // write frontier. Reporting it lets the controller
                        // downgrade the sink's read hold on its input, which
                        // unblocks upstream compaction and avoids a full
                        // re-snapshot on restart. Payloads still buffered in
                        // `pending` are deliberately not accounted for:
                        // losing them on restart is within the at-most-once
                        // contract.
                        write_frontier.borrow_mut().clone_from(&frontier);
                    }
                    Some(Some(Event::Data(_cap, mut batches))) => {
                        to_publish.clear();

                        for batch in batches.drain(..) {
                            for_each_diff_pair(&batch, |_key, _time, diff_pair: DiffPair<Row>| {
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
                                to_publish.push((topic_buf.clone(), payload));
                            });
                        }

                        for (topic, payload) in to_publish.drain(..) {
                            let Some(window) = connection.dedup_window else {
                                stage_message(
                                    sink_id,
                                    &topic,
                                    payload,
                                    &mut msgs_to_send,
                                    &mut staged_bytes,
                                );
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
                                stage_message(
                                    sink_id,
                                    &topic,
                                    payload,
                                    &mut msgs_to_send,
                                    &mut staged_bytes,
                                );
                            } else {
                                if pending.insert(topic, payload).is_some() {
                                    drop_stats.conflated += 1;
                                }
                                // Bound memory on high-cardinality topic
                                // spaces by flushing everything early rather
                                // than dropping payloads.
                                if pending.len() >= MAX_DEDUP_CACHE_ENTRIES {
                                    for (topic, payload) in std::mem::take(&mut pending) {
                                        note_published(&mut dedup_cache, &topic, now, window);
                                        stage_message(
                                            sink_id,
                                            &topic,
                                            payload,
                                            &mut msgs_to_send,
                                            &mut staged_bytes,
                                        );
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
                        let payload = pending.remove(&topic).expect("due topic is pending");
                        note_published(&mut dedup_cache, &topic, now, window);
                        stage_message(
                            sink_id,
                            &topic,
                            payload,
                            &mut msgs_to_send,
                            &mut staged_bytes,
                        );
                    }
                }

                if !msgs_to_send.is_empty() {
                    let staged_msgs = u64::cast_from(msgs_to_send.len());
                    statistics.inc_messages_staged_by(staged_msgs);
                    statistics.inc_bytes_staged_by(staged_bytes);
                    if let Err(err) = session.publish_multiple(&msgs_to_send) {
                        tracing::warn!(
                            sink_id = %sink_id,
                            "Solace sink: publish_multiple failed: {}", err
                        );
                    } else {
                        statistics.inc_messages_committed_by(staged_msgs);
                        statistics.inc_bytes_committed_by(staged_bytes);
                    }
                }

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

/// Build a Direct-delivery message for `topic` and stage it for publishing,
/// accumulating its payload size into `staged_bytes`. Rows whose topic or
/// message cannot be built are dropped with a warning.
fn stage_message(
    sink_id: GlobalId,
    topic: &str,
    payload: Vec<u8>,
    msgs: &mut Vec<OutboundMessage>,
    staged_bytes: &mut u64,
) {
    let payload_len = u64::cast_from(payload.len());
    let dest = match MessageDestination::new(DestinationType::Topic, topic) {
        Ok(d) => d,
        Err(err) => {
            tracing::warn!(
                sink_id = %sink_id,
                "Solace sink: invalid topic '{}': {}", topic, err
            );
            return;
        }
    };
    match OutboundMessageBuilder::new()
        .delivery_mode(DeliveryMode::Direct)
        .destination(dest)
        .payload(payload)
        .build()
    {
        Ok(m) => {
            msgs.push(m);
            *staged_bytes += payload_len;
        }
        Err(err) => {
            tracing::warn!(
                sink_id = %sink_id,
                "Solace sink: failed to build message for topic '{}': {}",
                topic,
                err
            );
        }
    }
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
/// initialization time. The resulting slice is passed to [`render_compiled`]
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
