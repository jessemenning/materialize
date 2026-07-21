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
//! template.  Only insertions (`diff_pair.after.is_some()`) are published;
//! retractions are silently dropped.  An optional in-memory dedup cache
//! suppresses repeated publishes to the same rendered topic within a window.

use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;
use std::time::{Duration, Instant};

use itertools::Itertools as _;

use differential_dataflow::VecCollection;
use futures::StreamExt;
use mz_interchange::avro::DiffPair;
use mz_interchange::envelopes::for_each_diff_pair;
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
    DeliveryMode, DestinationType, MessageDestination, OutboundMessageBuilder,
};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::StreamVec;

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

            // In-memory dedup: rendered_topic -> last publish time
            let mut dedup_cache: BTreeMap<String, Instant> = BTreeMap::new();

            // Reusable buffers declared outside the event loop so their heap
            // capacity persists across batches, avoiding per-batch allocations.
            let mut topic_buf = String::new();
            let mut to_publish: Vec<(String, Vec<u8>)> = Vec::new();
            let mut msgs_to_send = Vec::new();

            while let Some(event) = input.next().await {
                if let Event::Data(_cap, mut batches) = event {
                    to_publish.clear();

                    for batch in batches.drain(..) {
                        for_each_diff_pair(&batch, |_key, _time, diff_pair: DiffPair<Row>| {
                            if let Some(row) = diff_pair.after {
                                let datums: Vec<Datum> = row.iter().collect();
                                if !render_compiled_into(
                                    &compiled_template,
                                    &datums,
                                    &mut topic_buf,
                                ) || topic_buf.is_empty()
                                {
                                    return;
                                }
                                let payload = row_to_json(&row, &connection.value_desc);
                                to_publish.push((topic_buf.clone(), payload));
                            }
                        });
                    }

                    msgs_to_send.clear();
                    for (topic, payload) in to_publish.drain(..) {
                        if !should_publish(&topic, &mut dedup_cache, connection.dedup_window) {
                            continue;
                        }
                        let dest =
                            match MessageDestination::new(DestinationType::Topic, topic.clone()) {
                                Ok(d) => d,
                                Err(err) => {
                                    tracing::warn!(
                                        sink_id = %sink_id,
                                        "Solace sink: invalid topic '{}': {}", topic, err
                                    );
                                    continue;
                                }
                            };
                        match OutboundMessageBuilder::new()
                            .delivery_mode(DeliveryMode::Direct)
                            .destination(dest)
                            .payload(payload)
                            .build()
                        {
                            Ok(m) => msgs_to_send.push(m),
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
                    if !msgs_to_send.is_empty() {
                        if let Err(err) = session.publish_multiple(&msgs_to_send) {
                            tracing::warn!(
                                sink_id = %sink_id,
                                "Solace sink: publish_multiple failed: {}", err
                            );
                        }
                    }
                }
            }
        });

        (health_stream, vec![button.press_on_drop()])
    }
}

/// Hard cap on the number of entries in the dedup cache. When full, expired
/// entries are evicted first; if still full (all within the window), the cache
/// is cleared to prevent OOM on high-cardinality topic spaces.
const MAX_DEDUP_CACHE_ENTRIES: usize = 100_000;

fn should_publish(
    topic: &str,
    cache: &mut BTreeMap<String, Instant>,
    window: Option<Duration>,
) -> bool {
    let Some(window) = window else {
        return true;
    };
    let now = Instant::now();
    if let Some(last) = cache.get(topic) {
        if now.duration_since(*last) < window {
            return false;
        }
    }
    if cache.len() >= MAX_DEDUP_CACHE_ENTRIES {
        // Evict expired entries first to reclaim space.
        cache.retain(|_, last| now.duration_since(*last) < window);
        // If all entries are within the window (e.g. burst of unique topics),
        // clear completely to avoid OOM. A cleared cache may re-publish once per
        // topic, which is acceptable given the alternative of unbounded growth.
        if cache.len() >= MAX_DEDUP_CACHE_ENTRIES {
            cache.clear();
        }
    }
    cache.insert(topic.to_string(), now);
    true
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
