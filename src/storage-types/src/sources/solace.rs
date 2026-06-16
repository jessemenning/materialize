// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to Solace Platform sources.
//!
//! The Solace source reads from a Solace broker via guaranteed (persistent)
//! messaging — either a durable queue or a durable topic endpoint with a
//! topic subscription. The broker-assigned, monotonic
//! ReplicationGroupMessageId (RGMID) doubles as the source's resumption
//! token, plumbed through Materialize's existing reclock/remap machinery
//! (see [`crate::sources::postgres`] for the closest scalar-watermark
//! analog).
//!
//! Phase 1 scope: types & schemas only. Runtime ingestion lives in the
//! `storage` crate (Phase 3+).

use std::fmt;
use std::sync::LazyLock;

use mz_repr::{CatalogItemId, Datum, GlobalId, RelationDesc, Row, SqlColumnType, SqlScalarType};
use serde::{Deserialize, Serialize};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::timestamp::Refines;
use timely::progress::{PathSummary, Timestamp};

use crate::AlterCompatible;
use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::controller::AlterError;
use crate::sources::{SourceConnection, SourceTimestamp};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.sources.solace.rs"
));

/// What the Solace source binds to on the broker.
///
/// Solace exposes two flavors of durable consumption: directly from a
/// queue, or from a *durable topic endpoint* (DTE) that aggregates one
/// or more topic subscriptions. The two are mutually exclusive at
/// `CREATE SOURCE` time.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum SolaceBindEntity {
    /// Bind to an existing durable queue by name.
    Queue { name: String },
    /// Bind to an existing durable topic endpoint and (re)apply a topic
    /// subscription to it on every connect. Supports `*` (single-level)
    /// and `>` (terminal multi-level) SMF wildcards. Phase 6+.
    TopicEndpoint { name: String, subscription: String },
}

impl SolaceBindEntity {
    /// The human-readable name for use in `mz_internal.mz_sources.external_reference`
    /// and error messages.
    pub fn external_reference(&self) -> &str {
        match self {
            SolaceBindEntity::Queue { name } => name,
            SolaceBindEntity::TopicEndpoint { name, .. } => name,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SolaceSourceConnection<C: ConnectionAccess = InlinedConnection> {
    pub connection: C::Solace,
    pub connection_id: CatalogItemId,
    pub bind_entity: SolaceBindEntity,
    /// Metadata columns surfaced by `INCLUDE` clauses. One entry per
    /// `INCLUDE` item; `TopicLevels` expands to multiple physical columns
    /// at column-desc time via [`solace_metadata_columns_desc`].
    pub metadata_columns: Vec<(String, SolaceMetadataKind)>,
    /// Solace flow window size. Default 255 (Solace's default).
    pub ack_window_size: u32,
    /// Maximum unacked messages before the broker stops delivering. Default 10000.
    /// `-1` means "broker-configured maximum".
    pub flow_max_unacked: i32,
    /// When true, RGMIDs at or below the persisted watermark are dropped at
    /// ingestion time, giving exactly-once delivery into persist. When false,
    /// post-restart redeliveries flow through (at-least-once escape hatch).
    pub deduplicate: bool,
    /// Number of consumer workers. For a non-exclusive queue, each active
    /// worker opens its own flow and the broker load-balances messages across
    /// them. Effective parallelism = min(parallelism, cluster worker_count).
    pub parallelism: u32,
    /// When true, use `SOLCLIENT_FLOW_PROP_ACKMODE_AUTO`: the Solace C SDK
    /// acks messages immediately on delivery, before Materialize processes
    /// them. Eliminates exactly-once but maximises throughput for sources
    /// where at-most-once delivery is acceptable (e.g. rolling telemetry
    /// windows). When false (default), ack after persist-commit.
    pub auto_ack: bool,
}

impl<R: ConnectionResolver> IntoInlineConnection<SolaceSourceConnection, R>
    for SolaceSourceConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> SolaceSourceConnection {
        let SolaceSourceConnection {
            connection,
            connection_id,
            bind_entity,
            metadata_columns,
            ack_window_size,
            flow_max_unacked,
            deduplicate,
            parallelism,
            auto_ack,
        } = self;
        SolaceSourceConnection {
            connection: r.resolve_connection(connection).unwrap_solace(),
            connection_id,
            bind_entity,
            metadata_columns,
            ack_window_size,
            flow_max_unacked,
            deduplicate,
            parallelism,
            auto_ack,
        }
    }
}

/// Schema of the per-source remap shard for Solace sources in MVP scope:
/// a single nullable `bytea` column holding the raw 16-byte RGMID
/// watermark. `NULL` represents the "no progress yet" state.
///
/// Phase 8 (partitioned queues) extends this to a partition-keyed map.
pub static SOLACE_PROGRESS_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("rgmid", SqlScalarType::Bytes.nullable(true))
        .finish()
});

impl<C: ConnectionAccess> SourceConnection for SolaceSourceConnection<C> {
    fn name(&self) -> &'static str {
        "solace"
    }

    fn external_reference(&self) -> Option<&str> {
        Some(self.bind_entity.external_reference())
    }

    fn default_key_desc(&self) -> RelationDesc {
        // Solace messages have no separate key field; UPSERT keys come
        // from `INCLUDE`-d metadata columns (e.g. APPLICATION MESSAGE ID).
        RelationDesc::empty()
    }

    fn default_value_desc(&self) -> RelationDesc {
        RelationDesc::builder()
            .with_column("value", SqlScalarType::Bytes.nullable(true))
            .finish()
    }

    fn timestamp_desc(&self) -> RelationDesc {
        SOLACE_PROGRESS_DESC.clone()
    }

    fn connection_id(&self) -> Option<CatalogItemId> {
        Some(self.connection_id)
    }

    fn supports_read_only(&self) -> bool {
        // The source must actively bind, consume, and ack — there is no
        // passive read-only mode against a Solace broker.
        false
    }

    fn prefers_single_replica(&self) -> bool {
        // Only one cluster replica should hold the Solace flow at a time;
        // other replicas stand by for HA. Matches plan §"Cluster replicas
        // vs. parallelism".
        true
    }
}

impl<C: ConnectionAccess> AlterCompatible for SolaceSourceConnection<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }

        let SolaceSourceConnection {
            connection,
            connection_id,
            bind_entity,
            metadata_columns,
            ack_window_size,
            flow_max_unacked,
            deduplicate,
            parallelism,
            auto_ack,
        } = self;

        let compatibility_checks = [
            (
                connection.alter_compatible(id, &other.connection).is_ok(),
                "connection",
            ),
            (connection_id == &other.connection_id, "connection_id"),
            (bind_entity == &other.bind_entity, "bind_entity"),
            (
                metadata_columns == &other.metadata_columns,
                "metadata_columns",
            ),
            (ack_window_size == &other.ack_window_size, "ack_window_size"),
            (
                flow_max_unacked == &other.flow_max_unacked,
                "flow_max_unacked",
            ),
            (deduplicate == &other.deduplicate, "deduplicate"),
            (parallelism == &other.parallelism, "parallelism"),
            (auto_ack == &other.auto_ack, "auto_ack"),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "SolaceSourceConnection incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );
                return Err(AlterError { id });
            }
        }
        Ok(())
    }
}

/// Which piece of Solace message metadata a column corresponds to.
///
/// One entry in [`SolaceSourceConnection::metadata_columns`] maps to one
/// physical column, *except* `TopicLevels` which expands to `count`
/// columns at desc time (see [`solace_metadata_columns_desc`]).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SolaceMetadataKind {
    /// 16-byte broker-opaque RGMID, exposed as `bytea`.
    ReplicationGroupMessageId,
    /// Broker receive timestamp (`getRcvTimestamp`).
    BrokerTimestamp,
    /// Sender-set publish timestamp (`getSenderTimestamp`); may be NULL.
    SenderTimestamp,
    /// Sender-set application message ID, exposed as `text`.
    ApplicationMessageId,
    /// Sender-set correlation ID, exposed as `text`.
    CorrelationId,
    /// Resolved topic the message was published to, exposed as `text`.
    /// For queue sources this is whatever topic the publisher used; for
    /// DTE sources this is the concrete topic that matched the subscription.
    Topic,
    /// Decompose the resolved topic on `/`. The outer alias in
    /// `(String, SolaceMetadataKind)` is the *prefix*; this kind expands
    /// to `count` columns named `<prefix>_1`..`<prefix>_<count>`.
    /// Messages with fewer levels than `count` get NULL trailing columns;
    /// messages with more get the suffix joined back into the last column.
    TopicLevels { count: u32 },
    /// Partition ID for partitioned queues; NULL for non-partitioned
    /// queues. Phase 8.
    Partition,
    /// All user properties, exposed as `map[text => text]` (mirrors the
    /// representation of Kafka headers).
    UserProperties,
}

/// Expand a list of metadata column items into concrete `(name, type)`
/// pairs for the source's relation descriptor.
///
/// `TopicLevels { count }` expands to `count` `text NULLABLE` columns named
/// `<prefix>_1`..`<prefix>_<count>`; all other kinds emit a single column
/// named after the alias.
pub fn solace_metadata_columns_desc(
    metadata_columns: &Vec<(String, SolaceMetadataKind)>,
) -> Vec<(String, SqlColumnType)> {
    let mut out = Vec::with_capacity(metadata_columns.len());
    for (alias, kind) in metadata_columns {
        match kind {
            SolaceMetadataKind::ReplicationGroupMessageId => {
                out.push((alias.clone(), SqlScalarType::Bytes.nullable(false)));
            }
            SolaceMetadataKind::BrokerTimestamp => {
                out.push((
                    alias.clone(),
                    SqlScalarType::Timestamp { precision: None }.nullable(true),
                ));
            }
            SolaceMetadataKind::SenderTimestamp => {
                out.push((
                    alias.clone(),
                    SqlScalarType::Timestamp { precision: None }.nullable(true),
                ));
            }
            SolaceMetadataKind::ApplicationMessageId
            | SolaceMetadataKind::CorrelationId
            | SolaceMetadataKind::Topic => {
                out.push((alias.clone(), SqlScalarType::String.nullable(true)));
            }
            SolaceMetadataKind::TopicLevels { count } => {
                for i in 1..=*count {
                    out.push((format!("{alias}_{i}"), SqlScalarType::String.nullable(true)));
                }
            }
            SolaceMetadataKind::Partition => {
                out.push((alias.clone(), SqlScalarType::Int32.nullable(true)));
            }
            SolaceMetadataKind::UserProperties => {
                out.push((
                    alias.clone(),
                    SqlScalarType::Map {
                        value_type: Box::new(SqlScalarType::String),
                        custom_id: None,
                    }
                    .nullable(false),
                ));
            }
        }
    }
    out
}

/// Per-export Solace-specific details captured at `CREATE TABLE … FROM
/// SOURCE` / `CREATE SUBSOURCE` planning time.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SolaceSourceExportDetails {
    pub metadata_columns: Vec<(String, SolaceMetadataKind)>,
}

impl AlterCompatible for SolaceSourceExportDetails {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        let Self { metadata_columns } = self;
        let compatibility_checks = [(
            metadata_columns == &other.metadata_columns,
            "metadata_columns",
        )];
        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "SolaceSourceExportDetails incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );
                return Err(AlterError { id });
            }
        }
        Ok(())
    }
}

/// Source timestamp for the non-partitioned-queue MVP: the raw 16-byte
/// Solace ReplicationGroupMessageId, or `None` (Absent) when no message
/// has been observed yet.
///
/// **Ordering**: derived byte-wise. Within a single broker (or HA pair)
/// this matches the broker's authoritative comparator because the RGMID
/// byte layout is designed to be byte-comparable. For cross-broker
/// scenarios (DR replication failover), call the explicit broker
/// comparator from `solace_rs` (`compare_replication_group_message_ids`)
/// instead of relying on `Ord`. Phase 8 generalizes this to per-partition
/// RGMIDs via `Partitioned<RangeBound<PartitionId>, …>`.
#[derive(
    Clone,
    Copy,
    Default,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize
)]
pub struct SolaceTimestamp(pub Option<[u8; 16]>);

impl fmt::Display for SolaceTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => write!(f, "Absent"),
            Some(bytes) => {
                write!(f, "Rgmid(")?;
                for b in &bytes {
                    write!(f, "{:02x}", b)?;
                }
                write!(f, ")")
            }
        }
    }
}

impl Timestamp for SolaceTimestamp {
    // No need to describe complex summaries.
    type Summary = ();

    fn minimum() -> Self {
        SolaceTimestamp(None)
    }
}

impl TotalOrder for SolaceTimestamp {}

impl PartialOrder for SolaceTimestamp {
    fn less_equal(&self, other: &Self) -> bool {
        self <= other
    }
}

impl PathSummary<SolaceTimestamp> for () {
    fn results_in(&self, src: &SolaceTimestamp) -> Option<SolaceTimestamp> {
        Some(*src)
    }

    fn followed_by(&self, _other: &Self) -> Option<Self> {
        Some(())
    }
}

impl Refines<()> for SolaceTimestamp {
    fn to_inner(_other: ()) -> Self {
        Self::minimum()
    }

    fn to_outer(self) -> () {}

    fn summarize(_path: Self::Summary) -> <() as Timestamp>::Summary {}
}

impl SolaceTimestamp {
    /// Return the smallest `SolaceTimestamp` that is strictly greater than
    /// `self`, or `None` if `self` is `Absent` or the maximum 128-bit value
    /// (overflow, astronomically unlikely with real broker-assigned RGMIDs).
    pub fn next(&self) -> Option<SolaceTimestamp> {
        let bytes = self.0?;
        let mut next = bytes;
        for i in (0..16).rev() {
            if next[i] < 0xFF {
                next[i] += 1;
                return Some(SolaceTimestamp(Some(next)));
            }
            next[i] = 0;
        }
        None
    }
}

impl columnation::Columnation for SolaceTimestamp {
    type InnerRegion = columnation::CopyRegion<SolaceTimestamp>;
}

impl SourceTimestamp for SolaceTimestamp {
    fn encode_row(&self) -> Row {
        match self.0 {
            None => Row::pack([Datum::Null]),
            Some(bytes) => {
                // Hold the array alive in a local so the &[u8] in Datum::Bytes
                // borrows from it.
                let row = Row::pack([Datum::Bytes(&bytes[..])]);
                row
            }
        }
    }

    fn decode_row(row: &Row) -> Self {
        let mut datums = row.iter();
        match (datums.next(), datums.next()) {
            (Some(Datum::Null), None) => SolaceTimestamp(None),
            (Some(Datum::Bytes(b)), None) => {
                let arr: [u8; 16] = b
                    .try_into()
                    .expect("SOLACE_PROGRESS_DESC guarantees 16-byte RGMID");
                SolaceTimestamp(Some(arr))
            }
            _ => panic!("invalid row {row:?}"),
        }
    }
}
