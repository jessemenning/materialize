// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Convenience helpers for planning Solace Platform sources from SQL.

use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    SolaceSinkConfigOption, SolaceSinkConfigOptionName, SolaceSourceConfigOption,
    SolaceSourceConfigOptionName,
};

use crate::names::Aug;

/// Solace default flow window size, mirroring `SOLCLIENT_FLOW_PROP_WINDOW_SIZE`
/// (255). The user can override via `WITH (ACK WINDOW SIZE = ...)`.
pub const DEFAULT_ACK_WINDOW_SIZE: i64 = 255;

/// Default broker-side maximum-unacked threshold. `-1` means "use the broker's
/// configured maximum"; positive values cap the broker delivery before pause.
pub const DEFAULT_FLOW_MAX_UNACKED: i64 = 10_000;

/// Default for the dedup-on-restart toggle. The whole point of the Solace source
/// is exactly-once via the RGMID watermark, so this is on by default.
pub const DEFAULT_DEDUPLICATE: bool = true;

/// Default consumer worker count for non-partitioned queues. Phase 8 will adjust
/// the default for partitioned queues to "one worker per partition".
pub const DEFAULT_PARALLELISM: i64 = 1;

/// Default ack mode. "client" means ack after persist-commit (exactly-once).
/// "auto" means ack on delivery (at-most-once, maximum throughput).
pub const DEFAULT_ACK_MODE: &str = "client";

generate_extracted_config!(
    SolaceSourceConfigOption,
    (Queue, String),
    (DurableTopicEndpoint, String),
    (TopicSubscription, String),
    (AckWindowSize, i64, Default(DEFAULT_ACK_WINDOW_SIZE)),
    (FlowMaxUnacked, i64, Default(DEFAULT_FLOW_MAX_UNACKED)),
    (Deduplicate, bool, Default(DEFAULT_DEDUPLICATE)),
    (Parallelism, i64, Default(DEFAULT_PARALLELISM)),
    (AckMode, String, Default(DEFAULT_ACK_MODE.to_string()))
);

generate_extracted_config!(
    SolaceSinkConfigOption,
    (Topic, String),
    (DedupWindow, String)
);
