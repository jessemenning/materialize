# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Performance benchmark DDL setup.
#
# Creates:
#   perf_src  — Solace source on mz_perf_q (FORMAT BYTES, INCLUDE metadata)
#   mv1       — Parse JSON payload, expose seq + send_ts_ms columns
#   mv2       — Per-broker-second aggregation (cascades from mv1)
#   mv3       — Running totals across all seconds (cascades from mv2)
#   perf_sink — Solace sink publishing mv3 updates to mz/perf/stats
#
# The workflow (mzcompose.py) has already provisioned mz_user, mz_perf_q,
# and a sink verification queue mz_perf_sink_q subscribed to mz/perf/stats.

> CREATE SECRET perf_pw AS 'changeme'

> CREATE CONNECTION perf_conn TO SOLACE (
    HOST 'tcp://solace:55555',
    MESSAGE VPN 'default',
    USERNAME 'mz_user',
    PASSWORD SECRET perf_pw
  )

> CREATE SOURCE perf_src
  IN CLUSTER quickstart
  FROM SOLACE CONNECTION perf_conn (${source-opts})
  FORMAT BYTES
  INCLUDE
    REPLICATION GROUP MESSAGE ID AS rgmid,
    BROKER TIMESTAMP AS broker_ts

# MV1: parse JSON payload, expose typed columns.
# Messages are JSON: {"seq": N, "send_ts_ms": unix_ms}
> CREATE MATERIALIZED VIEW mv1 AS
  SELECT
    rgmid,
    broker_ts,
    (convert_from(data, 'UTF8')::jsonb->>'seq')::bigint        AS seq,
    (convert_from(data, 'UTF8')::jsonb->>'send_ts_ms')::bigint AS send_ts_ms
  FROM perf_src

# MV2: per-broker-second aggregation — cascades from mv1.
> CREATE MATERIALIZED VIEW mv2 AS
  SELECT
    date_trunc('second', broker_ts) AS ts_second,
    count(*)                        AS msg_count,
    max(seq)                        AS max_seq,
    min(send_ts_ms)                 AS min_send_ts_ms,
    max(send_ts_ms)                 AS max_send_ts_ms
  FROM mv1
  GROUP BY date_trunc('second', broker_ts)

# MV3: running totals — cascades from mv2.
> CREATE MATERIALIZED VIEW mv3 AS
  SELECT
    sum(msg_count)::bigint                                  AS total_msgs,
    count(*)::bigint                                        AS seconds_with_data,
    (sum(msg_count)::numeric / NULLIF(count(*), 0))::numeric(10,1) AS avg_msgs_per_second,
    max(max_seq)                                            AS latest_seq,
    max(max_send_ts_ms)                                     AS latest_send_ts_ms
  FROM mv2

# Sink: publish mv3 running-totals updates to a Solace topic.
# The mzcompose workflow verifies messages arrive in mz_perf_sink_q.
> CREATE SINK perf_sink
  IN CLUSTER quickstart
  FROM mv3
  INTO SOLACE CONNECTION perf_conn (
    TOPIC 'mz/perf/stats'
  )

# Allow the sink session to open before the publisher starts flooding.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=3s
