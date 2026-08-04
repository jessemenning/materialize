# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Materialized-view-fed Solace sink test. This is the scenario that surfaced
# the sink's original silence bug: a sink reading a materialized view that
# keeps changing published its initial snapshot and then went quiet, because
# the sink never advanced a write frontier and (with DEDUP WINDOW) dropped
# rather than conflated suppressed rows.
#
# Two phases, each with its own verification queue provisioned by the
# mzcompose workflow:
#   Phase 1 (mz_mv_q, topic 'mz/sink-test/mv'): a running total over a single
#     key updated in separate statements. Every update must publish, and the
#     sink's write frontier in mz_internal.mz_frontiers must advance past 0
#     (it was pinned at the creation as_of before the frontier fix).
#   Phase 2 (mz_mv_null_q, topic 'mz/sink-test/mv-null/<region>'): a view whose
#     topic column is NULL for some rows. NULL-topic rows are dropped; the rest
#     publish.

> DROP SINK IF EXISTS sink_mv
> DROP SINK IF EXISTS sink_mv_null
> DROP MATERIALIZED VIEW IF EXISTS mv_totals
> DROP MATERIALIZED VIEW IF EXISTS mv_regions
> DROP TABLE IF EXISTS mv_src
> DROP TABLE IF EXISTS mv_null_src
> DROP CONNECTION IF EXISTS solace_sink_conn CASCADE
> DROP SECRET IF EXISTS solace_sink_pw

> CREATE SECRET solace_sink_pw AS 'changeme'

> CREATE CONNECTION solace_sink_conn TO SOLACE (
    HOST 'tcp://solace:55555',
    MESSAGE VPN 'default',
    USERNAME 'mz_user',
    PASSWORD SECRET solace_sink_pw
  )

# ---- Phase 1: continuous-change MV feed + write-frontier advancement --------

> CREATE TABLE mv_src (id INT, val INT)

# Running total per id. Inserting a new row for an existing id retracts the old
# total and inserts the new one; the sink publishes the insertion, so each
# update produces exactly one message.
> CREATE MATERIALIZED VIEW mv_totals AS
    SELECT id, sum(val)::INT AS total FROM mv_src GROUP BY id

> CREATE SINK sink_mv
  IN CLUSTER quickstart
  FROM mv_totals
  INTO SOLACE CONNECTION solace_sink_conn (
    TOPIC 'mz/sink-test/mv'
  )

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=3s

# Four updates to a single key in separate statements, each a distinct
# timestamp. Totals: 10, 30, 60, 100.
> INSERT INTO mv_src VALUES (1, 10)
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=700ms
> INSERT INTO mv_src VALUES (1, 20)
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=700ms
> INSERT INTO mv_src VALUES (1, 30)
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=700ms
> INSERT INTO mv_src VALUES (1, 40)

# The sink must advance a write frontier as it processes input progress. Before
# the frontier fix this stayed pinned at the creation as_of forever, which also
# blocked upstream compaction and forced a full re-snapshot on restart. This
# retrying assertion polls mz_frontiers until the sink's write frontier moves
# past 0.
> SELECT sinks.name
  FROM mz_internal.mz_frontiers frontiers
  JOIN mz_sinks sinks ON frontiers.object_id = sinks.id
  WHERE sinks.name = 'sink_mv' AND frontiers.write_frontier > 0
sink_mv

# Allow the last update to publish before the workflow consumes.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=2s

# ---- Phase 2: NULL topic column from a view --------------------------------

> CREATE TABLE mv_null_src (id INT, region TEXT)

> CREATE MATERIALIZED VIEW mv_regions AS
    SELECT id, region FROM mv_null_src

> CREATE SINK sink_mv_null
  IN CLUSTER quickstart
  FROM mv_regions
  INTO SOLACE CONNECTION solace_sink_conn (
    TOPIC 'mz/sink-test/mv-null/{region}'
  )

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=3s

# Two non-NULL regions publish; the NULL-region row is dropped.
> INSERT INTO mv_null_src VALUES (1, 'north'), (2, NULL), (3, 'south')

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=3s
