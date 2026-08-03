# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Dedup-window test for the Solace sink. The sink is created with a 3 s
# DEDUP WINDOW. All rows render to the same topic ('mz/sink-test/dedup').
#
# DEDUP WINDOW conflates rather than drops: the first row publishes
# immediately, rows arriving within the window are suppressed but the latest
# suppressed payload is buffered and flushed once the window elapses, even if
# no further rows arrive. So three values arriving over time within one window
# yield exactly two messages: the first value, then the latest value (the
# middle one is conflated away). The mzcompose workflow asserts both the count
# and that the second message carries the latest value, proving conflation
# keeps the newest payload and that a quiet stream still flushes.
#
# The three values are inserted in separate statements with gaps rather than a
# single multi-row INSERT. Rows in one INSERT land at a single timestamp and
# are logically concurrent, so "latest" among them is not well defined. The
# realistic case, and the one this exercises, is updates arriving over time.
#
# The verification queue (mz_dedup_q) subscribed to 'mz/sink-test/dedup'
# is provisioned by the mzcompose workflow before this script runs.

> DROP SINK IF EXISTS sink_dedup
> DROP TABLE IF EXISTS sink_dedup_events
> DROP CONNECTION IF EXISTS solace_sink_conn CASCADE
> DROP SECRET IF EXISTS solace_sink_pw

> CREATE SECRET solace_sink_pw AS 'changeme'

> CREATE CONNECTION solace_sink_conn TO SOLACE (
    HOST 'tcp://solace:55555',
    MESSAGE VPN 'default',
    USERNAME 'mz_user',
    PASSWORD SECRET solace_sink_pw
  )

# Single column used as the dedup key: every row maps to the same rendered
# topic ('mz/sink-test/dedup'), so successive inserts within the window are
# suppressed and conflated.
> CREATE TABLE sink_dedup_events (key TEXT, val INT)

> CREATE SINK sink_dedup
  IN CLUSTER quickstart
  FROM sink_dedup_events
  INTO SOLACE CONNECTION solace_sink_conn (
    TOPIC 'mz/sink-test/dedup',
    DEDUP WINDOW '3s'
  )

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=2s

# val=1 is the first row for this topic, so it publishes immediately and opens
# the 3 s window.
> INSERT INTO sink_dedup_events VALUES ('x', 1)

# val=2 arrives ~0.8 s later, still inside the window, so it is suppressed and
# buffered as the pending latest payload.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=800ms
> INSERT INTO sink_dedup_events VALUES ('x', 2)

# val=3 arrives ~0.8 s after that, still inside the window, so it conflates over
# val=2 in the pending buffer.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=800ms
> INSERT INTO sink_dedup_events VALUES ('x', 3)

# Wait for the dedup window to elapse. No further rows arrive, so this also
# exercises the timer-driven flush of the buffered latest payload (val=3).
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=4s
