# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Dedup-window test for the Solace sink. The sink is created with a 3 s
# DEDUP WINDOW. Three rows inserted rapidly all render to the same topic
# ('mz/sink-test/dedup'), so only the first is published within the window.
# After the window expires a fourth insert produces a second publish. The
# mzcompose workflow verifies exactly 2 messages arrive in Solace.
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
# suppressed.
> CREATE TABLE sink_dedup_events (key TEXT, val INT)

> CREATE SINK sink_dedup
  IN CLUSTER quickstart
  FROM sink_dedup_events
  INTO SOLACE CONNECTION solace_sink_conn (
    TOPIC 'mz/sink-test/dedup',
    DEDUP WINDOW '3s'
  )

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=2s

# Three inserts in rapid succession — all within the 3 s dedup window.
# Only the first triggers a publish; the next two are suppressed.
> INSERT INTO sink_dedup_events VALUES ('x', 1), ('x', 2), ('x', 3)

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=500ms

# Wait for the dedup window to expire (3 s window + headroom).
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=4s

# This insert arrives after the window; a second publish should go out.
> INSERT INTO sink_dedup_events VALUES ('x', 4)

# Give the sink time to process and publish the post-window row.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=2s
