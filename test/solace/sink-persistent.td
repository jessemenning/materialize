# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# End-to-end test for the Solace sink in persistent (guaranteed) delivery
# mode. The workflow has provisioned a verification queue
# (mz_sink_persistent_q) subscribed to 'mz/sink-test/persistent'. This script
# creates a sink with DELIVERY MODE 'persistent', inserts five rows, and
# sleeps long enough for the sink to publish and confirm them. The workflow
# then consumes from the queue and verifies all five arrived. Because the sink
# gates its frontier on broker acks, mz_frontiers advancing past 0 also
# confirms the acks were received.

> DROP SINK IF EXISTS sink_persistent
> DROP TABLE IF EXISTS sink_persistent_events
> DROP CONNECTION IF EXISTS solace_sink_conn CASCADE
> DROP SECRET IF EXISTS solace_sink_pw

> CREATE SECRET solace_sink_pw AS 'changeme'

> CREATE CONNECTION solace_sink_conn TO SOLACE (
    HOST 'tcp://solace:55555',
    MESSAGE VPN 'default',
    USERNAME 'mz_user',
    PASSWORD SECRET solace_sink_pw
  )

> CREATE TABLE sink_persistent_events (id INT, label TEXT)

> CREATE SINK sink_persistent
  IN CLUSTER quickstart
  FROM sink_persistent_events
  INTO SOLACE CONNECTION solace_sink_conn (
    TOPIC 'mz/sink-test/persistent',
    DELIVERY MODE 'persistent'
  )

# Allow the sink operator to connect and enter its processing loop before
# we insert rows.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=2s

> INSERT INTO sink_persistent_events VALUES (1, 'hello'), (2, 'world'), (3, 'foo'), (4, 'bar'), (5, 'baz')

# Wait for the sink to publish all five rows and receive broker acks.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=3s

# The write frontier only advances once the broker has acked the published
# data, so a frontier past 0 is evidence of confirmed delivery. This is a
# retrying assertion: it polls until the frontier moves.
> SELECT sinks.name
  FROM mz_internal.mz_frontiers frontiers
  JOIN mz_sinks sinks ON frontiers.object_id = sinks.id
  WHERE sinks.name = 'sink_persistent' AND frontiers.write_frontier > 0
sink_persistent
