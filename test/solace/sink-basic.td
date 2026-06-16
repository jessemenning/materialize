# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Basic round-trip test for the Solace sink. The mzcompose workflow has
# already provisioned the broker and a verification queue (mz_sink_basic_q)
# subscribed to 'mz/sink-test/basic'. This script creates a sink writing to
# that topic, inserts five rows, and sleeps long enough for the sink to
# publish them. The workflow then uses stm to consume and verify.

> DROP SINK IF EXISTS sink_basic
> DROP TABLE IF EXISTS sink_basic_events
> DROP CONNECTION IF EXISTS solace_sink_conn CASCADE
> DROP SECRET IF EXISTS solace_sink_pw

> CREATE SECRET solace_sink_pw AS 'changeme'

> CREATE CONNECTION solace_sink_conn TO SOLACE (
    HOST 'tcp://solace:55555',
    MESSAGE VPN 'default',
    USERNAME 'mz_user',
    PASSWORD SECRET solace_sink_pw
  )

> CREATE TABLE sink_basic_events (id INT, label TEXT)

> CREATE SINK sink_basic
  IN CLUSTER quickstart
  FROM sink_basic_events
  INTO SOLACE CONNECTION solace_sink_conn (
    TOPIC 'mz/sink-test/basic'
  )

# Allow the sink operator to connect and enter its processing loop before
# we insert rows. Without this pause the first batch of inserts can race
# the session-open path.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=2s

> INSERT INTO sink_basic_events VALUES (1, 'hello'), (2, 'world'), (3, 'foo'), (4, 'bar'), (5, 'baz')

# Wait for the sink to pick up and publish all five rows.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=3s
