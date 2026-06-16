# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reconnect test, part 1 of 3: create the sink and publish two rows before
# the broker is killed. The mzcompose workflow verifies these two messages
# arrive before killing Solace.
#
# Run with --no-reset so the table and sink survive across the three phases.

> DROP SINK IF EXISTS sink_reconnect
> DROP TABLE IF EXISTS sink_reconnect_events
> DROP CONNECTION IF EXISTS solace_sink_conn CASCADE
> DROP SECRET IF EXISTS solace_sink_pw

> CREATE SECRET solace_sink_pw AS 'changeme'

> CREATE CONNECTION solace_sink_conn TO SOLACE (
    HOST 'tcp://solace:55555',
    MESSAGE VPN 'default',
    USERNAME 'mz_user',
    PASSWORD SECRET solace_sink_pw
  )

> CREATE TABLE sink_reconnect_events (id INT, phase TEXT)

> CREATE SINK sink_reconnect
  IN CLUSTER quickstart
  FROM sink_reconnect_events
  INTO SOLACE CONNECTION solace_sink_conn (
    TOPIC 'mz/sink-test/reconnect'
  )

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=2s

> INSERT INTO sink_reconnect_events VALUES (1, 'pre-crash'), (2, 'pre-crash')

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=3s
