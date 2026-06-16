# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Dynamic topic routing test for the Solace sink. The TOPIC template contains
# a '{region}' placeholder that is substituted per-row. A NULL region causes
# the entire row to be silently dropped (not published). The mzcompose
# workflow verifies:
#   - exactly 4 of 5 inserts produce a message (the NULL row is dropped)
#   - each message lands on the correct per-region topic
#
# The verification queue (mz_dynamic_q) with wildcard subscription
# 'mz/sink-test/>' is provisioned by the mzcompose workflow.

> DROP SINK IF EXISTS sink_dynamic
> DROP TABLE IF EXISTS sink_dynamic_events
> DROP CONNECTION IF EXISTS solace_sink_conn CASCADE
> DROP SECRET IF EXISTS solace_sink_pw

> CREATE SECRET solace_sink_pw AS 'changeme'

> CREATE CONNECTION solace_sink_conn TO SOLACE (
    HOST 'tcp://solace:55555',
    MESSAGE VPN 'default',
    USERNAME 'mz_user',
    PASSWORD SECRET solace_sink_pw
  )

> CREATE TABLE sink_dynamic_events (id INT, region TEXT, count INT)

> CREATE SINK sink_dynamic
  IN CLUSTER quickstart
  FROM sink_dynamic_events
  INTO SOLACE CONNECTION solace_sink_conn (
    TOPIC 'mz/sink-test/{region}'
  )

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

# 2 rows → mz/sink-test/us-east
# 1 row  → mz/sink-test/us-west
# 1 row  → mz/sink-test/eu-west
# 1 row  → NULL region → silently dropped (no message published)
> INSERT INTO sink_dynamic_events VALUES
    (1, 'us-east', 100),
    (2, 'us-east', 200),
    (3, 'us-west', 150),
    (4, 'eu-west', 75),
    (5, NULL, 999)

# Allow the sink to publish all four non-null rows.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=3s
