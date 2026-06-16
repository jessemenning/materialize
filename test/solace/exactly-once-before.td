# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Crash-restart dedup test, part 1: set up the source, publish a first
# batch of messages, and wait for them to land in the source before the
# workflow kills materialized.

# Drop any objects left over from a prior run (e.g. round-trip) so this
# file is idempotent when executed after another workflow.
> DROP SOURCE IF EXISTS solace_src CASCADE
> DROP CONNECTION IF EXISTS solace_conn CASCADE
> DROP SECRET IF EXISTS solace_pw CASCADE

> CREATE SECRET solace_pw AS 'changeme';

> CREATE CONNECTION solace_conn TO SOLACE (
    HOST 'tcp://solace:55555',
    MESSAGE VPN 'default',
    USERNAME 'mz_user',
    PASSWORD SECRET solace_pw
  );

> CREATE SOURCE solace_src
  IN CLUSTER quickstart
  FROM SOLACE CONNECTION solace_conn (QUEUE 'mz_test_q')
  FORMAT BYTES
  INCLUDE REPLICATION GROUP MESSAGE ID AS rgmid;

# First batch — five messages tagged with the `pre-` prefix so we can tell
# them apart from the after-restart batch.

$ http-request method=POST url=http://mz_user:changeme@solace:9000/QUEUE/mz_test_q content-type=application/octet-stream
pre-1

$ http-request method=POST url=http://mz_user:changeme@solace:9000/QUEUE/mz_test_q content-type=application/octet-stream
pre-2

$ http-request method=POST url=http://mz_user:changeme@solace:9000/QUEUE/mz_test_q content-type=application/octet-stream
pre-3

$ http-request method=POST url=http://mz_user:changeme@solace:9000/QUEUE/mz_test_q content-type=application/octet-stream
pre-4

$ http-request method=POST url=http://mz_user:changeme@solace:9000/QUEUE/mz_test_q content-type=application/octet-stream
pre-5

# Wait for all five to land before the workflow kills materialized. Once
# this SELECT succeeds, every `pre-*` row is durable in persist and the
# next-frontier-advance will trigger broker acks.
> SELECT count(*) FROM solace_src
5

# All five RGMIDs are distinct (sanity check before the restart).
> SELECT count(DISTINCT rgmid) FROM solace_src
5
