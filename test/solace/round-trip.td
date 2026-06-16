# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# End-to-end happy path for the Solace source connector. The workflow
# (mzcompose.py) has already provisioned a client-username `mz_user` and a
# durable queue `mz_test_q` on the broker via SEMP; this script publishes
# five messages via the REST messaging endpoint and verifies that all five
# round-trip through `CREATE SOURCE ... FROM SOLACE`.

# ----------------------------------------------------------------------------
# Connection + source setup.
# ----------------------------------------------------------------------------

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
  INCLUDE
    REPLICATION GROUP MESSAGE ID AS rgmid,
    BROKER TIMESTAMP AS broker_ts;

# ----------------------------------------------------------------------------
# Publish five messages via Solace REST messaging.
#
# `accept-additional-status-codes=200` is required because Solace's REST
# messaging endpoint returns 200 (rather than the 2xx range curl considers
# success by default) on a successfully published guaranteed message.
# ----------------------------------------------------------------------------

$ http-request method=POST url=http://mz_user:changeme@solace:9000/QUEUE/mz_test_q content-type=application/octet-stream
hello-1

$ http-request method=POST url=http://mz_user:changeme@solace:9000/QUEUE/mz_test_q content-type=application/octet-stream
hello-2

$ http-request method=POST url=http://mz_user:changeme@solace:9000/QUEUE/mz_test_q content-type=application/octet-stream
hello-3

$ http-request method=POST url=http://mz_user:changeme@solace:9000/QUEUE/mz_test_q content-type=application/octet-stream
hello-4

$ http-request method=POST url=http://mz_user:changeme@solace:9000/QUEUE/mz_test_q content-type=application/octet-stream
hello-5

# ----------------------------------------------------------------------------
# Verify all five messages flowed through.
# ----------------------------------------------------------------------------

> SELECT convert_from(data, 'UTF8') FROM solace_src ORDER BY 1
hello-1
hello-2
hello-3
hello-4
hello-5

# Every message has a non-null 16-byte RGMID...
> SELECT count(*) FROM solace_src WHERE rgmid IS NOT NULL AND octet_length(rgmid) = 16
5

# ...and a non-null broker-assigned timestamp.
> SELECT count(*) FROM solace_src WHERE broker_ts IS NOT NULL
5

# The five RGMIDs are all distinct.
> SELECT count(DISTINCT rgmid) FROM solace_src
5
