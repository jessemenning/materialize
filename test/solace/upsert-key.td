# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Metadata-keyed upsert for the Solace source: ENVELOPE UPSERT (KEY (...))
# where the key is assembled from INCLUDE metadata columns rather than a
# native message key. Solace FORMAT sources have no key field, so the key
# comes from the topic. Here the third topic level is the upsert key.
#
# The workflow (mzcompose.py) has provisioned:
#   mz_upsert_q       subscribed to mz/upsert/>
#   mz_upsert_null_q  subscribed to mznull/>
# and the client-username mz_user via provision_broker.

> CREATE SECRET solace_pw AS 'changeme';

> CREATE CONNECTION solace_conn TO SOLACE (
    HOST 'tcp://solace:55555',
    MESSAGE VPN 'default',
    USERNAME 'mz_user',
    PASSWORD SECRET solace_pw
  );

# ----------------------------------------------------------------------------
# Happy path: key on the third topic level (tl_3).
# ----------------------------------------------------------------------------

> CREATE SOURCE upsert_src
  IN CLUSTER quickstart
  FROM SOLACE CONNECTION solace_conn (QUEUE 'mz_upsert_q')
  FORMAT BYTES
  INCLUDE TOPIC LEVELS AS tl (COUNT = 3)
  ENVELOPE UPSERT (KEY (tl_3));

# Publish to topics mz/upsert/<key>. The queue's mz/upsert/> subscription
# fans these into mz_upsert_q, and the source keys upsert on tl_3 = <key>.
# `accept-additional-status-codes=200` mirrors round-trip.td: Solace REST
# returns 200 on a successful guaranteed publish.

$ http-request method=POST url=http://mz_user:changeme@solace:9000/TOPIC/mz/upsert/alpha content-type=application/octet-stream
v1

$ http-request method=POST url=http://mz_user:changeme@solace:9000/TOPIC/mz/upsert/beta content-type=application/octet-stream
v1

# Second write to the same key (alpha) must replace the first, not accumulate.
$ http-request method=POST url=http://mz_user:changeme@solace:9000/TOPIC/mz/upsert/alpha content-type=application/octet-stream
v2

# Latest-write-per-key: alpha collapses to v2, beta stays v1. RGMIDs increase
# with spool order within the queue, so the later alpha write wins.
> SELECT tl_3, convert_from(data, 'UTF8') FROM upsert_src ORDER BY tl_3
alpha v2
beta v1

# Exactly two keys survive.
> SELECT count(*) FROM upsert_src
2

# The key column is populated from the topic, and the earlier value columns
# (tl_1, tl_2) reflect the fixed prefix.
> SELECT tl_1, tl_2 FROM upsert_src WHERE tl_3 = 'alpha'
mz upsert

# ----------------------------------------------------------------------------
# Null key: a message whose key level is absent surfaces as an upsert error
# rather than silently keying on NULL. Isolated on its own source so the
# error does not poison the happy-path assertions above.
# ----------------------------------------------------------------------------

> CREATE SOURCE upsert_null_src
  IN CLUSTER quickstart
  FROM SOLACE CONNECTION solace_conn (QUEUE 'mz_upsert_null_q')
  FORMAT BYTES
  INCLUDE TOPIC LEVELS AS tl (COUNT = 3)
  ENVELOPE UPSERT (KEY (tl_3));

# Only two topic levels, so tl_3 is NULL -> the key is null.
$ http-request method=POST url=http://mz_user:changeme@solace:9000/TOPIC/mznull/x content-type=application/octet-stream
orphan

! SELECT * FROM upsert_null_src
contains:record with NULL key
