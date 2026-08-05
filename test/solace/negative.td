# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Planning-time rejections for invalid Solace DDL. These all fail before any
# broker I/O, so no queue or published messages are needed.

> CREATE SECRET solace_pw AS 'changeme'

> CREATE CONNECTION solace_conn TO SOLACE (
    HOST 'tcp://solace:55555',
    MESSAGE VPN 'default',
    USERNAME 'mz_user',
    PASSWORD SECRET solace_pw
  )

# ----------------------------------------------------------------------------
# Source option validation.
# ----------------------------------------------------------------------------

# QUEUE and DURABLE TOPIC ENDPOINT are mutually exclusive.
! CREATE SOURCE bad IN CLUSTER quickstart
  FROM SOLACE CONNECTION solace_conn (QUEUE 'q', DURABLE TOPIC ENDPOINT 'dte', TOPIC SUBSCRIPTION 'a/b')
  FORMAT BYTES
contains:QUEUE and DURABLE TOPIC ENDPOINT are mutually exclusive

# Neither QUEUE nor DTE.
! CREATE SOURCE bad IN CLUSTER quickstart
  FROM SOLACE CONNECTION solace_conn
  FORMAT BYTES
contains:Solace source requires exactly one of QUEUE or DURABLE TOPIC ENDPOINT

# DURABLE TOPIC ENDPOINT is not implemented in the runtime.
! CREATE SOURCE bad IN CLUSTER quickstart
  FROM SOLACE CONNECTION solace_conn (DURABLE TOPIC ENDPOINT 'dte', TOPIC SUBSCRIPTION 'a/b')
  FORMAT BYTES
contains:DURABLE TOPIC ENDPOINT is not yet supported

# ACK WINDOW SIZE must be positive.
! CREATE SOURCE bad IN CLUSTER quickstart
  FROM SOLACE CONNECTION solace_conn (QUEUE 'q', ACK WINDOW SIZE 0)
  FORMAT BYTES
contains:ACK WINDOW SIZE must be greater than zero

# DEDUPLICATE = false is not supported.
! CREATE SOURCE bad IN CLUSTER quickstart
  FROM SOLACE CONNECTION solace_conn (QUEUE 'q', DEDUPLICATE false)
  FORMAT BYTES
contains:DEDUPLICATE = false is not yet supported

# PARALLELISM > 1 is not supported.
! CREATE SOURCE bad IN CLUSTER quickstart
  FROM SOLACE CONNECTION solace_conn (QUEUE 'q', PARALLELISM 4)
  FORMAT BYTES
contains:PARALLELISM greater than 1 is not yet supported

# ACK MODE must be AUTO or CLIENT.
! CREATE SOURCE bad IN CLUSTER quickstart
  FROM SOLACE CONNECTION solace_conn (QUEUE 'q', ACK MODE 'bogus')
  FORMAT BYTES
contains:ACK MODE must be AUTO or CLIENT

# INCLUDE KEY is not valid on Solace sources.
! CREATE SOURCE bad IN CLUSTER quickstart
  FROM SOLACE CONNECTION solace_conn (QUEUE 'q')
  FORMAT BYTES
  INCLUDE KEY
contains:INCLUDE KEY is not supported for Solace sources

# ----------------------------------------------------------------------------
# ENVELOPE UPSERT (KEY (...)) requires the key columns to be INCLUDE metadata.
# ----------------------------------------------------------------------------

! CREATE SOURCE bad IN CLUSTER quickstart
  FROM SOLACE CONNECTION solace_conn (QUEUE 'q')
  FORMAT BYTES
  INCLUDE TOPIC LEVELS (COUNT = 2) AS lvl
  ENVELOPE UPSERT (KEY (nonexistent))
contains:key column "nonexistent" not found in source metadata

# ----------------------------------------------------------------------------
# Sink option validation.
# ----------------------------------------------------------------------------

> CREATE TABLE t (id int, val text)

# Missing TOPIC.
! CREATE SINK bad IN CLUSTER quickstart
  FROM t INTO SOLACE CONNECTION solace_conn
contains:Solace sink must specify TOPIC

# Unknown column in the topic template.
! CREATE SINK bad IN CLUSTER quickstart
  FROM t INTO SOLACE CONNECTION solace_conn (TOPIC 'events/{nope}')
contains:column "nope" referenced in TOPIC template does not exist

# FORMAT is not supported on Solace sinks (always JSON).
! CREATE SINK bad IN CLUSTER quickstart
  FROM t INTO SOLACE CONNECTION solace_conn (TOPIC 'events') FORMAT JSON
contains:FORMAT is not supported for Solace sinks

# ENVELOPE is not supported on Solace sinks.
! CREATE SINK bad IN CLUSTER quickstart
  FROM t INTO SOLACE CONNECTION solace_conn (TOPIC 'events') ENVELOPE UPSERT
contains:ENVELOPE is not supported for Solace sinks
