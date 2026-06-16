# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Crash-restart dedup test, part 2: after materialized has been killed and
# restarted, verify that
#
# * the five `pre-*` rows from the first batch survived the restart (persist
#   durability);
# * the broker's redelivery of any not-yet-acked `pre-*` messages is caught
#   by the dedup-on-restart path and silently ack'd + dropped, NOT
#   re-ingested as duplicates;
# * a fresh batch of five `post-*` messages is correctly picked up by the
#   resumed source.

# First, verify the five `pre-*` rows are still durably present after the
# restart. (`solace_src` and its connection/secret survive because they live
# in the catalog, which is durable.)
> SELECT convert_from(data, 'UTF8') FROM solace_src ORDER BY 1
pre-1
pre-2
pre-3
pre-4
pre-5

> SELECT count(DISTINCT rgmid) FROM solace_src
5

# Publish a fresh batch and wait for them.

$ http-request method=POST url=http://mz_user:changeme@solace:9000/QUEUE/mz_test_q content-type=application/octet-stream
post-1

$ http-request method=POST url=http://mz_user:changeme@solace:9000/QUEUE/mz_test_q content-type=application/octet-stream
post-2

$ http-request method=POST url=http://mz_user:changeme@solace:9000/QUEUE/mz_test_q content-type=application/octet-stream
post-3

$ http-request method=POST url=http://mz_user:changeme@solace:9000/QUEUE/mz_test_q content-type=application/octet-stream
post-4

$ http-request method=POST url=http://mz_user:changeme@solace:9000/QUEUE/mz_test_q content-type=application/octet-stream
post-5

# Final result: every pre- and post- message exactly once. If dedup-on-restart
# missed a `pre-*` redelivery this would be > 10, and if persist lost any of
# the pre-* batch it would be < 10.
> SELECT count(*) FROM solace_src
10

> SELECT count(DISTINCT rgmid) FROM solace_src
10

> SELECT convert_from(data, 'UTF8') FROM solace_src ORDER BY 1
post-1
post-2
post-3
post-4
post-5
pre-1
pre-2
pre-3
pre-4
pre-5
