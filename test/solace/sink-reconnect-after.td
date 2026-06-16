# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reconnect test, part 3 of 3: after the broker has been restarted and
# re-provisioned, insert two more rows. The Solace sink reconnects
# automatically (reconnect_retries=-1, 1 s between attempts); once the
# session is re-established these rows are published normally. The
# mzcompose workflow verifies exactly 2 messages arrive post-restart,
# confirming that reconnect works and the during-outage rows were not
# re-delivered (fire-and-forget semantics).
#
# Run with --no-reset to reuse the table and sink from parts 1 and 2.

> INSERT INTO sink_reconnect_events VALUES (10, 'post-restart'), (11, 'post-restart')

# Allow extra time: the sink may still be completing its reconnect handshake
# when we insert, so give it more headroom than the other tests.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s
