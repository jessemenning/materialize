# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reconnect test, part 2 of 3: insert rows while the broker is down. The
# sink uses DeliveryMode::Direct (fire-and-forget), so publish calls fail
# silently — these rows are NOT expected to appear in Solace. The test
# verifies this absence by asserting the final message count is exactly the
# pre- and post-crash rows combined.
#
# Run with --no-reset to reuse the table and sink from part 1.

> INSERT INTO sink_reconnect_events VALUES (3, 'during-outage'), (4, 'during-outage')

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=1s
