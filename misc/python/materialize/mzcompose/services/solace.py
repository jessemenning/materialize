# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""A Solace Platform event broker (single-node Standard edition).

Used by ``test/solace/`` to exercise the Solace source connector end-to-end
against a real broker.

The image is heavy by Materialize-test standards (it boots in ~30s, needs ~1G
of shared memory, and reserves ~512M of RAM at idle) but it is the only way
to validate the runtime's exactly-once protocol against an actual broker.
"""

from materialize.mzcompose.service import Service, ServiceHealthcheck


class Solace(Service):
    """A single-node Solace Platform broker (Standard edition).

    The default configuration exposes:
    * Port ``55555`` — SMF, the plain-text wire protocol Materialize's source
      connects to.
    * Port ``8080`` — the SEMP REST API, used by the workflow to provision
      queues and client-usernames before the test runs.
    * Port ``9000`` — the REST messaging endpoint, used by testdrive's
      ``$ http-request`` directive to publish test messages.

    Admin credentials are ``admin``/``admin`` (set via the broker's standard
    image envvars); the test client-username is created on demand via SEMP.
    """

    DEFAULT_SOLACE_TAG = "10.10"

    def __init__(
        self,
        name: str = "solace",
        image: str | None = None,
        healthcheck: ServiceHealthcheck | None = None,
    ) -> None:
        if image is None:
            image = f"solace/solace-pubsub-standard:{Solace.DEFAULT_SOLACE_TAG}"

        if healthcheck is None:
            # The broker exposes SEMP on 8080 once the message-spool subsystem
            # is up; that's the right moment to start provisioning queues and
            # opening SMF sessions. start_period is 300s: the broker takes
            # 25-40s on a laptop but 3-5 minutes on CI runners (cold image
            # pull, different kernel, no hugepages). After start_period the
            # 30x2s retries add 60s more, for a 6-minute ceiling.
            #
            # Endpoint: /SEMP/v2/config/about/api returns 200 once the SEMP
            # subsystem is ready. The top-level __about path returns 400 "API
            # not supported" in Solace 10.x; /msgVpns works too but /about/api
            # is lighter (no VPN enumeration).
            healthcheck = {
                "test": [
                    "CMD-SHELL",
                    "curl -sf -u admin:admin http://localhost:8080/SEMP/v2/config/about/api >/dev/null",
                ],
                "interval": "2s",
                "timeout": "5s",
                "retries": 30,
                "start_period": "300s",
            }

        super().__init__(
            name=name,
            config={
                "image": image,
                "networks": {"default": {"aliases": [name]}},
                "ports": [55555, 8080, 9000],
                # Solace requires shared memory for the message spool.
                "shm_size": "1g",
                "ulimits": {
                    "core": 1,
                    # Solace's internal consul process opens many file
                    # descriptors (gossip sockets, raft log, RPC). A low hard
                    # limit (e.g. 6592) causes consul to crash silently on
                    # startup. Match the value used in production deployments.
                    "nofile": {"soft": 1048576, "hard": 1048576},
                },
                "environment": [
                    "username_admin_globalaccesslevel=admin",
                    "username_admin_password=admin",
                    # Trim the connection count from the default 1000 to keep
                    # memory usage tractable in test environments.
                    "system_scaling_maxconnectioncount=100",
                ],
                "healthcheck": healthcheck,
            },
        )
