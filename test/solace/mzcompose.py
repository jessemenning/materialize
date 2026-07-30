# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
End-to-end test for the Solace Platform source connector.

Brings up a single-node Solace broker, provisions a queue + a client-username
via SEMP, and runs testdrive scripts that publish messages via the broker's
REST messaging endpoint and verify they round-trip through a Materialize
``CREATE SOURCE ... FROM SOLACE``.

Use ``--workflow=round-trip`` (the default) for the basic happy-path test,
or ``--workflow=exactly-once`` for the crash-restart dedup test.

Host prerequisite
-----------------
The Solace broker maps many memory regions at startup. Linux's default
``vm.max_map_count`` of 65530 is far too low; the broker's internal consul
process crashes with "Unable to raise event; rc(would block)" without:

    sudo sysctl -w vm.max_map_count=512000

On WSL2 this must be set inside the WSL2 kernel (not just the Docker VM)::

    # /etc/sysctl.d/99-solace.conf
    vm.max_map_count=512000

The workflows below attempt to apply this automatically via a privileged
``busybox`` container, but that requires the Docker daemon to allow
``--privileged``. If the attempt fails a warning is printed and you must set
it manually.
"""

import base64
import json
import re
import statistics
import subprocess
import threading
import time
import urllib.error
import urllib.request

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.solace import Solace
from materialize.mzcompose.services.testdrive import Testdrive


class Stm(Service):
    """solace-tryme-cli (stm) — Solace Platform test CLI.

    Installed from npm (@solace-community/stm) at container start. Use
    ``c.exec("stm", "stm", "receive", ...)`` to consume messages from a
    Solace queue via WebSocket SMF (``ws://solace:8008``).
    """

    def __init__(self) -> None:
        super().__init__(
            name="stm",
            config={
                "image": "node:20-alpine",
                "command": [
                    "/bin/sh",
                    "-c",
                    "npm install -g @solace-community/stm && touch /tmp/stm-ready && tail -f /dev/null",
                ],
                "healthcheck": {
                    "test": ["CMD", "test", "-f", "/tmp/stm-ready"],
                    "interval": "2s",
                    "timeout": "5s",
                    "retries": 60,
                    "start_period": "120s",
                },
                "networks": {"default": {"aliases": ["stm"]}},
            },
        )


SERVICES = [
    Solace(),
    Materialized(sanity_restart=False),
    Testdrive(),
    Stm(),
]


# Broker bootstrap performed once per workflow run, after the broker reports
# healthy on SEMP. Idempotent — re-running over a still-warm broker just
# observes 409s from the conflicting POSTs.
TEST_CLIENT_USERNAME = "mz_user"
TEST_CLIENT_PASSWORD = "changeme"
TEST_QUEUE = "mz_test_q"


def ensure_vm_max_map_count(c: Composition) -> None:
    """Raise vm.max_map_count to 512000 if it is below that threshold.

    Solace's internal consul process crashes on startup when this value is too
    low (Linux default: 65530). We try two approaches in order:

    1. A privileged busybox container — works in most CI environments.
    2. A direct ``sysctl`` call on the host — works in local dev when Docker
       Desktop / WSL2 shares the host kernel.

    Both are best-effort: if neither succeeds we print a warning with manual
    instructions rather than aborting the workflow.
    """
    REQUIRED = 512000
    try:
        current = int(
            subprocess.check_output(
                ["sysctl", "-n", "vm.max_map_count"], text=True
            ).strip()
        )
        if current >= REQUIRED:
            return
    except Exception:
        pass

    # Try via a privileged container first (works in most CI setups).
    try:
        c.exec(
            "busybox",
            "sysctl",
            "-w",
            f"vm.max_map_count={REQUIRED}",
            check=False,
        )
    except Exception:
        pass

    # Try directly on the host (works when Docker uses the host kernel).
    try:
        subprocess.run(
            ["sudo", "sysctl", "-w", f"vm.max_map_count={REQUIRED}"],
            check=True,
            capture_output=True,
        )
        return
    except Exception:
        pass

    print(
        f"\nWARNING: could not set vm.max_map_count={REQUIRED}. "
        "The Solace broker may crash on startup. "
        "Fix manually with:\n"
        f"  sudo sysctl -w vm.max_map_count={REQUIRED}\n"
        "On WSL2, also add to /etc/sysctl.d/99-solace.conf and run "
        "'sudo sysctl --system'.\n"
    )


def provision_broker(c: Composition) -> None:
    """Create the test client-username and queue via SEMP REST.

    Calls the SEMP API from the mzcompose host process using the mapped port,
    so there is no dependency on the testdrive container's exec behaviour.

    HTTP 200 (created), 400 (duplicate/already-exists), and 409 (conflict) are
    all treated as success: re-running against a warm broker is normal dev
    iteration.
    """
    semp_port = c.port("solace", 8080)
    semp_root = f"http://127.0.0.1:{semp_port}/SEMP/v2/config/msgVpns/default"
    credentials = base64.b64encode(b"admin:admin").decode()

    def _post(path: str, body: dict) -> None:
        # Only a genuine ALREADY_EXISTS is benign. SEMP also returns HTTP 400
        # while the message VPN is warming up after broker start; skipping
        # those silently leaves the queue uncreated and the test ingests
        # nothing, so retry with backoff instead.
        data = json.dumps(body).encode()
        last_err = "unknown"
        for attempt in range(20):
            req = urllib.request.Request(
                f"{semp_root}{path}",
                data=data,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Basic {credentials}",
                },
            )
            try:
                with urllib.request.urlopen(req) as resp:
                    print(f"SEMP POST {path}: {resp.status}")
                    return
            except urllib.error.HTTPError as e:
                body_text = e.read().decode(errors="replace")[:300]
                if "ALREADY_EXISTS" in body_text or e.code == 409:
                    print(f"SEMP POST {path}: {e.code} (already exists, continuing)")
                    return
                last_err = f"{e.code} — {body_text}"
            except urllib.error.URLError as e:
                last_err = str(e)
            print(f"SEMP POST {path}: not ready ({last_err}), retry {attempt + 1}/20")
            time.sleep(3)
        raise RuntimeError(f"SEMP POST {path} failed after retries: {last_err}")

    _post(
        "/clientUsernames",
        {
            "clientUsername": TEST_CLIENT_USERNAME,
            "password": TEST_CLIENT_PASSWORD,
            "enabled": True,
            "aclProfileName": "default",
            "clientProfileName": "default",
        },
    )
    _post(
        "/queues",
        {
            "queueName": TEST_QUEUE,
            "accessType": "exclusive",
            "permission": "consume",
            "ingressEnabled": True,
            "egressEnabled": True,
        },
    )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run the round-trip happy-path test by default."""
    workflow_round_trip(c, parser)


# ---------------------------------------------------------------------------
# Sink helper functions
# ---------------------------------------------------------------------------


def provision_sink_queues(c: Composition, queues: list[tuple[str, str]]) -> None:
    """Create durable queues with topic subscriptions for sink verification.

    Each element of ``queues`` is a ``(queue_name, topic_subscription)`` pair.
    A queue's topic subscription causes any message published to a matching
    Solace topic to be fan-out-delivered to that queue, allowing stm to
    consume and verify sink output.

    Calls are idempotent: 400/409 responses (already-exists) are silently
    accepted so the function can be called repeatedly against a warm broker.
    """
    semp_port = c.port("solace", 8080)
    semp_root = f"http://127.0.0.1:{semp_port}/SEMP/v2/config/msgVpns/default"
    credentials = base64.b64encode(b"admin:admin").decode()

    def _post(path: str, body: dict) -> None:
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            f"{semp_root}{path}",
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Basic {credentials}",
            },
        )
        try:
            with urllib.request.urlopen(req) as resp:
                print(f"SEMP POST {path}: {resp.status}")
        except urllib.error.HTTPError as e:
            if e.code in (400, 409):
                print(f"SEMP POST {path}: {e.code} (already exists, continuing)")
            else:
                raise

    for queue_name, topic_sub in queues:
        _post(
            "/queues",
            {
                "queueName": queue_name,
                "accessType": "exclusive",
                "permission": "consume",
                "ingressEnabled": True,
                "egressEnabled": True,
            },
        )
        _post(
            f"/queues/{queue_name}/subscriptions",
            {"subscriptionTopic": topic_sub},
        )


def consume_from_queue(
    c: Composition,
    queue: str,
    exit_after: int = 15,
) -> list[dict]:
    """Consume messages from a Solace queue using stm and return parsed results.

    ``exit_after`` is the number of seconds stm waits before exiting. Because
    messages are queued by the broker before stm runs (the testdrive script
    sleeps long enough for the sink to publish first), stm receives all
    buffered messages almost immediately and then idles until ``exit_after``
    expires.

    Returns a list of dicts with keys ``"topic"`` (str) and ``"payload"``
    (str — raw JSON emitted by the sink).
    """
    result = c.exec(
        "stm",
        "stm",
        "receive",
        "--url",
        "ws://solace:8008",
        "--vpn",
        "default",
        "--username",
        "mz_user",
        "--password",
        "changeme",
        "--queue",
        queue,
        "--output-mode",
        "FULL",
        "--log-level",
        "ERROR",
        "--exit-after",
        str(exit_after),
        capture=True,
        check=False,
    )
    return _parse_stm_output(result.stdout)


def _parse_stm_output(output: str) -> list[dict]:
    """Parse stm --output-mode FULL stdout into a list of message dicts.

    stm v0.x FULL mode emits a block per received message. Each block starts
    with a line containing "message Received -" and ends at the next such line
    (or EOF). Within each block the destination topic is on a "Destination:"
    line formatted as "[Topic <name>]", and the payload follows the
    "message: Payload" line as a JavaScript-style object (stm prints JSON
    keys without quotes).

    Each returned dict has:
        "topic"   – the Solace destination topic string
        "payload" – the message payload as a valid JSON string
    """
    messages: list[dict] = []

    # Split the output into one block per message. The marker line looks like:
    #   ✔  success   success: message Received - [Topic mz/sink-test/basic], ...
    blocks = re.split(r"(?m)^.*?message Received -.*$\n?", output)

    for block in blocks:
        if not block.strip():
            continue

        # Extract destination topic from the "Destination:" properties line.
        # stm FULL prints: "Destination:   [Topic mz/sink-test/basic]"
        # Strip the "[Topic ...]" wrapper to return the bare topic name.
        topic = ""
        topic_m = re.search(r"Destination:\s+\[Topic ([^\]]+)\]", block)
        if topic_m:
            topic = topic_m.group(1).strip()

        # Payload follows the "message: Payload" marker.
        # stm renders JSON with unquoted keys (JS object notation), e.g.:
        #   { id: 5, label: "baz" }
        # Convert to valid JSON by quoting bare identifier keys.
        payload = ""
        payload_m = re.search(r"message:\s+Payload\s*\n(\{.*?\})", block, re.DOTALL)
        if payload_m:
            raw = payload_m.group(1).strip()
            # Quote any unquoted object key (word chars before a colon that
            # is not already preceded by a double-quote).
            fixed = re.sub(r'(?<!")\b([A-Za-z_]\w*)\s*:', r'"\1":', raw)
            payload = fixed

        if payload:
            messages.append({"topic": topic, "payload": payload})

    return messages


def workflow_round_trip(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Publish 5 messages to a queue and verify all five flow through the
    source as rows, with INCLUDE-metadata columns populated."""
    parser.parse_args()

    ensure_vm_max_map_count(c)
    c.up("solace", "materialized")
    provision_broker(c)
    c.run_testdrive_files("round-trip.td")


def workflow_exactly_once(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Crash Materialize between publish and re-select; verify zero
    duplicates and zero loss after restart."""
    parser.parse_args()

    ensure_vm_max_map_count(c)
    c.up("solace", "materialized")
    provision_broker(c)
    c.run_testdrive_files("--no-reset", "exactly-once-before.td")
    c.kill("materialized")
    c.up("materialized")
    c.run_testdrive_files("--no-reset", "exactly-once-after.td")


# ---------------------------------------------------------------------------
# Sink workflows
# ---------------------------------------------------------------------------


def workflow_sink_round_trip(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Basic Solace sink end-to-end test.

    Creates a sink writing to a static topic, inserts five rows, then uses
    stm to consume and verify all five messages arrived with the correct
    JSON payloads.
    """
    parser.parse_args()

    ensure_vm_max_map_count(c)
    c.up("solace", "materialized", "stm")
    provision_broker(c)
    provision_sink_queues(c, [("mz_sink_basic_q", "mz/sink-test/basic")])

    c.run_testdrive_files("sink-basic.td")

    messages = consume_from_queue(c, "mz_sink_basic_q")
    assert len(messages) == 5, (
        f"Expected 5 messages from sink, got {len(messages)}.\n"
        f"Raw stm output may need parse adjustment — check _parse_stm_output.\n"
        f"Messages: {messages}"
    )
    payloads = [json.loads(m["payload"]) for m in messages]
    labels = sorted(p["label"] for p in payloads)
    assert labels == [
        "bar",
        "baz",
        "foo",
        "hello",
        "world",
    ], f"Unexpected labels: {labels}"
    print("sink_round_trip: PASSED")


def workflow_sink_dedup(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Dedup-window test for the Solace sink.

    Three rows inserted rapidly all map to the same rendered topic so only
    the first is published within the window. After the window expires a
    fourth insert produces a second publish. Verifies exactly 2 messages.
    """
    parser.parse_args()

    ensure_vm_max_map_count(c)
    c.up("solace", "materialized", "stm")
    provision_broker(c)
    provision_sink_queues(c, [("mz_dedup_q", "mz/sink-test/dedup")])

    c.run_testdrive_files("sink-dedup.td")

    messages = consume_from_queue(c, "mz_dedup_q", exit_after=12)
    assert len(messages) == 2, (
        f"Expected exactly 2 messages (dedup suppressed rapid duplicates), "
        f"got {len(messages)}.\nMessages: {messages}"
    )
    print("sink_dedup: PASSED")


def workflow_sink_dynamic_topic(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Dynamic topic routing test for the Solace sink.

    Inserts rows with different 'region' column values; the TOPIC template
    '{region}' routes each row to a distinct topic. A NULL region row is
    silently dropped. Verifies 4 messages on the correct per-region topics.
    """
    parser.parse_args()

    ensure_vm_max_map_count(c)
    c.up("solace", "materialized", "stm")
    provision_broker(c)
    # One wildcard queue captures all per-region topic variants.
    provision_sink_queues(c, [("mz_dynamic_q", "mz/sink-test/>")])

    c.run_testdrive_files("sink-dynamic-topic.td")

    messages = consume_from_queue(c, "mz_dynamic_q")
    assert len(messages) == 4, (
        f"Expected 4 messages (1 NULL row silently dropped), "
        f"got {len(messages)}.\nMessages: {messages}"
    )

    topics = sorted(m["topic"] for m in messages if m["topic"])
    expected_topics = sorted(
        [
            "mz/sink-test/eu-west",
            "mz/sink-test/us-east",
            "mz/sink-test/us-east",
            "mz/sink-test/us-west",
        ]
    )
    assert (
        topics == expected_topics
    ), f"Topic routing mismatch.\nGot:      {topics}\nExpected: {expected_topics}"
    print("sink_dynamic_topic: PASSED")


def workflow_sink_reconnect(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Broker reconnect test for the Solace sink.

    Publishes rows before and after a broker kill+restart cycle. The sink
    uses reconnect_retries=-1 (infinite) so it reconnects automatically once
    the broker returns. Rows published during the outage use Direct delivery
    and are silently dropped (fire-and-forget). Verifies:
      - 2 pre-crash messages arrive before the kill
      - 2 post-restart messages arrive after reconnect
      - 0 during-outage messages (no ghost delivery)
    """
    parser.parse_args()

    ensure_vm_max_map_count(c)
    c.up("solace", "materialized", "stm")
    provision_broker(c)
    provision_sink_queues(c, [("mz_reconnect_q", "mz/sink-test/reconnect")])

    # Phase 1: publish before crash and verify they arrive.
    c.run_testdrive_files("--no-reset", "sink-reconnect-before.td")
    pre_msgs = consume_from_queue(c, "mz_reconnect_q", exit_after=15)
    assert len(pre_msgs) == 2, (
        f"Expected 2 pre-crash messages, got {len(pre_msgs)}.\n" f"Messages: {pre_msgs}"
    )
    print(f"sink_reconnect phase 1: {len(pre_msgs)} pre-crash messages — OK")

    # Phase 2: kill broker; insert rows that will be silently dropped.
    c.kill("solace")
    c.run_testdrive_files("--no-reset", "sink-reconnect-during.td")

    # Phase 3: restart broker and re-provision (broker loses all state on restart).
    c.up("solace")
    provision_broker(c)
    provision_sink_queues(c, [("mz_reconnect_q", "mz/sink-test/reconnect")])
    # Allow the Solace SDK's reconnect loop (1 s between retries) to
    # re-establish the session after the broker healthcheck passes.
    time.sleep(5)

    # Drain any during-outage rows that the sink replays after reconnect.
    # The Materialize sink checkpoints progress; after broker reconnect it
    # replays from the last persisted watermark, which may include rows that
    # were committed to Materialize while the broker was down. Drain these
    # so the Phase 4 assertion only sees post-restart messages.
    consume_from_queue(c, "mz_reconnect_q", exit_after=8)

    # Phase 4: publish after reconnect and verify.
    c.run_testdrive_files("--no-reset", "sink-reconnect-after.td")
    post_msgs = consume_from_queue(c, "mz_reconnect_q", exit_after=20)
    post_only = [m for m in post_msgs if "post-restart" in m.get("payload", "")]
    assert len(post_only) == 2, (
        f"Expected 2 post-restart messages, got {len(post_only)}.\n"
        f"Messages: {post_msgs}"
    )
    print(f"sink_reconnect phase 4: {len(post_only)} post-restart messages — OK")
    print("sink_reconnect: PASSED")


# ---------------------------------------------------------------------------
# Performance benchmark
# ---------------------------------------------------------------------------

PERF_QUEUE = "mz_perf_q"
PERF_SINK_TOPIC = "mz/perf/stats"
PERF_SINK_QUEUE = "mz_perf_sink_q"

# Performance goals — each is a preset of source options and dyncfg values.
# Pass `--goal <name>` to workflow_perf to select one.
PERF_GOALS: dict[str, dict] = {
    "latency": {
        "description": "Minimize ingest lag; exactly-once guarantee preserved",
        "ack_window_size": 255,
        "flow_max_unacked": 10000,
        "ack_mode": "client",
        "parallelism": 1,
        "deduplicate": True,
        "probe_interval": "200ms",
    },
    "balanced": {
        "description": "Balanced throughput and latency; exactly-once guarantee preserved",
        "ack_window_size": 255,
        "flow_max_unacked": 10000,
        "ack_mode": "client",
        "parallelism": 1,
        "deduplicate": True,
        "probe_interval": "500ms",
    },
    "throughput": {
        "description": "Maximum throughput; at-most-once (exactly-once NOT guaranteed)",
        "ack_window_size": 255,
        "flow_max_unacked": 10000,
        "ack_mode": "auto",
        "parallelism": 1,
        "deduplicate": False,
        "probe_interval": "500ms",
    },
}


def _semp_post_perf(semp_port: int, path: str, body: dict) -> int:
    """POST to SEMP; return status code. Only a genuine ALREADY_EXISTS is
    treated as benign. SEMP also returns HTTP 400 while the message VPN is
    still warming up after broker start; treating those as already-exists
    silently skips queue creation and produces a zero-ingestion run, so any
    other 4xx is retried with backoff and eventually raised.
    """
    semp_root = f"http://127.0.0.1:{semp_port}/SEMP/v2/config/msgVpns/default"
    credentials = base64.b64encode(b"admin:admin").decode()
    data = json.dumps(body).encode()
    last_err = "unknown"
    for attempt in range(20):
        req = urllib.request.Request(
            f"{semp_root}{path}",
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Basic {credentials}",
            },
        )
        try:
            with urllib.request.urlopen(req) as resp:
                print(f"SEMP POST {path}: {resp.status}")
                return resp.status
        except urllib.error.HTTPError as e:
            body_text = e.read().decode(errors="replace")[:300]
            if "ALREADY_EXISTS" in body_text or e.code == 409:
                print(f"SEMP POST {path}: {e.code} (already exists, continuing)")
                return e.code
            last_err = f"{e.code} — {body_text}"
        except urllib.error.URLError as e:
            last_err = str(e)
        print(f"SEMP POST {path}: not ready ({last_err}), retry {attempt + 1}/20")
        time.sleep(3)
    raise RuntimeError(f"SEMP POST {path} failed after retries: {last_err}")


def _semp_queue_stats(semp_port: int, queue: str) -> dict:
    """Return SEMP monitor stats for a queue.

    q_backlog = lastSpooledMsgId - highestAckedMsgId is the reliable pending
    message count: it measures messages published minus the highest application-
    level ACK watermark, so it works for both ack_mode=client and ack_mode=auto.
    spooledMsgCount is unreliable with ack_mode=auto (the spool count does not
    decrease as messages are consumed; the ack IDs do).
    """
    url = (
        f"http://127.0.0.1:{semp_port}/SEMP/v2/monitor"
        f"/msgVpns/default/queues/{queue}"
    )
    credentials = base64.b64encode(b"admin:admin").decode()
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Basic {credentials}"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            d = json.loads(resp.read()).get("data", {})
            last_id = d.get("lastSpooledMsgId")
            acked_id = d.get("highestAckedMsgId")
            if last_id is not None and acked_id is not None:
                q_backlog = int(last_id) - int(acked_id)
            else:
                q_backlog = -1
            return {
                "spooledMsgCount": int(d.get("spooledMsgCount", -1)),
                "msgSpoolUsage": int(d.get("msgSpoolUsage", -1)),
                "txUnackedMsgCount": int(d.get("txUnackedMsgCount", -1)),
                "lastSpooledMsgId": int(last_id) if last_id is not None else -1,
                "highestAckedMsgId": int(acked_id) if acked_id is not None else -1,
                "q_backlog": q_backlog,
            }
    except Exception:
        return {
            "spooledMsgCount": -1,
            "msgSpoolUsage": -1,
            "txUnackedMsgCount": -1,
            "lastSpooledMsgId": -1,
            "highestAckedMsgId": -1,
            "q_backlog": -1,
        }


def _get_sink_queue_msg_count(semp_port: int, queue: str) -> int:
    """Return current spooled message count for a queue via SEMP monitor."""
    return _semp_queue_stats(semp_port, queue)["spooledMsgCount"]


def _ghcr_alias_tag() -> str:
    """Return the ghcr alias tag for the current git branch.

    Mirrors the logic in ci/solace/build.py: main -> "latest", any other branch
    -> sanitized branch name (e.g. fix/solace-probe-frontier -> fix-solace-probe-frontier).
    """
    import re

    try:
        branch = subprocess.check_output(
            ["git", "branch", "--show-current"], text=True
        ).strip()
    except Exception:
        branch = ""
    if not branch or branch == "main":
        return "latest"
    sanitized = re.sub(r"[^a-zA-Z0-9._-]", "-", branch).strip("-")
    return sanitized or "dev"


def _ensure_images_from_ghcr(branch_tag: str | None = None) -> None:
    """Pull images from ghcr.io/jessemenning and retag with local fingerprint hashes.

    mzbuild computes a content-addressed fingerprint for each image that changes
    with every commit (it hashes ALL git-tracked files). Rather than triggering a
    full local Rust recompile, this pulls the pre-built alias-tagged images from
    our ghcr fork and retaggs them with whatever fingerprint hash the local mzbuild
    expects, so acquire() finds them in cache and skips the build entirely.
    """
    if branch_tag is None:
        branch_tag = _ghcr_alias_tag()
    from pathlib import Path

    from materialize import mzbuild as _mzbuild

    GHCR_FORK = "ghcr.io/jessemenning/materialize-solace"
    TARGET_IMAGES = {"materialized", "testdrive"}

    repo = _mzbuild.Repository(Path("."), profile=_mzbuild.Profile.OPTIMIZED)
    deps = repo.resolve_dependencies(
        image for image in repo if image.name in TARGET_IMAGES
    )

    for dep in [d for d in deps if d.name in TARGET_IMAGES]:
        alias = f"{GHCR_FORK}/{dep.name}:{branch_tag}"
        fingerprint_tag = (
            dep.spec()
        )  # e.g. ghcr.io/materializeinc/.../materialized:mzbuild-XXXX
        print(f"==> Pulling {alias}")
        subprocess.run(["docker", "pull", alias], check=True, capture_output=False)
        print(f"==> Retagging as {fingerprint_tag}")
        subprocess.run(["docker", "tag", alias, fingerprint_tag], check=True)


def workflow_perf(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Performance benchmark: sustained 250 msg/sec, 3 cascading MVs, 1 Solace sink.

    Publishes JSON messages to a Solace queue at the target rate, then polls
    Materialize to measure:

    - Actual ingest throughput (rows/sec at source level)
    - Ingest lag: time from last publish until all rows are visible in perf_src
    - MV cascade lag: time for mv3.total_msgs to match source count
    - Sink delivery: message count in the sink verification queue

    Pass --duration and --rate to override defaults.
    """
    parser.add_argument(
        "--goal",
        choices=list(PERF_GOALS),
        required=True,
        help="Performance goal: latency | balanced | throughput",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Seconds to publish at the target rate (default: 30)",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=250,
        help="Target publish rate in messages/sec (default: 250)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Seconds between Materialize poll samples (default: 1.0)",
    )
    parser.add_argument(
        "--probe-interval",
        type=str,
        default=None,
        help="Override solace_probe_interval dyncfg (e.g. '200ms'). Defaults to goal's value.",
    )
    parser.add_argument(
        "--ghcr-tag",
        type=str,
        default=None,
        help="ghcr alias tag to pull (e.g. 'fix-solace-probe-frontier', 'latest'). "
        "Defaults to the sanitized current git branch name, or 'latest' on main.",
    )
    args = parser.parse_args()

    goal_cfg = PERF_GOALS[args.goal]
    probe_interval = args.probe_interval or goal_cfg["probe_interval"]

    DURATION = args.duration
    RATE = args.rate
    TOTAL = DURATION * RATE
    POLL_INTERVAL = args.poll_interval

    print(f"\n{'='*60}")
    print(f"Solace perf benchmark: {RATE} msg/sec × {DURATION}s = {TOTAL} messages")
    print(f"Goal: {args.goal} — {goal_cfg['description']}")
    print(
        f"  ack_window_size={goal_cfg['ack_window_size']}  flow_max_unacked={goal_cfg['flow_max_unacked']}"
    )
    print(
        f"  ack_mode={goal_cfg['ack_mode']}  parallelism={goal_cfg['parallelism']}  deduplicate={goal_cfg['deduplicate']}"
    )
    print(f"  probe_interval={probe_interval}")
    print(f"{'='*60}")

    # NOTE: Docker cache pre-warming (pulling ghcr.io/jessemenning images and
    # retagging with the local fingerprint hash) must happen BEFORE mzbuild runs —
    # i.e., before bin/mzcompose is invoked. Use test/solace/run-perf.sh instead
    # of calling bin/mzcompose directly; it runs _ensure_images_from_ghcr via
    # bin/pyactivate first, then execs mzcompose.

    # ---- Infrastructure setup -----------------------------------------------
    ensure_vm_max_map_count(c)
    c.up("solace", "materialized")
    provision_broker(c)

    semp_port = c.port("solace", 8080)

    # Source queue — non-exclusive when parallelism > 1 so multiple workers can bind
    _semp_post_perf(
        semp_port,
        "/queues",
        {
            "queueName": PERF_QUEUE,
            "accessType": (
                "non-exclusive" if goal_cfg["parallelism"] > 1 else "exclusive"
            ),
            "permission": "consume",
            "ingressEnabled": True,
            "egressEnabled": True,
        },
    )
    # Sink verification queue subscribed to the perf sink topic
    _semp_post_perf(
        semp_port,
        "/queues",
        {
            "queueName": PERF_SINK_QUEUE,
            "accessType": "exclusive",
            "permission": "consume",
            "ingressEnabled": True,
            "egressEnabled": True,
        },
    )
    _semp_post_perf(
        semp_port,
        f"/queues/{PERF_SINK_QUEUE}/subscriptions",
        {"subscriptionTopic": PERF_SINK_TOPIC},
    )

    # ---- Apply dyncfg overrides BEFORE source creation ----------------------
    # SOLACE_PROBE_INTERVAL is re-read by the source's probe ticker after
    # every tick, so changes apply to running sources too. Setting it before
    # CREATE SOURCE just avoids one interval of mixed cadence.
    mz_sys_port = c.port("materialized", 6877)
    subprocess.run(
        [
            "psql",
            "-h",
            "127.0.0.1",
            "-p",
            str(mz_sys_port),
            "-U",
            "mz_system",
            "-d",
            "materialize",
            "-c",
            f"ALTER SYSTEM SET solace_probe_interval = '{probe_interval}'",
        ],
        check=True,
        capture_output=True,
    )
    print(f"solace_probe_interval set to {probe_interval}")

    # ---- DDL setup via direct SQL -------------------------------------------
    print("\nCreating source, MVs, and sink via SQL...")
    ack_mode = goal_cfg["ack_mode"]
    parallelism = goal_cfg["parallelism"]
    ack_window_size = goal_cfg["ack_window_size"]
    flow_max_unacked = goal_cfg["flow_max_unacked"]
    deduplicate = str(goal_cfg["deduplicate"]).lower()
    c.sql("CREATE SECRET IF NOT EXISTS perf_pw AS 'changeme'")
    c.sql(
        "CREATE CONNECTION IF NOT EXISTS perf_conn TO SOLACE ("
        "  HOST 'tcp://solace:55555',"
        "  MESSAGE VPN 'default',"
        "  USERNAME 'mz_user',"
        "  PASSWORD SECRET perf_pw"
        ")"
    )
    c.sql(
        f"CREATE SOURCE perf_src"
        f"  IN CLUSTER quickstart"
        f"  FROM SOLACE CONNECTION perf_conn ("
        f"    QUEUE 'mz_perf_q',"
        f"    ACK WINDOW SIZE {ack_window_size},"
        f"    FLOW MAX UNACKED {flow_max_unacked},"
        f"    ACK MODE '{ack_mode}',"
        f"    PARALLELISM {parallelism},"
        f"    DEDUPLICATE {deduplicate}"
        f"  )"
        f"  FORMAT BYTES"
        f"  INCLUDE"
        f"    REPLICATION GROUP MESSAGE ID AS rgmid,"
        f"    BROKER TIMESTAMP AS broker_ts"
    )
    c.sql(
        "CREATE MATERIALIZED VIEW mv1 AS"
        "  SELECT"
        "    rgmid,"
        "    broker_ts,"
        "    (convert_from(data, 'UTF8')::jsonb->>'seq')::bigint        AS seq,"
        "    (convert_from(data, 'UTF8')::jsonb->>'send_ts_ms')::bigint AS send_ts_ms"
        "  FROM perf_src"
    )
    c.sql(
        "CREATE MATERIALIZED VIEW mv2 AS"
        "  SELECT"
        "    date_trunc('second', broker_ts) AS ts_second,"
        "    count(*)                        AS msg_count,"
        "    max(seq)                        AS max_seq,"
        "    min(send_ts_ms)                 AS min_send_ts_ms,"
        "    max(send_ts_ms)                 AS max_send_ts_ms"
        "  FROM mv1"
        "  GROUP BY date_trunc('second', broker_ts)"
    )
    c.sql(
        "CREATE MATERIALIZED VIEW mv3 AS"
        "  SELECT"
        "    sum(msg_count)::bigint                                             AS total_msgs,"
        "    count(*)::bigint                                                   AS seconds_with_data,"
        "    (sum(msg_count)::numeric / NULLIF(count(*), 0))::numeric(10,1)    AS avg_msgs_per_second,"
        "    max(max_seq)                                                       AS latest_seq,"
        "    max(max_send_ts_ms)                                                AS latest_send_ts_ms"
        "  FROM mv2"
    )
    c.sql(
        "CREATE SINK perf_sink"
        "  IN CLUSTER quickstart"
        "  FROM mv3"
        "  INTO SOLACE CONNECTION perf_conn ("
        "    TOPIC 'mz/perf/stats'"
        "  )"
    )
    time.sleep(3)
    print("DDL setup complete.\n")

    # ---- Publisher (SMF via solace-pubsubplus SDK, runs in background thread) --
    from solace.messaging.messaging_service import MessagingService
    from solace.messaging.resources.topic import Topic as SolaceTopic

    smf_port = c.port("solace", 55555)
    messaging_service = (
        MessagingService.builder()
        .from_properties(
            {
                "solace.messaging.transport.host": f"tcp://127.0.0.1:{smf_port}",
                "solace.messaging.service.vpn-name": "default",
                "solace.messaging.authentication.scheme.basic.username": "mz_user",
                "solace.messaging.authentication.scheme.basic.password": "changeme",
            }
        )
        .build()
    )
    messaging_service.connect()
    smf_publisher = (
        messaging_service.create_persistent_message_publisher_builder().build()
    )
    smf_publisher.start()
    smf_dest = SolaceTopic.of(f"#P2P/QUE/{PERF_QUEUE}")
    msg_builder = messaging_service.message_builder()

    publish_results: dict = {}

    publish_start = time.monotonic()

    def _publish_thread() -> None:
        errors = 0
        rate_interval = 1.0 / RATE
        for seq in range(TOTAL):
            target = publish_start + seq * rate_interval
            delta = target - time.monotonic()
            if delta > 0:
                time.sleep(delta)
            payload = json.dumps({"seq": seq, "send_ts_ms": int(time.time() * 1000)})
            try:
                msg = msg_builder.build(payload)
                smf_publisher.publish(message=msg, destination=smf_dest)
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"  publish error: {e}")
        end = time.monotonic()
        smf_publisher.terminate()
        messaging_service.disconnect()
        publish_results.update(
            {
                "end": end,
                "errors": errors,
                "total_published": TOTAL - errors,
                "actual_duration": end - publish_start,
                "actual_rate": (
                    (TOTAL - errors) / (end - publish_start)
                    if end > publish_start
                    else 0
                ),
            }
        )

    pub_thread = threading.Thread(target=_publish_thread, daemon=True)
    print(f"Publishing {TOTAL} messages at {RATE} msg/sec (background thread)...")
    pub_thread.start()

    # ---- Measurement loop — runs during AND after publishing ----------------
    # Columns:
    #   Phase     — PUBLISHING or CATCH-UP
    #   Elapsed   — seconds since publish start (negative = still publishing)
    #   pub_sent  — estimated messages published so far
    #   src_count — messages visible in perf_src
    #   src_ms    — SELECT count(*) FROM perf_src query latency (frontier freshness proxy)
    #   mv3_total — messages aggregated in mv3
    #   mv3_ms    — SELECT … FROM mv3 query latency
    #   lag_ms    — wall-clock - latest send_ts_ms visible in mv3 (end-to-end latency)
    #   broker_lag_ms — avg(broker_ts - send_ts_ms) from mv1 (broker timestamping delay)
    #   q_backlog — rxMsgCount - txMsgCount from SEMP (reliable regardless of ack_mode)
    #   q_delta   — change in q_backlog since last poll (negative = source consuming)
    COL_FMT = (
        f"{'Phase':>10}  {'Elapsed':>8}  {'pub_sent':>8}  {'src_count':>9}  "
        f"{'src_ms':>6}  {'mv3_total':>9}  {'mv3_ms':>6}  "
        f"{'lag_ms':>7}  {'brkr_lag':>8}  {'q_backlog':>9}  {'q_delta':>7}"
    )
    print("\n" + COL_FMT)
    print("-" * len(COL_FMT))

    sample_times: list[float] = []  # elapsed after publish_end
    src_counts: list[int] = []
    mv3_totals: list[int] = []
    ingest_lag_ms_samples: list[float] = []
    broker_lag_ms_samples: list[float] = []
    src_ms_samples: list[int] = []
    mv3_ms_samples: list[int] = []

    prev_q_depth: int = -1
    last_poll = time.monotonic()

    # Poll during publishing, then continue until caught up or deadline.
    while pub_thread.is_alive() or (
        publish_results
        and time.monotonic() < publish_results["end"] + max(60, POLL_INTERVAL * 4)
    ):
        # Rate-limit polls to POLL_INTERVAL
        sleep_for = max(0.0, POLL_INTERVAL - (time.monotonic() - last_poll))
        time.sleep(sleep_for)
        last_poll = time.monotonic()

        phase = "PUBLISHING" if pub_thread.is_alive() else "CATCH-UP"
        # Estimate messages sent so far (only meaningful while still publishing)
        if pub_thread.is_alive():
            elapsed_since_start = time.monotonic() - publish_start
            pub_sent = min(TOTAL, int(elapsed_since_start * RATE))
            elapsed_str = f"-{max(0, TOTAL/RATE - elapsed_since_start):6.1f}s"
        else:
            pub_sent = publish_results.get("total_published", TOTAL)
            elapsed_after = time.monotonic() - publish_results["end"]
            elapsed_str = f"+{elapsed_after:6.1f}s"

        src_count = 0
        mv3_total = 0
        try:
            src_t0 = time.monotonic()
            src_rows = c.sql_query("SELECT count(*) FROM perf_src")
            src_ms = int((time.monotonic() - src_t0) * 1000)
            src_count = int(src_rows[0][0]) if src_rows else 0

            mv3_t0 = time.monotonic()
            mv3_rows = c.sql_query(
                "SELECT total_msgs, latest_seq, latest_send_ts_ms FROM mv3"
            )
            mv3_ms = int((time.monotonic() - mv3_t0) * 1000)
            if mv3_rows and mv3_rows[0][0] is not None:
                mv3_total = int(mv3_rows[0][0])
                mv3_send_ts_ms = int(mv3_rows[0][2]) if mv3_rows[0][2] else 0
                lag_ms = (
                    int(time.time() * 1000) - mv3_send_ts_ms
                    if mv3_send_ts_ms > 0
                    else -1
                )
            else:
                mv3_total, mv3_send_ts_ms, lag_ms = 0, 0, -1

            # Broker-vs-publisher timestamp delta: how much the broker clock
            # leads or lags the Python publisher's send_ts_ms.  A value near 0
            # means the broker timestamps messages immediately on receipt.
            broker_lag_row = c.sql_query(
                "SELECT avg(extract(epoch from broker_ts) * 1000 - send_ts_ms::float)"
                "  FROM mv1 WHERE send_ts_ms > 0"
            )
            broker_lag_ms = (
                float(broker_lag_row[0][0])
                if broker_lag_row and broker_lag_row[0][0] is not None
                else -1.0
            )

            qs = _semp_queue_stats(semp_port, PERF_QUEUE)
            q_backlog = qs["q_backlog"]
            first_poll = prev_q_depth < 0
            q_delta = (
                q_backlog - prev_q_depth if not first_poll and q_backlog >= 0 else 0
            )
            prev_q_depth = q_backlog

            broker_lag_str = (
                f"{broker_lag_ms:+.0f}ms" if broker_lag_ms >= -0.5 else "   -"
            )
            q_delta_str = "-" if first_poll else f"{q_delta:+d}"

            print(
                f"{phase:>10}  {elapsed_str:>8}  {pub_sent:>8}  {src_count:>9}  "
                f"{src_ms:>5}ms  {mv3_total:>9}  {mv3_ms:>5}ms  "
                f"{lag_ms:>6}ms  {broker_lag_str:>8}  {q_backlog:>9}  {q_delta_str:>7}"
            )

            if not pub_thread.is_alive():
                elapsed_after = time.monotonic() - publish_results["end"]
                sample_times.append(elapsed_after)
                src_counts.append(src_count)
                mv3_totals.append(mv3_total)
                # Filter stale lag readings (> 60s means publish ended long ago)
                if 0 < lag_ms < 60_000:
                    ingest_lag_ms_samples.append(lag_ms)
                if broker_lag_ms >= 0:
                    broker_lag_ms_samples.append(broker_lag_ms)
                src_ms_samples.append(src_ms)
                mv3_ms_samples.append(mv3_ms)

            if (
                not pub_thread.is_alive()
                and src_count >= publish_results.get("total_published", TOTAL)
                and mv3_total >= publish_results.get("total_published", TOTAL)
            ):
                print("All messages visible in source and mv3 — done polling.\n")
                break

        except Exception as exc:
            print(f"  [query error: {exc}]")

    pub_thread.join(timeout=10)

    actual_duration = publish_results.get("actual_duration", 0)
    actual_rate = publish_results.get("actual_rate", 0)
    total_published = publish_results.get("total_published", 0)
    pub_errors = publish_results.get("errors", TOTAL)
    publish_results.get("end", time.monotonic())

    print(
        f"\nPublished {total_published}/{TOTAL} in {actual_duration:.1f}s "
        f"({actual_rate:.0f} msg/sec, {pub_errors} errors)\n"
    )

    # ---- Sink check ---------------------------------------------------------
    print("Checking sink delivery count via SEMP monitor (5s grace)...")
    time.sleep(5)
    sink_msg_count = _get_sink_queue_msg_count(semp_port, PERF_SINK_QUEUE)
    print(f"Sink queue '{PERF_SINK_QUEUE}': {sink_msg_count} messages spooled\n")

    # ---- Report -------------------------------------------------------------
    src_catchup_s = next(
        (sample_times[i] for i, v in enumerate(src_counts) if v >= total_published),
        None,
    )
    mv3_catchup_s = next(
        (sample_times[i] for i, v in enumerate(mv3_totals) if v >= total_published),
        None,
    )

    def _ms(samples: list, fn=statistics.mean) -> str:
        return f"{fn(samples):.0f}ms" if samples else "n/a"

    print("=" * 60)
    print("PERFORMANCE REPORT")
    print("=" * 60)
    print(f"Target rate:            {RATE} msg/sec")
    print(f"Actual publish rate:    {actual_rate:.0f} msg/sec")
    print(f"Messages attempted:     {TOTAL}")
    print(f"Messages published:     {total_published}")
    print(f"Publish errors:         {pub_errors}")
    print()
    print(
        "Source catch-up:        "
        + (
            f"+{src_catchup_s:.1f}s after last publish"
            if src_catchup_s is not None
            else "NOT REACHED"
        )
    )
    print(
        "MV3 catch-up:           "
        + (
            f"+{mv3_catchup_s:.1f}s after last publish"
            if mv3_catchup_s is not None
            else "NOT REACHED"
        )
    )
    print()
    print("-- Ingest lag (wall-clock minus latest send_ts_ms visible in mv3) --")
    print(f"  avg:    {_ms(ingest_lag_ms_samples)}")
    print(f"  median: {_ms(ingest_lag_ms_samples, statistics.median)}")
    print(
        f"  max:    {_ms(ingest_lag_ms_samples, max) if ingest_lag_ms_samples else 'n/a'}"
    )
    print()
    print("-- Lag decomposition --")
    avg_broker = statistics.mean(broker_lag_ms_samples) if broker_lag_ms_samples else -1
    avg_src_ms = statistics.mean(src_ms_samples) if src_ms_samples else -1
    avg_mv3_ms = statistics.mean(mv3_ms_samples) if mv3_ms_samples else -1
    print(f"  broker_lag (broker_ts - send_ts_ms, avg):  {avg_broker:+.0f}ms")
    print("    — broker-side timestamping delay; near 0 = broker stamps on receipt")
    print(f"  src query latency (avg):   {avg_src_ms:.0f}ms")
    print("    — time SELECT count(*) FROM perf_src blocks waiting for frontier;")
    print("      ≈ probe_interval means the frontier advanced just before the query")
    print(f"  mv3 query latency (avg):   {avg_mv3_ms:.0f}ms")
    print("    — includes mv1→mv2→mv3 cascade compute after frontier advance")
    print()
    print(f"Sink messages spooled:  {sink_msg_count}")
    print("=" * 60)
