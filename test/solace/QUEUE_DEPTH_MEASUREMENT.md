# Measuring Solace Queue Depth via SEMP

## TL;DR

Use `lastSpooledMsgId - highestAckedMsgId`, not `spooledMsgCount`.

## Background

The SEMP v2 monitor API (`GET /SEMP/v2/monitor/msgVpns/{vpn}/queues/{queue}`) exposes
several fields that appear to measure queue depth. Only the watermark-difference formula
is reliable across all ack modes.

## Why `spooledMsgCount` is wrong

`spooledMsgCount` is a **cumulative** counter — "the number of guaranteed messages
that were spooled in the Queue" (past tense). With `ack_mode=auto`, the Solace C SDK
acknowledges messages on delivery, before the application processes them. The broker
therefore never sees unacknowledged messages and never decrements the spool count.
Result: `spooledMsgCount` stays pinned at the total-ever-published value and reads as
300,000 even when the queue is fully drained.

With `ack_mode=client` the count does decrease as acks flow, but it still lags because
acks are batched and sent asynchronously.

## The correct formula

```
q_backlog = lastSpooledMsgId - highestAckedMsgId
```

Field definitions from the broker's own SEMP spec
(`GET /SEMP/v2/monitor/spec`, `definitions.MsgVpnQueue.properties`):

| Field | Type | Definition |
|-------|------|------------|
| `lastSpooledMsgId` | int64 | "The identifier (ID) of the last guaranteed message spooled in the Queue." |
| `highestAckedMsgId` | int64 | "The highest identifier (ID) of guaranteed messages in the Queue that were acknowledged." |

Because RGMIDs are monotonically increasing within a broker/HA pair, their difference
gives the number of messages that have arrived but whose acknowledgement has not yet
been recorded — i.e., the true pending backlog.

**Empirical verification:** after a completed 300k-message test with `ack_mode=auto`,
both IDs equalled 303,001 → backlog = 0. During publishing at 500 msg/sec the backlog
held steady at 16–153 messages (one probe-interval's worth of in-flight messages).

## Reference implementation

```python
def _semp_queue_stats(semp_port: int, queue: str) -> dict:
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
                "lastSpooledMsgId": int(last_id) if last_id is not None else -1,
                "highestAckedMsgId": int(acked_id) if acked_id is not None else -1,
                "q_backlog": q_backlog,
            }
    except Exception:
        return {"spooledMsgCount": -1, "lastSpooledMsgId": -1,
                "highestAckedMsgId": -1, "q_backlog": -1}
```

The live implementation is in `test/solace/mzcompose.py` → `_semp_queue_stats()`.

## Additional fields of interest

| Field | Use |
|-------|-----|
| `lowestAckedMsgId` | `highestAckedMsgId - lowestAckedMsgId` = window of in-flight messages; useful for diagnosing a stuck consumer |
| `msgSpoolUsage` | Bytes on disk; useful for spotting large messages or accumulation |
| `txUnackedMsgCount` | Messages delivered to a consumer but not yet acked — only meaningful with `ack_mode=client` |

## Fields that do NOT exist on this endpoint

`rxMsgCount` and `txMsgCount` are **not** present in the SEMP v2 monitor queue
response. Do not use them.
