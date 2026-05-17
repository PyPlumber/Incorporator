***

> 📑 **Reference** — production-debugging deep dive. Not a numbered
> tutorial; reach for this when you need durable error logs and DLQ
> retry patterns for a `stream()` / `fjord()` pipeline.

***

# 🩺 Production Debugging: `get_error()` + LoggedIncorporator

When a production pipeline fails at 03:00, you don't want to grep a
multi-GB log. You want a structured list of failures you can iterate,
filter, and feed back into a retry loop.

That's what `LoggedIncorporator` + `get_error()` give you. Subclass
`LoggedIncorporator` and pass `enable_logging=True` on each verb call,
and every failure that class encounters lands in
`logs/<ClassName>_error.log` as JSON lines — readable from any other
process, retrievable in your own Python with one async call.

---

## The Production Loop

```text
   incorp() / stream() / fjord()  ───┐
                                     │ emit Wave on each wave
   ┌─────────────────────────┐       ▼
   │  failed_sources on Wave │ ◄─── permanent failure
   └─────────────┬───────────┘
                 │
   ┌─────────────▼───────────┐
   │  logs/<Class>_error.log │ ◄─── JSON line per failure
   └─────────────┬───────────┘
                 │
                 ▼
       await Class.get_error()   ───►  List[dict]  ───►  DLQ retry
```

`failed_sources` is the **live** view (this wave); `get_error()` is
the **durable** view (everything this class has ever logged).

---

## Step 1: Subclass `LoggedIncorporator`

```python
from incorporator import LoggedIncorporator


class Webhook(LoggedIncorporator):
    """Production webhook ingester — every failure hits disk when
    enable_logging=True is passed on each verb call."""
```

That's the structural setup. `LoggedIncorporator` configures the
class's `QueueHandler` background thread the first time you pass
`enable_logging=True` to one of its verbs, so disk I/O never blocks
your event loop. Logging stays **opt-in per call** — pass
`enable_logging=True` when you want the disk trail, omit it for
quick exploratory calls.

---

## Step 2: Run a Pipeline With Some Bad URLs

```python
import asyncio
from incorporator import LoggedIncorporator


class Webhook(LoggedIncorporator):
    pass


async def main():
    # Mix of good + bad URLs to exercise the DLQ path.
    sources = [
        "https://jsonplaceholder.typicode.com/users/1",
        "https://jsonplaceholder.typicode.com/users/2",
        "https://this-host-does-not-exist.example.invalid/data",
    ]
    webhooks = await Webhook.incorp(inc_url=sources, inc_code="id", enable_logging=True)

    print(f"Loaded {len(webhooks)} records.")
    print(f"failed_sources (live view): {webhooks.failed_sources}")


asyncio.run(main())
```

After the run, `logs/Webhook_error.log` contains one JSON line per
permanent failure — URLs redacted of query strings, full `Wave` dump
attached.

---

## Step 3: Query `get_error()` After the Run

`get_error()` tails the class's error log and returns each line as a
parsed dict. Disk read runs in a worker thread.

```python
errors = await Webhook.get_error()

for record in errors:
    print(record["timestamp"], "—", record["msg"])
    # Each record contains:
    #   level:     "ERROR"
    #   msg:       human-readable summary
    #   meta:      flat key:"value" summary (class, identity, origin)
    #   wave:      full Wave dump (chunk index, rows, failed_sources, etc.)
    #   timestamp: ISO-8601
```

Safe to call when no errors have been logged yet — returns `[]`.

---

## Step 4: Wire It Into a DLQ Retry Loop

The production pattern: drain `get_error()`, extract the failed URLs
from each record's `wave.failed_sources`, and reissue them as a
follow-up `incorp()` call.

```python
async def retry_failed_webhooks():
    errors = await Webhook.get_error()
    dlq_urls = []
    for record in errors:
        wave = record.get("wave") or {}
        dlq_urls.extend(wave.get("failed_sources", []))

    if not dlq_urls:
        print("✅ No DLQ entries — pipeline is clean.")
        return

    # Dedup before retry — the same URL may appear across multiple waves.
    dlq_urls = list(set(dlq_urls))
    print(f"♻️  Retrying {len(dlq_urls)} previously-failed URLs.")
    return await Webhook.incorp(inc_url=dlq_urls, inc_code="id", enable_logging=True)
```

That's the entire DLQ retry shape. No external queue service, no log
parsing scripts — just two `await`s.

---

## What `LoggedIncorporator` Writes

For each instance of `LoggedIncorporator`, three log files are
maintained under `logs/`:

| File | Contents |
|---|---|
| `<ClassName>_api.log` | Successful chunks — every Wave that touched the pipeline |
| `<ClassName>_error.log` | Permanent failures only — same shape, parsed by `get_error()` |
| `<ClassName>_debug.log` | Internal lifecycle events (daemon start/stop, shutdown drain) |

Rotation, queueing, and thread management are all internal. The cap on
concurrent background listeners (`MAX_LOG_THREADS`) is enforced
automatically — the oldest listener is evicted when a new class
subscribes.

---

## When to reach for `get_error()`

| Situation | Use `get_error()`? |
|---|---|
| Post-run audit of a batch `incorp()` | ✅ Yes — one async call, get structured records |
| DLQ retry orchestrator | ✅ Yes — feed `wave.failed_sources` back into `incorp()` |
| Live observability during a `stream()` | ❌ No — read `wave.failed_sources` off the live wave |
| Cross-process inspection (separate retry worker) | ✅ Yes — the log file is the contract |

---

## Where to Go Next

| Goal | Read |
|---|---|
| Wrap a single source in a daemon and stream waves | [Tutorial 5 — Streaming Daemons](./5_streaming_daemon.md) |
| Keep a registry live with `refresh()` and inspect `failed_sources` | [Tutorial 4 — Stateful Refresh](./4_stateful_refresh.md) |
| Detect orchestration-level failures across N sources | [Tutorial 7 — Tideweaver](./7_tideweaver.md) |
| See every public method that surfaces error state | [Library Reference](./library_reference.md) |
| Ship `LoggedIncorporator` pipelines with structured logs | [Deployment Guide](./deployment.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/docs/debugging.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
