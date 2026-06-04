***

> 📑 **Reference** — production-debugging deep dive. Not a numbered
> tutorial; reach for this when you need durable error logs and a
> structured retry loop via RejectEntry for a `stream()` / `fjord()`
> pipeline.

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
       await Class.get_error()   ───►  list[dict]  ───►  retry loop via RejectEntry
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
    # Mix of good + bad URLs to exercise the rejects path.
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
    print(record["time"], "—", record["msg"])
    # Each record contains:
    #   level:     "ERROR"
    #   msg:       human-readable summary
    #   meta:      flat key:"value" summary (class, identity, origin)
    #   wave:      full Wave dump (chunk index, rows, failed_sources, etc.)
    #   time: ISO-8601 string
```

Safe to call when no errors have been logged yet — returns `[]`.

---

## Step 4: Wire It Into a Rejects Retry Loop

The production pattern: drain `get_error()`, extract the failed URLs
from each record's `wave.failed_sources`, and reissue them as a
follow-up `incorp()` call.

```python
async def retry_failed_webhooks():
    errors = await Webhook.get_error()
    reject_urls = []
    for record in errors:
        wave = record.get("wave") or {}
        reject_urls.extend(wave.get("failed_sources", []))

    if not reject_urls:
        print("✅ No rejected sources — pipeline is clean.")
        return

    # Dedup before retry — the same URL may appear across multiple waves.
    reject_urls = list(set(reject_urls))
    print(f"♻️  Retrying {len(reject_urls)} previously-failed URLs.")
    return await Webhook.incorp(inc_url=reject_urls, inc_code="id", enable_logging=True)
```

That's the entire reject-retry shape. No external queue service, no log
parsing scripts — just two `await`s.

---

## Three structured diagnostics worth knowing

Three diagnostics fire automatically — no config required.  Two land
on the wave's `failed_sources`; the third emits a one-time log
warning.

### 1. Structured rejects (`entry.error_kind`, `entry.retry_after`)

`failed_sources: list[str]` is the legacy bare-string view; the
structured equivalent is `result.rejects: list[RejectEntry]`. Each
entry carries `source` + `error_kind` (the exception class name) +
`message` + `retry_after` (parsed from any HTTP `Retry-After`
header) + `wave_index`. Reach for it when retry orchestration
needs to honour the server's backoff hints:

```python
from incorporator import RejectEntry

result = await Webhook.incorp(inc_url=urls, enable_logging=True)
for entry in result.rejects:
    if entry.error_kind == "HTTPStatusError" and entry.retry_after:
        schedule_retry(entry.source, after=entry.retry_after)
    else:
        dlq_queue.put(entry.source)
```

`failed_sources` stays as a derived view (`[entry.source for entry in
result.rejects]`) — existing code keeps working unchanged.

On the orchestration side, `Tideweaver.rejects` returns the same
`list[RejectEntry]` type, but `error_kind` can be one of four
canal-layer string literals — `"PenstockLimited"`, `"SurgeHalted"`,
`"SkipAhead"`, `"GateBlocked"` — for skips the scheduler made before
the dependent tick ran.  Each entry carries `from_name` / `to_name` /
`cooldown_sec` so per-edge attribution is straightforward.  See
[Orchestration debugging](#orchestration-debugging--loggedtideweaver--architecttune)
below.

### 2. Seed-error formatter — missing-peer `KeyError`

When a fjord pipeline's `inflow(state)` raises `KeyError` because a
peer source hasn't seeded yet, the framework rewrites the
corresponding `failed_sources` entry to a copy-pasteable suggestion:

```text
inflow(state) for source 'Race' raised KeyError on missing peer
'Track' — guard inflow(state) against missing keys (e.g.
state.get('Track')) or add depends_on=['Track'] to enforce ordering.
```

Either guard the access (`state.get('X')` returns `None` instead of
raising) or declare the ordering on the dependent source's
`stream_params` entry (`depends_on=["X"]` makes fjord wait for X
before seeding this source).  T9 and T10 walk both patterns.

### 3. Bare-class data-loss warning

When a sidecar `outflow.py` pre-declares an `Incorporator` subclass
with no fields beyond the base three (`inc_code`, `inc_name`,
`last_rcd`), Pydantic V2's default `extra='ignore'` silently drops
every row field on `model_validate`.  The framework emits a one-time
`WARNING` per class identity so the failure mode is visible:

```text
WARNING: Pre-declared subclass `FantasyTeam` has no declared fields
beyond the base three; Pydantic V2 will silently drop every row
column. Either declare the fields explicitly or remove the class
declaration to let infer_dynamic_schema take over.
```

If you see this in production logs, either flesh out the subclass
with the fields you intend to keep, or remove the pre-declaration
entirely (the engine will build a dynamic class from the row keys
at first emit — T10 documents this path under "Don't pre-declare
the output class").

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
| Rejects retry orchestrator | ✅ Yes — feed `wave.failed_sources` back into `incorp()` |
| Live observability during a `stream()` | ❌ No — read `wave.failed_sources` off the live wave |
| Cross-process inspection (separate retry worker) | ✅ Yes — the log file is the contract |

---

## Orchestration debugging — `LoggedTideweaver` + `architect.tune()`

For Tideweaver pipelines, the parallel pair is `LoggedTideweaver` +
`tune()`.  `LoggedTideweaver` is a drop-in for `Tideweaver` that routes
every yielded `Tide` and every accumulated `RejectEntry` to disk via
the same `QueueHandler` pipeline; `tune()` reads those records and
emits a `TuningReport` of severity-sorted hints.  Both names live in
the orchestration subpackage, not at the top level:

```python
from incorporator.observability.tideweaver import LoggedTideweaver, tune

tw = LoggedTideweaver(watershed, enable_logging=True, logger_name="ArbSession")
tides = [tide async for tide in tw.run()]

# Post-window: surface what the scheduler skipped + why.
for entry in tw.rejects:
    if entry.error_kind in {"PenstockLimited", "SurgeHalted",
                            "SkipAhead", "GateBlocked"}:
        print(entry.from_name, "→", entry.to_name, entry.error_kind,
              entry.cooldown_sec)

# Post-window: ask the framework what to adjust.
report = tune(rejects=tw.rejects, tides=tides, pass_interval=tw.pass_interval)
print(report.render())            # hint blocks, severity-sorted

# Cross-process replay from disk (separate retry / analysis worker):
past_tides   = await LoggedTideweaver.get_tides(logger_name="ArbSession")
past_rejects = await LoggedTideweaver.get_rejects(logger_name="ArbSession")
```

Unlike `LoggedIncorporator`'s three files, `LoggedTideweaver` also
writes a dedicated `logs/<logger_name>_tide.log` — every yielded `Tide`
(fired and no-op alike) lands there, so `get_tides()` reads that one
file sorted by `tide_number` rather than merging `_error.log` +
`_debug.log`. Rejects still live in `logs/<logger_name>_error.log`.

`tw.summary(tides=tides)` is the instance-method convenience for the
same `TuningReport`.  Each `tide.current_outcomes` is a
`list[CurrentOutcome]` carrying per-current `status` / `reason` /
`in_flight_sec` — read it to see which currents fired and which
skipped, per pass.

| Situation | Reach for |
|---|---|
| Per-pass live monitor | `async for tide in tw.run(): print(tide)` (Tide records carry `wake_reason`, `heap_depth`, `next_due_in_sec`) |
| Per-edge skip audit | `tw.rejects` filtered by `error_kind` ∈ {canal-layer strings above} |
| Post-window tuning recommendations | `tune(rejects=..., tides=..., pass_interval=...)` → `TuningReport` |
| Cross-process replay | `LoggedTideweaver.get_tides(...)` / `get_rejects(...)` |

> **Empty-output stalls fire a WARNING.** When a `CustomCurrent` tick
> succeeds but yields no rows while its upstream snapshot was non-empty,
> the scheduler emits a one-line `WARNING` per pass naming the current
> and its upstream(s) — the signal for a tick body, predicate, or
> missing-`conv_dict` bug that silently drops every row. It is a log
> warning, not a `RejectEntry`, so watch `logs/<logger_name>_debug.log`
> (or stderr) rather than `tw.rejects`.

---

## Where to Go Next

| Goal | Read |
|---|---|
| Wrap a single source in a daemon and stream waves | [Tutorial 8 — Streaming Daemons](../examples/08-streaming-daemon/README.md) |
| Keep a registry live with `refresh()` and inspect `failed_sources` | [Tutorial 7 — Stateful Refresh](../examples/07-stateful-refresh/README.md) |
| Detect orchestration-level failures across N sources | [Tutorial 11 — Tideweaver](../examples/11-tideweaver/README.md) |
| See every public method that surfaces error state | [Library Reference](./library_reference.md) |
| Ship `LoggedIncorporator` pipelines with structured logs | [Deployment Guide](./deployment.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/docs/debugging.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
