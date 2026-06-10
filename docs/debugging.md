***

> 📑 **Reference** — production-debugging deep dive. Not a numbered
> tutorial; reach for this when you need durable error logs, the
> api/error routing split, and a structured retry loop via `RejectEntry`
> for a `stream()` / `fjord()` pipeline.

***

# 🩺 Production Debugging: `LoggedIncorporator` + Reader API

When a production pipeline fails at 03:00, you don't want to grep a
multi-GB log. You want a structured list of failures you can iterate,
filter, and feed back into a retry loop.

That's what `LoggedIncorporator` + its reader API give you. Subclass
`LoggedIncorporator` and pass `enable_logging=True` on each verb call,
and every failure that class encounters is routed to one of two JSON-line
log files under `logs/` — readable from any other process, retrievable
in your own Python with one async call.

**The routing rule (v1.3.3):** URL/internet-traffic errors — HTTP 4xx/5xx
responses, network timeouts, and connection failures — route to
`logs/<ClassName>_api.log`. Codebase errors — parse failures, schema
errors, and other non-HTTP failures — route to `logs/<ClassName>_error.log`.
The `debug.log` is the superset of both. The file location tells you
whether the fault was the API's or your code's.

---

## The Production Loop

```text
   incorp() / stream() / fjord()  ───┐
                                     │ emit Wave on each wave
   ┌─────────────────────────┐       ▼
   │  failed_sources on Wave │ ◄─── permanent failure
   └─────────────┬───────────┘
                 │
        is_url_traffic_error?
         ┌───────┴────────┐
       True             False
         ▼                 ▼
  _api.log           _error.log
  (URL/HTTP errors)  (codebase/parse errors)
         └───────┬────────┘
                 ▼
       await Class.get_rejects()  ───►  list[dict]  ───►  retry loop via RejectEntry
       await Class.get_error()    ───►  codebase errors only
       await Class.get_api()      ───►  URL/HTTP errors only
```

`failed_sources` is the **live** view (this wave); `get_rejects()` is
the **durable** view — it unions `_api.log` + `_error.log` so every
reject is covered regardless of which file it landed in.

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

After the run, failures are routed to either `logs/Webhook_api.log`
(URL/internet errors, e.g. the unreachable host above) or
`logs/Webhook_error.log` (parse/codebase errors) — both carry one JSON
line per failure, URLs redacted of query strings, full `Wave` dump
attached.

---

## Step 3: Query the Reader API After the Run

Three async readers provide structured access to the log files. All
return `[]` when no log file exists yet.

```python
# All failures across both logs — the union of api.log + error.log,
# filtered to records that carry a top-level "reject" key.
rejects = await Webhook.get_rejects()

# URL/internet-traffic failures only (is_url_traffic_error=True).
api_errors = await Webhook.get_api()

# Codebase/parse failures only (is_url_traffic_error=False).
code_errors = await Webhook.get_error()

# Current session log (see get_current below for per-session use).
for record in rejects:
    print(record["time"], "—", record["msg"])
    # Each record contains:
    #   level:     "ERROR"
    #   msg:       human-readable summary
    #   meta:      flat key:"value" summary (class, identity, origin)
    #   reject:    structured RejectEntry fields (source, error_kind,
    #              message, is_url_traffic_error, retry_after, ...)
    #   wave:      full Wave dump (chunk index, rows, failed_sources, etc.)
    #   time: ISO-8601 string
```

**When to reach for each reader:**

| Situation | Use |
|---|---|
| All failures, want to retry everything | `get_rejects()` |
| Only API/network failures (flaky host, rate limit) | `get_api()` |
| Only parse/schema/codebase failures | `get_error()` |
| Live view during a current run | `wave.failed_sources` on the Wave |

---

## Step 4: Wire It Into a Rejects Retry Loop

The production pattern: drain `get_rejects()`, extract the failed URLs
from each record's reject entry, and reissue them as a follow-up
`incorp()` call. `get_rejects()` unions `_api.log` + `_error.log` so
both URL-traffic failures and codebase failures are covered in one call.

```python
async def retry_failed_webhooks():
    rejects = await Webhook.get_rejects()
    reject_urls = []
    for record in rejects:
        entry = record.get("reject") or {}
        if entry.get("source"):
            reject_urls.append(entry["source"])

    if not reject_urls:
        print("No rejected sources — pipeline is clean.")
        return

    # Dedup before retry — the same URL may appear across multiple waves.
    reject_urls = list(set(reject_urls))
    print(f"Retrying {len(reject_urls)} previously-failed URLs.")
    return await Webhook.incorp(inc_url=reject_urls, inc_code="id", enable_logging=True)
```

That's the entire reject-retry shape. No external queue service, no log
parsing scripts — just two `await`s.

---

## Three structured diagnostics worth knowing

Three diagnostics fire automatically — no config required.  Two land
on the wave's `failed_sources`; the third emits a one-time log
warning.

### 1. Structured rejects (`entry.error_kind`, `entry.is_url_traffic_error`, `entry.retry_after`)

`failed_sources: list[str]` is the legacy bare-string view; the
structured equivalent is `result.rejects: list[RejectEntry]`. Each
entry carries `source` + `error_kind` (the exception class name) +
`message` + `is_url_traffic_error` (bool, `True` for HTTP 4xx/5xx /
network errors, `False` for parse/schema errors) + `retry_after`
(parsed from any HTTP `Retry-After` header) + `wave_index`.

`is_url_traffic_error` is the programmatic form of the `_api.log` /
`_error.log` routing split. It is stamped on every `RejectEntry` at
construction time from the exception type — `True` when the underlying
exception is an httpx `HTTPStatusError` or `RequestError` (or when an
`IncorporatorNetworkError` wraps one via `__cause__`). This flag is
always present and never `None`.

Reach for it when retry orchestration needs to distinguish network
failures from codebase failures:

```python
from incorporator import RejectEntry

result = await Webhook.incorp(inc_url=urls, enable_logging=True)
for entry in result.rejects:
    if entry.is_url_traffic_error and entry.retry_after:
        schedule_retry(entry.source, after=entry.retry_after)
    elif entry.is_url_traffic_error:
        transient_queue.put(entry.source)
    else:
        # Parse/schema/codebase failure — investigate before retrying.
        dlq_queue.put(entry.source)
```

`RejectEntry.__str__` now includes the HTTP reason phrase when available.
For a 429 response you get `[HTTP 429 Too Many Requests]`; for
non-standard codes the status code alone appears (e.g. `[HTTP 522]`).

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

For each instance of `LoggedIncorporator`, four log files are maintained
under `logs/`. The routing split is enforced at the Python `logging`
filter level — every record is classified at write time, before it
touches any file handler.

| File | Contents | Reader |
|---|---|---|
| `<ClassName>_api.log` | URL/internet-traffic errors: HTTP 4xx/5xx responses, network timeouts, and connection failures where `is_url_traffic_error=True` | `get_api()` |
| `<ClassName>_error.log` | All non-API-routed records at INFO and above: successful waves, parse failures, schema errors, and canal skips | `get_error()` |
| `<ClassName>_debug.log` | Superset of all records — every record that lands in `api.log` or `error.log` also lands here, plus DEBUG-floor lifecycle events | grep / external tooling |
| `<ClassName>_tide.log` | Tideweaver sessions only: every yielded `Tide` (fired and no-op), sorted by `tide_number` | `LoggedTideweaver.get_tides()` |

To get all rejects regardless of which file they landed in, use
`get_rejects()` — it unions `_api.log` + `_error.log` and returns only
records that carry a top-level `"reject"` key.

> **Why `_error.log` receives successful waves:** the file was originally
> named for failures, but it routes by `StandardFilter` (non-API records
> at INFO and above). Successful wave records are INFO-level and pass that
> filter. The distinction between success and failure within `_error.log`
> is the presence or absence of `wave.failed_sources`.

Rotation, queueing, and thread management are all internal. The cap on
concurrent background listeners (`MAX_LOG_THREADS`) is enforced
automatically — the oldest listener is evicted when a new class
subscribes.

---

## When to reach for which reader

| Situation | Use |
|---|---|
| Post-run audit — want all failures | `get_rejects()` — unions `_api.log` + `_error.log` |
| Retry only network/API failures | `get_api()` — URL-traffic errors only |
| Investigate parse/schema failures | `get_error()` — codebase errors only |
| Live observability during a `stream()` | `wave.failed_sources` off the live wave |
| Cross-process inspection (separate retry worker) | `get_rejects()` — the union is the contract |
| Classify a failure before retrying | `entry.is_url_traffic_error` on each `RejectEntry` |

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

Unlike `LoggedIncorporator`'s four files, `LoggedTideweaver` uses the
same file set but keyed by `logger_name` rather than class name. The
`logger_name` resolves in order: explicit `logger_name` constructor
argument, then `watershed.name`, then `"Tideweaver"`.

`get_rejects()` now unions `_error.log` + `_api.log` (same as
`LoggedIncorporator`) — it returns records with a top-level `"reject"`
key from both files. Canal-layer reject records (`PenstockLimited`,
`SurgeHalted`, `SkipAhead`, `GateBlocked`) route to `_error.log`;
verb-layer rejects from HTTP failures may route to `_api.log` depending
on `is_url_traffic_error`.

`get_scheduler_events()` reads `_error.log` filtered to records with a
top-level `"scheduler_event"` key. Watershed lifecycle events
(`watershed_started`, `watershed_completed`) and scheduler diagnostics
(`isolated_tick_failure`, `tick_parked`, `empty_output`,
`empty_parent_snapshot`, `fjord_flush_failure`) all land there at WARNING
or ERROR level.

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
| Watershed lifecycle events | `LoggedTideweaver.get_scheduler_events(logger_name)` — returns `watershed_started` / `watershed_completed` records plus scheduler diagnostics |
| Distinguish API vs codebase failures in verb-layer rejects | `entry.is_url_traffic_error` on each `RejectEntry`; check `_api.log` for URL-traffic errors |

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
| Full reader API signatures and routing model | [API Atlas — Observability layer](./api_atlas.md#observability-layer-loggedincorporator--loggedtideweaver) |
| Ship `LoggedIncorporator` pipelines with structured logs | [Deployment Guide](./deployment.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/docs/debugging.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
