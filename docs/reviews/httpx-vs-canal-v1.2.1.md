# Canal engineering ↔ httpx + tenacity — v1.2.x review

**Branch:** `code-review` at `59af8c9` (== `main`, == current worktree —
zero diff; the audit applies to the v1.2.1 codebase)
**Companion to:** the deleted `canal_evaluation.md` and
`canal_integration_audit.md` (recoverable from `eb5d96a^`); both shipped
under the `workflow` branch and informed the v1.2.0 → v1.2.1 cycle
**Date:** 2026-05-27

---

## 1. Summary

1. **Does the v1.2.x canal layer interact with httpx properly?** ✅ Yes.
   The unified `Penstock` primitive (v1.2.0 G item) is the deliberate
   architectural answer: one hierarchy, two call styles (async
   `acquire` for HTTP, sync `consume_reason` for the canal edge). Every
   HTTP call origin routes through `execute_request()` in
   [fetch.py](../../incorporator/io/fetch.py); no pipeline engine
   issues a raw `httpx.*` request. Pre-Penstock (`FixedIntervalThrottle`
   in the now-deleted `io/throttle.py`) and current `Penstock` share
   the same gate-before-request shape — no queue/scheduler hops added,
   no regression.

2. **Are there steps that were faster before Penstock/etc?** ❌ No.
   `canal_evaluation.md` recorded 8 `perf:` commits between
   `bdc7fa4..v1.2.0` (deque reservoir, adaptive heap-driven wakeup,
   AsyncClient pooling × 3, lazy ClassVar, pre-classified conv_dict ops,
   inc-closure caching) with no regressions. The v1.2.1 work added the
   `_BATCH_INSERT_MODE` gate (~100-200 ns/row saved on bulk inserts) and
   AIMD chunk_size adaptation. Throughput is monotone-improving or flat.

3. **Are there httpx function calls newer strategies could use?** Yes,
   one: **`httpx.AsyncClient.stream()` is unused everywhere.** All
   response bodies in [fetch.py](../../incorporator/io/fetch.py) are
   materialized via `.read()` / `.text`. The chunked-paginator path is
   fine (pages are already bounded), but `outflow` / `export` to
   streaming-friendly formats (NDJSON, CSV) holds the entire upstream
   response in memory before writing the first row to disk. **Finding 5**
   below proposes an opt-in `stream_to_path` parameter on
   `execute_request()` that solves this without a format-handler
   refactor. Other httpx features (`Limits`, `Timeout`,
   `event_hooks["response"]`, HTTP/2, `follow_redirects`,
   `raise_for_status`, typed exception boundary) are in active use.

4. **Async discipline + DSA / efficiency across imported libraries?**
   ✅ Confirmed clean. Zero `time.sleep` / `requests` / `httpx.Client(`
   / `asyncio.timeout` (3.11+) in the package. `deque`, `heapq`,
   `itertools.islice`, `functools.lru_cache`, `pydantic.TypeAdapter`
   memoization, and `asyncio.gather(return_exceptions=True)` are
   deployed where they pay. `WeakValueDictionary` strong-ref snapshots
   in the scheduler are intentional design, not a missed optimisation.
   Full verification at §5.

**Ten findings** total, three actionable code-change bundles (queued
for the `incorporator-code-orchestrator` chain), one
no-orchestrator-needed doc clarification, four doc-only / deferred
items. None of them are regression fixes.

---

## 2. What the prior audits already closed (v1.2.1 reference)

| Item | Source | Status |
|---|---|---|
| 10/10 scheduler ↔ canal hook coverage | `canal_integration_audit.md` §3 | ✅ verified again here |
| HTTP throttle path through unified Penstock | v1.2.0 G | ✅ confirmed at fetch.py:317 |
| A-F-1: canal rejects populate `RejectEntry` with `from_name` / `to_name` | v1.2.1 | ✅ at architect.py:1147 |
| A-F-3 + A-F-4: TypeAdapter benchmark + batched `list[Cls].validate_python` | v1.2.1 | ✅ |
| A-F-9: per-paginator Penstock composition | v1.2.1 | ✅ |
| `_tune_surge_threshold` rule | v1.2.1 | ✅ at architect.py:1241; filters `error_kind in ("SkipAhead", "SurgeHalted")` at architect.py:1258 |
| 8 `perf:` commits, no regressions | `canal_evaluation.md` §3 | ✅ |
| Python 3.9 dropped, PEP 585 / 604 rollout | v1.2.1 | ✅ |

This review picks up at the **deferred-items** list in AGENTS.md and
asks which are still open, which are httpx- or tenacity-shaped, and
which need action.

---

## 3. Per-primitive httpx-interaction sweep

The canal toolkit comprises six primitives plus FlowState. Each is
walked here against the question *does it touch httpx, where is the
seam, and what's still open?*

### 3.1 Gate (HardLock / SoftPass / Weir)

Reads `GateContext(up_in_flight, up_last_wave_at, last_consumed, now)`
to decide whether the downstream Current fires this pass. No httpx
seam — there is no HTTP-layer notion of "dependency on another
request," and httpx provides no analogue (ordering on the HTTP side is
expressed via async/await). Weir mode (v1.2.0) was the binary
hard/soft → trinary upgrade and is exercised by routing tests.
**No open finding.**

### 3.2 SurgeBarrier ("storm surge")

Trips when upstream in-flight tick exceeds `threshold_multiple ×
upstream.interval`. Actions: `skip` (→ `error_kind="SkipAhead"`),
`halt` (→ `"SurgeHalted"`), `bypass` (skips this edge's gate AND
penstock for the pass).

**Indirect but load-bearing httpx seam.** SurgeBarrier reads
`up_in_flight` from `SurgeContext`. When the upstream Current is an
HTTP-fetching Stream, that in-flight time is bounded above by httpx's
per-call `Timeout`. The relationship is not documented anywhere:

- **High httpx timeout (e.g. 60s), Stream interval 3s, threshold 2.0:**
  SurgeBarrier trips at 6s — well before httpx times out → downstream
  is bypassed → canal absorbs the slowdown.
- **Low httpx timeout (e.g. 2s), Stream interval 10s, threshold 2.0:**
  httpx times out at 2s → Stream emits an HTTP `RejectEntry` →
  in-flight drops back to 0 → SurgeBarrier never trips. Slow upstreams
  surface as HTTP rejects, not surge rejects.

Both behaviours are correct, but **the SurgeBarrier threshold and the
per-host httpx `Timeout` are implicit siblings.** Tuning advice that
ignores one is incomplete.

> **Finding 3 (doc only).** Document the SurgeBarrier↔httpx-Timeout
> relationship in this audit and in the `SurgeBarrier` docstring at
> [flow.py:132](../../incorporator/observability/tideweaver/flow.py).
> A future `_tune_*` rule could detect when `httpx_timeout > threshold
> × interval` and recommend re-tuning either knob.

A second SurgeBarrier finding emerged from the bypass spec:

> **Finding 4 (one-line docstring).** `SurgeBarrier(action="bypass")`
> skips this edge's *canal* gate and *canal* penstock for the pass. It
> does **NOT** skip the per-host HTTP `BoundPenstock` consulted at
> [fetch.py:317](../../incorporator/io/fetch.py). A user reading
> "bypass" as "skip all rate limiting" will be surprised. Correct
> architecture (the two penstock layers serve different purposes), but
> undocumented. Add one line to the `SurgeBarrier` docstring.

### 3.3 Penstock (unified hierarchy)

Six built-in subclasses (Null, Sustained, Burst, Window, Backpressure,
Signal). Same hierarchy serves both layers via two call styles: async
`acquire(state, lock)` for HTTP outbound, sync `consume_reason(state,
flow, now)` for the canal edge. Sits in front of `client.request()` at
[fetch.py:317](../../incorporator/io/fetch.py), gated via async lock.
Acquired exactly once per tenacity attempt (not per chunk). Tenacity is
`AsyncRetrying` so `retrying.statistics["attempt_number"]` populates
`RejectEntry.attempt_number` — a v1.2.1-locked invariant.

Two telemetry asymmetries between the HTTP path and the canal path:

> **Finding 1 (cooldown_sec).** HTTP rejects populate
> `RejectEntry.cooldown_sec` from `httpx.HTTPStatusError.response.headers["Retry-After"]`.
> Canal rejects leave it `None` even though the penstock has the
> information (next allowed time − now). Extend
> `Penstock.consume_reason` to return `(reason, cooldown_sec)`. Built-in
> computations:
> - `SustainedPenstock`: `1/rate - (now - last_consumed_at)`
> - `BurstPenstock`: time until next token (`(1 - tokens) / refill_rate` when below 1)
> - `WindowPenstock`: `oldest_window_entry + window - now`
> - `BackpressurePenstock`: recompute throttled rate; return `1/rate`
> - `NullPenstock` / `SignalPenstock`: `None`
>
> Third-party `Penstock` subclasses overriding `consume_reason` keep
> working via a default-argument shim that returns `None`.

> **Finding 2 (attempt_number + duration_sec).** HTTP rejects populate
> `attempt_number` (tenacity) and `duration_sec` (`perf_counter()`
> bracket). Canal rejects leave both `None`, which is exactly why
> `_tune_retry_policy` defensive-skips on canal rejects (documented in
> AGENTS.md). Closing this asymmetry unlocks the rule for canal data.
> Bookkeep synthetic `attempt_number=1` and a `duration_sec` measured
> from edge-eligible to skip-emit, attach to canal rejects in the 4
> scheduler skip sites.

### 3.4 Reservoir (per-edge `deque[depth=N]`)

Bounded wave history per scheduler edge. `collections.deque` with
explicit popleft + spillway hook on overflow. **No httpx seam today.**
The HTTP path has no notion of "bounded response history" — we don't
cache responses anywhere. F-6 (deferred) proposed adopting
Reservoir/Spillway at the `stream()` / `fjord()` verb layer; v1.2.1
shipped AIMD chunk_size adaptation as an alternative-mechanism
solution. **No open finding for this review.**

### 3.5 Spillway (DropOldest / RaiseOverflow / ExportToArchive)

Handles displaced waves when a Reservoir trims past `depth`. No httpx
interaction. Spillway operates on `Wave` objects, not HTTP responses.
ExportToArchive's strong-ref backlog (`archive_cls._spillway_backlog`)
is a scheduler-side artifact. **No open finding.**

### 3.6 FlowObserver (NullObserver / LoggingObserver / SignalObserver)

Four sync hooks fired by the scheduler: `on_fire`, `on_skip`,
`on_spillway`, `on_reservoir_level`. Per AGENTS.md, hooks must NOT
await — the scheduler does not await them.

**Parallel observer surface, unused on the HTTP side.** httpx ships its
own `event_hooks` (request, response). We use the response hook for
SSRF blocking at [fetch.py:137](../../incorporator/io/fetch.py); the
request hook is unused. The two surfaces serve different layers
(FlowObserver = tick events; httpx event_hooks = request lifecycle), so
unifying them would be miscategorising — they're not the same kind of
observer.

> **Finding 6 (flag only).** httpx's request event_hook is unused and
> would be a natural place to add HTTP-layer attempt-timing telemetry
> that currently lives in `_safe_execute`'s `perf_counter()` bracket.
> Lower priority than Findings 1, 2, 5, 7. Flag, do not act.

The A1 narrow-context leak (observer takes `scheduler: "Tideweaver"` as
its first positional arg) is deliberate per commit `47127ee` per
AGENTS.md; out of scope.

### 3.7 FlowState (per-edge mutable bookkeeping)

Composed into `_EdgeState.flow_state` (v1.2.0 i14). Tracks
`last_consumed_at`, `bucket_tokens`, `bucket_last_refill_at`,
`window_log`. The HTTP path has a parallel bookkeeping object via
`BoundPenstock`. Already covered under Penstock. **No standalone open
finding.**

### 3.8 Streaming — the one unused httpx feature

All response bodies in [fetch.py](../../incorporator/io/fetch.py) are
materialized via `.read()` / `.text`. For the chunked-paginator path
this is fine (pages are already bounded). For `outflow` / `export` to
streaming-friendly formats (NDJSON, CSV — both
`FormatType.is_append_safe`), it means we hold the entire upstream
response in memory before writing the first row to disk.

> **Finding 5 (separate session).** Add an opt-in `stream_to_path: Path
> | None = None` parameter to `execute_request()`. When set, the
> function calls `client.stream()` and forwards chunks to the path's
> open file handle, returning a sentinel response with empty body. The
> Penstock gate still fires exactly once per attempt — acquired before
> `client.stream()`, not per chunk. Tenacity retry semantics
> unchanged. Format handlers do not need to learn about streaming; the
> outflow engine writes to the file path directly, and the format
> handler reads the completed file from disk on the next chunk
> boundary. This avoids the stream-aware-format-handlers refactor that
> would otherwise gate the change.

---

## 4. Tenacity-interaction sweep

Tenacity is used in **two** places, not one — a fact AGENTS.md doesn't
mention. This corrects an assumption a Penstock-only audit would have
missed.

### 4.1 HTTP path — `fetch.py`

```python
# fetch.py:307
AsyncRetrying(
    stop=stop_after_attempt(8),
    wait=wait_random_exponential(multiplier=1.5, min=2, max=30),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    reraise=True,
)
```

Predicate retries on network errors and ALL HTTP status errors (5xx,
429, etc.). `IncorporatorNetworkError` (permanent 4xx, raised at
fetch.py:346) is NOT retried. `retrying.statistics["attempt_number"]`
extracted post-loop and attached to the raised exception as
`e._incorporator_attempt_number` — populates
`RejectEntry.attempt_number`. The v1.2.1-locked invariant.

### 4.2 Canal path — `scheduler.py` (previously unrecorded)

```python
# scheduler.py:670
AsyncRetrying(
    stop=stop_after_attempt(5),
    wait=wait_random_exponential(multiplier=1.0, min=0.5, max=8.0),
)
```

Wraps the tick body (restart-policy retry, distinct from inner HTTP
retries). Catch site at scheduler.py:685 handles `RetryError`.
**Crucially:** `retrying.statistics["attempt_number"]` is NOT extracted
on this path, so a tick that burns all 5 attempts emits a `RejectEntry`
with `attempt_number=None`.

> **Finding 7 (telemetry parity for the scheduler retry).** Port the
> fetch.py extraction pattern to scheduler.py:670-685: read
> `retrying.statistics["attempt_number"]` after the loop, attach to
> `RetryError` exception as `e._incorporator_attempt_number`, the
> reject-build path reads it the same way the HTTP path does. Sibling
> to Finding 2 but for the *tick-failure* path, not the *skip-emit*
> path.

### 4.3 Compound retry budget

Worst-case attempts on a single Tideweaver tick that drives an HTTP
fetch: **5 × 8 = 40 attempts**. The two budgets are independent and
may be intentional, but they're nowhere documented. With
`wait_random_exponential(max=30)` on the HTTP path and
`wait_random_exponential(max=8)` on the canal path, a pathological
run can spend `5 × (8 × 30) = 1200 s = 20 min` on a single tick before
surfacing.

> **Finding 9 (compound budget warning rule).** Add a third sub-rule
> to `_tune_retry_policy` that compares `outer_stop × inner_stop ×
> inner_wait_max` against the configured `pass_interval` and emits a
> HIGH-severity hint when the compound budget can exceed one pass.
> Reads existing `Tide` records for `pass_interval`, existing
> `RejectEntry` records for inner/outer attempt counts. No new
> framework knobs required.

### 4.4 `_tune_retry_policy` — only 2 sub-rules, not the full tenacity surface

Located at architect.py:1423-1507. Two sub-rules:

1. **`stop_after_attempt` ceiling detection** (lines 1451-1479): groups
   `HTTPStatusError` rejects by host; if >50% hit the max attempt
   count, emit MED hint to raise the ceiling.
2. **`wait_random_exponential` tuning** (lines 1481-1505): if median
   `duration_sec` ≈ median `cooldown_sec` (within 50%), emit LOW hint.

Cannot recommend `wait_chain` (tiered backoff per exception type),
`stop_after_delay` (wall-clock budget instead of attempt count), or
`retry_if_result` (retry on response-content predicate), because
`execute_request` doesn't currently *accept* those as configuration.
Recommending them is gated on first exposing them.

> **Finding 8 (deferred coupling).** `wait_chain` /
> `stop_after_delay` rules would benefit `_tune_retry_policy` but the
> framework needs to accept those as configuration first. Two-step:
> expose, then add rules. Defer unless a user requests these knobs.

### 4.5 Unused tenacity surface (confirmed clean)

Absent from the repo: `wait_chain`, `stop_after_delay`,
`retry_if_result`, `retry_if_not_exception_type`, `before_sleep`,
`before_sleep_log`, `after`, `retry_error_callback`. No sync
`Retrying()` calls — `AsyncRetrying` only.

> **Finding 10 (FlowObserver↔tenacity bridge, flag only).**
> `before_sleep` / `after` hooks are unused. Could plumb into a new
> `FlowObserver.on_retry(edge, attempt, exc)` hook so HTTP-layer
> retries become visible to canal observers. **However**, FlowObserver
> hooks must not await (AGENTS.md), and tenacity callbacks fire from
> inside the retry coroutine — the sync/async surface needs
> verification before this can be recommended. Flag, do not act.

---

## 5. Async discipline + DSA + library-feature usage — confirmed clean

Dedicated sweep with the lens *"after Tideweaver + canals shipped, is
async used properly everywhere, are best DSA / efficiency choices in
place within our imported libraries, and is no regression hiding?"*

### 5.1 Async discipline

| Pattern | Status | Evidence |
|---|---|---|
| `time.sleep(` in async paths | ✅ Zero | No hits in `incorporator/` |
| `requests.` / `urllib.request` | ✅ Zero | No hits |
| `httpx.Client(` (sync) | ✅ Zero | `AsyncClient` only |
| `threading.Lock` in async paths | ✅ Only `_counter_lock` at base.py:47 — used in synchronous `Incorporator.save()`, not async | |
| `asyncio.timeout` (3.11+) | ✅ Zero | `asyncio.wait_for` only at _shared.py:85, scheduler.py:406 |
| `BoundPenstock` lock lifecycle | ✅ Lazy construction inside coroutine at penstock.py:440-441 | Preserves 3.10 compatibility |

### 5.2 DSA / efficiency

| Pattern | Status | Evidence |
|---|---|---|
| `deque` for FIFO trims | ✅ `Reservoir.waves` at scheduler.py:73; `_ring` at chunked.py:65 | `maxlen` where appropriate |
| `heapq` for scheduler due-list | ✅ scheduler.py:383-433 (`_push_due`, `_run_pass`) | The "adaptive heap-driven wakeup" perf commit `ad9041f` |
| `itertools.islice` for paginator batching | ✅ local.py:259, 331 | |
| `list.pop(0)` antipatterns | ✅ Zero in hot paths | scheduler.py:77 has explicit comment about the deque choice |
| `list(snapshot)` materializations in scheduler hot path | ✅ Intentional — strong-ref WeakValueDictionary preservation | scheduler.py:740, 879, 927, 935 |
| `sorted()` inside hot loops | ✅ Only one, _outflow.py:424, telemetry-only, tiny set | |

### 5.3 Library-feature usage

| Library | Feature | Status | Evidence |
|---|---|---|---|
| functools | `lru_cache` | ✅ Deployed where it pays | converters.py:342 (4096), :359 (128), formats.py:205 (4096), compression.py:66 (4096) |
| pydantic | `TypeAdapter` memoization | ✅ Module-singleton via `_get_cached_adapter(lru_cache)` | converters.py:343-356 |
| pydantic | `model_construct()` | ✅ At all framework-internal write sites | fetch.py:78, chunked.py:111/149, fjord.py:118/345/361/377, scheduler.py:491/551/566/601/625, _shared.py:49/66, _outflow.py:431, _stateful_shim.py:102/119/134 |
| asyncio | `gather(return_exceptions=True)` | ✅ Wide use | fetch.py:728/775, fjord.py:292/309/483, scheduler.py:407 |
| asyncio | `TaskGroup` (3.11+) | ✅ Correctly skipped | 3.10 baseline |
| asyncio | `as_completed` | ✅ Correctly skipped | We need ALL results, not first-completion |
| weakref | `WeakValueDictionary` | ✅ `Incorporator.inc_dict` paired with scheduler snapshots | |

### 5.4 Locked invariants spot-check

- ✅ `AsyncRetrying` (not `@retry` decorator) at fetch.py:307
- ✅ `_BATCH_INSERT_MODE` gate at schema/factory.py:416, 420
- ✅ `_CURRENT_CHUNK_CLASS` ContextVar set/reset at chunked.py:94, 98

### 5.5 Verdict

After the v1.2.x Tideweaver + canal cycle, the codebase is
**async-discipline-compliant, DSA-conscious, and library-feature-maximal
within the Python 3.10 baseline.** No async-blocking violations, no DSA
antipatterns in hot paths, no regressions against the locked
invariants. The findings below are about *extending* telemetry parity
and adopting one unused httpx feature — they're not fixes for
regressions; they're additive.

---

## 6. Findings consolidated

| # | Primitive | Finding | Action |
|---|-----------|---------|--------|
| 1 | Penstock | `RejectEntry.cooldown_sec` is `None` on canal rejects | Extend `consume_reason` signature; populate at 4 scheduler skip sites |
| 2 | Penstock | `attempt_number` / `duration_sec` are `None` on canal rejects → `_tune_retry_policy` defensive-skips | Add synthetic counters in `_EdgeState`; populate alongside Finding 1 |
| 3 | SurgeBarrier | Threshold ↔ httpx Timeout relationship is undocumented | This doc; flag future tune rule |
| 4 | SurgeBarrier | `action="bypass"` bypasses canal penstock only, not HTTP per-host penstock | One-line docstring clarification |
| 5 | fetch.py | `httpx.stream()` is unused; all responses materialized | Opt-in `stream_to_path` parameter on `execute_request()` |
| 6 | FlowObserver | httpx request-event hook is unused | Flag only; do not act |
| 7 | Tenacity (scheduler) | Canal tick `AsyncRetrying` doesn't extract `attempt_number` | Port fetch.py extraction pattern to scheduler.py:670-685 |
| 8 | Tenacity (architect.tune) | `_tune_retry_policy` can't recommend `wait_chain` / `stop_after_delay` because framework doesn't expose them | Two-step: expose knobs first, then add rules. Defer until requested |
| 9 | Tenacity (architect.tune) | No warning when compound retry budget exceeds `pass_interval` | Add third sub-rule to `_tune_retry_policy`; pure addition |
| 10 | FlowObserver ↔ Tenacity | `before_sleep` / `after` callbacks unused | Flag; sync/async surface needs verification first |

### Bundle map (orchestrator-routed work)

- **Bundle A — Findings 1 + 2 + 7** (telemetry parity, single architect
  brief): touches penstock.py, scheduler.py, architect.py
- **Bundle B — Finding 4** (one-line SurgeBarrier docstring): can ride
  with Bundle A or stand alone
- **Bundle C — Finding 5** (streaming opt-in): separate architect brief
  (touches `execute_request` retry semantics)
- **Bundle D — Finding 9** (compound budget tune rule): pure architect.py
  addition, can ride with Bundle A
- **Findings 3, 6, 8, 10** captured in this doc; no code action

---

## 7. Out of scope (deferred items)

From AGENTS.md's deferred-items list, these stay deferred:

- **F-6 strict Reservoir/Spillway adoption in Stream/Fjord** — AIMD
  chunk_size shipped as the alternative-mechanism solution. No
  httpx-shaped work here.
- **FlowObserver A1 narrow-context leak** — deliberate per commit
  `47127ee`.
- **`flow.py` 685-LOC split** — navigation-only; no semantic change.
- **`Wave.bytes_processed` for non-chunked callers** — real telemetry
  gap, but not httpx-shaped (would need a different ContextVar
  pattern). Belongs in its own review.
- **Pydantic audit** — v1.2.1 already absorbed F-3/F-4; the next pass
  would find 5-10 new items and deserves its own session.

Also out of scope:

- **Rate-limit as `httpx.AsyncBaseTransport` / event hook.** The v1.2.0
  G item was the deliberate answer — Penstock unification gives one
  hierarchy across both layers. Moving the gate into a transport would
  un-unify it.
- **Shared `httpx.AsyncClient` across `incorp()` calls.** Different
  sources legitimately want different `Limits` / `Timeout`s; the
  per-fetch client lifetime is correct.
- **FlowObserver↔httpx-event-hook unification.** Different layers, not
  the same observer kind.
