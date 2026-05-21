"""The :class:`Tideweaver` orchestrator — async run-loop, dep gating, drain.

The scheduler walks the :class:`Watershed`'s topological order on every pass.
Each :class:`Current` ticks on its own interval; hard edges gate the dependent
until the upstream has emitted a new wave since the dependent last consumed.
Soft edges only sequence the in-pass order — no data wait.  Skip-ahead
short-circuits a dependent when its upstream's in-flight tick has been running
longer than ``skip_threshold * dependent.interval``.

Per-tick bodies live in this module too:

* :class:`Stream` → ``cls.stream(..., stateful_polling=False)`` chunking drain.
* :class:`Fjord` → "fjord flush": snapshot upstream registries, then delegate to
  :func:`incorporator.observability.pipeline._outflow.flush`, the shared
  per-class build-and-export primitive that the legacy ``_outflow_daemon``
  also uses.  Shape semantics (single-output list / multi-output dict,
  user-pre-declared classes vs. ``infer_dynamic_schema``) match
  :meth:`Incorporator.fjord` exactly.
* :class:`Export` → ``cls.export(...)``.

Restart policy via :mod:`tenacity` (already a dep).  ``"isolate"`` traps and
logs, ``"fail_watershed"`` re-raises.
"""

from __future__ import annotations

import asyncio
import heapq
import logging
import time
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Tuple, cast

import httpx
from pydantic import BaseModel, ConfigDict
from tenacity import AsyncRetrying, RetryError, stop_after_attempt, wait_random_exponential

from ...io.fetch import HTTPClientBuilder
from ..pipeline._outflow import flush
from .current import Current, Export, Fjord, Stream
from .tide import Tide
from .watershed import Watershed

logger = logging.getLogger(__name__)

TickFactory = Callable[[Current], Awaitable[None]]


class _CurrentState(BaseModel):
    """Per-current scheduler bookkeeping.

    Instances are pre-allocated for every current at :class:`Tideweaver`
    construction time and looked up by ``current.name`` in
    :attr:`Tideweaver._state`.  Replaces four parallel dicts
    (``_last_tick_started`` / ``_last_wave_at`` / ``_started_at`` /
    ``_inflight``) keyed by the same name with one struct per current.
    ``_last_consumed`` stays separate on :class:`Tideweaver` because its
    key is the *edge* tuple ``(dependent, upstream)``, not the current
    name.

    ``arbitrary_types_allowed=True`` is required so :attr:`in_flight`
    can hold an :class:`asyncio.Task` — Pydantic V2 doesn't have a
    native validator for it but accepts it as an opaque type when
    allowed.  The model is mutable (Pydantic default) so the scheduler's
    tick wrapper can update fields in place.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    last_tick_started: Optional[float] = None
    """Monotonic time the current's most recent tick was spawned, or None."""

    last_wave_at: Optional[datetime] = None
    """UTC time the current's most recent tick finished, or None."""

    started_at: Optional[float] = None
    """Monotonic time of the CURRENTLY in-flight tick.  Cleared when the
    tick completes — distinguishes "running now" from "ran before"."""

    in_flight: Optional[asyncio.Task[None]] = None
    """The asyncio Task for the most recent tick.  Persists after the
    task completes (``.done()`` returns True) so the scheduler can
    still inspect the finished task — matches today's semantics."""


class Tideweaver:
    """Orchestrate multiple Incorporator pipelines on independent intervals within one time window.

    Use it for windowed batch jobs (a 4-hour market session, a NASCAR
    race weekend), multi-source diamond dashboards that fuse parallel
    feeds into a single mark-to-market view, and dependency-gated
    workflows where a downstream stage must wait on fresh upstream
    data:

    .. code-block:: python

        watershed = Watershed.diamond(
            window=(start, end),
            head=binance_stream,
            middle=[coinbase_stream, kraken_stream],
            tail=arb_fjord,
        )
        async for tide in Tideweaver(watershed).run():
            log_tide(tide)

    Args:
        watershed: The :class:`Watershed` plan to run.
        tick_factory: Optional override for per-current tick bodies,
            used by tests to inject deterministic stubs.  Defaults to
            the production dispatch on :class:`Stream` / :class:`Fjord`
            / :class:`Export`.
        pass_interval: Seconds between scheduler passes.  Defaults to
            ``min(c.interval for c in currents) / 2`` clamped to
            ``[0.05, 1.0]``.

    Internally the scheduler walks the watershed's topological order
    each pass, tracking per-current last-tick times, in-flight tasks,
    and per-edge consumption watermarks.  Hard edges gate the
    dependent until the upstream emits a new wave since the dependent
    last consumed; soft edges only sequence the in-pass order.  A
    skip-ahead short-circuit drops a dependent when the upstream's
    in-flight tick has run longer than
    ``skip_threshold * dependent.interval``.
    """

    def __init__(
        self,
        watershed: Watershed,
        *,
        tick_factory: Optional[TickFactory] = None,
        pass_interval: Optional[float] = None,
    ) -> None:
        self.watershed = watershed
        self.tick_factory = tick_factory
        self.pass_interval = pass_interval or max(
            0.05,
            min(1.0, min(c.interval for c in watershed.currents) / 2.0),
        )

        # Per-current scheduler bookkeeping bundled into one struct per
        # current (see :class:`_CurrentState`).  Pre-allocated for every
        # current so reads in the gate / spawn / wrapper hot paths never
        # need ``dict.get(name, default)`` defensives — the entry always
        # exists.  ``_last_consumed`` stays a standalone dict because
        # its key is the edge tuple, not the current name.
        self._state: Dict[str, _CurrentState] = {c.name: _CurrentState() for c in watershed.currents}
        self._last_consumed: Dict[Tuple[str, str], datetime] = {}
        self._tide_number = 0
        self._currents_by_name: Dict[str, Current] = {c.name: c for c in watershed.currents}
        self._topo: List[str] = watershed.toposort()
        self._upstream: Dict[str, List[Tuple[str, str]]] = {c.name: [] for c in watershed.currents}
        for e in watershed.edges:
            self._upstream[e.to_name].append((e.from_name, e.mode))
        # Watershed topology is immutable for the run, so cache the
        # transitive-closure result per dependent.  Populated lazily by
        # ``_transitive_upstreams``.
        self._transitive_cache: Dict[str, List[str]] = {}

        # Adaptive-wakeup state.  The loop sleeps until the earliest of:
        # (a) the heap's next due-time, (b) ``_wake_event`` (set by any
        # ``_tick_wrapper.finally`` so downstream hard-edge dependents
        # re-evaluate immediately after an upstream wave lands), or
        # (c) shutdown.  Heap entries are ``(due_at_monotonic, counter,
        # name)`` triples — the counter is a tiebreaker so heapq never
        # tries to compare ``Current`` names lexicographically when two
        # entries share the same due time.
        self._wake_event: asyncio.Event = asyncio.Event()
        self._due_heap: List[Tuple[float, int, str]] = []
        self._heap_counter: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> AsyncIterator[Tide]:
        """Enter the orchestration loop — one async iteration per scheduler pass until the window closes.

        Each yielded :class:`Tide` carries the names of currents that
        ``fired`` this pass, ``(name, reason)`` pairs for currents that
        were ``skipped`` (gated by interval or upstream wait), and the
        pass ``duration_sec``.  When the watershed window's end is
        reached the loop drains in-flight ticks (bounded by
        ``watershed.drain_timeout``) and then exits cleanly.
        """
        # Across-drain HTTP client pool — keyed by the HTTP-config tuple
        # so two currents with identical config share one client while
        # currents with distinct configs each get their own.  Initialised
        # here (not in ``__init__``) so the Tideweaver instance is safely
        # reusable; each ``run()`` invocation gets a fresh pool that the
        # ``finally`` block below ``aclose()``s.
        self._client_pool: Dict[Tuple[Any, ...], httpx.AsyncClient] = {}
        shutdown_event = asyncio.Event()
        stopper = asyncio.create_task(self._shutdown_at_window_end(shutdown_event))
        try:
            while not shutdown_event.is_set():
                tide = await self._run_pass(shutdown_event)
                yield tide
                if shutdown_event.is_set():
                    break
                await self._wait_for_next_event(shutdown_event)
        finally:
            await self._drain()
            stopper.cancel()
            try:
                await stopper
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001
                logger.debug("Tideweaver: shutdown stopper exited unexpectedly: %s", exc)
            # Close pooled HTTP clients AFTER ``_drain()`` has settled
            # any in-flight ticks — so no tick is mid-request on a
            # client we're about to close.
            for client in self._client_pool.values():
                await client.aclose()
            self._client_pool.clear()

    async def _wait_for_next_event(self, shutdown_event: asyncio.Event) -> None:
        """Adaptive sleep until the next interesting moment.

        Wakes on the earliest of:
          * The heap's next-due-time (timer event from a previous tick).
          * ``self._wake_event`` (set by any ``_tick_wrapper.finally`` —
            so downstream hard-edge dependents re-evaluate as soon as an
            upstream wave lands, without polling).
          * ``shutdown_event`` (window close).

        When the heap is empty we fall back to ``self.pass_interval`` as a
        safety-net cap so any bug that orphans a current still gets a
        re-check within bounded time.  This preserves the
        one-Tide-per-pass contract — every wake produces exactly one
        :meth:`_run_pass` iteration.
        """
        next_due = self._due_heap[0][0] if self._due_heap else None
        now = time.monotonic()
        if next_due is None:
            timeout: float = self.pass_interval
        else:
            timeout = max(0.0, next_due - now)
        if timeout <= 0.0:
            # Heap already expired — re-pass immediately.  Still clear the
            # wake event so a stale set() from earlier doesn't no-op the
            # next sleep.
            self._wake_event.clear()
            return
        shutdown_waiter = asyncio.create_task(shutdown_event.wait())
        wake_waiter = asyncio.create_task(self._wake_event.wait())
        try:
            await asyncio.wait(
                [shutdown_waiter, wake_waiter],
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            self._wake_event.clear()
            for t in (shutdown_waiter, wake_waiter):
                if not t.done():
                    t.cancel()

    def _push_due(self, name: str, due_at: float) -> None:
        """Push a ``(due_at, counter, name)`` heap entry for an adaptive wake."""
        self._heap_counter += 1
        heapq.heappush(self._due_heap, (due_at, self._heap_counter, name))

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------

    async def _shutdown_at_window_end(self, shutdown_event: asyncio.Event) -> None:
        end = self.watershed.window[1]
        delay = max(0.0, (end - datetime.now(timezone.utc)).total_seconds())
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        shutdown_event.set()

    async def _drain(self) -> None:
        pending = [s.in_flight for s in self._state.values() if s.in_flight is not None and not s.in_flight.done()]
        if not pending:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True),
                timeout=self.watershed.drain_timeout,
            )
        except asyncio.TimeoutError:
            for t in pending:
                if not t.done():
                    t.cancel()

    # ------------------------------------------------------------------
    # Pass-level scheduling
    # ------------------------------------------------------------------

    async def _run_pass(self, shutdown_event: asyncio.Event) -> Tide:
        self._tide_number += 1
        started = time.monotonic()
        fired: List[str] = []
        skipped: List[Tuple[str, str]] = []

        # Housekeeping: drop heap entries whose due-time has already
        # passed.  The pass that runs at the due moment is the
        # authoritative re-check; subsequent stale entries for the same
        # name are harmless but discarding them keeps the heap small.
        while self._due_heap and self._due_heap[0][0] <= started:
            heapq.heappop(self._due_heap)

        for name in self._topo:
            if shutdown_event.is_set():
                break
            current = self._currents_by_name[name]
            existing = self._state[name].in_flight
            if existing is not None and not existing.done():
                skipped.append((name, "still_running"))
                continue
            reason = self._gate_reason(current, time.monotonic())
            if reason is not None:
                skipped.append((name, reason))
                continue
            self._spawn_tick(current)
            fired.append(name)
            # Schedule the adaptive wake for this current's next interval.
            # Data-gated skips don't need an entry — ``_wake_event`` fires
            # when their upstream tick completes — but firing here covers
            # the steady-state where each current re-fires on its own
            # cadence.
            self._push_due(name, time.monotonic() + current.interval)

        duration = time.monotonic() - started
        return Tide(tide_number=self._tide_number, fired=fired, skipped=skipped, duration_sec=duration)

    def _gate_reason(self, current: Current, now: float) -> Optional[str]:
        """Return ``None`` if ``current`` may fire; else a short skip reason."""
        last = self._state[current.name].last_tick_started
        if last is not None and (now - last) < current.interval:
            return "not_due"
        for up_name, mode in self._upstream[current.name]:
            up_state = self._state[up_name]
            in_flight = up_state.in_flight
            if in_flight is not None and not in_flight.done():
                started = up_state.started_at
                elapsed = now - (started if started is not None else now)
                if elapsed > current.skip_threshold * current.interval:
                    return "skip_ahead"
                if mode == "hard":
                    return "awaiting_upstream"
            if mode == "hard":
                upstream_wave = up_state.last_wave_at
                if upstream_wave is None:
                    return "awaiting_upstream"
                consumed = self._last_consumed.get((current.name, up_name))
                if consumed is not None and upstream_wave <= consumed:
                    return "awaiting_upstream"
        return None

    def _spawn_tick(self, current: Current) -> None:
        now_mono = time.monotonic()
        state = self._state[current.name]
        state.last_tick_started = now_mono
        state.started_at = now_mono
        # Capture upstream consumption BEFORE the tick runs so a fast upstream
        # finishing concurrently doesn't double-count its wave.
        #
        # ``consumed_snapshot`` is threaded as a positional arg deliberately —
        # a :class:`contextvars.ContextVar` alternative was evaluated and
        # rejected: the dict construction is microsecond-class, and a
        # ContextVar would silently leak if a future retry mechanism stops
        # preserving task context (the current ``tenacity.AsyncRetrying``
        # does preserve it, but that's an implementation detail of the
        # ``on_error="restart"`` policy in ``_tick_wrapper`` below).
        consumed_snapshot: Dict[str, datetime] = {}
        for up_name, _mode in self._upstream[current.name]:
            up_wave = self._state[up_name].last_wave_at
            if up_wave is not None:
                consumed_snapshot[up_name] = up_wave
        task = asyncio.create_task(self._tick_wrapper(current, consumed_snapshot))
        state.in_flight = task

    async def _tick_wrapper(
        self,
        current: Current,
        consumed_snapshot: Dict[str, datetime],
    ) -> None:
        """Run one tick under the current's :attr:`on_error` policy."""
        try:
            if current.on_error == "restart":
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(5),
                    wait=wait_random_exponential(multiplier=1.0, min=0.5, max=8.0),
                    reraise=True,
                ):
                    with attempt:
                        await self._invoke_tick(current)
            elif current.on_error == "isolate":
                try:
                    await self._invoke_tick(current)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Tideweaver: isolated tick failure on %s: %s", current.name, exc)
            else:  # fail_watershed
                await self._invoke_tick(current)
        except (Exception, RetryError):
            if current.on_error == "fail_watershed":
                raise
            logger.error("Tideweaver: tick failed for %s after retries; current parked.", current.name)
        finally:
            # Record the wave timestamp + bump consumed for hard upstreams.
            wave_at = datetime.now(timezone.utc)
            state = self._state[current.name]
            state.last_wave_at = wave_at
            for up_name, value in consumed_snapshot.items():
                self._last_consumed[(current.name, up_name)] = value
            # Also bump consumed for any upstream that produced a wave during this tick.
            for up_name, _mode in self._upstream[current.name]:
                latest = self._state[up_name].last_wave_at
                if latest is not None:
                    self._last_consumed[(current.name, up_name)] = latest
            state.started_at = None
            # Wake the run loop so downstream hard-edge dependents see the
            # new wave on the very next pass instead of waiting out the
            # full ``pass_interval`` safety-net cap.
            self._wake_event.set()

    async def _invoke_tick(self, current: Current) -> None:
        if self.tick_factory is not None:
            await self.tick_factory(current)
            return
        if isinstance(current, Stream):
            await self._tick_stream(current)
        elif isinstance(current, Fjord):
            await self._tick_fjord(current)
        elif isinstance(current, Export):
            await self._tick_export(current)
        else:
            raise NotImplementedError(
                f"Tideweaver has no default tick body for bare Current; "
                f"subclass Stream/Fjord/Export or pass a tick_factory.  Got: {type(current).__name__}"
            )

    # ------------------------------------------------------------------
    # HTTP client pool helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _client_pool_key(incorp_params: Dict[str, Any]) -> Tuple[Any, ...]:
        """Build the hashable key for one HTTP-client config bundle.

        Mirrors the five kwargs ``HTTPClientBuilder.build_client`` consumes
        (also used by ``fetch.py:579-585``).  Headers is converted to a
        ``frozenset`` because dicts are unhashable.
        """
        headers = incorp_params.get("headers")
        headers_frozen = frozenset(headers.items()) if headers else frozenset()
        return (
            incorp_params.get("timeout", 15.0),
            incorp_params.get("ignore_ssl", False),
            incorp_params.get("concurrency_limit", 50),
            incorp_params.get("block_internal_redirects", False),
            headers_frozen,
        )

    def _get_or_create_client(self, incorp_params: Dict[str, Any]) -> httpx.AsyncClient:
        """Look up — or lazily build — the pooled client for this config bundle."""
        key = self._client_pool_key(incorp_params)
        client = self._client_pool.get(key)
        if client is None:
            client = HTTPClientBuilder.build_client(
                concurrency_limit=incorp_params.get("concurrency_limit", 50),
                ignore_ssl=incorp_params.get("ignore_ssl", False),
                timeout=incorp_params.get("timeout", 15.0),
                headers=incorp_params.get("headers"),
                block_internal_redirects=incorp_params.get("block_internal_redirects", False),
            )
            self._client_pool[key] = client
        return client

    # ------------------------------------------------------------------
    # Per-current tick bodies
    # ------------------------------------------------------------------

    async def _tick_stream(self, current: Stream) -> None:
        """One chunking-mode drain of ``cls.stream(...)``.

        Incorporator's ``inc_dict`` is a ``WeakValueDictionary`` — without an
        external strong reference, instances die before a downstream Fjord
        flush can read them.  We park a strong-ref snapshot on the class as
        ``_tideweaver_snapshot`` so the registry stays alive between ticks;
        the Fjord flush reads through to that attribute when present.

        The snapshot must be accumulated WHILE the chunked engine is still
        iterating, not after.  The chunked engine ``del``s its per-chunk
        ``dataset`` local right after each yield, so by the time the ``async
        for`` exits the only strong refs to the just-streamed instances are
        gone and ``inc_dict.values()`` is empty.  Pinning instances into the
        ``accumulated`` dict at each wave boundary keeps them alive into the
        next chunk and across the final yield.
        """
        incorp_params = current.incorp_params
        # File-mode incorps don't touch HTTP — skip client injection.
        # HTTP-mode currents get a pooled client keyed by their HTTP
        # config (timeout / ignore_ssl / concurrency_limit / headers /
        # block_internal_redirects).  Two currents with identical config
        # share one client; distinct configs each get their own — matches
        # the per-current flexibility requirement.  Shallow-copy avoids
        # mutating the user's ``Current.incorp_params``.
        if bool(incorp_params.get("inc_file")):
            params_with_client = incorp_params
        else:
            pooled = self._get_or_create_client(incorp_params)
            params_with_client = {**incorp_params, "_client": pooled}
        kwargs: Dict[str, Any] = {
            "incorp_params": params_with_client,
            "poll_interval": None,
            "stateful_polling": False,
        }
        if current.refresh_params is not None:
            kwargs["refresh_params"] = current.refresh_params
        if current.export_params is not None:
            kwargs["export_params"] = current.export_params
        inflow = current.inflow or self.watershed.inflow
        if inflow is not None:
            kwargs["inflow"] = inflow
        accumulated: Dict[Any, Any] = {}
        async for _wave in current.cls.stream(**kwargs):
            accumulated.update(current.cls.inc_dict)
        # Strong-ref snapshot — keeps the WeakValueDictionary entries alive.
        # Runtime-only escape-hatch attribute (no field on Incorporator itself).
        # ``_outflow.py:flush`` parks the same attribute on Fjord output
        # classes, so downstream readers walk Stream and Fjord upstreams
        # uniformly via ``getattr(dep.cls, "_tideweaver_snapshot", None)``.
        cls_any = cast(Any, current.cls)
        cls_any._tideweaver_snapshot = list(accumulated.values())

    async def _tick_fjord(self, current: Fjord) -> None:
        """One fjord flush: snapshot upstream → outflow(state) → build → export.

        Delegates the outflow → normalize → per-class build/export to the
        shared :func:`incorporator.observability.pipeline._outflow.flush`
        generator (also used by the legacy ``_outflow_daemon``).  The
        scheduler's job is just snapshotting the upstream state and logging
        per-class failures; the outflow primitive owns the rest.
        """
        from ...usercode import load_outflow_module

        outflow_path = current.outflow or self.watershed.outflow
        if outflow_path is None:
            raise ValueError(
                f"Fjord current {current.name!r} requires an outflow= path (per-current or watershed-level)."
            )
        outflow_fn, outflow_module = load_outflow_module(outflow_path)

        # Snapshot upstream Currents' Incorporator class registries.
        # Walks the *transitive* upstream closure so a Fjord flush in a
        # diamond/chain sees state from every source that feeds it, not just
        # its direct edges.  This matches user intuition: "give my outflow
        # everything upstream" instead of "give me what my immediate parents
        # processed."
        upstream_names = self._transitive_upstreams(current.name)
        deps = [self._currents_by_name[up_name] for up_name in upstream_names]
        state: Dict[str, List[Any]] = {}
        for dep in deps:
            # Prefer the chunking-mode strong-ref snapshot if the upstream is a
            # Stream; fall back to live inc_dict (which works for sources that
            # naturally hold strong refs).
            snapshot = getattr(dep.cls, "_tideweaver_snapshot", None)
            if snapshot is not None:
                state[dep.cls.__name__] = list(snapshot)
            else:
                state[dep.cls.__name__] = list(dep.cls.inc_dict.values())

        async for derived_name, _count, err in flush(
            outflow_fn,
            state,
            default_output_class_name=current.cls.__name__,
            base_class=current.cls,
            export_params=current.export_params,
            outflow_module=outflow_module,
        ):
            if err is not None:
                logger.warning(
                    "Tideweaver: Fjord flush %r raised on derived class %r: %s",
                    current.name,
                    derived_name,
                    err,
                )

    async def _tick_export(self, current: Export) -> None:
        """One ``cls.export(...)`` call against the upstream class's registry.

        :meth:`Incorporator.export` requires ``instance`` as a keyword-only
        argument with no default. The Export current's intent (per its
        docstring) is to snapshot ``cls.inc_dict`` to disk, so this tick
        body resolves ``instance`` itself: prefer the strong-ref snapshot
        parked by an upstream :class:`Stream` (``_tideweaver_snapshot``);
        otherwise fall back to ``cls.inc_dict.values()`` for sources that
        naturally hold strong refs.
        """
        snapshot = getattr(current.cls, "_tideweaver_snapshot", None)
        if snapshot is not None:
            instance = list(snapshot)
        else:
            instance = list(current.cls.inc_dict.values())
        if not instance:
            return
        params = dict(current.export_params)
        params.setdefault("instance", instance)
        await current.cls.export(**params)

    # ------------------------------------------------------------------
    # Graph utilities
    # ------------------------------------------------------------------

    def _transitive_upstreams(self, name: str) -> List[str]:
        """Return the set of names reachable upstream from ``name`` (excluding self).

        Order is topological among the reachable ancestors so downstream
        consumers (e.g. logging) see a stable shape.  Memoised on
        ``self._transitive_cache``; the watershed topology is immutable
        for the run.
        """
        cached = self._transitive_cache.get(name)
        if cached is not None:
            return cached
        seen: set[str] = set()
        stack = [up_name for up_name, _mode in self._upstream[name]]
        while stack:
            up = stack.pop()
            if up in seen:
                continue
            seen.add(up)
            stack.extend(grand for grand, _mode in self._upstream[up] if grand not in seen)
        result = [n for n in self._topo if n in seen]
        self._transitive_cache[name] = result
        return result
