"""The :class:`Tideweaver` orchestrator — async run-loop, dep gating, drain.

The scheduler walks the :class:`Watershed`'s topological order on every pass.
Each :class:`Current` ticks on its own interval; each inbound edge's
:class:`~.flow.FlowControl` decides whether the dependent fires this pass.
``HardLock`` blocks until the upstream has emitted a new wave since the
dependent last consumed; ``SoftPass`` is fire-and-forget; ``Weir`` is the
middle ground — gates on freshness but doesn't block on in-flight upstream.
``SurgeBarrier`` (when configured on an edge) overrides the gate when
upstream has been in-flight longer than ``threshold_multiple * dependent.interval``,
producing ``"skip_ahead"`` (action ``"skip"``), ``"surge_halted"``
(action ``"halt"``), or letting the dependent fire (action ``"bypass"``).

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
from collections import deque
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Deque,
    Dict,
    FrozenSet,
    List,
    Optional,
    Set,
    Tuple,
    cast,
)

if TYPE_CHECKING:
    from ..wave import Wave
    from .architect import TuningReport

import httpx
from pydantic import BaseModel, ConfigDict, Field
from tenacity import AsyncRetrying, RetryError, stop_after_attempt, wait_random_exponential

from ...io.fetch import HTTPClientBuilder
from ...io.penstock import FlowState
from ...rejects import RejectEntry
from ..pipeline._outflow import flush
from .current import Current, CustomCurrent, Export, Fjord, Stream
from .current_outcome import CurrentOutcome
from .flow import FlowControl, GateContext, SurgeContext
from .tide import Tide
from .watershed import Watershed

logger = logging.getLogger(__name__)

TickFactory = Callable[[Current], Awaitable[None]]


class _EdgeState(BaseModel):
    """Per-edge scheduler bookkeeping, keyed by ``(from_name, to_name)``."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    waves: Deque[List[Any]] = Field(default_factory=deque)
    """FIFO history of wave snapshots; each entry is one tick's instance list.

    ``collections.deque`` (not ``list``) — appends are O(1) and the trim
    loop's eviction uses ``popleft()`` instead of ``list.pop(0)``, which
    is O(n).  The reservoir's ``depth`` cap is enforced by the scheduler
    around the append site rather than via ``maxlen`` because the spillway
    needs the displaced wave passed to ``overflow()`` before it's gone.
    """

    overflow_count: int = 0
    """Cumulative count of waves displaced from the reservoir (for diagnostics)."""

    flow_state: FlowState = Field(default_factory=FlowState)
    """Mutable counters owned by the edge's :class:`Penstock`.

    Carries the per-edge rate-limit bookkeeping:
    ``last_consumed_at`` (monotonic watermark, set by the
    ``_tick_wrapper.finally`` block on every successful consumption),
    ``bucket_tokens`` / ``bucket_last_refill_at``
    (:class:`~incorporator.io.penstock.BurstPenstock`), and
    ``window_log``
    (:class:`~incorporator.io.penstock.WindowPenstock`).  See
    :class:`incorporator.io.penstock.FlowState`.

    Composed (not inlined) as of i14 so the same shape can serve both
    HTTP throttling (via ``BoundPenstock``) and edge throttling (via
    ``_EdgeState``) without duplicating the field set."""


class _CurrentState(BaseModel):
    """Per-current scheduler bookkeeping, keyed by ``current.name`` in :attr:`Tideweaver._state`."""

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
    still inspect the finished task for restart / error-isolation logic."""


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
    and per-edge consumption watermarks.  Each inbound edge's
    :class:`~.flow.FlowControl` decides the pass/hold question; a
    per-edge :class:`~.flow.SurgeBarrier` can override the gate when
    upstream is in-flight beyond the configured threshold.
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

        self._state: Dict[str, _CurrentState] = {c.name: _CurrentState() for c in watershed.currents}
        # ``_last_consumed`` keys on edge tuples, not current names — so it's
        # a standalone dict instead of a field on ``_CurrentState``.
        self._last_consumed: Dict[Tuple[str, str], datetime] = {}
        self._tide_number = 0
        # Canal-layer skip records accumulated across the run.  Populated at
        # the four ``_gate_reason`` skip-emit sites (penstock-limited,
        # surge-halted, surge skip-ahead, gate-blocked); exposed via the
        # ``rejects`` property.  Parallel to ``IncorporatorList.rejects``
        # at the verb layer — gives callers a structured DLQ view of every
        # canal skip that never reached a tick body.
        self._canal_rejects: List[RejectEntry] = []
        self._currents_by_name: Dict[str, Current] = {c.name: c for c in watershed.currents}
        self._topo: List[str] = watershed.toposort()
        self._upstream: Dict[str, List[Tuple[str, FlowControl]]] = {c.name: [] for c in watershed.currents}
        self._downstream: Dict[str, List[Tuple[str, FlowControl]]] = {c.name: [] for c in watershed.currents}
        self._edge_state: Dict[Tuple[str, str], _EdgeState] = {}
        for e in watershed.edges:
            self._upstream[e.to_name].append((e.from_name, e.flow))
            self._downstream[e.from_name].append((e.to_name, e.flow))
            self._edge_state[(e.from_name, e.to_name)] = _EdgeState()
        self._transitive_cache: Dict[str, List[str]] = {}

        # Heap entries are ``(due_at_monotonic, counter, name)`` — counter
        # tiebreaks so heapq never compares Current names lexicographically.
        self._wake_event: asyncio.Event = asyncio.Event()
        self._due_heap: List[Tuple[float, int, str]] = []
        self._heap_counter: int = 0
        self._run_started_at: Optional[float] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def rejects(self) -> List[RejectEntry]:
        """Canal-layer skips accumulated during the run, as structured records.

        Parallel to :attr:`incorporator.IncorporatorList.rejects` at the
        verb layer — same shape (``RejectEntry`` with ``source``,
        ``error_kind``, ``message``, ``wave_index``), different origin.
        Where ``IncorporatorList.rejects`` tracks HTTP failures + fjord
        seed errors, ``Tideweaver.rejects`` tracks **scheduler-level
        skips** that never reached a tick body:

        * ``"PenstockLimited"`` — edge penstock blocked consumption
          (e.g. SustainedPenstock at-rate-ceiling, BurstPenstock empty).
        * ``"SurgeHalted"`` — :class:`SurgeBarrier` with ``action="halt"``
          tripped while upstream was over the threshold.
        * ``"SkipAhead"`` — :class:`SurgeBarrier` with ``action="skip"``
          tripped, deferring this pass.
        * ``"GateBlocked"`` — gate returned a non-transient block reason
          (e.g. ``"awaiting_upstream"`` is *excluded* as a normal
          transient state — upstream not yet emitted, in-flight, or
          already consumed this wave; only post-startup gate blocks
          land here).

        Returns a defensive copy — callers can mutate the list without
        affecting the scheduler's accumulator.
        """
        return list(self._canal_rejects)

    def summary(
        self,
        *,
        tides: Optional[List[Tide]] = None,
        waves: Optional[List["Wave"]] = None,
    ) -> "TuningReport":
        """End-of-run convenience: feed accumulated rejects, tides, and waves to :func:`architect.tune`.

        Collects :attr:`rejects` from the scheduler and passes them to
        :func:`~incorporator.observability.tideweaver.architect.tune`
        alongside any tides and waves supplied by the caller.  The
        scheduler's ``pass_interval`` is forwarded automatically.

        Args:
            tides: Tide records collected during the run (e.g. by
                appending each yielded :class:`Tide` inside the
                ``async for`` loop).  ``None`` is treated as an empty
                list.
            waves: Wave records from any upstream Stream currents,
                collected in the same run.  ``None`` is treated as an
                empty list.

        Returns:
            A :class:`~incorporator.observability.tideweaver.architect.TuningReport`
            with structured tuning hints for this run.
        """
        from .architect import tune  # lazy — avoids module cycle

        return tune(
            rejects=self.rejects,
            tides=tides,
            waves=waves,
            pass_interval=self.pass_interval,
        )

    async def run(self) -> AsyncIterator[Tide]:
        """Enter the orchestration loop — one async iteration per scheduler pass until the window closes.

        Each yielded :class:`Tide` carries the names of currents that
        ``fired`` this pass, ``(name, reason)`` pairs for currents that
        were ``skipped`` (gated by interval or upstream wait), and the
        pass ``duration_sec``.  When the watershed window's end is
        reached the loop drains in-flight ticks (bounded by
        ``watershed.drain_timeout``) and then exits cleanly.
        """
        # HTTP client pool + phase-offset anchor reset per-run, so a
        # Tideweaver instance is safely reusable across ``run()`` calls.
        self._client_pool: Dict[Tuple[Any, ...], httpx.AsyncClient] = {}
        self._run_started_at = time.monotonic()
        for c in self.watershed.currents:
            if c.phase_offset_sec > 0.0:
                self._push_due(c.name, self._run_started_at + c.phase_offset_sec)
        shutdown_event = asyncio.Event()
        stopper = asyncio.create_task(self._shutdown_at_window_end(shutdown_event))
        wake_reason = "startup"
        try:
            while not shutdown_event.is_set():
                tide = await self._run_pass(shutdown_event, wake_reason)
                yield tide
                if shutdown_event.is_set():
                    break
                wake_reason = await self._wait_for_next_event(shutdown_event)
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

    async def _wait_for_next_event(self, shutdown_event: asyncio.Event) -> str:
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

        Returns:
            The wake reason: ``"timer"`` (heap due-time elapsed),
            ``"wake_event"`` (upstream tick completed), ``"pass_interval"``
            (safety-net timeout with empty heap), or ``"shutdown"``
            (window-close event fired).
        """
        next_due = self._due_heap[0][0] if self._due_heap else None
        now = time.monotonic()
        if next_due is None:
            timeout: float = self.pass_interval
            fallback_reason = "pass_interval"
        else:
            timeout = max(0.0, next_due - now)
            fallback_reason = "timer"
        if timeout <= 0.0:
            # Heap already expired — re-pass immediately.  Still clear the
            # wake event so a stale set() from earlier doesn't no-op the
            # next sleep.
            self._wake_event.clear()
            return "timer"
        shutdown_waiter = asyncio.create_task(shutdown_event.wait())
        wake_waiter = asyncio.create_task(self._wake_event.wait())
        wake_reason = fallback_reason
        try:
            done, _ = await asyncio.wait(
                [shutdown_waiter, wake_waiter],
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if shutdown_waiter in done:
                wake_reason = "shutdown"
            elif wake_waiter in done:
                wake_reason = "wake_event"
            # else: timed out → fallback_reason already set
        finally:
            self._wake_event.clear()
            for t in (shutdown_waiter, wake_waiter):
                if not t.done():
                    t.cancel()
        return wake_reason

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

    async def _run_pass(self, shutdown_event: asyncio.Event, wake_reason: str) -> Tide:
        self._tide_number += 1
        started = time.monotonic()
        fired: List[str] = []
        skipped: List[Tuple[str, str]] = []
        outcomes: List[CurrentOutcome] = []
        rejects_before = len(self._canal_rejects)
        in_flight_at_start = sum(1 for s in self._state.values() if s.in_flight is not None and not s.in_flight.done())

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
            state = self._state[name]
            existing = state.in_flight
            if existing is not None and not existing.done():
                skipped.append((name, "still_running"))
                outcomes.append(
                    CurrentOutcome(
                        name=name,
                        status="still_running",
                        in_flight_sec=(time.monotonic() - state.started_at) if state.started_at is not None else None,
                        last_wave_at=state.last_wave_at,
                    )
                )
                continue
            reason, bypassed = self._gate_reason(current, time.monotonic())
            if reason is not None:
                skipped.append((name, reason))
                outcomes.append(
                    CurrentOutcome(
                        name=name,
                        status="skipped",
                        reason=reason,
                        bypassed_edges=tuple(bypassed),
                        last_wave_at=state.last_wave_at,
                    )
                )
                continue
            self._spawn_tick(current, bypassed)
            fired.append(name)
            outcomes.append(
                CurrentOutcome(
                    name=name,
                    status="fired",
                    bypassed_edges=tuple(bypassed),
                    last_wave_at=state.last_wave_at,
                )
            )
            # Schedule the adaptive wake for this current's next interval.
            # Data-gated skips don't need an entry — ``_wake_event`` fires
            # when their upstream tick completes — but firing here covers
            # the steady-state where each current re-fires on its own
            # cadence.
            self._push_due(name, time.monotonic() + current.interval)

        pass_end = time.monotonic()
        duration = pass_end - started
        # Use ``pass_end`` (not ``started``) as the reference: the topo walk
        # took ``duration`` seconds, and consumers want time-until-next-pass
        # from "now," not from pass start.  Always positive when non-None
        # because the stale-pop loop above discarded entries with
        # ``due <= started`` (and any new entries pushed during the walk
        # have ``due = monotonic + interval > pass_end`` by construction).
        next_due_in_sec = (self._due_heap[0][0] - pass_end) if self._due_heap else None
        return Tide.model_construct(
            tide_number=self._tide_number,
            fired=fired,
            skipped=skipped,
            current_outcomes=outcomes,
            duration_sec=duration,
            wake_reason=wake_reason,
            heap_depth=len(self._due_heap),
            in_flight_count_at_start=in_flight_at_start,
            canal_rejects_added=len(self._canal_rejects) - rejects_before,
            next_due_in_sec=next_due_in_sec,
            timestamp=datetime.now(timezone.utc),
        )

    def _gate_reason(self, current: Current, now: float) -> Tuple[Optional[str], FrozenSet[str]]:
        """Return ``(None, bypassed)`` if ``current`` may fire; else ``(reason, set())``.

        Walks each inbound edge's :class:`FlowControl`:

        1. :class:`SurgeBarrier` (if configured) gets first look.  When
           tripped under ``action="skip"`` it shortcuts to
           ``"skip_ahead"``; under ``"bypass"`` it lets the dependent
           fire ignoring this edge's gate; under ``"halt"`` it returns
           ``"surge_halted"``.
        2. :class:`Gate` (HardLock / SoftPass / Weir) handles the normal
           pass/hold decision.

        The second return value is the set of upstream names whose
        :class:`SurgeBarrier` tripped with ``action="bypass"`` this pass.
        Penstock post-consumption must be skipped for these edges.
        """
        last = self._state[current.name].last_tick_started
        if last is None:
            # First tick — honor ``phase_offset_sec`` for green-wave coordination.
            if current.phase_offset_sec > 0.0 and self._run_started_at is not None:
                if (now - self._run_started_at) < current.phase_offset_sec:
                    return "phase_offset", frozenset()
        elif (now - last) < current.interval:
            return "not_due", frozenset()
        bypassed: Set[str] = set()
        for up_name, flow in self._upstream[current.name]:
            edge = (up_name, current.name)
            up_state = self._state[up_name]
            up_in_flight = up_state.in_flight is not None and not up_state.in_flight.done()

            surge = flow.surge_barrier
            if surge is not None:
                surge_ctx = SurgeContext(
                    up_in_flight=up_in_flight,
                    up_started_at=up_state.started_at,
                    dependent_interval=current.interval,
                    now=now,
                )
                if surge.is_tripped(surge_ctx):
                    if surge.action == "skip":
                        flow.observer.on_skip(self, edge, "skip_ahead")
                        self._canal_rejects.append(
                            RejectEntry.model_construct(
                                source=current.cls.__name__,
                                error_kind="SkipAhead",
                                message=f"edge {edge[0]}→{edge[1]}: surge skip-ahead",
                                retry_after=None,
                                wave_index=self._tide_number,
                                from_name=up_name,
                                to_name=current.name,
                                cooldown_sec=None,
                            )
                        )
                        return "skip_ahead", frozenset()
                    if surge.action == "halt":
                        flow.observer.on_skip(self, edge, "surge_halted")
                        self._canal_rejects.append(
                            RejectEntry.model_construct(
                                source=current.cls.__name__,
                                error_kind="SurgeHalted",
                                message=f"edge {edge[0]}→{edge[1]}: surge halted",
                                retry_after=None,
                                wave_index=self._tide_number,
                                from_name=up_name,
                                to_name=current.name,
                                cooldown_sec=None,
                            )
                        )
                        return "surge_halted", frozenset()
                    # action == "bypass" — fire ignoring this edge's gate AND penstock.
                    bypassed.add(up_name)
                    continue

            gate_ctx = GateContext(
                up_in_flight=up_in_flight,
                up_last_wave_at=up_state.last_wave_at,
                last_consumed=self._last_consumed.get(edge),
                now=now,
            )
            gate_reason = flow.gate.gate_reason(gate_ctx)
            if gate_reason is not None:
                flow.observer.on_skip(self, edge, gate_reason)
                # ``awaiting_upstream`` is the normal pre-first-wave state for
                # a HardLock or Weir edge — every fresh window starts with at
                # least one of these per dependent, and a long-running daemon
                # accumulates one per re-tick while upstream is in-flight.
                # Recording each as a reject would pollute the structured DLQ
                # with non-failure events.  All other gate reasons (currently
                # none from the in-tree Gate hierarchy, but custom Gate
                # subclasses may emit new ones) DO populate rejects.
                if gate_reason != "awaiting_upstream":
                    self._canal_rejects.append(
                        RejectEntry.model_construct(
                            source=current.cls.__name__,
                            error_kind="GateBlocked",
                            message=f"edge {edge[0]}→{edge[1]}: {gate_reason}",
                            retry_after=None,
                            wave_index=self._tide_number,
                            from_name=up_name,
                            to_name=current.name,
                            cooldown_sec=None,
                        )
                    )
                return gate_reason, frozenset()
            # Penstock — edge-layer flow-rate strategy.  Delegates to the
            # strategy class (SustainedPenstock / BurstPenstock /
            # WindowPenstock / BackpressurePenstock / SignalPenstock); each
            # mutates its own slice of ``_EdgeState`` for bookkeeping and
            # returns a skip reason or None.
            if flow.penstock is not None:
                edge_state = self._edge_state.get(edge)
                if edge_state is not None:
                    penstock_reason = flow.penstock.consume_reason(edge_state, flow, now)
                    if penstock_reason is not None:
                        flow.observer.on_skip(self, edge, penstock_reason)
                        self._canal_rejects.append(
                            RejectEntry.model_construct(
                                source=current.cls.__name__,
                                error_kind="PenstockLimited",
                                message=f"edge {edge[0]}→{edge[1]}: {penstock_reason}",
                                retry_after=None,
                                wave_index=self._tide_number,
                                from_name=up_name,
                                to_name=current.name,
                                cooldown_sec=None,
                            )
                        )
                        return penstock_reason, frozenset()
        return None, frozenset(bypassed)

    def _spawn_tick(self, current: Current, bypassed_upstreams: FrozenSet[str] = frozenset()) -> None:
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
        for up_name, _flow in self._upstream[current.name]:
            up_wave = self._state[up_name].last_wave_at
            if up_wave is not None:
                consumed_snapshot[up_name] = up_wave
        task = asyncio.create_task(self._tick_wrapper(current, consumed_snapshot, bypassed_upstreams))
        state.in_flight = task

    async def _tick_wrapper(
        self,
        current: Current,
        consumed_snapshot: Dict[str, datetime],
        bypassed_upstreams: FrozenSet[str] = frozenset(),
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
            now_mono = time.monotonic()

            # Single pass over upstream edges — does four jobs that all key
            # on the canonical ``(from_name, to_name)`` = ``(up_name, current.name)``
            # edge tuple (matches ``_edge_state`` initialisation at line 177
            # and the read site in ``_gate_reason``).
            #
            # 1. Bump ``_last_consumed`` to the pre-tick snapshot value (so
            #    the next gate cycle knows this edge's consumption watermark)
            #    AND to the post-tick ``last_wave_at`` if upstream emitted
            #    during this tick.  The post-tick read wins on overlap.
            # 2. Update ``edge_state.flow_state.last_consumed_at`` (monotonic
            #    watermark read by SustainedPenstock / BackpressurePenstock).
            # 3. Fire ``Penstock.post_consume`` so BurstPenstock debits its
            #    token and WindowPenstock appends to its log — skipped on
            #    bypassed edges per the bypass contract.
            # 4. Fire ``FlowObserver.on_fire`` for every non-bypassed edge
            #    that contributed to this tick.
            for up_name, edge_flow in self._upstream[current.name]:
                edge_key = (up_name, current.name)
                snapshot_value = consumed_snapshot.get(up_name)
                if snapshot_value is not None:
                    self._last_consumed[edge_key] = snapshot_value
                latest = self._state[up_name].last_wave_at
                if latest is not None:
                    self._last_consumed[edge_key] = latest

                edge_state = self._edge_state.get(edge_key)
                bypassed = up_name in bypassed_upstreams
                if edge_state is not None:
                    edge_state.flow_state.last_consumed_at = now_mono
                    if edge_flow.penstock is not None and not bypassed:
                        edge_flow.penstock.post_consume(edge_state, now_mono)
                if not bypassed:
                    edge_flow.observer.on_fire(self, edge_key, self._tide_number)
            state.started_at = None
            # Push this tick's wave content into every outgoing edge's
            # reservoir.  Reads the strong-ref ``_tideweaver_snapshot`` the
            # tick body parks on its output class (Stream parks one in
            # ``_tick_stream``; Fjord parks one per derived class in
            # ``_outflow.flush``).  Falls back to ``cls.inc_dict.values()``
            # when no snapshot is parked — preserves the legacy behavior of
            # custom tick factories that mutate ``inc_dict`` directly
            # (e.g. test doubles).  Empty waves are skipped to avoid
            # polluting the reservoir with no-op ticks.
            snapshot_attr = getattr(current.cls, "_tideweaver_snapshot", None)
            wave_snapshot = list(snapshot_attr) if snapshot_attr else list(current.cls.inc_dict.values())
            if wave_snapshot:
                for downstream_name, edge_flow in self._downstream[current.name]:
                    edge_key = (current.name, downstream_name)
                    edge_state = self._edge_state[edge_key]
                    edge_state.waves.append(wave_snapshot)
                    while len(edge_state.waves) > edge_flow.reservoir.depth:
                        displaced = edge_state.waves.popleft()
                        edge_state.overflow_count += 1
                        edge_flow.spillway.overflow(edge_key, displaced, edge_state.overflow_count)
                        # Observer hook — one call per displaced wave so a
                        # MetricsObserver subclass can count spillway events
                        # without monkey-patching the spillway itself.
                        edge_flow.observer.on_spillway(self, edge_key, displaced, edge_state.overflow_count)
                    # Observer hook — reservoir occupancy after append.
                    # Fires every tick (including no-overflow appends) so a
                    # threshold-based observer can page on near-full.
                    edge_flow.observer.on_reservoir_level(
                        self,
                        edge_key,
                        len(edge_state.waves),
                        edge_flow.reservoir.depth,
                    )
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
        elif isinstance(current, CustomCurrent):
            await current.tick(self)
        else:
            raise NotImplementedError(
                f"Tideweaver has no default tick body for bare Current; "
                f"subclass Stream / Fjord / Export / CustomCurrent or pass a tick_factory.  "
                f"Got: {type(current).__name__}"
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

        # Snapshot upstream Currents' Incorporator class registries.  Two
        # distinct resolution paths, split for clarity:
        #
        # 1. Direct upstreams (those that share an edge with this Fjord) —
        #    read the per-edge reservoir's latest wave so depth>1 buffering
        #    and replay land cleanly.  Fall through to the class snapshot
        #    when the reservoir is empty (dependent fires before upstream
        #    has emitted).
        # 2. Transitive upstreams (further up the closure with no direct
        #    edge) — there's no edge-state to consult, so go straight to
        #    the class snapshot or live inc_dict.
        #
        # ``last_consumed_at`` is bumped by ``_tick_wrapper.finally``, not
        # here — keeps the Penstock accounting in one place.
        state: Dict[str, List[Any]] = {}
        direct_upstreams = {up_name for up_name, _flow in self._upstream[current.name]}
        all_upstreams = self._transitive_upstreams(current.name)

        # Direct: edge reservoir → class snapshot → inc_dict fallback.
        for up_name in all_upstreams:
            if up_name not in direct_upstreams:
                continue
            dep = self._currents_by_name[up_name]
            edge_state = self._edge_state.get((up_name, current.name))
            if edge_state is not None and edge_state.waves:
                state[dep.cls.__name__] = list(edge_state.waves[-1])
                continue
            snapshot = getattr(dep.cls, "_tideweaver_snapshot", None)
            state[dep.cls.__name__] = list(snapshot) if snapshot is not None else list(dep.cls.inc_dict.values())

        # Transitive (non-direct) upstreams — class snapshot only.
        for up_name in all_upstreams:
            if up_name in direct_upstreams:
                continue
            dep = self._currents_by_name[up_name]
            snapshot = getattr(dep.cls, "_tideweaver_snapshot", None)
            state[dep.cls.__name__] = list(snapshot) if snapshot is not None else list(dep.cls.inc_dict.values())

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
