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
  :func:`incorporator.observability.pipeline.outflow.flush`, the shared
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
import statistics
import time
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
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
from ..logger import _route_scheduler_event_to_log, _route_to_log, current_meta
from ..pipeline.outflow import flush
from ._retry_defaults import (
    _CANAL_OUTER_STOP,
    _CANAL_OUTER_WAIT_MAX,
    _CANAL_OUTER_WAIT_MIN,
    _CANAL_OUTER_WAIT_MULTIPLIER,
)
from .current import Current, CustomCurrent, Export, Fjord, Stream
from .current_outcome import CurrentOutcome
from .flow import FlowControl, GateContext, SurgeContext
from .reasons import SkipReason, WakeReason
from .tide import Tide
from .watershed import Watershed

logger = logging.getLogger(__name__)

TickFactory = Callable[[Current], Awaitable[None]]


class _EdgeState(BaseModel):
    """Per-edge scheduler bookkeeping, keyed by ``(from_name, to_name)``."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    waves: deque[list[Any]] = Field(default_factory=deque)
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

    eligibility_start_perf: float | None = None
    """``time.perf_counter()`` recorded when this edge first became eligible
    for firing in the current window.  Set on first entry to the per-edge
    loop body; reset to ``None`` in ``_tick_wrapper.finally`` after a
    successful consumption so the next firing window starts fresh.
    Used to populate ``duration_sec`` on canal-layer :class:`~incorporator.rejects.RejectEntry`
    records at the four skip-emit sites."""


class _CurrentState(BaseModel):
    """Per-current scheduler bookkeeping, keyed by ``current.name`` in :attr:`Tideweaver._state`."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    last_tick_started: float | None = None
    """Monotonic time the current's most recent tick was spawned, or None."""

    last_wave_at: datetime | None = None
    """UTC time the current's most recent tick finished, or None."""

    started_at: float | None = None
    """Monotonic time of the CURRENTLY in-flight tick.  Cleared when the
    tick completes — distinguishes "running now" from "ran before"."""

    in_flight: asyncio.Task[None] | None = None
    """The asyncio Task for the most recent tick.  Persists after the
    task completes (``.done()`` returns True) so the scheduler can
    still inspect the finished task for restart / error-isolation logic."""


def _build_canal_reject(
    source: str,
    error_kind: str,
    message: str,
    *,
    wave_index: int,
    from_name: str | None = None,
    to_name: str | None = None,
    duration_sec: float | None = None,
    cooldown_sec: float | None = None,
    session: str | None = None,
) -> RejectEntry:
    """Construct a canal-layer :class:`~incorporator.rejects.RejectEntry`.

    All five canal skip sites (SkipAhead, SurgeHalted, GateBlocked,
    PenstockLimited, SourceLoadFailure) share identical field semantics:
    ``retry_after`` is always ``None`` (no HTTP context), ``attempt_number``
    is always 1 (no retry loop at the canal layer).

    Args:
        source: Class name of the current being skipped.
        error_kind: Canal skip kind string (e.g. ``"SkipAhead"``).
        message: Human-readable skip detail including edge label.
        wave_index: Current tide/wave index from the scheduler.
        from_name: Upstream current name, or ``None`` for source-load failures.
        to_name: Downstream current name, or ``None`` for source-load failures.
        duration_sec: Elapsed seconds since the edge became eligible, or ``None``.
        cooldown_sec: Penstock-state cooldown hint, or ``None``.
        session: Logger name for the active session, or ``None``.

    Returns:
        A frozen :class:`~incorporator.rejects.RejectEntry` ready to append
        to the scheduler's ``_canal_rejects`` list.
    """
    return RejectEntry.model_construct(
        source=source,
        error_kind=error_kind,
        message=message,
        retry_after=None,
        wave_index=wave_index,
        from_name=from_name,
        to_name=to_name,
        attempt_number=1,
        duration_sec=duration_sec,
        cooldown_sec=cooldown_sec,
        session=session,
    )


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
        tick_factory: TickFactory | None = None,
        pass_interval: float | None = None,
        backlog_backoff_factor: float = 1.0,
        logger_name: str | None = None,
        log_currents: bool = True,
    ) -> None:
        self.watershed = watershed
        self.tick_factory = tick_factory
        self.logger_name = logger_name
        self.log_currents = log_currents
        self.pass_interval = pass_interval or max(
            0.05,
            min(1.0, min(c.interval for c in watershed.currents) / 2.0),
        )
        self._backlog_backoff_factor = backlog_backoff_factor
        self._recent_pass_metrics: deque[tuple[float, int]] = deque(maxlen=10)

        self._state: dict[str, _CurrentState] = {c.name: _CurrentState() for c in watershed.currents}
        # ``_last_consumed`` keys on edge tuples, not current names — so it's
        # a standalone dict instead of a field on ``_CurrentState``.
        self._last_consumed: dict[tuple[str, str], datetime] = {}
        self._tide_number = 0
        # Canal-layer skip records accumulated across the run.  Populated at
        # the four ``_gate_reason`` skip-emit sites (penstock-limited,
        # surge-halted, surge skip-ahead, gate-blocked); exposed via the
        # ``rejects`` property.  Parallel to ``IncorporatorList.rejects``
        # at the verb layer — gives callers a structured DLQ view of every
        # canal skip that never reached a tick body.
        self._canal_rejects: list[RejectEntry] = []
        # ids of rejects already routed to the session log at their tick site
        # (with per-current meta).  The LoggedTideweaver.run finally-sweep skips
        # these so a tick-routed reject is not emitted a second time.
        self._routed_reject_ids: set[int] = set()
        self._currents_by_name: dict[str, Current] = {c.name: c for c in watershed.currents}
        self._topo: list[str] = watershed.toposort()
        self._upstream: dict[str, list[tuple[str, FlowControl]]] = {c.name: [] for c in watershed.currents}
        self._downstream: dict[str, list[tuple[str, FlowControl]]] = {c.name: [] for c in watershed.currents}
        self._edge_state: dict[tuple[str, str], _EdgeState] = {}
        for e in watershed.edges:
            self._upstream[e.to_name].append((e.from_name, e.flow))
            self._downstream[e.from_name].append((e.to_name, e.flow))
            self._edge_state[(e.from_name, e.to_name)] = _EdgeState()
        self._transitive_cache: dict[str, list[str]] = {}

        # Heap entries are ``(due_at_monotonic, counter, name)`` — counter
        # tiebreaks so heapq never compares Current names lexicographically.
        self._wake_event: asyncio.Event = asyncio.Event()
        self._due_heap: list[tuple[float, int, str]] = []
        self._heap_counter: int = 0
        self._run_started_at: float | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def rejects(self) -> list[RejectEntry]:
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
        tides: list[Tide] | None = None,
        waves: list[Wave] | None = None,
    ) -> TuningReport:
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
        self._client_pool: dict[tuple[Any, ...], httpx.AsyncClient] = {}
        self._run_started_at = time.monotonic()
        for c in self.watershed.currents:
            if c.phase_offset_sec > 0.0:
                self._push_due(c.name, self._run_started_at + c.phase_offset_sec)
        shutdown_event = asyncio.Event()
        stopper = asyncio.create_task(self._shutdown_at_window_end(shutdown_event))
        wake_reason: WakeReason = WakeReason.STARTUP
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

    async def _wait_for_next_event(self, shutdown_event: asyncio.Event) -> WakeReason:
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
            fallback_reason: WakeReason = WakeReason.PASS_INTERVAL
        else:
            timeout = max(0.0, next_due - now)
            fallback_reason = WakeReason.TIMER
        if self._backlog_backoff_factor > 1.0 and len(self._recent_pass_metrics) >= 5:
            total_currents = len(self.watershed.currents)
            if total_currents > 0:
                med_inflight = statistics.median(m[1] for m in self._recent_pass_metrics)
                med_duration = statistics.median(m[0] for m in self._recent_pass_metrics)
                if med_inflight > 0.8 * total_currents and med_duration > 0.8 * self.pass_interval:
                    timeout = min(timeout * self._backlog_backoff_factor, 5.0 * self.pass_interval)
        if timeout <= 0.0:
            # Heap already expired — re-pass immediately.  Still clear the
            # wake event so a stale set() from earlier doesn't no-op the
            # next sleep.
            self._wake_event.clear()
            return WakeReason.TIMER
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
                wake_reason = WakeReason.SHUTDOWN
            elif wake_waiter in done:
                wake_reason = WakeReason.WAKE_EVENT
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

    async def _run_pass(self, shutdown_event: asyncio.Event, wake_reason: WakeReason) -> Tide:
        self._tide_number += 1
        started = time.monotonic()
        fired: list[str] = []
        skipped: list[tuple[str, SkipReason]] = []
        outcomes: list[CurrentOutcome] = []
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
                skipped.append((name, SkipReason.STILL_RUNNING))
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
        tide = Tide.model_construct(
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
            session=self.logger_name,
            timestamp=datetime.now(timezone.utc),
        )
        if self._backlog_backoff_factor > 1.0:
            self._recent_pass_metrics.append((tide.duration_sec, tide.in_flight_count_at_start))
        return tide

    def _gate_reason(self, current: Current, now: float) -> tuple[SkipReason | None, frozenset[str]]:
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
                    return SkipReason.PHASE_OFFSET, frozenset()
        elif (now - last) < current.interval:
            return SkipReason.NOT_DUE, frozenset()
        bypassed: set[str] = set()
        for up_name, flow in self._upstream[current.name]:
            edge = (up_name, current.name)
            up_state = self._state[up_name]
            up_in_flight = up_state.in_flight is not None and not up_state.in_flight.done()

            # Retrieve edge_state early so eligibility_start_perf is available
            # at all four skip-emit sites below.
            edge_state = self._edge_state.get(edge)
            if edge_state is not None and edge_state.eligibility_start_perf is None:
                edge_state.eligibility_start_perf = time.perf_counter()

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
                        flow.observer.on_skip(self, edge, SkipReason.SKIP_AHEAD)
                        self._canal_rejects.append(
                            _build_canal_reject(
                                current.cls.__name__,
                                "SkipAhead",
                                f"edge {edge[0]}→{edge[1]}: surge skip-ahead",
                                wave_index=self._tide_number,
                                from_name=up_name,
                                to_name=current.name,
                                duration_sec=(
                                    (time.perf_counter() - edge_state.eligibility_start_perf)
                                    if edge_state is not None and edge_state.eligibility_start_perf is not None
                                    else None
                                ),
                                session=self.logger_name,
                            )
                        )
                        return SkipReason.SKIP_AHEAD, frozenset()
                    if surge.action == "halt":
                        flow.observer.on_skip(self, edge, SkipReason.SURGE_HALTED)
                        self._canal_rejects.append(
                            _build_canal_reject(
                                current.cls.__name__,
                                "SurgeHalted",
                                f"edge {edge[0]}→{edge[1]}: surge halted",
                                wave_index=self._tide_number,
                                from_name=up_name,
                                to_name=current.name,
                                duration_sec=(
                                    (time.perf_counter() - edge_state.eligibility_start_perf)
                                    if edge_state is not None and edge_state.eligibility_start_perf is not None
                                    else None
                                ),
                                session=self.logger_name,
                            )
                        )
                        return SkipReason.SURGE_HALTED, frozenset()
                    # action == "bypass" — fire ignoring this edge's gate AND penstock.
                    bypassed.add(up_name)
                    continue

            gate_ctx = GateContext(
                up_in_flight=up_in_flight,
                up_last_wave_at=up_state.last_wave_at,
                last_consumed=self._last_consumed.get(edge),
                now=now,
            )
            gate_reason_str = flow.gate.gate_reason(gate_ctx)
            if gate_reason_str is not None:
                gate_skip = SkipReason(gate_reason_str) if isinstance(gate_reason_str, str) else gate_reason_str
                flow.observer.on_skip(self, edge, gate_skip)
                # ``awaiting_upstream`` is the normal pre-first-wave state for
                # a HardLock or Weir edge — every fresh window starts with at
                # least one of these per dependent, and a long-running daemon
                # accumulates one per re-tick while upstream is in-flight.
                # Recording each as a reject would pollute the structured DLQ
                # with non-failure events.  All other gate reasons (currently
                # none from the in-tree Gate hierarchy, but custom Gate
                # subclasses may emit new ones) DO populate rejects.
                if gate_skip != SkipReason.AWAITING_UPSTREAM:
                    self._canal_rejects.append(
                        _build_canal_reject(
                            current.cls.__name__,
                            "GateBlocked",
                            f"edge {edge[0]}→{edge[1]}: {gate_skip.value}",
                            wave_index=self._tide_number,
                            from_name=up_name,
                            to_name=current.name,
                            duration_sec=(
                                (time.perf_counter() - edge_state.eligibility_start_perf)
                                if edge_state is not None and edge_state.eligibility_start_perf is not None
                                else None
                            ),
                            session=self.logger_name,
                        )
                    )
                return gate_skip, frozenset()
            # Penstock — edge-layer flow-rate strategy.  Delegates to the
            # strategy class (SustainedPenstock / BurstPenstock /
            # WindowPenstock / BackpressurePenstock / SignalPenstock); each
            # mutates its own slice of ``_EdgeState`` for bookkeeping and
            # returns a (reason, cooldown_sec) tuple or None.
            if flow.penstock is not None and edge_state is not None:
                raw = flow.penstock.consume_reason(edge_state, flow, now)
                # Back-compat shim: third-party subclasses may still return str | None.
                if isinstance(raw, str):
                    penstock_reason_str, cooldown_sec = raw, None
                elif raw is not None:
                    penstock_reason_str, cooldown_sec = raw
                else:
                    penstock_reason_str, cooldown_sec = None, None
                if penstock_reason_str is not None:
                    # Normalise at the scheduler boundary — io/penstock.py returns
                    # a plain string to avoid importing from tideweaver (cycle risk).
                    penstock_skip = (
                        SkipReason(penstock_reason_str) if isinstance(penstock_reason_str, str) else penstock_reason_str
                    )
                    flow.observer.on_skip(self, edge, penstock_skip)
                    self._canal_rejects.append(
                        _build_canal_reject(
                            current.cls.__name__,
                            "PenstockLimited",
                            f"edge {edge[0]}→{edge[1]}: {penstock_skip.value}",
                            wave_index=self._tide_number,
                            from_name=up_name,
                            to_name=current.name,
                            duration_sec=(
                                (time.perf_counter() - edge_state.eligibility_start_perf)
                                if edge_state.eligibility_start_perf is not None
                                else None
                            ),
                            cooldown_sec=cooldown_sec,
                            session=self.logger_name,
                        )
                    )
                    return penstock_skip, frozenset()
        return None, frozenset(bypassed)

    def _spawn_tick(self, current: Current, bypassed_upstreams: frozenset[str] = frozenset()) -> None:
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
        consumed_snapshot: dict[str, datetime] = {}
        for up_name, _flow in self._upstream[current.name]:
            up_wave = self._state[up_name].last_wave_at
            if up_wave is not None:
                consumed_snapshot[up_name] = up_wave
        task = asyncio.create_task(self._tick_wrapper(current, consumed_snapshot, bypassed_upstreams))
        state.in_flight = task

    async def _tick_wrapper(
        self,
        current: Current,
        consumed_snapshot: dict[str, datetime],
        bypassed_upstreams: frozenset[str] = frozenset(),
    ) -> None:
        """Run one tick under the current's :attr:`on_error` policy."""
        retrying: AsyncRetrying | None = None
        _tick_raised: bool = False
        try:
            if current.on_error == "restart":
                retrying = AsyncRetrying(
                    stop=stop_after_attempt(_CANAL_OUTER_STOP),
                    wait=wait_random_exponential(
                        multiplier=_CANAL_OUTER_WAIT_MULTIPLIER, min=_CANAL_OUTER_WAIT_MIN, max=_CANAL_OUTER_WAIT_MAX
                    ),
                    reraise=True,
                )
                async for attempt in retrying:
                    with attempt:
                        await self._invoke_tick(current)
            elif current.on_error == "isolate":
                try:
                    await self._invoke_tick(current)
                except Exception as exc:  # noqa: BLE001
                    if isinstance(self.logger_name, str):
                        _route_scheduler_event_to_log(
                            self.logger_name,
                            "isolated_tick_failure",
                            current.name,
                            f"Tideweaver: isolated tick failure on {current.name}: {exc}",
                            cls_name=current.cls.__name__,
                            tide_number=self._tide_number,
                        )
                    else:
                        logger.warning("Tideweaver: isolated tick failure on %s: %s", current.name, exc)
                    _tick_raised = True
            else:  # fail_watershed
                await self._invoke_tick(current)
        except (Exception, RetryError) as e:
            attempt_number = retrying.statistics.get("attempt_number", 1) if retrying is not None else 1
            try:
                e._incorporator_attempt_number = attempt_number  # type: ignore[attr-defined]
            except AttributeError:
                pass
            _tick_raised = True
            if current.on_error == "fail_watershed":
                raise
            if isinstance(self.logger_name, str):
                _route_scheduler_event_to_log(
                    self.logger_name,
                    "tick_parked",
                    current.name,
                    f"Tideweaver: tick failed for {current.name} after retries; current parked.",
                    cls_name=current.cls.__name__,
                    tide_number=self._tide_number,
                )
            else:
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
                    # Reset eligibility timer so the next firing window starts fresh.
                    edge_state.eligibility_start_perf = None
                if not bypassed:
                    edge_flow.observer.on_fire(self, edge_key, self._tide_number)
            state.started_at = None
            # Push this tick's wave content into every outgoing edge's
            # reservoir.  Reads the strong-ref ``_tideweaver_snapshot`` the
            # tick body parks on its output class (Stream parks one in
            # ``_tick_stream``; Fjord parks one per derived class in
            # ``outflow.flush``).  Falls back to ``cls.inc_dict.values()``
            # when no snapshot is parked — preserves the legacy behavior of
            # custom tick factories that mutate ``inc_dict`` directly
            # (e.g. test doubles).  Empty waves are skipped to avoid
            # polluting the reservoir with no-op ticks.
            snapshot_attr = getattr(current.cls, "_tideweaver_snapshot", None)
            wave_snapshot = list(snapshot_attr) if snapshot_attr else list(current.cls.inc_dict.values())
            if not wave_snapshot and not _tick_raised and isinstance(current, CustomCurrent):
                upstream_had_data = any(
                    getattr(self._currents_by_name[up_name].cls, "_tideweaver_snapshot", None)
                    for up_name, _ in self._upstream[current.name]
                )
                if upstream_had_data:
                    upstream_names = ", ".join(up for up, _ in self._upstream[current.name])
                    _empty_detail = (
                        f"Tideweaver: {current.name} tick produced empty output despite non-empty "
                        f"upstream snapshot(s) ({upstream_names}); check the tick body / predicate / "
                        f"missing conv_dict on the upstream (fires each tick while the condition persists)."
                    )
                    if isinstance(self.logger_name, str):
                        _route_scheduler_event_to_log(
                            self.logger_name,
                            "empty_output",
                            current.name,
                            _empty_detail,
                            cls_name=current.cls.__name__,
                            tide_number=self._tide_number,
                        )
                    else:
                        logger.warning(
                            "Tideweaver: %s tick produced empty output despite non-empty "
                            "upstream snapshot(s) (%s); check the tick body / predicate / "
                            "missing conv_dict on the upstream (fires each tick while the "
                            "condition persists).",
                            current.name,
                            upstream_names,
                        )
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
            await current._run_tick(self)
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
    def _client_pool_key(incorp_params: dict[str, Any]) -> tuple[Any, ...]:
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

    def _get_or_create_client(self, incorp_params: dict[str, Any]) -> httpx.AsyncClient:
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
        """One chunking-mode drain of ``cls.stream(...)`` or a parent-child incorp() fan-out.

        Incorporator's ``inc_dict`` is a ``WeakValueDictionary`` — without an
        external strong reference, instances die before a downstream Fjord
        flush can read them.  We park a strong-ref snapshot on the class as
        ``_tideweaver_snapshot`` so the registry stays alive between ticks;
        the Fjord flush reads through to that attribute when present.

        When ``current.parent_current`` is set, the stream acts as a child
        drill: filter the parent's ``_tideweaver_snapshot``, then call
        ``cls.incorp(inc_parent=filtered, ...)`` directly instead of
        running ``cls.stream()``.

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

        if current.parent_current is not None:
            upstream_current = self._currents_by_name[current.parent_current]
            pre_snap = getattr(upstream_current.cls, "_tideweaver_snapshot", None)
            if not pre_snap:
                _snap_detail = (
                    f"Tideweaver: Stream {current.name!r} parent_current={current.parent_current!r} snapshot is "
                    f"empty; skipping tick (no rows to drill). Fires each tick while the condition persists "
                    f"-- confirm the parent's tick is firing."
                )
                if isinstance(self.logger_name, str):
                    _route_scheduler_event_to_log(
                        self.logger_name,
                        "empty_parent_snapshot",
                        current.name,
                        _snap_detail,
                        cls_name=current.cls.__name__,
                        tide_number=self._tide_number,
                    )
                else:
                    logger.warning(
                        "Tideweaver: Stream %r parent_current=%r snapshot is empty; "
                        "skipping tick (no rows to drill). Fires each tick while the "
                        "condition persists -- confirm the parent's tick is firing.",
                        current.name,
                        current.parent_current,
                    )
                return
            incorp_call_params = {**params_with_client, "inc_parent": cast(Any, list(pre_snap))}
            _pc_result = await current.cls.incorp(**incorp_call_params)
            if isinstance(self.logger_name, str) and self.log_currents:
                _pc_rejects = getattr(_pc_result, "rejects", None) or []
                for _reject in _pc_rejects:
                    _route_to_log(self.logger_name, _reject, extra_meta=current_meta(current))
            cast(Any, current.cls)._tideweaver_snapshot = list(current.cls.inc_dict.values())
            return

        kwargs: dict[str, Any] = {
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
        accumulated: dict[Any, Any] = {}
        async for _wave in current.cls.stream(**kwargs):
            accumulated.update(current.cls.inc_dict)
            if isinstance(self.logger_name, str) and self.log_currents:
                _route_to_log(self.logger_name, _wave, extra_meta=current_meta(current))
                for _reject in _wave.rejects:
                    _route_to_log(self.logger_name, _reject, extra_meta=current_meta(current))
        # A stream that produces zero rows while its configured inc_file is absent
        # is a source-load failure, not legitimately empty data: record a
        # SourceLoadFailure reject so the run can surface it (incorp() logs the
        # fetch error and returns an empty registry rather than raising).
        # The reject is appended from a tick Task, so it can land in a later Tide's
        # `canal_rejects_added` than the pass that produced it; the cumulative
        # `tw.rejects`, read at run end, is authoritative.
        inc_file_path = incorp_params.get("inc_file")
        if not accumulated and inc_file_path and isinstance(inc_file_path, str) and not Path(inc_file_path).is_file():
            self._canal_rejects.append(
                _build_canal_reject(
                    current.cls.__name__,
                    "SourceLoadFailure",
                    (
                        f"Stream '{current.name}' produced zero rows because the configured "
                        f"inc_file does not exist: {inc_file_path}"
                    ),
                    wave_index=self._tide_number,
                    from_name=None,
                    to_name=None,
                    duration_sec=None,
                    cooldown_sec=None,
                    session=self.logger_name,
                )
            )
            # Route immediately with current_meta so the session log carries the
            # per-current code tag, and record its id so the LoggedTideweaver.run
            # finally-sweep skips it (single emission, not doubled).
            if isinstance(self.logger_name, str) and self.log_currents:
                _slf_reject = self._canal_rejects[-1]
                _route_to_log(self.logger_name, _slf_reject, extra_meta=current_meta(current))
                self._routed_reject_ids.add(id(_slf_reject))
        # Strong-ref snapshot — keeps the WeakValueDictionary entries alive.
        # Runtime-only escape-hatch attribute (no field on Incorporator itself).
        # ``outflow.flush`` parks the same attribute on Fjord output
        # classes, so downstream readers walk Stream and Fjord upstreams
        # uniformly via ``getattr(dep.cls, "_tideweaver_snapshot", None)``.
        cls_any = cast(Any, current.cls)
        cls_any._tideweaver_snapshot = list(accumulated.values())

    async def _tick_fjord(self, current: Fjord) -> None:
        """One fjord flush: snapshot upstream → outflow(state) → build → export.

        Delegates the outflow → normalize → per-class build/export to the
        shared :func:`incorporator.observability.pipeline.outflow.flush`
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
        state: dict[str, list[Any]] = {}
        direct_upstreams = {up_name for up_name, _flow in self._upstream[current.name]}
        all_upstreams = self._transitive_upstreams(current.name)

        # Direct: parent_currents filter → edge reservoir → class snapshot → inc_dict fallback.
        for up_name in all_upstreams:
            if up_name not in direct_upstreams:
                continue
            dep = self._currents_by_name[up_name]
            if up_name in current.parent_currents:
                # parent_currents semantics: name an upstream by current-name and read its
                # registry snapshot directly — bypasses the reservoir/wave path because
                # parent-child drills want full per-current state, not the last edge wave.
                snapshot = getattr(dep.cls, "_tideweaver_snapshot", None)
                rows: list[Any] = list(snapshot) if snapshot is not None else list(dep.cls.inc_dict.values())
                state[dep.cls.__name__] = rows
                if not rows:
                    _fjord_snap_detail = (
                        f"Tideweaver: Fjord {current.name!r} parent_currents={up_name!r} upstream snapshot is "
                        f"empty; state[{dep.cls.__name__!r}] is [] this tick -- confirm the parent's tick is firing."
                    )
                    if isinstance(self.logger_name, str):
                        _route_scheduler_event_to_log(
                            self.logger_name,
                            "empty_parent_snapshot",
                            current.name,
                            _fjord_snap_detail,
                            cls_name=dep.cls.__name__,
                            tide_number=self._tide_number,
                        )
                    else:
                        logger.warning(
                            "Tideweaver: Fjord %r parent_currents=%r upstream snapshot is empty; "
                            "state[%r] is [] this tick -- confirm the parent's tick is firing.",
                            current.name,
                            up_name,
                            dep.cls.__name__,
                        )
                continue
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
                _flush_detail = (
                    f"Tideweaver: Fjord flush {current.name!r} raised on derived class {derived_name!r}: {err}"
                )
                if isinstance(self.logger_name, str):
                    _route_scheduler_event_to_log(
                        self.logger_name,
                        "fjord_flush_failure",
                        current.name,
                        _flush_detail,
                        cls_name=derived_name,
                        tide_number=self._tide_number,
                    )
                else:
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

    def _transitive_upstreams(self, name: str) -> list[str]:
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
