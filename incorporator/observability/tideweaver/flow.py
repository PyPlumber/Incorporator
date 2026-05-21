"""Edge flow-control primitives — locks, weirs, penstocks, reservoirs, spillways.

Real canal/dam engineering models every passage point with the same five
primitives this module exposes, composed by :class:`FlowControl`:

* :class:`Gate` (subclasses :class:`HardLock`, :class:`SoftPass`,
  :class:`Weir`) — pass/hold decision per pass.
* :class:`SurgeBarrier` — conditional override when upstream is in-flight
  beyond a threshold (storm-surge barrier semantics).
* :class:`Penstock` (subclasses :class:`SustainedPenstock`,
  :class:`BurstPenstock`, :class:`WindowPenstock`,
  :class:`BackpressurePenstock`, :class:`SignalPenstock`) —
  flow-rate strategies across the edge.  ``Backpressure`` reads
  the reservoir to adapt — closing a control loop between the two
  primitives.
* :class:`Reservoir` — FIFO buffer of recent waves; absorbs upstream
  surges and supports replay.
* :class:`Spillway` (subclasses :class:`DropOldest`, :class:`RaiseOverflow`,
  :class:`ExportToArchive`) — overflow handler when the reservoir fills.

``Current.phase_offset_sec`` (green-wave coordination) lives on
:class:`~.current.Current` rather than here — it's a property of the
dependent's tick schedule, not of one edge.

:class:`FlowControl` composes the five edge-level primitives into the
single per-edge config object the scheduler reads.  Mirrors the
``ThrottleStrategy`` split in :mod:`incorporator.io.throttle` (HTTP-layer
host throttles) with a parallel hierarchy here for wave-layer decisions.

Layer separation: HTTP throttles (``io.throttle``) limit *requests per
host*; Penstocks here limit *wave consumption per edge*.  They compose
multiplicatively in a chain — see :mod:`incorporator.observability.tideweaver`
package docstring for the canal-metaphor mapping.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Literal, Optional, Tuple, Type

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .current import Current
    from .scheduler import Tideweaver


# ---------------------------------------------------------------------------
# Gate hierarchy — pass/hold decision per upstream
# ---------------------------------------------------------------------------


class Gate(BaseModel):
    """Pass/hold decision for one upstream of a dependent current.

    Stateless — gates read scheduler state but hold none of their own.
    Pydantic-shaped to match the :class:`_CurrentState` style and so
    future per-gate config (staleness windows, batch sizes) lands
    naturally as fields.
    """

    model_config = ConfigDict(frozen=True)

    def gate_reason(
        self,
        scheduler: "Tideweaver",
        dependent: "Current",
        up_name: str,
        now: float,
    ) -> Optional[str]:
        """Return a skip reason string, or ``None`` to allow firing."""
        raise NotImplementedError


class HardLock(Gate):
    """Sequential chamber: blocks on in-flight + freshness + first emission.

    Skip-ahead is NOT this class's responsibility — it lives on
    :class:`SurgeBarrier` as a separable primitive, so weir and soft
    modes can opt in to surge-handling without inheriting hard-lock
    gating, and so the threshold lives on the edge (where it belongs
    architecturally) rather than on the dependent Current.
    """

    def gate_reason(
        self,
        scheduler: "Tideweaver",
        dependent: "Current",
        up_name: str,
        now: float,
    ) -> Optional[str]:
        up_state = scheduler._state[up_name]
        if up_state.in_flight is not None and not up_state.in_flight.done():
            return "awaiting_upstream"
        last_wave = up_state.last_wave_at
        if last_wave is None:
            return "awaiting_upstream"
        consumed = scheduler._last_consumed.get((dependent.name, up_name))
        if consumed is not None and consumed >= last_wave:
            return "awaiting_upstream"
        return None


class SoftPass(Gate):
    """Open channel: no gating; downstream fires on its own cadence."""

    def gate_reason(
        self,
        scheduler: "Tideweaver",
        dependent: "Current",
        up_name: str,
        now: float,
    ) -> Optional[str]:
        return None


class Weir(Gate):
    """Passive overflow: fires on own interval once upstream has emitted a fresh wave.

    Like :class:`HardLock`: requires a wave the dependent hasn't yet
    consumed.  Unlike :class:`HardLock`: does NOT block while upstream
    is in-flight, and does NOT trigger skip-ahead.  The dependent uses
    whatever wave is currently parked; the next interval picks up any
    wave that lands between now and then.

    Effect: chains with realistic Stream intervals (1-3s) feeding fast
    Fjord/Export tails (0.2-0.5s) keep their data dependency without
    starving on the in-flight gate.
    """

    def gate_reason(
        self,
        scheduler: "Tideweaver",
        dependent: "Current",
        up_name: str,
        now: float,
    ) -> Optional[str]:
        last_wave = scheduler._state[up_name].last_wave_at
        if last_wave is None:
            return "awaiting_upstream"
        consumed = scheduler._last_consumed.get((dependent.name, up_name))
        if consumed is not None and consumed >= last_wave:
            return "awaiting_upstream"
        return None


# ---------------------------------------------------------------------------
# SurgeBarrier — conditional override when upstream is under extreme load
# ---------------------------------------------------------------------------


SurgeAction = Literal["skip", "bypass", "halt"]


class SurgeBarrier(BaseModel):
    """Storm-surge barrier: overrides the gate when upstream is in-flight too long.

    A real canal storm-surge barrier closes (or opens, depending on
    design) when tidal conditions exceed a threshold.  Here, "extreme"
    means the upstream tick has been in-flight for longer than
    ``threshold_multiple * dependent.interval``.  When tripped, the
    configured action overrides the normal gate decision:

    * ``"skip"`` — dependent skips this pass (reason ``"skip_ahead"``).
      Matches today's :class:`HardLock` baked-in ``skip_threshold``.
    * ``"bypass"`` — dependent fires unconditionally, ignoring the
      gate for this upstream.  Suitable for "I want SOMETHING through
      even if it's stale" Fjord tails.
    * ``"halt"`` — dependent stops firing entirely (reason
      ``"surge_halted"``).  Circuit-breaker-like; useful when
      downstream side effects shouldn't run while upstream is stuck.

    Threshold is per-edge (lives on :class:`FlowControl`), not per-Current
    (today's location), so a single dependent can declare different
    surge tolerances for different upstreams.
    """

    model_config = ConfigDict(frozen=True)

    threshold_multiple: float = Field(
        default=2.0,
        gt=0.0,
        description="Trip when (now - upstream.started_at) > threshold_multiple * dependent.interval",
    )
    action: SurgeAction = "skip"

    def is_tripped(
        self,
        scheduler: "Tideweaver",
        dependent: "Current",
        up_name: str,
        now: float,
    ) -> bool:
        """Return True iff upstream has been in-flight longer than threshold * interval."""
        up_state = scheduler._state[up_name]
        if up_state.in_flight is None or up_state.in_flight.done():
            return False
        if up_state.started_at is None:
            return False
        return (now - up_state.started_at) > self.threshold_multiple * dependent.interval


# ---------------------------------------------------------------------------
# Penstock hierarchy — edge-layer flow-rate strategies
# ---------------------------------------------------------------------------


class Penstock(BaseModel):
    """Edge-layer flow-rate strategy.  Subclasses define a concrete mechanic.

    Different from :class:`~incorporator.io.throttle.ThrottleStrategy`
    (which limits HTTP calls per host): Penstock limits *wave
    consumption* across one Watershed edge.  A slow downstream can
    throttle how fast it sucks waves from a hot upstream without
    affecting upstream's HTTP emission rate.  See ``flow.py`` module
    docstring for the canal-metaphor mapping.

    Override :meth:`consume_reason` to implement a concrete mechanic.
    The scheduler delegates per-edge in the gate cycle; strategies
    read scheduler state, mutate per-edge bookkeeping on
    :class:`_EdgeState`, and return a skip reason or ``None``.
    """

    model_config = ConfigDict(frozen=True)

    def consume_reason(
        self,
        scheduler: "Tideweaver",
        edge_state: Any,
        flow: "FlowControl",
        now: float,
    ) -> Optional[str]:
        """Return a skip reason string, or ``None`` to allow consumption.

        Strategies may mutate ``edge_state`` for bookkeeping (token
        refill, window log eviction, etc.).  ``flow`` is the enclosing
        :class:`FlowControl` — needed by
        :class:`BackpressurePenstock` to read ``flow.reservoir.depth``;
        ignored by the simpler strategies.
        """
        raise NotImplementedError


class SustainedPenstock(Penstock):
    """Sustained-rate leaky bucket: minimum gap ``1 / rate_per_sec`` between consumptions.

    The simplest strategy and the closest analog to a real-world
    *fixed-orifice penstock* — outflow proportional to a constant
    cross-section.  Use when downstream just needs a smooth steady cap.
    """

    rate_per_sec: float = Field(gt=0.0, description="Max sustained wave consumptions per second.")

    def consume_reason(
        self,
        scheduler: "Tideweaver",
        edge_state: Any,
        flow: "FlowControl",
        now: float,
    ) -> Optional[str]:
        if edge_state.last_consumed_at is None:
            return None
        min_gap = 1.0 / self.rate_per_sec
        if (now - edge_state.last_consumed_at) < min_gap:
            return "penstock_limited"
        return None


class BurstPenstock(Penstock):
    """Token bucket — allow an initial burst of ``burst`` waves, then sustain ``rate_per_sec``.

    Mirrors :class:`incorporator.io.throttle.BurstThrottle` at the
    wave layer.  Bucket starts full; each refill tops it up at
    ``rate_per_sec`` tokens/sec; each consumption draws one token.
    The scheduler debits a token on a successful tick (in
    ``_tick_wrapper.finally``); ``consume_reason`` only refills + reads.
    """

    rate_per_sec: float = Field(gt=0.0, description="Refill rate (tokens / second).")
    burst: int = Field(ge=1, description="Bucket capacity — max tokens held.")

    def consume_reason(
        self,
        scheduler: "Tideweaver",
        edge_state: Any,
        flow: "FlowControl",
        now: float,
    ) -> Optional[str]:
        # First-touch initialization: bucket starts full.
        if edge_state.bucket_tokens is None:
            edge_state.bucket_tokens = float(self.burst)
            edge_state.bucket_last_refill_at = now
        else:
            elapsed = now - (edge_state.bucket_last_refill_at or now)
            edge_state.bucket_tokens = min(
                float(self.burst),
                edge_state.bucket_tokens + elapsed * self.rate_per_sec,
            )
            edge_state.bucket_last_refill_at = now
        if edge_state.bucket_tokens < 1.0:
            return "penstock_limited"
        return None


class WindowPenstock(Penstock):
    """Rolling-window quota: at most ``cap`` consumptions per ``window_sec``.

    The right shape for hard API quotas of the form "N requests per
    hour" — bursty within the window, hard wall when ``cap`` is
    reached, opens up again as the window slides forward.

    Different from :class:`BurstPenstock`: there's no refill rate.
    The window is a fixed lookback; consumptions outside it are
    forgotten.
    """

    window_sec: float = Field(gt=0.0, description="Rolling lookback window in seconds.")
    cap: int = Field(ge=1, description="Max consumptions within the window.")

    def consume_reason(
        self,
        scheduler: "Tideweaver",
        edge_state: Any,
        flow: "FlowControl",
        now: float,
    ) -> Optional[str]:
        cutoff = now - self.window_sec
        # Evict entries older than the window.  Mutation here is the
        # natural place — keeps the log bounded as the window slides.
        edge_state.window_log = [t for t in edge_state.window_log if t > cutoff]
        if len(edge_state.window_log) >= self.cap:
            return "penstock_limited"
        return None


class BackpressurePenstock(Penstock):
    """Reservoir-aware adaptive rate — the canal-toolkit synergy point.

    Reads its own edge's :class:`Reservoir` fullness to scale the
    effective rate between ``min_rate`` and ``max_rate``::

        fullness = len(edge_state.waves) / flow.reservoir.depth
        effective_rate = max_rate - (max_rate - min_rate) * fullness

    * Empty reservoir → ``max_rate`` (no backpressure; drain freely).
    * Full reservoir → ``min_rate`` (downstream is overwhelmed; slow
      consumption to let the buffer + upstream emission absorb load).

    Closes a control loop between Penstock and Reservoir — a feature
    only possible because both primitives live in the same architecture.
    """

    min_rate: float = Field(gt=0.0, description="Effective rate when reservoir is full.")
    max_rate: float = Field(gt=0.0, description="Effective rate when reservoir is empty.")

    def consume_reason(
        self,
        scheduler: "Tideweaver",
        edge_state: Any,
        flow: "FlowControl",
        now: float,
    ) -> Optional[str]:
        depth = max(1, flow.reservoir.depth)
        fullness = min(1.0, len(edge_state.waves) / depth)
        effective_rate = self.max_rate - (self.max_rate - self.min_rate) * fullness
        if effective_rate <= 0.0:
            return "penstock_limited"
        if edge_state.last_consumed_at is None:
            return None
        min_gap = 1.0 / effective_rate
        if (now - edge_state.last_consumed_at) < min_gap:
            return "penstock_limited"
        return None


class SignalPenstock(Penstock):
    """User-supplied callable returns the current rate.

    The escape hatch for everything the other strategies don't cover —
    time-of-day schedules, CPU-load-driven throttling, external-metric
    integration, mirroring a remote rate API.  The callable runs
    synchronously inside the gate cycle, so keep it cheap.

    Receives ``(scheduler, edge_state, now)``; returns the allowed
    rate in waves/sec.  A return of ``<= 0`` blocks the edge entirely
    (useful for circuit-breaker-style halts).
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    rate_fn: Callable[[Any, Any, float], float] = Field(
        description="Callable returning the current allowed rate in waves/sec.",
    )

    def consume_reason(
        self,
        scheduler: "Tideweaver",
        edge_state: Any,
        flow: "FlowControl",
        now: float,
    ) -> Optional[str]:
        rate = self.rate_fn(scheduler, edge_state, now)
        if rate <= 0.0:
            return "penstock_limited"
        if edge_state.last_consumed_at is None:
            return None
        min_gap = 1.0 / rate
        if (now - edge_state.last_consumed_at) < min_gap:
            return "penstock_limited"
        return None


# ---------------------------------------------------------------------------
# Reservoir — FIFO ring buffer of recent waves (active in Phase 2)
# ---------------------------------------------------------------------------


class Reservoir(BaseModel):
    """FIFO ring buffer of recent waves on the edge.

    Depth 1 (default) matches today's implicit single-wave snapshot.
    Deeper reservoirs absorb upstream surges and let downstream replay
    or aggregate over a window.

    Declared in Phase 1 (this commit); activated in Phase 2 when the
    scheduler's ``_edge_state`` container starts holding waves here.
    """

    model_config = ConfigDict(frozen=True)

    depth: int = Field(
        default=1,
        ge=1,
        le=1024,
        description="Max wave count to hold; sliding window when full.",
    )


# ---------------------------------------------------------------------------
# Spillway — overflow handler (DropOldest active in P1; others in P4)
# ---------------------------------------------------------------------------


class Spillway(BaseModel):
    """Decide what to do when the reservoir is full and a new wave arrives."""

    model_config = ConfigDict(frozen=True)

    def overflow(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        displaced_wave: object,
    ) -> None:
        """Handle one displaced wave.  Default subclass is a no-op."""
        return None


class DropOldest(Spillway):
    """Silently drop the displaced wave — matches today's WeakValueDict behavior.

    Default for :class:`FlowControl` so existing watersheds keep
    today's silent-drop semantics.
    """

    def overflow(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        displaced_wave: object,
    ) -> None:
        return None


class RaiseOverflow(Spillway):
    """Log + count overflows.  Never raises during a tick — diagnostic only."""

    def overflow(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        displaced_wave: object,
    ) -> None:
        # ``edge_state.overflow_count`` is bumped by the scheduler before
        # ``overflow()`` is called, so we can read it for the log line.
        state = scheduler._edge_state.get(edge)
        count = state.overflow_count if state is not None else "?"
        logger.warning(
            "Tideweaver: spillway overflow on edge %s → %s (count=%s)",
            edge[0],
            edge[1],
            count,
        )


class ExportToArchive(Spillway):
    """Route displaced waves to a strong-ref backlog on an archive class.

    Appends each displaced wave's instances to
    ``archive_cls._spillway_backlog`` — a plain Python list living on
    the class object as a strong-ref store.  A downstream :class:`Export`
    or out-of-band drainer can read the backlog and persist what would
    otherwise be lost when the reservoir slides forward.

    Why a list (not ``inc_dict``)?  ``inc_dict`` is a ``WeakValueDictionary``
    and would drop instances immediately; a backlog list holds strong refs
    so they survive until the user explicitly drains them.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    archive_cls: Type[Any] = Field(
        description="Incorporator subclass (or any class) that receives displaced wave instances.",
    )

    def overflow(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        displaced_wave: object,
    ) -> None:
        if not isinstance(displaced_wave, list):
            return None
        backlog: Optional[List[Any]] = getattr(self.archive_cls, "_spillway_backlog", None)
        if backlog is None:
            backlog = []
            self.archive_cls._spillway_backlog = backlog
        backlog.extend(displaced_wave)


# ---------------------------------------------------------------------------
# FlowControl — per-edge composition of all primitives
# ---------------------------------------------------------------------------


class FlowControl(BaseModel):
    """Per-edge flow control composed from the canal primitives.

    Defaults match today's behavior exactly:

    * ``gate=HardLock()`` — hard gating
    * ``penstock=None`` — no rate limit
    * ``reservoir=Reservoir(depth=1)`` — single-wave snapshot (today's)
    * ``spillway=DropOldest()`` — silent drop on overflow (today's)
    * ``surge_barrier=None`` — no skip-ahead; opt in to extreme-condition handling

    To match today's hard-mode default (which embedded skip-ahead at
    ``skip_threshold=2.0`` on :class:`~.current.Current`),
    :func:`flow_from_mode("hard")` returns
    ``FlowControl(gate=HardLock(), surge_barrier=SurgeBarrier())``.
    Weir and soft modes default to ``surge_barrier=None`` — they don't
    pre-block on in-flight upstream, so the barrier is moot for them.
    """

    model_config = ConfigDict(frozen=True)

    gate: Gate = Field(default_factory=HardLock)
    penstock: Optional[Penstock] = None
    reservoir: Reservoir = Field(default_factory=Reservoir)
    spillway: Spillway = Field(default_factory=DropOldest)
    surge_barrier: Optional[SurgeBarrier] = None


# ---------------------------------------------------------------------------
# Mode-string shorthand — the user-facing API surface
# ---------------------------------------------------------------------------


GateMode = Literal["hard", "soft", "weir"]


_GATE_BY_MODE: Dict[str, Type[Gate]] = {
    "hard": HardLock,
    "soft": SoftPass,
    "weir": Weir,
}


def flow_from_mode(mode: GateMode) -> FlowControl:
    """Build a :class:`FlowControl` with mode-appropriate defaults.

    Hard mode includes a default :class:`SurgeBarrier` (threshold=2.0,
    action="skip") so it matches today's behavior end-to-end.  Weir and
    soft modes don't include a SurgeBarrier — they're already permissive
    about in-flight upstream.
    """
    gate_cls = _GATE_BY_MODE.get(mode)
    if gate_cls is None:
        raise ValueError(f"unknown GateMode: {mode!r} (expected one of {sorted(_GATE_BY_MODE)})")
    if mode == "hard":
        return FlowControl(gate=HardLock(), surge_barrier=SurgeBarrier())
    return FlowControl(gate=gate_cls())


__all__ = [
    "BackpressurePenstock",
    "BurstPenstock",
    "DropOldest",
    "ExportToArchive",
    "FlowControl",
    "Gate",
    "GateMode",
    "HardLock",
    "Penstock",
    "RaiseOverflow",
    "Reservoir",
    "SignalPenstock",
    "SoftPass",
    "Spillway",
    "SurgeAction",
    "SurgeBarrier",
    "SustainedPenstock",
    "Weir",
    "WindowPenstock",
    "flow_from_mode",
]
