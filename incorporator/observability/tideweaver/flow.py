"""Edge flow-control primitives — locks, weirs, penstocks, reservoirs, spillways.

Real canal/dam engineering models every passage point with the same five
primitives this module exposes, composed by :class:`FlowControl`:

* :class:`Gate` (subclasses :class:`HardLock`, :class:`SoftPass`,
  :class:`Weir`) — pass/hold decision per pass.
* :class:`SurgeBarrier` — conditional override when upstream is in-flight
  beyond a threshold (storm-surge barrier semantics).
* :class:`Penstock` — flow-rate limit (waves/sec) across the edge.
* :class:`Reservoir` — FIFO buffer of recent waves; absorbs upstream
  surges and supports replay.
* :class:`Spillway` (subclasses :class:`DropOldest`, :class:`RaiseOverflow`,
  :class:`ExportToArchive`) — overflow handler when the reservoir fills.

``Current.phase_offset_sec`` (green-wave coordination) lives on
:class:`~.current.Current` rather than here — it's a property of the
dependent's tick schedule, not of one edge.

:class:`FlowControl` composes the five edge-level primitives into the
single per-edge config object the scheduler reads.  Mirrors the
``ThrottleStrategy`` split in :mod:`incorporator.io.throttle` (sluices,
flow-rate) with a parallel hierarchy here for pass/hold decisions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Literal, Optional, Tuple, Type

from pydantic import BaseModel, ConfigDict, Field

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
# Penstock — flow-rate limit at the edge layer (active in Phase 3)
# ---------------------------------------------------------------------------


class Penstock(BaseModel):
    """Flow-rate limit at the edge layer.

    Different from :class:`~incorporator.io.throttle.ThrottleStrategy`:
    that limits HTTP calls per host; this limits *wave consumption*
    across one Watershed edge.  A slow downstream can throttle how fast
    it sucks waves from a hot upstream without affecting upstream's
    emission rate.

    Declared in Phase 1 (this commit); activated in Phase 3 when the
    scheduler honors ``rate_per_sec`` in its gating cycle.
    """

    model_config = ConfigDict(frozen=True)

    rate_per_sec: float = Field(
        gt=0.0,
        description="Max wave consumptions per second across this edge.",
    )


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
    """Log + count overflows.  Never raises during a tick (Phase 4 activates the log)."""

    def overflow(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        displaced_wave: object,
    ) -> None:
        # Concrete implementation lands in Phase 4 — bumps state.overflow_count
        # and emits a logger.warning.  Today this is still a no-op.
        return None


class ExportToArchive(Spillway):
    """Route displaced waves to an archive class's ``inc_dict``.

    Declared in Phase 1; concrete in Phase 4.  When active, the
    displaced wave's instances are re-registered under ``archive_cls``
    so an out-of-band drain (an Export current downstream, a daily
    Parquet snapshot, etc.) can persist what would otherwise be lost.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    archive_cls: Type[object] = Field(
        description="Incorporator subclass that receives displaced wave instances.",
    )

    def overflow(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        displaced_wave: object,
    ) -> None:
        # Phase 4 will rewrite this to register instances onto
        # ``self.archive_cls.inc_dict``.  Today no-op so the class is
        # constructible without changing behavior.
        return None


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
    "DropOldest",
    "ExportToArchive",
    "FlowControl",
    "Gate",
    "GateMode",
    "HardLock",
    "Penstock",
    "RaiseOverflow",
    "Reservoir",
    "SoftPass",
    "Spillway",
    "SurgeAction",
    "SurgeBarrier",
    "Weir",
    "flow_from_mode",
]
