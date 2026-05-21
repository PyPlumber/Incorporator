"""Edge flow-control primitives — gates, surge barriers, penstocks, reservoirs, spillways.

:class:`FlowControl` composes five orthogonal per-edge primitives:
gating (:class:`Gate`), conditional override (:class:`SurgeBarrier`),
flow-rate limiting (:class:`Penstock`), wave buffering (:class:`Reservoir`),
and overflow handling (:class:`Spillway`).  HTTP-layer host throttles
in :mod:`incorporator.io.throttle` are a separate concern: they cap
requests per host, Penstocks here cap wave consumption per edge.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Annotated, Any, Callable, Dict, List, Literal, Optional, Tuple, Type, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .current import Current
    from .scheduler import Tideweaver


# ---------------------------------------------------------------------------
# Gate hierarchy — pass/hold decision per upstream
# ---------------------------------------------------------------------------


class Gate(BaseModel):
    """Pass/hold decision for one upstream of a dependent current."""

    model_config = ConfigDict(frozen=True)

    def gate_reason(
        self,
        scheduler: "Tideweaver",
        dependent: "Current",
        up_name: str,
        now: float,
    ) -> Optional[str]:
        """Return a skip reason, or ``None`` to allow firing."""
        raise NotImplementedError


class HardLock(Gate):
    """Blocks until upstream emits a fresh wave and is not in-flight."""

    type: Literal["hard"] = "hard"

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

    type: Literal["soft"] = "soft"

    def gate_reason(
        self,
        scheduler: "Tideweaver",
        dependent: "Current",
        up_name: str,
        now: float,
    ) -> Optional[str]:
        return None


class Weir(Gate):
    """Requires a fresh wave but ignores upstream in-flight state — dependent fires on its own cadence."""

    type: Literal["weir"] = "weir"

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
    """Overrides the gate when upstream has been in-flight longer than ``threshold_multiple * dependent.interval``.

    Actions when tripped:

    * ``"skip"`` — dependent skips this pass (reason ``"skip_ahead"``).
    * ``"bypass"`` — dependent fires unconditionally, ignoring the gate for this upstream.
    * ``"halt"`` — dependent stops firing (reason ``"surge_halted"``).
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
    """Edge-layer flow-rate strategy: caps wave consumption per edge."""

    model_config = ConfigDict(frozen=True)

    def consume_reason(
        self,
        scheduler: "Tideweaver",
        edge_state: Any,
        flow: "FlowControl",
        now: float,
    ) -> Optional[str]:
        """Return a skip reason, or ``None`` to allow consumption.  May mutate ``edge_state``."""
        raise NotImplementedError


class SustainedPenstock(Penstock):
    """Leaky bucket: minimum gap ``1 / rate_per_sec`` between consumptions."""

    type: Literal["sustained"] = "sustained"
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
    """Token bucket: initial burst of ``burst`` waves, then refills at ``rate_per_sec`` tokens/sec."""

    type: Literal["burst"] = "burst"
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
    """Rolling-window quota: at most ``cap`` consumptions per ``window_sec``."""

    type: Literal["window"] = "window"
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
    """Rate scales with reservoir fullness: empty → ``max_rate``, full → ``min_rate``.

    ``effective_rate = max_rate - (max_rate - min_rate) * fullness``
    where ``fullness = len(edge_state.waves) / flow.reservoir.depth``.
    """

    type: Literal["backpressure"] = "backpressure"
    min_rate: float = Field(gt=0.0, description="Effective rate when reservoir is full.")
    max_rate: float = Field(gt=0.0, description="Effective rate when reservoir is empty.")

    @model_validator(mode="after")
    def _check_rate_ordering(self) -> "BackpressurePenstock":
        """Reject inverted ``min_rate``/``max_rate`` — the formula assumes ``min_rate < max_rate``.

        ``effective_rate = max_rate - (max_rate - min_rate) * fullness`` only
        produces the intended backpressure curve (slower as the reservoir
        fills) when ``min_rate < max_rate``.  Swapped values silently invert
        the semantics — a full reservoir gets a *higher* effective rate than
        an empty one.  Reject at construction time.
        """
        if self.min_rate >= self.max_rate:
            raise ValueError(
                f"BackpressurePenstock requires min_rate < max_rate "
                f"(got min_rate={self.min_rate}, max_rate={self.max_rate}); "
                "min_rate is the floor when the reservoir is full, "
                "max_rate is the ceiling when it is empty."
            )
        return self

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
    """User callable returns the current rate.

    ``rate_fn(scheduler, edge_state, now) -> float`` runs in the gate
    cycle; a return ``<= 0`` blocks the edge entirely.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    type: Literal["signal"] = "signal"
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
# Reservoir — FIFO ring buffer of recent waves
# ---------------------------------------------------------------------------


class Reservoir(BaseModel):
    """FIFO ring buffer of recent waves on the edge.  Depth 1 holds only the latest wave."""

    model_config = ConfigDict(frozen=True)

    depth: int = Field(
        default=1,
        ge=1,
        le=1024,
        description="Max wave count to hold; sliding window when full.",
    )


# ---------------------------------------------------------------------------
# Spillway — overflow handler for reservoir displacement
# ---------------------------------------------------------------------------


class Spillway(BaseModel):
    """Handle a displaced wave when the reservoir overflows."""

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
    """Silently drop the displaced wave."""

    type: Literal["drop_oldest"] = "drop_oldest"

    def overflow(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        displaced_wave: object,
    ) -> None:
        return None


class RaiseOverflow(Spillway):
    """Log every overflow at WARNING level (never raises)."""

    type: Literal["raise_overflow"] = "raise_overflow"

    def overflow(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        displaced_wave: object,
    ) -> None:
        state = scheduler._edge_state.get(edge)
        count = state.overflow_count if state is not None else "?"
        logger.warning(
            "Tideweaver: spillway overflow on edge %s → %s (count=%s)",
            edge[0],
            edge[1],
            count,
        )


class ExportToArchive(Spillway):
    """Append each displaced wave's instances to ``archive_cls._spillway_backlog`` (a strong-ref list)."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    type: Literal["export_to_archive"] = "export_to_archive"
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
# FlowObserver — per-edge lifecycle hook for declarative telemetry
# ---------------------------------------------------------------------------


class FlowObserver(BaseModel):
    """Per-edge lifecycle observer — declarative telemetry channel.

    Optional sixth :class:`FlowControl` primitive.  The scheduler calls
    one of four hooks at every per-edge event:

    * :meth:`on_fire` — dependent's tick fired with this upstream's wave.
    * :meth:`on_skip` — the gate / penstock / surge barrier on this edge
      returned a skip reason (``"awaiting_upstream"`` / ``"penstock_limited"``
      / ``"skip_ahead"`` / ``"surge_halted"``).
    * :meth:`on_spillway` — a wave was displaced from a full reservoir on
      this edge (fires after :meth:`Spillway.overflow`).
    * :meth:`on_reservoir_level` — the reservoir was appended to; ``used``
      / ``capacity`` describe the post-append occupancy.

    Hooks are **synchronous and cheap** — the scheduler does not ``await``
    them.  Slow work should be queued / dispatched off-thread by the
    observer subclass.

    Default subclass is the no-op base.  Concrete options ship as
    :class:`NullObserver` (explicit-default, identical to base),
    :class:`LoggingObserver` (per-event ``logging`` emission with
    configurable levels), and :class:`SignalObserver` (user callable for
    metric pipelines).
    """

    model_config = ConfigDict(frozen=True)

    def on_fire(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        wave_number: int,
    ) -> None:
        """Dependent fired this pass — upstream's wave contributed to the tick."""
        return None

    def on_skip(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        reason: str,
    ) -> None:
        """This edge produced ``reason`` — its gate / penstock / surge barrier blocked the dependent."""
        return None

    def on_spillway(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        displaced_wave: object,
        overflow_count: int,
    ) -> None:
        """A wave was displaced from a full reservoir on this edge."""
        return None

    def on_reservoir_level(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        used: int,
        capacity: int,
    ) -> None:
        """The reservoir was appended to; ``used`` / ``capacity`` are the post-append occupancy."""
        return None


class NullObserver(FlowObserver):
    """Default — emit nothing.  Cheap function-call overhead per event."""

    type: Literal["null"] = "null"


_LogLevel = Literal["debug", "info", "warning"]


class LoggingObserver(FlowObserver):
    """Emit per-event records through Python ``logging`` at configurable levels.

    Per-event level defaults match the legacy hand-rolled emissions:
    fire/skip at DEBUG (most users don't want this in production),
    spillway at WARNING (mirrors today's :class:`RaiseOverflow`), and
    reservoir-level at DEBUG with an optional fraction threshold.

    Records carry a ``meta`` line in the same flat ``key: value`` shape
    other Tideweaver telemetry uses, suitable for downstream JSONL
    ingestion.
    """

    type: Literal["logging"] = "logging"
    fire_level: _LogLevel = "debug"
    skip_level: _LogLevel = "debug"
    spillway_level: _LogLevel = "warning"
    reservoir_level_level: _LogLevel = "debug"
    reservoir_threshold: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Only emit on_reservoir_level when used/capacity >= this fraction.",
    )

    @staticmethod
    def _level_to_int(level: _LogLevel) -> int:
        return {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING}[level]

    def on_fire(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        wave_number: int,
    ) -> None:
        logger.log(
            self._level_to_int(self.fire_level),
            "Tideweaver: edge %s → %s fired (wave=%d)",
            edge[0],
            edge[1],
            wave_number,
        )

    def on_skip(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        reason: str,
    ) -> None:
        logger.log(
            self._level_to_int(self.skip_level),
            "Tideweaver: edge %s → %s skipped (reason=%s)",
            edge[0],
            edge[1],
            reason,
        )

    def on_spillway(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        displaced_wave: object,
        overflow_count: int,
    ) -> None:
        logger.log(
            self._level_to_int(self.spillway_level),
            "Tideweaver: spillway overflow on edge %s → %s (count=%d)",
            edge[0],
            edge[1],
            overflow_count,
        )

    def on_reservoir_level(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        used: int,
        capacity: int,
    ) -> None:
        if capacity == 0:
            return None
        fraction = used / capacity
        if fraction < self.reservoir_threshold:
            return None
        logger.log(
            self._level_to_int(self.reservoir_level_level),
            "Tideweaver: reservoir on edge %s → %s at %d/%d (%.1f%%)",
            edge[0],
            edge[1],
            used,
            capacity,
            fraction * 100.0,
        )


class SignalObserver(FlowObserver):
    """Forward every event to a user callable.

    The callable receives ``(event_kind, edge, payload_dict)`` where
    ``event_kind`` is one of ``"fire"`` / ``"skip"`` / ``"spillway"`` /
    ``"reservoir_level"`` and ``payload_dict`` carries the per-event
    data (``wave_number`` / ``reason`` / ``displaced_wave``+``overflow_count``
    / ``used``+``capacity``).

    String-form ``callback`` in ``watershed.json`` (e.g. ``"my_metrics_sink"``
    or ``"module.path:func"``) is resolved by the config loader at load
    time, matching :class:`SignalPenstock.rate_fn`.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    type: Literal["signal"] = "signal"
    callback: Callable[[str, Tuple[str, str], Dict[str, Any]], None] = Field(
        description="Callable invoked once per per-edge event.",
    )

    def on_fire(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        wave_number: int,
    ) -> None:
        self.callback("fire", edge, {"wave_number": wave_number})

    def on_skip(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        reason: str,
    ) -> None:
        self.callback("skip", edge, {"reason": reason})

    def on_spillway(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        displaced_wave: object,
        overflow_count: int,
    ) -> None:
        self.callback(
            "spillway",
            edge,
            {"displaced_wave": displaced_wave, "overflow_count": overflow_count},
        )

    def on_reservoir_level(
        self,
        scheduler: "Tideweaver",
        edge: Tuple[str, str],
        used: int,
        capacity: int,
    ) -> None:
        self.callback("reservoir_level", edge, {"used": used, "capacity": capacity})


# ---------------------------------------------------------------------------
# FlowControl — per-edge composition of all primitives
# ---------------------------------------------------------------------------


_GateUnion = Annotated[
    Union[HardLock, SoftPass, Weir],
    Field(discriminator="type"),
]
_PenstockUnion = Annotated[
    Union[SustainedPenstock, BurstPenstock, WindowPenstock, BackpressurePenstock, SignalPenstock],
    Field(discriminator="type"),
]
_SpillwayUnion = Annotated[
    Union[DropOldest, RaiseOverflow, ExportToArchive],
    Field(discriminator="type"),
]
_ObserverUnion = Annotated[
    Union[NullObserver, LoggingObserver, SignalObserver],
    Field(discriminator="type"),
]


class FlowControl(BaseModel):
    """Per-edge composition of gate / penstock / reservoir / spillway / surge_barrier / observer.

    Defaults: ``HardLock`` gate, ``Reservoir(depth=1)``, ``DropOldest``
    spillway, ``NullObserver`` (no-op telemetry), no penstock, no surge
    barrier.

    ``gate`` / ``penstock`` / ``spillway`` / ``observer`` are Pydantic
    discriminated unions keyed on each strategy's ``type`` Literal —
    JSON dicts like ``{"gate": {"type": "weir"}, "observer": {"type":
    "logging", "spillway_level": "info"}}`` deserialize directly via
    :meth:`model_validate`.
    """

    model_config = ConfigDict(frozen=True)

    gate: _GateUnion = Field(default_factory=HardLock)
    penstock: Optional[_PenstockUnion] = None
    reservoir: Reservoir = Field(default_factory=Reservoir)
    spillway: _SpillwayUnion = Field(default_factory=DropOldest)
    surge_barrier: Optional[SurgeBarrier] = None
    observer: _ObserverUnion = Field(default_factory=NullObserver)


# ---------------------------------------------------------------------------
# Mode-string shorthand — the user-facing API surface
# ---------------------------------------------------------------------------


GateMode = Literal["hard", "soft", "weir"]


def flow_from_mode(mode: GateMode) -> FlowControl:
    """Build a :class:`FlowControl` for ``"hard"``, ``"soft"``, or ``"weir"``.

    ``"hard"`` attaches a default :class:`SurgeBarrier` (threshold 2.0,
    action ``"skip"``); the others leave ``surge_barrier=None``.  Branches
    rather than dict-dispatches because the ``"hard"`` mode bundles a
    SurgeBarrier that the other two modes don't ship.
    """
    if mode == "hard":
        return FlowControl(gate=HardLock(), surge_barrier=SurgeBarrier())
    if mode == "soft":
        return FlowControl(gate=SoftPass())
    if mode == "weir":
        return FlowControl(gate=Weir())
    raise ValueError(f"unknown GateMode: {mode!r} (expected one of ['hard', 'soft', 'weir'])")


__all__ = [
    "BackpressurePenstock",
    "BurstPenstock",
    "DropOldest",
    "ExportToArchive",
    "FlowControl",
    "FlowObserver",
    "Gate",
    "GateMode",
    "HardLock",
    "LoggingObserver",
    "NullObserver",
    "Penstock",
    "RaiseOverflow",
    "Reservoir",
    "SignalObserver",
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
