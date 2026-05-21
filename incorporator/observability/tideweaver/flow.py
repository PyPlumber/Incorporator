"""Edge flow-control primitives — gates, surge barriers, penstocks, reservoirs, spillways.

:class:`FlowControl` composes five orthogonal per-edge primitives:
gating (:class:`Gate`), conditional override (:class:`SurgeBarrier`),
flow-rate limiting (:class:`Penstock`), wave buffering
(:class:`Reservoir`), and overflow handling (:class:`Spillway`).

:class:`Penstock` and its concrete subclasses (:class:`SustainedPenstock`,
:class:`BurstPenstock`, :class:`WindowPenstock`, :class:`SignalPenstock`,
:class:`NullPenstock`) live in :mod:`incorporator.io.penstock` — the
canal-toolkit vocabulary is shared with the HTTP throttle layer.  This
module re-exports them and adds the edge-only
:class:`BackpressurePenstock` (which reads reservoir context).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any, Callable, Dict, List, Literal, Optional, Tuple, Type, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ...io.penstock import (
    BurstPenstock,
    NullPenstock,
    Penstock,
    SignalPenstock,
    SustainedPenstock,
    WindowPenstock,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .scheduler import Tideweaver


# ---------------------------------------------------------------------------
# Narrow context value types passed to per-edge strategies.  Drop the
# scheduler-as-arg pattern so strategies can be unit-tested without one
# and the scheduler-to-strategy boundary stops leaking in both directions.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GateContext:
    """Four facts a :class:`Gate` reads to decide pass/hold on one upstream edge.

    Built by the scheduler from ``_state`` + ``_last_consumed``; passed
    to :meth:`Gate.gate_reason`.  Gates do not see (or need) the
    scheduler itself.
    """

    up_in_flight: bool
    up_last_wave_at: Optional[datetime]
    last_consumed: Optional[datetime]
    now: float


@dataclass(frozen=True)
class SurgeContext:
    """Facts :class:`SurgeBarrier` reads to decide whether to override the gate."""

    up_in_flight: bool
    up_started_at: Optional[float]
    dependent_interval: float
    now: float


# ---------------------------------------------------------------------------
# Gate hierarchy — pass/hold decision per upstream
# ---------------------------------------------------------------------------


class Gate(BaseModel):
    """Pass/hold decision for one upstream of a dependent current."""

    model_config = ConfigDict(frozen=True)

    def gate_reason(self, ctx: GateContext) -> Optional[str]:
        """Return a skip reason, or ``None`` to allow firing."""
        raise NotImplementedError


class HardLock(Gate):
    """Blocks until upstream emits a fresh wave and is not in-flight."""

    type: Literal["hard"] = "hard"

    def gate_reason(self, ctx: GateContext) -> Optional[str]:
        if ctx.up_in_flight:
            return "awaiting_upstream"
        if ctx.up_last_wave_at is None:
            return "awaiting_upstream"
        if ctx.last_consumed is not None and ctx.last_consumed >= ctx.up_last_wave_at:
            return "awaiting_upstream"
        return None


class SoftPass(Gate):
    """Open channel: no gating; downstream fires on its own cadence."""

    type: Literal["soft"] = "soft"

    def gate_reason(self, ctx: GateContext) -> Optional[str]:
        return None


class Weir(Gate):
    """Requires a fresh wave but ignores upstream in-flight state — dependent fires on its own cadence."""

    type: Literal["weir"] = "weir"

    def gate_reason(self, ctx: GateContext) -> Optional[str]:
        if ctx.up_last_wave_at is None:
            return "awaiting_upstream"
        if ctx.last_consumed is not None and ctx.last_consumed >= ctx.up_last_wave_at:
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

    def is_tripped(self, ctx: SurgeContext) -> bool:
        """Return True iff upstream has been in-flight longer than threshold * interval."""
        if not ctx.up_in_flight or ctx.up_started_at is None:
            return False
        return (ctx.now - ctx.up_started_at) > self.threshold_multiple * ctx.dependent_interval


# ---------------------------------------------------------------------------
# Penstock hierarchy — edge-layer flow-rate strategies.
#
# Penstock + Null/Sustained/Burst/Window/Signal are imported at the top
# of the module from incorporator.io.penstock; only the edge-specific
# BackpressurePenstock is defined here, since it reads reservoir context.
# ---------------------------------------------------------------------------


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

    def consume_reason(self, edge_state: Any, flow: "FlowControl", now: float) -> Optional[str]:
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
        edge: Tuple[str, str],
        displaced_wave: object,
        overflow_count: int,
    ) -> None:
        """Handle one displaced wave.  Default subclass is a no-op."""
        return None


class DropOldest(Spillway):
    """Silently drop the displaced wave."""

    type: Literal["drop_oldest"] = "drop_oldest"

    def overflow(
        self,
        edge: Tuple[str, str],
        displaced_wave: object,
        overflow_count: int,
    ) -> None:
        return None


class RaiseOverflow(Spillway):
    """Log every overflow at WARNING level (never raises)."""

    type: Literal["raise_overflow"] = "raise_overflow"

    def overflow(
        self,
        edge: Tuple[str, str],
        displaced_wave: object,
        overflow_count: int,
    ) -> None:
        logger.warning(
            "Tideweaver: spillway overflow on edge %s → %s (count=%d)",
            edge[0],
            edge[1],
            overflow_count,
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
        edge: Tuple[str, str],
        displaced_wave: object,
        overflow_count: int,
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
    "NullPenstock",
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
