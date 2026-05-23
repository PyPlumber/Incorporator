"""The :class:`Tide` log record — one per :class:`Tideweaver` scheduler pass.

Mirrors :class:`~incorporator.Wave`'s shape so it routes through the same
``observability/logger.py`` machinery (``setup_class_logger``, ``JSONFormatter``).
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, field_serializer

from .current_outcome import CurrentOutcome


class Tide(BaseModel):
    """The per-pass log record yielded by :meth:`Tideweaver.run` — use it to diagnose orchestration scheduling.

    Inspect each ``Tide`` to see which currents fired this pass,
    which got gated (and why), and how long the pass took — the
    primary signal for debugging a windowed daemon that is dropping
    ticks or stalling on hard edges:

    .. code-block:: python

        async for tide in Tideweaver(watershed).run():
            print(
                f"Pass {tide.tide_number}: fired={tide.fired}, "
                f"skipped={tide.skipped}, dt={tide.duration_sec:.2f}s"
            )

    Attributes:
        tide_number: Monotonic 1-indexed counter of scheduler passes.
        fired: Names of currents that produced a wave this pass.
        skipped: ``(name, reason)`` pairs for currents gated out;
            reasons are ``"not_due"`` (interval not elapsed),
            ``"still_running"`` (previous tick still in flight),
            ``"awaiting_upstream"`` (hard edge waiting on a fresh
            upstream wave), or ``"skip_ahead"`` (upstream in-flight
            tick exceeded the skip threshold).
        current_outcomes: Structured per-current outcome list for this
            pass.  Each entry is a :class:`CurrentOutcome` slotted
            dataclass carrying ``name``, ``status``,
            ``reason``, ``bypassed_edges``, ``in_flight_sec``, and
            ``last_wave_at``.  Populated in the same loop that builds
            ``fired`` and ``skipped`` — no second pass over currents.
        duration_sec: Wall-clock duration of the pass in seconds.
        wake_reason: What triggered this pass — one of ``"startup"``
            (first pass), ``"timer"`` (heap due-time elapsed),
            ``"wake_event"`` (upstream tick completed, set
            ``_wake_event``), ``"pass_interval"`` (safety-net
            ``pass_interval`` timeout with empty heap), or
            ``"shutdown"`` (window-close event fired).
        heap_depth: Number of entries remaining in the due-time heap
            after housekeeping.  Growth over time signals that currents
            are being added faster than they fire.
        in_flight_count_at_start: Number of in-flight tasks observed
            at the start of the pass body, before the topo-walk loop.
            High counts combined with ``wake_reason="pass_interval"``
            suggest raising ``pass_interval``.
        canal_rejects_added: Count of new canal-layer
            :class:`~incorporator.RejectEntry` records added to the
            scheduler's accumulator during this pass.  Equivalent to
            ``len(tideweaver.rejects)`` delta between passes.
        next_due_in_sec: Seconds until the nearest heap entry fires,
            computed at pass end.  ``None`` when the heap is empty
            (all currents are data-gated, not timer-gated).
        timestamp: UTC timestamp when the pass completed.

    Frozen Pydantic model — instances can be passed around without
    worrying about mutation, and the shape mirrors :class:`Wave` so
    ``Tide`` records route through the same
    ``observability/logger.py`` machinery.

    ``model_config`` includes ``arbitrary_types_allowed=True`` because
    :class:`CurrentOutcome` is a plain dataclass, not a Pydantic model.
    A ``@field_serializer`` converts the list to dicts for
    ``model_dump(mode="json")``.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    tide_number: int = Field(..., description="Monotonic 1-indexed counter of scheduler passes.")
    fired: List[str] = Field(default_factory=list, description="Names of currents that produced a wave.")
    skipped: List[Tuple[str, str]] = Field(
        default_factory=list,
        description="(current_name, reason) pairs for currents gated out this pass.",
    )
    current_outcomes: List[CurrentOutcome] = Field(
        default_factory=list,
        description="Structured per-current outcome list for this pass.",
    )
    duration_sec: float = Field(..., description="Wall-clock duration of the pass in seconds.")
    wake_reason: str = Field(
        default="startup",
        description=("What triggered this pass: 'startup', 'timer', 'wake_event', 'pass_interval', or 'shutdown'."),
    )
    heap_depth: int = Field(default=0, description="Due-time heap entries remaining after housekeeping.")
    in_flight_count_at_start: int = Field(
        default=0,
        description="In-flight task count at pass start, before the topo-walk loop.",
    )
    canal_rejects_added: int = Field(
        default=0,
        description="Canal-layer RejectEntry records added to the accumulator during this pass.",
    )
    next_due_in_sec: Optional[float] = Field(
        default=None,
        description="Seconds until the nearest heap entry fires; None when the heap is empty.",
    )
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_serializer("current_outcomes")
    def _serialize_current_outcomes(self, outcomes: List[CurrentOutcome]) -> List[Dict[str, Any]]:
        """Serialize :class:`CurrentOutcome` dataclasses to plain dicts for JSON output.

        Pydantic v2 calls ``@field_serializer`` at class level — independent
        of validation — so it fires correctly under both ``Tide(...)`` and
        ``Tide.model_construct(...)``.

        Args:
            outcomes: The list of :class:`CurrentOutcome` instances to serialize.

        Returns:
            A list of plain dicts suitable for JSON serialization.
        """
        return [dataclasses.asdict(o) for o in outcomes]

    def log_meta(self) -> str:
        """Compact, single-line meta string mirroring :meth:`Wave.log_meta`.

        Used by the existing log-routing machinery so ``Tide`` records share
        the flat ``meta`` shape with instance-level log records.

        Returns:
            A single-line string with key scheduling metrics.
        """
        return (
            f"tide_number:{self.tide_number}, fired:{len(self.fired)}, "
            f"skipped:{len(self.skipped)}, duration_sec:{self.duration_sec:.3f}"
        )
