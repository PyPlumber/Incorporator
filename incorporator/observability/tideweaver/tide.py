"""The :class:`Tide` log record — one per :class:`Tideweaver` scheduler pass.

Mirrors :class:`~incorporator.Wave`'s shape so it routes through the same
``observability/logger.py`` machinery (``setup_class_logger``, ``JSONFormatter``).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Tuple

from pydantic import BaseModel, ConfigDict, Field


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
        duration_sec: Wall-clock duration of the pass in seconds.
        timestamp: UTC timestamp when the pass completed.

    Frozen Pydantic model — instances can be passed around without
    worrying about mutation, and the shape mirrors :class:`Wave` so
    ``Tide`` records route through the same
    ``observability/logger.py`` machinery.
    """

    model_config = ConfigDict(frozen=True)

    tide_number: int = Field(..., description="Monotonic 1-indexed counter of scheduler passes.")
    fired: List[str] = Field(default_factory=list, description="Names of currents that produced a wave.")
    skipped: List[Tuple[str, str]] = Field(
        default_factory=list,
        description="(current_name, reason) pairs for currents gated out this pass.",
    )
    duration_sec: float = Field(..., description="Wall-clock duration of the pass in seconds.")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def log_meta(self) -> str:
        """Compact, single-line meta string mirroring :meth:`Wave.log_meta`.

        Used by the existing log-routing machinery so ``Tide`` records share
        the flat ``meta`` shape with instance-level log records.
        """
        return (
            f"tide_number:{self.tide_number}, fired:{len(self.fired)}, "
            f"skipped:{len(self.skipped)}, duration_sec:{self.duration_sec:.3f}"
        )
