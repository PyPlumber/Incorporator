"""The :class:`Tide` log record — one per :class:`Tideweaver` scheduler pass.

Mirrors :class:`~incorporator.Wave`'s shape so it routes through the same
``observability/logger.py`` machinery (``setup_class_logger``, ``JSONFormatter``).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Tuple

from pydantic import BaseModel, ConfigDict, Field


class Tide(BaseModel):
    """One scheduler pass: which currents fired, which were skipped, how long it took.

    A :class:`Tideweaver` emits one ``Tide`` per pass through its currents
    list.  ``fired`` lists the currents that produced a wave this pass;
    ``skipped`` records the currents that were eligible (in topological
    order) but gated out, paired with a short reason string
    (``"not_due"``, ``"awaiting_upstream"``, ``"skip_ahead"``).

    The model is frozen so callers can pass instances around without
    worrying about mutation.
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
