"""Per-current outcome record for one Tideweaver scheduler pass.

Kept in its own module so it can be imported by both ``tide.py`` and
``scheduler.py`` without introducing a module-graph cycle.  Imports only
stdlib — no Pydantic, no incorporator internals.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class CurrentOutcome:
    """Structured outcome for one current in a scheduler pass.

    Plain slotted dataclass — NOT a Pydantic model — so per-pass
    construction costs roughly 200 ns each instead of 500 ns – 2 µs.
    At N=50 currents per pass that's roughly 10 µs/pass total, well
    under the savings from switching ``Tide(...)`` to
    ``Tide.model_construct(...)``.

    Attributes:
        name: Current name from :attr:`~incorporator.observability.tideweaver.current.Current.name`.
        status: Outcome category — one of ``"fired"``, ``"skipped"``,
            or ``"still_running"``.
        reason: Human-readable skip or gate reason (e.g. ``"not_due"``,
            ``"still_running"``, ``"awaiting_upstream"``).  ``None`` for
            fired currents.
        bypassed_edges: Upstream names whose :class:`~incorporator.observability.tideweaver.flow.SurgeBarrier`
            tripped with ``action="bypass"`` this pass.  Empty tuple when
            none were bypassed.
        in_flight_sec: Seconds the current has been in-flight at pass
            start.  Populated only when ``status="still_running"`` and
            the monotonic start timestamp is recorded.
        last_wave_at: UTC timestamp of the most recent wave emitted by
            this current.  ``None`` if the current has never fired.
        parent_snapshot_size: Upstream snapshot row count consumed by
            a parent-child tick (Stream with ``parent_current`` set or
            Fjord with ``parent_currents`` populated).  ``None`` for
            ticks without parent-child semantics.
        filter_match_count: Row count after applying the parent-child
            filter (``parent_filter`` / ``parent_filters``).  ``None``
            when no filter is set or for non-parent-child ticks.  Zero
            indicates the filter matched no rows.

    Additional ``reason`` values the scheduler may set on the outcome
    when a parent-child tick was skipped:

    - ``"parent_snapshot_empty"`` — upstream snapshot was None or empty;
      the tick body skipped because there was nothing to drill.
    - ``"filter_matched_zero"`` — upstream had rows but the filter
      matched zero of them; the tick body skipped after applying the
      predicate.
    """

    name: str
    status: str
    reason: str | None = None
    bypassed_edges: tuple[str, ...] = ()
    in_flight_sec: float | None = None
    last_wave_at: datetime | None = None
    parent_snapshot_size: int | None = None
    filter_match_count: int | None = None

    def __str__(self) -> str:
        """Compact log-friendly representation.

        Returns:
            ``"name:status"`` when reason is absent; ``"name:status(reason)"``
            when reason is present.
        """
        if self.reason is not None:
            return f"{self.name}:{self.status}({self.reason})"
        return f"{self.name}:{self.status}"
