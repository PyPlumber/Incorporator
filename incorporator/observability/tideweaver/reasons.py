"""Scheduler-event enums shared across the Tideweaver sub-package.

Kept in a dedicated module so ``tide.py`` and ``scheduler.py`` can both import
them without pulling in each other's heavier dependencies.
"""

from __future__ import annotations

from enum import Enum


class SkipReason(str, Enum):
    """Why a current was not fired during a scheduler pass.

    ``str``-subclass so ``SkipReason.SURGE_HALTED == "surge_halted"`` is
    ``True`` — existing membership tests and comparison code keep working
    without modification, and Pydantic v2 serialises the value (not the
    name) automatically.
    """

    STILL_RUNNING = "still_running"
    NOT_DUE = "not_due"
    PHASE_OFFSET = "phase_offset"
    AWAITING_UPSTREAM = "awaiting_upstream"
    SKIP_AHEAD = "skip_ahead"
    SURGE_HALTED = "surge_halted"
    PENSTOCK_LIMITED = "penstock_limited"


class WakeReason(str, Enum):
    """Why a scheduler pass started.

    ``str``-subclass so comparisons against plain string literals keep
    working and Pydantic v2 serialises the value automatically.

    See :meth:`Tideweaver._wait_for_next_event`.
    """

    STARTUP = "startup"
    TIMER = "timer"
    WAKE_EVENT = "wake_event"
    PASS_INTERVAL = "pass_interval"  # noqa: S105
    SHUTDOWN = "shutdown"
