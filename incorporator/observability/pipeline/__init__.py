"""Autonomous orchestration pipeline for the Incorporator framework.

Split into three engines (see sibling modules) plus the shared daemons /
helpers they use:

- :mod:`.chunked`  — Engine 1: O(1)-memory paginator-driven streaming.
- :mod:`.stateful` — Engine 2: decoupled refresh/export daemon schedules.
- :mod:`.fjord`    — Engine 3: multi-source stateful streaming with combined
  ``outflow(state)`` output.

The public entry point is :func:`run_pipeline`, which dispatches between the
chunked and stateful engines. The fjord engine has its own classmethod on
:class:`~incorporator.base.Incorporator` (``fjord``) that imports
``_run_fjord_engine`` directly.
"""

from typing import Any, AsyncGenerator, Dict, Optional

# Re-export every previously top-level symbol so existing imports
# (`from incorporator.observability.pipeline import _refresh_daemon`,
#  `from .observability.pipeline import run_pipeline`, etc.) keep working.
from ..logger import Wave
from ._daemons import _export_daemon, _refresh_daemon
from ._outflow import _outflow_daemon
from ._shared import _enrich_and_load, _interruptible_sleep, _row_count
from .chunked import _run_chunking_engine
from .fjord import _run_fjord_engine
from .stateful import _run_stateful_engine

__all__ = [
    "run_pipeline",
    "DEFAULT_REFRESH_INTERVAL_SEC",
    "DEFAULT_EXPORT_INTERVAL_SEC",
    "_enrich_and_load",
    "_export_daemon",
    "_interruptible_sleep",
    "_outflow_daemon",
    "_refresh_daemon",
    "_row_count",
    "_run_chunking_engine",
    "_run_fjord_engine",
    "_run_stateful_engine",
]


# Module-level cadence defaults — applied at the bottom of the cascade
# (per-entry override > top-level kwarg > poll_interval > these).  These
# prevent the silent "daemon ticks once and exits" failure mode when a
# stateful pipeline is started with no interval kwargs at all.
DEFAULT_REFRESH_INTERVAL_SEC: float = 60.0
DEFAULT_EXPORT_INTERVAL_SEC: float = 300.0


async def run_pipeline(
    cls: Any,
    incorp_params: Dict[str, Any],
    refresh_params: Optional[Dict[str, Any]],
    export_params: Optional[Dict[str, Any]],
    poll_interval: Optional[float],
    stateful_polling: bool,
    refresh_interval: Optional[float] = None,
    export_interval: Optional[float] = None,
) -> AsyncGenerator[Wave, None]:
    """Run a single-source streaming pipeline, picking the engine to match the workload.

    Two engines back ``stream()``:

    * **Stateful** (``stateful_polling=True``) — seed the registry once with
      ``incorp()``, then run independent refresh and export daemons on decoupled
      schedules. Right for live mark-to-market dashboards, slow indicators, and any
      workload that reads ``cls.inc_dict`` between cycles.
    * **Chunking** (the default) — paginator-driven sequential O(1) ingestion; each
      wave loads the next chunk and releases it from memory. Right for bulk drains
      of paginated sources and historical backfills.

    The default interval cascade applies bottom-up: explicit ``refresh_interval`` /
    ``export_interval`` override ``poll_interval``, which overrides
    ``DEFAULT_REFRESH_INTERVAL_SEC`` / ``DEFAULT_EXPORT_INTERVAL_SEC``. This prevents
    a daemon spawned without any interval kwargs from ticking once and exiting silently.

    Yields:
        Wave: one per daemon iteration (stateful) or one per chunk (chunking),
        success or failure.
    """
    paginator = incorp_params.get("inc_page")

    if stateful_polling:
        # Cascade: explicit kwarg > poll_interval > module default.  The
        # final fallback to DEFAULT_*_INTERVAL_SEC prevents a daemon
        # spawned with no interval kwargs from ticking once and exiting
        # silently (a real failure mode users hit when refresh_interval
        # was left at None).
        async for wave in _run_stateful_engine(
            cls=cls,
            incorp_params=incorp_params,
            refresh_params=refresh_params,
            export_params=export_params,
            r_interval=refresh_interval or poll_interval or DEFAULT_REFRESH_INTERVAL_SEC,
            e_interval=export_interval or poll_interval or DEFAULT_EXPORT_INTERVAL_SEC,
        ):
            yield wave
    else:
        async for wave in _run_chunking_engine(
            cls=cls,
            incorp_params=incorp_params,
            refresh_params=refresh_params,
            export_params=export_params,
            poll_interval=poll_interval,
            paginator=paginator,
        ):
            yield wave
