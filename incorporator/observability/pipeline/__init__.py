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
    """Dual-engine pipeline dispatcher.

    Routes to :func:`_run_stateful_engine` when ``stateful_polling=True``
    (independent refresh/export daemon tasks on decoupled schedules), or
    :func:`_run_chunking_engine` for sequential O(1) chunked ingestion with
    optional continuous polling.
    """
    paginator = incorp_params.get("inc_page")

    if stateful_polling:
        async for wave in _run_stateful_engine(
            cls=cls,
            incorp_params=incorp_params,
            refresh_params=refresh_params,
            export_params=export_params,
            r_interval=refresh_interval or poll_interval,
            e_interval=export_interval or poll_interval,
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
