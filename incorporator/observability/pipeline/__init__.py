"""Autonomous orchestration pipeline for the Incorporator framework.

Split into two engines (see sibling modules) plus the shared daemons /
helpers they use:

- :mod:`.chunked`  — Engine 1: O(1)-memory paginator-driven streaming
  (the only engine reached via :func:`run_pipeline`).
- :mod:`.fjord`    — Engine 2: multi-source stateful streaming with combined
  ``outflow(state)`` output.  Reached via :meth:`Incorporator.fjord`, and
  via :meth:`Incorporator.stream(stateful_polling=True)` through the shim
  in :func:`incorporator.base._stream_stateful_via_fjord` (single-source
  identity outflow).

Per-source daemons (``_refresh_daemon`` / ``_export_daemon``) live in
:mod:`._daemons` and are reused by the fjord engine.
"""

from typing import Any, AsyncGenerator, Dict, Optional

# Re-export every previously top-level symbol so existing imports
# (`from incorporator.observability.pipeline import _refresh_daemon`,
#  `from .observability.pipeline import run_pipeline`, etc.) keep working.
from ..logger import Wave
from ._daemons import _export_daemon, _refresh_daemon
from ._dispatch import assert_engine_supported
from ._outflow import _outflow_daemon
from ._shared import _enrich_and_load, _interruptible_sleep, _row_count
from .chunked import _run_chunking_engine
from .fjord import _run_fjord_engine

__all__ = [
    "run_pipeline",
    "assert_engine_supported",
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
]


# Module-level cadence defaults — applied at the bottom of the cascade
# (per-entry override > top-level kwarg > poll_interval > these).  Used by
# the fjord engine (per-source) and by the stateful-stream shim
# (single-source) to prevent the silent "daemon ticks once and exits"
# failure mode when no interval kwargs are passed at all.
DEFAULT_REFRESH_INTERVAL_SEC: float = 60.0
DEFAULT_EXPORT_INTERVAL_SEC: float = 300.0


async def run_pipeline(
    cls: Any,
    incorp_params: Dict[str, Any],
    refresh_params: Optional[Dict[str, Any]],
    export_params: Optional[Dict[str, Any]],
    poll_interval: Optional[float],
) -> AsyncGenerator[Wave, None]:
    """Run a single-source chunking pipeline — the engine behind ``stream()``.

    Stateful streaming (``stream(stateful_polling=True)``) no longer comes
    through here.  It's a thin shim in :mod:`incorporator.base` that
    synthesises a one-source ``stream_params`` list and an identity
    ``outflow(state)`` so the fjord engine can run a single-source
    stateful pipeline transparently — Python-object identity in
    ``cls.inc_dict`` survives across waves thanks to the IncorporatorList
    pass-through fast path in :func:`._outflow.flush`.

    Yields:
        Wave: one per chunk, success or failure.  The chunking engine
        releases each chunk before fetching the next so RSS stays flat
        regardless of total data volume.
    """
    paginator = incorp_params.get("inc_page")
    async for wave in _run_chunking_engine(
        cls=cls,
        incorp_params=incorp_params,
        refresh_params=refresh_params,
        export_params=export_params,
        poll_interval=poll_interval,
        paginator=paginator,
    ):
        yield wave
