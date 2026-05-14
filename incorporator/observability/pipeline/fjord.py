"""Fjord engine (Engine 3): multi-source stateful streaming with combined outflow."""

import asyncio
import logging
import time
from typing import Any, AsyncGenerator, Dict, List, Optional, Union

from ..logger import Wave
from ._daemons import _export_daemon, _refresh_daemon
from ._outflow import _outflow_daemon
from ._shared import _row_count

logger = logging.getLogger(__name__)


def _resolve_per_source_interval(
    top_level: Union[float, Dict[Any, float], None],
    entry: Dict[str, Any],
    key: str,
) -> Optional[float]:
    """Pick the interval value for one fjord stream entry.

    Priority chain:
      1. Per-entry override — ``entry[key]`` if explicitly set.
      2. Top-level dict — ``top_level[class_name]`` or ``top_level[cls]``
         when the top-level kwarg is a dict keyed by class name (string,
         JSON-compatible) or class object (Python-ergonomic).
      3. Top-level scalar — used as the default for every source.
      4. ``None`` — when nothing matches.  The pipeline-level cascade in
         ``observability/pipeline/__init__.py`` then applies the
         framework default (60 s for refresh, 300 s for export).
    """
    if key in entry:
        return entry[key]
    if isinstance(top_level, dict):
        cls = entry.get("cls")
        if cls is not None and cls in top_level:
            return top_level[cls]
        cls_name = getattr(cls, "__name__", None)
        if cls_name is not None and cls_name in top_level:
            return top_level[cls_name]
        return None
    return top_level


async def _run_fjord_engine(
    output_class_name: str,
    base_class: Any,
    stream_params: List[Dict[str, Any]],
    outflow_fn: Any,
    export_params: Dict[str, Any],
    r_interval: Optional[float],
    e_interval: Optional[float],
) -> AsyncGenerator[Wave, None]:
    """Multi-source stateful streaming engine for ``Incorporator.fjord()``.

    Generalisation of ``_run_stateful_engine`` to N sources with one
    outflow-and-export daemon. The output class is built dynamically by
    ``_outflow_daemon`` on the first non-empty tick — this engine just
    plumbs the name + base class through.

    Lifecycle:
      1. Seed phase: concurrent ``entry["cls"].incorp(**entry["incorp_params"])``
         across all entries via ``asyncio.gather``. One ``incorp`` wave yielded
         per source.
      2. Daemon phase: per-source refresh daemons (always), per-source export
         daemons (when ``export_params`` is set on the entry), and one outflow
         daemon. All coordinate via a single shared ``asyncio.Lock``.
      3. Shutdown: ``shutdown_event.set()`` → cancel tasks → drain queue → exit.
    """
    # ------------------------------------------------------------------
    # 1. Seed phase — concurrent incorp() across all sources.
    # ------------------------------------------------------------------
    source_classes: List[Any] = [entry["cls"] for entry in stream_params]
    seed_tasks = [asyncio.create_task(entry["cls"].incorp(**entry["incorp_params"])) for entry in stream_params]

    seed_start = time.perf_counter()
    seed_results = await asyncio.gather(*seed_tasks, return_exceptions=True)
    seed_elapsed = time.perf_counter() - seed_start

    # Validate seed phase — every source must have produced data.
    source_refs: List[List[Any]] = []
    for entry, result in zip(stream_params, seed_results):
        cls_name = entry["cls"].__name__
        if isinstance(result, Exception):
            yield Wave(
                chunk_index=1,
                operation=f"fjord_incorp:{cls_name}",
                rows_processed=0,
                failed_sources=[f"Seed Error: {result}"],
                processing_time_sec=seed_elapsed,
            )
            return
        if not result:
            yield Wave(
                chunk_index=1,
                operation=f"fjord_incorp:{cls_name}",
                rows_processed=0,
                failed_sources=[f"Initial incorp() for {cls_name} yielded no data"],
                processing_time_sec=seed_elapsed,
            )
            return
        source_refs.append([result])
        yield Wave(
            chunk_index=1,
            operation=f"fjord_incorp:{cls_name}",
            rows_processed=_row_count(result),
            processing_time_sec=seed_elapsed,
        )

    # ------------------------------------------------------------------
    # 2. Daemon phase — spawn refresh + per-stream export + outflow tasks.
    # ------------------------------------------------------------------
    lock = asyncio.Lock()
    wave_queue: asyncio.Queue[Optional[Wave]] = asyncio.Queue()
    shutdown_event = asyncio.Event()
    tasks: List[asyncio.Task[Any]] = []

    for idx, entry in enumerate(stream_params):
        entry_cls = entry["cls"]
        # Refresh defaults to ON.  Pass "refresh_params": {} for explicit
        # default kwargs, or "refresh_params": None to opt OUT of refresh
        # for this specific source.  Missing key = default-on with {}.
        refresh_params = entry.get("refresh_params", {})
        stream_export_params = entry.get("export_params")
        # Per-entry interval overrides fall back to the top-level interval.
        # Top-level can be a scalar (applies to all sources) or a dict
        # keyed by class name / class object (per-source override).  When
        # the entire cascade returns None, fall back to the module-level
        # default so a daemon spawned with no intervals still ticks.
        from . import DEFAULT_EXPORT_INTERVAL_SEC, DEFAULT_REFRESH_INTERVAL_SEC

        entry_r_interval = (
            _resolve_per_source_interval(r_interval, entry, "refresh_interval")
            or DEFAULT_REFRESH_INTERVAL_SEC
        )
        entry_e_interval = (
            _resolve_per_source_interval(e_interval, entry, "export_interval")
            or DEFAULT_EXPORT_INTERVAL_SEC
        )

        if refresh_params is not None:
            tasks.append(
                asyncio.create_task(
                    _refresh_daemon(
                        cls=entry_cls,
                        dataset_ref=source_refs[idx],
                        refresh_params=refresh_params,
                        lock=lock,
                        wave_queue=wave_queue,
                        shutdown_event=shutdown_event,
                        r_interval=entry_r_interval,
                        operation_label=f"fjord_refresh:{entry_cls.__name__}",
                    )
                )
            )

        if stream_export_params is not None:
            tasks.append(
                asyncio.create_task(
                    _export_daemon(
                        cls=entry_cls,
                        dataset_ref=source_refs[idx],
                        export_params=stream_export_params,
                        lock=lock,
                        wave_queue=wave_queue,
                        shutdown_event=shutdown_event,
                        e_interval=entry_e_interval,
                        operation_label=f"export:{entry_cls.__name__}",
                    )
                )
            )

    # Always spawn the outflow daemon — it's the whole point of fjord.
    tasks.append(
        asyncio.create_task(
            _outflow_daemon(
                output_class_name=output_class_name,
                base_class=base_class,
                source_refs=source_refs,
                source_classes=source_classes,
                outflow_fn=outflow_fn,
                export_params=export_params,
                lock=lock,
                wave_queue=wave_queue,
                shutdown_event=shutdown_event,
                e_interval=e_interval,
            )
        )
    )

    async def _waiter() -> None:
        await asyncio.gather(*tasks, return_exceptions=True)
        await wave_queue.put(None)

    waiter_task = asyncio.create_task(_waiter())

    try:
        while True:
            wave = await wave_queue.get()
            if wave is None:
                break
            yield wave
    finally:
        shutdown_event.set()
        for t in tasks:
            if not t.done():
                t.cancel()
        try:
            await waiter_task
        except asyncio.CancelledError:
            pass  # Expected during shutdown
        except Exception as exc:
            logger.warning("Fjord drain raised during shutdown: %s", exc)
