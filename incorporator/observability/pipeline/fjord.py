"""Fjord engine (Engine 3): multi-source stateful streaming with combined outflow."""

import asyncio
import logging
import time
from typing import Any, AsyncGenerator, Dict, List, Optional

from ..logger import AuditResult
from ._daemons import _export_daemon, _refresh_daemon
from ._outflow import _outflow_daemon
from ._shared import _row_count

logger = logging.getLogger(__name__)


async def _run_fjord_engine(
    output_class_name: str,
    base_class: Any,
    stream_params: List[Dict[str, Any]],
    outflow_fn: Any,
    export_params: Dict[str, Any],
    r_interval: Optional[float],
    e_interval: Optional[float],
) -> AsyncGenerator[AuditResult, None]:
    """Multi-source stateful streaming engine for ``Incorporator.fjord()``.

    Generalisation of ``_run_stateful_engine`` to N sources with one
    outflow-and-export daemon. The output class is built dynamically by
    ``_outflow_daemon`` on the first non-empty tick — this engine just
    plumbs the name + base class through.

    Lifecycle:
      1. Seed phase: concurrent ``entry["cls"].incorp(**entry["incorp_params"])``
         across all entries via ``asyncio.gather``. One ``incorp`` audit yielded
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
            yield AuditResult(
                chunk_index=1,
                operation=f"fjord_incorp:{cls_name}",
                rows_processed=0,
                failed_sources=[f"Seed Error: {result}"],
                processing_time_sec=seed_elapsed,
            )
            return
        if not result:
            yield AuditResult(
                chunk_index=1,
                operation=f"fjord_incorp:{cls_name}",
                rows_processed=0,
                failed_sources=[f"Initial incorp() for {cls_name} yielded no data"],
                processing_time_sec=seed_elapsed,
            )
            return
        source_refs.append([result])
        yield AuditResult(
            chunk_index=1,
            operation=f"fjord_incorp:{cls_name}",
            rows_processed=_row_count(result),
            processing_time_sec=seed_elapsed,
        )

    # ------------------------------------------------------------------
    # 2. Daemon phase — spawn refresh + per-stream export + outflow tasks.
    # ------------------------------------------------------------------
    lock = asyncio.Lock()
    audit_queue: asyncio.Queue[Optional[AuditResult]] = asyncio.Queue()
    shutdown_event = asyncio.Event()
    tasks: List[asyncio.Task[Any]] = []

    for idx, entry in enumerate(stream_params):
        entry_cls = entry["cls"]
        refresh_params = entry.get("refresh_params")
        stream_export_params = entry.get("export_params")
        # Per-entry interval overrides fall back to the top-level interval.
        entry_r_interval = entry.get("refresh_interval", r_interval)
        entry_e_interval = entry.get("export_interval", e_interval)

        if refresh_params is not None:
            tasks.append(
                asyncio.create_task(
                    _refresh_daemon(
                        cls=entry_cls,
                        dataset_ref=source_refs[idx],
                        refresh_params=refresh_params,
                        lock=lock,
                        audit_queue=audit_queue,
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
                        audit_queue=audit_queue,
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
                audit_queue=audit_queue,
                shutdown_event=shutdown_event,
                e_interval=e_interval,
            )
        )
    )

    async def _waiter() -> None:
        await asyncio.gather(*tasks, return_exceptions=True)
        await audit_queue.put(None)

    waiter_task = asyncio.create_task(_waiter())

    try:
        while True:
            audit = await audit_queue.get()
            if audit is None:
                break
            yield audit
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
