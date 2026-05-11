"""
Autonomous Orchestration Pipeline for the Incorporator Framework.
Handles Dual-Engine execution (O(1) Chunking and Stateful Polling).
"""

import asyncio
import logging
import time
from typing import Any, AsyncGenerator, Dict, List, Optional

from .logger import AuditResult

logger = logging.getLogger(__name__)


async def _enrich_and_load(
    cls: Any,
    dataset: Any,
    refresh_params: Optional[Dict[str, Any]],
    export_params: Optional[Dict[str, Any]],
    force_append: bool,
) -> None:
    """Atomic helper for the Enrich (Refresh) and Load (Export) phases."""
    if refresh_params:
        await cls.refresh(instance=dataset, **refresh_params)

    if export_params:
        params = export_params.copy() if force_append else export_params
        if force_append:
            params["if_exists"] = "append"
        await cls.export(instance=dataset, **params)


async def run_pipeline(
    cls: Any,
    incorp_params: Dict[str, Any],
    refresh_params: Optional[Dict[str, Any]],
    export_params: Optional[Dict[str, Any]],
    poll_interval: Optional[float],
    stateful_polling: bool,
    refresh_interval: Optional[float] = None,
    export_interval: Optional[float] = None,
) -> AsyncGenerator[AuditResult, None]:
    paginator = incorp_params.get("inc_page")

    # ==========================================
    # ENGINE 2: STATEFUL POLLING (Decoupled Schedules)
    # ==========================================
    if stateful_polling:
        init_start_time = time.perf_counter()
        initial_dataset = await cls.incorp(**incorp_params)
        init_elapsed = time.perf_counter() - init_start_time

        if not initial_dataset:
            yield AuditResult(
                chunk_index=1,
                operation="incorp",
                rows_processed=0,
                failed_sources=["Initial incorp() yielded no data"],
                processing_time_sec=init_elapsed,
            )
            return

        # Mutable container so refresh() results propagate to export()
        dataset_ref: List[Any] = [initial_dataset]

        r_interval = refresh_interval or poll_interval
        e_interval = export_interval or poll_interval

        # Safe async coordination primitives
        lock = asyncio.Lock()
        audit_queue: asyncio.Queue[Optional[AuditResult]] = asyncio.Queue()
        shutdown_event = asyncio.Event()

        async def _refresh_daemon() -> None:
            loop_idx = 0
            r_params = refresh_params or {}
            while not shutdown_event.is_set():
                loop_idx += 1
                start_time = time.perf_counter()
                try:
                    async with lock:  # 🛡️ ENSURE ATOMIC MUTATION
                        refreshed = await cls.refresh(instance=dataset_ref[0], **r_params)
                        if refreshed is not None:
                            dataset_ref[0] = refreshed
                    rows = len(dataset_ref[0]) if isinstance(dataset_ref[0], list) else 1
                    await audit_queue.put(
                        AuditResult(
                            chunk_index=loop_idx,
                            operation="refresh",
                            rows_processed=rows,
                            processing_time_sec=time.perf_counter() - start_time,
                        )
                    )
                except Exception as e:
                    await audit_queue.put(
                        AuditResult(
                            chunk_index=loop_idx,
                            operation="refresh",
                            rows_processed=0,
                            failed_sources=[f"Refresh Error: {e}"],
                            processing_time_sec=0.0,
                        )
                    )

                if r_interval is None:
                    break

                # Sleep but wake immediately on shutdown
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=r_interval)
                    break  # shutdown_event fired during sleep
                except asyncio.TimeoutError:
                    pass  # normal interval elapsed, loop again

        async def _export_daemon() -> None:
            loop_idx = 0
            e_params = export_params or {}
            while not shutdown_event.is_set():
                loop_idx += 1
                start_time = time.perf_counter()
                try:
                    async with lock:  # ENSURE ATOMIC READ
                        await cls.export(instance=dataset_ref[0], **e_params)
                    rows = len(dataset_ref[0]) if isinstance(dataset_ref[0], list) else 1
                    await audit_queue.put(
                        AuditResult(
                            chunk_index=loop_idx,
                            operation="export",
                            rows_processed=rows,
                            processing_time_sec=time.perf_counter() - start_time,
                        )
                    )
                except Exception as e:
                    await audit_queue.put(
                        AuditResult(
                            chunk_index=loop_idx,
                            operation="export",
                            rows_processed=0,
                            failed_sources=[f"Export Error: {e}"],
                            processing_time_sec=0.0,
                        )
                    )

                if e_interval is None:
                    break

                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=e_interval)
                    break
                except asyncio.TimeoutError:
                    pass

        tasks = []
        if refresh_params:
            tasks.append(asyncio.create_task(_refresh_daemon()))
        if export_params:
            tasks.append(asyncio.create_task(_export_daemon()))

        if not tasks:
            rows = len(dataset_ref[0]) if isinstance(dataset_ref[0], list) else 1
            yield AuditResult(
                chunk_index=1,
                operation="incorp",
                rows_processed=rows,
                processing_time_sec=init_elapsed,
            )
            return

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
                pass  # Expected during shutdown — daemons were cancelled
            except Exception as exc:
                logger.warning("Stateful polling drain raised during shutdown: %s", exc)
        return

    # ==========================================
    # ENGINE 1: O(1) CHUNKING (Sequential Sync)
    # ==========================================
    while True:
        chunk_idx = 0
        while True:
            chunk_idx += 1
            start_time = time.perf_counter()

            params = incorp_params.copy()
            if paginator:
                if getattr(paginator, "is_exhausted", False):
                    break
                params["call_lim"] = 1

            try:
                dataset = await cls.incorp(**params)

                if not dataset and not paginator:
                    break
                if getattr(paginator, "is_exhausted", False) and not dataset:
                    break

                rows = len(dataset) if isinstance(dataset, list) else (1 if dataset else 0)

                if rows > 0:
                    await _enrich_and_load(cls, dataset, refresh_params, export_params, force_append=True)

                yield AuditResult(
                    chunk_index=chunk_idx,
                    operation="chunk",
                    rows_processed=rows,
                    processing_time_sec=time.perf_counter() - start_time,
                )

                del dataset
                await asyncio.sleep(0)

                if not paginator:
                    break
            except Exception as e:
                yield AuditResult(
                    chunk_index=chunk_idx,
                    operation="chunk",
                    rows_processed=0,
                    failed_sources=[str(e)],
                    processing_time_sec=time.perf_counter() - start_time,
                )
                break

        if poll_interval is None:
            break

        if paginator and hasattr(paginator, "reset"):
            paginator.reset()

        await asyncio.sleep(poll_interval)
