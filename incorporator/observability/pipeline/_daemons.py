"""Per-source refresh and export daemons used by stateful and fjord engines."""

import asyncio
import time
from typing import Any, Dict, List, Optional

from ..logger import Wave
from ._shared import _interruptible_sleep, _row_count


async def _refresh_daemon(
    cls: Any,
    dataset_ref: List[Any],
    refresh_params: Dict[str, Any],
    lock: asyncio.Lock,
    wave_queue: "asyncio.Queue[Optional[Wave]]",
    shutdown_event: asyncio.Event,
    r_interval: Optional[float],
    operation_label: str = "refresh",
) -> None:
    """Runs the independent refresh loop on its own schedule.

    Acquires ``lock`` before mutating ``dataset_ref[0]`` so the export daemon
    always snapshots a consistent state.  Enqueues one :class:`Wave` per
    iteration — success or failure — for the drain loop to yield downstream.

    ``operation_label`` overrides the :attr:`Wave.operation` field so the
    fjord engine can tag refreshes per source class
    (e.g. ``"fjord_refresh:Coin"``). Defaults to ``"refresh"`` for the
    single-source stateful engine.
    """
    loop_idx = 0
    while not shutdown_event.is_set():
        loop_idx += 1
        start_time = time.perf_counter()
        try:
            async with lock:  # ENSURE ATOMIC MUTATION
                refreshed = await cls.refresh(instance=dataset_ref[0], **refresh_params)
                if refreshed is not None:
                    dataset_ref[0] = refreshed
            await wave_queue.put(
                Wave(
                    chunk_index=loop_idx,
                    operation=operation_label,
                    rows_processed=_row_count(dataset_ref[0]),
                    processing_time_sec=time.perf_counter() - start_time,
                )
            )
        except Exception as e:
            await wave_queue.put(
                Wave(
                    chunk_index=loop_idx,
                    operation=operation_label,
                    rows_processed=0,
                    failed_sources=[f"Refresh Error: {e}"],
                    processing_time_sec=0.0,
                )
            )

        if r_interval is None:
            break

        if await _interruptible_sleep(shutdown_event, r_interval):
            break


async def _export_daemon(
    cls: Any,
    dataset_ref: List[Any],
    export_params: Dict[str, Any],
    lock: asyncio.Lock,
    wave_queue: "asyncio.Queue[Optional[Wave]]",
    shutdown_event: asyncio.Event,
    e_interval: Optional[float],
    operation_label: str = "export",
) -> None:
    """Runs the independent export loop on its own schedule.

    Snapshots ``dataset_ref[0]`` under ``lock`` (O(1) pointer copy), then
    releases the lock before the actual export so ``_refresh_daemon`` can
    proceed concurrently during long I/O writes (e.g. 10 M-row exports).

    ``operation_label`` overrides the :attr:`Wave.operation` field so the
    fjord engine can tag per-source exports (e.g. ``"export:BinanceFutures"``).
    Defaults to ``"export"`` for the single-source stateful engine.
    """
    loop_idx = 0
    while not shutdown_event.is_set():
        loop_idx += 1
        start_time = time.perf_counter()
        try:
            async with lock:
                snapshot = dataset_ref[0]
            await cls.export(instance=snapshot, **export_params)
            await wave_queue.put(
                Wave(
                    chunk_index=loop_idx,
                    operation=operation_label,
                    rows_processed=_row_count(snapshot),
                    processing_time_sec=time.perf_counter() - start_time,
                )
            )
        except Exception as e:
            await wave_queue.put(
                Wave(
                    chunk_index=loop_idx,
                    operation=operation_label,
                    rows_processed=0,
                    failed_sources=[f"Export Error: {e}"],
                    processing_time_sec=0.0,
                )
            )

        if e_interval is None:
            break

        if await _interruptible_sleep(shutdown_event, e_interval):
            break
