"""Stateful polling engine (Engine 2): decoupled refresh/export daemon schedules."""

import asyncio
import logging
import time
from typing import Any, AsyncGenerator, Dict, List, Optional

from ..logger import Wave
from ._daemons import _export_daemon, _refresh_daemon
from ._shared import _row_count

logger = logging.getLogger(__name__)


async def _run_stateful_engine(
    cls: Any,
    incorp_params: Dict[str, Any],
    refresh_params: Optional[Dict[str, Any]],
    export_params: Optional[Dict[str, Any]],
    r_interval: Optional[float],
    e_interval: Optional[float],
) -> AsyncGenerator[Wave, None]:
    """ENGINE 2 — Stateful Polling (Decoupled Schedules).

    Runs ``incorp()`` once to seed the dataset, then spawns ``_refresh_daemon``
    and ``_export_daemon`` as independent asyncio tasks on their own intervals.
    Yields one :class:`Wave` per daemon iteration until both tasks complete
    or the generator is cancelled.
    """
    init_start_time = time.perf_counter()
    initial_dataset = await cls.incorp(**incorp_params)
    init_elapsed = time.perf_counter() - init_start_time

    if not initial_dataset:
        yield Wave(
            chunk_index=1,
            operation="incorp",
            rows_processed=0,
            failed_sources=["Initial incorp() yielded no data"],
            processing_time_sec=init_elapsed,
        )
        return

    # Mutable single-element list so daemons can swap the reference atomically.
    dataset_ref: List[Any] = [initial_dataset]

    lock = asyncio.Lock()
    wave_queue: asyncio.Queue[Optional[Wave]] = asyncio.Queue()
    shutdown_event = asyncio.Event()

    tasks = []
    # ``is not None`` rather than truthy: empty dict ``{}`` MUST opt into the
    # daemon (= "run with default kwargs").  Pre-fix, ``if refresh_params:``
    # silently skipped because Python treats ``{}`` as falsy — contradicting
    # the documented contract and matching the bug the user hit when refresh
    # didn't fire on tutorial 7.
    if refresh_params is not None:
        tasks.append(
            asyncio.create_task(
                _refresh_daemon(
                    cls=cls,
                    dataset_ref=dataset_ref,
                    refresh_params=refresh_params,
                    lock=lock,
                    wave_queue=wave_queue,
                    shutdown_event=shutdown_event,
                    r_interval=r_interval,
                )
            )
        )
    if export_params is not None:
        tasks.append(
            asyncio.create_task(
                _export_daemon(
                    cls=cls,
                    dataset_ref=dataset_ref,
                    export_params=export_params,
                    lock=lock,
                    wave_queue=wave_queue,
                    shutdown_event=shutdown_event,
                    e_interval=e_interval,
                )
            )
        )

    if not tasks:
        # No daemons requested — emit the initial incorp result and exit.
        yield Wave(
            chunk_index=1,
            operation="incorp",
            rows_processed=_row_count(dataset_ref[0]),
            processing_time_sec=init_elapsed,
        )
        return

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
            pass  # Expected during shutdown — daemons were cancelled
        except Exception as exc:
            logger.warning("Stateful polling drain raised during shutdown: %s", exc)
