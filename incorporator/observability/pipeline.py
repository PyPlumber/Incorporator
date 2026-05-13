"""
Autonomous Orchestration Pipeline for the Incorporator Framework.
Handles Dual-Engine execution (O(1) Chunking and Stateful Polling).
"""

import asyncio
import gc
import logging
import time
from typing import Any, AsyncGenerator, Dict, List, Optional

from .logger import AuditResult

logger = logging.getLogger(__name__)


async def _interruptible_sleep(event: asyncio.Event, timeout: Optional[float]) -> bool:
    """Sleeps for `timeout` seconds, returning True immediately if `event` fires first."""
    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        return False


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


def _row_count(dataset: Any) -> int:
    """Returns the number of rows in a dataset (list length, 1 for a single object, 0 for falsy)."""
    return len(dataset) if isinstance(dataset, list) else (1 if dataset else 0)


async def _refresh_daemon(
    cls: Any,
    dataset_ref: List[Any],
    refresh_params: Dict[str, Any],
    lock: asyncio.Lock,
    audit_queue: "asyncio.Queue[Optional[AuditResult]]",
    shutdown_event: asyncio.Event,
    r_interval: Optional[float],
    operation_label: str = "refresh",
) -> None:
    """Runs the independent refresh loop on its own schedule.

    Acquires ``lock`` before mutating ``dataset_ref[0]`` so the export daemon
    always snapshots a consistent state.  Enqueues one ``AuditResult`` per
    iteration — success or failure — for the drain loop to yield downstream.

    ``operation_label`` overrides the ``AuditResult.operation`` field so the
    fjord engine can tag refreshes per source class
    (e.g. ``"fjord_refresh:Coin"``). Defaults to ``"refresh"`` so existing
    callers (``_run_stateful_engine``) are unchanged.
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
            await audit_queue.put(
                AuditResult(
                    chunk_index=loop_idx,
                    operation=operation_label,
                    rows_processed=_row_count(dataset_ref[0]),
                    processing_time_sec=time.perf_counter() - start_time,
                )
            )
        except Exception as e:
            await audit_queue.put(
                AuditResult(
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
    audit_queue: "asyncio.Queue[Optional[AuditResult]]",
    shutdown_event: asyncio.Event,
    e_interval: Optional[float],
    operation_label: str = "export",
) -> None:
    """Runs the independent export loop on its own schedule.

    Snapshots ``dataset_ref[0]`` under ``lock`` (O(1) pointer copy), then
    releases the lock before the actual export so ``_refresh_daemon`` can
    proceed concurrently during long I/O writes (e.g. 10 M-row exports).

    ``operation_label`` overrides the ``AuditResult.operation`` field so the
    fjord engine can tag per-source exports (e.g. ``"export:BinanceFutures"``).
    Defaults to ``"export"`` so existing callers stay unchanged.
    """
    loop_idx = 0
    while not shutdown_event.is_set():
        loop_idx += 1
        start_time = time.perf_counter()
        try:
            async with lock:
                snapshot = dataset_ref[0]
            await cls.export(instance=snapshot, **export_params)
            await audit_queue.put(
                AuditResult(
                    chunk_index=loop_idx,
                    operation=operation_label,
                    rows_processed=_row_count(snapshot),
                    processing_time_sec=time.perf_counter() - start_time,
                )
            )
        except Exception as e:
            await audit_queue.put(
                AuditResult(
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


async def _run_stateful_engine(
    cls: Any,
    incorp_params: Dict[str, Any],
    refresh_params: Optional[Dict[str, Any]],
    export_params: Optional[Dict[str, Any]],
    r_interval: Optional[float],
    e_interval: Optional[float],
) -> AsyncGenerator[AuditResult, None]:
    """ENGINE 2 — Stateful Polling (Decoupled Schedules).

    Runs ``incorp()`` once to seed the dataset, then spawns ``_refresh_daemon``
    and ``_export_daemon`` as independent asyncio tasks on their own intervals.
    Yields one ``AuditResult`` per daemon iteration until both tasks complete or
    the generator is cancelled.
    """
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

    # Mutable single-element list so daemons can swap the reference atomically.
    dataset_ref: List[Any] = [initial_dataset]

    lock = asyncio.Lock()
    audit_queue: asyncio.Queue[Optional[AuditResult]] = asyncio.Queue()
    shutdown_event = asyncio.Event()

    tasks = []
    if refresh_params:
        tasks.append(
            asyncio.create_task(
                _refresh_daemon(
                    cls=cls,
                    dataset_ref=dataset_ref,
                    refresh_params=refresh_params,
                    lock=lock,
                    audit_queue=audit_queue,
                    shutdown_event=shutdown_event,
                    r_interval=r_interval,
                )
            )
        )
    if export_params:
        tasks.append(
            asyncio.create_task(
                _export_daemon(
                    cls=cls,
                    dataset_ref=dataset_ref,
                    export_params=export_params,
                    lock=lock,
                    audit_queue=audit_queue,
                    shutdown_event=shutdown_event,
                    e_interval=e_interval,
                )
            )
        )

    if not tasks:
        # No daemons requested — emit the initial incorp result and exit.
        yield AuditResult(
            chunk_index=1,
            operation="incorp",
            rows_processed=_row_count(dataset_ref[0]),
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


async def _run_chunking_engine(
    cls: Any,
    incorp_params: Dict[str, Any],
    refresh_params: Optional[Dict[str, Any]],
    export_params: Optional[Dict[str, Any]],
    poll_interval: Optional[float],
    paginator: Any,
) -> AsyncGenerator[AuditResult, None]:
    """ENGINE 1 — O(1) Chunking (Sequential).

    Loops over paginator-driven or single-shot ``incorp()`` calls, calling
    ``_enrich_and_load`` per chunk and yielding one ``AuditResult`` per chunk.
    Releases each dataset from memory immediately after yielding so RSS stays
    flat regardless of total data volume.  Sleeps ``poll_interval`` between
    full passes when continuous polling is requested.
    """
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

                rows = _row_count(dataset)

                if rows > 0:
                    await _enrich_and_load(cls, dataset, refresh_params, export_params, force_append=True)

                yield AuditResult(
                    chunk_index=chunk_idx,
                    operation="chunk",
                    rows_processed=rows,
                    processing_time_sec=time.perf_counter() - start_time,
                )

                del dataset
                gc.collect()
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


async def _combine_daemon(
    cls: Any,
    source_refs: List[List[Any]],
    source_classes: List[Any],
    combine_fn: Any,
    export_params: Dict[str, Any],
    lock: asyncio.Lock,
    audit_queue: "asyncio.Queue[Optional[AuditResult]]",
    shutdown_event: asyncio.Event,
    e_interval: Optional[float],
) -> None:
    """Periodic combine-and-export daemon for the fjord engine.

    On every tick:
      1. Snapshot each ``source_refs[i][0]`` under ``lock`` into a state dict
         keyed by ``source_classes[i].__name__`` (O(N) pointer copies, not deep
         copies — release the lock fast).
      2. Outside the lock, invoke the user's ``combine_fn(state)``.  The return
         value must be an iterable of dicts (or anything ``cls(**row)`` accepts).
      3. Clear ``cls.inc_dict`` (so successive ticks reflect the latest joined
         view, not an ever-growing accumulator), then build ``cls(**row)``
         instances for each returned row — they auto-register via
         ``model_post_init``.
      4. Export the populated list via ``cls.export(instance=combined, **export_params)``.
      5. Enqueue a ``combine`` AuditResult.

    Failures in any phase enqueue an audit with ``failed_sources`` populated
    but never crash the daemon.
    """
    loop_idx = 0
    while not shutdown_event.is_set():
        loop_idx += 1
        start_time = time.perf_counter()
        try:
            # Snapshot phase — under lock, O(N) pointer reads.
            async with lock:
                state = {source_classes[i].__name__: source_refs[i][0] for i in range(len(source_classes))}

            # Combine phase — user code outside the lock.
            combined_rows = combine_fn(state)
            if combined_rows is None:
                combined_rows = []

            # Build instances of cls — auto-registers into cls.inc_dict.
            # Clear first so the registry reflects the current tick's view only.
            cls.inc_dict.clear()
            combined_instances = [cls(**row) if isinstance(row, dict) else row for row in combined_rows]

            # Retain a strong reference on the class so cls.inc_dict
            # (WeakValueDictionary) stays populated between ticks. Without this,
            # the combined instances would be GC'd as soon as the daemon's
            # local list goes out of scope, defeating the "object map" contract.
            cls._fjord_snapshot = combined_instances

            # Export phase — uses the existing cls.export() pipeline.
            if combined_instances:
                await cls.export(instance=combined_instances, **export_params)

            await audit_queue.put(
                AuditResult(
                    chunk_index=loop_idx,
                    operation="combine",
                    rows_processed=len(combined_instances),
                    processing_time_sec=time.perf_counter() - start_time,
                )
            )
        except Exception as e:
            await audit_queue.put(
                AuditResult(
                    chunk_index=loop_idx,
                    operation="combine",
                    rows_processed=0,
                    failed_sources=[f"Combine Error: {e}"],
                    processing_time_sec=time.perf_counter() - start_time,
                )
            )

        if e_interval is None:
            break

        if await _interruptible_sleep(shutdown_event, e_interval):
            break


async def _run_fjord_engine(
    cls: Any,
    stream_params: List[Dict[str, Any]],
    combine_fn: Any,
    export_params: Dict[str, Any],
    r_interval: Optional[float],
    e_interval: Optional[float],
) -> AsyncGenerator[AuditResult, None]:
    """Multi-source stateful streaming engine for ``Incorporator.fjord()``.

    Generalisation of ``_run_stateful_engine`` to N sources with one
    combine-and-export daemon producing instances of ``cls``.

    Lifecycle:
      1. Seed phase: concurrent ``entry["cls"].incorp(**entry["incorp_params"])``
         across all entries via ``asyncio.gather``. One ``incorp`` audit yielded
         per source.
      2. Daemon phase: per-source refresh daemons (always), per-source export
         daemons (when ``export_params`` is set on the entry), and one combine
         daemon. All coordinate via a single shared ``asyncio.Lock``.
      3. Shutdown: ``shutdown_event.set()`` → cancel tasks → drain queue → exit.
    """
    # ------------------------------------------------------------------
    # 1. Seed phase — concurrent incorp() across all sources.
    # ------------------------------------------------------------------
    source_classes: List[Any] = [entry["cls"] for entry in stream_params]
    seed_tasks = [
        asyncio.create_task(entry["cls"].incorp(**entry["incorp_params"])) for entry in stream_params
    ]

    seed_start = time.perf_counter()
    seed_results = await asyncio.gather(*seed_tasks, return_exceptions=True)
    seed_elapsed = time.perf_counter() - seed_start

    # Validate seed phase — every source must have produced data.
    source_refs: List[List[Any]] = []
    for idx, (entry, result) in enumerate(zip(stream_params, seed_results)):
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
    # 2. Daemon phase — spawn refresh + per-stream export + combine tasks.
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

    # Always spawn the combine daemon — it's the whole point of fjord.
    tasks.append(
        asyncio.create_task(
            _combine_daemon(
                cls=cls,
                source_refs=source_refs,
                source_classes=source_classes,
                combine_fn=combine_fn,
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
    """Dual-engine pipeline dispatcher.

    Routes to ``_run_stateful_engine`` when ``stateful_polling=True`` (independent
    refresh/export daemon tasks on decoupled schedules), or ``_run_chunking_engine``
    for sequential O(1) chunked ingestion with optional continuous polling.
    """
    paginator = incorp_params.get("inc_page")

    if stateful_polling:
        async for audit in _run_stateful_engine(
            cls=cls,
            incorp_params=incorp_params,
            refresh_params=refresh_params,
            export_params=export_params,
            r_interval=refresh_interval or poll_interval,
            e_interval=export_interval or poll_interval,
        ):
            yield audit
    else:
        async for audit in _run_chunking_engine(
            cls=cls,
            incorp_params=incorp_params,
            refresh_params=refresh_params,
            export_params=export_params,
            poll_interval=poll_interval,
            paginator=paginator,
        ):
            yield audit
