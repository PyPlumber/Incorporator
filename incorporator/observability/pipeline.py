"""
Autonomous Orchestration Pipeline for the Incorporator Framework.
Handles Dual-Engine execution (O(1) Chunking and Stateful Polling).
"""

import asyncio
import gc
import logging
import time
from typing import Any, AsyncGenerator, Dict, List, Optional, cast

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


async def _outflow_daemon(
    output_class_name: str,
    base_class: Any,
    source_refs: List[List[Any]],
    source_classes: List[Any],
    outflow_fn: Any,
    export_params: Dict[str, Any],
    lock: asyncio.Lock,
    audit_queue: "asyncio.Queue[Optional[AuditResult]]",
    shutdown_event: asyncio.Event,
    e_interval: Optional[float],
) -> None:
    """Periodic outflow-and-export daemon for the fjord engine.

    On every tick:
      1. Snapshot each ``source_refs[i][0]`` under ``lock`` into a state dict
         keyed by ``source_classes[i].__name__`` (O(N) pointer copies, not deep
         copies — release the lock fast).
      2. Outside the lock, invoke the user's ``outflow_fn(state)``.  The return
         value is normalised to ``list[dict]`` (a single ``dict`` is wrapped;
         ``None``/empty is treated as a zero-row tick).
      3. On an empty return, emit an ``outflow:<ClassName>`` audit with
         ``rows_processed=0`` and skip the build/export — same behaviour as a
         stream chunk that yielded zero rows.
      4. Otherwise: build (or cache-hit) the dynamic output class via
         ``infer_dynamic_schema(output_class_name, rows, base_class)``. The
         schema registry is keyed by ``(name, frozenset(field_keys), id(base))``
         so successive ticks with the same row shape return the same class
         object — no class churn.
      5. Clear the dynamic class's ``inc_dict`` (so the registry reflects only
         this tick's view), instantiate one object per row (auto-registering
         via Pydantic's ``model_post_init``), retain a strong-ref snapshot on
         the class to defeat the WeakValueDictionary GC, and export via the
         existing ``DynamicCls.export()`` pipeline.

    Failures in any phase enqueue an audit with ``failed_sources`` populated
    but never crash the daemon.
    """
    # Local import keeps the observability layer free of a hard schema dep.
    from ..schema.builder import infer_dynamic_schema

    operation = f"outflow:{output_class_name}"
    loop_idx = 0
    while not shutdown_event.is_set():
        loop_idx += 1
        start_time = time.perf_counter()
        try:
            # Snapshot phase — under lock, O(N) pointer reads.
            async with lock:
                state = {source_classes[i].__name__: source_refs[i][0] for i in range(len(source_classes))}

            # Outflow phase — user code outside the lock.
            rows = outflow_fn(state)
            if isinstance(rows, dict):
                rows = [rows]
            elif rows is None:
                rows = []
            else:
                rows = list(rows)

            if not rows:
                # Zero-row tick: audit and continue. No dynamic class needed.
                await audit_queue.put(
                    AuditResult(
                        chunk_index=loop_idx,
                        operation=operation,
                        rows_processed=0,
                        processing_time_sec=time.perf_counter() - start_time,
                    )
                )
            else:
                # Build (or cache-hit) the dynamic output class from the row shape.
                # base_class is an Incorporator subclass (`base.py` plumbs it
                # through as `Incorporator`); infer_dynamic_schema returns a
                # Pydantic subclass inheriting from it, so the runtime object
                # has Incorporator's inc_dict / export / _fjord_snapshot
                # attributes — cast for mypy.
                DynamicCls = cast(Any, infer_dynamic_schema(output_class_name, rows, base_class))

                # Reset registry for this tick's view, then materialise instances.
                DynamicCls.inc_dict.clear()
                instances = [DynamicCls(**row) for row in rows]

                # Retain a strong reference on the class so DynamicCls.inc_dict
                # (WeakValueDictionary) stays populated between ticks. Without
                # this the instances would be GC'd as soon as the daemon's
                # local list goes out of scope, defeating the "object map"
                # contract.
                DynamicCls._fjord_snapshot = instances

                await DynamicCls.export(instance=instances, **export_params)

                await audit_queue.put(
                    AuditResult(
                        chunk_index=loop_idx,
                        operation=operation,
                        rows_processed=len(instances),
                        processing_time_sec=time.perf_counter() - start_time,
                    )
                )
        except Exception as e:
            await audit_queue.put(
                AuditResult(
                    chunk_index=loop_idx,
                    operation=operation,
                    rows_processed=0,
                    failed_sources=[f"Outflow Error: {e}"],
                    processing_time_sec=time.perf_counter() - start_time,
                )
            )

        if e_interval is None:
            break

        if await _interruptible_sleep(shutdown_event, e_interval):
            break


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
