"""Fjord-specific outflow daemon: snapshot sources, run user fn, export combined output."""

import asyncio
import time
from typing import Any, Dict, List, Optional, cast

from ..logger import AuditResult
from ._shared import _interruptible_sleep


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
    # Local import keeps the observability layer free of a hard schema dep at
    # module-import time.
    from ...schema.builder import infer_dynamic_schema

    operation = f"outflow:{output_class_name}"
    loop_idx = 0
    # Pre-compute state dict keys once — avoids re-allocating the key list on
    # every tick.  The key order is stable for the lifetime of the daemon.
    state_keys = [cls.__name__ for cls in source_classes]
    while not shutdown_event.is_set():
        loop_idx += 1
        start_time = time.perf_counter()
        try:
            # Snapshot phase — under lock, O(N) pointer reads only.
            # dict(zip(...)) is slightly faster than a comprehension with index
            # arithmetic and reuses the pre-computed key list.
            async with lock:
                state = dict(zip(state_keys, [ref[0] for ref in source_refs]))

            # Outflow phase — user code outside the lock.
            # asyncio.to_thread releases the GIL so CPU-heavy outflow functions
            # (e.g. complex multi-source joins) don't block refresh/export
            # daemons running on other sources.
            rows = await asyncio.to_thread(outflow_fn, state)
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
