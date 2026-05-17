"""Chunked sequential engine (Engine 1): O(1)-memory paginator-driven streaming."""

import asyncio
import time
from typing import Any, AsyncGenerator, Dict, Optional

from ...exceptions import IncorporatorFormatError
from ...io.formats import infer_format
from ...io.handlers._base import supports_append
from ..logger import Wave
from ._shared import _enrich_and_load, _row_count


def _pre_flight_chunked_append_check(
    export_params: Optional[Dict[str, Any]],
    paginator: Any,
) -> None:
    """Fail-fast guard for paginated chunked streams hitting append-rejected formats.

    Chunked mode produces NEW data per chunk (different rows each tick).
    Append-rejected formats (Parquet / Feather / ORC / Excel / XML / JSON)
    can't accumulate chunks — every chunk would clobber the prior chunk's
    output, which is silent data loss.  When a paginator is in play AND the
    export target rejects append, raise immediately so the user picks an
    append-friendly format (NDJSON / CSV / SQLite / Avro) before the
    pipeline starts running.

    Single-shot chunked mode (no paginator) is exempt: only one chunk
    fires, so monolithic targets are fine.
    """
    if export_params is None or paginator is None:
        return
    file_path = export_params.get("file_path")
    if file_path is None:
        return
    try:
        fmt = infer_format(file_path)
    except Exception:
        return
    if supports_append(fmt):
        return
    raise IncorporatorFormatError(
        f"Chunked streaming to {fmt.value!r} would lose data — every chunk would "
        f"overwrite the prior chunk's output.  Switch the export target to an "
        f"append-friendly format (.ndjson / .csv / .sqlite / .avro), drop the "
        f"paginator for a single-shot write, or use stateful_polling=True if you "
        f"want the file to always hold the latest registry snapshot."
    )


async def _run_chunking_engine(
    cls: Any,
    incorp_params: Dict[str, Any],
    refresh_params: Optional[Dict[str, Any]],
    export_params: Optional[Dict[str, Any]],
    poll_interval: Optional[float],
    paginator: Any,
) -> AsyncGenerator[Wave, None]:
    """Stream a paginated source one chunk at a time, with flat memory.

    Loops over paginator-driven or single-shot ``incorp()`` calls, calling
    ``_enrich_and_load`` per chunk. Releases each dataset from memory immediately
    after yielding so RSS stays flat regardless of total data volume. Sleeps
    ``poll_interval`` between full passes when continuous polling is requested.

    This is the engine behind ``stream(stateful_polling=False)`` — the right shape
    for bulk drains of paginated sources, historical backfills, and warehouse seeds
    where each chunk is independent of the next.

    Yields:
        Wave: one per chunk, success or failure.
    """
    # Pre-flight: monolithic export targets are incompatible with paginated
    # chunked streaming (every chunk would clobber the prior).  Fail loud
    # before the pipeline starts, not on chunk 2 mid-write.
    _pre_flight_chunked_append_check(export_params, paginator)

    while True:
        chunk_idx = 0
        while True:
            chunk_idx += 1
            start_time = time.perf_counter()

            # Only copy incorp_params when we must mutate it (paginator path).
            # Single-shot mode never mutates params, so we skip the copy.
            if paginator:
                if getattr(paginator, "is_exhausted", False):
                    break
                params = incorp_params.copy()
                params["call_lim"] = 1
            else:
                params = incorp_params

            try:
                dataset = await cls.incorp(**params)

                if not dataset and not paginator:
                    break
                if getattr(paginator, "is_exhausted", False) and not dataset:
                    break

                rows = _row_count(dataset)

                if rows > 0:
                    await _enrich_and_load(cls, dataset, refresh_params, export_params, force_append=True)

                yield Wave(
                    chunk_index=chunk_idx,
                    operation="chunk",
                    rows_processed=rows,
                    processing_time_sec=time.perf_counter() - start_time,
                )

                del dataset
                # Yield the event loop so other tasks can run between chunks.
                # Manual gc.collect() removed — Python's generational GC handles
                # short-lived datasets without manual intervention, and calling
                # gc.collect() here would block the event loop for milliseconds.
                await asyncio.sleep(0)

                if not paginator:
                    break
            except Exception as e:
                yield Wave(
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
