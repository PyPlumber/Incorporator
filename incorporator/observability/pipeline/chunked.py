"""Chunked sequential engine (Engine 1): O(1)-memory paginator-driven streaming."""

import asyncio
import time
from typing import Any, AsyncGenerator, Dict, Optional

import httpx

from ...io.fetch import HTTPClientBuilder
from ..logger import Wave
from ._shared import _enrich_and_load, _row_count


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
    # Build one shared client per drain.  Skip when in file-mode (httpx
    # unused) or when the caller supplied one (we don't own its lifetime).
    is_file_mode = bool(incorp_params.get("inc_file"))
    caller_supplied_client = "_client" in incorp_params
    shared_client: Optional[httpx.AsyncClient] = None
    if not is_file_mode and not caller_supplied_client:
        shared_client = HTTPClientBuilder.build_client(
            concurrency_limit=incorp_params.get("concurrency_limit", 50),
            ignore_ssl=incorp_params.get("ignore_ssl", False),
            timeout=incorp_params.get("timeout", 15.0),
            headers=incorp_params.get("headers"),
            block_internal_redirects=incorp_params.get("block_internal_redirects", False),
        )
    try:
        while True:
            chunk_idx = 0
            while True:
                chunk_idx += 1
                start_time = time.perf_counter()

                # Copy so the ``_client`` slot-in doesn't mutate the caller's dict.
                if paginator:
                    if getattr(paginator, "is_exhausted", False):
                        break
                    params = incorp_params.copy()
                    params["call_lim"] = 1
                else:
                    params = dict(incorp_params)
                if shared_client is not None:
                    params.setdefault("_client", shared_client)

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
    finally:
        if shared_client is not None:
            await shared_client.aclose()
