"""Chunked sequential engine (Engine 1): O(1)-memory paginator-driven streaming."""

import asyncio
import time
from typing import Any, AsyncGenerator, Dict, Optional

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
    # The monolithic-format + paginator data-loss guard runs at the
    # ``stream()`` call site via ``assert_engine_supported`` so the
    # traceback points at the user code, not at this generator.
    #
    # Build ONE httpx.AsyncClient for the entire drain and thread it via
    # the ``_client`` hook ``fetch_concurrent_payloads`` already accepts.
    # Previously each chunk rebuilt the client, paying HTTP/2 settings
    # negotiation + TLS handshake + keepalive-pool init N times per N-chunk
    # paginated drain.  File-mode incorps ignore the client (``fetch.py``
    # short-circuits the client construction when ``is_file_mode`` is True
    # AND no ``_client`` was passed); the unused client we built costs
    # only the constructor allocation, reclaimed on ``aclose()`` below.
    #
    # Each drain owns its own client so distinct Tideweaver currents (with
    # different timeouts / headers / SSL config in their ``incorp_params``)
    # keep working as they do today — the pooling is within a drain, not
    # across drains.
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

                # Always copy so the ``_client`` slot-in doesn't mutate the
                # caller's ``incorp_params``.  ``setdefault`` honours an
                # already-supplied ``_client`` (a future caller threading
                # their own takes precedence over our shared client).
                if paginator:
                    if getattr(paginator, "is_exhausted", False):
                        break
                    params = incorp_params.copy()
                    params["call_lim"] = 1
                else:
                    params = dict(incorp_params)
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
    finally:
        await shared_client.aclose()
