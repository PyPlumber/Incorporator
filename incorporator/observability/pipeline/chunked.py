"""Chunked sequential engine (Engine 1): O(1)-memory paginator-driven streaming."""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
from collections import deque
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from pydantic import ValidationError

from ...io.fetch import _CURRENT_CHUNK_CLASS, HTTPClientBuilder
from ..logger import Wave
from ._shared import _enrich_and_load, _row_count

logger = logging.getLogger(__name__)


async def _run_chunking_engine(
    cls: Any,
    incorp_params: dict[str, Any],
    refresh_params: Optional[dict[str, Any]],
    export_params: Optional[dict[str, Any]],
    poll_interval: Optional[float],
    paginator: Any,
    adapt_chunk_size: bool = False,
    chunk_size_min: int = 100,
    chunk_size_max: int = 100_000,
    target_min_sec: float = 0.030,
    target_max_sec: float = 0.100,
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
    _aimd_enabled = adapt_chunk_size
    _ring: deque[float] = deque(maxlen=5)
    if _aimd_enabled and paginator is not None:
        if not hasattr(paginator, "chunk_size"):
            logger.debug(
                "AIMD: paginator %r has no chunk_size attribute; adaptation disabled.",
                type(paginator).__name__,
            )
            _aimd_enabled = False

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
                    conv_start = time.perf_counter()
                    token = _CURRENT_CHUNK_CLASS.set(cls)
                    try:
                        dataset = await cls.incorp(**params)
                    finally:
                        _CURRENT_CHUNK_CLASS.reset(token)
                    conv_elapsed = time.perf_counter() - conv_start

                    if not dataset and not paginator:
                        break
                    if getattr(paginator, "is_exhausted", False) and not dataset:
                        break

                    rows = _row_count(dataset)

                    if rows > 0:
                        await _enrich_and_load(cls, dataset, refresh_params, export_params, force_append=True)

                    wave_obj = Wave.model_construct(
                        chunk_index=chunk_idx,
                        operation="chunk",
                        rows_processed=rows,
                        failed_sources=[],
                        processing_time_sec=time.perf_counter() - start_time,
                        source_url=getattr(cls, "inc_url", None) or getattr(cls, "inc_file", None),
                        bytes_processed=cls._last_bytes_processed,
                        http_retry_count=cls._last_http_retry_count,
                        validation_error_count=0,
                        schema_cache_hit=cls._last_schema_cache_hit,
                        conv_dict_time_sec=conv_elapsed,
                        timestamp=datetime.now(timezone.utc),
                    )
                    yield wave_obj

                    if _aimd_enabled and paginator is not None:
                        _ring.append(wave_obj.processing_time_sec)
                        if len(_ring) == _ring.maxlen:
                            med = statistics.median(_ring)
                            current_cs = paginator.chunk_size
                            if med < target_min_sec:
                                new_cs = min(chunk_size_max, current_cs + current_cs // 5)
                            elif med > target_max_sec:
                                new_cs = max(chunk_size_min, current_cs // 2)
                            else:
                                new_cs = current_cs
                            if new_cs != current_cs:
                                paginator.chunk_size = new_cs

                    del dataset
                    # Yield the event loop so other tasks can run between chunks.
                    await asyncio.sleep(0)

                    if not paginator:
                        break
                except Exception as e:
                    val_errors = e.error_count() if isinstance(e, ValidationError) else 0
                    yield Wave.model_construct(
                        chunk_index=chunk_idx,
                        operation="chunk",
                        rows_processed=0,
                        failed_sources=[str(e)],
                        processing_time_sec=time.perf_counter() - start_time,
                        source_url=getattr(cls, "inc_url", None) or getattr(cls, "inc_file", None),
                        bytes_processed=None,
                        http_retry_count=cls._last_http_retry_count,
                        validation_error_count=val_errors,
                        schema_cache_hit=True,
                        conv_dict_time_sec=None,
                        timestamp=datetime.now(timezone.utc),
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
