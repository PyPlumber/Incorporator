"""Chunked sequential engine (Engine 1): O(1)-memory paginator-driven streaming."""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
from collections import deque
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any

import httpx
from pydantic import ValidationError

from ..io.fetch import _CURRENT_CHUNK_CLASS, HTTPClientBuilder
from ..observability.logger import Wave
from ._shared import _enrich_and_load, _row_count

logger = logging.getLogger(__name__)

# One-time-per-process WARNING dedup for the two AIMD tuner diagnostics below,
# mirroring _warn_on_bare_user_class's guard-then-emit idiom (outflow.py) so a
# long-running poll loop doesn't spam the same diagnosis every tick.
_AIMD_LOW_FLOOR_WARNED = False
_AIMD_PARKED_WARNED: set[int] = set()


async def _run_chunking_engine(
    cls: Any,
    incorp_params: dict[str, Any],
    refresh_params: dict[str, Any] | None,
    export_params: dict[str, Any] | None,
    poll_interval: float | None,
    paginator: Any,
    adapt_chunk_size: bool = False,
    chunk_size_min: int = 100,
    chunk_size_max: int = 100_000,
    # Parse-only signal target window — derived from architect.py constants:
    #   _PARSE_TOO_FAST_P50=0.001s (1 ms, chunk instant-to-parse / noise floor)
    #   _PARSE_MEMORY_P99=0.100s   (100 ms, parse/validate dominates)
    # Old 0.030 s floor was calibrated for end-to-end including HTTP latency;
    # with HTTP stripped the "too fast" threshold is 1 ms, not 30 ms.
    target_min_sec: float = 0.001,
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
    shared_client: httpx.AsyncClient | None = None
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
    # Hysteresis state: an adjustment only mutates ``paginator.chunk_size`` once its
    # direction is recorded; ``_aimd_last_dir`` tracks the most recently APPLIED
    # direction and ``_aimd_alternation_streak`` counts consecutive applied
    # adjustments that reversed direction from the one before. A genuine regime
    # change reverses once and then either holds or continues in the new
    # direction, so it never accumulates a streak — only a size-independent,
    # flip-flopping signal (e.g. alternating cache hit/miss latency) keeps
    # reversing decision after decision. Once the streak crosses the threshold,
    # the tuner parks (stops mutating) and emits a single WARNING.
    _aimd_last_dir: str | None = None
    _aimd_alternation_streak = 0
    _aimd_parked = False
    _AIMD_ALTERNATION_PARK_THRESHOLD = 3
    if _aimd_enabled and paginator is not None:
        if not hasattr(paginator, "chunk_size"):
            logger.debug(
                "AIMD: paginator %r has no chunk_size attribute; adaptation disabled.",
                type(paginator).__name__,
            )
            _aimd_enabled = False
        elif chunk_size_min < 5:
            global _AIMD_LOW_FLOOR_WARNED
            if not _AIMD_LOW_FLOOR_WARNED:
                _AIMD_LOW_FLOOR_WARNED = True
                logger.warning(
                    "AIMD: chunk_size_min=%d is unusually small (< 5); the tuner's "
                    "growth step is floored at +1 per decision at this size, so "
                    "convergence toward chunk_size_max will be slow.",
                    chunk_size_min,
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
                    conv_start = time.perf_counter()
                    token = _CURRENT_CHUNK_CLASS.set(cls)
                    try:
                        dataset = await cls.incorp(**params)
                    finally:
                        _CURRENT_CHUNK_CLASS.reset(token)
                    conv_elapsed = time.perf_counter() - conv_start

                    if not dataset and not paginator:
                        _pending_rejects = list(dataset.rejects) if hasattr(dataset, "rejects") else []
                        if _pending_rejects:
                            yield Wave.model_construct(
                                chunk_index=chunk_idx,
                                operation="chunk",
                                rows_processed=0,
                                failed_sources=[],
                                rejects=_pending_rejects,
                                processing_time_sec=time.perf_counter() - start_time,
                                source_url=getattr(cls, "inc_url", None) or getattr(cls, "inc_file", None),
                                bytes_processed=None,
                                bytes_downloaded=None,
                                http_fetch_time_sec=None,
                                http_retry_count=cls._last_http_retry_count,
                                validation_error_count=0,
                                schema_cache_hit=True,
                                conv_dict_time_sec=conv_elapsed,
                                timestamp=datetime.now(timezone.utc),
                            )
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
                        rejects=list(dataset.rejects) if hasattr(dataset, "rejects") else [],
                        processing_time_sec=time.perf_counter() - start_time,
                        source_url=getattr(cls, "inc_url", None) or getattr(cls, "inc_file", None),
                        bytes_processed=cls._last_bytes_processed,
                        bytes_downloaded=cls._last_bytes_downloaded,
                        http_fetch_time_sec=cls._last_http_fetch_time_sec,
                        http_retry_count=cls._last_http_retry_count,
                        validation_error_count=0,
                        schema_cache_hit=cls._last_schema_cache_hit,
                        conv_dict_time_sec=conv_elapsed,
                        timestamp=datetime.now(timezone.utc),
                    )
                    yield wave_obj

                    if _aimd_enabled and paginator is not None:
                        # Parse-only signal: aligns AIMD window with _tune_chunk_size's split-time path.
                        # Thresholds derived from architect.py: _PARSE_TOO_FAST_P50=0.001s, _PARSE_MEMORY_P99=0.100s.
                        # For file-mode (http_fetch_time_sec=None after P0 reset), end-to-end IS parse/IO only.
                        if wave_obj.http_fetch_time_sec is not None:
                            _ring.append(max(0.0, wave_obj.processing_time_sec - wave_obj.http_fetch_time_sec))
                        else:
                            _ring.append(wave_obj.processing_time_sec)
                        if len(_ring) == _ring.maxlen:
                            med = statistics.median(_ring)
                            current_cs = paginator.chunk_size
                            direction: str | None
                            if med < target_min_sec:
                                direction = "grow"
                                new_cs = min(chunk_size_max, current_cs + max(1, current_cs // 5))
                            elif med > target_max_sec:
                                direction = "shrink"
                                new_cs = max(chunk_size_min, current_cs // 2)
                            else:
                                direction = None
                                new_cs = current_cs
                            if not _aimd_parked and direction is not None and new_cs != current_cs:
                                if _aimd_last_dir is not None and direction != _aimd_last_dir:
                                    _aimd_alternation_streak += 1
                                else:
                                    _aimd_alternation_streak = 0
                                if _aimd_alternation_streak >= _AIMD_ALTERNATION_PARK_THRESHOLD:
                                    _aimd_parked = True
                                    if id(paginator) not in _AIMD_PARKED_WARNED:
                                        _AIMD_PARKED_WARNED.add(id(paginator))
                                        logger.warning(
                                            "AIMD: chunk_size tuner detected a flip-flopping, "
                                            "size-independent latency signal (direction reversed "
                                            "%d times in a row) — parking at chunk_size=%d instead "
                                            "of limit-cycling. This usually means the P50/P99 "
                                            "targets don't match a bimodal or cache-dependent "
                                            "workload; consider disabling adapt_chunk_size or "
                                            "widening target_min_sec/target_max_sec.",
                                            _aimd_alternation_streak,
                                            current_cs,
                                        )
                                else:
                                    paginator.chunk_size = new_cs
                                    _aimd_last_dir = direction
                                # Post-adjustment cooldown: stale pre-adjustment samples must not
                                # drive the next decision. The len(_ring)==maxlen guard above then
                                # forces 5 fresh samples under the new chunk_size before deciding again.
                                _ring.clear()

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
                        rejects=[],
                        processing_time_sec=time.perf_counter() - start_time,
                        source_url=getattr(cls, "inc_url", None) or getattr(cls, "inc_file", None),
                        bytes_processed=None,
                        bytes_downloaded=None,
                        http_fetch_time_sec=None,
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
