"""Chunked sequential engine (Engine 1): O(1)-memory paginator-driven streaming."""

import asyncio
import gc
import time
from typing import Any, AsyncGenerator, Dict, Optional

from ..logger import AuditResult
from ._shared import _enrich_and_load, _row_count


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
