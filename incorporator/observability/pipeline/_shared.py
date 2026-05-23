"""Cross-engine helpers shared by chunked / stateful / fjord engines."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from ..logger import Wave


@asynccontextmanager
async def _daemon_tick(
    wave_queue: "asyncio.Queue[Wave | None]",
    *,
    chunk_index: int,
    operation: str,
    error_prefix: str,
    row_count_fn: Callable[[], int],
) -> AsyncIterator[None]:
    """Wrap a daemon tick in uniform timing + Wave-enqueue + error handling.

    Use this from any daemon body that follows the standard pattern:

        async with _daemon_tick(wave_queue, chunk_index=i, operation="export",
                                 error_prefix="Export Error",
                                 row_count_fn=lambda: _row_count(snapshot)):
            ...  # the work — may raise, may mutate state

    On clean exit it emits a success :class:`Wave` carrying the row count
    and elapsed time.  On exception it converts the error to a failed
    :class:`Wave` (``rows_processed=0``, prefixed message in
    ``failed_sources``) and **suppresses the raise** so the daemon's outer
    loop can keep ticking — that matches the long-standing
    "never crash the daemon on a transient error" contract.

    ``row_count_fn`` is invoked AFTER the body succeeds, so it sees any
    post-mutation state the body produced (e.g. ``_refresh_daemon``
    rebinding ``dataset_ref[0]``).
    """
    start = time.perf_counter()
    try:
        yield
    except Exception as exc:
        await wave_queue.put(
            Wave.model_construct(
                chunk_index=chunk_index,
                operation=operation,
                rows_processed=0,
                failed_sources=[f"{error_prefix}: {exc}"],
                processing_time_sec=0.0,
                source_url=None,
                bytes_processed=None,
                http_retry_count=0,
                validation_error_count=0,
                schema_cache_hit=True,
                conv_dict_time_sec=None,
                timestamp=datetime.now(timezone.utc),
            )
        )
        return
    await wave_queue.put(
        Wave.model_construct(
            chunk_index=chunk_index,
            operation=operation,
            rows_processed=row_count_fn(),
            processing_time_sec=time.perf_counter() - start,
            source_url=None,
            bytes_processed=None,
            http_retry_count=0,
            validation_error_count=0,
            schema_cache_hit=True,
            conv_dict_time_sec=None,
            timestamp=datetime.now(timezone.utc),
        )
    )


async def _interruptible_sleep(event: asyncio.Event, timeout: float | None) -> bool:
    """Sleeps for `timeout` seconds, returning True immediately if `event` fires first."""
    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        return False


def _resolve_if_exists_for_export(
    file_path: str | None,
    force_append: bool,
    user_override: str | None,
) -> str | None:
    """Decide the ``if_exists`` value for a streaming export tick.

    Pipeline behaviour contract:
      * User passed ``if_exists`` explicitly → honour it verbatim.
      * ``force_append=False`` (single-shot, OR every tick in a
        **stateful daemon** where the SAME registry is re-exported)
        → return None, let the handler use its default ("replace"
        semantics).
      * ``force_append=True`` (subsequent ticks of the **chunked
        engine** where each tick brings NEW data) AND format
        supports append → "append".
      * ``force_append=True`` AND format CANNOT append → "replace" so
        the monolithic file always holds the latest registry snapshot
        rather than crashing on the second tick.

    Callers:
      * Chunked engine (``_enrich_and_load``) — uses
        ``force_append=True`` after chunk 1.
      * Stateful daemons (``_export_daemon``, ``_outflow_daemon``) —
        ALWAYS use ``force_append=False`` so each tick replaces (the
        same records were just re-exported in place, appending would
        duplicate).

    Returns the chosen ``if_exists`` value, or None when no override is
    needed (handler default applies).
    """
    if user_override is not None:
        return user_override  # explicit user wins
    if not force_append:
        return None  # first tick / single-shot
    # Subsequent tick: prefer append on supported formats, else replace.
    from ...io.formats import infer_format
    from ...io.handlers._base import supports_append

    if file_path is None:
        return "append"  # no path to inspect; legacy default
    try:
        fmt = infer_format(file_path)
    except Exception:
        return "append"  # unknowable → assume append-friendly
    return "append" if supports_append(fmt) else "replace"


async def _enrich_and_load(
    cls: Any,
    dataset: Any,
    refresh_params: dict[str, Any] | None,
    export_params: dict[str, Any] | None,
    force_append: bool,
) -> None:
    """Atomic helper for the Enrich (Refresh) and Load (Export) phases."""
    # ``is not None`` rather than truthy: empty dict ``{}`` MUST opt into the
    # call ("run with default kwargs"); a truthy check treats ``{}`` as falsy
    # and silently skips, contradicting the documented contract.
    if refresh_params is not None:
        await cls.refresh(instance=dataset, **refresh_params)

    if export_params is not None:
        resolved = _resolve_if_exists_for_export(
            file_path=export_params.get("file_path"),
            force_append=force_append,
            user_override=export_params.get("if_exists"),
        )
        if resolved is None:
            await cls.export(instance=dataset, **export_params)
        else:
            params = export_params.copy()
            params["if_exists"] = resolved
            await cls.export(instance=dataset, **params)


def _row_count(dataset: Any) -> int:
    """Returns the number of rows in a dataset (list length, 1 for a single object, 0 for falsy)."""
    return len(dataset) if isinstance(dataset, list) else (1 if dataset else 0)
