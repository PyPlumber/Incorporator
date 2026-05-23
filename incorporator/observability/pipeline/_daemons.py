"""Per-source refresh and export daemons used by stateful and fjord engines."""

from __future__ import annotations

import asyncio
from typing import Any, Optional

from ..logger import Wave  # re-exported for callers that still import it from here
from ._shared import _daemon_tick, _interruptible_sleep, _resolve_if_exists_for_export, _row_count

__all__ = ["Wave", "_refresh_daemon", "_export_daemon"]


async def _refresh_daemon(
    cls: Any,
    dataset_ref: list[Any],
    refresh_params: dict[str, Any],
    lock: asyncio.Lock,
    wave_queue: "asyncio.Queue[Optional[Wave]]",
    shutdown_event: asyncio.Event,
    r_interval: Optional[float],
    operation_label: str = "refresh",
) -> None:
    """Periodically re-fetch the source and atomically update the in-memory registry.

    Each tick calls ``cls.refresh(instance=dataset_ref[0], **refresh_params)`` under
    ``lock`` so the export daemon never reads a half-mutated state. Sleeps
    ``r_interval`` between ticks; exits cleanly when ``shutdown_event`` is set.

    ``operation_label`` overrides the :attr:`Wave.operation` field so the fjord
    engine can tag refreshes per source class (e.g. ``"fjord_refresh:Coin"``).
    Defaults to ``"refresh"`` for the single-source stateful engine.

    Observability:
        Enqueues one :class:`Wave` per iteration into ``wave_queue`` — success or
        failure — for the drain loop to yield downstream.
    """
    loop_idx = 0
    while not shutdown_event.is_set():
        loop_idx += 1
        async with _daemon_tick(
            wave_queue,
            chunk_index=loop_idx,
            operation=operation_label,
            error_prefix="Refresh Error",
            row_count_fn=lambda: _row_count(dataset_ref[0]),
        ):
            async with lock:  # ENSURE ATOMIC MUTATION
                refreshed = await cls.refresh(instance=dataset_ref[0], **refresh_params)
                if refreshed is not None:
                    dataset_ref[0] = refreshed

        if r_interval is None:
            break

        if await _interruptible_sleep(shutdown_event, r_interval):
            break


async def _export_daemon(
    cls: Any,
    dataset_ref: list[Any],
    export_params: dict[str, Any],
    lock: asyncio.Lock,
    wave_queue: "asyncio.Queue[Optional[Wave]]",
    shutdown_event: asyncio.Event,
    e_interval: Optional[float],
    operation_label: str = "export",
) -> None:
    """Periodically snapshot the in-memory registry and write it to disk.

    Each tick captures ``dataset_ref[0]`` under ``lock`` (O(1) pointer copy), then
    releases the lock before the actual export so ``_refresh_daemon`` can proceed
    concurrently during long I/O writes (e.g. 10 M-row exports). Sleeps
    ``e_interval`` between ticks; exits cleanly when ``shutdown_event`` is set.

    ``operation_label`` overrides the :attr:`Wave.operation` field so the fjord
    engine can tag per-source exports (e.g. ``"export:BinanceFutures"``). Defaults
    to ``"export"`` for the single-source stateful engine.

    Observability:
        Enqueues one :class:`Wave` per iteration into ``wave_queue`` — success or
        failure — for the drain loop to yield downstream.
    """
    loop_idx = 0
    # The export body re-binds ``snapshot`` per tick — capture it via a
    # nullable closure so ``row_count_fn`` sees the latest value without
    # passing it through the helper's signature.
    snapshot: list[Any] = [None]
    while not shutdown_event.is_set():
        loop_idx += 1
        async with _daemon_tick(
            wave_queue,
            chunk_index=loop_idx,
            operation=operation_label,
            error_prefix="Export Error",
            row_count_fn=lambda: _row_count(snapshot[0]),
        ):
            async with lock:
                snapshot[0] = dataset_ref[0]
            # Stateful daemon semantics: every tick re-exports the SAME
            # registry (same rows updated in place by refresh()), so the
            # destination file should always hold the latest snapshot,
            # never accumulate duplicates of the same records.  We
            # therefore force ``replace`` semantics on every tick unless
            # the user explicitly asks for ``if_exists="append"`` (e.g.
            # "log every snapshot to a forensic NDJSON archive").
            #
            # The ``_resolve_if_exists_for_export`` helper with
            # ``force_append=False`` returns None for "use handler
            # default", which is "replace" for every append-friendly
            # format (NDJSON / CSV / SQLite / Avro) and the only
            # supported mode for monolithic formats (Parquet / Excel /
            # XML / JSON).
            resolved = _resolve_if_exists_for_export(
                file_path=export_params.get("file_path"),
                force_append=False,
                user_override=export_params.get("if_exists"),
            )
            params = export_params if resolved is None else {**export_params, "if_exists": resolved}
            await cls.export(instance=snapshot[0], **params)

        if e_interval is None:
            break

        if await _interruptible_sleep(shutdown_event, e_interval):
            break
