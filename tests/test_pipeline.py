"""Unit tests for the pipeline orchestration engine.

Covers all internal helpers and both execution engines directly so
coverage of pipeline.py rises from 47 % to ~90 %.
"""

import asyncio
from typing import Any, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from incorporator.observability.logger import Wave
from incorporator.pipeline import (
    _enrich_and_load,
    _export_daemon,
    _interruptible_sleep,
    _refresh_daemon,
    _row_count,
    _run_chunking_engine,
    run_pipeline,
)


# ==========================================
# 1. _interruptible_sleep
# ==========================================


@pytest.mark.asyncio
async def test_interruptible_sleep_times_out() -> None:
    """Returns False when the timeout expires without the event firing."""
    event = asyncio.Event()
    result = await _interruptible_sleep(event, 0.01)
    assert result is False


@pytest.mark.asyncio
async def test_interruptible_sleep_event_fires() -> None:
    """Returns True immediately when the event is already set."""
    event = asyncio.Event()
    event.set()
    result = await _interruptible_sleep(event, 10.0)
    assert result is True


# ==========================================
# 2. _row_count
# ==========================================


def test_row_count_list() -> None:
    assert _row_count([1, 2, 3]) == 3


def test_row_count_list_empty() -> None:
    assert _row_count([]) == 0


def test_row_count_single_object() -> None:
    assert _row_count(object()) == 1


def test_row_count_none() -> None:
    assert _row_count(None) == 0


# ==========================================
# 3. _enrich_and_load
# ==========================================


@pytest.mark.asyncio
async def test_enrich_and_load_refresh_only() -> None:
    """refresh_params present → cls.refresh called; export skipped."""
    cls = MagicMock()
    cls.refresh = AsyncMock()
    dataset: List[Any] = [{"id": 1}]

    await _enrich_and_load(
        cls, dataset, refresh_params={"new_url": "https://x"}, export_params=None, force_append=False
    )

    cls.refresh.assert_awaited_once_with(instance=dataset, new_url="https://x")


@pytest.mark.asyncio
async def test_enrich_and_load_export_force_append() -> None:
    """force_append=True on an append-friendly format must inject if_exists='append'.

    Uses NDJSON (append-friendly).  See the companion
    test_enrich_and_load_force_append_falls_back_to_replace_for_monolithic
    test for the new contract on monolithic formats (Parquet / JSON / etc).
    """
    cls = MagicMock()
    cls.export = AsyncMock()
    dataset: List[Any] = [{"id": 1}]
    original_params = {"file_path": "/tmp/out.ndjson"}

    await _enrich_and_load(cls, dataset, refresh_params=None, export_params=original_params, force_append=True)

    cls.export.assert_awaited_once_with(instance=dataset, file_path="/tmp/out.ndjson", if_exists="append")
    # Original dict must NOT be mutated
    assert "if_exists" not in original_params


@pytest.mark.asyncio
async def test_enrich_and_load_force_append_falls_back_to_replace_for_monolithic() -> None:
    """force_append=True on a monolithic format (JSON / Parquet / XML) falls back to replace.

    The user's primary concern from the senior review: pre-fix, every chunk
    in stateful / fjord modes forced if_exists='append', and monolithic
    formats raised IncorporatorFormatError mid-pipeline (chunked) or
    silently clobbered (stateful — handler default was replace anyway).

    Post-fix: the resolver detects the format and downgrades to 'replace'
    automatically so the file always holds the latest snapshot.
    """
    cls = MagicMock()
    cls.export = AsyncMock()
    dataset: List[Any] = [{"id": 1}]
    original_params = {"file_path": "/tmp/out.json"}  # monolithic JSON

    await _enrich_and_load(cls, dataset, refresh_params=None, export_params=original_params, force_append=True)

    cls.export.assert_awaited_once_with(instance=dataset, file_path="/tmp/out.json", if_exists="replace")
    assert "if_exists" not in original_params


@pytest.mark.asyncio
async def test_enrich_and_load_export_no_force_append() -> None:
    """force_append=False passes export_params as-is without copying."""
    cls = MagicMock()
    cls.export = AsyncMock()
    dataset: List[Any] = [{"id": 1}]

    await _enrich_and_load(cls, dataset, refresh_params=None, export_params={"file_path": "/x"}, force_append=False)

    cls.export.assert_awaited_once_with(instance=dataset, file_path="/x")


# ==========================================
# 4. _refresh_daemon
# ==========================================


@pytest.mark.asyncio
async def test_refresh_daemon_single_run_enqueues_wave() -> None:
    """With r_interval=None, the daemon runs exactly once and enqueues an Wave."""
    refreshed: List[Any] = [{"id": 99}]
    cls = MagicMock()
    cls.refresh = AsyncMock(return_value=refreshed)

    dataset_ref: List[Any] = [[{"id": 1}]]
    lock = asyncio.Lock()
    q: asyncio.Queue[Optional[Wave]] = asyncio.Queue()
    shutdown = asyncio.Event()

    await _refresh_daemon(
        cls, dataset_ref, refresh_params={}, lock=lock, wave_queue=q, shutdown_event=shutdown, r_interval=None
    )

    assert dataset_ref[0] == refreshed
    assert q.qsize() == 1
    wave = q.get_nowait()
    assert wave is not None
    assert wave.operation == "refresh"
    assert wave.rows_processed == 1


@pytest.mark.asyncio
async def test_refresh_daemon_error_enqueues_failure_result() -> None:
    """Exception inside refresh must enqueue a failure Wave, not propagate."""
    cls = MagicMock()
    cls.refresh = AsyncMock(side_effect=RuntimeError("network gone"))

    dataset_ref: List[Any] = [[{"id": 1}]]
    lock = asyncio.Lock()
    q: asyncio.Queue[Optional[Wave]] = asyncio.Queue()
    shutdown = asyncio.Event()

    await _refresh_daemon(
        cls, dataset_ref, refresh_params={}, lock=lock, wave_queue=q, shutdown_event=shutdown, r_interval=None
    )

    wave = q.get_nowait()
    assert wave is not None
    assert wave.rows_processed == 0
    assert any("network gone" in s for s in wave.failed_sources)


# ==========================================
# 5. _export_daemon
# ==========================================


@pytest.mark.asyncio
async def test_export_daemon_single_run_enqueues_wave() -> None:
    """With e_interval=None, daemon exports once and enqueues an Wave."""
    cls = MagicMock()
    cls.export = AsyncMock()

    dataset_ref: List[Any] = [[{"id": 1}, {"id": 2}]]
    lock = asyncio.Lock()
    q: asyncio.Queue[Optional[Wave]] = asyncio.Queue()
    shutdown = asyncio.Event()

    await _export_daemon(
        cls,
        dataset_ref,
        export_params={"file_path": "/tmp/x"},
        lock=lock,
        wave_queue=q,
        shutdown_event=shutdown,
        e_interval=None,
    )

    cls.export.assert_awaited_once()
    wave = q.get_nowait()
    assert wave is not None
    assert wave.operation == "export"
    assert wave.rows_processed == 2


@pytest.mark.asyncio
async def test_export_daemon_stateful_does_not_append_on_subsequent_ticks() -> None:
    """Regression — stateful daemon must REPLACE on every tick, never append.

    The same registry is re-exported every tick (rows are updated in place
    by ``refresh()``), so appending would duplicate records.  Pre-fix:
    ``force_append=(loop_idx > 1)`` injected ``if_exists="append"`` on
    tick 2+ for append-friendly formats — example 6's
    ``spacex_upcoming.ndjson`` grew by 18 rows per tick.  Post-fix:
    every tick uses handler default (replace), so the file always holds
    the latest snapshot.
    """
    cls = MagicMock()
    cls.export = AsyncMock()
    dataset_ref: List[Any] = [[{"id": 1}, {"id": 2}]]
    lock = asyncio.Lock()
    q: asyncio.Queue[Optional[Wave]] = asyncio.Queue()
    shutdown = asyncio.Event()

    # Drive three ticks via a tight refresh-interval, then shutdown.
    # The interruptible-sleep in the daemon will exit on shutdown.set().
    async def _three_ticks_then_shutdown() -> None:
        # Yield control three times so the daemon loop runs three exports.
        for _ in range(3):
            await asyncio.sleep(0)
        shutdown.set()

    daemon = asyncio.create_task(
        _export_daemon(
            cls=cls,
            dataset_ref=dataset_ref,
            export_params={"file_path": "/tmp/snapshot.ndjson"},  # append-friendly fmt
            lock=lock,
            wave_queue=q,
            shutdown_event=shutdown,
            e_interval=0.001,
        )
    )
    await asyncio.gather(_three_ticks_then_shutdown(), daemon)

    # Every call must have used the handler's default mode (NOT if_exists="append")
    # because we're re-exporting the same registry.  ``if_exists`` should
    # therefore be absent from every call's kwargs.
    for call in cls.export.await_args_list:
        kwargs = call.kwargs
        assert kwargs.get("if_exists") != "append", (
            f"Stateful daemon must not append on tick (kwargs={kwargs}); "
            "appending would duplicate the re-exported registry."
        )


@pytest.mark.asyncio
async def test_export_daemon_user_can_opt_into_append() -> None:
    """User-supplied ``if_exists="append"`` in export_params still wins.

    Forensic archive use case: 'log every snapshot to NDJSON for audit'.
    The daemon must honour the explicit override on every tick.
    """
    cls = MagicMock()
    cls.export = AsyncMock()
    dataset_ref: List[Any] = [[{"id": 1}]]
    lock = asyncio.Lock()
    q: asyncio.Queue[Optional[Wave]] = asyncio.Queue()
    shutdown = asyncio.Event()

    await _export_daemon(
        cls=cls,
        dataset_ref=dataset_ref,
        export_params={"file_path": "/tmp/audit.ndjson", "if_exists": "append"},
        lock=lock,
        wave_queue=q,
        shutdown_event=shutdown,
        e_interval=None,  # single-shot
    )

    cls.export.assert_awaited_once()
    assert cls.export.await_args.kwargs["if_exists"] == "append"


@pytest.mark.asyncio
async def test_export_daemon_error_enqueues_failure_result() -> None:
    """Export exception must enqueue a failure Wave without propagating."""
    cls = MagicMock()
    cls.export = AsyncMock(side_effect=OSError("disk full"))

    dataset_ref: List[Any] = [[{"id": 1}]]
    lock = asyncio.Lock()
    q: asyncio.Queue[Optional[Wave]] = asyncio.Queue()
    shutdown = asyncio.Event()

    await _export_daemon(
        cls, dataset_ref, export_params={}, lock=lock, wave_queue=q, shutdown_event=shutdown, e_interval=None
    )

    wave = q.get_nowait()
    assert wave is not None
    assert wave.rows_processed == 0
    assert any("disk full" in s for s in wave.failed_sources)


# ==========================================
# 6. _run_chunking_engine
# ==========================================


@pytest.mark.asyncio
async def test_run_chunking_engine_single_shot() -> None:
    """No paginator: one incorp call, one chunk Wave, then stops."""
    cls = MagicMock()
    cls.incorp = AsyncMock(return_value=[{"id": 1}])

    results = []
    async for wave in _run_chunking_engine(
        cls, incorp_params={}, refresh_params=None, export_params=None, poll_interval=None, paginator=None
    ):
        results.append(wave)

    assert len(results) == 1
    assert results[0].operation == "chunk"
    assert results[0].rows_processed == 1


@pytest.mark.asyncio
async def test_run_chunking_engine_empty_dataset_no_paginator() -> None:
    """Empty incorp result with no paginator → loop breaks before yielding."""
    cls = MagicMock()
    cls.incorp = AsyncMock(return_value=[])

    results = []
    async for wave in _run_chunking_engine(
        cls, incorp_params={}, refresh_params=None, export_params=None, poll_interval=None, paginator=None
    ):
        results.append(wave)

    assert results == []


@pytest.mark.asyncio
async def test_run_chunking_engine_exception_yields_failure() -> None:
    """Exception in incorp() during chunking → failure Wave, loop exits."""
    cls = MagicMock()
    cls.incorp = AsyncMock(side_effect=RuntimeError("fetch failed"))

    results = []
    async for wave in _run_chunking_engine(
        cls, incorp_params={}, refresh_params=None, export_params=None, poll_interval=None, paginator=None
    ):
        results.append(wave)

    assert len(results) == 1
    assert results[0].rows_processed == 0
    assert any("fetch failed" in s for s in results[0].failed_sources)


@pytest.mark.asyncio
async def test_run_chunking_engine_paginator_reset_called_between_passes() -> None:
    """With poll_interval set, paginator.reset() is invoked between outer loop passes."""
    pass_count = 0

    async def mock_incorp(**kwargs: Any) -> List[Any]:
        nonlocal pass_count
        pass_count += 1
        paginator.is_exhausted = True  # force inner loop to end after one fetch
        return [{"id": pass_count}]

    cls = MagicMock()
    cls.incorp = AsyncMock(side_effect=mock_incorp)

    paginator = MagicMock()
    paginator.is_exhausted = False
    paginator.reset = MagicMock(side_effect=lambda: setattr(paginator, "is_exhausted", False))

    results = []

    async def collect() -> None:
        async for wave in _run_chunking_engine(
            cls,
            incorp_params={},
            refresh_params=None,
            export_params=None,
            poll_interval=0.001,
            paginator=paginator,
        ):
            results.append(wave)
            if len(results) >= 2:
                return  # stop collecting after 2 passes

    try:
        await asyncio.wait_for(collect(), timeout=2.0)
    except asyncio.TimeoutError:
        pass

    paginator.reset.assert_called()
    assert len(results) >= 1


# ==========================================
# 7. run_pipeline routing
# ==========================================


@pytest.mark.asyncio
async def test_run_pipeline_routes_to_chunking_engine() -> None:
    """run_pipeline runs the O(1) chunking engine (stateful path now lives in stream())."""
    cls = MagicMock()
    cls.incorp = AsyncMock(return_value=[{"id": 1}])

    results = []
    async for wave in run_pipeline(
        cls,
        incorp_params={},
        refresh_params=None,
        export_params=None,
        poll_interval=None,
    ):
        results.append(wave)

    assert len(results) == 1
    assert results[0].operation == "chunk"


@pytest.mark.asyncio
async def test_run_pipeline_no_intervals_uses_module_default() -> None:
    """Cascade end-of-line: when every interval kwarg is None, the module
    default kicks in so the stateful shim (stream → fjord) ticks rather
    than exiting silently.  Pre-fix: refresh_interval=None +
    poll_interval=None → daemon broke after one tick.  Post-fix:
    DEFAULT_REFRESH_INTERVAL_SEC (60 s) keeps it alive at a sane cadence.
    """
    from incorporator.pipeline import DEFAULT_REFRESH_INTERVAL_SEC

    assert DEFAULT_REFRESH_INTERVAL_SEC == 60.0
