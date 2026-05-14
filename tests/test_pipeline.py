"""Unit tests for the pipeline orchestration engine.

Covers all internal helpers and both execution engines directly so
coverage of pipeline.py rises from 47 % to ~90 %.
"""

import asyncio
from typing import Any, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from incorporator.observability.logger import Wave
from incorporator.observability.pipeline import (
    _enrich_and_load,
    _export_daemon,
    _interruptible_sleep,
    _refresh_daemon,
    _row_count,
    _run_chunking_engine,
    _run_stateful_engine,
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

    await _enrich_and_load(cls, dataset, refresh_params={"new_url": "https://x"}, export_params=None, force_append=False)

    cls.refresh.assert_awaited_once_with(instance=dataset, new_url="https://x")


@pytest.mark.asyncio
async def test_enrich_and_load_export_force_append() -> None:
    """force_append=True must inject if_exists='append' into a copy of export_params."""
    cls = MagicMock()
    cls.export = AsyncMock()
    dataset: List[Any] = [{"id": 1}]
    original_params = {"file_path": "/tmp/out.json"}

    await _enrich_and_load(cls, dataset, refresh_params=None, export_params=original_params, force_append=True)

    cls.export.assert_awaited_once_with(instance=dataset, file_path="/tmp/out.json", if_exists="append")
    # Original dict must NOT be mutated
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

    await _refresh_daemon(cls, dataset_ref, refresh_params={}, lock=lock, wave_queue=q, shutdown_event=shutdown, r_interval=None)

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

    await _refresh_daemon(cls, dataset_ref, refresh_params={}, lock=lock, wave_queue=q, shutdown_event=shutdown, r_interval=None)

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
        cls, dataset_ref, export_params={"file_path": "/tmp/x"}, lock=lock, wave_queue=q, shutdown_event=shutdown, e_interval=None
    )

    cls.export.assert_awaited_once()
    wave = q.get_nowait()
    assert wave is not None
    assert wave.operation == "export"
    assert wave.rows_processed == 2


@pytest.mark.asyncio
async def test_export_daemon_error_enqueues_failure_result() -> None:
    """Export exception must enqueue a failure Wave without propagating."""
    cls = MagicMock()
    cls.export = AsyncMock(side_effect=OSError("disk full"))

    dataset_ref: List[Any] = [[{"id": 1}]]
    lock = asyncio.Lock()
    q: asyncio.Queue[Optional[Wave]] = asyncio.Queue()
    shutdown = asyncio.Event()

    await _export_daemon(cls, dataset_ref, export_params={}, lock=lock, wave_queue=q, shutdown_event=shutdown, e_interval=None)

    wave = q.get_nowait()
    assert wave is not None
    assert wave.rows_processed == 0
    assert any("disk full" in s for s in wave.failed_sources)


# ==========================================
# 6. _run_stateful_engine
# ==========================================


@pytest.mark.asyncio
async def test_run_stateful_engine_empty_dataset_exits_early() -> None:
    """incorp() returning empty yields one error Wave then stops."""
    cls = MagicMock()
    cls.incorp = AsyncMock(return_value=[])

    results = []
    async for wave in _run_stateful_engine(
        cls, incorp_params={}, refresh_params=None, export_params=None, r_interval=None, e_interval=None
    ):
        results.append(wave)

    assert len(results) == 1
    assert results[0].rows_processed == 0
    assert results[0].failed_sources


@pytest.mark.asyncio
async def test_run_stateful_engine_no_daemons_emits_incorp_result() -> None:
    """No refresh/export params → emit one incorp Wave and exit cleanly."""
    cls = MagicMock()
    cls.incorp = AsyncMock(return_value=[{"id": 1}, {"id": 2}])

    results = []
    async for wave in _run_stateful_engine(
        cls, incorp_params={}, refresh_params=None, export_params=None, r_interval=None, e_interval=None
    ):
        results.append(wave)

    assert len(results) == 1
    assert results[0].operation == "incorp"
    assert results[0].rows_processed == 2


@pytest.mark.asyncio
async def test_run_stateful_engine_with_refresh_daemon() -> None:
    """refresh_params spawns the refresh daemon; its Wave is yielded."""
    cls = MagicMock()
    cls.incorp = AsyncMock(return_value=[{"id": 1}])
    cls.refresh = AsyncMock(return_value=[{"id": 99}])

    results = []
    async for wave in _run_stateful_engine(
        cls,
        incorp_params={},
        refresh_params={"new_url": "https://x"},
        export_params=None,
        r_interval=None,  # daemon runs once then exits
        e_interval=None,
    ):
        results.append(wave)

    assert any(a.operation == "refresh" for a in results)


@pytest.mark.asyncio
async def test_run_stateful_engine_with_export_daemon() -> None:
    """export_params spawns the export daemon; its Wave is yielded."""
    cls = MagicMock()
    cls.incorp = AsyncMock(return_value=[{"id": 1}])
    cls.export = AsyncMock()

    results = []
    async for wave in _run_stateful_engine(
        cls,
        incorp_params={},
        refresh_params=None,
        export_params={"file_path": "/tmp/out.json"},
        r_interval=None,
        e_interval=None,  # daemon runs once then exits
    ):
        results.append(wave)

    assert any(a.operation == "export" for a in results)
    cls.export.assert_awaited_once()


# ==========================================
# 7. _run_chunking_engine
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
# 8. run_pipeline routing
# ==========================================


@pytest.mark.asyncio
async def test_run_pipeline_routes_to_chunking_engine() -> None:
    """stateful_polling=False routes through the O(1) chunking engine."""
    cls = MagicMock()
    cls.incorp = AsyncMock(return_value=[{"id": 1}])

    results = []
    async for wave in run_pipeline(
        cls,
        incorp_params={},
        refresh_params=None,
        export_params=None,
        poll_interval=None,
        stateful_polling=False,
    ):
        results.append(wave)

    assert len(results) == 1
    assert results[0].operation == "chunk"


@pytest.mark.asyncio
async def test_run_pipeline_routes_to_stateful_engine() -> None:
    """stateful_polling=True routes through the stateful polling engine."""
    cls = MagicMock()
    cls.incorp = AsyncMock(return_value=[])  # empty → early exit path

    results = []
    async for wave in run_pipeline(
        cls,
        incorp_params={},
        refresh_params=None,
        export_params=None,
        poll_interval=None,
        stateful_polling=True,
    ):
        results.append(wave)

    assert len(results) == 1
    assert results[0].rows_processed == 0  # empty-dataset early-exit Wave


@pytest.mark.asyncio
async def test_run_pipeline_refresh_interval_falls_back_to_poll_interval() -> None:
    """refresh_interval=None falls back to poll_interval in stateful engine."""
    cls = MagicMock()
    cls.incorp = AsyncMock(return_value=[{"id": 1}])
    cls.refresh = AsyncMock(return_value=[{"id": 2}])

    results = []
    async for wave in run_pipeline(
        cls,
        incorp_params={},
        refresh_params={"new_url": "https://x"},
        export_params=None,
        poll_interval=None,
        stateful_polling=True,
        refresh_interval=None,  # falls back to poll_interval (also None → daemon runs once)
        export_interval=None,
    ):
        results.append(wave)

    assert any(a.operation == "refresh" for a in results)
