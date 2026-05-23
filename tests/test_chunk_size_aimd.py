"""Tests for AIMD chunk_size adaptation in the chunked streaming engine."""

from __future__ import annotations

import asyncio
from typing import Any, AsyncGenerator, List, Optional
from unittest.mock import AsyncMock, patch

import pytest

from incorporator.observability.pipeline.chunked import _run_chunking_engine
from incorporator.observability.wave import Wave


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _MockPaginator:
    """Minimal paginator stub exposing a ``chunk_size`` attribute for AIMD tests."""

    def __init__(self, num_chunks: int = 10, chunk_size: int = 1000) -> None:
        self.chunk_size = chunk_size
        self._remaining = num_chunks
        self.is_exhausted = False

    def reset(self) -> None:
        """No-op reset (single-pass tests don't loop)."""


class _MockPaginatorNoChunkSize:
    """Paginator stub WITHOUT a chunk_size attribute — AIMD must disable itself."""

    def __init__(self, num_chunks: int = 3) -> None:
        self._remaining = num_chunks
        self.is_exhausted = False

    def reset(self) -> None:
        """No-op reset."""


class _MockCls:
    """Minimal class-like object satisfying the fields _run_chunking_engine reads."""

    inc_url: Optional[str] = "https://example.com/api"
    inc_file: Optional[str] = None
    _last_bytes_processed: Optional[int] = None
    _last_schema_cache_hit: bool = True

    @classmethod
    async def incorp(cls, **kwargs: Any) -> List[dict]:
        """Return a one-row dataset so rows > 0 and the engine doesn't short-circuit."""
        return [{"id": 1}]


async def _collect_waves(
    paginator: Any,
    num_chunks: int,
    adapt_chunk_size: bool = False,
    chunk_size_min: int = 100,
    chunk_size_max: int = 100_000,
    target_min_sec: float = 0.030,
    target_max_sec: float = 0.100,
    processing_time_override: Optional[float] = None,
) -> List[Wave]:
    """Drive _run_chunking_engine, collecting *num_chunks* Wave objects then stopping.

    Patches ``_enrich_and_load`` to a no-op and optionally overrides
    ``time.perf_counter`` to produce a deterministic ``processing_time_sec``.
    """
    waves: List[Wave] = []

    async def _noop_enrich(*args: Any, **kwargs: Any) -> None:
        pass

    chunk_counter = [0]
    real_perf_counter = __import__("time").perf_counter

    def _fake_perf(original=real_perf_counter) -> float:
        # Each call increments by processing_time_override/2 so that
        # start_time → wave_obj uses exactly processing_time_override.
        if processing_time_override is not None:
            chunk_counter[0] += 1
            return chunk_counter[0] * (processing_time_override / 2.0)
        return original()

    ctx = (
        patch("incorporator.observability.pipeline.chunked.time.perf_counter", side_effect=_fake_perf)
        if processing_time_override is not None
        else __import__("contextlib").nullcontext()
    )

    with ctx:
        with patch("incorporator.observability.pipeline.chunked._enrich_and_load", new=AsyncMock(side_effect=_noop_enrich)):
            gen: AsyncGenerator[Wave, None] = _run_chunking_engine(
                cls=_MockCls,
                incorp_params={},
                refresh_params=None,
                export_params=None,
                poll_interval=None,
                paginator=paginator,
                adapt_chunk_size=adapt_chunk_size,
                chunk_size_min=chunk_size_min,
                chunk_size_max=chunk_size_max,
                target_min_sec=target_min_sec,
                target_max_sec=target_max_sec,
            )
            async for wave in gen:
                waves.append(wave)
                paginator._remaining -= 1
                if paginator._remaining <= 0:
                    paginator.is_exhausted = True
                if len(waves) >= num_chunks:
                    break

    return waves


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aimd_disabled_no_change(monkeypatch: pytest.MonkeyPatch) -> None:
    """adapt_chunk_size=False must leave paginator.chunk_size unchanged regardless of wave timing."""
    paginator = _MockPaginator(num_chunks=10, chunk_size=1000)
    # Force very fast processing so AIMD would grow chunk_size if enabled.
    await _collect_waves(paginator, num_chunks=8, adapt_chunk_size=False, processing_time_override=0.001)
    assert paginator.chunk_size == 1000


@pytest.mark.asyncio
async def test_aimd_fast_chunks_increase(monkeypatch: pytest.MonkeyPatch) -> None:
    """When median processing_time < target_min_sec, chunk_size must grow after 5 waves."""
    paginator = _MockPaginator(num_chunks=10, chunk_size=1000)
    initial_cs = paginator.chunk_size

    # 0.001s << target_min_sec=0.030 → should trigger growth.
    await _collect_waves(
        paginator,
        num_chunks=6,
        adapt_chunk_size=True,
        chunk_size_min=100,
        chunk_size_max=100_000,
        target_min_sec=0.030,
        target_max_sec=0.100,
        processing_time_override=0.001,
    )

    assert paginator.chunk_size > initial_cs, "chunk_size should have grown for fast chunks"


@pytest.mark.asyncio
async def test_aimd_slow_chunks_decrease(monkeypatch: pytest.MonkeyPatch) -> None:
    """When median processing_time > target_max_sec, chunk_size must halve after 5 waves."""
    paginator = _MockPaginator(num_chunks=10, chunk_size=1000)
    initial_cs = paginator.chunk_size

    # 0.500s >> target_max_sec=0.100 → should trigger shrinkage (halving).
    await _collect_waves(
        paginator,
        num_chunks=6,
        adapt_chunk_size=True,
        chunk_size_min=100,
        chunk_size_max=100_000,
        target_min_sec=0.030,
        target_max_sec=0.100,
        processing_time_override=0.500,
    )

    assert paginator.chunk_size < initial_cs, "chunk_size should have shrunk for slow chunks"
    assert paginator.chunk_size == max(100, initial_cs // 2)


@pytest.mark.asyncio
async def test_aimd_bounded_by_min_max(monkeypatch: pytest.MonkeyPatch) -> None:
    """chunk_size must not grow past chunk_size_max or shrink below chunk_size_min."""
    # Test upper bound: start close to max, feed fast waves.
    paginator_high = _MockPaginator(num_chunks=20, chunk_size=90_000)
    await _collect_waves(
        paginator_high,
        num_chunks=15,
        adapt_chunk_size=True,
        chunk_size_min=100,
        chunk_size_max=100_000,
        target_min_sec=0.030,
        target_max_sec=0.100,
        processing_time_override=0.001,
    )
    assert paginator_high.chunk_size <= 100_000

    # Test lower bound: start close to min, feed slow waves.
    paginator_low = _MockPaginator(num_chunks=20, chunk_size=150)
    await _collect_waves(
        paginator_low,
        num_chunks=15,
        adapt_chunk_size=True,
        chunk_size_min=100,
        chunk_size_max=100_000,
        target_min_sec=0.030,
        target_max_sec=0.100,
        processing_time_override=0.500,
    )
    assert paginator_low.chunk_size >= 100


@pytest.mark.asyncio
async def test_aimd_paginator_without_chunk_size_no_op(monkeypatch: pytest.MonkeyPatch) -> None:
    """When the paginator has no chunk_size attribute, AIMD must disable cleanly with no exception."""
    paginator = _MockPaginatorNoChunkSize(num_chunks=6)

    # Should complete without AttributeError or any other exception.
    waves: List[Wave] = []
    async with asyncio.timeout(5.0):
        with patch(
            "incorporator.observability.pipeline.chunked._enrich_and_load",
            new=AsyncMock(side_effect=lambda *a, **kw: None),
        ):
            gen = _run_chunking_engine(
                cls=_MockCls,
                incorp_params={},
                refresh_params=None,
                export_params=None,
                poll_interval=None,
                paginator=paginator,
                adapt_chunk_size=True,
            )
            async for wave in gen:
                waves.append(wave)
                paginator._remaining -= 1
                if paginator._remaining <= 0:
                    paginator.is_exhausted = True
                if len(waves) >= 3:
                    break

    assert len(waves) >= 1
    assert not hasattr(paginator, "chunk_size")


@pytest.mark.asyncio
async def test_aimd_ring_buffer_not_full_no_change(monkeypatch: pytest.MonkeyPatch) -> None:
    """chunk_size must be unchanged when fewer than 5 waves have been emitted.

    AIMD only fires when the ring buffer is full (maxlen=5) to prevent
    premature adjustments from noise.
    """
    paginator = _MockPaginator(num_chunks=10, chunk_size=1000)
    initial_cs = paginator.chunk_size

    # Collect only 3 waves — ring buffer not yet full.
    await _collect_waves(
        paginator,
        num_chunks=3,
        adapt_chunk_size=True,
        chunk_size_min=100,
        chunk_size_max=100_000,
        target_min_sec=0.030,
        target_max_sec=0.100,
        processing_time_override=0.001,
    )

    assert paginator.chunk_size == initial_cs


@pytest.mark.asyncio
async def test_aimd_target_window_no_change(monkeypatch: pytest.MonkeyPatch) -> None:
    """chunk_size must be stable when median processing_time is in [target_min, target_max]."""
    paginator = _MockPaginator(num_chunks=10, chunk_size=1000)
    initial_cs = paginator.chunk_size

    # 0.060s sits between target_min=0.030 and target_max=0.100 → no change.
    await _collect_waves(
        paginator,
        num_chunks=7,
        adapt_chunk_size=True,
        chunk_size_min=100,
        chunk_size_max=100_000,
        target_min_sec=0.030,
        target_max_sec=0.100,
        processing_time_override=0.060,
    )

    assert paginator.chunk_size == initial_cs
