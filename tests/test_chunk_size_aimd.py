"""Tests for AIMD chunk_size adaptation in the chunked streaming engine."""

from __future__ import annotations

import asyncio
from typing import Any, AsyncGenerator, List, Optional
from unittest.mock import AsyncMock, patch

import pytest

from incorporator.observability.pipeline.chunked import _run_chunking_engine
from incorporator.observability.tideweaver.architect import _tune_chunk_size
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
    _last_bytes_downloaded: Optional[int] = None
    _last_http_fetch_time_sec: Optional[float] = None
    _last_schema_cache_hit: bool = True
    _last_http_retry_count: int = 0

    @classmethod
    async def incorp(cls, **kwargs: Any) -> List[dict]:
        """Return a one-row dataset so rows > 0 and the engine doesn't short-circuit."""
        return [{"id": 1}]


class _MockNetworkCls:
    """Class-like object that reports a high HTTP fetch time — simulates a network-bound source.

    ``_last_http_fetch_time_sec=0.050`` (50 ms) means the chunked engine builds
    ``wave_obj.http_fetch_time_sec=0.050``, so the AIMD parse-only remainder is
    ``max(0, processing_time_sec - 0.050)``.
    """

    inc_url: Optional[str] = "https://example.com/api"
    inc_file: Optional[str] = None
    _last_bytes_processed: Optional[int] = None
    _last_bytes_downloaded: Optional[int] = None
    _last_http_fetch_time_sec: Optional[float] = 0.050
    _last_schema_cache_hit: bool = True
    _last_http_retry_count: int = 0

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
    mock_cls: Any = None,
) -> List[Wave]:
    """Drive _run_chunking_engine, collecting *num_chunks* Wave objects then stopping.

    Patches ``_enrich_and_load`` to a no-op and optionally overrides
    ``time.perf_counter`` to produce a deterministic ``processing_time_sec``.
    Pass ``mock_cls`` to substitute a different class double (e.g.
    ``_MockNetworkCls``); defaults to ``_MockCls``.
    """
    cls_under_test = mock_cls if mock_cls is not None else _MockCls
    waves: List[Wave] = []

    async def _noop_enrich(*args: Any, **kwargs: Any) -> None:
        pass

    chunk_counter = [0]
    real_perf_counter = __import__("time").perf_counter

    def _fake_perf(original=real_perf_counter) -> float:
        # Each call increments by processing_time_override/2. The per-chunk
        # processing_time_sec spans several bracketed perf_counter calls, so the
        # effective end-to-end time is a fixed multiple of the override
        # (~1.5x — e.g. override=0.052 -> ~0.078 s), not exactly the override.
        # Tests assert on the resulting band (grow / no-change / shrink), not the
        # exact value, so the multiple is immaterial to correctness.
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
        with patch(
            "incorporator.observability.pipeline.chunked._enrich_and_load", new=AsyncMock(side_effect=_noop_enrich)
        ):
            gen: AsyncGenerator[Wave, None] = _run_chunking_engine(
                cls=cls_under_test,
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

    async def _drive() -> None:
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

    await asyncio.wait_for(_drive(), timeout=5.0)

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


@pytest.mark.asyncio
async def test_aimd_network_bound_no_shrink(monkeypatch: pytest.MonkeyPatch) -> None:
    """Network-bound sources must NOT trigger AIMD shrinkage when parse is fast.

    _MockNetworkCls reports http_fetch_time_sec=0.050 (50 ms HTTP). The AIMD ring
    receives the parse-only remainder ``max(0, processing_time_sec - 0.050)``.
    With the fake perf clock, ``processing_time_override=0.052`` drives the
    bracketed perf_counter calls to an effective end-to-end processing_time_sec of
    ~0.078 s, so the parse-only remainder is ~0.028 s — well inside the new target
    window [0.001, 0.100] → no shrink. Under the OLD end-to-end signal the full
    ~0.078 s exceeded the old 0.030 floor and would have shrunk the chunk.
    Uses the new default target window (target_min_sec=0.001, target_max_sec=0.100).
    """
    paginator = _MockPaginator(num_chunks=10, chunk_size=1000)
    initial_cs = paginator.chunk_size

    # _MockNetworkCls reports _last_http_fetch_time_sec=0.050; with the fake
    # perf clock the effective processing_time_sec is ~0.078 s, so the parse-only
    # remainder is ~0.028 s (inside the window).
    await _collect_waves(
        paginator,
        num_chunks=7,
        adapt_chunk_size=True,
        chunk_size_min=100,
        chunk_size_max=100_000,
        target_min_sec=0.001,
        target_max_sec=0.100,
        processing_time_override=0.052,
        mock_cls=_MockNetworkCls,
    )

    assert paginator.chunk_size == initial_cs, (
        "AIMD must not shrink chunk_size when the parse-only remainder (~28 ms) is within target window"
    )


@pytest.mark.asyncio
async def test_aimd_file_mode_still_adapts(monkeypatch: pytest.MonkeyPatch) -> None:
    """File-mode sources (http_fetch_time_sec=None) must still adapt on end-to-end signal.

    _MockCls leaves _last_http_fetch_time_sec=None, simulating a file/SQLite
    source after the P0 telemetry reset.  Feeding slow waves (0.500 s >> 0.100 s
    target_max) must still shrink chunk_size, confirming the end-to-end fallback
    branch is exercised for no-network sources.
    """
    paginator = _MockPaginator(num_chunks=10, chunk_size=1000)
    initial_cs = paginator.chunk_size

    await _collect_waves(
        paginator,
        num_chunks=7,
        adapt_chunk_size=True,
        chunk_size_min=100,
        chunk_size_max=100_000,
        target_min_sec=0.001,
        target_max_sec=0.100,
        processing_time_override=0.500,
        mock_cls=_MockCls,
    )

    assert paginator.chunk_size < initial_cs, (
        "AIMD must still shrink chunk_size for file-mode sources with slow end-to-end time"
    )


def test_aimd_network_and_offline_agree_fine() -> None:
    """Online AIMD parse signal and offline _tune_chunk_size must agree on a network-bound case.

    Constructs 25 synthetic Wave records with http_fetch_time_sec=0.050 and
    processing_time_sec=0.052 (parse-only = 2 ms).  The offline tuner must emit
    severity='info' (well-tuned).  The AIMD ring signal for each wave must be
    max(0, 0.052 - 0.050) = 0.002 s, which lies in [0.001, 0.100] → no shrink.
    """
    from datetime import datetime, timezone

    waves = [
        Wave.model_construct(
            chunk_index=i + 1,
            operation="chunk",
            rows_processed=100,
            failed_sources=[],
            processing_time_sec=0.052,
            source_url="https://example.com/api",
            bytes_processed=102400,
            bytes_downloaded=102400,
            http_fetch_time_sec=0.050,
            http_retry_count=0,
            validation_error_count=0,
            schema_cache_hit=True,
            conv_dict_time_sec=0.001,
            timestamp=datetime.now(timezone.utc),
        )
        for i in range(25)
    ]

    hints = _tune_chunk_size(waves)
    assert hints, "Expected at least one hint from _tune_chunk_size"
    chunk_hint = next((h for h in hints if h.knob == "chunk_size"), None)
    assert chunk_hint is not None
    assert chunk_hint.severity == "info", (
        f"Offline tuner should report 'info' (well-tuned) for 2 ms parse; got {chunk_hint.severity!r}: "
        f"{chunk_hint.signal}"
    )

    # Confirm the AIMD parse-only signal for these same waves sits in [0.001, 0.100].
    target_min_sec = 0.001
    target_max_sec = 0.100
    for w in waves:
        assert w.http_fetch_time_sec is not None
        parse_signal = max(0.0, w.processing_time_sec - w.http_fetch_time_sec)
        assert target_min_sec <= parse_signal <= target_max_sec, (
            f"AIMD ring signal {parse_signal:.4f}s must be within [{target_min_sec}, {target_max_sec}]"
        )
