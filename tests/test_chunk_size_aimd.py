"""Tests for AIMD chunk_size adaptation in the chunked streaming engine."""

from __future__ import annotations

import logging
from typing import Any, AsyncGenerator, List, Optional
from unittest.mock import AsyncMock, patch

import pytest

from incorporator.pipeline.chunked import _run_chunking_engine
from incorporator.tideweaver.architect import _tune_chunk_size
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
        patch("incorporator.pipeline.chunked.time.perf_counter", side_effect=_fake_perf)
        if processing_time_override is not None
        else __import__("contextlib").nullcontext()
    )

    with ctx:
        with patch("incorporator.pipeline.chunked._enrich_and_load", new=AsyncMock(side_effect=_noop_enrich)):
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
    """When the paginator has no chunk_size attribute, AIMD must disable cleanly with no exception.

    No wall-clock ``asyncio.wait_for`` bound here: this test proves clean AIMD
    disablement, not timing, and a tight timeout flakes under heavy parallel
    CPU contention (observed failing once at ~5x fast-tier load; stable
    12/12 as a unit and 3/3 isolated).
    """
    paginator = _MockPaginatorNoChunkSize(num_chunks=6)

    # Should complete without AttributeError or any other exception.
    waves: List[Wave] = []

    async def _drive() -> None:
        with patch(
            "incorporator.pipeline.chunked._enrich_and_load",
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

    await _drive()

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


@pytest.mark.asyncio
async def test_aimd_single_adjustment_per_regime(monkeypatch: pytest.MonkeyPatch) -> None:
    """A sustained slow regime must halve chunk_size exactly once, not cascade.

    Without clearing the ring after an adjustment, the median stays
    dominated by stale pre-adjustment samples for ~2 more decision cycles,
    producing repeated halvings (e.g. 1000 -> 500 -> 250) from a single
    slow regime. Feeding a constant slow processing_time_override across
    10 waves (5 pre-adjustment + 5 post-adjustment) must halve exactly
    once at wave 5, then hold at initial // 2 through wave 10 — the ring
    only re-evaluates once a fresh set of 5 post-clear samples has
    accumulated.
    """
    paginator = _MockPaginator(num_chunks=20, chunk_size=1000)
    initial_cs = paginator.chunk_size

    waves = await _collect_waves(
        paginator,
        num_chunks=10,
        adapt_chunk_size=True,
        chunk_size_min=100,
        chunk_size_max=100_000,
        target_min_sec=0.030,
        target_max_sec=0.100,
        processing_time_override=0.500,
    )

    assert len(waves) == 10
    assert paginator.chunk_size == max(100, initial_cs // 2), (
        "A single sustained-slow regime must halve exactly once, not cascade to initial // 4"
    )


@pytest.mark.asyncio
async def test_aimd_regime_change_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    """A decrease decision after a fast->slow regime flip must use only post-flip samples.

    Feeds 5 fast waves (triggers growth, visible once wave 6 is generated),
    then 5 slow waves (triggers shrink, visible once wave 11 is generated).
    An adjustment computed from wave N's sample is only applied to
    ``paginator.chunk_size`` starting with wave N+1, so 11 waves must be
    collected to observe the effect of the 10th (5th post-clear slow)
    sample. Because the ring is cleared after the growth decision, the
    5 post-flip slow samples are the only samples driving the shrink
    decision, so the slow regime is correctly detected and the chunk_size
    shrinks from its grown value — not left inflated by stale fast
    samples diluting the slow median.
    """
    paginator = _MockPaginator(num_chunks=20, chunk_size=1000)
    initial_cs = paginator.chunk_size

    async def _noop_enrich(*args: Any, **kwargs: Any) -> None:
        pass

    chunk_counter = [0]
    # First 5 waves fast (0.001s override), next 5 waves slow (0.500s override).
    # 4 perf_counter() calls occur per wave (start_time, conv_start, conv_elapsed,
    # processing_time_sec), so wave boundaries fall every 4 calls.
    fast_override = 0.001
    slow_override = 0.500
    real_perf_counter = __import__("time").perf_counter

    def _fake_perf(original=real_perf_counter) -> float:
        chunk_counter[0] += 1
        wave_number = (chunk_counter[0] - 1) // 4 + 1
        override = fast_override if wave_number <= 5 else slow_override
        return chunk_counter[0] * (override / 2.0)

    waves: List[Wave] = []
    with patch("incorporator.pipeline.chunked.time.perf_counter", side_effect=_fake_perf):
        with patch("incorporator.pipeline.chunked._enrich_and_load", new=AsyncMock(side_effect=_noop_enrich)):
            gen: AsyncGenerator[Wave, None] = _run_chunking_engine(
                cls=_MockCls,
                incorp_params={},
                refresh_params=None,
                export_params=None,
                poll_interval=None,
                paginator=paginator,
                adapt_chunk_size=True,
                chunk_size_min=100,
                chunk_size_max=100_000,
                target_min_sec=0.030,
                target_max_sec=0.100,
            )
            async for wave in gen:
                waves.append(wave)
                paginator._remaining -= 1
                if paginator._remaining <= 0:
                    paginator.is_exhausted = True
                if len(waves) >= 11:
                    break

    assert len(waves) == 11
    grown_cs = min(100_000, initial_cs + initial_cs // 5)
    assert paginator.chunk_size == max(100, grown_cs // 2), (
        "After the fast->slow regime flip, the decrease must halve the grown chunk_size using "
        "only the post-flip slow samples, not a blend with stale fast samples"
    )


@pytest.mark.asyncio
async def test_aimd_growth_floor_progresses_below_five(monkeypatch: pytest.MonkeyPatch) -> None:
    """D6-07: growth must make progress even when current_cs < 5, not freeze forever.

    ``current_cs // 5`` is 0 for any ``current_cs < 5``, so the pre-fix growth
    step ``current_cs + current_cs // 5`` is a no-op and the tuner freezes at a
    tiny size forever even when every window says "grow" — reachable whenever
    a user configures ``chunk_size_min`` below 5. This test fails pre-fix
    (chunk_size stays pinned at 3 across all decisions) and passes post-fix
    (``max(1, current_cs // 5)`` guarantees +1 per decision at minimum).
    """
    paginator = _MockPaginator(num_chunks=40, chunk_size=3)
    initial_cs = paginator.chunk_size

    # 4 decisions worth of fast waves (5 waves per decision, plus 1 to observe
    # the 4th decision's effect) — enough to prove sustained progress, not a
    # single +1 blip.
    await _collect_waves(
        paginator,
        num_chunks=21,
        adapt_chunk_size=True,
        chunk_size_min=1,
        chunk_size_max=100_000,
        target_min_sec=0.030,
        target_max_sec=0.100,
        processing_time_override=0.001,
    )

    assert paginator.chunk_size > initial_cs + 1, (
        "growth must progress past a single +1 step when current_cs < 5 across multiple decisions"
    )


@pytest.mark.asyncio
async def test_aimd_low_floor_warning_emitted_once(caplog: pytest.LogCaptureFixture) -> None:
    """D6-07 (optional observability): chunk_size_min < 5 emits one WARNING, not per-tick spam."""
    import incorporator.pipeline.chunked as chunked_mod

    chunked_mod._AIMD_LOW_FLOOR_WARNED = False
    paginator = _MockPaginator(num_chunks=20, chunk_size=3)

    with caplog.at_level(logging.WARNING, logger="incorporator.pipeline.chunked"):
        await _collect_waves(
            paginator,
            num_chunks=12,
            adapt_chunk_size=True,
            chunk_size_min=1,
            chunk_size_max=100_000,
            target_min_sec=0.030,
            target_max_sec=0.100,
            processing_time_override=0.001,
        )

    low_floor_records = [r for r in caplog.records if "chunk_size_min" in r.getMessage()]
    assert len(low_floor_records) == 1, "the low-floor WARNING must fire exactly once, not per decision"
    assert not getattr(low_floor_records[0], "is_api", False), (
        "AIMD tuner diagnostics are logic/degradation signals — must never carry is_api=True"
    )


@pytest.mark.asyncio
async def test_aimd_bimodal_signal_parks_instead_of_limit_cycling(monkeypatch: pytest.MonkeyPatch) -> None:
    """D6-06: a bimodal, size-independent signal must PARK, not limit-cycle forever.

    Alternates ``processing_time_override`` between a value below
    ``target_min_sec`` and a value above ``target_max_sec`` every wave,
    independent of ``current_cs`` (simulating e.g. alternating cache
    hit/miss latency). Pre-fix, the asymmetric grow/shrink steps
    deterministically ratchet chunk_size down to chunk_size_min and then
    limit-cycle between it and one grow-step above indefinitely, mutating
    ``paginator.chunk_size`` on every single decision. Post-fix, the tuner
    must detect the repeated direction reversal, park at a stable size, and
    stop mutating — bounding the number of chunk_size changes.
    """
    paginator = _MockPaginator(num_chunks=200, chunk_size=1000)

    async def _noop_enrich(*args: Any, **kwargs: Any) -> None:
        pass

    chunk_counter = [0]
    fast_override = 0.001
    slow_override = 0.500
    real_perf_counter = __import__("time").perf_counter

    def _fake_perf(original=real_perf_counter) -> float:
        # 4 perf_counter() calls per wave (see test_aimd_regime_change_isolation);
        # alternate the signal every wave, independent of chunk_size.
        chunk_counter[0] += 1
        wave_number = (chunk_counter[0] - 1) // 4 + 1
        override = fast_override if wave_number % 2 == 1 else slow_override
        return chunk_counter[0] * (override / 2.0)

    waves: List[Wave] = []
    chunk_size_history: List[int] = []
    with patch("incorporator.pipeline.chunked.time.perf_counter", side_effect=_fake_perf):
        with patch("incorporator.pipeline.chunked._enrich_and_load", new=AsyncMock(side_effect=_noop_enrich)):
            gen: AsyncGenerator[Wave, None] = _run_chunking_engine(
                cls=_MockCls,
                incorp_params={},
                refresh_params=None,
                export_params=None,
                poll_interval=None,
                paginator=paginator,
                adapt_chunk_size=True,
                chunk_size_min=100,
                chunk_size_max=100_000,
                target_min_sec=0.030,
                target_max_sec=0.100,
            )
            async for wave in gen:
                waves.append(wave)
                chunk_size_history.append(paginator.chunk_size)
                paginator._remaining -= 1
                if paginator._remaining <= 0:
                    paginator.is_exhausted = True
                if len(waves) >= 120:
                    break

    mutation_count = sum(1 for a, b in zip(chunk_size_history, chunk_size_history[1:]) if a != b)
    assert mutation_count <= 4, (
        f"bimodal size-independent signal must park after a bounded number of reversals, "
        f"got {mutation_count} mutations across {len(waves)} waves"
    )
    # Once parked, the size must hold steady for the remainder of the drain.
    assert chunk_size_history[-1] == chunk_size_history[-20], (
        "tuner must have settled at a stable size well before the end of the drain"
    )


@pytest.mark.asyncio
async def test_aimd_bimodal_signal_emits_park_warning_not_api_log(caplog: pytest.LogCaptureFixture) -> None:
    """D6-06: parking on a bimodal signal must emit a WARNING via the module logger, not <cls>_api.log.

    Asserts the WARNING fires from ``incorporator.pipeline.chunked`` (a plain
    module logger with no ``is_api`` extra) — the logging-contract HARD rule
    that logic/degradation signals never land on the class-named
    ``<cls>_api.log`` surface, which is reserved for URL/httpx traffic.
    """
    import incorporator.pipeline.chunked as chunked_mod

    chunked_mod._AIMD_PARKED_WARNED.clear()
    paginator = _MockPaginator(num_chunks=200, chunk_size=1000)

    async def _noop_enrich(*args: Any, **kwargs: Any) -> None:
        pass

    chunk_counter = [0]
    fast_override = 0.001
    slow_override = 0.500
    real_perf_counter = __import__("time").perf_counter

    def _fake_perf(original=real_perf_counter) -> float:
        chunk_counter[0] += 1
        wave_number = (chunk_counter[0] - 1) // 4 + 1
        override = fast_override if wave_number % 2 == 1 else slow_override
        return chunk_counter[0] * (override / 2.0)

    waves: List[Wave] = []
    with caplog.at_level(logging.WARNING, logger="incorporator.pipeline.chunked"):
        with patch("incorporator.pipeline.chunked.time.perf_counter", side_effect=_fake_perf):
            with patch("incorporator.pipeline.chunked._enrich_and_load", new=AsyncMock(side_effect=_noop_enrich)):
                gen = _run_chunking_engine(
                    cls=_MockCls,
                    incorp_params={},
                    refresh_params=None,
                    export_params=None,
                    poll_interval=None,
                    paginator=paginator,
                    adapt_chunk_size=True,
                    chunk_size_min=100,
                    chunk_size_max=100_000,
                    target_min_sec=0.030,
                    target_max_sec=0.100,
                )
                async for wave in gen:
                    waves.append(wave)
                    paginator._remaining -= 1
                    if paginator._remaining <= 0:
                        paginator.is_exhausted = True
                    if len(waves) >= 120:
                        break

    park_records = [r for r in caplog.records if "parking at chunk_size" in r.getMessage()]
    assert len(park_records) == 1, "the park WARNING must fire exactly once, not per subsequent decision"
    assert park_records[0].levelno == logging.WARNING
    assert park_records[0].name == "incorporator.pipeline.chunked"
    assert not getattr(park_records[0], "is_api", False), (
        "the park/give-up signal is tuner logic, not URL traffic — must never carry is_api=True "
        "(that routing is exclusive to APIFilter-backed <cls>_api.log handlers)"
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
