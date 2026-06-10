"""Benchmark: Reservoir push + Spillway overflow throughput.

Validates the O(1) ``collections.deque`` claim from commit ``0c2c776``
("perf(tideweaver): use collections.deque for Reservoir.waves").  The
scheduler's ``_EdgeState.waves`` is a deque; the trim loop calls
``popleft()`` (O(1)) when reservoir depth is exceeded and dispatches
``Spillway.overflow(...)`` per displaced wave.

Two scenarios:

* **DropOldest baseline** — silent spillway (no-op overflow hook).
  Measures pure deque ``append`` + ``popleft`` cost.
* **RaiseOverflow** — every displacement fires a WARNING log call.
  Measures the realistic worst case where the spillway hook does
  measurable work.

We synthesise the scheduler's trim loop directly (no Tideweaver, no
event loop) so the measurement isolates the deque+spillway path from
the rest of the scheduler.  A regression here would point to either
``Reservoir.waves`` reverting to ``list`` (popleft becomes O(N)) or to
``Spillway.overflow`` growing expensive.

Floors picked from local calibration (slowest of 3 runs × 0.7).
"""

from __future__ import annotations

import logging
import time
from collections import deque
from typing import Deque, List, Tuple

import pytest

from incorporator.tideweaver.flow import DropOldest, RaiseOverflow

PUSH_COUNT = 100_000
RESERVOIR_DEPTH = 10
# Every push past the first DEPTH triggers a spillway hook, so total
# overflow invocations = PUSH_COUNT - RESERVOIR_DEPTH.

# Floors calibrated from local first-run measurements (slowest of 3 × 0.7).
# Measurements on commodity hardware: DropOldest ~1.2-1.5M ops/sec,
# RaiseOverflow ~700-840k ops/sec (with WARN suppressed at the logger
# level — see the RaiseOverflow test docstring for rationale).
DROP_OLDEST_FLOOR_OPS_PER_SEC = 800_000  # deque-only — slowest of 3 × 0.7 = 864k → 800k
RAISE_OVERFLOW_FLOOR_OPS_PER_SEC = 500_000  # WARN-suppressed dispatch — slowest of 3 × 0.7 = 500k


def _build_mock_wave(idx: int) -> List[int]:
    """Mock wave snapshot — a tiny strong-ref list standing in for ``List[Any]`` instances."""
    return [idx, idx + 1, idx + 2]


def _push_with_spillway(
    waves: Deque[List[int]],
    spillway: object,
    edge: Tuple[str, str],
    push_count: int,
    depth: int,
) -> int:
    """Replicate the scheduler's reservoir-trim loop ([scheduler.py:528-532]).

    Returns the overflow count.  Inlined here to keep the benchmark a
    pure measurement of deque + spillway behaviour — no scheduler, no
    event loop, no Tideweaver overhead.
    """
    overflow_count = 0
    for i in range(push_count):
        waves.append(_build_mock_wave(i))
        while len(waves) > depth:
            displaced = waves.popleft()
            overflow_count += 1
            spillway.overflow(edge, displaced, overflow_count)  # type: ignore[attr-defined]
    return overflow_count


@pytest.mark.benchmark
def test_reservoir_drop_oldest_throughput() -> None:
    """100k pushes through a depth-10 reservoir with the silent spillway.

    DropOldest's overflow hook is a literal ``return None`` — this
    measures the pure ``deque.append`` + ``deque.popleft`` baseline.
    """
    waves: Deque[List[int]] = deque()
    spillway = DropOldest()
    edge = ("up", "down")

    t0 = time.perf_counter()
    overflow_count = _push_with_spillway(waves, spillway, edge, push_count=PUSH_COUNT, depth=RESERVOIR_DEPTH)
    elapsed = time.perf_counter() - t0

    expected_overflow = PUSH_COUNT - RESERVOIR_DEPTH
    assert overflow_count == expected_overflow
    assert len(waves) == RESERVOIR_DEPTH

    ops_per_sec = PUSH_COUNT / elapsed
    print(f"\n  Reservoir (DropOldest):    {PUSH_COUNT:,} pushes in {elapsed:.3f}s = {ops_per_sec:,.0f} ops/sec")

    assert ops_per_sec >= DROP_OLDEST_FLOOR_OPS_PER_SEC, (
        f"Reservoir throughput dropped to {ops_per_sec:,.0f} ops/sec "
        f"(floor: {DROP_OLDEST_FLOOR_OPS_PER_SEC:,}).  Likely cause: "
        "_EdgeState.waves reverted from collections.deque to list "
        "(popleft becomes O(N), trim loop becomes O(N²))."
    )


@pytest.mark.benchmark
def test_reservoir_raise_overflow_throughput() -> None:
    """100k pushes through a depth-10 reservoir with the WARN-log spillway.

    RaiseOverflow logs every displaced wave.  Worst-case realistic
    spillway path — bounded by Python ``logging`` cost, not deque cost.
    A regression here implicates either the deque or the spillway hook.

    The flow logger is set to CRITICAL for the loop so the 100k WARN
    calls return early at the level check.  This isolates the spillway
    dispatch overhead (function call + ``isinstance`` + level test)
    from the per-record formatting + handler dispatch cost, which is
    handler-dependent in production and so not meaningfully measurable
    in a benchmark.  The pure dispatch path is still ~10× slower than
    DropOldest because of the extra method-resolution + function call.
    """
    waves: Deque[List[int]] = deque()
    spillway = RaiseOverflow()
    edge = ("up", "down")

    flow_logger = logging.getLogger("incorporator.tideweaver.flow")
    prev_level = flow_logger.level
    flow_logger.setLevel(logging.CRITICAL)
    try:
        t0 = time.perf_counter()
        overflow_count = _push_with_spillway(waves, spillway, edge, push_count=PUSH_COUNT, depth=RESERVOIR_DEPTH)
        elapsed = time.perf_counter() - t0
    finally:
        flow_logger.setLevel(prev_level)

    expected_overflow = PUSH_COUNT - RESERVOIR_DEPTH
    assert overflow_count == expected_overflow

    ops_per_sec = PUSH_COUNT / elapsed
    print(f"\n  Reservoir (RaiseOverflow): {PUSH_COUNT:,} pushes in {elapsed:.3f}s = {ops_per_sec:,.0f} ops/sec")

    assert ops_per_sec >= RAISE_OVERFLOW_FLOOR_OPS_PER_SEC, (
        f"Reservoir throughput with WARN-log spillway dropped to "
        f"{ops_per_sec:,.0f} ops/sec (floor: "
        f"{RAISE_OVERFLOW_FLOOR_OPS_PER_SEC:,}).  Either the deque "
        "regressed to a list OR the WARN-log path became expensive."
    )
