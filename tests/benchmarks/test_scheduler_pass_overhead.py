"""Benchmark: Tideweaver scheduler-pass overhead under zero-work currents.

Measures how many ``Tide`` records the scheduler emits during a fixed
window when every current's tick body is a no-op.  Catches regressions
in the ``_run_pass`` loop, ``_gate_reason`` cost, asyncio task-creation,
and the adaptive heap-driven wakeup (commit ``ad9041f``).

The scheduler's ``pass_interval`` clamps to a 0.05s minimum, so the
theoretical ceiling is ~20 passes/sec.  We run a 2-second window with
N parallel currents (no edges → no gate evaluations beyond the initial
empty upstream list) at ``interval=0.05`` so every current is always due.
The benchmark measures pure scheduler overhead — every observed deficit
from the 20 passes/sec ceiling is loop bookkeeping + Tide emission cost.

Two scenarios:

* **Light load (N=5):** baseline overhead with a small fan-out.
* **Heavier load (N=50):** scales the per-pass work (5+50 ≈ 10× the
  per-current bookkeeping).  Catches super-linear regressions in
  ``_spawn_tick`` or ``_tick_wrapper`` setup.

Floors picked from local calibration (slowest of 3 runs × 0.7), per
the standard convention in ``tests/benchmarks/``.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import pytest

from incorporator import Incorporator
from incorporator.tideweaver import (
    Current,
    Stream,
    Tideweaver,
    Watershed,
)


class _BenchTarget(Incorporator):
    """Minimal Incorporator subclass — never actually invoked thanks to ``tick_factory``."""


def _make_bench_targets(n: int) -> List[type]:
    """Build ``n`` distinct Incorporator subclasses so Watershed's cls.__name__ collision
    validator doesn't reject N parallel currents that would otherwise all share ``_BenchTarget``.
    """
    return [type(f"_BenchTarget{i}", (Incorporator,), {}) for i in range(n)]


def _window(seconds: float) -> Tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(seconds=seconds))


async def _noop_tick(current: Current) -> None:
    """Zero-work tick body — measures pure scheduler overhead."""


WINDOW_SECONDS = 2.0

# Floors calibrated from local first-run measurements (slowest of 3 × 0.7,
# rounded down to a friendly number).  Measured ~33 passes/sec on
# commodity hardware (well above the ``pass_interval=0.05`` floor of 20/s
# — the adaptive heap-driven wakeup at commit ``ad9041f`` lets passes
# fire as currents come due, not just on the pass_interval cadence).
#
# Notable measurement: N=5 and N=50 run within 1% of each other.  The
# per-pass cost is dominated by the Tide emit + loop bookkeeping, not
# per-current iteration — both floors land at the same value.
LIGHT_FLOOR_PASSES_PER_SEC = 20.0  # N=5 — slowest of 3 × 0.7 = 22.96 → floor 20
HEAVY_FLOOR_PASSES_PER_SEC = 20.0  # N=50 — same as N=5; per-current cost is sub-1%


async def _run_and_count(n_currents: int) -> Tuple[int, float]:
    """Spin a Tideweaver with N zero-work currents; return (tide_count, elapsed_seconds)."""
    targets = _make_bench_targets(n_currents)
    streams: List[Stream] = [
        Stream(name=f"c{i}", cls=targets[i], interval=0.05, incorp_params={}) for i in range(n_currents)
    ]
    ws = Watershed.parallel(currents=streams, window=_window(WINDOW_SECONDS))
    tw = Tideweaver(ws, tick_factory=_noop_tick, pass_interval=0.05)

    t0 = time.perf_counter()
    tide_count = 0
    async for _tide in tw.run():
        tide_count += 1
    elapsed = time.perf_counter() - t0
    return tide_count, elapsed


@pytest.mark.asyncio
async def test_scheduler_pass_overhead_light() -> None:
    """5 parallel zero-work currents → ≥ LIGHT_FLOOR passes/sec.

    Light load establishes the baseline overhead of one scheduler pass.
    The theoretical ceiling at ``pass_interval=0.05`` is 20 passes/sec;
    the floor leaves headroom for the per-pass scheduler bookkeeping
    (Tide construction, ``_run_pass`` loop, due-heap maintenance).
    """
    tide_count, elapsed = await _run_and_count(n_currents=5)
    passes_per_sec = tide_count / elapsed
    print(f"\n  Scheduler pass overhead (N=5):  {tide_count} Tides in {elapsed:.2f}s = {passes_per_sec:.1f} passes/sec")
    assert passes_per_sec >= LIGHT_FLOOR_PASSES_PER_SEC, (
        f"Scheduler dropped to {passes_per_sec:.1f} passes/sec under 5 "
        "zero-work currents (floor: "
        f"{LIGHT_FLOOR_PASSES_PER_SEC}).  Suggests regression in the "
        "_run_pass loop, _gate_reason cost, or Tide-record construction."
    )


@pytest.mark.asyncio
async def test_scheduler_pass_overhead_heavy() -> None:
    """50 parallel zero-work currents → ≥ HEAVY_FLOOR passes/sec.

    Catches super-linear regressions: with N=50 each pass spawns 50
    asyncio tasks, walks the upstream/downstream maps 50 times, and
    runs the gate decision (with an empty upstream list each).  A
    significant drop between the N=5 and N=50 measurements points to
    per-current overhead growing faster than expected.
    """
    tide_count, elapsed = await _run_and_count(n_currents=50)
    passes_per_sec = tide_count / elapsed
    print(f"\n  Scheduler pass overhead (N=50): {tide_count} Tides in {elapsed:.2f}s = {passes_per_sec:.1f} passes/sec")
    assert passes_per_sec >= HEAVY_FLOOR_PASSES_PER_SEC, (
        f"Scheduler dropped to {passes_per_sec:.1f} passes/sec under 50 "
        f"zero-work currents (floor: {HEAVY_FLOOR_PASSES_PER_SEC}).  "
        "Suggests _spawn_tick or _tick_wrapper grew super-linear "
        "per-current cost."
    )
