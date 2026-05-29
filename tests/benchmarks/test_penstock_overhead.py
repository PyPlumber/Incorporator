"""Benchmark: Penstock ``consume_reason`` + ``post_consume`` per-call cost.

Measures the per-call overhead of every Penstock subclass — both the
shared base implementations (``Penstock.consume_reason`` /
``Penstock.post_consume`` at ``io/penstock.py:164-214``) and the
backpressure variant that overrides ``consume_reason`` directly
(``BackpressurePenstock.consume_reason`` at ``flow.py:197-216``).

Phase 2's A-F-9 (paginator-penstock integration) will exercise these
from per-page loops at higher call rates than the Tideweaver scheduler
ever did.  Regression here is high-impact: a 10× slowdown in
``consume_reason`` translates directly into paginator throughput loss.

Each variant is calibrated separately because the work shape differs:

* ``NullPenstock`` — single ``return None`` branch; fastest baseline.
* ``SustainedPenstock`` — one float subtract + comparison.
* ``BurstPenstock`` — float arithmetic + ``min()`` clamp + state mutation.
* ``WindowPenstock`` — list comprehension (eviction) + append per call.
* ``SignalPenstock`` — lambda call + sustained-style arithmetic.
* ``BackpressurePenstock`` — division + sustained-style arithmetic, plus
  reservoir-fullness read from the parent ``FlowControl``.

We drive each penstock through 100k ``consume_reason`` + ``post_consume``
cycles with a synthetic monotonic clock (no ``time.monotonic()`` per
call — that would add measurement noise the user never pays for in
production).  All penstocks are configured to always permit so we
measure the evaluate path's overhead, not throttle sleeping.

Floors picked from local calibration (slowest of 3 runs × 0.7).
"""

from __future__ import annotations

import time
from collections import deque
from typing import Any, Deque

import pytest

from incorporator.io.penstock import (
    BurstPenstock,
    FlowState,
    NullPenstock,
    SignalPenstock,
    SustainedPenstock,
    WindowPenstock,
)
from incorporator.observability.tideweaver.flow import (
    BackpressurePenstock,
    FlowControl,
    Reservoir,
)

CALL_COUNT = 100_000

# Floors calibrated from local first-run measurements (slowest of 3 × 0.7,
# rounded down to a friendly number).  Measurements on commodity hardware
# ranged 476k-962k ops/sec across the six variants.  Notable: with a
# steady-state ``window_log`` of 1 entry (delta=1.0s > window_sec=0.5s),
# WindowPenstock's list-comp eviction is roughly the same cost as the
# other variants — when ``window_log`` grows unbounded it would dominate.
NULL_FLOOR = 600_000  # NullPenstock — slowest of 3 × 0.7 = 605k
SUSTAINED_FLOOR = 350_000  # SustainedPenstock — slowest of 3 × 0.7 = 374k
BURST_FLOOR = 300_000  # BurstPenstock — slowest of 3 × 0.7 = 333k
WINDOW_FLOOR = 400_000  # WindowPenstock — slowest of 3 × 0.7 = 430k
SIGNAL_FLOOR = 350_000  # SignalPenstock — slowest of 3 × 0.7 = 378k
BACKPRESSURE_FLOOR = 500_000  # BackpressurePenstock — slowest of 3 × 0.7 = 512k


class _MockEdgeState:
    """Duck-typed stand-in for the scheduler's ``_EdgeState``.

    Carries a ``flow_state`` attribute (matches the i14 composition
    pattern) plus a ``waves`` deque (BackpressurePenstock reads this for
    fullness).  Constructed once per benchmark — its lifetime spans the
    whole 100k-call loop, so it accumulates state realistically.
    """

    def __init__(self) -> None:
        self.flow_state = FlowState()
        self.waves: Deque[Any] = deque()


def _drive_penstock(penstock: Any, flow: Any, call_count: int) -> float:
    """Drive ``consume_reason`` + ``post_consume`` for ``call_count`` cycles.

    Uses a synthetic monotonic clock (start + i × delta) so the
    measurement is purely the penstock's cost, not the per-call
    ``time.monotonic()`` syscall.  Returns elapsed wall-clock seconds.
    """
    edge_state = _MockEdgeState()
    start_clock = 1000.0  # arbitrary monotonic base; only deltas matter
    delta = 1.0  # 1-second per "tick" — generous so SustainedPenstock never blocks

    t0 = time.perf_counter()
    for i in range(call_count):
        now = start_clock + i * delta
        reason = penstock.consume_reason(edge_state, flow, now)
        if reason is None:
            penstock.post_consume(edge_state, now)
    elapsed = time.perf_counter() - t0
    return elapsed


def _report(name: str, elapsed: float, floor: float) -> float:
    """Compute ops/sec, print, assert floor.  Returns ops/sec."""
    ops_per_sec = CALL_COUNT / elapsed
    print(f"\n  Penstock {name:<22} {CALL_COUNT:,} consume+post cycles in {elapsed:.3f}s = {ops_per_sec:,.0f} ops/sec")
    assert ops_per_sec >= floor, (
        f"{name} dropped to {ops_per_sec:,.0f} ops/sec (floor: {floor:,.0f}). "
        "Suggests regression in evaluate/record OR in the shared "
        "Penstock.consume_reason wrapper."
    )
    return ops_per_sec


@pytest.mark.benchmark
def test_null_penstock_overhead() -> None:
    """NullPenstock — the always-permit baseline.  Fastest case."""
    elapsed = _drive_penstock(NullPenstock(), flow=None, call_count=CALL_COUNT)
    _report("NullPenstock", elapsed, NULL_FLOOR)


@pytest.mark.benchmark
def test_sustained_penstock_overhead() -> None:
    """SustainedPenstock at a high rate — always permits, measures evaluate cost."""
    penstock = SustainedPenstock(rate_per_sec=1_000_000.0)
    elapsed = _drive_penstock(penstock, flow=None, call_count=CALL_COUNT)
    _report("SustainedPenstock", elapsed, SUSTAINED_FLOOR)


@pytest.mark.benchmark
def test_burst_penstock_overhead() -> None:
    """BurstPenstock with huge bucket — bucket arithmetic dominates."""
    # With delta=1s and rate_per_sec=1M, every call refills 1M tokens; bucket
    # tops out at 1M and we consume 1 per call → never blocks.
    penstock = BurstPenstock(rate_per_sec=1_000_000.0, burst=1_000_000)
    elapsed = _drive_penstock(penstock, flow=None, call_count=CALL_COUNT)
    _report("BurstPenstock", elapsed, BURST_FLOOR)


@pytest.mark.benchmark
def test_window_penstock_overhead() -> None:
    """WindowPenstock — list-comp eviction is the dominant cost.

    With ``window_sec=0.5`` and ``delta=1.0`` between calls, every call
    evicts the previous entry before appending its own — ``window_log``
    stays at 1 entry steady-state.  This measures the list-comp + append
    pair without letting the log grow unbounded (which would skew the
    benchmark toward eviction cost rather than per-call cost).
    """
    penstock = WindowPenstock(window_sec=0.5, cap=10_000_000)
    elapsed = _drive_penstock(penstock, flow=None, call_count=CALL_COUNT)
    _report("WindowPenstock", elapsed, WINDOW_FLOOR)


@pytest.mark.benchmark
def test_signal_penstock_overhead() -> None:
    """SignalPenstock — adds a Python lambda call to the sustained path."""
    penstock = SignalPenstock(rate_fn=lambda state, now: 1_000_000.0)
    elapsed = _drive_penstock(penstock, flow=None, call_count=CALL_COUNT)
    _report("SignalPenstock", elapsed, SIGNAL_FLOOR)


@pytest.mark.benchmark
def test_backpressure_penstock_overhead() -> None:
    """BackpressurePenstock — reads reservoir context from the parent FlowControl.

    The mock edge state's ``waves`` deque stays empty for the whole loop,
    so fullness is 0.0 and the effective rate is ``max_rate=1_000_000``.
    Measures the fullness calculation + sustained-style arithmetic.
    """
    penstock = BackpressurePenstock(min_rate=1.0, max_rate=1_000_000.0)
    # FlowControl with a depth-1 reservoir (the default).  Backpressure
    # reads ``flow.reservoir.depth`` for the fullness denominator.
    flow = FlowControl(penstock=penstock, reservoir=Reservoir(depth=1))
    elapsed = _drive_penstock(penstock, flow=flow, call_count=CALL_COUNT)
    _report("BackpressurePenstock", elapsed, BACKPRESSURE_FLOOR)
