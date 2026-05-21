"""Unit tests for the unified Penstock primitive in :mod:`incorporator.io.penstock`.

Covers the new ``evaluate`` / ``record`` / ``acquire`` interface that
both the HTTP throttle layer and the Tideweaver edge layer share.
Existing :mod:`tests.test_tideweaver` exercises ``consume_reason`` /
``post_consume`` at the edge layer end-to-end; here we test the gates
in isolation against the canonical :class:`FlowState`.
"""

from __future__ import annotations

import asyncio

import pytest

from incorporator.io.penstock import (
    BoundPenstock,
    BurstPenstock,
    FlowState,
    NullPenstock,
    Penstock,
    SignalPenstock,
    SustainedPenstock,
    WindowPenstock,
)

# ---------------------------------------------------------------------------
# FlowState — the mutable state container
# ---------------------------------------------------------------------------


def test_flow_state_defaults() -> None:
    """A fresh :class:`FlowState` has all counters at their pre-touch values."""
    s = FlowState()
    assert s.last_consumed_at is None
    assert s.bucket_tokens is None
    assert s.bucket_last_refill_at is None
    assert s.window_log == []


# ---------------------------------------------------------------------------
# NullPenstock — always permits, never records
# ---------------------------------------------------------------------------


def test_null_evaluate_always_permits() -> None:
    """:meth:`NullPenstock.evaluate` returns ``None`` (permit) unconditionally."""
    pen = NullPenstock()
    state = FlowState()
    assert pen.evaluate(state, 0.0) is None
    assert pen.evaluate(state, 1000.0) is None


def test_null_record_does_nothing() -> None:
    """:meth:`NullPenstock.record` leaves state untouched."""
    pen = NullPenstock()
    state = FlowState()
    pen.record(state, 42.0)
    assert state == FlowState()


# ---------------------------------------------------------------------------
# SustainedPenstock — leaky bucket / minimum gap
# ---------------------------------------------------------------------------


def test_sustained_first_call_permitted() -> None:
    """First consumption (no prior ``last_consumed_at``) is always permitted."""
    pen = SustainedPenstock(rate_per_sec=10.0)
    state = FlowState()
    assert pen.evaluate(state, 0.0) is None


def test_sustained_blocks_when_gap_too_short() -> None:
    """Second consumption inside the min-gap returns positive wait seconds."""
    pen = SustainedPenstock(rate_per_sec=10.0)  # 100ms gap
    state = FlowState(last_consumed_at=0.0)
    wait = pen.evaluate(state, 0.05)  # 50ms elapsed, need 100
    assert wait is not None
    assert wait == pytest.approx(0.05, abs=1e-6)


def test_sustained_permits_after_gap() -> None:
    """Consumption is permitted once the min-gap has elapsed."""
    pen = SustainedPenstock(rate_per_sec=10.0)
    state = FlowState(last_consumed_at=0.0)
    assert pen.evaluate(state, 0.2) is None


def test_sustained_record_advances_watermark() -> None:
    """:meth:`record` sets ``last_consumed_at`` to ``now``."""
    pen = SustainedPenstock(rate_per_sec=10.0)
    state = FlowState()
    pen.record(state, 42.0)
    assert state.last_consumed_at == 42.0


# ---------------------------------------------------------------------------
# BurstPenstock — token bucket
# ---------------------------------------------------------------------------


def test_burst_first_touch_initialises_full_bucket() -> None:
    """The bucket is lazily initialised to ``burst`` tokens on first evaluation."""
    pen = BurstPenstock(rate_per_sec=1.0, burst=5)
    state = FlowState()
    assert pen.evaluate(state, 0.0) is None
    assert state.bucket_tokens == 5.0


def test_burst_record_debits_one_token() -> None:
    """Each :meth:`record` consumes exactly one token."""
    pen = BurstPenstock(rate_per_sec=1.0, burst=3)
    state = FlowState()
    pen.evaluate(state, 0.0)
    pen.record(state, 0.0)
    assert state.bucket_tokens == 2.0


def test_burst_blocks_when_empty_and_returns_refill_wait() -> None:
    """An empty bucket returns the seconds until 1 token has refilled."""
    pen = BurstPenstock(rate_per_sec=2.0, burst=1)  # 0.5s per token
    state = FlowState()
    # Burn the bucket dry.
    pen.evaluate(state, 0.0)
    pen.record(state, 0.0)
    # Try again immediately.
    wait = pen.evaluate(state, 0.0)
    assert wait is not None
    assert wait == pytest.approx(0.5, abs=1e-6)


def test_burst_refills_over_time() -> None:
    """Time-based refill restores tokens up to ``burst`` capacity."""
    pen = BurstPenstock(rate_per_sec=10.0, burst=5)
    state = FlowState()
    pen.evaluate(state, 0.0)
    for _ in range(5):
        pen.record(state, 0.0)
    # Bucket empty; allow 1s = 10 tokens of refill but cap at burst=5.
    pen.evaluate(state, 1.0)
    assert state.bucket_tokens == 5.0


# ---------------------------------------------------------------------------
# WindowPenstock — rolling-window quota
# ---------------------------------------------------------------------------


def test_window_permits_under_cap() -> None:
    """Consumptions under the cap return permit."""
    pen = WindowPenstock(window_sec=10.0, cap=3)
    state = FlowState()
    assert pen.evaluate(state, 0.0) is None


def test_window_blocks_at_cap_with_wait_to_oldest_expiry() -> None:
    """At cap, returns seconds until the oldest entry falls out of the window."""
    pen = WindowPenstock(window_sec=10.0, cap=2)
    state = FlowState(window_log=[0.0, 5.0])
    wait = pen.evaluate(state, 6.0)  # cap reached; oldest expires at 10.0
    assert wait is not None
    assert wait == pytest.approx(4.0, abs=1e-6)


def test_window_evicts_stale_entries_during_evaluate() -> None:
    """Entries older than the window are dropped during evaluation."""
    pen = WindowPenstock(window_sec=10.0, cap=2)
    state = FlowState(window_log=[0.0, 5.0])
    pen.evaluate(state, 15.0)
    # The 0.0 entry should be evicted (15 - 10 = cutoff 5; only > 5 survives).
    assert 0.0 not in state.window_log


def test_window_record_appends() -> None:
    """:meth:`record` appends ``now`` to the rolling log."""
    pen = WindowPenstock(window_sec=10.0, cap=3)
    state = FlowState()
    pen.record(state, 1.0)
    pen.record(state, 2.0)
    assert state.window_log == [1.0, 2.0]


# ---------------------------------------------------------------------------
# SignalPenstock — user-supplied rate function
# ---------------------------------------------------------------------------


def test_signal_zero_rate_returns_inf() -> None:
    """A ``rate_fn`` return of ``<= 0`` returns ``inf`` (caller blocks)."""
    pen = SignalPenstock(rate_fn=lambda state, now: 0.0)
    state = FlowState()
    wait = pen.evaluate(state, 0.0)
    assert wait == float("inf")


def test_signal_positive_rate_acts_as_sustained() -> None:
    """Positive rate enforces the corresponding min-gap (sustained behaviour)."""
    pen = SignalPenstock(rate_fn=lambda state, now: 10.0)  # 100ms gap
    state = FlowState(last_consumed_at=0.0)
    wait = pen.evaluate(state, 0.05)
    assert wait is not None
    assert wait == pytest.approx(0.05, abs=1e-6)


# ---------------------------------------------------------------------------
# acquire() — HTTP-style sleep-until-permitted
# ---------------------------------------------------------------------------


def _looptime_now() -> float:
    """Helper to fetch a stable monotonic-loop time inside an async test."""
    return asyncio.get_running_loop().time()


async def _bench_acquire(pen: Penstock, state: FlowState, lock: asyncio.Lock) -> float:
    start = _looptime_now()
    await pen.acquire(state, lock)
    return _looptime_now() - start


@pytest.mark.asyncio
async def test_acquire_null_returns_immediately() -> None:
    """:class:`NullPenstock` acquire is effectively free."""
    pen = NullPenstock()
    elapsed = await _bench_acquire(pen, FlowState(), asyncio.Lock())
    assert elapsed < 0.05


@pytest.mark.asyncio
async def test_acquire_sustained_sleeps_for_gap() -> None:
    """:class:`SustainedPenstock` acquire sleeps when called twice rapidly."""
    pen = SustainedPenstock(rate_per_sec=20.0)  # 50ms gap
    state = FlowState()
    lock = asyncio.Lock()
    # First acquire is free (no prior watermark).
    await pen.acquire(state, lock)
    # Second acquire should sleep approximately the min gap.
    elapsed = await _bench_acquire(pen, state, lock)
    assert elapsed >= 0.04, f"expected >=40ms sleep, got {elapsed:.3f}s"


@pytest.mark.asyncio
async def test_acquire_record_updates_state() -> None:
    """After :meth:`acquire`, ``last_consumed_at`` is advanced."""
    pen = SustainedPenstock(rate_per_sec=10.0)
    state = FlowState()
    await pen.acquire(state, asyncio.Lock())
    assert state.last_consumed_at is not None
    assert state.last_consumed_at > 0.0


# ---------------------------------------------------------------------------
# BoundPenstock — the (penstock, state, lock) binding used by HTTP host registry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bound_penstock_acquire_delegates() -> None:
    """:class:`BoundPenstock` is a thin (penstock, state, lock) holder."""
    bound = BoundPenstock(
        penstock=SustainedPenstock(rate_per_sec=100.0),
        state=FlowState(),
        lock=asyncio.Lock(),
    )
    await bound.acquire()
    assert bound.state.last_consumed_at is not None


# ---------------------------------------------------------------------------
# consume_reason() — edge-style skip semantic (the default Penstock impl)
# ---------------------------------------------------------------------------


def test_consume_reason_returns_none_when_permitted() -> None:
    """Default ``consume_reason`` returns ``None`` when ``evaluate`` permits."""
    pen = SustainedPenstock(rate_per_sec=10.0)
    state = FlowState()
    assert pen.consume_reason(state, flow=None, now=0.0) is None


def test_consume_reason_returns_string_when_blocked() -> None:
    """Default ``consume_reason`` translates a wait into ``"penstock_limited"``."""
    pen = SustainedPenstock(rate_per_sec=10.0)
    state = FlowState(last_consumed_at=0.0)
    assert pen.consume_reason(state, flow=None, now=0.05) == "penstock_limited"


def test_post_consume_advances_state() -> None:
    """``post_consume`` calls ``record``; ``last_consumed_at`` advances."""
    pen = SustainedPenstock(rate_per_sec=10.0)
    state = FlowState()
    pen.post_consume(state, 42.0)
    assert state.last_consumed_at == 42.0
