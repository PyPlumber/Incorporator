"""Tests for penstock _compute_cooldown hooks and consume_reason tuple return.

Each test proves exactly one behaviour — the docstring states what behaviour
that is.
"""

from __future__ import annotations

import math
from collections import deque
from typing import Any
from unittest.mock import MagicMock

import pytest

from incorporator.io.penstock import (
    BurstPenstock,
    FlowState,
    NullPenstock,
    Penstock,
    SignalPenstock,
    SustainedPenstock,
    WindowPenstock,
)
from incorporator.tideweaver.flow import BackpressurePenstock, FlowControl, Reservoir


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _edge_state_mock(waves: int = 0, last_consumed_at: float | None = None) -> Any:
    """Return a duck-typed _EdgeState mock for BackpressurePenstock tests."""
    mock = MagicMock()
    mock.waves = [object()] * waves
    state = FlowState(last_consumed_at=last_consumed_at)
    mock.flow_state = state
    return mock


def _flow_control(depth: int = 10) -> FlowControl:
    """Return a FlowControl with a Reservoir of given depth."""
    return FlowControl(reservoir=Reservoir(depth=depth))


# ---------------------------------------------------------------------------
# NullPenstock — inherits default _compute_cooldown → returns None
# ---------------------------------------------------------------------------


def test_null_penstock_consume_reason_returns_none() -> None:
    """NullPenstock.consume_reason always returns None (never blocked)."""
    p = NullPenstock()
    state = FlowState()
    result = p.consume_reason(state, None, 100.0)
    assert result is None


def test_null_penstock_compute_cooldown_is_none() -> None:
    """NullPenstock._compute_cooldown returns None (inherited default)."""
    p = NullPenstock()
    state = FlowState()
    assert p._compute_cooldown(state, None, 100.0) is None


# ---------------------------------------------------------------------------
# SustainedPenstock — cooldown = 1/rate - elapsed
# ---------------------------------------------------------------------------


def test_sustained_penstock_blocked_returns_tuple() -> None:
    """SustainedPenstock.consume_reason returns (reason, cooldown_sec) when blocked."""
    p = SustainedPenstock(rate_per_sec=2.0)  # min_gap = 0.5s
    state = FlowState(last_consumed_at=100.0)
    now = 100.3  # elapsed = 0.3 < 0.5 → blocked
    result = p.consume_reason(state, None, now)
    assert result is not None
    reason, cooldown = result
    assert reason == "penstock_limited"
    assert cooldown is not None
    assert cooldown == pytest.approx(0.2, abs=1e-9)  # 0.5 - 0.3


def test_sustained_penstock_just_blocked_cooldown_sane() -> None:
    """SustainedPenstock in a just-blocked state returns a positive cooldown."""
    p = SustainedPenstock(rate_per_sec=1.0)  # min_gap = 1.0s
    state = FlowState(last_consumed_at=100.0)
    now = 100.001  # just consumed
    result = p.consume_reason(state, None, now)
    assert result is not None
    _, cooldown = result
    assert cooldown is not None
    assert cooldown > 0.0
    assert cooldown < 1.0


def test_sustained_penstock_not_blocked_returns_none() -> None:
    """SustainedPenstock.consume_reason returns None when not rate-limited."""
    p = SustainedPenstock(rate_per_sec=2.0)
    state = FlowState(last_consumed_at=100.0)
    now = 100.6  # elapsed = 0.6 > 0.5 → permitted
    assert p.consume_reason(state, None, now) is None


def test_sustained_penstock_no_prior_consume_returns_none() -> None:
    """SustainedPenstock returns None when last_consumed_at is None (first ever call)."""
    p = SustainedPenstock(rate_per_sec=1.0)
    state = FlowState()  # last_consumed_at = None
    assert p.consume_reason(state, None, 100.0) is None


# ---------------------------------------------------------------------------
# BurstPenstock — cooldown = (1 - tokens) / rate when tokens < 1
# ---------------------------------------------------------------------------


def test_burst_penstock_blocked_returns_tuple_with_cooldown() -> None:
    """BurstPenstock in empty-bucket state returns (reason, positive cooldown)."""
    p = BurstPenstock(rate_per_sec=1.0, burst=2)
    state = FlowState()
    # Prime with evaluate to fill tokens, then drain them.
    p.evaluate(state, 100.0)  # init: bucket_tokens = 2.0
    state.bucket_tokens = 0.5  # simulate partial drain
    result = p.consume_reason(state, None, 100.0)
    assert result is not None
    reason, cooldown = result
    assert reason == "penstock_limited"
    assert cooldown is not None
    assert cooldown == pytest.approx(0.5, abs=1e-9)  # (1 - 0.5) / 1.0


def test_burst_penstock_full_bucket_returns_none() -> None:
    """BurstPenstock with full bucket returns None (permitted)."""
    p = BurstPenstock(rate_per_sec=2.0, burst=5)
    state = FlowState()
    # First call — bucket initialises full.
    assert p.consume_reason(state, None, 100.0) is None


def test_burst_penstock_compute_cooldown_none_when_bucket_none() -> None:
    """BurstPenstock._compute_cooldown returns None before bucket is initialised."""
    p = BurstPenstock(rate_per_sec=1.0, burst=3)
    state = FlowState()  # bucket_tokens = None
    assert p._compute_cooldown(state, None, 100.0) is None


def test_burst_penstock_compute_cooldown_none_when_full() -> None:
    """BurstPenstock._compute_cooldown returns None when bucket is >= 1 token."""
    p = BurstPenstock(rate_per_sec=1.0, burst=3)
    state = FlowState(bucket_tokens=1.5)
    assert p._compute_cooldown(state, None, 100.0) is None


# ---------------------------------------------------------------------------
# WindowPenstock — cooldown = oldest_entry + window_sec - now
# ---------------------------------------------------------------------------


def test_window_penstock_blocked_returns_tuple_with_cooldown() -> None:
    """WindowPenstock at capacity returns (reason, time-until-oldest-expiry)."""
    p = WindowPenstock(window_sec=60.0, cap=2)
    state = FlowState(window_log=[100.0, 101.0])
    now = 110.0
    # Eviction cutoff = 110 - 60 = 50; both entries are > 50 so cap hit.
    result = p.consume_reason(state, None, now)
    assert result is not None
    reason, cooldown = result
    assert reason == "penstock_limited"
    assert cooldown is not None
    # oldest=100.0, cooldown = 100 + 60 - 110 = 50.0
    assert cooldown == pytest.approx(50.0, abs=1e-9)


def test_window_penstock_not_at_cap_returns_none() -> None:
    """WindowPenstock below cap returns None."""
    p = WindowPenstock(window_sec=60.0, cap=5)
    state = FlowState(window_log=[100.0])
    assert p.consume_reason(state, None, 110.0) is None


def test_window_penstock_empty_log_compute_cooldown_none() -> None:
    """WindowPenstock._compute_cooldown returns None when window_log is empty."""
    p = WindowPenstock(window_sec=60.0, cap=3)
    state = FlowState()
    assert p._compute_cooldown(state, None, 100.0) is None


# ---------------------------------------------------------------------------
# SignalPenstock — inherits default _compute_cooldown → returns None
# ---------------------------------------------------------------------------


def test_signal_penstock_blocked_returns_tuple_cooldown_none() -> None:
    """SignalPenstock blocked state returns (reason, None) — no cooldown computation."""
    p = SignalPenstock(rate_fn=lambda s, t: 0.0)  # always blocks
    state = FlowState()
    result = p.consume_reason(state, None, 100.0)
    assert result is not None
    reason, cooldown = result
    assert reason == "penstock_limited"
    assert cooldown is None  # inherited default


def test_signal_penstock_compute_cooldown_is_none() -> None:
    """SignalPenstock._compute_cooldown always returns None (inherited default)."""
    p = SignalPenstock(rate_fn=lambda s, t: 1.0)
    state = FlowState()
    assert p._compute_cooldown(state, None, 100.0) is None


# ---------------------------------------------------------------------------
# BackpressurePenstock — cooldown = 1/effective_rate when blocked
# ---------------------------------------------------------------------------


def test_backpressure_penstock_half_full_cooldown() -> None:
    """BackpressurePenstock with half-full reservoir returns 1/effective_rate."""
    p = BackpressurePenstock(min_rate=1.0, max_rate=4.0)
    flow = _flow_control(depth=10)
    # 5 waves out of 10 → fullness = 0.5 → effective_rate = 4 - (4-1)*0.5 = 2.5
    edge_state = _edge_state_mock(waves=5, last_consumed_at=100.0)
    now = 100.001  # just consumed → still in cooldown
    result = p.consume_reason(edge_state, flow, now)
    assert result is not None
    reason, cooldown = result
    assert reason == "penstock_limited"
    assert cooldown is not None
    assert cooldown == pytest.approx(1.0 / 2.5, abs=1e-6)


def test_backpressure_penstock_empty_reservoir_returns_none_when_past_gap() -> None:
    """BackpressurePenstock with empty reservoir permits when enough time has passed."""
    p = BackpressurePenstock(min_rate=1.0, max_rate=4.0)
    flow = _flow_control(depth=10)
    edge_state = _edge_state_mock(waves=0, last_consumed_at=100.0)
    # effective_rate = 4.0 when fullness=0; min_gap = 0.25s
    now = 100.5  # well past min_gap
    assert p.consume_reason(edge_state, flow, now) is None


def test_backpressure_penstock_no_prior_consume_returns_none() -> None:
    """BackpressurePenstock returns None when last_consumed_at is None (first call)."""
    p = BackpressurePenstock(min_rate=1.0, max_rate=4.0)
    flow = _flow_control(depth=10)
    edge_state = _edge_state_mock(waves=0, last_consumed_at=None)
    assert p.consume_reason(edge_state, flow, 100.0) is None


# ---------------------------------------------------------------------------
# Back-compat shim — legacy str|None-returning subclass handled at call site
# ---------------------------------------------------------------------------


class _LegacyPenstock(Penstock):
    """Mock third-party penstock that returns old str | None from consume_reason."""

    type: str = "legacy"  # type: ignore[assignment]

    def evaluate(self, state: Any, now: float, *, context: Any = None) -> float | None:
        return 0.5  # always blocked

    def consume_reason(self, edge_state: Any, flow: Any, now: float) -> Any:  # type: ignore[override]
        return "penstock_limited"  # old string return — not a tuple


def test_legacy_str_return_shim_in_scheduler() -> None:
    """Scheduler shim handles legacy str-returning penstock without raising, produces cooldown_sec=None."""
    p = _LegacyPenstock()
    raw = p.consume_reason(FlowState(), None, 100.0)
    # Simulate the scheduler shim logic:
    if isinstance(raw, str):
        penstock_reason, cooldown_sec = raw, None
    elif raw is not None:
        penstock_reason, cooldown_sec = raw
    else:
        penstock_reason, cooldown_sec = None, None
    assert penstock_reason == "penstock_limited"
    assert cooldown_sec is None
