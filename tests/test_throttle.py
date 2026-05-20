"""Unit tests for :mod:`incorporator.io.throttle`.

Covers each concrete strategy, the resolver's precedence order, the
``INCORPORATOR_RATE_LIMIT_BYPASS`` env var, the public
``register_host_throttle`` extension point, and the new per-host
fan-out isolation property that this module unlocks (a CoinGecko +
PokeAPI fan-out now runs each host at its own rate instead of
collapsing to the minimum).
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, List

import pytest

from incorporator.io.throttle import (
    BurstThrottle,
    DEFAULT_RPS,
    FixedIntervalThrottle,
    NullThrottle,
    ThrottleStrategy,
    _BYPASS_ENV_VAR,
    _HOST_FACTORIES,
    known_host_rates,
    register_host_throttle,
    resolve_throttle,
)


# ---------------------------------------------------------------------------
# Strategy classes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_null_throttle_acquire_returns_immediately() -> None:
    """NullThrottle.acquire is a no-op: returns under 1 ms even when called repeatedly."""
    throttle = NullThrottle()
    start = time.perf_counter()
    for _ in range(100):
        await throttle.acquire()
    elapsed = time.perf_counter() - start
    assert elapsed < 0.05, f"NullThrottle should be ~free, took {elapsed:.3f}s"


@pytest.mark.asyncio
async def test_fixed_interval_enforces_minimum_gap() -> None:
    """Two consecutive acquires on a 10-rps throttle wait ~100ms between them."""
    throttle = FixedIntervalThrottle(10.0)  # 100ms interval
    await throttle.acquire()  # first call: no wait
    t0 = time.perf_counter()
    await throttle.acquire()  # second call: must wait ~100ms
    elapsed = time.perf_counter() - t0
    assert 0.08 < elapsed < 0.20, f"second acquire should wait ~100ms, got {elapsed:.3f}s"


def test_fixed_interval_rejects_zero_or_negative_rps() -> None:
    """FixedIntervalThrottle requires positive rps — use NullThrottle for no throttle."""
    for bad in (0.0, -1.0, -0.001):
        with pytest.raises(ValueError, match="requests_per_second > 0"):
            FixedIntervalThrottle(bad)


@pytest.mark.asyncio
async def test_burst_throttle_allows_initial_burst_then_steady_state() -> None:
    """A 5-token bucket refilled at 5 rps lets the first 5 calls fire instantly."""
    throttle = BurstThrottle(requests_per_second=5.0, burst=5)
    start = time.perf_counter()
    for _ in range(5):
        await throttle.acquire()
    burst_elapsed = time.perf_counter() - start
    assert burst_elapsed < 0.05, f"first 5 (within burst) should be ~free, took {burst_elapsed:.3f}s"
    # 6th call should wait ~200ms (1 token / 5 rps).
    t0 = time.perf_counter()
    await throttle.acquire()
    sixth_elapsed = time.perf_counter() - t0
    assert 0.15 < sixth_elapsed < 0.30, f"sixth acquire should wait ~200ms, got {sixth_elapsed:.3f}s"


def test_burst_throttle_rejects_invalid_args() -> None:
    """BurstThrottle requires rps > 0 and burst >= 1."""
    for bad_args in [(0.0, 1), (-1.0, 1), (1.0, 0), (1.0, -1)]:
        with pytest.raises(ValueError, match="requests_per_second > 0 and burst >= 1"):
            BurstThrottle(*bad_args)


def test_all_strategies_satisfy_protocol() -> None:
    """NullThrottle, FixedIntervalThrottle, BurstThrottle are runtime_checkable as ThrottleStrategy."""
    assert isinstance(NullThrottle(), ThrottleStrategy)
    assert isinstance(FixedIntervalThrottle(1.0), ThrottleStrategy)
    assert isinstance(BurstThrottle(1.0, 1), ThrottleStrategy)


# ---------------------------------------------------------------------------
# resolve_throttle precedence
# ---------------------------------------------------------------------------


def test_resolve_throttle_caller_rps_wins() -> None:
    """Caller-supplied requests_per_second overrides the per-host registry."""
    # CoinGecko's registered rate is 0.2; explicit 5.0 should win.
    strategy = resolve_throttle("https://api.coingecko.com/x", requests_per_second=5.0)
    assert isinstance(strategy, FixedIntervalThrottle)
    assert strategy.rate == 5.0


def test_resolve_throttle_zero_rps_returns_null() -> None:
    """requests_per_second=0 means 'no throttle' — even on a registered host."""
    strategy = resolve_throttle("https://api.coingecko.com/x", requests_per_second=0)
    assert isinstance(strategy, NullThrottle)


def test_resolve_throttle_negative_rps_returns_null() -> None:
    """Negative rps is also a no-throttle marker."""
    strategy = resolve_throttle("https://example.com/x", requests_per_second=-1.0)
    assert isinstance(strategy, NullThrottle)


def test_resolve_throttle_caller_rps_with_burst_returns_burst_throttle() -> None:
    """rps + burst → BurstThrottle, not FixedIntervalThrottle."""
    strategy = resolve_throttle("https://example.com/x", requests_per_second=10.0, burst=3)
    assert isinstance(strategy, BurstThrottle)
    assert strategy.rate == 10.0
    assert strategy.capacity == 3.0


def test_resolve_throttle_known_host_returns_host_strategy() -> None:
    """A registered host (CoinGecko) resolves to its FixedIntervalThrottle at 0.2 rps."""
    strategy = resolve_throttle("https://api.coingecko.com/api/v3/coins/markets")
    assert isinstance(strategy, FixedIntervalThrottle)
    assert strategy.rate == 0.2


def test_resolve_throttle_unknown_host_falls_back_to_default() -> None:
    """Unknown host → FixedIntervalThrottle(DEFAULT_RPS)."""
    strategy = resolve_throttle("https://api.binance.us/api/v3/ticker/price")
    assert isinstance(strategy, FixedIntervalThrottle)
    assert strategy.rate == DEFAULT_RPS


def test_resolve_throttle_non_string_source_falls_back_to_default() -> None:
    """Non-string sources (None, Path, etc.) skip the host lookup."""
    strategy = resolve_throttle(None)
    assert isinstance(strategy, FixedIntervalThrottle)
    assert strategy.rate == DEFAULT_RPS


def test_resolve_throttle_env_var_bypass_returns_null(monkeypatch: pytest.MonkeyPatch) -> None:
    """INCORPORATOR_RATE_LIMIT_BYPASS=1 forces NullThrottle everywhere."""
    monkeypatch.setenv(_BYPASS_ENV_VAR, "1")
    # Even on a registered host with an explicit caller rate, the env var wins.
    assert isinstance(resolve_throttle("https://api.coingecko.com/x"), NullThrottle)
    assert isinstance(resolve_throttle("https://api.coingecko.com/x", requests_per_second=5.0), NullThrottle)
    assert isinstance(resolve_throttle("https://api.binance.us/x"), NullThrottle)


def test_resolve_throttle_env_var_other_values_dont_bypass(monkeypatch: pytest.MonkeyPatch) -> None:
    """Only the exact string ``"1"`` triggers the bypass — guards against typos."""
    for value in ("0", "true", "yes", ""):
        monkeypatch.setenv(_BYPASS_ENV_VAR, value)
        assert not isinstance(resolve_throttle("https://api.coingecko.com/x"), NullThrottle), (
            f"env value {value!r} should not bypass"
        )


# ---------------------------------------------------------------------------
# register_host_throttle extension point
# ---------------------------------------------------------------------------


def test_register_host_throttle_adds_custom_strategy(monkeypatch: pytest.MonkeyPatch) -> None:
    """A user-registered host factory wins over the default fallback."""
    sentinel_rate = 7.7
    monkeypatch.setitem(
        _HOST_FACTORIES, "test.example.com", lambda: FixedIntervalThrottle(sentinel_rate)
    )
    strategy = resolve_throttle("https://test.example.com/api")
    assert isinstance(strategy, FixedIntervalThrottle)
    assert strategy.rate == sentinel_rate


def test_register_host_throttle_overrides_built_in(monkeypatch: pytest.MonkeyPatch) -> None:
    """register_host_throttle can shadow a built-in registry entry."""
    monkeypatch.setitem(
        _HOST_FACTORIES, "api.coingecko.com", lambda: FixedIntervalThrottle(99.0)
    )
    strategy = resolve_throttle("https://api.coingecko.com/x")
    assert isinstance(strategy, FixedIntervalThrottle)
    assert strategy.rate == 99.0


def test_known_host_rates_returns_current_registry() -> None:
    """``known_host_rates()`` exposes the registry for diagnostics."""
    rates = known_host_rates()
    assert rates["api.coingecko.com"] == 0.2
    assert rates["pokeapi.co"] == 1.5
    assert rates["vpic.nhtsa.dot.gov"] == 1.5


def test_resolver_returns_fresh_strategy_per_call() -> None:
    """Two calls for the same host return DIFFERENT instances — each fan-out leg has its own state."""
    a = resolve_throttle("https://api.coingecko.com/x")
    b = resolve_throttle("https://api.coingecko.com/x")
    assert a is not b, "resolve_throttle must return a fresh instance so siblings don't share lock state"


# ---------------------------------------------------------------------------
# Per-host isolation in fan-out — the load-bearing new capability.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fan_out_two_hosts_run_in_parallel_at_own_rates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two hosts in a fan-out each run at their own rate concurrently — not collapsed to the min.

    Simulates two coingecko-like and one pokeapi-like host with realistic
    intervals. The total wall-clock time must be near ``max(host_interval)``
    (parallel), NOT ``sum(host_intervals)`` (serial) and NOT
    ``min_rate * total_requests`` (the legacy collapse).
    """
    # Override with fast rates so the test runs quickly but still proves the property.
    monkeypatch.setitem(_HOST_FACTORIES, "host-a.example.com", lambda: FixedIntervalThrottle(20.0))  # 50ms interval
    monkeypatch.setitem(_HOST_FACTORIES, "host-b.example.com", lambda: FixedIntervalThrottle(2.0))  # 500ms interval

    # Build per-host throttle map — exactly what fetch_concurrent_payloads now does.
    by_host: dict = {}
    sources = [
        "https://host-a.example.com/1",
        "https://host-a.example.com/2",
        "https://host-b.example.com/1",
    ]
    for src in sources:
        from urllib.parse import urlparse

        host = urlparse(src).hostname or ""
        if host not in by_host:
            by_host[host] = resolve_throttle(src)

    async def one_request(src: str) -> str:
        host = urlparse(src).hostname or ""
        throttle = by_host[host]
        await throttle.acquire()
        return src

    # Fire all 3 requests concurrently.  host-a fires twice (one at 0ms,
    # one at 50ms); host-b fires once (at 0ms).  Wall-clock should be
    # close to 50ms — well under host-b's 500ms interval and well under
    # the legacy ``min(2.0) * 3 = 1500ms`` collapse.
    start = time.perf_counter()
    await asyncio.gather(*(one_request(s) for s in sources))
    elapsed = time.perf_counter() - start

    assert elapsed < 0.30, (
        f"fan-out across two hosts must run in parallel at each host's rate, "
        f"not collapsed to the minimum.  Elapsed {elapsed:.3f}s, expected < 0.3s."
    )
