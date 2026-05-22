"""Unit tests for the per-host penstock registry in :mod:`incorporator.io.penstock`.

Covers :func:`resolve_penstock`'s precedence order, the
``INCORPORATOR_RATE_LIMIT_BYPASS`` env var, the public
:func:`register_host_penstock` extension point, and the per-host
fan-out isolation property (a CoinGecko + PokeAPI fan-out runs each
host at its own rate concurrently instead of collapsing to the
minimum).

Gate-level math (token-bucket refill, sliding-window eviction, etc.)
is covered by :mod:`tests.test_io_penstock`.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from incorporator.io.penstock import (
    DEFAULT_RPS,
    BurstPenstock,
    NullPenstock,
    SustainedPenstock,
    _BYPASS_ENV_VAR,
    _HOST_PENSTOCKS,
    known_host_rates,
    register_host_penstock,
    resolve_penstock,
)

# ---------------------------------------------------------------------------
# resolve_penstock precedence
# ---------------------------------------------------------------------------


def test_resolve_caller_rps_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    """Caller-supplied requests_per_second overrides any registered per-host rate."""
    monkeypatch.setitem(_HOST_PENSTOCKS, "api.example.com", SustainedPenstock(rate_per_sec=0.2))
    bound = resolve_penstock("https://api.example.com/x", requests_per_second=5.0)
    assert isinstance(bound.penstock, SustainedPenstock)
    assert bound.penstock.rate_per_sec == 5.0


def test_resolve_zero_rps_returns_null(monkeypatch: pytest.MonkeyPatch) -> None:
    """requests_per_second=0 means 'no throttle' — even on a registered host."""
    monkeypatch.setitem(_HOST_PENSTOCKS, "api.example.com", SustainedPenstock(rate_per_sec=0.2))
    bound = resolve_penstock("https://api.example.com/x", requests_per_second=0)
    assert isinstance(bound.penstock, NullPenstock)


def test_resolve_negative_rps_returns_null() -> None:
    """Negative rps is also a no-throttle marker."""
    bound = resolve_penstock("https://example.com/x", requests_per_second=-1.0)
    assert isinstance(bound.penstock, NullPenstock)


def test_resolve_caller_rps_with_burst_returns_burst_penstock() -> None:
    """rps + burst → BurstPenstock, not SustainedPenstock."""
    bound = resolve_penstock("https://example.com/x", requests_per_second=10.0, burst=3)
    assert isinstance(bound.penstock, BurstPenstock)
    assert bound.penstock.rate_per_sec == 10.0
    assert bound.penstock.burst == 3


def test_resolve_known_host_returns_registered_penstock(monkeypatch: pytest.MonkeyPatch) -> None:
    """A registered host resolves to its registered SustainedPenstock rate."""
    monkeypatch.setitem(_HOST_PENSTOCKS, "api.coingecko.com", SustainedPenstock(rate_per_sec=0.2))
    bound = resolve_penstock("https://api.coingecko.com/api/v3/coins/markets")
    assert isinstance(bound.penstock, SustainedPenstock)
    assert bound.penstock.rate_per_sec == 0.2


def test_resolve_unknown_host_falls_back_to_default() -> None:
    """Unknown host → SustainedPenstock(rate_per_sec=DEFAULT_RPS)."""
    bound = resolve_penstock("https://api.binance.us/api/v3/ticker/price")
    assert isinstance(bound.penstock, SustainedPenstock)
    assert bound.penstock.rate_per_sec == DEFAULT_RPS


def test_resolve_non_string_source_falls_back_to_default() -> None:
    """Non-string sources (None, Path, etc.) skip the host lookup."""
    bound = resolve_penstock(None)
    assert isinstance(bound.penstock, SustainedPenstock)
    assert bound.penstock.rate_per_sec == DEFAULT_RPS


def test_resolve_env_var_bypass_returns_null(monkeypatch: pytest.MonkeyPatch) -> None:
    """INCORPORATOR_RATE_LIMIT_BYPASS=1 forces NullPenstock everywhere."""
    monkeypatch.setitem(_HOST_PENSTOCKS, "api.example.com", SustainedPenstock(rate_per_sec=0.2))
    monkeypatch.setenv(_BYPASS_ENV_VAR, "1")
    # Even on a registered host with an explicit caller rate, the env var wins.
    assert isinstance(resolve_penstock("https://api.example.com/x").penstock, NullPenstock)
    assert isinstance(
        resolve_penstock("https://api.example.com/x", requests_per_second=5.0).penstock,
        NullPenstock,
    )
    assert isinstance(resolve_penstock("https://api.binance.us/x").penstock, NullPenstock)


def test_resolve_env_var_other_values_dont_bypass(monkeypatch: pytest.MonkeyPatch) -> None:
    """Only the exact string ``"1"`` triggers the bypass — guards against typos."""
    monkeypatch.setitem(_HOST_PENSTOCKS, "api.example.com", SustainedPenstock(rate_per_sec=0.2))
    for value in ("0", "true", "yes", ""):
        monkeypatch.setenv(_BYPASS_ENV_VAR, value)
        assert not isinstance(
            resolve_penstock("https://api.example.com/x").penstock, NullPenstock
        ), f"env value {value!r} should not bypass"


# ---------------------------------------------------------------------------
# register_host_penstock extension point
# ---------------------------------------------------------------------------


def test_register_adds_custom_penstock(monkeypatch: pytest.MonkeyPatch) -> None:
    """A user-registered host penstock wins over the default fallback."""
    sentinel_rate = 7.7
    monkeypatch.setitem(_HOST_PENSTOCKS, "test.example.com", SustainedPenstock(rate_per_sec=sentinel_rate))
    bound = resolve_penstock("https://test.example.com/api")
    assert isinstance(bound.penstock, SustainedPenstock)
    assert bound.penstock.rate_per_sec == sentinel_rate


def test_register_accepts_instance_directly(monkeypatch: pytest.MonkeyPatch) -> None:
    """``register_host_penstock`` accepts a Penstock instance (the canonical form)."""
    # Snapshot + restore so the global registry isn't polluted.
    monkeypatch.setattr(
        "incorporator.io.penstock._HOST_PENSTOCKS", dict(_HOST_PENSTOCKS), raising=False
    )
    register_host_penstock("acme.example.com", SustainedPenstock(rate_per_sec=3.3))
    bound = resolve_penstock("https://acme.example.com/x")
    assert isinstance(bound.penstock, SustainedPenstock)
    assert bound.penstock.rate_per_sec == 3.3


def test_register_accepts_factory_callable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Legacy zero-arg factory callable form is also accepted (back-compat)."""
    monkeypatch.setattr(
        "incorporator.io.penstock._HOST_PENSTOCKS", dict(_HOST_PENSTOCKS), raising=False
    )
    register_host_penstock("factory.example.com", lambda: SustainedPenstock(rate_per_sec=4.4))
    bound = resolve_penstock("https://factory.example.com/x")
    assert isinstance(bound.penstock, SustainedPenstock)
    assert bound.penstock.rate_per_sec == 4.4


def test_register_re_registration_replaces(monkeypatch: pytest.MonkeyPatch) -> None:
    """Re-registering an already-registered host replaces the previous penstock."""
    monkeypatch.setitem(_HOST_PENSTOCKS, "api.example.com", SustainedPenstock(rate_per_sec=0.5))
    monkeypatch.setitem(_HOST_PENSTOCKS, "api.example.com", SustainedPenstock(rate_per_sec=99.0))
    bound = resolve_penstock("https://api.example.com/x")
    assert isinstance(bound.penstock, SustainedPenstock)
    assert bound.penstock.rate_per_sec == 99.0


def test_top_level_import_surface() -> None:
    """``from incorporator import register_host_penstock`` works.

    Pins the v1.3.0 surface-promotion: users have one import path for
    the registration API rather than reaching into a submodule.
    """
    import incorporator

    assert hasattr(incorporator, "register_host_penstock")
    assert "register_host_penstock" in incorporator.__all__
    # Same callable as the penstock submodule.
    from incorporator.io.penstock import register_host_penstock as deep_ref

    assert incorporator.register_host_penstock is deep_ref


def test_default_registry_is_empty() -> None:
    """Fresh import: framework ships no implicit per-host throttling.

    Prior to v1.3.0 the throttle registry shipped pre-registered with
    ``api.coingecko.com`` / ``pokeapi.co`` / ``vpic.nhtsa.dot.gov``.
    The framework now ships penstock-agnostic; callers register hosts
    they care about explicitly.
    """
    from incorporator.io.penstock import _HOST_PENSTOCKS as live_registry

    # Sanity: the well-known historical hosts are NOT pre-registered.
    assert "api.coingecko.com" not in live_registry
    assert "pokeapi.co" not in live_registry
    assert "vpic.nhtsa.dot.gov" not in live_registry


def test_known_host_rates_returns_current_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """``known_host_rates()`` exposes whatever is currently registered."""
    monkeypatch.setitem(_HOST_PENSTOCKS, "api.coingecko.com", SustainedPenstock(rate_per_sec=0.2))
    monkeypatch.setitem(_HOST_PENSTOCKS, "pokeapi.co", SustainedPenstock(rate_per_sec=1.5))
    monkeypatch.setitem(_HOST_PENSTOCKS, "vpic.nhtsa.dot.gov", SustainedPenstock(rate_per_sec=1.5))
    rates = known_host_rates()
    assert rates["api.coingecko.com"] == 0.2
    assert rates["pokeapi.co"] == 1.5
    assert rates["vpic.nhtsa.dot.gov"] == 1.5


@pytest.mark.asyncio
async def test_resolver_returns_fresh_bound_per_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two calls for the same host return DIFFERENT BoundPenstock instances.

    Each fan-out leg needs its own state + lock so siblings don't share
    refill bookkeeping.  The penstock config can be shared, but state
    must not be.

    The lock is constructed eagerly on Python 3.10+ and lazily on 3.9
    (where ``asyncio.Lock()`` cannot run outside a coroutine).  Either
    way, after each leg awaits ``acquire()``, the locks must be
    per-instance — that is the runtime invariant the fan-out path
    relies on.
    """
    monkeypatch.setitem(_HOST_PENSTOCKS, "api.example.com", SustainedPenstock(rate_per_sec=0.2))
    a = resolve_penstock("https://api.example.com/x")
    b = resolve_penstock("https://api.example.com/x")
    assert a is not b, "resolve_penstock must return a fresh BoundPenstock"
    assert a.state is not b.state
    await a.acquire()
    await b.acquire()
    assert a.lock is not b.lock


# ---------------------------------------------------------------------------
# Per-host isolation in fan-out — the load-bearing capability
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fan_out_two_hosts_run_in_parallel_at_own_rates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two hosts in a fan-out each run at their own rate concurrently — not collapsed to min.

    Simulates two host-a-like requests and one host-b-like request with
    realistic intervals. Total wall-clock time must be near
    ``max(host_interval)`` (parallel), NOT ``sum(host_intervals)``
    (serial) and NOT ``min_rate * total_requests`` (the legacy collapse).
    """
    monkeypatch.setitem(_HOST_PENSTOCKS, "host-a.example.com", SustainedPenstock(rate_per_sec=20.0))  # 50ms
    monkeypatch.setitem(_HOST_PENSTOCKS, "host-b.example.com", SustainedPenstock(rate_per_sec=2.0))  # 500ms

    from urllib.parse import urlparse

    sources = [
        "https://host-a.example.com/1",
        "https://host-a.example.com/2",
        "https://host-b.example.com/1",
    ]
    by_host: dict = {}
    for src in sources:
        host = urlparse(src).hostname or ""
        if host not in by_host:
            by_host[host] = resolve_penstock(src)

    async def one_request(src: str) -> str:
        host = urlparse(src).hostname or ""
        bound = by_host[host]
        await bound.acquire()
        return src

    start = time.perf_counter()
    await asyncio.gather(*(one_request(s) for s in sources))
    elapsed = time.perf_counter() - start

    assert elapsed < 0.30, (
        f"fan-out across two hosts must run in parallel at each host's rate, "
        f"not collapsed to the minimum.  Elapsed {elapsed:.3f}s, expected < 0.3s."
    )
