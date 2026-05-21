"""Pluggable throttle strategies for outbound HTTP volume control.

The :class:`ThrottleStrategy` Protocol defines a single ``async acquire``
method that callers must await before issuing a request. Three built-in
implementations cover the common cases:

- :class:`NullThrottle` — no-op; for trusted internal sources, local
  files, test mocks, or callers who manage throughput externally.
- :class:`FixedIntervalThrottle` — enforces a minimum interval between
  requests. The original ``RateLimiter`` behaviour, kept as the default
  for unknown hosts and explicit ``requests_per_second`` callers.
- :class:`BurstThrottle` — token bucket with capacity ``burst`` refilled
  at ``requests_per_second``. Allows an initial burst then steady-state
  throttling; useful when an API publishes a documented burst window.

Strategies are picked per-source via :func:`resolve_throttle`, which
honours (in order): the ``INCORPORATOR_RATE_LIMIT_BYPASS`` env var,
caller-supplied ``requests_per_second`` (with ``0`` meaning "no
throttle"), the per-host registry (:data:`_HOST_FACTORIES`), and the
documented :data:`DEFAULT_RPS` of 15 requests/sec.

Within a fan-out the resolver returns one strategy *per source*, so a
mixed-host call (e.g. CoinGecko + PokeAPI) runs each host at its own
rate instead of collapsing to the minimum — the legacy behaviour.

Users with in-house APIs can register their own strategies via
:func:`register_host_throttle`.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, Callable, Dict, Optional, Protocol, runtime_checkable
from urllib.parse import urlparse

# ==========================================
# 1. STRATEGY PROTOCOL
# ==========================================


@runtime_checkable
class ThrottleStrategy(Protocol):
    """Acquire one slot before issuing an outbound request.

    Implementations may sleep, lock, or no-op — the only contract is
    that the awaitable resolves when the caller is permitted to send.
    """

    async def acquire(self) -> None: ...


# ==========================================
# 2. CONCRETE STRATEGIES
# ==========================================


class NullThrottle:
    """No-op throttle; ``acquire()`` returns immediately.

    Use for trusted internal sources, local files, test mocks, or
    on-prem mirrors where the caller already controls volume. Selected
    by :func:`resolve_throttle` when ``requests_per_second <= 0`` or when
    the ``INCORPORATOR_RATE_LIMIT_BYPASS`` env var is set to ``"1"``.
    """

    __slots__ = ()

    async def acquire(self) -> None:  # noqa: D401 — Protocol method
        """Return immediately — no throttling applied."""
        return None


class FixedIntervalThrottle:
    """Enforce a minimum interval between requests.

    Equivalent to a token bucket of capacity 1 refilled at
    ``requests_per_second``: every ``acquire()`` waits until at least
    ``1 / requests_per_second`` seconds have elapsed since the last call.

    This is the original :class:`RateLimiter` behaviour, kept as the
    fallback for unknown hosts and the default for explicit
    ``requests_per_second`` callers.

    Args:
        requests_per_second: Positive float. Pass ``0`` to opt out via
            :class:`NullThrottle` instead.

    Raises:
        ValueError: If ``requests_per_second`` is not positive — callers
            should use :class:`NullThrottle` for the no-throttle case
            instead of relying on a degenerate :class:`FixedIntervalThrottle`.
    """

    __slots__ = ("rate", "interval", "_lock", "_last_call")

    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise ValueError(
                f"FixedIntervalThrottle requires requests_per_second > 0 "
                f"(got {requests_per_second}); use NullThrottle for no throttle."
            )
        self.rate = requests_per_second
        self.interval = 1.0 / requests_per_second
        self._lock = asyncio.Lock()
        self._last_call = 0.0

    async def acquire(self) -> None:  # noqa: D401 — Protocol method
        """Block until the minimum inter-request interval has elapsed."""
        async with self._lock:
            now = asyncio.get_running_loop().time()
            elapsed = now - self._last_call
            if elapsed < self.interval:
                await asyncio.sleep(self.interval - elapsed)
            self._last_call = asyncio.get_running_loop().time()


class BurstThrottle:
    """Token bucket with explicit burst capacity.

    The bucket starts full at ``burst`` tokens and refills at
    ``requests_per_second``. ``acquire()`` consumes one token and waits
    only when the bucket is empty. This is the right shape when an API
    publishes a documented burst allowance ("100 requests then 10/min").

    Args:
        requests_per_second: Steady-state refill rate. Must be positive.
        burst: Bucket capacity; the maximum number of requests that can
            fire back-to-back before throttling kicks in. Must be >= 1.

    Raises:
        ValueError: On non-positive ``requests_per_second`` or
            ``burst < 1``.
    """

    __slots__ = ("rate", "capacity", "_tokens", "_last_refill", "_lock")

    def __init__(self, requests_per_second: float, burst: int) -> None:
        if requests_per_second <= 0 or burst < 1:
            raise ValueError(
                f"BurstThrottle requires requests_per_second > 0 and burst >= 1 "
                f"(got requests_per_second={requests_per_second}, burst={burst})."
            )
        self.rate = requests_per_second
        self.capacity = float(burst)
        self._tokens = float(burst)
        self._last_refill = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:  # noqa: D401 — Protocol method
        """Consume one token, waiting only when the bucket is empty."""
        async with self._lock:
            now = asyncio.get_running_loop().time()
            elapsed = now - self._last_refill
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            self._last_refill = now
            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / self.rate
                await asyncio.sleep(wait)
                self._tokens = 0.0
                self._last_refill = asyncio.get_running_loop().time()
            else:
                self._tokens -= 1.0


# ==========================================
# 3. PER-HOST REGISTRY + RESOLVER
# ==========================================


#: Per-host strategy factories — keyed by lowercase hostname.  **Empty by
#: default**: the framework ships with no implicit per-host throttling.
#: Use :func:`register_host_throttle` to attach a strategy for any host
#: that requires one; the factory is called once per :func:`resolve_throttle`
#: invocation so each fan-out leg gets independent state.
#:
#: Migration from v1.2.0 implicit hosts — register these at startup if you
#: relied on them:
#:
#: .. code-block:: python
#:
#:     from incorporator import register_host_throttle
#:     from incorporator.io.throttle import FixedIntervalThrottle
#:
#:     # CoinGecko public (anon): 5–15 req/min — 0.2 = 12 req/min, headroom.
#:     register_host_throttle("api.coingecko.com", lambda: FixedIntervalThrottle(0.2))
#:     # PokeAPI: 100 req/min documented ceiling — 1.5 = 90 req/min.
#:     register_host_throttle("pokeapi.co", lambda: FixedIntervalThrottle(1.5))
#:     # NHTSA vPIC: 100–200 req/min documented; method-agnostic.
#:     register_host_throttle("vpic.nhtsa.dot.gov", lambda: FixedIntervalThrottle(1.5))
_HOST_FACTORIES: Dict[str, Callable[[], ThrottleStrategy]] = {}


DEFAULT_RPS: float = 15.0
"""Throttle rate used when no host match and no caller-supplied rate."""


_BYPASS_ENV_VAR: str = "INCORPORATOR_RATE_LIMIT_BYPASS"
"""Set to ``"1"`` to force :class:`NullThrottle` everywhere — test-only."""


def register_host_throttle(host: str, factory: Callable[[], ThrottleStrategy]) -> None:
    """Register a per-host throttle factory.

    Use this to attach a strategy to an in-house API or to override a
    built-in entry.  The factory is called once per
    :func:`resolve_throttle` invocation so each fan-out leg gets an
    independent throttle instance.

    Args:
        host: Lowercase hostname (e.g. ``"api.internal.acme.com"``).
        factory: Zero-arg callable returning a fresh
            :class:`ThrottleStrategy`.

    Example::

        from incorporator.io.throttle import (
            BurstThrottle,
            register_host_throttle,
        )

        register_host_throttle(
            "api.internal.acme.com",
            lambda: BurstThrottle(requests_per_second=50.0, burst=200),
        )
    """
    _HOST_FACTORIES[host] = factory


def resolve_throttle(
    source: Any,
    *,
    requests_per_second: Optional[float] = None,
    burst: Optional[int] = None,
) -> ThrottleStrategy:
    """Pick a throttle strategy for one source URL.

    Precedence:

    1. ``INCORPORATOR_RATE_LIMIT_BYPASS=1`` env var → :class:`NullThrottle`
       (the global test-friendly escape hatch).
    2. Caller-supplied ``requests_per_second <= 0`` → :class:`NullThrottle`
       (the documented per-call escape hatch).
    3. Caller-supplied positive ``requests_per_second`` → either
       :class:`FixedIntervalThrottle` or :class:`BurstThrottle` if
       ``burst`` was also passed.
    4. Per-host registry match → the registered factory's strategy.
    5. Fallback → ``FixedIntervalThrottle(DEFAULT_RPS)`` (15 req/sec).

    Args:
        source: A URL string (or any value with a hostname extractable
            via ``urllib.parse``).  Non-string sources skip the host
            lookup and use the default.
        requests_per_second: Caller override.  ``0`` or negative selects
            :class:`NullThrottle`.
        burst: Optional burst capacity; if supplied alongside a positive
            ``requests_per_second``, returns :class:`BurstThrottle`.

    Returns:
        A fresh :class:`ThrottleStrategy` instance suitable for one
        source's :class:`ThrottleStrategy.acquire` calls.
    """
    if os.environ.get(_BYPASS_ENV_VAR) == "1":
        return NullThrottle()
    if requests_per_second is not None:
        if requests_per_second <= 0:
            return NullThrottle()
        if burst is not None:
            return BurstThrottle(requests_per_second, burst)
        return FixedIntervalThrottle(requests_per_second)
    if isinstance(source, str):
        host = urlparse(source).hostname or ""
        factory = _HOST_FACTORIES.get(host)
        if factory is not None:
            return factory()
    return FixedIntervalThrottle(DEFAULT_RPS)


# ==========================================
# 4. DIAGNOSTICS HELPERS (backward compat)
# ==========================================


def known_host_rates() -> Dict[str, float]:
    """Return ``host → rate`` for every host in the registry.

    Intended for diagnostics, logging, and tests that want to assert
    the registered rate without instantiating the factory.  Only
    :class:`FixedIntervalThrottle` and :class:`BurstThrottle` expose a
    ``rate`` attribute; custom strategies without one are skipped.
    """
    rates: Dict[str, float] = {}
    for host, factory in _HOST_FACTORIES.items():
        strategy = factory()
        rate = getattr(strategy, "rate", None)
        if isinstance(rate, (int, float)):
            rates[host] = float(rate)
    return rates
