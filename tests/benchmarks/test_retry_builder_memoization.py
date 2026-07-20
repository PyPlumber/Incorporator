"""Benchmark: memoized per-method retry stop/wait builders.

``execute_request`` (``io/fetch.py``) built a fresh ``_make_http_stop`` /
``_make_http_wait`` closure pair on EVERY call, even though both are pure
functions of the HTTP method string and there are at most a handful of
distinct verbs (``_IDEMPOTENT_METHODS`` enumerates six). ``_make_http_wait``
in particular constructs two ``wait_random_exponential`` instances per call.

The fix decorates both builders with ``functools.lru_cache`` keyed on the
uppercased method string, so repeat calls for the same verb return the
already-built closures instead of re-constructing them. This benchmark times
N cold (unique-method, cache-miss) builds against N warm (repeated-method,
cache-hit) builds to produce the measured delta cited in the commit message.

The ``AsyncRetrying`` object itself is NOT memoized (see the comment above
``_make_http_stop`` in ``io/fetch.py``) — this benchmark only exercises the
two builder functions, matching the actual optimization's scope.
"""

from __future__ import annotations

import time

import pytest

from incorporator.io.fetch import _make_http_stop, _make_http_wait

N = 100_000


def _report(label: str, elapsed: float, n: int) -> None:
    """Print elapsed seconds and per-call microseconds for commit-message evidence."""
    print(f"\n  {label:<28} {elapsed * 1000:.1f} ms total, {elapsed / n * 1e6:.3f} us/call")


@pytest.mark.benchmark
def test_warm_lookup_faster_than_cold_construct() -> None:
    """Repeated (cache-hit) builder calls are faster than always-unique (cache-miss) calls.

    Cold path clears the ``lru_cache`` before every call so each of the N
    iterations misses and rebuilds; warm path calls with a fixed method
    string so every iteration after the first hits the cache.
    """
    t0 = time.perf_counter()
    for _ in range(N):
        _make_http_stop.cache_clear()
        _make_http_wait.cache_clear()
        _make_http_stop("GET")
        _make_http_wait("GET")
    cold_elapsed = time.perf_counter() - t0
    _report("cold (cache cleared each call)", cold_elapsed, N)

    _make_http_stop.cache_clear()
    _make_http_wait.cache_clear()
    t0 = time.perf_counter()
    for _ in range(N):
        _make_http_stop("GET")
        _make_http_wait("GET")
    warm_elapsed = time.perf_counter() - t0
    _report("warm (memoized lookup)", warm_elapsed, N)

    speedup = cold_elapsed / warm_elapsed if warm_elapsed > 0 else float("inf")
    print(f"\n  speedup: {speedup:.1f}x")

    assert warm_elapsed < cold_elapsed, (
        f"warm lookup ({warm_elapsed * 1000:.2f}ms) was not faster than cold construct "
        f"({cold_elapsed * 1000:.2f}ms) — memoization isn't taking effect."
    )
