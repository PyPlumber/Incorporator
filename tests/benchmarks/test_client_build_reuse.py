"""Benchmark: shared default verify ``SSLContext`` across client builds.

``HTTPClientBuilder.build_client`` (``io/fetch.py``) used to pass
``verify=not ignore_ssl`` straight into ``httpx.AsyncClient(...)``. With
``verify=True``, httpx builds a fresh default ``SSLContext`` and loads the
certifi CA bundle SYNCHRONOUSLY, on the event loop, on every single call —
measured at ~1.1s vs ~54ms for ``ignore_ssl=True`` (2026-07-06, 21x). Every
bare ``incorp()``/``refresh()``/``test()``/``architect()`` call that doesn't
inject a ``_client`` pays this in full, every time.

The fix caches the built ``SSLContext`` at module scope in ``io/fetch.py``
and reuses it across every subsequent ``build_client()`` call with
``ignore_ssl=False`` (the default). This benchmark proves the win directly:
the first (cold-cache) build still pays the real cert-bundle I/O, the
second (warm-cache) build should be near-instant.
"""

from __future__ import annotations

import ssl
import time

import pytest

import incorporator.io.fetch as fetch
from incorporator.io.fetch import HTTPClientBuilder

# Warm-build ceiling: two orders of magnitude under the measured ~1.1s cold
# cost, with generous headroom above a typical <1ms warm build so this
# doesn't flake on a loaded CI runner.
WARM_BUILD_CEILING_SEC = 0.05


def _report(label: str, elapsed: float) -> None:
    """Print elapsed seconds for a client build, for commit-message evidence."""
    print(f"\n  {label:<24} {elapsed * 1000:.1f} ms")


@pytest.mark.benchmark
async def test_second_client_build_reuses_cached_ssl_context() -> None:
    """Second ``build_client()`` call is near-instant thanks to the shared context.

    Resets the module's private ``_default_ssl_context`` cache to ``None``
    to simulate a true cold start, then times two consecutive
    ``verify=True`` (default) builds. The first build is informational only
    — it does real cert-bundle I/O and its cost varies by machine/CI load.
    The second build is asserted against a hard ceiling: it must reuse the
    now-warm cached context rather than rebuilding it.
    """
    fetch._default_ssl_context = None

    t0 = time.perf_counter()
    client_a = HTTPClientBuilder.build_client()
    first_elapsed = time.perf_counter() - t0
    await client_a.aclose()
    _report("first build (cold cache)", first_elapsed)

    t0 = time.perf_counter()
    client_b = HTTPClientBuilder.build_client()
    second_elapsed = time.perf_counter() - t0
    await client_b.aclose()
    _report("second build (warm cache)", second_elapsed)

    speedup = first_elapsed / second_elapsed if second_elapsed > 0 else float("inf")
    print(f"\n  speedup: {speedup:.1f}x")

    assert second_elapsed < WARM_BUILD_CEILING_SEC, (
        f"warm build took {second_elapsed * 1000:.1f}ms (ceiling: {WARM_BUILD_CEILING_SEC * 1000:.0f}ms). "
        "Suggests the shared SSLContext cache in io/fetch.py isn't being reused."
    )
    assert isinstance(fetch._default_ssl_context, ssl.SSLContext)
