"""Baseline for the inspector's `capture_signals` end-to-end probe.

Chain β (DataKind + classify routing) will be benchmarked against these numbers.
"""

from __future__ import annotations

import time

import pytest

from incorporator.tools.inspector import capture_signals

# 1000-row mixed-type fixture: string IDs, ints, floats, ISO datetime strings,
# URLs, garbage sentinels, and occasional nested dicts.
_FIXTURE: list[dict[str, object]] = [
    {
        "id": f"item-{i}",
        "count": i,
        "score": round(i * 0.123, 3),
        "created_at": "2026-05-30T12:00:00Z" if i % 5 != 0 else None,
        "url": f"https://api.example.com/items/{i}" if i % 7 != 0 else "n/a",
        "status": "active" if i % 3 != 0 else "unknown",
        "ratio": f"{i / 1000:.4f}" if i % 4 != 0 else "",
        "meta": {"source": "test", "rank": i % 100} if i % 10 == 0 else None,
    }
    for i in range(1000)
]

N = 100


@pytest.mark.benchmark
def test_inspector_capture_signals_baseline() -> None:
    """capture_signals on 1000-row fixture × 100 calls — baseline end-to-end probe throughput."""
    t0 = time.perf_counter()
    for _ in range(N):
        capture_signals(_FIXTURE, {})
    elapsed = time.perf_counter() - t0
    ms_per = elapsed / N * 1e3
    print(f"\n  capture_signals {N} calls (1000 rows each): {elapsed:.3f}s = {ms_per:.2f} ms/call")
