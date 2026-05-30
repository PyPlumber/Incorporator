"""Baseline for the parses_as_* predicate family.

Chain β (DataKind value type + classify) will be benchmarked against these numbers.
"""

from __future__ import annotations

import time

import pytest

from incorporator.schema.converters import parses_as_datetime, parses_as_float, parses_as_int

# 10K mixed values: garbage sentinels, ints, floats, datetime strings, junk strings.
_SAMPLE: list[object] = (
    [None, "", "n/a", "unknown"] * 625
    + [42, "42", 0, -1] * 625
    + [3.14, "3.14", 1.0, "0.0"] * 625
    + ["2026-05-30T12:00:00Z", "2026-05-30", "2025-01-01T00:00:00Z", "2024-12-31"] * 500
    + ["hello", "world", "foo", "bar", "baz"] * 100
)
assert len(_SAMPLE) == 10_000

# N chosen so total runtime is ~1s per predicate on reference hardware.
N = 10


@pytest.mark.benchmark
def test_parses_as_datetime_baseline() -> None:
    """parses_as_datetime over 10K mixed values × 10 passes — baseline throughput."""
    t0 = time.perf_counter()
    for _ in range(N):
        for v in _SAMPLE:
            parses_as_datetime(v)
    elapsed = time.perf_counter() - t0
    total_values = N * len(_SAMPLE)
    us_per = elapsed / total_values * 1e6
    print(f"\n  parses_as_datetime {total_values:,} calls: {elapsed:.3f}s = {us_per:.2f} µs/value")


@pytest.mark.benchmark
def test_parses_as_int_baseline() -> None:
    """parses_as_int over 10K mixed values × 10 passes — baseline throughput."""
    t0 = time.perf_counter()
    for _ in range(N):
        for v in _SAMPLE:
            parses_as_int(v)
    elapsed = time.perf_counter() - t0
    total_values = N * len(_SAMPLE)
    us_per = elapsed / total_values * 1e6
    print(f"\n  parses_as_int {total_values:,} calls: {elapsed:.3f}s = {us_per:.2f} µs/value")


@pytest.mark.benchmark
def test_parses_as_float_baseline() -> None:
    """parses_as_float over 10K mixed values × 10 passes — baseline throughput."""
    t0 = time.perf_counter()
    for _ in range(N):
        for v in _SAMPLE:
            parses_as_float(v)
    elapsed = time.perf_counter() - t0
    total_values = N * len(_SAMPLE)
    us_per = elapsed / total_values * 1e6
    print(f"\n  parses_as_float {total_values:,} calls: {elapsed:.3f}s = {us_per:.2f} µs/value")
