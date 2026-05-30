"""Baseline for the per-row dot-path resolution hot loop.

Chain β (DataPath value type) will be benchmarked against these numbers.
"""

from __future__ import annotations

import time

import pytest

from incorporator.schema.extractors import _drill_path

N = 100_000


@pytest.mark.benchmark
def test_drill_path_depth1_baseline() -> None:
    """100K _drill_path calls at depth-1 — single dict key lookup."""
    record = {"a": "x"}
    t0 = time.perf_counter()
    for _ in range(N):
        _drill_path(record, "a")
    elapsed = time.perf_counter() - t0
    us_per = elapsed / N * 1e6
    print(f"\n  _drill_path depth-1 {N:,}: {elapsed:.3f}s = {us_per:.2f} µs/op")


@pytest.mark.benchmark
def test_drill_path_depth3_baseline() -> None:
    """100K _drill_path calls at depth-3 — three nested dict keys."""
    record = {"a": {"b": {"c": "x"}}}
    t0 = time.perf_counter()
    for _ in range(N):
        _drill_path(record, "a.b.c")
    elapsed = time.perf_counter() - t0
    us_per = elapsed / N * 1e6
    print(f"\n  _drill_path depth-3 {N:,}: {elapsed:.3f}s = {us_per:.2f} µs/op")


@pytest.mark.benchmark
def test_drill_path_depth5_baseline() -> None:
    """100K _drill_path calls at depth-5 — mixed dict/list/dict navigation."""
    record = {"a": {"b": [{"c": {"d": "x"}}]}}
    t0 = time.perf_counter()
    for _ in range(N):
        _drill_path(record, "a.b.0.c.d")
    elapsed = time.perf_counter() - t0
    us_per = elapsed / N * 1e6
    print(f"\n  _drill_path depth-5 {N:,}: {elapsed:.3f}s = {us_per:.2f} µs/op")
