"""Baseline for the per-row dot-path resolution hot loop.

Chain α (old): parse + resolve on every call (_drill_path).
Chain β (new): parse once, resolve N times (DataPath.parse + .resolve).

Both parse cost and per-resolve cost are reported separately so the
comparison is honest: the Reviewer gates on per-resolve time (the hot path),
not the amortised parse cost.
"""

from __future__ import annotations

import time

import pytest

from incorporator.schema.path import DataPath

N = 100_000


@pytest.mark.benchmark
def test_drill_path_depth1_baseline() -> None:
    """100K DataPath.resolve calls at depth-1 — single dict key lookup.

    Parse cost is reported separately; the gate applies to resolve time.
    """
    record = {"a": "x"}
    _path = DataPath.parse("a")
    # Measure parse cost in isolation (one-shot, not in loop).
    t_parse0 = time.perf_counter()
    DataPath.parse("a")
    parse_us = (time.perf_counter() - t_parse0) * 1e6

    t0 = time.perf_counter()
    for _ in range(N):
        _path.resolve(record)
    elapsed = time.perf_counter() - t0
    us_per = elapsed / N * 1e6
    print(f"\n  DataPath depth-1 {N:,}: resolve {elapsed:.3f}s = {us_per:.2f} µs/op  |  parse = {parse_us:.2f} µs")


@pytest.mark.benchmark
def test_drill_path_depth3_baseline() -> None:
    """100K DataPath.resolve calls at depth-3 — three nested dict keys.

    Parse cost is reported separately; the gate applies to resolve time.
    """
    record = {"a": {"b": {"c": "x"}}}
    _path = DataPath.parse("a.b.c")
    t_parse0 = time.perf_counter()
    DataPath.parse("a.b.c")
    parse_us = (time.perf_counter() - t_parse0) * 1e6

    t0 = time.perf_counter()
    for _ in range(N):
        _path.resolve(record)
    elapsed = time.perf_counter() - t0
    us_per = elapsed / N * 1e6
    print(f"\n  DataPath depth-3 {N:,}: resolve {elapsed:.3f}s = {us_per:.2f} µs/op  |  parse = {parse_us:.2f} µs")


@pytest.mark.benchmark
def test_drill_path_depth5_baseline() -> None:
    """100K DataPath.resolve calls at depth-5 — mixed dict/list/dict navigation.

    Parse cost is reported separately; the gate applies to resolve time.
    """
    record = {"a": {"b": [{"c": {"d": "x"}}]}}
    _path = DataPath.parse("a.b.0.c.d")
    t_parse0 = time.perf_counter()
    DataPath.parse("a.b.0.c.d")
    parse_us = (time.perf_counter() - t_parse0) * 1e6

    t0 = time.perf_counter()
    for _ in range(N):
        _path.resolve(record)
    elapsed = time.perf_counter() - t0
    us_per = elapsed / N * 1e6
    print(f"\n  DataPath depth-5 {N:,}: resolve {elapsed:.3f}s = {us_per:.2f} µs/op  |  parse = {parse_us:.2f} µs")
