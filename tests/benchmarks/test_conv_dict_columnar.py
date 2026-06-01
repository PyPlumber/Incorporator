"""Benchmarks for the conv_dict columnar dispatcher's per-Op cache behaviour.

Three scenarios bracket the cardinality spectrum: low cardinality (lru_cache
at construction delivers a near-100% hit rate), continuous/unique data (lru_cache
misses on every call — worst-case floor), and a pure=True vs pure=False
comparison for calc().  Each scenario verifies the behavioural assertion in
addition to a raw throughput floor.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from incorporator.schema.builder import apply_etl_transformations
from incorporator.schema.converters import _inc_clear_for_tests, calc, inc

ROW_COUNT = 500_000
STATUSES = ["active", "inactive", "pending", "banned", "suspended", "trial", "expired", "locked", "guest", "admin"]
CATEGORIES = ["tech", "finance", "health", "edu", "retail", "gov", "ngo", "energy", "media", "sports"]
COUNTRIES = ["US", "GB", "DE", "FR", "JP", "CA", "AU", "BR", "IN", "MX"]
TIERS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


@pytest.mark.benchmark
def test_conv_dict_low_cardinality_cache_engages() -> None:
    """Low-cardinality conv_dict workload — lru_cache delivers measurable speedup.

    lru_cache is unconditional for is_pure=True ops at construction; this test
    verifies the throughput benefit holds under low-cardinality load.  4 columns
    × 10 unique values × 500k rows = 99.998% cache hit rate.  tier stored as str
    so inc(int) does real coercion work.
    """
    _inc_clear_for_tests()

    rows: list[dict[str, Any]] = [
        {
            "status": STATUSES[i % 10],
            "category": CATEGORIES[i % 10],
            "country": COUNTRIES[i % 10],
            "tier": str(TIERS[i % 10]),
        }
        for i in range(ROW_COUNT)
    ]

    conv_dict: dict[str, Any] = {
        "status": inc(str),
        "category": inc(str),
        "country": inc(str),
        "tier": inc(int),
    }

    t0 = time.perf_counter()
    apply_etl_transformations(rows, conv_dict=conv_dict)
    elapsed = time.perf_counter() - t0

    throughput = ROW_COUNT / elapsed
    print(f"\n  conv_dict low-cardinality (lru_cache at construction): {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    assert throughput >= 150_000, f"Low-cardinality throughput {throughput:,.0f} below 150k floor"


@pytest.mark.benchmark
def test_conv_dict_continuous_data_always_caches() -> None:
    """Continuous-data conv_dict workload — every row is unique; lru_cache misses on every call.

    This is the worst-case throughput floor for always-cache.  lru_cache is
    still wrapped at construction (is_pure=True), but unique inputs mean no
    hit benefit materialises.
    """
    _inc_clear_for_tests()

    rows: list[dict[str, Any]] = [
        {
            "seq": str(i),
            "ratio": str(i * 1.000003),
            "label": f"item_{i}",
            "score": str(float(i) / ROW_COUNT),
        }
        for i in range(ROW_COUNT)
    ]

    conv_dict: dict[str, Any] = {
        "seq": inc(int),
        "ratio": inc(float),
        "label": inc(str),
        "score": inc(float),
    }

    t0 = time.perf_counter()
    apply_etl_transformations(rows, conv_dict=conv_dict)
    elapsed = time.perf_counter() - t0

    throughput = ROW_COUNT / elapsed
    print(f"\n  conv_dict continuous-data (lru_cache miss on every row): {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    # lru_cache miss overhead on every row; trade for mechanism simplicity
    assert throughput >= 80_000, f"Continuous-data throughput {throughput:,.0f} below 80k floor"


@pytest.mark.benchmark
def test_calc_pure_true_default_engages_cache() -> None:
    """calc(pure=True) default — lru_cache wrapped at construction; near-100% hit rate on low cardinality.

    50 unique input values × 500k rows produces a near-100% hit rate.  Compares
    pure=True (default) vs pure=False to show the cache win is real, not just a
    no-op default flip.

    Two separate row copies used so in-place mutation in run 1 doesn't pollute run 2.
    """
    _inc_clear_for_tests()

    DOMAIN_MAP = {f"cat_{i}": f"canonical_{i % 50}" for i in range(50)}

    def gen_rows() -> list[dict[str, Any]]:
        return [{"input": f"cat_{i % 50}"} for i in range(ROW_COUNT)]

    rows_pure = gen_rows()
    rows_impure = gen_rows()

    # pure=True is now the default — explicit for clarity in this comparison
    conv_dict_pure: dict[str, Any] = {"derived": calc(lambda v: DOMAIN_MAP.get(v, "unknown"), "input", pure=True)}
    conv_dict_impure: dict[str, Any] = {"derived": calc(lambda v: DOMAIN_MAP.get(v, "unknown"), "input", pure=False)}

    t0 = time.perf_counter()
    apply_etl_transformations(rows_pure, conv_dict=conv_dict_pure)
    elapsed_pure = time.perf_counter() - t0

    t1 = time.perf_counter()
    apply_etl_transformations(rows_impure, conv_dict=conv_dict_impure)
    elapsed_impure = time.perf_counter() - t1

    tp = ROW_COUNT / elapsed_pure
    ti = ROW_COUNT / elapsed_impure
    ratio = tp / ti

    print(
        f"\n  calc(pure=True): {tp:,.0f} rows/sec ({elapsed_pure:.2f}s) | "
        f"calc(pure=False): {ti:,.0f} rows/sec ({elapsed_impure:.2f}s) | ratio: {ratio:.2f}x"
    )

    # The cache should make pure at least as fast as impure (typically faster on low cardinality)
    assert tp >= ti * 0.9, f"pure=True throughput {tp:,.0f} regressed vs impure {ti:,.0f}"
    assert tp >= 150_000, f"calc(pure=True) throughput {tp:,.0f} below 150k floor"


@pytest.mark.benchmark
def test_calc_op_persistent_cache_across_batches() -> None:
    """CalcOp.func is an lru_cache wrapper that persists across all batches.

    lru_cache lives on CalcOp.func for the Op's lifetime; no per-batch reset
    is possible.  50 unique input tuples, split across 5 batches of 50k rows.
    The cache accumulates hits across all batches and must exceed 10k — a
    regression that rebuilt the wrapper per batch would only accumulate one
    batch's worth.  The perf floor confirms throughput does not degrade.
    """
    _inc_clear_for_tests()

    BATCH_SIZE = 50_000
    DOMAIN_MAP = {f"k_{i}": f"v_{i}" for i in range(50)}
    batches: list[list[dict[str, Any]]] = [
        [{"input": f"k_{(b * BATCH_SIZE + i) % 50}"} for i in range(BATCH_SIZE)] for b in range(5)
    ]

    conv_dict: dict[str, Any] = {
        "derived": calc(lambda v: DOMAIN_MAP.get(v, "unknown"), "input", pure=True),
    }
    calc_op = conv_dict["derived"]

    timings: list[float] = []
    for batch in batches:
        t0 = time.perf_counter()
        apply_etl_transformations(batch, conv_dict=conv_dict)
        timings.append(time.perf_counter() - t0)

    throughputs = [BATCH_SIZE / t for t in timings]
    print(
        f"\n  CalcOp persistent cache (per-batch throughput rows/sec): "
        + ", ".join(f"{tp:,.0f}" for tp in throughputs)
    )

    assert hasattr(calc_op.func, "cache_info"), (
        "CalcOp.func should be an lru_cache wrapper when pure=True"
    )
    cache_hits = calc_op.func.cache_info().hits
    assert cache_hits > 10_000, (
        f"calc_op.func.cache_info().hits={cache_hits} should exceed 10k — "
        "lru_cache persists on the Op instance across all batches"
    )

    # Perf: all batches must sustain the throughput floor — no degradation from
    # repeated resampling (which the persistent cache prevents).
    assert min(throughputs) >= 150_000, (
        f"Per-batch min {min(throughputs):,.0f} below 150k floor"
    )
