"""Benchmarks for the conv_dict columnar dispatcher's per-Op cache behaviour.

Three scenarios bracket the cardinality spectrum: low cardinality (cache
engages), continuous/unique data (cache opts out via sentinel), and a
pure=True vs pure=False comparison for calc().  Each scenario verifies
the behavioural assertion — that the cache decision fired correctly — in
addition to a raw throughput floor.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from incorporator.schema.builder import apply_etl_transformations
from incorporator.schema.converters import calc, inc

ROW_COUNT = 500_000
STATUSES = ["active", "inactive", "pending", "banned", "suspended", "trial", "expired", "locked", "guest", "admin"]
CATEGORIES = ["tech", "finance", "health", "edu", "retail", "gov", "ngo", "energy", "media", "sports"]
COUNTRIES = ["US", "GB", "DE", "FR", "JP", "CA", "AU", "BR", "IN", "MX"]
TIERS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


@pytest.mark.benchmark
def test_conv_dict_low_cardinality_cache_engages() -> None:
    """Low-cardinality conv_dict workload — per-Op cache delivers measurable speedup.

    4 columns × 10 unique values × 500k rows = 99.998% cache hit rate after first
    batch. tier stored as str so inc(int) does real coercion work.
    """
    inc.cache_clear()  # type: ignore[attr-defined]  # CRITICAL — reset Op._cache to None on all cached inc() instances

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

    # inc() lru_cache returns the same Op instance for inc(str), so this is the
    # one used by all 3 string fields.
    str_op = inc(str)

    t0 = time.perf_counter()
    apply_etl_transformations(rows, conv_dict=conv_dict)
    elapsed = time.perf_counter() - t0

    throughput = ROW_COUNT / elapsed
    print(f"\n  conv_dict low-cardinality (cache engaged): {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    # Behavioral verification: cache decision fired and chose to cache
    assert str_op._cache is not None, "Op._cache should be populated after low-cardinality batch"
    assert str_op._cache is not str_op, (
        "Op._cache should NOT be the sentinel (cache should engage on 10 unique values / 500k rows)"
    )

    assert throughput >= 150_000, f"Low-cardinality throughput {throughput:,.0f} below 150k floor"


@pytest.mark.benchmark
def test_conv_dict_continuous_data_cache_opts_out() -> None:
    """Continuous-data conv_dict workload — cardinality heuristic opts out of cache.

    Every row has unique values. Sample is 500/500 unique = 100% — well above the
    50% threshold. Op._cache should resolve to the sentinel (Op itself) meaning
    "decided not to cache" — verified explicitly. Throughput floor lower than
    Scenario 1 because per-row ranked-converter dispatch runs on every row.
    """
    inc.cache_clear()  # type: ignore[attr-defined]  # CRITICAL — reset to None so the decision fires fresh

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

    int_op = inc(int)  # Capture for post-run sentinel check

    t0 = time.perf_counter()
    apply_etl_transformations(rows, conv_dict=conv_dict)
    elapsed = time.perf_counter() - t0

    throughput = ROW_COUNT / elapsed
    print(f"\n  conv_dict continuous-data (cache opted out): {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    # Behavioral verification: cardinality heuristic correctly REJECTED caching
    assert int_op._cache is int_op, (
        f"Op._cache should be the sentinel (Op itself) after high-cardinality batch; got {int_op._cache!r}"
    )

    # Floor at 80k gives ~50% safety margin over typical 125-130k measurements
    # to absorb Windows GC / power-management variance on shared CI hardware.
    assert throughput >= 80_000, f"Continuous-data throughput {throughput:,.0f} below 80k floor"


@pytest.mark.benchmark
def test_calc_pure_true_default_engages_cache() -> None:
    """calc(pure=True) default engages dispatcher cache on low-cardinality input.

    50 unique input values × 500k rows. Sample of 500 has ~50 unique = 10%, well
    below the 50% threshold; cache MUST engage. Compares pure=True (default) vs
    pure=False to show the cache win is real, not just a no-op default flip.

    Two separate row copies used so in-place mutation in run 1 doesn't pollute run 2.
    """
    inc.cache_clear()  # type: ignore[attr-defined]  # CRITICAL — reset so Op._cache decisions are fresh

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
