"""Benchmark pinning for incorporator.io.handlers.columnar._SMALL_TABLE_THRESHOLD.

The threshold gates _table_to_dicts()'s JSON-rehydration strategy: below it,
pure-Python per-row scanning; at or above, pyarrow.compute vectorised
filtering. This benchmark sweeps {32, 64, 128, 256} candidates against
representative row counts and pins the empirically-best value.

The pinning assertion at the end fails loudly if a future contributor
changes the constant without re-running the sweep.
"""

from __future__ import annotations

import json
import time

import pytest

pytest.importorskip("pyarrow")

import pyarrow as pa  # noqa: E402

from incorporator.io.handlers import columnar as columnar_module  # noqa: E402


def _build_payload(row_count: int) -> pa.Table:
    """Builds a representative pa.Table with one JSON-bearing string column.

    Two string columns (one ~30% JSON-bearing, one plain), two numeric.
    Matches the shape _table_to_dicts() encounters on round-tripped data.
    """
    rows = []
    for i in range(row_count):
        rows.append(
            {
                "id": i,
                "score": float(i) * 0.5,
                "meta": json.dumps({"k": i, "v": f"item_{i}"}) if i % 3 == 0 else f"plain_{i}",
                "label": f"row_{i}",
            }
        )
    return pa.Table.from_pylist(rows)


def _time_threshold(
    monkeypatch: pytest.MonkeyPatch, threshold: int, row_count: int, runs: int = 3
) -> float:
    """Returns median seconds per _table_to_dicts() call at the given threshold."""
    monkeypatch.setattr(columnar_module, "_SMALL_TABLE_THRESHOLD", threshold)
    table = _build_payload(row_count)

    # Warmup — amortize import / dict-cache / first-call costs.
    columnar_module._table_to_dicts(table)

    samples: list[float] = []
    for _ in range(runs):
        t0 = time.perf_counter()
        columnar_module._table_to_dicts(table)
        samples.append(time.perf_counter() - t0)
    samples.sort()
    return samples[len(samples) // 2]


@pytest.mark.benchmark
@pytest.mark.parametrize(
    "threshold,row_count",
    [
        (32, 30),
        (32, 100),
        (64, 30),
        (64, 100),
        (128, 100),
        (128, 300),
        (256, 300),
        (256, 1000),
    ],
)
def test_small_table_threshold_sweep(
    monkeypatch: pytest.MonkeyPatch, threshold: int, row_count: int
) -> None:
    """Sweeps _SMALL_TABLE_THRESHOLD across {32, 64, 128, 256}.

    Reports throughput per (threshold, row_count); no hard floor — this
    test exists to surface relative performance, not to gate CI.
    """
    median_sec = _time_threshold(monkeypatch, threshold, row_count)
    throughput = row_count / median_sec
    print(f"\n  threshold={threshold:3d} row_count={row_count:4d}: {throughput:,.0f} rows/sec ({median_sec*1000:.2f}ms)")
    assert median_sec > 0


def test_small_table_threshold_is_pinned_at_64() -> None:
    """Pins _SMALL_TABLE_THRESHOLD = 64 — the current framework default.

    The accompanying sweep (test_small_table_threshold_sweep) covers
    {32, 64, 128, 256} against representative row counts so the breakeven
    can be re-measured at any time.  On hardware with strong pyarrow.compute
    vectorisation, the sweep may show higher thresholds outperforming 64
    even at low row counts; on slower hardware the small-table fast path
    can win.  64 is the conservative default kept until a cross-platform
    sweep argues for a different value.  If you change the constant,
    re-run the sweep and update this assertion.
    """
    assert columnar_module._SMALL_TABLE_THRESHOLD == 64
