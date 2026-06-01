"""Throughput benchmark for incorporator.io.handlers.columnar._table_to_dicts.

Sweeps representative row counts under the sole Arrow vectorised path.
No pinning assertion — this suite exists to surface throughput regressions,
not to gate constant values.
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


@pytest.mark.benchmark
@pytest.mark.parametrize("row_count", [30, 100, 300, 1000])
def test_table_to_dicts_throughput_sweep(row_count: int) -> None:
    """Measures _table_to_dicts() throughput across row counts under the Arrow path.

    Reports rows/sec for each parametrized row_count. No hard floor — this test
    surfaces relative performance and does not gate CI.
    """
    table = _build_payload(row_count)

    # Warmup — amortize import / dict-cache / first-call costs.
    columnar_module._table_to_dicts(table)

    samples: list[float] = []
    for _ in range(3):
        t0 = time.perf_counter()
        columnar_module._table_to_dicts(table)
        samples.append(time.perf_counter() - t0)
    samples.sort()
    median_sec = samples[len(samples) // 2]
    throughput = row_count / median_sec
    print(f"\n  row_count={row_count:4d}: {throughput:,.0f} rows/sec ({median_sec * 1000:.2f}ms)")
    assert median_sec > 0
