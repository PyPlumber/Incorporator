"""Benchmark: prove Avro streaming export sustains real-world throughput.

Avro writes via fastavro's generator-based writer — no full materialisation —
so throughput should rival columnar formats.  The handler requires a
``pydantic_schema`` kwarg the framework normally injects; we mock one here
so the bench exercises the same code path as a real pipeline.

Gated by ``pytest.importorskip("fastavro")`` because fastavro is an opt-in
extra (``pip install incorporator[avro]``).

ROW_COUNT is large (500k) so schema-binding overhead on the first row
averages out — at 50k rows it visibly skews the per-row throughput number.
"""

import time
from pathlib import Path
from typing import Iterable

import pytest

from incorporator.io.formats import FormatType
from incorporator.io.handlers import write_destination_data

pytest.importorskip("fastavro")

ROW_COUNT = 500_000  # matches Parquet/NDJSON so cross-format comparison is direct

# Pydantic-shape schema hint the framework would normally inject. The Avro
# handler reads `properties` to build its record schema; without this the
# handler emits a 0-field record and skips all the row data.
_SCHEMA_HINT = {
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "value": {"type": "number"},
        "active": {"type": "boolean"},
    }
}


def _generate_rows() -> Iterable[dict]:
    for i in range(ROW_COUNT):
        yield {"id": i, "name": f"row_{i}", "value": i * 1.5, "active": bool(i % 2)}


@pytest.mark.asyncio
async def test_avro_streaming_throughput(tmp_path: Path) -> None:
    """Avro streaming write must sustain at least 30k rows/sec.

    fastavro consumes a generator (O(1) memory) but per-row schema coercion
    via ``coerce_avro_value`` plus the ``sanitize_json_key`` call inside the
    record generator add per-row overhead the columnar writers avoid.
    Measured baseline on commodity hardware is ~45-50k rows/sec; the 30k
    floor leaves headroom for CI noise.
    """
    out_path = tmp_path / "stream.avro"

    t0 = time.perf_counter()
    await write_destination_data(
        _generate_rows(),
        out_path,
        FormatType.AVRO,
        sql_table="BenchmarkRecord",
        pydantic_schema=_SCHEMA_HINT,
    )
    elapsed = time.perf_counter() - t0

    throughput = ROW_COUNT / elapsed
    print(f"\n  Avro streaming write: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    # Round-trip sanity: read it back, confirm row count.
    assert out_path.exists()
    import fastavro

    with open(out_path, "rb") as f:
        row_count = sum(1 for _ in fastavro.reader(f))
    assert row_count == ROW_COUNT

    # 30k floor — fastavro is fast but per-row schema coercion + key
    # sanitisation in _record_generator adds overhead vs Parquet's batched
    # columnar encoding.  See module docstring for measured baseline.
    assert throughput >= 30_000, (
        f"Avro throughput {throughput:,.0f} rows/sec is below 30k floor. "
        "Suggests fastavro is being given a list instead of a generator, "
        "or that per-row schema coercion is allocating excessively."
    )
