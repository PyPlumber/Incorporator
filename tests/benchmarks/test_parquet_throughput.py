"""Benchmark: prove Parquet streaming write sustains real-world throughput.

Parquet uses columnar encoding + compression — it should comfortably beat the
row-oriented formats (NDJSON ~128k rows/sec, SQLite ~155k rows/sec) on the same
hardware. The benchmark feeds a 500k-row generator into ParquetHandler.write
and asserts >100k rows/sec end-to-end, including type coercion and Snappy
compression.

Gated by ``pytest.importorskip("pyarrow")`` because pyarrow is an opt-in extra.
"""

import time
from pathlib import Path
from typing import Iterable

import pytest

from incorporator.io.formats import FormatType
from incorporator.io.handlers import write_destination_data

pytest.importorskip("pyarrow")

ROW_COUNT = 500_000  # matches the JSON/NDJSON benchmark for direct comparison


def _generate_rows() -> Iterable[dict]:
    for i in range(ROW_COUNT):
        yield {"id": i, "name": f"row_{i}", "value": i * 1.5, "active": bool(i % 2)}


@pytest.mark.asyncio
async def test_parquet_streaming_throughput(tmp_path: Path) -> None:
    """Parquet streaming write must sustain at least 100k rows/sec."""
    out_path = tmp_path / "stream.parquet"

    t0 = time.perf_counter()
    await write_destination_data(_generate_rows(), out_path, FormatType.PARQUET)
    elapsed = time.perf_counter() - t0

    throughput = ROW_COUNT / elapsed
    print(f"\n  Parquet streaming write: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    assert out_path.exists()
    # Round-trip sanity: read it back, confirm row count
    import pyarrow.parquet as pq

    table = pq.read_table(out_path)
    assert table.num_rows == ROW_COUNT

    # 100k floor — Parquet should comfortably outpace the row-oriented formats.
    assert throughput >= 100_000, (
        f"Parquet throughput {throughput:,.0f} rows/sec is below the 100k floor. "
        "Investigate row-group batching, compression, or Arrow type inference overhead."
    )


@pytest.mark.asyncio
async def test_parquet_compression_size_vs_ndjson(tmp_path: Path) -> None:
    """Parquet (Snappy) must produce a meaningfully smaller file than NDJSON.

    Informational + correctness check. Parquet's columnar layout + dictionary
    encoding should beat NDJSON by a wide margin on this kind of repetitive
    data. We assert at least a 2× size reduction as a sanity floor; real-world
    ratios on this dataset are typically 5–10×.
    """
    ndjson_path = tmp_path / "out.ndjson"
    parquet_path = tmp_path / "out.parquet"

    # Reduced row count — we only care about size ratio, not throughput here.
    rows = [{"id": i, "name": f"row_{i}", "value": i * 1.5, "active": bool(i % 2)} for i in range(50_000)]

    await write_destination_data(iter(rows), ndjson_path, FormatType.NDJSON)
    await write_destination_data(iter(rows), parquet_path, FormatType.PARQUET)

    ndjson_size = ndjson_path.stat().st_size
    parquet_size = parquet_path.stat().st_size
    ratio = ndjson_size / parquet_size

    print(f"\n  NDJSON: {ndjson_size:,} bytes | Parquet: {parquet_size:,} bytes | ratio: {ratio:.1f}×")

    # 2× floor — Parquet should always beat NDJSON on repetitive data.
    assert ratio >= 2.0, (
        f"Parquet file is only {ratio:.1f}× smaller than NDJSON — suggests compression "
        "or dictionary encoding is misconfigured."
    )
