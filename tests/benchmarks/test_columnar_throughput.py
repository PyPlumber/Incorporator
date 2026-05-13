"""Benchmarks for Feather (Arrow IPC) and ORC throughput.

Both formats share pyarrow with Parquet and use the one-shot write API (no
streaming writer), so they materialize the dataset before writing. That means
they're slightly disadvantaged vs. Parquet's row-group streaming on memory,
but should match or beat Parquet on raw throughput because there's no
incremental encoding.

Floors are set to match Parquet's 100k rows/sec — anything below that suggests
a regression in row coercion or the type-bridge logic.
"""

import time
from pathlib import Path
from typing import Iterable

import pytest

from incorporator.io.formats import FormatType
from incorporator.io.handlers import write_destination_data

pytest.importorskip("pyarrow")

ROW_COUNT = 500_000


def _generate_rows() -> Iterable[dict]:
    for i in range(ROW_COUNT):
        yield {"id": i, "name": f"row_{i}", "value": i * 1.5, "active": bool(i % 2)}


@pytest.mark.asyncio
async def test_feather_streaming_throughput(tmp_path: Path) -> None:
    """Feather V2 write must sustain at least 100k rows/sec."""
    out_path = tmp_path / "stream.feather"

    t0 = time.perf_counter()
    await write_destination_data(_generate_rows(), out_path, FormatType.FEATHER)
    elapsed = time.perf_counter() - t0

    throughput = ROW_COUNT / elapsed
    print(f"\n  Feather write throughput: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    assert out_path.exists()
    import pyarrow.feather as feather

    table = feather.read_table(out_path)
    assert table.num_rows == ROW_COUNT

    assert throughput >= 100_000, f"Feather throughput {throughput:,.0f} rows/sec below 100k floor."


@pytest.mark.asyncio
async def test_orc_streaming_throughput(tmp_path: Path) -> None:
    """ORC write must sustain at least 100k rows/sec."""
    pytest.importorskip("pyarrow.orc")

    out_path = tmp_path / "stream.orc"

    t0 = time.perf_counter()
    await write_destination_data(_generate_rows(), out_path, FormatType.ORC)
    elapsed = time.perf_counter() - t0

    throughput = ROW_COUNT / elapsed
    print(f"\n  ORC write throughput: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    assert out_path.exists()
    from pyarrow import orc

    orc_file = orc.ORCFile(out_path)
    assert orc_file.nrows == ROW_COUNT

    assert throughput >= 100_000, f"ORC throughput {throughput:,.0f} rows/sec below 100k floor."


@pytest.mark.asyncio
async def test_feather_size_vs_parquet(tmp_path: Path) -> None:
    """Feather and Parquet should produce comparably-sized files on similar data.

    Informational benchmark — we just print the ratio. Feather uses LZ4 by
    default and Parquet uses Snappy; both are fast columnar compressors and
    sizes should be within ~2× of each other on this dataset.
    """
    feather_path = tmp_path / "out.feather"
    parquet_path = tmp_path / "out.parquet"
    rows = [{"id": i, "name": f"row_{i}", "value": i * 1.5, "active": bool(i % 2)} for i in range(50_000)]

    await write_destination_data(iter(rows), feather_path, FormatType.FEATHER)
    await write_destination_data(iter(rows), parquet_path, FormatType.PARQUET)

    feather_size = feather_path.stat().st_size
    parquet_size = parquet_path.stat().st_size
    print(f"\n  Feather: {feather_size:,} bytes | Parquet: {parquet_size:,} bytes")

    # Sanity: both must be meaningfully smaller than a naive JSON dump (~3.5 MB)
    assert feather_size < 2_000_000
    assert parquet_size < 2_000_000
