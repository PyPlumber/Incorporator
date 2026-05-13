"""Benchmark: prove CSV/TSV/PSV streaming export sustains real-world throughput.

All three formats share the same ``CSVHandler`` engine — only the delimiter
differs — so a single parametrized test covers all three.  Delimited writes
are pure string serialisation: no compression, no schema inference, no
columnar encoding, so they should comfortably beat row-oriented JSON.

CI floor of 100k rows/sec is conservative; real-world numbers on this
synthetic dataset typically land in the 300k–500k range.
"""

import time
from pathlib import Path
from typing import Iterable

import pytest

from incorporator.io.formats import FormatType
from incorporator.io.handlers import write_destination_data

ROW_COUNT = 500_000  # matches NDJSON/Parquet so cross-format comparison is direct


def _generate_rows() -> Iterable[dict]:
    for i in range(ROW_COUNT):
        yield {"id": i, "name": f"row_{i}", "value": i * 1.5, "active": bool(i % 2)}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fmt", "ext", "delim"),
    [
        (FormatType.CSV, "csv", ","),
        (FormatType.TSV, "tsv", "\t"),
        (FormatType.PSV, "psv", "|"),
    ],
    ids=["csv", "tsv", "psv"],
)
async def test_delimited_streaming_throughput(tmp_path: Path, fmt: FormatType, ext: str, delim: str) -> None:
    """CSV/TSV/PSV streaming write must sustain at least 100k rows/sec."""
    out_path = tmp_path / f"stream.{ext}"

    t0 = time.perf_counter()
    await write_destination_data(_generate_rows(), out_path, fmt)
    elapsed = time.perf_counter() - t0

    throughput = ROW_COUNT / elapsed
    print(f"\n  {fmt.name} streaming write: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    # Sanity: file exists, has the right delimiter on the header line.
    assert out_path.exists()
    header_line = out_path.read_text(encoding="utf-8").splitlines()[0]
    assert delim in header_line, f"Header line {header_line!r} missing delimiter {delim!r}"

    # 100k floor — delimited writers are pure string concat, should comfortably
    # beat row-oriented JSON (~150k/sec on the same dataset).
    assert throughput >= 100_000, (
        f"{fmt.name} throughput {throughput:,.0f} rows/sec is below 100k floor. "
        "Suggests CSV writer is doing per-row dict allocation or escaping in a hot loop."
    )
