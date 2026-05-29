"""Benchmark: prove streaming export sustains real-world throughput.

The Incorporator export pipeline pulls rows lazily from an iterable — no full
list is materialised in RAM. This benchmark feeds it a 500k-row generator and
asserts the per-row throughput is comfortable above a CI-safe floor.
"""

import time
from pathlib import Path
from typing import Iterable

import pytest

from incorporator.io.formats import FormatType
from incorporator.io.handlers import write_destination_data


ROW_COUNT = 500_000  # large enough for measurement noise to wash out


def _generate_rows() -> Iterable[dict]:
    """Yield ROW_COUNT lazy rows — no full materialisation."""
    for i in range(ROW_COUNT):
        yield {"id": i, "name": f"row_{i}", "value": i * 1.5}


@pytest.mark.asyncio
async def test_ndjson_streaming_throughput(tmp_path: Path) -> None:
    """NDJSON streaming write must sustain at least 50k rows/sec end-to-end."""
    out_path = tmp_path / "stream.ndjson"

    t0 = time.perf_counter()
    await write_destination_data(_generate_rows(), out_path, FormatType.NDJSON)
    elapsed = time.perf_counter() - t0

    throughput = ROW_COUNT / elapsed
    print(f"\n  NDJSON streaming export: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    # Sanity check the output landed on disk
    assert out_path.exists()
    line_count = sum(1 for _ in out_path.read_text(encoding="utf-8").splitlines())
    assert line_count == ROW_COUNT

    # CI floor — real-world numbers are usually 150k+/sec
    assert throughput >= 50_000, (
        f"NDJSON throughput {throughput:,.0f} rows/sec is below 50k floor. "
        "Suggests we lost the streaming generator path and are materialising."
    )


@pytest.mark.asyncio
async def test_json_streaming_throughput(tmp_path: Path) -> None:
    """JSON-array streaming write must sustain at least 50k rows/sec."""
    out_path = tmp_path / "stream.json"

    t0 = time.perf_counter()
    await write_destination_data(_generate_rows(), out_path, FormatType.JSON)
    elapsed = time.perf_counter() - t0

    throughput = ROW_COUNT / elapsed
    print(f"\n  JSON streaming export: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    assert out_path.exists()
    # File must be a valid JSON array
    assert out_path.read_text(encoding="utf-8").lstrip().startswith("[")

    assert throughput >= 50_000, f"JSON throughput {throughput:,.0f} rows/sec is below 50k floor."
