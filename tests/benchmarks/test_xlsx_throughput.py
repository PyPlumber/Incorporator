"""Benchmark: prove XLSX streaming export sustains reasonable throughput.

openpyxl is the slowest writer in the framework — it builds a full in-memory
spreadsheet model before serialising, and pure-Python cell-by-cell writes
dominate runtime.  Realistic throughput is 5k–20k rows/sec on commodity
hardware; the 5k floor is set conservatively for CI.

ROW_COUNT is intentionally an order of magnitude smaller than the other
benches (10k vs 500k) so the suite runtime stays reasonable.  openpyxl at
500k rows takes ~30s+ which would dominate the whole benchmark suite.

Gated by ``pytest.importorskip("openpyxl")`` because xlsx is an opt-in extra.
"""

import time
from pathlib import Path
from typing import Iterable

import pytest

from incorporator.io.formats import FormatType
from incorporator.io.handlers import write_destination_data

pytest.importorskip("openpyxl")

ROW_COUNT = 10_000  # openpyxl is slow — large enough to average noise, small enough for CI


def _generate_rows() -> Iterable[dict]:
    for i in range(ROW_COUNT):
        yield {"id": i, "name": f"row_{i}", "value": i * 1.5, "active": bool(i % 2)}


@pytest.mark.asyncio
async def test_xlsx_streaming_throughput(tmp_path: Path) -> None:
    """XLSX write must sustain at least 5k rows/sec.

    openpyxl materialises the full workbook in memory (no streaming writer
    API for the default backend) — peak memory scales with row count.  A
    5k/sec floor reflects that this is the slowest serialiser by design and
    primarily useful for user-facing reports, not bulk data movement.
    """
    out_path = tmp_path / "stream.xlsx"

    t0 = time.perf_counter()
    await write_destination_data(_generate_rows(), out_path, FormatType.XLSX)
    elapsed = time.perf_counter() - t0

    throughput = ROW_COUNT / elapsed
    print(f"\n  XLSX streaming write: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    # Round-trip sanity: read the workbook back, confirm row count.
    # openpyxl's read_only mode reports ``max_row=None`` until the sheet is
    # iterated, so count rows explicitly via the iterator instead.
    assert out_path.exists()
    import openpyxl

    wb = openpyxl.load_workbook(out_path, read_only=True)
    ws = wb.active
    actual_rows = sum(1 for _ in ws.iter_rows()) - 1  # -1 for header
    wb.close()
    assert actual_rows == ROW_COUNT, f"Expected {ROW_COUNT} rows, got {actual_rows}"

    # 5k floor — openpyxl is the slowest serialiser by design (cell-by-cell
    # Python writes).  Below 5k suggests we lost the streaming generator
    # path and are double-materialising the dataset.
    assert throughput >= 5_000, (
        f"XLSX throughput {throughput:,.0f} rows/sec is below 5k floor. "
        "Suggests openpyxl is being given a list, doubling memory and runtime."
    )
