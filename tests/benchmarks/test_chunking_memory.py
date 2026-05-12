"""Benchmark: prove the O(1) memory chunking claim.

The chunking engine releases each dataset (``del dataset; gc.collect()``) after
yielding its ``AuditResult``, so peak memory must NOT grow linearly with the
total number of chunks processed.

Method: track ``tracemalloc.get_traced_memory()`` per chunk and assert the
delta between max and min observed values stays bounded by a small constant,
not the total row count.
"""

import gc
import json
import tracemalloc
from pathlib import Path
from typing import List

import pytest

from incorporator import Incorporator
from incorporator.io.pagination import CSVPaginator
from incorporator.observability.logger import AuditResult


# 100 rows × 1000 chunks = 100k total rows. Small enough for CI; large enough
# that any linear-growth bug would surface within seconds.
ROWS_PER_CHUNK = 100
CHUNK_COUNT = 1000


@pytest.fixture
def big_csv(tmp_path: Path) -> Path:
    """Build a CSV of ROWS_PER_CHUNK * CHUNK_COUNT rows."""
    csv_path = tmp_path / "big.csv"
    lines = ["id,value"]
    total = ROWS_PER_CHUNK * CHUNK_COUNT
    for i in range(total):
        lines.append(f"{i},val_{i}")
    csv_path.write_text("\n".join(lines), encoding="utf-8")
    return csv_path


@pytest.mark.asyncio
async def test_chunking_memory_stays_flat(big_csv: Path) -> None:
    """O(1) memory claim: peak memory delta must stay bounded across N chunks.

    The test fires up the chunking engine, samples ``tracemalloc.get_traced_memory()``
    at every yielded ``AuditResult``, and asserts max-min stays below a generous
    upper bound. A linear-growth regression would push max far past min.
    """

    class BenchModel(Incorporator):
        pass

    paginator = CSVPaginator(file_path=str(big_csv), chunk_size=ROWS_PER_CHUNK)
    incorp_params = {
        "inc_url": "local_csv_stream",  # ignored — paginator drives the stream
        "inc_page": paginator,
    }

    samples: List[int] = []
    audits: List[AuditResult] = []

    # Warm up + clean baseline
    gc.collect()
    tracemalloc.start()
    try:
        async for audit in BenchModel.stream(incorp_params=incorp_params, stateful_polling=False):
            audits.append(audit)
            current, _ = tracemalloc.get_traced_memory()
            samples.append(current)
    finally:
        tracemalloc.stop()

    # Sanity: we should have got at least 10 chunks of audit data
    assert len(audits) >= 10, f"Expected many chunks, got {len(audits)}"

    # Discard the first 2 samples to ignore one-time allocation costs (handler init, etc.)
    stable_samples = samples[2:]
    delta = max(stable_samples) - min(stable_samples)

    # The threshold is intentionally generous — we are asserting "not linear growth",
    # not a tight memory budget. With 100k total rows, linear behaviour would yield
    # tens of MB; O(1) chunking keeps delta well under 5 MB.
    MAX_DELTA_BYTES = 5 * 1024 * 1024
    assert delta < MAX_DELTA_BYTES, (
        f"Memory delta {delta / 1024 / 1024:.2f} MB across {len(stable_samples)} chunks "
        f"suggests linear growth, not O(1) chunking. samples min={min(stable_samples)} "
        f"max={max(stable_samples)}"
    )
