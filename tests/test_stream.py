"""
Unit tests for the Incorporator Autonomous Pipeline Orchestrator.
Tests both O(1) Chunking and Stateful Memory streams natively.
"""

import json
from pathlib import Path
from typing import Any, List

import pytest

from incorporator import FormatType, LoggedIncorporator
from incorporator.methods.logger import AuditResult
from incorporator.methods.paginate import CSVPaginator


# 1. Dummy Model for Testing
class StreamTargetModel(LoggedIncorporator):
    """Dynamic target for the stream to instantiate."""
    inc_code: Any = None
    name: str


@pytest.mark.asyncio
async def test_stream_engine_1_chunking_big_data(tmp_path: Path) -> None:
    """
    ENGINE 1: stateful_polling = False
    Ensures stream() uses the Paginator to yield strict O(1) memory chunks.
    """
    csv_file = tmp_path / "massive_dataset.csv"
    csv_file.write_text(
        "id,name\n1,Alice\n2,Bob\n3,Charlie\n4,Diana\n5,Eve\n",
        encoding="utf-8"
    )

    # Force a chunk size of 2 (5 rows / 2 = 3 chunks)
    paginator = CSVPaginator(file_path=str(csv_file), chunk_size=2)

    incorp_params = {
        "inc_url": "local_paginator_stream",
        "inc_page": paginator,
        "format_type": FormatType.JSON,
        "code_attr": "id"
    }

    audits: List[AuditResult] = []

    # Execute Engine 1 Stream
    async for audit in StreamTargetModel.stream(
            incorp_params=incorp_params,
            stateful_polling=False
    ):
        audits.append(audit)

    # Assert O(1) Chunking Mathematics
    assert len(audits) == 3
    assert audits[0].rows_processed == 2
    assert audits[1].rows_processed == 2
    assert audits[2].rows_processed == 1

    # Verify sequential indexing
    assert audits[0].chunk_index == 1
    assert audits[1].chunk_index == 2
    assert audits[2].chunk_index == 3
    assert paginator.is_exhausted is True


@pytest.mark.asyncio
async def test_stream_engine_2_stateful_live_data(tmp_path: Path) -> None:
    """
    ENGINE 2: stateful_polling = True
    Ensures stream() builds the memory graph exactly ONCE and loops against it.
    """
    json_file = tmp_path / "live_data.json"
    json_file.write_text(
        json.dumps([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}]),
        encoding="utf-8"
    )

    incorp_params = {
        "inc_file": str(json_file),
        "code_attr": "id"
    }

    audits: List[AuditResult] = []

    # Execute Engine 2 Stream (poll_interval=None forces it to break after 1 loop)
    async for audit in StreamTargetModel.stream(
            incorp_params=incorp_params,
            stateful_polling=True,
            poll_interval=None
    ):
        audits.append(audit)

    # Assert Stateful Polling Initialization
    assert len(audits) == 1
    assert audits[0].chunk_index == 1
    # It processed the entire live graph (3 items) at once
    assert audits[0].rows_processed == 3
    assert audits[0].failed_sources == []


@pytest.mark.asyncio
async def test_stream_engine_2_empty_failsafe(tmp_path: Path) -> None:
    """
    ENGINE 2: Empty Data Guard
    Ensures the daemon safely exits if initialization fails or yields 0 rows.
    """
    json_file = tmp_path / "empty_data.json"
    json_file.write_text("[]", encoding="utf-8")

    incorp_params = {
        "inc_file": str(json_file)
    }

    audits: List[AuditResult] = []

    async for audit in StreamTargetModel.stream(
            incorp_params=incorp_params,
            stateful_polling=True
    ):
        audits.append(audit)

    # Assert Graceful Shutdown
    assert len(audits) == 1
    assert audits[0].rows_processed == 0
    assert "Initial incorp() yielded no data" in audits[0].failed_sources[0]