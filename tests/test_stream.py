"""
Unit tests for the Incorporator Autonomous Pipeline Orchestrator.
Tests both O(1) Chunking and Stateful Memory streams natively.
"""

import json
from pathlib import Path
from typing import Any, List, Type

import pytest

from incorporator import FormatType, LoggedIncorporator
from incorporator.observability.logger import Wave
from incorporator.io.pagination import CSVPaginator


@pytest.fixture
def stream_target_model() -> Type[LoggedIncorporator]:
    """Build a fresh LoggedIncorporator subclass per test.

    State isolation: defining the class inside the fixture (called once per
    test by default) guarantees each test sees a clean ``inc_dict`` and
    ``_schema_union``. A module-level class would accumulate state across
    test runs and break under pytest-randomly or parallel execution.
    """

    class StreamTargetModel(LoggedIncorporator):
        """Dynamic target for the stream to instantiate."""

        inc_code: Any = None
        name: str

    return StreamTargetModel


@pytest.mark.asyncio
async def test_stream_engine_1_chunking_big_data(
    tmp_path: Path, stream_target_model: Type[LoggedIncorporator]
) -> None:
    """
    ENGINE 1: stateful_polling = False
    Ensures stream() uses the Paginator to yield strict O(1) memory chunks.
    """
    StreamTargetModel = stream_target_model
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

    waves: List[Wave] = []

    # Execute Engine 1 Stream
    async for wave in StreamTargetModel.stream(
            incorp_params=incorp_params,
            stateful_polling=False
    ):
        waves.append(wave)

    # Assert O(1) Chunking Mathematics
    assert len(waves) == 3
    assert waves[0].rows_processed == 2
    assert waves[1].rows_processed == 2
    assert waves[2].rows_processed == 1

    # Verify sequential indexing
    assert waves[0].chunk_index == 1
    assert waves[1].chunk_index == 2
    assert waves[2].chunk_index == 3
    assert paginator.is_exhausted is True


@pytest.mark.asyncio
async def test_stream_engine_2_stateful_live_data(
    tmp_path: Path, stream_target_model: Type[LoggedIncorporator]
) -> None:
    """
    ENGINE 2: stateful_polling = True
    Ensures stream() builds the memory graph exactly ONCE and loops against it.
    """
    StreamTargetModel = stream_target_model
    json_file = tmp_path / "live_data.json"
    json_file.write_text(
        json.dumps([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}]),
        encoding="utf-8"
    )

    incorp_params = {
        "inc_file": str(json_file),
        "code_attr": "id"
    }

    waves: List[Wave] = []

    # Execute Engine 2 Stream (poll_interval=None forces it to break after 1 loop)
    async for wave in StreamTargetModel.stream(
            incorp_params=incorp_params,
            stateful_polling=True,
            poll_interval=None
    ):
        waves.append(wave)

    # Assert Stateful Polling Initialization
    assert len(waves) == 1
    assert waves[0].chunk_index == 1
    # It processed the entire live graph (3 items) at once
    assert waves[0].rows_processed == 3
    assert waves[0].failed_sources == []


@pytest.mark.asyncio
async def test_stream_engine_2_empty_failsafe(
    tmp_path: Path, stream_target_model: Type[LoggedIncorporator]
) -> None:
    """
    ENGINE 2: Empty Data Guard
    Ensures the daemon safely exits if initialization fails or yields 0 rows.
    """
    StreamTargetModel = stream_target_model
    json_file = tmp_path / "empty_data.json"
    json_file.write_text("[]", encoding="utf-8")

    incorp_params = {
        "inc_file": str(json_file)
    }

    waves: List[Wave] = []

    async for wave in StreamTargetModel.stream(
            incorp_params=incorp_params,
            stateful_polling=True
    ):
        waves.append(wave)

    # Assert Graceful Shutdown
    assert len(waves) == 1
    assert waves[0].rows_processed == 0
    assert "Initial incorp() yielded no data" in waves[0].failed_sources[0]


@pytest.mark.asyncio
async def test_stream_outflow_without_stateful_polling_raises(tmp_path: Path) -> None:
    """outflow on stream is stateful-only — chunking mode must raise ValueError fast."""
    outflow_py = tmp_path / "outflow.py"
    outflow_py.write_text(
        "from incorporator import LoggedIncorporator\nclass Outflow(LoggedIncorporator): pass\n",
        encoding="utf-8",
    )

    class StreamModel(LoggedIncorporator):
        inc_code: Any = None

    gen = StreamModel.stream(
        incorp_params={"inc_url": "https://example.invalid/x"},
        outflow=outflow_py,
        # stateful_polling=False (default) — engine must refuse outflow here
    )
    with pytest.raises(ValueError, match="stateful_polling=True"):
        async for _ in gen:
            pass  # pragma: no cover — generator should raise on first iteration