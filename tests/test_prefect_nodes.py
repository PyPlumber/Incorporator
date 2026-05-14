"""Unit tests for the Incorporator Prefect Integration."""

import json
from pathlib import Path
from typing import Any, AsyncGenerator, List
from unittest.mock import patch

import pytest
from prefect.testing.utilities import prefect_test_harness

from incorporator.observability.logger import Wave
from incorporator.integrations.prefect import run_incorporator_flow


@pytest.fixture(scope="session")
def prefect_test_fixture():
    """Spins up an isolated in-process Prefect environment for the duration of this module.

    NOT autouse — must be requested explicitly so the Prefect SQLite backing store
    and env-var overrides don't bleed into unrelated tests.
    """
    with prefect_test_harness():
        yield


async def mock_stream(*args: Any, **kwargs: Any) -> AsyncGenerator[Wave, None]:
    yield Wave(chunk_index=1, rows_processed=1000, failed_sources=[], processing_time_sec=0.8)


@pytest.mark.asyncio
async def test_prefect_flow_missing_file(prefect_test_fixture: None) -> None:
    """Ensures the Flow throws a standard FileNotFoundError if config is missing."""
    with pytest.raises(FileNotFoundError):
        await run_incorporator_flow(config_path="missing_prefect_config.json")


@pytest.mark.asyncio
async def test_prefect_flow_success(tmp_path: Path, prefect_test_fixture: None) -> None:
    """Ensures the Prefect flow executes the stream task and aggregates results."""
    config_file = tmp_path / "prefect_pipeline.json"
    config_file.write_text(
        json.dumps({"incorp_params": {"inc_file": "dummy.csv"}, "export_params": {"file_path": "out.db"}}),
        encoding="utf-8",
    )

    with patch("incorporator.integrations.prefect.LoggedIncorporator.stream", new=mock_stream):
        results: List[Wave] = await run_incorporator_flow(config_path=str(config_file), poll_interval=None)

        assert len(results) == 1
        assert results[0].chunk_index == 1
        assert results[0].rows_processed == 1000
