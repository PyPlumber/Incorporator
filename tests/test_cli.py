"""Unit tests for the Incorporator Typer CLI."""

import json
from pathlib import Path
from typing import Any, AsyncGenerator
from unittest.mock import patch

import pytest

pytest.importorskip("typer")
from typer.testing import CliRunner

from incorporator.cli import app
from incorporator.methods.logger import AuditResult

runner = CliRunner()


async def mock_stream(*args: Any, **kwargs: Any) -> AsyncGenerator[AuditResult, None]:
    """Mocks the async generator to instantly yield 1 successful chunk."""
    yield AuditResult(chunk_index=1, rows_processed=500, failed_sources=[], processing_time_sec=1.5)


def test_cli_missing_config() -> None:
    """Ensures CLI fails gracefully if the JSON file is missing."""
    result = runner.invoke(app, ["stream", "non_existent.json"])
    assert result.exit_code == 1
    assert "Error: Configuration file not found" in result.stdout


def test_cli_stream_success(tmp_path: Path) -> None:
    """Ensures the CLI correctly parses JSON and executes the stream bridge."""
    config_file = tmp_path / "pipeline.json"
    config_file.write_text(json.dumps({"incorp_params": {"inc_url": "https://dummy.api"}}), encoding="utf-8")

    # Patch the real stream with our mock generator
    with patch("incorporator.cli.LoggedIncorporator.stream", new=mock_stream):
        result = runner.invoke(app, ["stream", str(config_file), "--poll", "60.0"])

        assert result.exit_code == 0
        assert "Starting Incorporator Stream" in result.stdout
        assert "Chunk 1" in result.stdout
        assert "500 rows" in result.stdout
