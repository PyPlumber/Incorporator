"""Unit tests for the Incorporator Typer CLI."""

import json
from pathlib import Path
from typing import Any, AsyncGenerator
from unittest.mock import patch

import pytest

pytest.importorskip("typer")
from typer.testing import CliRunner

from incorporator.cli import app
from incorporator.observability.logger import AuditResult

runner = CliRunner()


async def mock_stream(*args: Any, **kwargs: Any) -> AsyncGenerator[AuditResult, None]:
    """Mocks the async generator to instantly yield 1 successful chunk."""
    yield AuditResult(chunk_index=1, rows_processed=500, failed_sources=[], processing_time_sec=1.5)


async def mock_stream_with_failures(*args: Any, **kwargs: Any) -> AsyncGenerator[AuditResult, None]:
    """Mocks a stream that yields one chunk with failed sources."""
    yield AuditResult(
        chunk_index=1,
        rows_processed=10,
        failed_sources=["https://dead.example.com"],
        processing_time_sec=0.5,
    )


async def mock_stream_raises(*args: Any, **kwargs: Any) -> AsyncGenerator[AuditResult, None]:
    """Mocks a stream that raises mid-yield to test the fatal-exception path."""
    raise RuntimeError("simulated network catastrophe")
    yield  # pragma: no cover (unreachable — keeps mypy happy about AsyncGenerator)


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


# ==========================================
# ERROR-PATH COVERAGE
# ==========================================


def test_cli_invalid_json_config(tmp_path: Path) -> None:
    """A malformed JSON config must exit 1 with an 'Invalid JSON' error message."""
    config_file = tmp_path / "broken.json"
    config_file.write_text("{not: valid, json}", encoding="utf-8")

    result = runner.invoke(app, ["stream", str(config_file)])

    assert result.exit_code == 1
    assert "Invalid JSON" in result.stdout


def test_cli_missing_incorp_params(tmp_path: Path) -> None:
    """A config with no 'incorp_params' key must exit 1 with a clear message."""
    config_file = tmp_path / "no_params.json"
    config_file.write_text(json.dumps({"refresh_params": {}}), encoding="utf-8")

    result = runner.invoke(app, ["stream", str(config_file)])

    assert result.exit_code == 1
    assert "'incorp_params' must be defined" in result.stdout


def test_cli_stream_reports_failed_sources(tmp_path: Path) -> None:
    """When the stream yields an AuditResult with failed_sources, the CLI must surface them."""
    config_file = tmp_path / "pipe.json"
    config_file.write_text(json.dumps({"incorp_params": {"inc_url": "https://x"}}), encoding="utf-8")

    with patch("incorporator.cli.LoggedIncorporator.stream", new=mock_stream_with_failures):
        result = runner.invoke(app, ["stream", str(config_file)])

    assert result.exit_code == 0
    assert "Failures:" in result.stdout
    assert "dead.example.com" in result.stdout


def test_cli_keyboard_interrupt(tmp_path: Path) -> None:
    """KeyboardInterrupt raised inside asyncio.run must be caught; exit code stays 0."""
    config_file = tmp_path / "pipe.json"
    config_file.write_text(json.dumps({"incorp_params": {"inc_url": "https://x"}}), encoding="utf-8")

    with patch("incorporator.cli.asyncio.run", side_effect=KeyboardInterrupt):
        result = runner.invoke(app, ["stream", str(config_file)])

    # The stream() handler catches KeyboardInterrupt and emits a message without sys.exit(1)
    assert result.exit_code == 0
    assert "stopped by user" in result.stdout


def test_cli_fatal_exception_in_stream(tmp_path: Path) -> None:
    """A mid-stream exception must exit 1 with the fatal-error banner."""
    config_file = tmp_path / "pipe.json"
    config_file.write_text(json.dumps({"incorp_params": {"inc_url": "https://x"}}), encoding="utf-8")

    with patch("incorporator.cli.LoggedIncorporator.stream", new=mock_stream_raises):
        result = runner.invoke(app, ["stream", str(config_file)])

    assert result.exit_code == 1
    assert "Fatal Pipeline Error" in result.stdout
    assert "simulated network catastrophe" in result.stdout
