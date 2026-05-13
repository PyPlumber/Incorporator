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


# ==========================================
# FJORD SUBCOMMAND
# ==========================================

FJORD_USER_MODULE_SRC = '''
from incorporator import Incorporator

class Coin(Incorporator):
    pass

class BinanceFutures(Incorporator):
    pass

def outflow(state):
    return [{"inc_code": "stub", "marker": "ok"}]
'''


def _write_fjord_fixture(tmp_path: Path) -> tuple[Path, Path]:
    """Write a user-module + pipeline.json pair into tmp_path. Returns (config, module).

    The user module is named ``coin_market.py`` so fjord auto-derives the
    output class name ``CoinMarket``.
    """
    user_module = tmp_path / "coin_market.py"
    user_module.write_text(FJORD_USER_MODULE_SRC, encoding="utf-8")

    config = tmp_path / "fjord.json"
    config.write_text(
        json.dumps(
            {
                "code_file": "coin_market.py",
                "stream_params": [
                    {"cls_name": "Coin", "incorp_params": {"inc_url": "https://x"}},
                    {"cls_name": "BinanceFutures", "incorp_params": {"inc_url": "https://y"}},
                ],
                "export_params": {"file_path": str(tmp_path / "out.ndjson")},
            }
        ),
        encoding="utf-8",
    )
    return config, user_module


async def mock_fjord(*args: Any, **kwargs: Any) -> AsyncGenerator[AuditResult, None]:
    """Mocks the fjord async generator to yield two audits and exit."""
    yield AuditResult(
        chunk_index=1, operation="fjord_incorp:Coin", rows_processed=10, processing_time_sec=0.1
    )
    yield AuditResult(
        chunk_index=1, operation="outflow:CoinMarket", rows_processed=10, processing_time_sec=0.2
    )


def test_cli_fjord_success(tmp_path: Path) -> None:
    """fjord subcommand imports the code_file, resolves source classes, and drains audits.

    The output class is auto-derived from the filename — no ``output_class``
    JSON key.
    """
    config, _ = _write_fjord_fixture(tmp_path)

    # Patch the fjord classmethod on the base Incorporator — the CLI now calls
    # Incorporator.fjord(...) directly (no user-defined output class).
    with patch(
        "incorporator.cli.Incorporator.fjord",
        new=mock_fjord,
    ):
        result = runner.invoke(app, ["fjord", str(config)])

    assert result.exit_code == 0, result.stdout
    assert "Starting Incorporator Fjord" in result.stdout
    assert "fjord_incorp:Coin" in result.stdout
    assert "outflow:CoinMarket" in result.stdout


def test_cli_fjord_missing_required_keys(tmp_path: Path) -> None:
    """fjord config missing required keys exits 1 with clear error."""
    config = tmp_path / "fjord.json"
    config.write_text(json.dumps({"code_file": "x.py"}), encoding="utf-8")

    result = runner.invoke(app, ["fjord", str(config)])

    assert result.exit_code == 1
    assert "fjord config requires" in result.stdout
    # output_class is no longer required — make sure the error message dropped it.
    assert "output_class" not in result.stdout


def test_cli_fjord_code_file_not_found(tmp_path: Path) -> None:
    """fjord config pointing at a non-existent code_file exits 1."""
    config = tmp_path / "fjord.json"
    config.write_text(
        json.dumps(
            {
                "code_file": "ghost.py",
                "stream_params": [{"cls_name": "Coin", "incorp_params": {}}],
                "export_params": {"file_path": "out.ndjson"},
            }
        ),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["fjord", str(config)])

    assert result.exit_code == 1
    assert "code_file not found" in result.stdout


def test_cli_fjord_stream_missing_cls_name(tmp_path: Path) -> None:
    """stream_params entry missing cls_name exits 1."""
    user_module = tmp_path / "coin_market.py"
    user_module.write_text(FJORD_USER_MODULE_SRC, encoding="utf-8")

    config = tmp_path / "fjord.json"
    config.write_text(
        json.dumps(
            {
                "code_file": "coin_market.py",
                "stream_params": [{"incorp_params": {}}],
                "export_params": {"file_path": "out.ndjson"},
            }
        ),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["fjord", str(config)])
    assert result.exit_code == 1
    assert "missing 'cls_name'" in result.stdout
