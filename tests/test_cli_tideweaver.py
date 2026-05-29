"""CLI tests for `incorporator tideweaver run`.

Covers --json-output NDJSON Tide shape, --heartbeat-file touch behavior,
and --drain-timeout precedence (CLI > env var > default).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytest.importorskip("typer")
from typer.testing import CliRunner

from incorporator.cli import app
from incorporator.observability.tideweaver import Tide

runner = CliRunner()

# ---------------------------------------------------------------------------
# Shared watershed fixture helpers
# ---------------------------------------------------------------------------

_OUTFLOW_SRC = """\
from incorporator import Incorporator

class LapData(Incorporator):
    pass

def outflow(state):
    return []
"""

# A window firmly in the past so the Tideweaver exits after the first pass.
_PAST_WINDOW = {
    "start": "2020-01-01T00:00:00+00:00",
    "end": "2020-01-01T00:01:00+00:00",
}


def _write_watershed_fixture(tmp_path: Path) -> Path:
    """Write outflow.py + watershed.json into *tmp_path* and return the JSON path.

    The window is set in the past so the Tideweaver exits after a single
    scheduler pass without sleeping — keeps each test fast.
    """
    (tmp_path / "outflow.py").write_text(_OUTFLOW_SRC, encoding="utf-8")
    cfg = tmp_path / "watershed.json"
    cfg.write_text(
        json.dumps(
            {
                "window": _PAST_WINDOW,
                "shape": "parallel",
                "outflow": "outflow.py",
                "currents": [
                    {
                        "name": "laps",
                        "class": "LapData",
                        "verb": "stream",
                        "interval": 30,
                        "incorp_params": {},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return cfg


# ---------------------------------------------------------------------------
# NDJSON output shape
# ---------------------------------------------------------------------------


def test_cli_tideweaver_run_json_output_emits_ndjson(tmp_path: Path) -> None:
    """--json-output emits one NDJSON Tide line per scheduler pass.

    Verifies that each line is valid JSON and carries the required Tide
    fields: tide_number, fired, skipped, current_outcomes, wake_reason,
    duration_sec.  current_outcomes must be a list of dicts, not an
    opaque dataclass repr (exercises the @field_serializer on Tide).
    """
    cfg = _write_watershed_fixture(tmp_path)
    result = runner.invoke(app, ["tideweaver", "run", str(cfg), "--json-output"])

    assert result.exit_code == 0, result.stdout
    json_lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
    assert json_lines, f"expected at least one NDJSON tide line; got: {result.stdout!r}"

    for line in json_lines:
        record = json.loads(line)
        assert "tide_number" in record
        assert "fired" in record
        assert "skipped" in record
        assert "current_outcomes" in record
        assert "wake_reason" in record
        assert "duration_sec" in record
        # current_outcomes must be a list of plain dicts, not a dataclass repr.
        assert isinstance(record["current_outcomes"], list)
        for outcome in record["current_outcomes"]:
            assert isinstance(outcome, dict), f"current_outcomes entry must be a dict; got {type(outcome)}"


# ---------------------------------------------------------------------------
# Heartbeat file
# ---------------------------------------------------------------------------


def test_cli_tideweaver_run_heartbeat_file_touched(tmp_path: Path) -> None:
    """--heartbeat-file is created/touched after every scheduler pass.

    Uses a past-window watershed so the run completes in one pass; asserts
    the file exists afterwards.  The mtime check is omitted because the CI
    filesystem may not update mtime within the test's precision window.
    """
    cfg = _write_watershed_fixture(tmp_path)
    heartbeat = tmp_path / "tw_heartbeat.beat"
    assert not heartbeat.exists()

    result = runner.invoke(
        app,
        ["tideweaver", "run", str(cfg), "--heartbeat-file", str(heartbeat)],
    )

    assert result.exit_code == 0, result.stdout
    assert heartbeat.exists(), "heartbeat file must be created after a tideweaver pass"


# ---------------------------------------------------------------------------
# drain-timeout precedence
# ---------------------------------------------------------------------------


def test_cli_tideweaver_run_drain_timeout_cli_wins(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """--drain-timeout CLI flag overrides INCORPORATOR_DRAIN_TIMEOUT env var.

    Sets the env var to 100 and passes --drain-timeout 5.0 on the CLI; the
    watershed that reaches Tideweaver must carry drain_timeout=5.0.  Mocks
    Tideweaver.run to capture the watershed without running the scheduler.
    """
    monkeypatch.setenv("INCORPORATOR_DRAIN_TIMEOUT", "100")
    cfg = _write_watershed_fixture(tmp_path)

    captured: list[Any] = []

    async def _fake_run(self: Any) -> AsyncGenerator[Tide, None]:
        captured.append(self.watershed.drain_timeout)
        return
        yield  # pragma: no cover — make this an AsyncGenerator

    with patch("incorporator.cli.tideweaver.Tideweaver.run", _fake_run):
        result = runner.invoke(
            app,
            ["tideweaver", "run", str(cfg), "--drain-timeout", "5.0"],
        )

    assert result.exit_code == 0, result.stdout
    assert captured, "Tideweaver.run must have been called"
    assert captured[0] == pytest.approx(5.0), f"CLI --drain-timeout 5.0 must override env var 100; got {captured[0]}"


def test_cli_tideweaver_run_drain_timeout_env_var_used(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """INCORPORATOR_DRAIN_TIMEOUT env var is applied when --drain-timeout is absent.

    No CLI flag passed; env var set to 42.  The watershed that reaches
    Tideweaver must carry drain_timeout=42.0.
    """
    monkeypatch.setenv("INCORPORATOR_DRAIN_TIMEOUT", "42")
    cfg = _write_watershed_fixture(tmp_path)

    captured: list[Any] = []

    async def _fake_run(self: Any) -> AsyncGenerator[Tide, None]:
        captured.append(self.watershed.drain_timeout)
        return
        yield  # pragma: no cover

    with patch("incorporator.cli.tideweaver.Tideweaver.run", _fake_run):
        result = runner.invoke(app, ["tideweaver", "run", str(cfg)])

    assert result.exit_code == 0, result.stdout
    assert captured, "Tideweaver.run must have been called"
    assert captured[0] == pytest.approx(42.0), f"INCORPORATOR_DRAIN_TIMEOUT=42 must be applied; got {captured[0]}"


def test_cli_tideweaver_run_drain_timeout_default(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Without --drain-timeout or INCORPORATOR_DRAIN_TIMEOUT, the watershed default (30s) is used.

    Watershed.drain_timeout defaults to 30.0 in the Pydantic field definition.
    When neither the CLI flag nor the env var is set, that default must be
    preserved unchanged.
    """
    monkeypatch.delenv("INCORPORATOR_DRAIN_TIMEOUT", raising=False)
    cfg = _write_watershed_fixture(tmp_path)

    captured: list[Any] = []

    async def _fake_run(self: Any) -> AsyncGenerator[Tide, None]:
        captured.append(self.watershed.drain_timeout)
        return
        yield  # pragma: no cover

    with patch("incorporator.cli.tideweaver.Tideweaver.run", _fake_run):
        result = runner.invoke(app, ["tideweaver", "run", str(cfg)])

    assert result.exit_code == 0, result.stdout
    assert captured, "Tideweaver.run must have been called"
    assert captured[0] == pytest.approx(30.0), f"Default drain_timeout must be 30.0; got {captured[0]}"
