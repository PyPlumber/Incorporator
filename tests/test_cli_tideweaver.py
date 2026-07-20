"""CLI tests for `incorporator tideweaver run`.

Covers --json-output NDJSON Tide shape, --heartbeat-file touch behavior,
and --drain-timeout precedence (CLI > env var > default).
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytest.importorskip("typer")
from typer.testing import CliRunner

from incorporator.cli import app
from incorporator.io.penstock import _HOST_PENSTOCKS
from incorporator.tideweaver import Tide

runner = CliRunner()


@pytest.fixture(autouse=True)
def _reset_json_output_mode() -> Iterator[None]:
    """Reset the module-level ``_JSON_OUTPUT_MODE`` global after every test.

    Mirrors the identical fixture in ``tests/test_cli.py``. This file's new
    stdout-purity pin (below) flips ``set_json_output_mode(True)`` via
    `tideweaver run --json-output`; under randomized ordering a leaked
    ``True`` would route later tests' ``_err`` output to stderr, breaking
    their ``result.stdout`` assertions.
    """
    yield
    from incorporator.cli.runners import set_json_output_mode

    set_json_output_mode(False)


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


def _write_invalid_watershed_fixture(tmp_path: Path) -> Path:
    """Write a watershed.json referencing a class absent from outflow.py.

    ``build_watershed`` raises ``ValueError`` while resolving the current's
    ``class`` string, which ``_run_validation`` reports as "Config invalid".
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
                        "class": "NoSuchClass",
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


def test_cli_tideweaver_run_json_output_stdout_is_pure_ndjson_on_config_error(tmp_path: Path) -> None:
    """PIN: --json-output keeps stdout pure even on a config error.

    Before this fix, `tideweaver run`/`validate` never called
    `set_json_output_mode`, so `_err`'s "Config invalid" diagnostic printed
    to stdout instead of stderr, contaminating the NDJSON stream. A bad
    watershed.json (a `class` string that resolves to no Incorporator
    subclass in outflow.py) must exit 1 with the report on stderr and
    nothing on stdout.
    """
    cfg = _write_invalid_watershed_fixture(tmp_path)

    result = runner.invoke(app, ["tideweaver", "run", str(cfg), "--json-output"])

    assert result.exit_code == 1, result.stdout
    assert "Config invalid" in result.stderr
    assert "Config invalid" not in result.stdout
    assert result.stdout.strip() == "", f"stdout must stay empty under --json-output on error; got: {result.stdout!r}"


def test_cli_tideweaver_run_logs_flag_wires_configure_logs_option(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """PIN: --logs fires the shared `configure_logs_option` helper.

    Monkeypatches the shared helper (rather than asserting on real log
    output, which would fight pytest's own root-logger handler) and confirms
    it is called with ``enabled=True`` under ``--logs``.
    """
    from incorporator.cli import runners as runners_mod

    calls: list[bool] = []
    monkeypatch.setattr(runners_mod, "configure_logs_option", calls.append)
    cfg = _write_watershed_fixture(tmp_path)

    result = runner.invoke(app, ["tideweaver", "run", str(cfg), "--logs"])

    assert result.exit_code == 0, result.stdout
    assert calls == [True]


def test_cli_tideweaver_run_without_logs_flag_calls_helper_with_false(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Without --logs, `configure_logs_option` is still called, but with `enabled=False` (a no-op)."""
    from incorporator.cli import runners as runners_mod

    calls: list[bool] = []
    monkeypatch.setattr(runners_mod, "configure_logs_option", calls.append)
    cfg = _write_watershed_fixture(tmp_path)

    result = runner.invoke(app, ["tideweaver", "run", str(cfg)])

    assert result.exit_code == 0, result.stdout
    assert calls == [False]


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


# ---------------------------------------------------------------------------
# G2 regression: missing inc_file → non-zero exit; clean empty run → exit 0
# ---------------------------------------------------------------------------


def _write_watershed_with_missing_inc_file(tmp_path: Path) -> Path:
    """Write a watershed.json that references a non-existent inc_file.

    The current uses verb='stream' with an inc_file that does not exist on
    disk.  When the Tideweaver runs, the stream will emit a wave with
    failed_sources (file-not-found) and produce zero rows — triggering G2.
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
                        "incorp_params": {
                            "inc_file": "does_not_exist.json",
                            "inc_code": "id",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return cfg


def test_cli_tideweaver_run_missing_inc_file_exits_nonzero(tmp_path: Path) -> None:
    """G2: a watershed run that fails to load every source exits non-zero.

    Writes a watershed.json with an inc_file that does not exist.  The
    stream produces zero rows with failed_sources populated, which the
    scheduler records as a SourceLoadFailure reject.  The CLI must detect
    this and exit with code 1 + a summary message.
    """
    cfg = _write_watershed_with_missing_inc_file(tmp_path)
    result = runner.invoke(app, ["tideweaver", "run", str(cfg), "--json-output"])
    assert result.exit_code == 1, f"Expected exit 1 for missing inc_file; got {result.exit_code}: {result.stdout}"


def test_cli_tideweaver_run_clean_empty_run_exits_zero(tmp_path: Path) -> None:
    """G2: a watershed run that produces zero rows WITHOUT source failure exits 0.

    Uses the standard fixture where the current has no incorp source at all
    (empty incorp_params), so the stream produces no rows cleanly — no
    failed_sources.  The CLI must exit 0.
    """
    cfg = _write_watershed_fixture(tmp_path)
    result = runner.invoke(app, ["tideweaver", "run", str(cfg), "--json-output"])
    assert result.exit_code == 0, f"Expected exit 0 for clean empty run; got {result.exit_code}: {result.stdout}"


# ---------------------------------------------------------------------------
# Platform-review Stage 0 pin: build_watershed / host-penstock invocation count
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _restore_host_penstock_registry() -> Iterator[None]:
    """Snapshot/restore the process-global penstock registry around each test.

    Mutates ``_HOST_PENSTOCKS`` in place; never reassigns — every importer,
    including ``resolve_penstock``, holds a direct reference to this exact
    dict object (see ``tests/public/api/test_crypto_graph_etl.py``'s
    identical idiom). The fixture watershed used in this file carries no
    ``host_penstocks`` block today, but this is cheap insurance against a
    future fixture edit leaking state into ``tests/test_penstock_registry.py``
    or ``tests/test_security.py``.
    """
    snapshot = dict(_HOST_PENSTOCKS)
    yield
    _HOST_PENSTOCKS.clear()
    _HOST_PENSTOCKS.update(snapshot)


def test_cli_tideweaver_run_calls_register_host_penstocks_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`tideweaver run` invokes host-penstock registration exactly ONCE per run.

    Stage 2C of the platform-review program fixed ``_run_tideweaver``
    (cli/tideweaver.py) to build the Watershed exactly once: it now reuses
    the ``Watershed`` returned by ``_run_validation`` ->
    ``validate_config`` -> ``_validate_and_build_watershed`` ->
    ``build_watershed`` instead of calling ``build_watershed`` a second time
    to construct the Watershed it runs.  ``_register_host_penstocks`` fires
    inside ``build_watershed``, so the invocation count is now 1.

    Counts INVOCATIONS via a wrapping monkeypatch, not end-registry-state,
    because ``_register_host_penstocks`` is a plain dict overwrite — a
    second call would be invisible to a state-based assertion.
    """
    from incorporator.tideweaver import config as tw_config

    cfg = _write_watershed_fixture(tmp_path)

    call_count = 0
    real_register = tw_config._register_host_penstocks

    def _counting_register(raw: Any) -> None:
        nonlocal call_count
        call_count += 1
        real_register(raw)

    monkeypatch.setattr(tw_config, "_register_host_penstocks", _counting_register)

    result = runner.invoke(app, ["tideweaver", "run", str(cfg), "--json-output"])

    assert result.exit_code == 0, result.stdout
    assert call_count == 1, (
        f"Expected exactly 1 build_watershed/_register_host_penstocks invocation per "
        f"`tideweaver run` (the Watershed is now built once and reused); got {call_count}."
    )
