"""Tests for the ``incorporator tideweaver run --logs`` flag and related CLI fixes.

Covers:
* Finding 2 — ``--logs`` must build a :class:`LoggedTideweaver`, not a bare
  :class:`Tideweaver` + ``logging.basicConfig``.
* ``_run_tideweaver(logs=False)`` still builds a bare :class:`Tideweaver`.
* The ``logs`` kwarg is forwarded correctly from the Typer command body to
  ``_run_tideweaver``.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from incorporator import Incorporator
from incorporator.cli.tideweaver import _run_tideweaver
from incorporator.observability.tideweaver import LoggedTideweaver, Tideweaver, Watershed
from incorporator.observability.tideweaver.current import Stream


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


class _Source(Incorporator):
    """Stand-in source class for CLI runner tests."""


def _future_window() -> tuple[datetime, datetime]:
    """Return a short future window; scheduler exits immediately (no ticks)."""
    now = datetime.now(timezone.utc)
    return (now + timedelta(hours=1), now + timedelta(hours=2))


def _write_minimal_ws(tmp_path: Path) -> Path:
    """Write a minimal watershed.json with a past-end window so the run exits immediately."""
    outflow = tmp_path / "outflow.py"
    outflow.write_text(
        "from incorporator import Incorporator\nclass _Source(Incorporator):\n    pass\n",
        encoding="utf-8",
    )
    now = datetime.now(timezone.utc)
    # End the window 1 second in the past so Tideweaver.run() exits without
    # attempting any ticks — the window has already closed.
    body: dict[str, Any] = {
        "window": {
            "start": (now - timedelta(seconds=2)).isoformat(),
            "end": (now - timedelta(seconds=1)).isoformat(),
        },
        "shape": "parallel",
        "outflow": "outflow.py",
        "currents": [
            {"name": "src", "class": "_Source", "verb": "stream", "interval": 30, "incorp_params": {}},
        ],
    }
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    return cfg


# ---------------------------------------------------------------------------
# Unit tests: _run_tideweaver scheduler-type selection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_tideweaver_logs_false_builds_bare_tideweaver(tmp_path: Path) -> None:
    """``_run_tideweaver(logs=False)`` builds a bare :class:`Tideweaver`, not ``LoggedTideweaver``.

    Proves the ``logs=False`` default preserves the pre-fix behaviour: no
    ``QueueHandler`` setup, no disk writes, bare scheduler.
    """
    monkeypatched_instances: list[Tideweaver] = []

    async def _capture_run(self: Tideweaver) -> Any:  # type: ignore[return]
        monkeypatched_instances.append(self)
        # Return an async iterator that yields nothing so the `async for` in
        # _run_tideweaver terminates immediately without error.
        return
        yield  # pragma: no cover  — unreachable; makes this an async generator

    cfg = _write_minimal_ws(tmp_path)
    with patch("incorporator.cli.tideweaver.Tideweaver.run", _capture_run):
        await _run_tideweaver(cfg, json_output=False, heartbeat_file=None, logs=False)

    assert monkeypatched_instances, "Tideweaver.run must have been called"
    tw = monkeypatched_instances[0]
    assert type(tw) is Tideweaver, (
        f"logs=False must build bare Tideweaver; got {type(tw).__name__}"
    )


@pytest.mark.asyncio
async def test_run_tideweaver_logs_true_builds_logged_tideweaver(tmp_path: Path) -> None:
    """``_run_tideweaver(logs=True)`` builds a :class:`LoggedTideweaver` with ``enable_logging=True``.

    Proves Finding 2 is fixed: the ``--logs`` flag no longer routes through
    ``logging.basicConfig`` + bare ``Tideweaver``; it now constructs a
    ``LoggedTideweaver`` so the :class:`~logging.handlers.QueueHandler`-backed
    background thread receives every :class:`Tide` and :class:`~incorporator.RejectEntry`.
    """
    captured: list[Tideweaver] = []

    async def _capture_run(self: Tideweaver) -> Any:  # type: ignore[return]
        captured.append(self)
        return
        yield

    cfg = _write_minimal_ws(tmp_path)
    with patch("incorporator.observability.tideweaver.logged.LoggedTideweaver.run", _capture_run):
        await _run_tideweaver(cfg, json_output=False, heartbeat_file=None, logs=True)

    assert captured, "LoggedTideweaver.run must have been called"
    tw = captured[0]
    assert isinstance(tw, LoggedTideweaver), (
        f"logs=True must build LoggedTideweaver; got {type(tw).__name__}"
    )
    assert tw._enable_logging is True, "enable_logging must be True when logs=True"


@pytest.mark.asyncio
async def test_logged_tideweaver_defaults_logger_name_from_watershed_name(tmp_path: Path) -> None:
    """LoggedTideweaver defaults ``_logger_name`` to ``watershed.name`` when no explicit name is passed.

    Proves the Commit 2 resolution chain: watershed.name='MySession' →
    resolved_name='MySession' when logger_name kwarg is omitted.
    """
    from datetime import timedelta

    now = datetime.now(timezone.utc)
    ws = Watershed(
        window=(now + timedelta(hours=1), now + timedelta(hours=2)),
        name="MySession",
        currents=[
            Stream(name="src", cls=_Source, interval=30.0, incorp_params={}),
        ],
    )
    tw = LoggedTideweaver(ws, enable_logging=False)
    assert tw._logger_name == "MySession", (
        f"_logger_name must default to watershed.name; got {tw._logger_name!r}"
    )


@pytest.mark.asyncio
async def test_logged_tideweaver_explicit_logger_name_wins_over_watershed_name(tmp_path: Path) -> None:
    """An explicit logger_name kwarg beats watershed.name in LoggedTideweaver.

    Proves the resolution chain: explicit logger_name='Override' wins over
    watershed.name='MySession'.
    """
    from datetime import timedelta

    now = datetime.now(timezone.utc)
    ws = Watershed(
        window=(now + timedelta(hours=1), now + timedelta(hours=2)),
        name="MySession",
        currents=[
            Stream(name="src", cls=_Source, interval=30.0, incorp_params={}),
        ],
    )
    tw = LoggedTideweaver(ws, enable_logging=False, logger_name="Override")
    assert tw._logger_name == "Override", (
        f"explicit logger_name must win; got {tw._logger_name!r}"
    )


@pytest.mark.asyncio
async def test_logged_tideweaver_falls_back_to_tideweaver_when_no_name(tmp_path: Path) -> None:
    """LoggedTideweaver resolves to 'Tideweaver' when both watershed.name and logger_name are absent.

    Proves the fallback: no watershed.name and no explicit logger_name → 'Tideweaver'.
    """
    from datetime import timedelta

    now = datetime.now(timezone.utc)
    ws = Watershed(
        window=(now + timedelta(hours=1), now + timedelta(hours=2)),
        currents=[
            Stream(name="src", cls=_Source, interval=30.0, incorp_params={}),
        ],
    )
    tw = LoggedTideweaver(ws, enable_logging=False)
    assert tw._logger_name == "Tideweaver", (
        f"fallback must be 'Tideweaver'; got {tw._logger_name!r}"
    )


@pytest.mark.asyncio
async def test_run_tideweaver_logs_true_does_not_call_basicconfig(tmp_path: Path) -> None:
    """``_run_tideweaver(logs=True)`` must not call ``logging.basicConfig``.

    ``LoggedTideweaver.__init__`` with ``enable_logging=True`` calls
    ``setup_class_logger`` internally, which installs the QueueHandler pipeline.
    A redundant ``basicConfig`` call would bypass it and route to the root
    handler instead.
    """
    cfg = _write_minimal_ws(tmp_path)

    async def _noop_run(self: Tideweaver) -> Any:  # type: ignore[return]
        return
        yield

    with (
        patch("incorporator.observability.tideweaver.logged.LoggedTideweaver.run", _noop_run),
        patch("logging.basicConfig") as mock_basic,
    ):
        await _run_tideweaver(cfg, json_output=False, heartbeat_file=None, logs=True)

    mock_basic.assert_not_called()


@pytest.mark.asyncio
async def test_run_tideweaver_logs_false_does_not_call_basicconfig(tmp_path: Path) -> None:
    """``_run_tideweaver(logs=False)`` must not call ``logging.basicConfig`` either.

    Regression guard: the pre-fix code only called basicConfig when ``logs=True``,
    but after the fix neither path should touch it.
    """
    cfg = _write_minimal_ws(tmp_path)

    async def _noop_run(self: Tideweaver) -> Any:  # type: ignore[return]
        return
        yield

    with (
        patch("incorporator.cli.tideweaver.Tideweaver.run", _noop_run),
        patch("logging.basicConfig") as mock_basic,
    ):
        await _run_tideweaver(cfg, json_output=False, heartbeat_file=None, logs=False)

    mock_basic.assert_not_called()


@pytest.mark.asyncio
async def test_drain_timeout_assignment_precedes_scheduler_construction(tmp_path: Path) -> None:
    """``drain_timeout_override`` is applied to the ``Watershed`` before the scheduler is built.

    The scheduler reads ``watershed.drain_timeout`` at ``__init__`` time.
    If the override were applied AFTER construction, the drain contract would
    be wrong.  This test confirms the assignment order is preserved for
    both the ``logs=True`` and ``logs=False`` paths.
    """
    observed_drain: list[float | None] = []

    real_tideweaver_init = Tideweaver.__init__

    def _capture_init(self: Tideweaver, watershed: Watershed, **kwargs: Any) -> None:
        observed_drain.append(watershed.drain_timeout)
        real_tideweaver_init(self, watershed, **kwargs)

    async def _noop_run(self: Tideweaver) -> Any:  # type: ignore[return]
        return
        yield

    cfg = _write_minimal_ws(tmp_path)
    with (
        patch("incorporator.cli.tideweaver.Tideweaver.__init__", _capture_init),
        patch("incorporator.cli.tideweaver.Tideweaver.run", _noop_run),
    ):
        await _run_tideweaver(cfg, json_output=False, heartbeat_file=None, drain_timeout_override=77.0)

    assert observed_drain, "Tideweaver.__init__ must have been called"
    assert observed_drain[0] == pytest.approx(77.0), (
        f"drain_timeout must be set before scheduler init; got {observed_drain[0]}"
    )


# ---------------------------------------------------------------------------
# CLI integration: --logs flag is forwarded from the Typer command body
# ---------------------------------------------------------------------------


def test_cli_logs_flag_forwarded_to_run_tideweaver(tmp_path: Path) -> None:
    """``incorporator tideweaver run --logs <ws.json>`` passes ``logs=True`` to ``_run_tideweaver``.

    Confirms the Typer command body no longer calls ``logging.basicConfig``
    and does forward the flag.  Uses ``typer.testing.CliRunner`` + patch of
    ``_run_tideweaver`` to stay in the synchronous CLI test layer.
    """
    from typer.testing import CliRunner

    from incorporator.cli import app

    cfg = _write_minimal_ws(tmp_path)
    captured_kwargs: list[dict[str, Any]] = []

    async def _fake_run(config_path: Path, **kwargs: Any) -> None:
        captured_kwargs.append(kwargs)

    runner = CliRunner()
    with patch("incorporator.cli.tideweaver._run_tideweaver", _fake_run):
        result = runner.invoke(app, ["tideweaver", "run", "--logs", str(cfg)])

    assert captured_kwargs, f"_run_tideweaver was not called; stdout={result.stdout!r}"
    assert captured_kwargs[0].get("logs") is True, (
        f"--logs flag must forward logs=True; got kwargs={captured_kwargs[0]}"
    )


def test_cli_no_logs_flag_forwards_false(tmp_path: Path) -> None:
    """``incorporator tideweaver run <ws.json>`` (no ``--logs``) passes ``logs=False``.

    Confirms the default path is unaffected by the fix.
    """
    from typer.testing import CliRunner

    from incorporator.cli import app

    cfg = _write_minimal_ws(tmp_path)
    captured_kwargs: list[dict[str, Any]] = []

    async def _fake_run(config_path: Path, **kwargs: Any) -> None:
        captured_kwargs.append(kwargs)

    runner = CliRunner()
    with patch("incorporator.cli.tideweaver._run_tideweaver", _fake_run):
        result = runner.invoke(app, ["tideweaver", "run", str(cfg)])

    assert captured_kwargs, f"_run_tideweaver was not called; stdout={result.stdout!r}"
    assert captured_kwargs[0].get("logs") is False, (
        f"no --logs flag must forward logs=False; got kwargs={captured_kwargs[0]}"
    )
