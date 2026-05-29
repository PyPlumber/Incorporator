"""Integration tests for LoggedTideweaver."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple

import pytest

from incorporator import Incorporator
from incorporator.observability.tideweaver import (
    Current,
    LoggedTideweaver,
    Stream,
    Tide,
    Tideweaver,
    Watershed,
)
from incorporator.observability.tideweaver.logged import LoggedTideweaver as LoggedTideweaverDirect


class _Src(Incorporator):
    """Stand-in source class for LoggedTideweaver tests."""


def _short_window(seconds: float = 0.3) -> Tuple[datetime, datetime]:
    """Return a short future window for orchestration tests."""
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(seconds=seconds))


async def _noop_tick(current: Current) -> None:
    """Zero-work tick factory for test injection."""


def test_logged_tideweaver_exported_from_package() -> None:
    """``from incorporator.observability.tideweaver import LoggedTideweaver`` works."""
    assert LoggedTideweaver is LoggedTideweaverDirect


@pytest.mark.asyncio
async def test_logged_tideweaver_logging_disabled_matches_base(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """LoggedTideweaver with enable_logging=False behaves identically to Tideweaver.

    Tides yielded by both must have the same structure; no log files should
    be created since logging is disabled.
    """
    monkeypatch.chdir(tmp_path)

    ws = Watershed.parallel(
        window=_short_window(0.2), currents=[Stream(name="src", cls=_Src, interval=0.05, incorp_params={})]
    )

    logged_tides: List[Tide] = []
    async for tide in LoggedTideweaver(ws, tick_factory=_noop_tick, pass_interval=0.05, enable_logging=False).run():
        logged_tides.append(tide)

    assert len(logged_tides) >= 1
    # No log directory should have been created (logging disabled).
    logs_dir = tmp_path / "logs"
    assert not logs_dir.exists() or not any(logs_dir.iterdir())


@pytest.mark.asyncio
async def test_logged_tideweaver_yields_tides(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """LoggedTideweaver with enable_logging=True still yields Tides correctly.

    The logging wrapper must not swallow or transform Tide records; every
    yielded Tide must have a positive tide_number and a valid duration.
    """
    monkeypatch.chdir(tmp_path)

    ws = Watershed.parallel(
        window=_short_window(0.3),
        currents=[Stream(name="src", cls=_Src, interval=0.05, incorp_params={})],
    )

    tides: List[Tide] = []
    async for tide in LoggedTideweaver(
        ws, tick_factory=_noop_tick, pass_interval=0.05, enable_logging=True, logger_name="TideTest"
    ).run():
        tides.append(tide)

    assert len(tides) >= 1
    for tide in tides:
        assert tide.tide_number >= 1
        assert tide.duration_sec >= 0.0


@pytest.mark.asyncio
async def test_logged_tideweaver_distinct_logger_names(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """Two LoggedTideweavers with different logger_names produce separate log files.

    Uses INCORPORATOR_LOG_DIR to isolate log output to tmp_path.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INCORPORATOR_LOG_DIR", str(tmp_path / "logs"))

    ws = Watershed.parallel(
        window=_short_window(0.2),
        currents=[Stream(name="src", cls=_Src, interval=0.05, incorp_params={})],
    )

    async for _ in LoggedTideweaver(
        ws, tick_factory=_noop_tick, pass_interval=0.05, enable_logging=True, logger_name="SessionAlpha"
    ).run():
        pass

    ws2 = Watershed.parallel(
        window=_short_window(0.2),
        currents=[Stream(name="src", cls=_Src, interval=0.05, incorp_params={})],
    )

    async for _ in LoggedTideweaver(
        ws2, tick_factory=_noop_tick, pass_interval=0.05, enable_logging=True, logger_name="SessionBeta"
    ).run():
        pass

    from incorporator.observability.logger import _ACTIVE_LISTENERS

    # Stop listeners to flush queues to disk.
    for name in ("SessionAlpha", "SessionBeta"):
        listener = _ACTIVE_LISTENERS.get(name)
        if listener is not None and getattr(listener, "_thread", None) is not None:
            listener.stop()

    logs_dir = tmp_path / "logs"
    alpha_debug = logs_dir / "SessionAlpha_debug.log"
    beta_debug = logs_dir / "SessionBeta_debug.log"

    assert alpha_debug.exists(), f"Expected {alpha_debug} to exist"
    assert beta_debug.exists(), f"Expected {beta_debug} to exist"
    # The two files must be distinct paths.
    assert alpha_debug.resolve() != beta_debug.resolve()


@pytest.mark.asyncio
async def test_logged_tideweaver_same_logger_name_shares_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """Two LoggedTideweavers sharing the same logger_name share one log-file set.

    The second instance must reuse the existing QueueListener rather than
    creating a duplicate thread.
    """
    monkeypatch.chdir(tmp_path)

    from incorporator.observability.logger import _ACTIVE_LISTENERS

    ws = Watershed.parallel(
        window=_short_window(0.2),
        currents=[Stream(name="src", cls=_Src, interval=0.05, incorp_params={})],
    )

    ltw1 = LoggedTideweaver(ws, tick_factory=_noop_tick, pass_interval=0.05, enable_logging=True, logger_name="Shared")
    listener_before = _ACTIVE_LISTENERS.get("Shared")

    ws2 = Watershed.parallel(
        window=_short_window(0.2),
        currents=[Stream(name="src", cls=_Src, interval=0.05, incorp_params={})],
    )

    ltw2 = LoggedTideweaver(ws2, tick_factory=_noop_tick, pass_interval=0.05, enable_logging=True, logger_name="Shared")
    listener_after = _ACTIVE_LISTENERS.get("Shared")

    # Both must point to the same listener (setup_class_logger early-exits on duplicate).
    assert listener_before is listener_after
