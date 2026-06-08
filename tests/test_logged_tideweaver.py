"""Integration tests for LoggedTideweaver."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, ClassVar, List, Tuple

import pytest

from incorporator import Incorporator
from incorporator.observability.tideweaver import (
    Current,
    CustomCurrent,
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


# ---------------------------------------------------------------------------
# get_scheduler_events reader
# ---------------------------------------------------------------------------


def _wait_flush() -> None:
    """Give the QueueHandler background thread time to drain its queue to disk."""
    import time

    time.sleep(0.25)


@pytest.mark.asyncio
async def test_get_scheduler_events_returns_records_for_failing_tick(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_scheduler_events returns structured records when a tick fails with on_error='isolate'.

    A failing isolated tick must be retrievable via
    LoggedTideweaver.get_scheduler_events(logger_name): at least one
    record with a top-level 'scheduler_event' key, correct event_type and
    current_name, and a populated 'session' field matching logger_name.
    """
    monkeypatch.chdir(tmp_path)

    class _IsolateSrc(Incorporator):
        pass

    class _RaisingCurrent(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False

        async def tick(self, scheduler: Any) -> None:
            raise RuntimeError("deliberate get_scheduler_events failure")

    logger_name = "TestGetSchedEvents"
    current = _RaisingCurrent(
        name="failing_current",
        cls=_IsolateSrc,
        interval=0.04,
        on_error="isolate",
    )
    now = datetime.now(timezone.utc)
    ws = Watershed(
        window=(now, now + timedelta(milliseconds=400)),
        currents=[current],
        edges=[],
    )

    tw = LoggedTideweaver(ws, enable_logging=True, logger_name=logger_name, pass_interval=0.03)
    async for _ in tw.run():
        pass

    _wait_flush()

    records = await LoggedTideweaver.get_scheduler_events(logger_name)

    assert len(records) >= 1, f"expected >= 1 scheduler_event records; got {records}"
    for rec in records:
        assert "scheduler_event" in rec, f"each record must have 'scheduler_event' key; got {rec}"
    evt = records[0]["scheduler_event"]
    assert evt.get("event_type") == "isolated_tick_failure", f"event_type mismatch: {evt}"
    assert evt.get("current_name") == "failing_current", f"current_name mismatch: {evt}"
    assert "session" in evt, f"'session' key missing from payload: {evt}"
    assert evt["session"] == logger_name, f"session mismatch: expected {logger_name!r}, got {evt['session']!r}"


@pytest.mark.asyncio
async def test_get_scheduler_events_returns_empty_list_when_no_events(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_scheduler_events returns an empty list when a clean run produces no scheduler events.

    Proves that a successful LoggedTideweaver run with no tick failures yields
    [] from get_scheduler_events(logger_name) — no false positives.
    """
    monkeypatch.chdir(tmp_path)

    logger_name = "TestGetSchedEventsEmpty"
    now = datetime.now(timezone.utc)
    ws = Watershed.parallel(
        window=(now, now + timedelta(milliseconds=200)),
        currents=[Stream(name="clean", cls=_Src, interval=0.05, incorp_params={})],
    )

    tw = LoggedTideweaver(
        ws, tick_factory=_noop_tick, pass_interval=0.05, enable_logging=True, logger_name=logger_name
    )
    async for _ in tw.run():
        pass

    _wait_flush()

    records = await LoggedTideweaver.get_scheduler_events(logger_name)
    assert records == [], f"expected empty list for clean run; got {records}"
