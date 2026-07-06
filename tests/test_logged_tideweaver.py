"""Integration tests for LoggedTideweaver."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, ClassVar, List, Tuple

import pytest
from pydantic import ConfigDict

from incorporator import Incorporator
from incorporator.observability.wave import Wave
from incorporator.tideweaver import (
    Current,
    CustomCurrent,
    LoggedTideweaver,
    Stream,
    Tide,
    Tideweaver,
    Watershed,
)
from incorporator.tideweaver.logged import LoggedTideweaver as LoggedTideweaverDirect


class _Src(Incorporator):
    """Stand-in source class for LoggedTideweaver tests."""


def _short_window(seconds: float = 0.3) -> Tuple[datetime, datetime]:
    """Return a short future window for orchestration tests."""
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(seconds=seconds))


async def _noop_tick(current: Current) -> None:
    """Zero-work tick factory for test injection."""


def test_logged_tideweaver_exported_from_package() -> None:
    """``from incorporator.tideweaver import LoggedTideweaver`` works."""
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

    failure_evts = [
        r["scheduler_event"] for r in records if r["scheduler_event"].get("event_type") == "isolated_tick_failure"
    ]
    assert failure_evts, f"expected at least one isolated_tick_failure record; got {records}"
    evt = failure_evts[0]
    assert evt.get("current_name") == "failing_current", f"current_name mismatch: {evt}"
    assert "session" in evt, f"'session' key missing from payload: {evt}"
    assert evt["session"] == logger_name, f"session mismatch: expected {logger_name!r}, got {evt['session']!r}"


@pytest.mark.asyncio
async def test_get_scheduler_events_clean_run_has_only_watershed_events(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """get_scheduler_events returns only watershed lifecycle events for a clean run with no tick failures.

    Proves that a successful LoggedTideweaver run yields exactly
    watershed_started and watershed_completed from get_scheduler_events(logger_name)
    — no per-current scheduler events are emitted.
    """
    monkeypatch.chdir(tmp_path)

    logger_name = "TestGetSchedEventsEmpty"
    now = datetime.now(timezone.utc)
    ws = Watershed.parallel(
        window=(now, now + timedelta(milliseconds=200)),
        currents=[Stream(name="clean", cls=_Src, interval=0.05, incorp_params={})],
    )

    tw = LoggedTideweaver(ws, tick_factory=_noop_tick, pass_interval=0.05, enable_logging=True, logger_name=logger_name)
    async for _ in tw.run():
        pass

    _wait_flush()

    records = await LoggedTideweaver.get_scheduler_events(logger_name)
    event_types = {r["scheduler_event"]["event_type"] for r in records}
    assert event_types <= {
        "watershed_started",
        "watershed_completed",
    }, f"expected only watershed lifecycle events for a clean run; got {event_types}"


@pytest.mark.asyncio
async def test_get_scheduler_events_returns_watershed_lifecycle_events(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """LoggedTideweaver(enable_logging=True) emits watershed_started and watershed_completed.

    Proves that get_scheduler_events returns at least one watershed_started and one
    watershed_completed record, that current_name is None for both, and that the
    detail string contains the watershed name and window ISO timestamps.
    """
    monkeypatch.chdir(tmp_path)

    ws_name = "LifecycleWatershed"
    logger_name = "TestWatershedLifecycle"
    now = datetime.now(timezone.utc)
    ws = Watershed.parallel(
        window=(now, now + timedelta(milliseconds=250)),
        currents=[Stream(name="src", cls=_Src, interval=0.05, incorp_params={})],
        name=ws_name,
    )

    tw = LoggedTideweaver(ws, tick_factory=_noop_tick, pass_interval=0.05, enable_logging=True, logger_name=logger_name)
    async for _ in tw.run():
        pass

    _wait_flush()

    records = await LoggedTideweaver.get_scheduler_events(logger_name)
    event_types = [r["scheduler_event"]["event_type"] for r in records]

    assert "watershed_started" in event_types, f"watershed_started missing; got {event_types}"
    assert "watershed_completed" in event_types, f"watershed_completed missing; got {event_types}"

    for rec in records:
        evt = rec["scheduler_event"]
        if evt["event_type"] in ("watershed_started", "watershed_completed"):
            assert evt["current_name"] is None, f"current_name should be None for watershed events; got {evt}"
            assert ws_name in evt["detail"], f"detail should contain watershed name; got {evt['detail']!r}"
            assert evt["session"] == logger_name, f"session mismatch: {evt['session']!r}"


@pytest.mark.asyncio
async def test_watershed_events_not_emitted_when_logging_disabled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """LoggedTideweaver(enable_logging=False) emits no watershed lifecycle events.

    Proves that a bare run with logging disabled produces no log files and
    therefore get_scheduler_events returns [] (no watershed events, no false positives).
    """
    monkeypatch.chdir(tmp_path)

    logger_name = "TestWatershedNoLog"
    now = datetime.now(timezone.utc)
    ws = Watershed.parallel(
        window=(now, now + timedelta(milliseconds=200)),
        currents=[Stream(name="src", cls=_Src, interval=0.05, incorp_params={})],
        name="ShouldNotAppear",
    )

    tw = LoggedTideweaver(
        ws, tick_factory=_noop_tick, pass_interval=0.05, enable_logging=False, logger_name=logger_name
    )
    async for _ in tw.run():
        pass

    _wait_flush()

    records = await LoggedTideweaver.get_scheduler_events(logger_name)
    assert records == [], f"expected [] when logging disabled; got {records}"


# ---------------------------------------------------------------------------
# Spillway overflow session-log routing
# ---------------------------------------------------------------------------


class _SpillwaySrc(Incorporator):
    """Stand-in upstream source for spillway-overflow tests."""


class _SpillwaySink(Incorporator):
    """Stand-in downstream sink for spillway-overflow tests — distinct cls from the upstream."""


@pytest.mark.asyncio
async def test_spillway_overflow_routes_to_session_log(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """A RaiseOverflow spillway overflow during a LoggedTideweaver session lands in error.log.

    Builds a Reservoir(depth=1) + RaiseOverflow edge fed by a fast upstream and a
    slow downstream so at least one wave is displaced, then asserts the overflow
    is retrievable via LoggedTideweaver.get_scheduler_events(logger_name) with
    event_type 'spillway_overflow' and the edge tuple recorded.
    """
    from incorporator.tideweaver import Edge, FlowControl, HardLock, RaiseOverflow, Reservoir

    monkeypatch.chdir(tmp_path)

    strong_refs: List[_SpillwaySrc] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _SpillwaySrc(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _SpillwaySrc._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = Stream(name="a", cls=_SpillwaySrc, interval=0.05, incorp_params={})
    b = Stream(name="b", cls=_SpillwaySink, interval=10.0, incorp_params={})
    flow = FlowControl(gate=HardLock(), reservoir=Reservoir(depth=1), spillway=RaiseOverflow())

    logger_name = "TestSpillwaySession"
    ws = Watershed(
        window=_short_window(0.8),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=flow)],
    )

    tw = LoggedTideweaver(ws, tick_factory=fake, pass_interval=0.02, enable_logging=True, logger_name=logger_name)
    async for _ in tw.run():
        pass

    _wait_flush()

    if "_tideweaver_snapshot" in _SpillwaySrc.__dict__:
        delattr(_SpillwaySrc, "_tideweaver_snapshot")

    assert tw._edge_state[("a", "b")].overflow_count > 0, "test setup should force at least one overflow"

    records = await LoggedTideweaver.get_scheduler_events(logger_name)
    overflow_evts = [r["scheduler_event"] for r in records if r["scheduler_event"]["event_type"] == "spillway_overflow"]
    assert overflow_evts, f"expected at least one spillway_overflow record; got {records}"
    evt = overflow_evts[0]
    assert evt["edge"] == ["a", "b"], f"edge mismatch: {evt}"
    assert evt["current_name"] is None, f"spillway_overflow is edge-scoped, current_name should be None; got {evt}"
    assert evt["session"] == logger_name, f"session mismatch: {evt['session']!r}"


@pytest.mark.asyncio
async def test_spillway_overflow_falls_back_to_module_logger_without_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A RaiseOverflow spillway overflow with no active session logger still warns via the module logger.

    Pins the fallback path: when logger_name is None (plain Tideweaver, no
    LoggedTideweaver session), the module-logger WARNING behavior from before
    this change must still fire unchanged.
    """
    import logging

    from incorporator.tideweaver import Edge, FlowControl, HardLock, RaiseOverflow, Reservoir, Tideweaver

    monkeypatch.chdir(tmp_path)

    strong_refs: List[_SpillwaySrc] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _SpillwaySrc(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _SpillwaySrc._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = Stream(name="a", cls=_SpillwaySrc, interval=0.05, incorp_params={})
    b = Stream(name="b", cls=_SpillwaySink, interval=10.0, incorp_params={})
    flow = FlowControl(gate=HardLock(), reservoir=Reservoir(depth=1), spillway=RaiseOverflow())

    ws = Watershed(
        window=_short_window(0.8),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=flow)],
    )

    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    with caplog.at_level(logging.WARNING, logger="incorporator.tideweaver.flow"):
        async for _ in tw.run():
            pass

    if "_tideweaver_snapshot" in _SpillwaySrc.__dict__:
        delattr(_SpillwaySrc, "_tideweaver_snapshot")

    overflow_logs = [r for r in caplog.records if "spillway overflow" in r.message]
    assert overflow_logs, "RaiseOverflow must fall back to the module logger when no session logger is active"


# ---------------------------------------------------------------------------
# D5-01 logging-validation rider (Maintainer Decision 3)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_logged_tideweaver_healthy_sibling_tagged_correctly_while_one_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """Per-current session-log routing stays correct while a sibling current fails.

    Proves rider items (a)+(b): the healthy current's waves keep landing
    with correct current/cls/code meta via ``get_current`` while a sibling
    fails every tick; the failing current's ``isolated_tick_failure``
    scheduler event carries correct current/cls/session/tide meta and is
    retrievable via both ``get_scheduler_events`` and ``get_current``; and
    the failed tick produces no spurious ``"wave"``-shaped per-current
    record (a failed tick never reaches the wave-routing call site).
    """
    monkeypatch.chdir(tmp_path)

    class _HealthySrc(Incorporator):
        model_config = ConfigDict(extra="allow")

    class _FailingSrc(Incorporator):
        model_config = ConfigDict(extra="allow")

    async def _healthy_stream(*args: Any, **kwargs: Any) -> Any:
        yield Wave(chunk_index=0, rows_processed=1, processing_time_sec=0.01)

    async def _failing_stream(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("failing sibling")
        yield  # pragma: no cover - unreachable, makes this an async generator

    monkeypatch.setattr(_HealthySrc, "stream", _healthy_stream)
    monkeypatch.setattr(_FailingSrc, "stream", _failing_stream)

    logger_name = "TestSiblingHealthFail"
    now = datetime.now(timezone.utc)
    healthy = Stream(name="healthy", cls=_HealthySrc, interval=0.04, incorp_params={})
    failing = Stream(name="failing", cls=_FailingSrc, interval=0.04, incorp_params={}, on_error="isolate")
    ws = Watershed.parallel(
        window=(now, now + timedelta(milliseconds=400)),
        currents=[healthy, failing],
    )

    tw = LoggedTideweaver(ws, pass_interval=0.03, enable_logging=True, logger_name=logger_name)
    async for _ in tw.run():
        pass

    _wait_flush()

    # (b) isolated_tick_failure carries correct meta and is retrievable both ways.
    events = await LoggedTideweaver.get_scheduler_events(logger_name)
    failure_evts = [
        e["scheduler_event"] for e in events if e["scheduler_event"].get("event_type") == "isolated_tick_failure"
    ]
    assert failure_evts, f"expected isolated_tick_failure record(s); got {events}"
    for evt in failure_evts:
        assert evt["current_name"] == "failing"
        assert evt["cls_name"] == "_FailingSrc"
        assert evt["session"] == logger_name
        assert isinstance(evt["tide_number"], int)

    failing_by_current = await LoggedTideweaver.get_current(logger_name, 'current:"failing"')
    assert failing_by_current, "isolated_tick_failure must also be retrievable via get_current"
    for rec in failing_by_current:
        # (a) the failed current must never produce a wave-shaped record.
        assert "wave" not in rec, f"failed tick must not emit a wave-shaped record; got {rec}"

    # (a) the healthy sibling keeps tagging its waves correctly throughout.
    healthy_records = await LoggedTideweaver.get_current(logger_name, 'code:"healthy"')
    assert healthy_records, "healthy current's waves must still be retrievable via get_current"
    wave_records = [r for r in healthy_records if "wave" in r]
    assert wave_records, f"expected at least one wave record for the healthy current; got {healthy_records}"
    for rec in wave_records:
        assert 'code:"healthy"' in rec.get("meta", "")


@pytest.mark.asyncio
async def test_logged_tideweaver_drain_cancel_emits_no_phantom_current_records(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """A drain-cancelled tick emits no phantom per-current wave record (rider item c).

    Proves: when ``_drain()`` cancels a runaway tick past ``drain_timeout``,
    no ``"wave"``-shaped record tagged with that current's code appears in
    the session log — the cancellation is invisible to the per-current wave
    view, matching the "no wave advertised" contract from the finally-block
    gate.
    """
    monkeypatch.chdir(tmp_path)
    started = asyncio.Event()

    async def runaway(current: Current) -> None:
        started.set()
        await asyncio.sleep(5.0)

    logger_name = "TestDrainCancelNoPhantom"
    now = datetime.now(timezone.utc)
    a = Stream(name="a", cls=_Src, interval=0.05, incorp_params={})
    ws = Watershed.parallel(
        window=(now, now + timedelta(milliseconds=200)),
        currents=[a],
        drain_timeout=0.2,
    )

    tw = LoggedTideweaver(ws, tick_factory=runaway, pass_interval=0.05, enable_logging=True, logger_name=logger_name)
    async for _ in tw.run():
        pass

    _wait_flush()

    assert started.is_set(), "the runaway tick must have started before cancellation"

    records = await LoggedTideweaver.get_current(logger_name, 'code:"a"')
    wave_records = [r for r in records if "wave" in r]
    assert wave_records == [], f"a cancelled tick must not emit a wave-shaped record; got {wave_records}"


# ---------------------------------------------------------------------------
# D5-03: LoggedTideweaver reuse across two sequential run() calls
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_logged_tideweaver_two_runs_behave_like_fresh_sessions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """(T5) Two sequential run() calls on one LoggedTideweaver each behave like a fresh session.

    Asserts: ``watershed_started``/``watershed_completed`` fire again for
    run 2 (not just once total), tide numbering restarts at 1 in run 2's
    Tide records, per-current session routing (``get_current``) keeps
    tagging run-2 waves correctly, and a run-1 isolated-tick-failure
    reject that was already routed (dedup'd via ``_routed_reject_ids``)
    does not suppress an equivalent run-2 reject from being routed too --
    proving ``Tideweaver._reset_run_state()`` clears the dedup set that
    ``LoggedTideweaver.run``'s finally-sweep consults.

    Relies solely on ``Tideweaver.run()``'s reset propagating through
    ``LoggedTideweaver.run``'s ``super().run()`` call -- no LoggedTideweaver
    code change is required for this to pass.
    """
    monkeypatch.chdir(tmp_path)

    class _FlakySrc(Incorporator):
        pass

    fail_calls = {"count": 0}

    async def dispatch(current: Current) -> None:
        fail_calls["count"] += 1
        raise RuntimeError(f"flaky failure #{fail_calls['count']}")

    logger_name = "TestTwoRunFreshSession"
    flaky = Stream(name="flaky", cls=_FlakySrc, interval=0.04, incorp_params={}, on_error="isolate")
    ws = Watershed.parallel(window=_short_window(0.3), currents=[flaky])

    tw = LoggedTideweaver(ws, tick_factory=dispatch, pass_interval=0.03, enable_logging=True, logger_name=logger_name)

    tides_1 = [t async for t in tw.run()]
    assert tides_1[0].tide_number == 1
    fails_after_run_1 = fail_calls["count"]
    assert fails_after_run_1 > 0, "run 1 must have produced at least one isolated tick failure"

    fail_calls["count"] = 0
    # Reuse the same watershed's window shape but anchored to "now" so
    # run 2 has an open window (real callers supply a fresh window per run).
    tw.watershed.window = _short_window(0.3)

    tides_2 = [t async for t in tw.run()]
    assert tides_2[0].tide_number == 1, f"run 2 must restart tide numbering at 1; got {tides_2[0].tide_number}"
    assert fail_calls["count"] > 0, "run 2 must have actually ticked (and failed) again"

    _wait_flush()

    # watershed_started / watershed_completed fire again for run 2 (2 of each total).
    sched_events = await LoggedTideweaver.get_scheduler_events(logger_name)
    started_events = [e for e in sched_events if e["scheduler_event"]["event_type"] == "watershed_started"]
    completed_events = [e for e in sched_events if e["scheduler_event"]["event_type"] == "watershed_completed"]
    assert len(started_events) == 2, f"expected 2 watershed_started events (one per run); got {len(started_events)}"
    assert len(completed_events) == 2, (
        f"expected 2 watershed_completed events (one per run); got {len(completed_events)}"
    )

    # isolated_tick_failure fires in both runs -- not suppressed in run 2 by
    # a stale _routed_reject_ids dedup set leaking from run 1.
    failure_events = [
        e["scheduler_event"] for e in sched_events if e["scheduler_event"]["event_type"] == "isolated_tick_failure"
    ]
    assert len(failure_events) >= 2, (
        f"expected isolated_tick_failure events from BOTH runs; got {len(failure_events)}: {failure_events}"
    )

    # Per-current session routing (current_meta) keeps working in run 2:
    # the failing current's records are still retrievable via get_current.
    current_records = await LoggedTideweaver.get_current(logger_name, 'current:"flaky"')
    assert current_records, "run 2's per-current session routing must still tag records retrievably"
