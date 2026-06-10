"""Verification gates for Phase 2 scheduler-event structured logging.

Three tests covering the two routing paths:

A. LoggedTideweaver + on_error="isolate" raising tick → produces a
   "scheduler_event" record with event_type="isolated_tick_failure" in the
   session's error log (structured, retrievable).

B. LoggedTideweaver + on_error="restart" exhausting retries → produces a
   "scheduler_event" record with event_type="tick_parked" in the error log.

C. Bare Tideweaver (no logger_name) + on_error="isolate" raising tick →
   caplog captures the "isolated tick failure" WARNING from the module logger
   (byte-identical to pre-Phase-2 behavior).
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, ClassVar

import pytest

from incorporator import Incorporator, Tideweaver, Watershed
from incorporator.observability.logger import _safe_log_filename
from incorporator.tideweaver import CustomCurrent
from incorporator.tideweaver.logged import LoggedTideweaver

# ---------------------------------------------------------------------------
# Module-level classes so the scheduler builds them once per session.
# ---------------------------------------------------------------------------


class _StableSource(Incorporator):
    """Upstream class that the raising downstream depends on."""


class _BoomSink(Incorporator):
    """Downstream class whose tick always raises."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _short_window(ms: float = 400.0) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(milliseconds=ms))


def _reset_registries(*classes: type[Incorporator]) -> None:
    for cls in classes:
        cls.inc_dict.clear()
        if "_tideweaver_snapshot" in cls.__dict__:
            try:
                delattr(cls, "_tideweaver_snapshot")
            except AttributeError:
                pass


def _read_error_log(logger_name: str) -> list[dict[str, Any]]:
    """Return all JSONL records from the session's error log."""
    filename = _safe_log_filename(logger_name, "error.log")
    path = Path(filename).resolve()
    if not path.is_file():
        return []
    records: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except (json.JSONDecodeError, ValueError):
                    pass
    return records


def _wait_for_log_flush() -> None:
    """Give the QueueHandler background thread time to drain its queue."""
    import time

    time.sleep(0.25)


# ---------------------------------------------------------------------------
# Test A — LoggedTideweaver + isolate raises → scheduler_event in error log
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_logged_tideweaver_isolated_failure_produces_scheduler_event_record(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """LoggedTideweaver with on_error='isolate' raising tick writes a scheduler_event record.

    Proves that when a tick raises and is isolated, _route_scheduler_event_to_log
    is called (because logger_name is set on the base Tideweaver) and the
    structured payload lands in the session's error log with event_type=
    'isolated_tick_failure', retrievable by reading the file and filtering the key.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(_StableSource, _BoomSink)

    up1 = _StableSource(inc_code=1)
    _StableSource._tideweaver_snapshot = [up1]  # type: ignore[attr-defined]

    class _StableUp(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False

        async def tick(self, scheduler: Any) -> None:
            pass

    class _RaisingDown(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False

        async def tick(self, scheduler: Any) -> None:
            raise RuntimeError("deliberate isolated failure")

    logger_name = "TestIsolatedFailure"

    up_current = _StableUp(name="up_a", cls=_StableSource, interval=10.0)
    dn_current = _RaisingDown(
        name="down_a",
        cls=_BoomSink,
        interval=0.04,
        on_error="isolate",
    )

    from incorporator.tideweaver import Edge

    ws = Watershed(
        window=_short_window(400),
        currents=[up_current, dn_current],
        edges=[Edge(from_name="up_a", to_name="down_a")],
    )

    tw = LoggedTideweaver(ws, enable_logging=True, logger_name=logger_name, pass_interval=0.03)
    async for _ in tw.run():
        pass

    _wait_for_log_flush()

    records = _read_error_log(logger_name)
    sched_records = [r for r in records if "scheduler_event" in r]
    isolated_records = [
        r for r in sched_records if r["scheduler_event"].get("event_type") == "isolated_tick_failure"
    ]

    assert len(isolated_records) >= 1, (
        f"expected at least one scheduler_event record with event_type='isolated_tick_failure'; "
        f"got scheduler_event records: {sched_records}"
    )
    evt = isolated_records[0]["scheduler_event"]
    assert evt["current_name"] == "down_a", f"current_name mismatch: {evt}"
    assert "_BoomSink" in (evt.get("cls_name") or ""), f"cls_name missing or wrong: {evt}"


# ---------------------------------------------------------------------------
# Test B — LoggedTideweaver + restart exhausted → tick_parked in error log
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_logged_tideweaver_parked_tick_produces_scheduler_event_record(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """LoggedTideweaver with on_error='restart' exhausting retries writes a tick_parked record.

    Proves that when the tenacity retry loop is exhausted (RetryError path in
    _tick_wrapper), _route_scheduler_event_to_log is called with
    event_type='tick_parked' and the record lands in the session error log.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(_StableSource, _BoomSink)

    logger_name = "TestTickParked"

    class _AlwaysRaises(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False

        async def tick(self, scheduler: Any) -> None:
            raise RuntimeError("always fails")

    dn_current = _AlwaysRaises(
        name="parked_current",
        cls=_BoomSink,
        interval=0.01,
        on_error="restart",
    )

    ws = Watershed(
        window=_short_window(800),
        currents=[dn_current],
        edges=[],
    )

    tw = LoggedTideweaver(ws, enable_logging=True, logger_name=logger_name, pass_interval=0.01)
    async for _ in tw.run():
        pass

    _wait_for_log_flush()

    records = _read_error_log(logger_name)
    sched_records = [r for r in records if "scheduler_event" in r]
    parked_records = [r for r in sched_records if r["scheduler_event"].get("event_type") == "tick_parked"]

    assert len(parked_records) >= 1, (
        f"expected at least one scheduler_event record with event_type='tick_parked'; "
        f"got scheduler_event records: {sched_records}"
    )
    evt = parked_records[0]["scheduler_event"]
    assert evt["current_name"] == "parked_current", f"current_name mismatch: {evt}"
    assert records, f"error log should not be empty: {records}"


# ---------------------------------------------------------------------------
# Test C — bare Tideweaver (no logger_name) → caplog captures module logger
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bare_tideweaver_isolated_failure_emits_via_module_logger(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Bare Tideweaver (no logger_name) on_error='isolate' raises → module logger WARNING.

    Proves that when logger_name is None on the Tideweaver instance, the
    fallback branch emits the 'isolated tick failure' WARNING through the
    module logger (incorporator.tideweaver.scheduler), matching
    the pre-Phase-2 behavior exactly so no existing caplog-based tests break.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(_StableSource, _BoomSink)

    up1 = _StableSource(inc_code=2)
    _StableSource._tideweaver_snapshot = [up1]  # type: ignore[attr-defined]

    class _StableUpC(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False

        async def tick(self, scheduler: Any) -> None:
            pass

    class _RaisingDownC(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False

        async def tick(self, scheduler: Any) -> None:
            raise RuntimeError("bare tideweaver deliberate failure")

    from incorporator.tideweaver import Edge

    up_current = _StableUpC(name="up_c", cls=_StableSource, interval=10.0)
    dn_current = _RaisingDownC(
        name="down_c",
        cls=_BoomSink,
        interval=0.04,
        on_error="isolate",
    )

    ws = Watershed(
        window=_short_window(400),
        currents=[up_current, dn_current],
        edges=[Edge(from_name="up_c", to_name="down_c")],
    )

    # Bare Tideweaver — no logger_name set.
    tw = Tideweaver(ws, pass_interval=0.03)
    assert tw.logger_name is None, "bare Tideweaver must have logger_name=None"

    with caplog.at_level(logging.WARNING, logger="incorporator.tideweaver.scheduler"):
        async for _ in tw.run():
            pass

    isolated_records = [r for r in caplog.records if "isolated tick failure" in r.message]
    assert len(isolated_records) >= 1, (
        f"bare Tideweaver must emit 'isolated tick failure' via the module logger; "
        f"caplog records: {[r.message for r in caplog.records]}"
    )


# ---------------------------------------------------------------------------
# Test D — scheduler_event payload carries session key
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scheduler_event_payload_carries_session(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """scheduler_event structured records carry a 'session' key matching the logger_name.

    Proves that _route_scheduler_event_to_log includes session=logger_name in
    the payload so two concurrent runs are distinguishable by session inside a
    record, not just by filename.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(_StableSource, _BoomSink)

    logger_name = "TestSessionPayload"

    class _RaisingIsolated(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False

        async def tick(self, scheduler: Any) -> None:
            raise RuntimeError("session-payload test failure")

    dn_current = _RaisingIsolated(
        name="session_current",
        cls=_BoomSink,
        interval=0.04,
        on_error="isolate",
    )

    ws = Watershed(
        window=_short_window(400),
        currents=[dn_current],
        edges=[],
    )

    tw = LoggedTideweaver(ws, enable_logging=True, logger_name=logger_name, pass_interval=0.03)
    async for _ in tw.run():
        pass

    _wait_for_log_flush()

    records = _read_error_log(logger_name)
    sched_records = [r for r in records if "scheduler_event" in r]
    assert sched_records, "expected at least one scheduler_event record"
    for rec in sched_records:
        evt = rec["scheduler_event"]
        assert "session" in evt, f"scheduler_event payload must have 'session' key; got {evt}"
        assert evt["session"] == logger_name, (
            f"session must equal logger_name={logger_name!r}; got {evt['session']!r}"
        )


# ---------------------------------------------------------------------------
# Test E — _SCHEDULER_ERROR_EVENTS module-level constant
# ---------------------------------------------------------------------------


def test_scheduler_error_events_module_constant_exists() -> None:
    """_SCHEDULER_ERROR_EVENTS is a module-level frozenset in logger.py (not per-call).

    Proves the Phase-4 nit: the constant is defined at module level and contains
    the expected error event names.
    """
    from incorporator.observability.logger import _SCHEDULER_ERROR_EVENTS

    assert isinstance(_SCHEDULER_ERROR_EVENTS, frozenset), (
        f"_SCHEDULER_ERROR_EVENTS must be a frozenset; got {type(_SCHEDULER_ERROR_EVENTS)}"
    )
    assert "tick_parked" in _SCHEDULER_ERROR_EVENTS
    assert "fjord_flush_failure" in _SCHEDULER_ERROR_EVENTS


# ---------------------------------------------------------------------------
# Test F — canal RejectEntry carries session
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_canal_reject_entry_carries_session(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Canal-layer RejectEntry records carry a session field matching the scheduler's logger_name.

    Proves that _build_canal_reject passes session=self.logger_name so two
    concurrent runs produce distinguishable reject records.
    """
    monkeypatch.chdir(tmp_path)

    from incorporator import Incorporator
    from incorporator.tideweaver import Edge, FlowControl, HardLock
    from incorporator.tideweaver import SurgeBarrier
    from incorporator.tideweaver.scheduler import Tideweaver
    from incorporator.tideweaver.watershed import Watershed
    from incorporator.tideweaver.current import Stream

    class _UpSrc(Incorporator):
        pass

    class _DownSrc(Incorporator):
        pass

    async def slow_up(current: Any) -> None:
        if current.name == "up":
            import asyncio

            await asyncio.sleep(0.6)

    now = datetime.now(timezone.utc)
    up = Stream(name="up", cls=_UpSrc, interval=0.1, incorp_params={})
    dn = Stream(name="dn", cls=_DownSrc, interval=0.1, incorp_params={})
    halt_flow = FlowControl(
        gate=HardLock(),
        surge_barrier=SurgeBarrier(threshold_multiple=2.0, action="halt"),
    )
    ws = Watershed(
        window=(now, now + __import__("datetime").timedelta(milliseconds=700)),
        currents=[up, dn],
        edges=[Edge(from_name="up", to_name="dn", flow=halt_flow)],
    )

    session_name = "TestCanalSession"
    tw = Tideweaver(ws, tick_factory=slow_up, pass_interval=0.05, logger_name=session_name)

    tides = [t async for t in tw.run()]
    assert tides

    surge_rejects = [r for r in tw.rejects if r.error_kind == "SurgeHalted"]
    assert surge_rejects, f"expected SurgeHalted rejects; got {tw.rejects}"
    for r in surge_rejects:
        assert r.session == session_name, (
            f"canal reject must carry session={session_name!r}; got session={r.session!r}"
        )


# ---------------------------------------------------------------------------
# Test G — Tide model carries session
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tide_carries_session_from_scheduler(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tide records yielded by Tideweaver carry the session field matching logger_name.

    Proves that _run_pass passes session=self.logger_name into Tide.model_construct
    so each structured record is queryable by session.
    """
    monkeypatch.chdir(tmp_path)

    from incorporator import Incorporator
    from incorporator.tideweaver.scheduler import Tideweaver
    from incorporator.tideweaver.watershed import Watershed
    from incorporator.tideweaver.current import Stream

    class _SimpleSrc(Incorporator):
        pass

    async def noop(current: Any) -> None:
        pass

    now = datetime.now(timezone.utc)
    ws = Watershed(
        window=(now, now + __import__("datetime").timedelta(milliseconds=300)),
        currents=[Stream(name="s", cls=_SimpleSrc, interval=0.1, incorp_params={})],
    )

    session_name = "TestTideSession"
    tw = Tideweaver(ws, tick_factory=noop, pass_interval=0.05, logger_name=session_name)
    tides = [t async for t in tw.run()]
    assert tides
    for tide in tides:
        assert tide.session == session_name, (
            f"Tide must carry session={session_name!r}; got {tide.session!r}"
        )


@pytest.mark.asyncio
async def test_tide_session_is_none_without_logger_name(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tide records have session=None when no logger_name is set on the scheduler.

    Proves that the default (no logger_name) still produces session=None on
    every Tide — backward-compatible with callers that never set logger_name.
    """
    monkeypatch.chdir(tmp_path)

    from incorporator import Incorporator
    from incorporator.tideweaver.scheduler import Tideweaver
    from incorporator.tideweaver.watershed import Watershed
    from incorporator.tideweaver.current import Stream

    class _AnonSrc(Incorporator):
        pass

    async def noop(current: Any) -> None:
        pass

    now = datetime.now(timezone.utc)
    ws = Watershed(
        window=(now, now + __import__("datetime").timedelta(milliseconds=200)),
        currents=[Stream(name="s", cls=_AnonSrc, interval=0.1, incorp_params={})],
    )

    tw = Tideweaver(ws, tick_factory=noop, pass_interval=0.05)
    assert tw.logger_name is None
    tides = [t async for t in tw.run()]
    assert tides
    for tide in tides:
        assert tide.session is None, f"Tide session must be None without logger_name; got {tide.session!r}"
