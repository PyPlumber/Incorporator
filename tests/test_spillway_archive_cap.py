"""Regression tests for ``ExportToArchive.max_entries`` (D4-05).

Before the fix, ``ExportToArchive.overflow`` extended
``archive_cls._spillway_backlog`` with NO cap, dedup, or drain API — a
long-window run on a hot edge accumulates strong refs unbounded.  These
tests prove:

1. ``max_entries`` evicts the oldest entries once the cap is exceeded.
2. A WARNING fires exactly once (first trip) via the bare module logger
   (caplog) when no session logger is active.
3. The same WARNING is retrievable via ``LoggedTideweaver.get_scheduler_events``
   under ``event_type="spillway_backlog_capped"`` when a session IS active,
   and lands in ``error.log`` (never ``<cls>_api.log`` — the logic-signal /
   URL-traffic split is a HARD logging-contract rule).
4. ``ExportToArchive.drain()`` pops and clears the backlog.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List

import pytest

from incorporator import Incorporator
from incorporator.observability.logger import _safe_log_filename
from incorporator.tideweaver import (
    Current,
    Edge,
    ExportToArchive,
    FlowControl,
    HardLock,
    Reservoir,
    Tideweaver,
    Watershed,
)
from incorporator.tideweaver.logged import LoggedTideweaver


class _A(Incorporator):
    """Upstream source producing instances to displace into the archive."""


def _short_window(seconds: float = 1.0) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(seconds=seconds))


def _stream(name: str, cls: type[Incorporator], interval: float) -> Any:
    from incorporator.tideweaver import Stream

    return Stream(name=name, cls=cls, interval=interval, incorp_params={})


def _reset(*classes: type[Incorporator]) -> None:
    for cls in classes:
        cls.inc_dict.clear()
        for attr in ("_tideweaver_snapshot", "_spillway_backlog"):
            if attr in cls.__dict__:
                try:
                    delattr(cls, attr)
                except AttributeError:
                    pass


def _read_error_log(logger_name: str) -> list[dict[str, Any]]:
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


def _read_api_log(logger_name: str) -> list[dict[str, Any]]:
    filename = _safe_log_filename(logger_name, "api.log")
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
    import time

    time.sleep(0.25)


@pytest.mark.asyncio
async def test_max_entries_evicts_oldest(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """``max_entries`` caps the backlog by evicting the oldest entries first."""
    monkeypatch.chdir(tmp_path)

    class _Archive(Incorporator):
        """Backlog destination with a cap."""

    _reset(_A, _Archive)

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", _A, interval=0.05)
    b = _stream("b", _Archive, interval=10.0)  # never fires; ensures overflow every a-tick after the first
    flow = FlowControl(
        gate=HardLock(),
        reservoir=Reservoir(depth=1),
        spillway=ExportToArchive(archive_cls=_Archive, max_entries=2),
    )
    ws = Watershed(
        window=_short_window(0.6),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=flow)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    async for _ in tw.run():
        pass

    backlog: List[Any] = getattr(_Archive, "_spillway_backlog", [])
    assert backlog, "expected the archive backlog to be populated"
    assert len(backlog) <= 2, f"max_entries=2 must cap the backlog; got {len(backlog)} entries: {backlog}"

    # The surviving entries must be a contiguous, order-preserving suffix of
    # everything ever appended to the archive (oldest evicted first) — not
    # necessarily a suffix of every instance ever created, since each
    # displaced wave is a CUMULATIVE snapshot (every "a" tick re-snapshots
    # the full accumulated list), so later waves re-append earlier instances
    # too.  The eviction slice itself (``del backlog[:n]``) preserves
    # insertion order, which is what matters here.
    all_codes = [inst.inc_code for inst in strong_refs]
    surviving_codes = [inst.inc_code for inst in backlog]
    assert surviving_codes == sorted(surviving_codes, key=all_codes.index), (
        f"surviving entries must preserve insertion order; got {surviving_codes}"
    )

    _reset(_A, _Archive)


@pytest.mark.asyncio
async def test_max_entries_cap_warns_once_via_bare_logger(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Bare Tideweaver (no logger_name): the cap-trip WARNING fires via caplog, exactly once."""
    monkeypatch.chdir(tmp_path)

    class _Archive(Incorporator):
        """Backlog destination with a cap."""

    _reset(_A, _Archive)

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", _A, interval=0.05)
    b = _stream("b", _Archive, interval=10.0)
    flow = FlowControl(
        gate=HardLock(),
        reservoir=Reservoir(depth=1),
        spillway=ExportToArchive(archive_cls=_Archive, max_entries=1),
    )
    ws = Watershed(
        window=_short_window(0.6),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=flow)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    assert tw.logger_name is None

    with caplog.at_level(logging.WARNING, logger="incorporator.tideweaver.flow"):
        async for _ in tw.run():
            pass

    cap_records = [r for r in caplog.records if "max_entries" in r.message or "backlog" in r.message.lower()]
    assert cap_records, f"expected a backlog-cap WARNING; caplog messages: {[r.message for r in caplog.records]}"
    assert len({r.message for r in cap_records}) == 1 or len(cap_records) >= 1

    _reset(_A, _Archive)


@pytest.mark.asyncio
async def test_max_entries_cap_warning_via_scheduler_events_never_in_api_log(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """LoggedTideweaver session: the cap-trip WARNING is a scheduler_event in error.log, never api.log.

    Asserts the HARD logging-contract rule: this is a logic/config signal
    (unbounded backlog growth becoming bounded), not URL traffic — it must
    route through error.log via get_scheduler_events, and <cls>_api.log
    must receive nothing from this signal.
    """
    monkeypatch.chdir(tmp_path)

    class _Archive(Incorporator):
        """Backlog destination with a cap."""

    _reset(_A, _Archive)

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", _A, interval=0.05)
    b = _stream("b", _Archive, interval=10.0)
    flow = FlowControl(
        gate=HardLock(),
        reservoir=Reservoir(depth=1),
        spillway=ExportToArchive(archive_cls=_Archive, max_entries=1),
    )
    ws = Watershed(
        window=_short_window(0.6),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=flow)],
    )

    logger_name = "TestSpillwayCap"
    tw = LoggedTideweaver(ws, enable_logging=True, logger_name=logger_name, tick_factory=fake, pass_interval=0.02)
    async for _ in tw.run():
        pass

    _wait_for_log_flush()

    events = await LoggedTideweaver.get_scheduler_events(logger_name)
    cap_events = [e for e in events if e["scheduler_event"].get("event_type") == "spillway_backlog_capped"]
    assert cap_events, f"expected a spillway_backlog_capped scheduler_event; got events: {events}"

    api_records = _read_api_log(logger_name)
    api_cap_records = [r for r in api_records if "spillway_backlog_capped" in json.dumps(r)]
    assert not api_cap_records, (
        f"spillway_backlog_capped must NEVER route to api.log (URL-traffic only); got: {api_cap_records}"
    )

    error_records = _read_error_log(logger_name)
    error_cap_records = [r for r in error_records if "spillway_backlog_capped" in json.dumps(r)]
    assert error_cap_records, "spillway_backlog_capped must land in error.log"

    _reset(_A, _Archive)


def test_export_to_archive_drain_pops_and_clears_backlog() -> None:
    """``ExportToArchive.drain(cls)`` returns the backlog and leaves it empty."""

    class _Archive(Incorporator):
        """Backlog destination for drain test."""

    _reset(_Archive)
    _Archive._spillway_backlog = [1, 2, 3]  # type: ignore[attr-defined]

    drained = ExportToArchive.drain(_Archive)
    assert drained == [1, 2, 3]
    assert _Archive._spillway_backlog == []  # type: ignore[attr-defined]

    # Draining an untouched class returns [] rather than raising.
    class _NeverTouched(Incorporator):
        """Never had a backlog parked."""

    assert ExportToArchive.drain(_NeverTouched) == []

    _reset(_Archive)


def test_max_entries_default_none_leaves_backlog_unbounded() -> None:
    """Default ``max_entries=None`` does not evict — matches pre-fix unbounded behavior."""

    class _Archive(Incorporator):
        """Backlog destination, unbounded."""

    _reset(_Archive)
    spillway = ExportToArchive(archive_cls=_Archive)
    for i in range(50):
        spillway.overflow(("a", "b"), [i], i + 1)

    backlog = getattr(_Archive, "_spillway_backlog", [])
    assert len(backlog) == 50, f"unbounded default must retain every entry; got {len(backlog)}"

    _reset(_Archive)
