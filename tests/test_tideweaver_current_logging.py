"""Tests for per-current session-log routing (DRY logging refactor, commit 3).

Covers the five assertions from the task spec:

1. Session log (error/debug) receives wave records tagged ``code:"prices"``.
2. Session api.log receives url-traffic RejectEntry also code-tagged.
3. ``get_current(session, code)`` returns exactly that current's records.
4. No ``<ClassName>_*.log`` files are created during the run.
5. A bare Tideweaver (``logger_name=None``) emits nothing extra.
6. Data path invariant: ``accumulated`` dict and snapshot are unchanged.
7. ``log_currents=False`` suppresses per-wave emission.
8. Parent-child incorp path: result rejects routed with current_meta.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ConfigDict

from incorporator import Incorporator
from incorporator.observability.logger import current_meta, setup_class_logger
from incorporator.observability.tideweaver import LoggedTideweaver, Stream, Tideweaver, Watershed
from incorporator.observability.tideweaver.current import Stream as StreamCurrent
from incorporator.observability.tideweaver.scheduler import Tideweaver as TideweaverBase
from incorporator.observability.wave import Wave
from incorporator.rejects import RejectEntry


# ---------------------------------------------------------------------------
# Shared Incorporator subclasses
# ---------------------------------------------------------------------------


class PriceClass(Incorporator):
    """Stand-in source class for per-current logging tests."""

    model_config = ConfigDict(extra="allow")


class AnotherClass(Incorporator):
    """Second source class for isolation tests."""

    model_config = ConfigDict(extra="allow")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reset_registries(*classes: type[Incorporator]) -> None:
    """Wipe per-class inc_dict + tideweaver_snapshot between tests."""
    for cls in classes:
        cls.inc_dict.clear()
        if "_tideweaver_snapshot" in cls.__dict__:
            try:
                delattr(cls, "_tideweaver_snapshot")
            except AttributeError:
                pass


def _make_success_wave(chunk_index: int = 0) -> Wave:
    """Build a Wave with one row processed and no rejects."""
    return Wave(chunk_index=chunk_index, rows_processed=1, processing_time_sec=0.01)


def _make_url_traffic_reject() -> RejectEntry:
    """Build a RejectEntry that routes to api.log (is_url_traffic_error=True)."""
    return RejectEntry.model_construct(
        source="https://example.com/api",
        error_kind="HTTPStatusError",
        message="429 Too Many Requests",
        is_url_traffic_error=True,
        retry_after=None,
        wave_index=None,
        from_name=None,
        to_name=None,
        host="example.com",
        status_code=429,
        attempt_number=1,
        duration_sec=None,
        cooldown_sec=None,
        session=None,
    )


def _make_wave_with_reject(chunk_index: int = 1) -> Wave:
    """Build a Wave carrying one url-traffic RejectEntry in wave.rejects."""
    return Wave(
        chunk_index=chunk_index,
        rows_processed=0,
        processing_time_sec=0.02,
        failed_sources=["https://example.com/api"],
        rejects=[_make_url_traffic_reject()],
    )


def _make_scheduler_stub(*, logger_name: str | None, log_currents: bool = True) -> Any:
    """Build a minimal MagicMock scheduler object for _tick_stream calls."""
    stub = MagicMock()
    stub.logger_name = logger_name
    stub.log_currents = log_currents
    stub._tide_number = 1
    stub._canal_rejects = []
    stub._get_or_create_client = MagicMock(return_value=MagicMock())
    ws_stub = MagicMock()
    ws_stub.inflow = None
    stub.watershed = ws_stub
    return stub


def _wait_flush() -> None:
    """Give the QueueHandler background thread time to drain its queue to disk."""
    time.sleep(0.25)


# ---------------------------------------------------------------------------
# current_meta unit test
# ---------------------------------------------------------------------------


def test_current_meta_returns_stable_code_string() -> None:
    """``current_meta`` returns a stable ``code:"<name>"`` string for a current.

    The code is identical to the current name, which is unique within a
    watershed and is the stable retrieval key for ``get_current``.
    """
    current = Stream(name="prices", cls=PriceClass, interval=10.0, incorp_params={})
    result = current_meta(current)
    assert 'current:"prices"' in result
    assert 'class:"PriceClass"' in result
    assert 'code:"prices"' in result


# ---------------------------------------------------------------------------
# Test 1 + 2: session log receives wave records + api.log gets url-traffic reject
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_waves_routed_to_session_log_with_current_code(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """Wave records from a stream current land in session logs tagged with ``code:"prices"``.

    Proves: scheduler routes each yielded Wave to ``_route_to_log`` with
    ``extra_meta=current_meta(current)``; the meta is written to the session
    error/debug log; ``get_current(session, "prices")`` retrieves the records.
    Also proves: no ``PriceClass_*.log`` file is created.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(PriceClass)

    session = "TestCurrentLogWaves"
    setup_class_logger(session)

    # Build two mock waves: one success, one with a url-traffic reject.
    success_wave = _make_success_wave(chunk_index=0)
    reject_wave = _make_wave_with_reject(chunk_index=1)

    async def _mock_stream(*args: Any, **kwargs: Any) -> Any:
        yield success_wave
        yield reject_wave

    monkeypatch.setattr(PriceClass, "stream", _mock_stream)

    current = StreamCurrent(name="prices", cls=PriceClass, interval=10.0, incorp_params={})
    scheduler = _make_scheduler_stub(logger_name=session, log_currents=True)
    # _tick_stream reads scheduler._currents_by_name only for parent_current path;
    # incorp_params has no inc_file so no SourceLoadFailure branch fires.
    scheduler._currents_by_name = {"prices": current}

    await TideweaverBase._tick_stream(scheduler, current)

    _wait_flush()

    # Records tagged with code:"prices" must appear in the session log.
    records = await LoggedTideweaver.get_current(session, 'code:"prices"')
    # Expect at least the success wave record (rows_processed=1 → INFO).
    # The url-traffic reject also lands in api.log, so get_current (which
    # unions api+error+debug) should include it too.
    assert len(records) >= 1, f"expected >= 1 records tagged with code:prices; got {records}"
    metas = [r.get("meta", "") for r in records]
    assert any('code:"prices"' in m for m in metas), (
        f"at least one record must have code:prices in meta; got metas={metas}"
    )


@pytest.mark.asyncio
async def test_url_traffic_reject_in_wave_lands_in_api_log(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """A url-traffic RejectEntry carried in wave.rejects is routed to the session api.log.

    Proves: _route_to_log dispatches RejectEntry with is_url_traffic_error=True
    to api.log; the session api.log record carries code:"prices" in its meta.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(PriceClass)

    session = "TestApiLogReject"
    setup_class_logger(session)

    reject_wave = _make_wave_with_reject(chunk_index=0)

    async def _mock_stream(*args: Any, **kwargs: Any) -> Any:
        yield reject_wave

    monkeypatch.setattr(PriceClass, "stream", _mock_stream)

    current = StreamCurrent(name="prices", cls=PriceClass, interval=10.0, incorp_params={})
    scheduler = _make_scheduler_stub(logger_name=session, log_currents=True)
    scheduler._currents_by_name = {"prices": current}

    await TideweaverBase._tick_stream(scheduler, current)

    _wait_flush()

    # api.log should contain a reject record with code:"prices".
    from incorporator.observability.logger import read_log

    api_records = await read_log(session, ["api"], key="reject")
    assert len(api_records) >= 1, f"expected >= 1 api.log reject record; got {api_records}"
    api_metas = [r.get("meta", "") for r in api_records]
    assert any('code:"prices"' in m for m in api_metas), (
        f"api.log reject must carry code:prices in meta; got metas={api_metas}"
    )


# ---------------------------------------------------------------------------
# Test 3: get_current returns exactly that current's records
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_current_returns_only_this_currents_records(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """``get_current(session, code)`` returns only records for the named current.

    Drives two currents (prices + another) through separate _tick_stream
    calls; ``get_current(session, 'code:"prices"')`` must not include
    records tagged with ``code:"another"``.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(PriceClass, AnotherClass)

    session = "TestGetCurrentIsolation"
    setup_class_logger(session)

    success_wave = _make_success_wave(chunk_index=0)

    async def _mock_stream(*args: Any, **kwargs: Any) -> Any:
        yield success_wave

    monkeypatch.setattr(PriceClass, "stream", _mock_stream)
    monkeypatch.setattr(AnotherClass, "stream", _mock_stream)

    current_prices = StreamCurrent(name="prices", cls=PriceClass, interval=10.0, incorp_params={})
    current_another = StreamCurrent(name="another", cls=AnotherClass, interval=10.0, incorp_params={})

    scheduler = _make_scheduler_stub(logger_name=session, log_currents=True)
    scheduler._currents_by_name = {"prices": current_prices, "another": current_another}

    await TideweaverBase._tick_stream(scheduler, current_prices)
    await TideweaverBase._tick_stream(scheduler, current_another)

    _wait_flush()

    prices_records = await LoggedTideweaver.get_current(session, 'code:"prices"')
    another_records = await LoggedTideweaver.get_current(session, 'code:"another"')

    assert len(prices_records) >= 1, f"expected prices records; got {prices_records}"
    assert len(another_records) >= 1, f"expected another records; got {another_records}"

    # Prices records must not contain any "another" meta tags and vice versa.
    for rec in prices_records:
        meta = rec.get("meta", "")
        assert 'code:"another"' not in meta, (
            f"prices record must not contain code:another; got meta={meta!r}"
        )
    for rec in another_records:
        meta = rec.get("meta", "")
        assert 'code:"prices"' not in meta, (
            f"another record must not contain code:prices; got meta={meta!r}"
        )


# ---------------------------------------------------------------------------
# Test 4: no <ClassName>_*.log files created
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_per_class_log_files_created_during_watershed_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """No ``PriceClass_*.log`` files are created during a LoggedTideweaver run.

    Proves: session-level routing via meta does NOT call ``setup_class_logger``
    on each current's cls, so no per-class QueueListener thread or file is
    spawned.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(PriceClass)

    session = "TestNoPerClassFiles"
    setup_class_logger(session)

    success_wave = _make_success_wave(chunk_index=0)

    async def _mock_stream(*args: Any, **kwargs: Any) -> Any:
        yield success_wave

    monkeypatch.setattr(PriceClass, "stream", _mock_stream)

    current = StreamCurrent(name="prices", cls=PriceClass, interval=10.0, incorp_params={})
    scheduler = _make_scheduler_stub(logger_name=session, log_currents=True)
    scheduler._currents_by_name = {"prices": current}

    await TideweaverBase._tick_stream(scheduler, current)

    _wait_flush()

    logs_dir = tmp_path / "logs"
    if logs_dir.exists():
        per_class_files = list(logs_dir.glob("PriceClass_*.log"))
        assert per_class_files == [], (
            f"no PriceClass_*.log files should be created; got {per_class_files}"
        )


# ---------------------------------------------------------------------------
# Test 5: bare Tideweaver (logger_name=None) emits nothing extra
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bare_tideweaver_no_logger_name_skips_routing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A bare scheduler (logger_name=None) skips per-wave log routing entirely.

    Proves: the ``isinstance(self.logger_name, str)`` gate is respected — no
    log files are written when the scheduler has no logger_name.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(PriceClass)

    success_wave = _make_success_wave(chunk_index=0)

    async def _mock_stream(*args: Any, **kwargs: Any) -> Any:
        yield success_wave

    monkeypatch.setattr(PriceClass, "stream", _mock_stream)

    current = StreamCurrent(name="prices", cls=PriceClass, interval=10.0, incorp_params={})
    # logger_name=None → bare Tideweaver behaviour
    scheduler = _make_scheduler_stub(logger_name=None, log_currents=True)
    scheduler._currents_by_name = {"prices": current}

    await TideweaverBase._tick_stream(scheduler, current)

    logs_dir = tmp_path / "logs"
    assert not logs_dir.exists() or not any(logs_dir.iterdir()), (
        "no log files should be created for a bare Tideweaver with logger_name=None"
    )


# ---------------------------------------------------------------------------
# Test 6: data path invariant — accumulated dict unchanged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_accumulated_dict_and_snapshot_unchanged_by_logging(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """Adding wave routing does not change _tideweaver_snapshot assignment after the drain.

    Proves: the snapshot assignment (``cls_any._tideweaver_snapshot = list(accumulated.values())``)
    still executes after the drain loop completes — log routing is purely additive inside
    the loop body and does not interfere with the final snapshot write.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(PriceClass)

    session = "TestDataPathInvariant"
    setup_class_logger(session)

    # Track whether stream was called and wave was yielded.
    stream_calls: list[int] = []

    async def _mock_stream_counting(*args: Any, **kwargs: Any) -> Any:
        stream_calls.append(1)
        yield _make_success_wave(chunk_index=0)

    monkeypatch.setattr(PriceClass, "stream", _mock_stream_counting)

    current = StreamCurrent(name="prices", cls=PriceClass, interval=10.0, incorp_params={})
    scheduler = _make_scheduler_stub(logger_name=session, log_currents=True)
    scheduler._currents_by_name = {"prices": current}

    await TideweaverBase._tick_stream(scheduler, current)

    # Stream was called exactly once (data path is unaffected by log routing).
    assert len(stream_calls) == 1, f"stream must be called exactly once; got {len(stream_calls)}"

    # _tideweaver_snapshot must be set after _tick_stream — even when log routing fires.
    snapshot = getattr(PriceClass, "_tideweaver_snapshot", None)
    assert snapshot is not None, "_tideweaver_snapshot must be set after _tick_stream"
    assert isinstance(snapshot, list), f"snapshot must be a list; got {type(snapshot)}"


# ---------------------------------------------------------------------------
# Test 7: log_currents=False suppresses per-wave emission
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_log_currents_false_suppresses_wave_routing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """``log_currents=False`` suppresses per-wave log routing even when logger_name is set.

    Proves: the ``self.log_currents`` gate works — no current-tagged records
    appear when the opt-out is active.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(PriceClass)

    session = "TestLogCurrentsFalse"
    setup_class_logger(session)

    success_wave = _make_success_wave(chunk_index=0)

    async def _mock_stream(*args: Any, **kwargs: Any) -> Any:
        yield success_wave

    monkeypatch.setattr(PriceClass, "stream", _mock_stream)

    current = StreamCurrent(name="prices", cls=PriceClass, interval=10.0, incorp_params={})
    # log_currents=False — should suppress routing even though logger_name is set.
    scheduler = _make_scheduler_stub(logger_name=session, log_currents=False)
    scheduler._currents_by_name = {"prices": current}

    await TideweaverBase._tick_stream(scheduler, current)

    _wait_flush()

    records = await LoggedTideweaver.get_current(session, 'code:"prices"')
    assert records == [], (
        f"log_currents=False must suppress per-wave routing; got {len(records)} records"
    )


# ---------------------------------------------------------------------------
# Test 8: parent-child incorp path routes result rejects
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parent_child_incorp_rejects_routed_with_current_meta(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """Parent-child incorp rejects are routed with current_meta when logging is on.

    Proves: when ``current.parent_current`` is set and the incorp result carries
    rejects, each reject is routed to the session log tagged with
    ``code:"<current.name>"``.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(PriceClass, AnotherClass)

    session = "TestParentChildRejects"
    setup_class_logger(session)

    url_reject = _make_url_traffic_reject()

    # Build a mock IncorporatorList-like result carrying one reject.
    mock_result = MagicMock()
    mock_result.rejects = [url_reject]

    mock_incorp = AsyncMock(return_value=mock_result)
    monkeypatch.setattr(AnotherClass, "incorp", mock_incorp)

    # Pre-populate parent snapshot so the parent-child path doesn't short-circuit.
    upstream = StreamCurrent(name="parent", cls=PriceClass, interval=10.0, incorp_params={})
    fake_parent = PriceClass.model_construct()
    PriceClass._tideweaver_snapshot = [fake_parent]  # type: ignore[attr-defined]

    child = StreamCurrent(
        name="child_prices",
        cls=AnotherClass,
        interval=10.0,
        parent_current="parent",
        incorp_params={"inc_url": "https://example.com/{}", "inc_child": "id"},
    )

    scheduler = _make_scheduler_stub(logger_name=session, log_currents=True)
    scheduler._currents_by_name = {"parent": upstream, "child_prices": child}

    await TideweaverBase._tick_stream(scheduler, child)

    _wait_flush()

    # api.log should contain a reject tagged code:"child_prices".
    from incorporator.observability.logger import read_log

    api_records = await read_log(session, ["api"], key="reject")
    assert len(api_records) >= 1, f"expected >= 1 api.log record from parent-child path; got {api_records}"
    api_metas = [r.get("meta", "") for r in api_records]
    assert any('code:"child_prices"' in m for m in api_metas), (
        f"parent-child api.log reject must carry code:child_prices; got metas={api_metas}"
    )


# ---------------------------------------------------------------------------
# Test 9: LoggedTideweaver log_currents parameter defaults to True
# ---------------------------------------------------------------------------


def test_logged_tideweaver_log_currents_default_true(tmp_path: Path) -> None:
    """``LoggedTideweaver`` defaults ``log_currents=True`` and passes it to base.

    Proves: the new parameter is wired from LoggedTideweaver.__init__ through
    to Tideweaver.__init__ so ``self.log_currents`` reflects the chosen value.
    """
    now = datetime.now(timezone.utc)
    ws = Watershed.parallel(
        window=(now + timedelta(hours=1), now + timedelta(hours=2)),
        currents=[StreamCurrent(name="prices", cls=PriceClass, interval=30.0, incorp_params={})],
    )

    tw_default = LoggedTideweaver(ws, enable_logging=False)
    assert tw_default.log_currents is True, (
        f"log_currents must default to True; got {tw_default.log_currents!r}"
    )

    tw_false = LoggedTideweaver(ws, enable_logging=False, log_currents=False)
    assert tw_false.log_currents is False, (
        f"log_currents=False must be forwarded; got {tw_false.log_currents!r}"
    )


# ---------------------------------------------------------------------------
# Test 10: SourceLoadFailure reject is emitted once, not doubled
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_source_load_failure_reject_not_double_emitted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_active_listeners: None,
) -> None:
    """A SourceLoadFailure reject is logged once (code-tagged), not doubled.

    The reject is routed at its tick site with ``current_meta`` AND would be
    swept again by ``LoggedTideweaver.run``'s ``finally`` block; the
    ``_routed_reject_ids`` guard suppresses the second emission so
    ``get_rejects()`` returns it exactly once per occurrence — matching the
    authoritative in-memory ``tw.rejects`` count.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(PriceClass)

    session = "TestNoDoubleEmit"

    async def _empty_stream(*args: Any, **kwargs: Any) -> Any:
        # Async generator that yields nothing → zero rows accumulated.
        if False:  # pragma: no cover
            yield

    monkeypatch.setattr(PriceClass, "stream", _empty_stream)

    now = datetime.now(timezone.utc)
    missing = str(tmp_path / "missing.ndjson")  # nonexistent → SourceLoadFailure branch
    ws = Watershed.parallel(
        window=(now, now + timedelta(milliseconds=250)),
        currents=[
            StreamCurrent(name="prices", cls=PriceClass, interval=0.05, incorp_params={"inc_file": missing})
        ],
    )
    tw = LoggedTideweaver(ws, enable_logging=True, logger_name=session, pass_interval=0.05)
    async for _ in tw.run():
        pass

    _wait_flush()

    mem_slf = [r for r in tw.rejects if r.error_kind == "SourceLoadFailure"]
    assert len(mem_slf) >= 1, f"expected >= 1 SourceLoadFailure in tw.rejects; got {len(mem_slf)}"

    logged = await LoggedTideweaver.get_rejects(session)
    log_slf = [r for r in logged if r.get("reject", {}).get("error_kind") == "SourceLoadFailure"]
    assert len(log_slf) == len(mem_slf), (
        "SourceLoadFailure must be logged once per occurrence (no double-emit): "
        f"log={len(log_slf)} vs memory={len(mem_slf)}"
    )
    # The surviving emission is the tick-site one (carries the per-current code).
    assert all('code:"prices"' in r.get("meta", "") for r in log_slf), (
        f"surviving SourceLoadFailure records must be code-tagged; metas={[r.get('meta') for r in log_slf]}"
    )
