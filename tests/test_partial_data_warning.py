"""Gate test (a): partial-data UserWarning fires from base.py, not thread.py.

Proves that when ``incorp()`` or ``refresh()`` completes with at least one
reject, the resulting ``UserWarning`` is emitted from ``base.py`` (the
``warnings.warn`` call after the asyncio.to_thread join) rather than from
``concurrent/thread.py`` (the old factory.py site).

Also verifies that:
- The warning message contains the source identifier and error_kind.
- No warning fires when the result has no rejects.
- ``stacklevel`` attribution survives a nested ``child_incorp`` drill,
  landing on the real call site (not merely "not thread.py" -- that bar
  would also pass on asyncio internals).
- The four task-rooted engine paths (fjord source seeding, the stateful
  refresh daemon, a Tideweaver Stream tick, the architect probe fan-out)
  suppress the aggregate ``UserWarning`` entirely via
  ``_ENGINE_DRIVEN_CALL``, while the reject itself is still captured (not
  silently dropped).
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from incorporator import Incorporator, LoggedIncorporator
from incorporator.io import fetch


class _Source(Incorporator):
    pass


class _Source2(Incorporator):
    pass


async def _mock_ok(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Returns a valid single-record payload."""
    return httpx.Response(200, text=json.dumps([{"id": "abc"}]), request=httpx.Request("GET", url))


async def _mock_request_error(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Raises a RequestError so the fetch path appends a reject."""
    raise httpx.RequestError("connection refused", request=httpx.Request("GET", url))


@pytest.mark.asyncio
async def test_incorp_warns_from_base_py_with_reject(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """UserWarning fires after incorp() when a source fails, attributed to base.py."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", _mock_request_error)

    with pytest.warns(UserWarning) as rec:
        await _Source.incorp("https://api.example.com/data")

    assert len(rec) >= 1
    w = rec[0]
    # Warning must NOT originate from thread.py or concurrent internals.
    # The dynamic stacklevel walker (_reject_warning_stacklevel) skips every frame
    # whose file lives under the installed incorporator package and stops at the
    # first external frame — here, this test's own call line.  A hardcoded
    # stacklevel only happens to work for this single un-nested case; the old
    # factory.py site pointed to thread.py, and nested call chains (see the drill,
    # test(), and stream() tests below) would misattribute under a fixed level.
    assert "thread" not in w.filename.lower(), f"Warning attributed to thread internals: {w.filename!r}"
    assert "concurrent" not in w.filename.lower(), f"Warning attributed to concurrent internals: {w.filename!r}"
    # Message must contain the source and error kind
    assert "https://api.example.com/data" in str(w.message)
    assert "RequestError" in str(w.message)


@pytest.mark.asyncio
async def test_incorp_no_warn_when_no_rejects(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """No UserWarning fires when incorp() succeeds with no rejects."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", _mock_ok)

    with pytest.warns(UserWarning) as rec:
        await _Source2.incorp("https://api.example.com/data")
        # Inject a dummy warning so pytest.warns does not fail on empty
        import warnings

        warnings.warn("_sentinel_", UserWarning, stacklevel=1)

    # Only the sentinel should be present — no partial-data warning
    assert all("_sentinel_" in str(w.message) for w in rec), (
        f"Unexpected partial-data warning(s): {[str(w.message) for w in rec]}"
    )


# ---------------------------------------------------------------------------
# Exact attribution through a nested (child_incorp) drill — closes the
# "not thread.py" hole, which would also pass on asyncio/events.py.
# ---------------------------------------------------------------------------


class _DrillParent(Incorporator):
    pass


class _DrillChild(Incorporator):
    pass


@pytest.mark.asyncio
async def test_incorp_warning_attributed_to_real_caller_through_child_incorp_drill(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A reject surfaced through a child_incorp() drill still attributes to THIS test's call line.

    The drill call stack is: this test -> Incorporator.incorp() (outer, inc_parent set)
    -> schema.factory.child_incorp() -> Incorporator.incorp() (inner, does the actual
    fetch and raises the warning). A naive ``stacklevel=2`` would land on
    ``schema/factory.py``; the dynamic walk must still find this file.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", _mock_ok)

    parent = await _DrillParent.incorp("https://api.example.com/parents", inc_code="id")
    assert not parent.rejects

    monkeypatch.setattr(fetch, "execute_request", _mock_request_error)

    with pytest.warns(UserWarning) as rec:
        this_line = _current_lineno() + 1
        await _DrillChild.incorp(
            inc_url="https://api.example.com/children/{}",
            inc_parent=parent,
            inc_child="id",
            inc_code="id",
        )

    assert len(rec) == 1
    w = rec[0]
    assert Path(w.filename) == Path(__file__), f"Warning attributed to {w.filename!r}, not this test file"
    assert w.lineno == this_line, f"Warning attributed to line {w.lineno}, expected {this_line}"


def _current_lineno() -> int:
    """Returns the caller's line number — helper so the drill test can pin an exact line."""
    import inspect

    frame = inspect.currentframe()
    assert frame is not None and frame.f_back is not None
    return frame.f_back.f_lineno


# ---------------------------------------------------------------------------
# Three more nested call chains the CHANGELOG claims correct attribution for
# but that had no line-pinned regression test: a LoggedIncorporator wrap,
# Incorporator.test(), and the chunked stream() engine.  Each adds one or more
# package frames between the warn() call site and the test's own call line;
# the dynamic walker must still land on this file.
# ---------------------------------------------------------------------------


class _LoggedSource(LoggedIncorporator):
    pass


@pytest.mark.asyncio
async def test_logged_incorporator_wrap_warning_attributed_to_real_caller(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A reject surfaced through LoggedIncorporator.incorp()'s super() wrap still attributes here.

    Call chain at warn(): Incorporator.incorp() (base.py) -> LoggedIncorporator.incorp()
    (observability/logger.py, ``result = await super().incorp(...)``) -> this test's call line.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", _mock_request_error)

    with pytest.warns(UserWarning) as rec:
        this_line = _current_lineno() + 1
        await _LoggedSource.incorp("https://api.example.com/logged")

    assert len(rec) == 1
    w = rec[0]
    assert Path(w.filename) == Path(__file__), f"Warning attributed to {w.filename!r}, not this test file"
    assert w.lineno == this_line, f"Warning attributed to line {w.lineno}, expected {this_line}"


class _TestMethodSource(Incorporator):
    pass


@pytest.mark.asyncio
async def test_test_method_warning_attributed_to_real_caller(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A reject surfaced through Incorporator.test()'s internal incorp() call still attributes here.

    Call chain at warn(): Incorporator.incorp() (base.py) -> Incorporator.test() (base.py,
    ``result = await cls.incorp(**kwargs)``) -> this test's call line.  The fetch must
    succeed at the HTTP layer's own try/except but still populate ``result.rejects``, so
    the warning fires from inside incorp() itself rather than through test()'s separate
    ``except Exception`` branch (a different, non-warning path).
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", _mock_request_error)

    with pytest.warns(UserWarning) as rec:
        this_line = _current_lineno() + 1
        await _TestMethodSource.test(inc_url="https://api.example.com/probe")

    assert len(rec) == 1
    w = rec[0]
    assert Path(w.filename) == Path(__file__), f"Warning attributed to {w.filename!r}, not this test file"
    assert w.lineno == this_line, f"Warning attributed to line {w.lineno}, expected {this_line}"


class _ChunkedStreamSource(Incorporator):
    pass


@pytest.mark.asyncio
async def test_chunked_stream_warning_attributed_to_real_caller(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A reject surfaced through the chunked stream() engine still attributes here.

    Call chain at warn(): Incorporator.incorp() (base.py) -> _run_chunking_engine()
    (pipeline/chunked.py) -> run_pipeline() (pipeline/__init__.py) -> Incorporator.stream()
    (base.py) -> this test's ``async for`` call line.  The chunked engine (unlike the
    Tideweaver Stream tick) does not set ``_ENGINE_DRIVEN_CALL``, so the aggregate warning
    fires here rather than being suppressed.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", _mock_request_error)

    with pytest.warns(UserWarning) as rec:
        this_line = _current_lineno() + 1
        async for _wave in _ChunkedStreamSource.stream(
            incorp_params={"inc_url": "https://api.example.com/chunked"},
            poll_interval=None,
        ):
            break

    assert len(rec) == 1
    w = rec[0]
    assert Path(w.filename) == Path(__file__), f"Warning attributed to {w.filename!r}, not this test file"
    assert w.lineno == this_line, f"Warning attributed to line {w.lineno}, expected {this_line}"


# ---------------------------------------------------------------------------
# Task-rooted engine paths suppress the aggregate warning entirely — the
# reject is already served by Wave.failed_sources / Tideweaver.rejects and
# no user frame exists on the stack to attribute it to.
# ---------------------------------------------------------------------------


class _EngineSource(Incorporator):
    pass


@pytest.mark.asyncio
async def test_engine_driven_flag_suppresses_warning_directly(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, recwarn: pytest.WarningsRecorder
) -> None:
    """Setting ``_ENGINE_DRIVEN_CALL`` suppresses the aggregate warning while rejects still populate."""
    from incorporator.rejects import _ENGINE_DRIVEN_CALL

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", _mock_request_error)

    token = _ENGINE_DRIVEN_CALL.set(True)
    try:
        result = await _EngineSource.incorp("https://api.example.com/engine")
    finally:
        _ENGINE_DRIVEN_CALL.reset(token)

    assert result.rejects, "reject must still be captured even though the warning is suppressed"
    assert not any(issubclass(w.category, UserWarning) for w in recwarn.list)


@pytest.mark.asyncio
async def test_seed_one_source_is_engine_driven_no_warning(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, recwarn: pytest.WarningsRecorder
) -> None:
    """A fjord source-seed incorp() call fires no UserWarning even though it rejects."""
    from incorporator.pipeline.fjord import _seed_one_source

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", _mock_request_error)

    entry = {"cls": _EngineSource, "incorp_params": {"inc_url": "https://api.example.com/seed"}}
    result = await _seed_one_source(entry, {}, None)

    assert result.rejects, "reject must still be captured on the seeded result"
    assert not any(issubclass(w.category, UserWarning) for w in recwarn.list)


@pytest.mark.asyncio
async def test_refresh_daemon_is_engine_driven_no_warning(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, recwarn: pytest.WarningsRecorder
) -> None:
    """A stateful-refresh-daemon refresh() call fires no UserWarning even though it rejects."""
    from incorporator.pipeline._daemons import _refresh_daemon

    monkeypatch.chdir(tmp_path)

    class _DaemonSource(Incorporator):
        pass

    monkeypatch.setattr(fetch, "execute_request", _mock_ok)
    seeded = await _DaemonSource.incorp("https://api.example.com/daemon")
    assert not seeded.rejects

    monkeypatch.setattr(fetch, "execute_request", _mock_request_error)
    dataset_ref: list[Any] = [seeded]
    wave_queue: asyncio.Queue[Any] = asyncio.Queue()
    await _refresh_daemon(
        _DaemonSource,
        dataset_ref,
        {},
        asyncio.Lock(),
        wave_queue,
        asyncio.Event(),
        r_interval=None,
    )

    assert dataset_ref[0].rejects, "reject must still be captured on the refreshed result"
    assert not any(issubclass(w.category, UserWarning) for w in recwarn.list)


@pytest.mark.asyncio
async def test_tick_stream_sets_engine_driven_call_flag(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, recwarn: pytest.WarningsRecorder
) -> None:
    """A drilled Tideweaver Stream tick runs its incorp() call with _ENGINE_DRIVEN_CALL set.

    Task-rooted (each tick is its own asyncio.create_task) — no user frame exists
    on the stack, so base.py must not raise the aggregate warning here; the flag
    is the mechanism that tells it so. Uses a fake incorp() that observes the
    flag directly rather than driving a real HTTP failure through a mocked
    scheduler client, which is fragile and out of scope for this probe.
    """
    from unittest.mock import AsyncMock, MagicMock

    from pydantic import ConfigDict

    from incorporator.list import IncorporatorList
    from incorporator.rejects import _ENGINE_DRIVEN_CALL
    from incorporator.tideweaver import Stream
    from incorporator.tideweaver.scheduler import Tideweaver

    monkeypatch.chdir(tmp_path)

    class _TickUpstream(Incorporator):
        model_config = ConfigDict(extra="allow")

    class _TickChild(Incorporator):
        model_config = ConfigDict(extra="allow")

    _TickUpstream.inc_dict.clear()
    _TickChild.inc_dict.clear()

    upstream_snapshot = IncorporatorList(_TickUpstream, [_TickUpstream(item_id="abc")])
    _TickUpstream._tideweaver_snapshot = upstream_snapshot  # type: ignore[attr-defined]

    observed_flag: list[bool] = []

    async def _fake_incorp(**kwargs: Any) -> IncorporatorList[Any]:
        observed_flag.append(_ENGINE_DRIVEN_CALL.get())
        return IncorporatorList(_TickChild, [])

    monkeypatch.setattr(_TickChild, "incorp", AsyncMock(side_effect=_fake_incorp))

    upstream = Stream(name="up", cls=_TickUpstream, interval=1.0, incorp_params={"inc_file": "x"})
    child = Stream(
        name="child",
        cls=_TickChild,
        interval=1.0,
        parent_current="up",
        incorp_params={"inc_url": "https://api.example.com/{}", "inc_child": "item_id"},
    )

    stub = MagicMock()
    stub._currents_by_name = {"up": upstream, "child": child}
    stub._get_or_create_client = MagicMock(return_value=MagicMock())
    stub.watershed = MagicMock(inflow=None)
    stub.logger_name = MagicMock()  # not a str -> the _route_to_log branch is skipped

    await Tideweaver._tick_stream(stub, child)

    assert observed_flag == [True], "incorp() must observe _ENGINE_DRIVEN_CALL=True during a Stream tick"
    assert _ENGINE_DRIVEN_CALL.get() is False, "the flag must be reset after the tick completes"
    assert not any(issubclass(w.category, UserWarning) for w in recwarn.list)
