"""Tests for the empty-output WARNING emitted by the Tideweaver scheduler.

Four tests validate the ``_tick_raised`` suppression logic and the
``upstream_had_data`` gate introduced in ``_tick_wrapper``:

1. Empty CustomCurrent tick with non-empty upstream snapshot → WARNING fires.
2. Empty CustomCurrent tick with empty upstream snapshot → no WARNING.
3. CustomCurrent tick that populates inc_dict (auto-park ON) → no WARNING.
4. CustomCurrent tick that raises (on_error="isolate") → "isolated tick failure"
   WARNING fires but no empty-output WARNING.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar, List

import pytest

from incorporator import Incorporator, Tideweaver, Watershed
from incorporator.observability.tideweaver import CustomCurrent, Edge


# ---------------------------------------------------------------------------
# Module-level Incorporator subclasses
# ---------------------------------------------------------------------------


class UpstreamNode(Incorporator):
    """Minimal upstream class used as the upstream current's cls."""


class DownstreamNode(Incorporator):
    """Minimal downstream class used as the CustomCurrent's cls."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reset_registries(*classes: type[Incorporator]) -> None:
    """Wipe per-class inc_dict + parked snapshot between tests."""
    for cls in classes:
        cls.inc_dict.clear()
        if "_tideweaver_snapshot" in cls.__dict__:
            try:
                delattr(cls, "_tideweaver_snapshot")
            except AttributeError:
                pass


def _short_window(ms: float = 200.0) -> tuple[datetime, datetime]:
    """Build a short UTC window of *ms* milliseconds starting now."""
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(milliseconds=ms))


# ---------------------------------------------------------------------------
# Test 1 — empty tick + non-empty upstream → WARNING fires
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_tick_with_nonempty_upstream_emits_warning(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Empty CustomCurrent tick with non-empty upstream snapshot emits the WARNING.

    Proves that when a CustomCurrent's tick() returns without populating
    DownstreamNode.inc_dict and upstream _tideweaver_snapshot is non-empty,
    the scheduler logs the 'produced empty output' warning.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamNode, DownstreamNode)

    # Pre-populate upstream snapshot so the scheduler sees non-empty upstream.
    up1 = UpstreamNode(inc_code=1)
    up2 = UpstreamNode(inc_code=2)
    UpstreamNode._tideweaver_snapshot = [up1, up2]  # type: ignore[attr-defined]

    class EmptyUpstream(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False

        async def tick(self, scheduler: Any) -> None:
            # Keep upstream snapshot parked (simulate upstream already fired).
            pass

    class EmptyDownstream(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False

        async def tick(self, scheduler: Any) -> None:
            # Intentionally produce nothing — triggers the warning.
            pass

    up_current = EmptyUpstream(name="upstream", cls=UpstreamNode, interval=10.0)
    dn_current = EmptyDownstream(name="DownstreamFilter", cls=DownstreamNode, interval=0.05)

    ws = Watershed(
        window=_short_window(200),
        currents=[up_current, dn_current],
        edges=[Edge(from_name="upstream", to_name="DownstreamFilter")],
    )
    tw = Tideweaver(ws, pass_interval=0.02)

    with caplog.at_level("WARNING", logger="incorporator.observability.tideweaver.scheduler"):
        async for _ in tw.run():
            pass

    warn_records = [r for r in caplog.records if "produced empty output" in r.message]
    assert len(warn_records) >= 1, (
        f"expected at least one 'produced empty output' WARNING; got records: "
        f"{[r.message for r in caplog.records]}"
    )
    assert "DownstreamFilter" in warn_records[0].getMessage(), (
        f"warning message must interpolate the current name; got: {warn_records[0].getMessage()}"
    )


# ---------------------------------------------------------------------------
# Test 2 — empty tick + empty upstream → NO WARNING
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_tick_with_empty_upstream_no_warning(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Empty CustomCurrent tick with empty upstream snapshot emits no WARNING.

    Proves that the 'upstream_had_data' gate suppresses the warning when the
    upstream _tideweaver_snapshot is an empty list, preventing false alarms
    on source currents that have not yet produced data.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamNode, DownstreamNode)

    # Explicitly empty upstream snapshot.
    UpstreamNode._tideweaver_snapshot = []  # type: ignore[attr-defined]

    class EmptyUpstream2(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False

        async def tick(self, scheduler: Any) -> None:
            pass

    class EmptyDownstream2(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False

        async def tick(self, scheduler: Any) -> None:
            pass

    up_current = EmptyUpstream2(name="upstream2", cls=UpstreamNode, interval=10.0)
    dn_current = EmptyDownstream2(name="downstream2", cls=DownstreamNode, interval=0.05)

    ws = Watershed(
        window=_short_window(200),
        currents=[up_current, dn_current],
        edges=[Edge(from_name="upstream2", to_name="downstream2")],
    )
    tw = Tideweaver(ws, pass_interval=0.02)

    with caplog.at_level("WARNING", logger="incorporator.observability.tideweaver.scheduler"):
        async for _ in tw.run():
            pass

    warn_records = [r for r in caplog.records if "produced empty output" in r.message]
    assert len(warn_records) == 0, (
        f"expected no 'produced empty output' WARNING when upstream is empty; "
        f"got: {[r.message for r in warn_records]}"
    )


# ---------------------------------------------------------------------------
# Test 3 — tick produces data (auto-park ON) → NO WARNING
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tick_with_data_no_warning(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """CustomCurrent tick that populates inc_dict triggers auto-park, no WARNING.

    Proves that when tick() populates DownstreamNode.inc_dict (auto-park ON),
    wave_snapshot is non-empty after auto-park, so the warning guard's
    'not wave_snapshot' condition is False and no warning is emitted.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamNode, DownstreamNode)

    up1 = UpstreamNode(inc_code=10)
    UpstreamNode._tideweaver_snapshot = [up1]  # type: ignore[attr-defined]

    # Keep strong references to prevent WeakValueDictionary GC.
    strong_refs: List[DownstreamNode] = []

    class ProductiveUpstream(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False

        async def tick(self, scheduler: Any) -> None:
            pass

    class ProductiveDownstream(CustomCurrent):
        # auto_park_snapshot defaults to True — auto-park fires after tick().

        async def tick(self, scheduler: Any) -> None:
            dn = DownstreamNode(inc_code=99)
            DownstreamNode.inc_dict[dn.inc_code] = dn
            strong_refs.append(dn)

    up_current = ProductiveUpstream(name="upstream3", cls=UpstreamNode, interval=10.0)
    dn_current = ProductiveDownstream(name="downstream3", cls=DownstreamNode, interval=0.05)

    ws = Watershed(
        window=_short_window(200),
        currents=[up_current, dn_current],
        edges=[Edge(from_name="upstream3", to_name="downstream3")],
    )
    tw = Tideweaver(ws, pass_interval=0.02)

    with caplog.at_level("WARNING", logger="incorporator.observability.tideweaver.scheduler"):
        async for _ in tw.run():
            pass

    warn_records = [r for r in caplog.records if "produced empty output" in r.message]
    assert len(warn_records) == 0, (
        f"expected no warning when tick produces data; got: {[r.message for r in warn_records]}"
    )


# ---------------------------------------------------------------------------
# Test 4 — tick raises (on_error="isolate") → isolated warning, NO empty-output WARNING
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_raising_tick_suppresses_empty_output_warning(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Raising tick (on_error='isolate') emits 'isolated tick failure', not empty-output WARNING.

    Proves that _tick_raised=True set in the isolate except-branch prevents
    the empty-output WARNING from firing even though wave_snapshot will be
    empty after the exception. The only WARNING must be 'isolated tick failure'.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamNode, DownstreamNode)

    up1 = UpstreamNode(inc_code=20)
    UpstreamNode._tideweaver_snapshot = [up1]  # type: ignore[attr-defined]

    class StableUpstream(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False

        async def tick(self, scheduler: Any) -> None:
            pass

    class BoomDownstream(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False

        async def tick(self, scheduler: Any) -> None:
            raise RuntimeError("deliberate tick failure")

    up_current = StableUpstream(name="upstream4", cls=UpstreamNode, interval=10.0)
    dn_current = BoomDownstream(
        name="downstream4", cls=DownstreamNode, interval=0.05, on_error="isolate"
    )

    ws = Watershed(
        window=_short_window(200),
        currents=[up_current, dn_current],
        edges=[Edge(from_name="upstream4", to_name="downstream4")],
    )
    tw = Tideweaver(ws, pass_interval=0.02)

    with caplog.at_level("WARNING", logger="incorporator.observability.tideweaver.scheduler"):
        async for _ in tw.run():
            pass

    isolated_records = [r for r in caplog.records if "isolated tick failure" in r.message]
    empty_output_records = [r for r in caplog.records if "produced empty output" in r.message]

    assert len(isolated_records) >= 1, (
        f"expected at least one 'isolated tick failure' WARNING; "
        f"got records: {[r.message for r in caplog.records]}"
    )
    assert len(empty_output_records) == 0, (
        f"_tick_raised must suppress empty-output WARNING; "
        f"got: {[r.message for r in empty_output_records]}"
    )
