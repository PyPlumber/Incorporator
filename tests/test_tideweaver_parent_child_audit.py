"""Audit-surface tests for Stream + Fjord parent-child silent-skip diagnostics.

Six tests covering the runtime warnings the scheduler emits when a
parent-child Current would silently produce empty output:

1. Stream(parent_current=...) with empty upstream snapshot → "snapshot is empty" WARNING.
2. Stream(parent_current=..., parent_filter=...) with filter matching zero rows → "matched 0 of N rows" WARNING.
3. Stream(parent_current=...) with non-empty filter result → no WARNING.
4. Fjord(parent_currents=...) with empty upstream snapshot → "upstream snapshot is empty" WARNING.
5. Fjord(parent_currents=..., parent_filters=...) with filter matching zero rows → "matched 0 of N rows" WARNING.
6. Schema check: Wave + CurrentOutcome carry the new parent_snapshot_size / filter_match_count fields, default None.
"""

from __future__ import annotations

import logging
import operator
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ConfigDict

from incorporator import Incorporator
from incorporator.observability.tideweaver import Fjord, Stream
from incorporator.observability.tideweaver.current import Fjord as FjordCls
from incorporator.observability.tideweaver.current_outcome import CurrentOutcome
from incorporator.observability.wave import Wave

_AUDIT_LOGGER = "incorporator.observability.tideweaver.scheduler"


# ---------------------------------------------------------------------------
# Module-level Incorporator subclasses
# ---------------------------------------------------------------------------


class UpstreamCls(Incorporator):
    """Upstream class whose _tideweaver_snapshot the child Current reads."""

    model_config = ConfigDict(extra="allow")


class ChildStreamCls(Incorporator):
    """Downstream class the child Stream drives via incorp()."""

    model_config = ConfigDict(extra="allow")


class FjordOutputCls(Incorporator):
    """Downstream class the Fjord's outflow targets."""

    model_config = ConfigDict(extra="allow")


# ---------------------------------------------------------------------------
# Reset helper
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


# ---------------------------------------------------------------------------
# Stub schedulers — minimal surface required by _tick_stream / _tick_fjord
# ---------------------------------------------------------------------------


def _make_stream_scheduler(currents: list[Any]) -> Any:
    stub = MagicMock()
    stub._currents_by_name = {c.name: c for c in currents}
    stub._get_or_create_client = MagicMock(return_value=MagicMock())
    ws_stub = MagicMock()
    ws_stub.inflow = None
    stub.watershed = ws_stub
    return stub


def _make_fjord_scheduler(upstreams: list[Any], fjord_current: FjordCls) -> Any:
    stub = MagicMock()
    by_name = {c.name: c for c in (*upstreams, fjord_current)}
    stub._currents_by_name = by_name
    stub._upstream = {fjord_current.name: [(u.name, MagicMock()) for u in upstreams]}
    stub._edge_state = {}
    stub._transitive_upstreams = MagicMock(return_value=[u.name for u in upstreams])
    ws_stub = MagicMock()
    ws_stub.outflow = "/tmp/_unused_outflow.py"
    stub.watershed = ws_stub
    return stub


def _install_fjord_flush_capture(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    async def capturing_flush(_outflow_fn: Any, state: dict[str, Any], **_kw: Any) -> Any:
        captured["state"] = state
        return
        yield  # pragma: no cover

    def stub_loader(_path: Any) -> tuple[Any, Any]:
        return (lambda state: [], None)

    monkeypatch.setattr("incorporator.observability.tideweaver.scheduler.flush", capturing_flush)
    monkeypatch.setattr("incorporator.usercode.load_outflow_module", stub_loader)
    return captured


# ---------------------------------------------------------------------------
# Test 1 — Stream(parent_current=) empty upstream → WARNING
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_empty_upstream_emits_warning(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Stream with parent_current= and an empty upstream snapshot emits a snapshot-empty WARNING."""
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamCls, ChildStreamCls)
    # No _tideweaver_snapshot parked; inc_dict is empty.

    mock_incorp = AsyncMock()
    monkeypatch.setattr(ChildStreamCls, "incorp", mock_incorp)

    upstream = Stream(name="up", cls=UpstreamCls, interval=1.0, incorp_params={"inc_file": "x"})
    child = Stream(
        name="child",
        cls=ChildStreamCls,
        interval=1.0,
        parent_current="up",
        incorp_params={"inc_url": "http://x/{}", "inc_child": "inc_code"},
    )

    scheduler = _make_stream_scheduler([upstream, child])
    from incorporator.observability.tideweaver.scheduler import Tideweaver

    with caplog.at_level(logging.WARNING, logger=_AUDIT_LOGGER):
        await Tideweaver._tick_stream(scheduler, child)

    mock_incorp.assert_not_called()
    warnings = [r for r in caplog.records if "snapshot is empty" in r.getMessage()]
    assert warnings, f"expected snapshot-empty WARNING; got: {[r.getMessage() for r in caplog.records]}"
    assert "child" in warnings[0].getMessage(), "warning must name the Stream"
    assert "'up'" in warnings[0].getMessage(), "warning must name the parent_current"


# ---------------------------------------------------------------------------
# Test 2 — Stream(parent_filter=) matched zero → WARNING
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_filter_matched_zero_emits_warning(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Stream with parent_filter matching zero rows emits a matched-0 WARNING with the row count."""
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamCls, ChildStreamCls)

    row_a = UpstreamCls(inc_code=1, division=200)  # type: ignore[call-arg]
    row_b = UpstreamCls(inc_code=2, division=200)  # type: ignore[call-arg]
    UpstreamCls._tideweaver_snapshot = [row_a, row_b]  # type: ignore[attr-defined]

    mock_incorp = AsyncMock()
    monkeypatch.setattr(ChildStreamCls, "incorp", mock_incorp)

    upstream = Stream(name="up", cls=UpstreamCls, interval=1.0, incorp_params={"inc_file": "x"})
    child = Stream(
        name="child",
        cls=ChildStreamCls,
        interval=1.0,
        parent_current="up",
        parent_filter=("division", operator.eq, 999),
        incorp_params={"inc_url": "http://x/{}", "inc_child": "inc_code"},
    )

    scheduler = _make_stream_scheduler([upstream, child])
    from incorporator.observability.tideweaver.scheduler import Tideweaver

    with caplog.at_level(logging.WARNING, logger=_AUDIT_LOGGER):
        await Tideweaver._tick_stream(scheduler, child)

    mock_incorp.assert_not_called()
    warnings = [r for r in caplog.records if "matched 0 of" in r.getMessage()]
    assert warnings, f"expected matched-0 WARNING; got: {[r.getMessage() for r in caplog.records]}"
    msg = warnings[0].getMessage()
    assert "child" in msg, "warning must name the Stream"
    assert "matched 0 of 2 rows" in msg, "warning must include the original row count"
    assert "predicate attribute name" in msg, "warning must hint at the typo failure mode"


# ---------------------------------------------------------------------------
# Test 3 — Stream(parent_filter=) matched some → no WARNING
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_filter_matched_some_no_warning(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """When the filter matches at least one row, no parent-child WARNING fires."""
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamCls, ChildStreamCls)

    row_a = UpstreamCls(inc_code=1, division=201)  # type: ignore[call-arg]
    UpstreamCls._tideweaver_snapshot = [row_a]  # type: ignore[attr-defined]

    mock_incorp = AsyncMock(return_value=[])
    monkeypatch.setattr(ChildStreamCls, "incorp", mock_incorp)

    upstream = Stream(name="up", cls=UpstreamCls, interval=1.0, incorp_params={"inc_file": "x"})
    child = Stream(
        name="child",
        cls=ChildStreamCls,
        interval=1.0,
        parent_current="up",
        parent_filter=("division", operator.eq, 201),
        incorp_params={"inc_url": "http://x/{}", "inc_child": "inc_code"},
    )

    scheduler = _make_stream_scheduler([upstream, child])
    from incorporator.observability.tideweaver.scheduler import Tideweaver

    with caplog.at_level(logging.WARNING, logger=_AUDIT_LOGGER):
        await Tideweaver._tick_stream(scheduler, child)

    mock_incorp.assert_called_once()
    pc_warnings = [
        r for r in caplog.records if "matched 0 of" in r.getMessage() or "snapshot is empty" in r.getMessage()
    ]
    assert not pc_warnings, f"unexpected parent-child WARNING fired: {[r.getMessage() for r in pc_warnings]}"


# ---------------------------------------------------------------------------
# Test 4 — Fjord(parent_currents=) empty upstream → WARNING
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fjord_empty_upstream_emits_warning(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Fjord with parent_currents naming an empty upstream emits a snapshot-empty WARNING."""
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamCls, FjordOutputCls)
    # No _tideweaver_snapshot parked; inc_dict empty.

    upstream = Stream(name="up", cls=UpstreamCls, interval=1.0, incorp_params={"inc_file": "x"})
    fjord = Fjord(
        name="fjord",
        cls=FjordOutputCls,
        interval=1.0,
        parent_currents=["up"],
    )

    _install_fjord_flush_capture(monkeypatch)
    scheduler = _make_fjord_scheduler([upstream], fjord)
    from incorporator.observability.tideweaver.scheduler import Tideweaver

    with caplog.at_level(logging.WARNING, logger=_AUDIT_LOGGER):
        await Tideweaver._tick_fjord(scheduler, fjord)

    warnings = [r for r in caplog.records if "upstream snapshot is empty" in r.getMessage()]
    assert warnings, f"expected upstream-empty WARNING; got: {[r.getMessage() for r in caplog.records]}"
    msg = warnings[0].getMessage()
    assert "fjord" in msg, "warning must name the Fjord"
    assert "'up'" in msg, "warning must name the parent_currents entry"


# ---------------------------------------------------------------------------
# Test 5 — Fjord(parent_filters=) matched zero → WARNING
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fjord_filter_matched_zero_emits_warning(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Fjord with parent_filters matching zero rows emits a matched-0 WARNING."""
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamCls, FjordOutputCls)

    row_a = UpstreamCls(inc_code=1, division=200)  # type: ignore[call-arg]
    row_b = UpstreamCls(inc_code=2, division=200)  # type: ignore[call-arg]
    UpstreamCls._tideweaver_snapshot = [row_a, row_b]  # type: ignore[attr-defined]

    upstream = Stream(name="up", cls=UpstreamCls, interval=1.0, incorp_params={"inc_file": "x"})
    fjord = Fjord(
        name="fjord",
        cls=FjordOutputCls,
        interval=1.0,
        parent_currents=["up"],
        parent_filters={"up": ("division", operator.eq, 999)},
    )

    _install_fjord_flush_capture(monkeypatch)
    scheduler = _make_fjord_scheduler([upstream], fjord)
    from incorporator.observability.tideweaver.scheduler import Tideweaver

    with caplog.at_level(logging.WARNING, logger=_AUDIT_LOGGER):
        await Tideweaver._tick_fjord(scheduler, fjord)

    warnings = [r for r in caplog.records if "matched 0 of" in r.getMessage()]
    assert warnings, f"expected matched-0 WARNING; got: {[r.getMessage() for r in caplog.records]}"
    msg = warnings[0].getMessage()
    assert "fjord" in msg, "warning must name the Fjord"
    assert "matched 0 of 2 rows" in msg, "warning must include the original row count"
    assert "predicate attribute name" in msg, "warning must hint at the typo failure mode"


# ---------------------------------------------------------------------------
# Test 6 — Wave + CurrentOutcome carry the new parent_snapshot_size / filter_match_count fields
# ---------------------------------------------------------------------------


def test_wave_and_current_outcome_carry_parent_child_fields() -> None:
    """Schema check: Wave + CurrentOutcome accept and default the new audit fields to None.

    Constructing both with defaults must not break — and an explicit
    populate must round-trip. This is the schema-side test for the
    new fields that runtime instrumentation can populate later.
    """
    wave_default = Wave(chunk_index=0, rows_processed=10, processing_time_sec=0.5)
    assert wave_default.parent_snapshot_size is None
    assert wave_default.filter_match_count is None

    wave_populated = Wave(
        chunk_index=1,
        rows_processed=5,
        processing_time_sec=0.7,
        parent_snapshot_size=30,
        filter_match_count=5,
    )
    assert wave_populated.parent_snapshot_size == 30
    assert wave_populated.filter_match_count == 5

    outcome_default = CurrentOutcome(name="x", status="fired")
    assert outcome_default.parent_snapshot_size is None
    assert outcome_default.filter_match_count is None

    outcome_populated = CurrentOutcome(
        name="child",
        status="fired",
        parent_snapshot_size=30,
        filter_match_count=5,
        last_wave_at=datetime.now(timezone.utc),
    )
    assert outcome_populated.parent_snapshot_size == 30
    assert outcome_populated.filter_match_count == 5
