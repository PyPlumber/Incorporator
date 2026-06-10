"""Audit-surface tests for Stream + Fjord parent-child snapshot-empty diagnostics.

Three tests covering the runtime warnings the scheduler emits when a
parent-child Current would silently produce empty output:

1. Stream(parent_current=...) with empty upstream snapshot → "snapshot is empty" WARNING.
2. Fjord(parent_currents=...) with empty upstream snapshot → "upstream snapshot is empty" WARNING.
3. Schema check: Wave + CurrentOutcome carry the parent_snapshot_size field, default None.

Row filtering is NOT a framework primitive; this file's coverage focuses on the
parent's snapshot being absent or empty. For filter-related cases (e.g.
"my filter matched 0 of N rows"), the framework's answer is filter-at-source
(URL query params, SQL WHERE) — not a scheduler-emitted WARNING.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ConfigDict

from incorporator import Incorporator
from incorporator.tideweaver import Fjord, Stream
from incorporator.tideweaver.current import Fjord as FjordCls
from incorporator.tideweaver.current_outcome import CurrentOutcome
from incorporator.observability.wave import Wave

_AUDIT_LOGGER = "incorporator.tideweaver.scheduler"


class UpstreamCls(Incorporator):
    """Upstream class whose _tideweaver_snapshot the child Current reads."""

    model_config = ConfigDict(extra="allow")


class ChildStreamCls(Incorporator):
    """Downstream class the child Stream drives via incorp()."""

    model_config = ConfigDict(extra="allow")


class FjordOutputCls(Incorporator):
    """Downstream class the Fjord's outflow targets."""

    model_config = ConfigDict(extra="allow")


def _reset_registries(*classes: type[Incorporator]) -> None:
    for cls in classes:
        cls.inc_dict.clear()
        if "_tideweaver_snapshot" in cls.__dict__:
            try:
                delattr(cls, "_tideweaver_snapshot")
            except AttributeError:
                pass


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

    monkeypatch.setattr("incorporator.tideweaver.scheduler.flush", capturing_flush)
    monkeypatch.setattr("incorporator.usercode.load_outflow_module", stub_loader)
    return captured


@pytest.mark.asyncio
async def test_stream_empty_upstream_emits_warning(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Stream with parent_current= and an empty upstream snapshot emits a snapshot-empty WARNING."""
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamCls, ChildStreamCls)

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
    from incorporator.tideweaver.scheduler import Tideweaver

    with caplog.at_level(logging.WARNING, logger=_AUDIT_LOGGER):
        await Tideweaver._tick_stream(scheduler, child)

    mock_incorp.assert_not_called()
    warnings = [r for r in caplog.records if "snapshot is empty" in r.getMessage()]
    assert warnings, f"expected snapshot-empty WARNING; got: {[r.getMessage() for r in caplog.records]}"
    assert "child" in warnings[0].getMessage()
    assert "'up'" in warnings[0].getMessage()


@pytest.mark.asyncio
async def test_fjord_empty_upstream_emits_warning(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Fjord with parent_currents naming an empty upstream emits a snapshot-empty WARNING."""
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamCls, FjordOutputCls)

    upstream = Stream(name="up", cls=UpstreamCls, interval=1.0, incorp_params={"inc_file": "x"})
    fjord = Fjord(
        name="fjord",
        cls=FjordOutputCls,
        interval=1.0,
        parent_currents=["up"],
    )

    _install_fjord_flush_capture(monkeypatch)
    scheduler = _make_fjord_scheduler([upstream], fjord)
    from incorporator.tideweaver.scheduler import Tideweaver

    with caplog.at_level(logging.WARNING, logger=_AUDIT_LOGGER):
        await Tideweaver._tick_fjord(scheduler, fjord)

    warnings = [r for r in caplog.records if "upstream snapshot is empty" in r.getMessage()]
    assert warnings, f"expected upstream-empty WARNING; got: {[r.getMessage() for r in caplog.records]}"
    msg = warnings[0].getMessage()
    assert "fjord" in msg
    assert "'up'" in msg


def test_wave_and_current_outcome_carry_parent_snapshot_size_field() -> None:
    """Schema check: Wave + CurrentOutcome accept and default parent_snapshot_size to None."""
    wave_default = Wave(chunk_index=0, rows_processed=10, processing_time_sec=0.5)
    assert wave_default.parent_snapshot_size is None

    wave_populated = Wave(
        chunk_index=1,
        rows_processed=5,
        processing_time_sec=0.7,
        parent_snapshot_size=30,
    )
    assert wave_populated.parent_snapshot_size == 30

    outcome_default = CurrentOutcome(name="x", status="fired")
    assert outcome_default.parent_snapshot_size is None

    outcome_populated = CurrentOutcome(
        name="child",
        status="fired",
        parent_snapshot_size=30,
        last_wave_at=datetime.now(timezone.utc),
    )
    assert outcome_populated.parent_snapshot_size == 30
