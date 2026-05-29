"""Unit tests for ``Fjord.parent_currents``.

Two tests covering the parent-child declaration on ``Fjord``:

1. None/missing upstream snapshot → state-dict entry is empty list.
2. ``Watershed._validate_graph`` auto-derives one hard-gate edge per name in
   ``parent_currents``.

Row filtering itself is NOT a framework primitive — each named parent declares
its scope at the URL or other source-side filter; the framework does not
post-filter rows.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ConfigDict

from incorporator import Incorporator
from incorporator.observability.tideweaver import Fjord, Stream, Watershed
from incorporator.observability.tideweaver.current import Fjord as FjordCls


class UpstreamA(Incorporator):
    """First upstream class whose _tideweaver_snapshot the Fjord reads."""

    model_config = ConfigDict(extra="allow")


class UpstreamB(Incorporator):
    """Second upstream class for multi-parent tests."""

    model_config = ConfigDict(extra="allow")


class FjordCls_(Incorporator):
    """Downstream class the Fjord's outflow targets."""

    model_config = ConfigDict(extra="allow")


def _reset_registries(*classes: type[Incorporator]) -> None:
    """Wipe per-class inc_dict + parked snapshot between tests."""
    for cls in classes:
        cls.inc_dict.clear()
        if "_tideweaver_snapshot" in cls.__dict__:
            try:
                delattr(cls, "_tideweaver_snapshot")
            except AttributeError:
                pass


def _make_stub_scheduler(
    upstreams: list[Any],
    fjord_current: FjordCls,
    *,
    outflow_path: str = "/tmp/_unused_outflow.py",
) -> Any:
    """Build a minimal Tideweaver stub exposing only what _tick_fjord needs."""
    stub = MagicMock()
    by_name = {c.name: c for c in (*upstreams, fjord_current)}
    stub._currents_by_name = by_name
    stub._upstream = {fjord_current.name: [(u.name, MagicMock()) for u in upstreams]}
    stub._edge_state = {}
    stub._transitive_upstreams = MagicMock(return_value=[u.name for u in upstreams])

    ws_stub = MagicMock()
    ws_stub.outflow = outflow_path
    stub.watershed = ws_stub
    return stub


def _install_flush_capture(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
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
# Test 1 — None/missing upstream snapshot → state-dict entry is empty list
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_none_upstream_snapshot_yields_empty_state_entry(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """When the upstream has no parked snapshot, state[cls.__name__] is empty."""
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamA, FjordCls_)
    # No _tideweaver_snapshot parked; inc_dict is empty.

    upstream = Stream(name="up", cls=UpstreamA, interval=1.0, incorp_params={"inc_file": "x"})
    fjord = Fjord(
        name="fjord",
        cls=FjordCls_,
        interval=1.0,
        parent_currents=["up"],
    )

    captured = _install_flush_capture(monkeypatch)
    scheduler = _make_stub_scheduler([upstream], fjord)
    from incorporator.observability.tideweaver.scheduler import Tideweaver

    await Tideweaver._tick_fjord(scheduler, fjord)

    assert captured["state"]["UpstreamA"] == [], "no snapshot + empty inc_dict must yield empty list"


# ---------------------------------------------------------------------------
# Test 2 — Watershed auto-derives one hard-gate edge per parent_currents name
# ---------------------------------------------------------------------------


def test_watershed_auto_derives_edges_from_parent_currents() -> None:
    """For each name in Fjord.parent_currents, _validate_graph adds an auto-derived Edge.

    Idempotent against explicit depends_on, and orphan references raise.
    """
    start = datetime(2026, 5, 29, tzinfo=timezone.utc)
    end = start + timedelta(seconds=60)

    upstream_a = Stream(name="up_a", cls=UpstreamA, interval=1.0, incorp_params={"inc_file": "a"})
    upstream_b = Stream(name="up_b", cls=UpstreamB, interval=1.0, incorp_params={"inc_file": "b"})
    fjord = Fjord(
        name="fjord",
        cls=FjordCls_,
        interval=1.0,
        parent_currents=["up_a", "up_b"],
        outflow="/tmp/_unused.py",  # type: ignore[arg-type]
    )

    ws = Watershed(window=(start, end), currents=[upstream_a, upstream_b, fjord])
    edge_pairs = {(e.from_name, e.to_name) for e in ws.edges}
    assert ("up_a", "fjord") in edge_pairs, f"Expected auto-derived edge 'up_a'->'fjord'; got {edge_pairs}"
    assert ("up_b", "fjord") in edge_pairs, f"Expected auto-derived edge 'up_b'->'fjord'; got {edge_pairs}"

    # Idempotent: explicit depends_on must not duplicate the auto-derived edge
    fjord_with_deps = Fjord(
        name="fjord2",
        cls=FjordCls_,
        interval=1.0,
        depends_on=["up_a"],
        parent_currents=["up_a"],
        outflow="/tmp/_unused.py",  # type: ignore[arg-type]
    )
    ws2 = Watershed(window=(start, end), currents=[upstream_a, fjord_with_deps])
    count = sum(1 for e in ws2.edges if e.from_name == "up_a" and e.to_name == "fjord2")
    assert count == 1, f"Edge 'up_a'->'fjord2' must appear exactly once; found {count}"

    # Orphan parent_currents reference must raise
    fjord_orphan = Fjord(
        name="fjord3",
        cls=FjordCls_,
        interval=1.0,
        parent_currents=["nonexistent"],
        outflow="/tmp/_unused.py",  # type: ignore[arg-type]
    )
    with pytest.raises(ValueError, match=r"parent_currents references 'nonexistent'"):
        Watershed(window=(start, end), currents=[upstream_a, fjord_orphan])
