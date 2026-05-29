"""Unit tests for ``Stream.parent_current``.

Four tests covering the parent-child drill declaration on ``Stream``:

1. ``None`` or missing upstream ``_tideweaver_snapshot`` silently skips.
2. Model validator: ``inc_parent`` in ``incorp_params`` + ``parent_current`` raises.
3. ``Watershed._validate_graph`` auto-derives a hard-gate edge from ``parent_current``.
4. Orphan ``parent_current`` (name not in watershed) raises at ``Watershed`` construction.

Row filtering itself is NOT a framework primitive — the parent declares its
scope at the URL or other source-side filter; the framework does not
post-filter rows.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ConfigDict

from incorporator import Incorporator
from incorporator.observability.tideweaver import Stream, Watershed
from incorporator.observability.tideweaver.current import Stream as StreamCls

# ---------------------------------------------------------------------------
# Module-level Incorporator subclasses
# ---------------------------------------------------------------------------


class UpstreamCls(Incorporator):
    """Upstream class whose _tideweaver_snapshot the child Stream reads."""

    model_config = ConfigDict(extra="allow")


class ChildCls(Incorporator):
    """Downstream class the child Stream drives via incorp()."""

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
# Minimal scheduler stub for _tick_stream
# ---------------------------------------------------------------------------


def _make_stub_scheduler(currents: list[StreamCls]) -> Any:
    """Build a minimal Tideweaver stub exposing only what _tick_stream needs."""
    stub = MagicMock()
    stub._currents_by_name = {c.name: c for c in currents}
    # Synchronous return — _tick_stream calls this with `pooled = self._get_or_create_client(...)`,
    # not `await`, so a plain MagicMock return value is correct here.
    stub._get_or_create_client = MagicMock(return_value=MagicMock())

    ws_stub = MagicMock()
    ws_stub.inflow = None
    stub.watershed = ws_stub
    return stub


# ---------------------------------------------------------------------------
# Test 1 — None or missing upstream snapshot silently skips
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_none_or_empty_upstream_snapshot_silently_skips(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """When the upstream _tideweaver_snapshot is None or absent, incorp() is not called.

    Proves first-tick safety: when Tideweaver hasn't reached the parent
    current yet (_tideweaver_snapshot absent), _tick_stream silently returns
    without calling incorp() on the child.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamCls, ChildCls)

    mock_incorp = AsyncMock(return_value=[])
    monkeypatch.setattr(ChildCls, "incorp", mock_incorp)

    upstream = Stream(name="up", cls=UpstreamCls, interval=1.0, incorp_params={"inc_file": "x"})
    child = Stream(
        name="child",
        cls=ChildCls,
        interval=1.0,
        parent_current="up",
        incorp_params={"inc_url": "http://x/{}"},
    )

    scheduler = _make_stub_scheduler([upstream, child])
    from incorporator.observability.tideweaver.scheduler import Tideweaver

    # Case A: _tideweaver_snapshot absent (getattr returns None)
    await Tideweaver._tick_stream(scheduler, child)
    mock_incorp.assert_not_called()

    # Case B: _tideweaver_snapshot is empty list (falsy)
    UpstreamCls._tideweaver_snapshot = []  # type: ignore[attr-defined]
    await Tideweaver._tick_stream(scheduler, child)
    mock_incorp.assert_not_called()


# ---------------------------------------------------------------------------
# Test 2 — model validator: inc_parent + parent_current is mutex
# ---------------------------------------------------------------------------


def test_inc_parent_with_parent_current_raises(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Stream construction rejects inc_parent inside incorp_params when parent_current is set."""
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError, match="inc_parent inside incorp_params, not both"):
        Stream(
            name="s",
            cls=ChildCls,
            interval=1.0,
            parent_current="up",
            incorp_params={"inc_parent": [], "inc_url": "http://x/{}"},
        )


# ---------------------------------------------------------------------------
# Test 3 — Watershed auto-derives edge from parent_current
# ---------------------------------------------------------------------------


def test_watershed_auto_derives_edge(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Watershed._validate_graph auto-adds a hard-gate edge from parent_current to child.

    Proves that a Stream with parent_current="up" causes the Watershed to
    append Edge(from_name="up", to_name="child") without the caller declaring
    it manually. Idempotent with explicit depends_on.
    """
    monkeypatch.chdir(tmp_path)

    upstream = Stream(name="up", cls=UpstreamCls, interval=1.0, incorp_params={"inc_file": "x"})
    child = Stream(
        name="child",
        cls=ChildCls,
        interval=1.0,
        parent_current="up",
        incorp_params={"inc_url": "http://x/{}"},
    )
    start = datetime.now(timezone.utc)
    end = start + timedelta(minutes=1)
    ws = Watershed(window=(start, end), currents=[upstream, child])
    edge_pairs = {(e.from_name, e.to_name) for e in ws.edges}
    assert ("up", "child") in edge_pairs, f"Expected auto-derived edge 'up'->'child'; got {edge_pairs}"

    # Idempotent: explicit depends_on should not create a duplicate
    child2 = Stream(
        name="child2",
        cls=ChildCls,
        interval=1.0,
        parent_current="up",
        depends_on=["up"],
        incorp_params={"inc_url": "http://x/{}"},
    )
    ws2 = Watershed(window=(start, end), currents=[upstream, child2])
    count = sum(1 for e in ws2.edges if e.from_name == "up" and e.to_name == "child2")
    assert count == 1, f"Edge 'up'->'child2' must appear exactly once; found {count}"


# ---------------------------------------------------------------------------
# Test 4 — orphan parent_current raises
# ---------------------------------------------------------------------------


def test_orphan_parent_current_raises(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """When parent_current names a current that doesn't exist in the watershed, raise."""
    monkeypatch.chdir(tmp_path)

    child = Stream(
        name="child",
        cls=ChildCls,
        interval=1.0,
        parent_current="nonexistent",
        incorp_params={"inc_url": "http://x/{}"},
    )
    start = datetime.now(timezone.utc)
    end = start + timedelta(minutes=1)
    with pytest.raises(ValueError, match="parent_current='nonexistent'"):
        Watershed(window=(start, end), currents=[child])
