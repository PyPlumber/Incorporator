"""Unit tests for ``Stream.parent_current`` + ``Stream.parent_filter`` (Phase B+C).

Seven tests covering the parent-child drill mode on ``Stream``:

1. Tuple ``parent_filter`` resolves the upstream snapshot and calls ``incorp()``.
2. Callable ``parent_filter`` resolves the upstream snapshot and calls ``incorp()``.
3. Empty filtered list short-circuits: ``incorp()`` is never called.
4. ``None`` or missing upstream ``_tideweaver_snapshot`` silently skips.
5. Model validators: missing ``parent_current`` with ``parent_filter`` raises;
   ``inc_parent`` in ``incorp_params`` + ``parent_current`` raises;
   malformed tuple raises.
6. ``Watershed._validate_graph`` auto-derives a hard-gate edge from ``parent_current``.
7. Orphan ``parent_current`` (name not in watershed) raises at ``Watershed`` construction.
"""

from __future__ import annotations

import operator
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
# Test 1 — tuple parent_filter resolves and drills
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tuple_parent_filter_resolves_and_drills(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Tuple parent_filter selects matching rows and calls incorp() with them.

    Proves that when the upstream snapshot has three rows with different
    ``division`` values, a structured-tuple ``("division", operator.eq, 201)``
    passes exactly the matching row to ``ChildCls.incorp(inc_parent=[...])``
    and that ``_tideweaver_snapshot`` is parked on the child class afterward.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamCls, ChildCls)

    row_a = UpstreamCls(inc_code=1, division=201)  # type: ignore[call-arg]
    row_b = UpstreamCls(inc_code=2, division=200)  # type: ignore[call-arg]
    row_c = UpstreamCls(inc_code=3, division=202)  # type: ignore[call-arg]
    UpstreamCls._tideweaver_snapshot = [row_a, row_b, row_c]  # type: ignore[attr-defined]

    drilled = [ChildCls(inc_code=1)]
    mock_incorp = AsyncMock(return_value=drilled)
    monkeypatch.setattr(ChildCls, "incorp", mock_incorp)

    upstream = Stream(name="up", cls=UpstreamCls, interval=1.0, incorp_params={"inc_file": "x"})
    child = Stream(
        name="child",
        cls=ChildCls,
        interval=1.0,
        parent_current="up",
        parent_filter=("division", operator.eq, 201),
        incorp_params={"inc_url": "http://x/{}", "inc_child": "inc_code"},
    )

    scheduler = _make_stub_scheduler([upstream, child])
    from incorporator.observability.tideweaver.scheduler import Tideweaver

    await Tideweaver._tick_stream(scheduler, child)

    mock_incorp.assert_called_once()
    call_kwargs = mock_incorp.call_args.kwargs
    assert call_kwargs["inc_parent"] == [row_a], f"Expected only row_a; got {call_kwargs['inc_parent']}"
    snap = getattr(ChildCls, "_tideweaver_snapshot", None)
    assert snap is not None, "_tideweaver_snapshot must be parked after the drill"


# ---------------------------------------------------------------------------
# Test 2 — callable parent_filter resolves and drills
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_callable_parent_filter_resolves_and_drills(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Callable parent_filter lambda selects matching rows and calls incorp().

    Proves that a ``lambda r: r.division == 201`` callable produces the
    same filtering behaviour as the structured-tuple form.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamCls, ChildCls)

    row_a = UpstreamCls(inc_code=1, division=201)  # type: ignore[call-arg]
    row_b = UpstreamCls(inc_code=2, division=200)  # type: ignore[call-arg]
    UpstreamCls._tideweaver_snapshot = [row_a, row_b]  # type: ignore[attr-defined]

    drilled = [ChildCls(inc_code=1)]
    mock_incorp = AsyncMock(return_value=drilled)
    monkeypatch.setattr(ChildCls, "incorp", mock_incorp)

    upstream = Stream(name="up", cls=UpstreamCls, interval=1.0, incorp_params={"inc_file": "x"})
    child = Stream(
        name="child",
        cls=ChildCls,
        interval=1.0,
        parent_current="up",
        parent_filter=lambda r: r.division == 201,  # type: ignore[union-attr]
        incorp_params={"inc_url": "http://x/{}", "inc_child": "inc_code"},
    )

    scheduler = _make_stub_scheduler([upstream, child])
    from incorporator.observability.tideweaver.scheduler import Tideweaver

    await Tideweaver._tick_stream(scheduler, child)

    mock_incorp.assert_called_once()
    call_kwargs = mock_incorp.call_args.kwargs
    assert call_kwargs["inc_parent"] == [row_a], f"Expected only row_a; got {call_kwargs['inc_parent']}"


# ---------------------------------------------------------------------------
# Test 3 — empty filtered list short-circuits
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_filtered_list_short_circuits(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """When parent_filter rejects every row, incorp() is never called.

    Proves that a predicate matching nothing (``operator.eq, 999``) causes
    _tick_stream to return before calling incorp(), preventing unnecessary
    network traffic.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamCls, ChildCls)

    row_a = UpstreamCls(inc_code=1, division=201)  # type: ignore[call-arg]
    UpstreamCls._tideweaver_snapshot = [row_a]  # type: ignore[attr-defined]

    mock_incorp = AsyncMock(return_value=[])
    monkeypatch.setattr(ChildCls, "incorp", mock_incorp)

    upstream = Stream(name="up", cls=UpstreamCls, interval=1.0, incorp_params={"inc_file": "x"})
    child = Stream(
        name="child",
        cls=ChildCls,
        interval=1.0,
        parent_current="up",
        parent_filter=("division", operator.eq, 999),
        incorp_params={"inc_url": "http://x/{}"},
    )

    scheduler = _make_stub_scheduler([upstream, child])
    from incorporator.observability.tideweaver.scheduler import Tideweaver

    await Tideweaver._tick_stream(scheduler, child)

    mock_incorp.assert_not_called()


# ---------------------------------------------------------------------------
# Test 4 — None or missing upstream snapshot silently skips
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
# Test 5 — model validators
# ---------------------------------------------------------------------------


def test_model_validators(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Construction-time validators on parent_current / parent_filter raise correctly.

    Proves three guard clauses:
    - parent_filter without parent_current raises ValueError.
    - inc_parent in incorp_params with parent_current raises ValueError.
    - Malformed tuple (non-callable op) raises ValueError.
    """
    monkeypatch.chdir(tmp_path)

    # Guard 1: parent_filter requires parent_current
    with pytest.raises(ValueError, match="parent_filter requires parent_current"):
        Stream(
            name="s",
            cls=ChildCls,
            interval=1.0,
            parent_filter=lambda r: True,
        )

    # Guard 2: inc_parent in incorp_params conflicts with parent_current
    with pytest.raises(ValueError, match="inc_parent inside incorp_params, not both"):
        Stream(
            name="s",
            cls=ChildCls,
            interval=1.0,
            parent_current="up",
            incorp_params={"inc_parent": [], "inc_url": "http://x/{}"},
        )

    # Guard 3: malformed tuple (second element not callable)
    with pytest.raises(ValueError, match="parent_filter tuple"):
        Stream(
            name="s",
            cls=ChildCls,
            interval=1.0,
            parent_current="up",
            parent_filter=("division", "not_callable", 201),  # type: ignore[arg-type]
        )


# ---------------------------------------------------------------------------
# Test 6 — Watershed auto-derives edge from parent_current
# ---------------------------------------------------------------------------


def test_watershed_auto_derives_edge(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Watershed._validate_graph auto-adds a hard-gate edge from parent_current to child.

    Proves that a Stream with parent_current="up" causes the Watershed to
    append Edge(from_name="up", to_name="child") without the caller declaring
    it explicitly, and that declaring it explicitly is idempotent (no duplicate).
    """
    monkeypatch.chdir(tmp_path)

    start = datetime.now(timezone.utc)
    end = start + timedelta(hours=1)

    upstream = Stream(name="up", cls=UpstreamCls, interval=1.0, incorp_params={"inc_file": "x"})
    child = Stream(
        name="child",
        cls=ChildCls,
        interval=2.0,
        parent_current="up",
        incorp_params={"inc_url": "http://x/{}"},
    )

    ws = Watershed(window=(start, end), currents=[upstream, child])
    edge_pairs = {(e.from_name, e.to_name) for e in ws.edges}
    assert ("up", "child") in edge_pairs, f"Expected auto-derived edge 'up'→'child'; got {edge_pairs}"

    # Idempotent: explicit depends_on should not create a duplicate
    child2 = Stream(
        name="child2",
        cls=ChildCls,
        interval=2.0,
        parent_current="up",
        depends_on=["up"],
        incorp_params={"inc_url": "http://x/{}"},
    )
    ws2 = Watershed(window=(start, end), currents=[upstream, child2])
    count = sum(1 for e in ws2.edges if e.from_name == "up" and e.to_name == "child2")
    assert count == 1, f"Edge 'up'→'child2' must appear exactly once; found {count}"


# ---------------------------------------------------------------------------
# Test 7 — orphan parent_current raises at Watershed construction
# ---------------------------------------------------------------------------


def test_orphan_parent_current_raises(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Watershed rejects a Stream whose parent_current names a non-existent current.

    Proves that Stream(parent_current="missing") with no current named "missing"
    in the watershed raises ValueError at construction time with a message
    mentioning both the stream name and the unknown parent_current value.
    """
    monkeypatch.chdir(tmp_path)

    start = datetime.now(timezone.utc)
    end = start + timedelta(hours=1)

    child = Stream(
        name="child",
        cls=ChildCls,
        interval=1.0,
        parent_current="missing",
        incorp_params={"inc_url": "http://x/{}"},
    )

    with pytest.raises(ValueError, match="missing"):
        Watershed(window=(start, end), currents=[child])
