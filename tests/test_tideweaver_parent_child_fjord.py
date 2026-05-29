"""Unit tests for ``Fjord.parent_currents`` + ``Fjord.parent_filters``.

Seven tests covering the parent-child filter mode on ``Fjord``:

1. Tuple ``parent_filter`` filters the named upstream's state-dict entry.
2. Callable ``parent_filter`` filters the named upstream's state-dict entry.
3. Empty filter result → state-dict entry is empty list (no silent skip; outflow still fires).
4. None/missing upstream snapshot → state-dict entry is empty list.
5. Validator: malformed tuple → ValueError at construction.
6. Validator: orphan filter key not in ``parent_currents`` → ValueError at construction.
7. ``Watershed._validate_graph`` auto-derives a hard-gate edge from each ``parent_currents`` name → Fjord.
"""

from __future__ import annotations

import operator
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ConfigDict

from incorporator import Incorporator
from incorporator.observability.tideweaver import Fjord, Stream, Watershed
from incorporator.observability.tideweaver.current import Fjord as FjordCls

# ---------------------------------------------------------------------------
# Module-level Incorporator subclasses
# ---------------------------------------------------------------------------


class UpstreamA(Incorporator):
    """First upstream class whose _tideweaver_snapshot the Fjord reads."""

    model_config = ConfigDict(extra="allow")


class UpstreamB(Incorporator):
    """Second upstream class for multi-parent tests."""

    model_config = ConfigDict(extra="allow")


class FjordCls_(Incorporator):
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
# Stub scheduler for _tick_fjord
# ---------------------------------------------------------------------------


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


async def _empty_flush(*_args: Any, **_kwargs: Any) -> Any:
    """No-op async-generator stub used to bypass the real outflow pipeline."""
    return
    yield  # pragma: no cover  (makes this an async generator)


def _install_flush_capture(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Patch flush() and load_outflow_module() to capture the state dict."""
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
# Test 1 — tuple parent_filter filters the state-dict entry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tuple_parent_filter_filters_state(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Tuple parent_filter selects matching rows in the state dict."""
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamA, FjordCls_)

    row_a = UpstreamA(inc_code=1, division=201)  # type: ignore[call-arg]
    row_b = UpstreamA(inc_code=2, division=200)  # type: ignore[call-arg]
    row_c = UpstreamA(inc_code=3, division=201)  # type: ignore[call-arg]
    UpstreamA._tideweaver_snapshot = [row_a, row_b, row_c]  # type: ignore[attr-defined]

    upstream = Stream(name="up", cls=UpstreamA, interval=1.0, incorp_params={"inc_file": "x"})
    fjord = Fjord(
        name="fjord",
        cls=FjordCls_,
        interval=1.0,
        parent_currents=["up"],
        parent_filters={"up": ("division", operator.eq, 201)},
    )

    captured = _install_flush_capture(monkeypatch)
    scheduler = _make_stub_scheduler([upstream], fjord)
    from incorporator.observability.tideweaver.scheduler import Tideweaver

    await Tideweaver._tick_fjord(scheduler, fjord)

    state_rows = captured["state"]["UpstreamA"]
    assert state_rows == [row_a, row_c], f"Expected only rows with division=201; got {state_rows}"


# ---------------------------------------------------------------------------
# Test 2 — callable parent_filter filters the state-dict entry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_callable_parent_filter_filters_state(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Callable parent_filter selects matching rows in the state dict."""
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamA, FjordCls_)

    row_a = UpstreamA(inc_code=1, score=10)  # type: ignore[call-arg]
    row_b = UpstreamA(inc_code=2, score=20)  # type: ignore[call-arg]
    UpstreamA._tideweaver_snapshot = [row_a, row_b]  # type: ignore[attr-defined]

    def above_15(row: Any) -> bool:
        return getattr(row, "score", 0) > 15

    upstream = Stream(name="up", cls=UpstreamA, interval=1.0, incorp_params={"inc_file": "x"})
    fjord = Fjord(
        name="fjord",
        cls=FjordCls_,
        interval=1.0,
        parent_currents=["up"],
        parent_filters={"up": above_15},
    )

    captured = _install_flush_capture(monkeypatch)
    scheduler = _make_stub_scheduler([upstream], fjord)
    from incorporator.observability.tideweaver.scheduler import Tideweaver

    await Tideweaver._tick_fjord(scheduler, fjord)

    state_rows = captured["state"]["UpstreamA"]
    assert state_rows == [row_b], f"Expected only row_b (score=20); got {state_rows}"


# ---------------------------------------------------------------------------
# Test 3 — empty filter result → state-dict entry is empty list
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_filter_result_yields_empty_state_entry(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """When the filter matches zero rows, state[cls.__name__] is [] — not skipped."""
    monkeypatch.chdir(tmp_path)
    _reset_registries(UpstreamA, FjordCls_)

    row_a = UpstreamA(inc_code=1, division=200)  # type: ignore[call-arg]
    UpstreamA._tideweaver_snapshot = [row_a]  # type: ignore[attr-defined]

    upstream = Stream(name="up", cls=UpstreamA, interval=1.0, incorp_params={"inc_file": "x"})
    fjord = Fjord(
        name="fjord",
        cls=FjordCls_,
        interval=1.0,
        parent_currents=["up"],
        parent_filters={"up": ("division", operator.eq, 999)},
    )

    captured = _install_flush_capture(monkeypatch)
    scheduler = _make_stub_scheduler([upstream], fjord)
    from incorporator.observability.tideweaver.scheduler import Tideweaver

    await Tideweaver._tick_fjord(scheduler, fjord)

    assert captured["state"]["UpstreamA"] == [], "filter matching zero rows must yield empty list, not skip"


# ---------------------------------------------------------------------------
# Test 4 — None/missing upstream snapshot → state-dict entry is empty list
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
        parent_filters={},  # no filter — pass-through
    )

    captured = _install_flush_capture(monkeypatch)
    scheduler = _make_stub_scheduler([upstream], fjord)
    from incorporator.observability.tideweaver.scheduler import Tideweaver

    await Tideweaver._tick_fjord(scheduler, fjord)

    assert captured["state"]["UpstreamA"] == [], "no snapshot + empty inc_dict must yield empty list"


# ---------------------------------------------------------------------------
# Test 5 — validator: malformed parent_filter tuple
# ---------------------------------------------------------------------------


def test_validator_rejects_malformed_parent_filter_tuple() -> None:
    """parent_filters[key] tuple with non-callable op raises ValueError.

    Note on layering: Pydantic v2's ``tuple[str, Any, Any]`` coercion catches
    wrong-length tuples FIRST (raising ``ValidationError``), so the model
    validator only sees 3-element tuples and its job is to enforce
    ``callable(filter[1])``.  This mirrors Stream's same layering.
    """
    with pytest.raises(ValueError, match=r"tuple must be \(attr: str, op: Callable, value: Any\)"):
        Fjord(
            name="fjord",
            cls=FjordCls_,
            interval=1.0,
            parent_currents=["up"],
            parent_filters={"up": ("division", "not-callable", 201)},  # type: ignore[dict-item]
        )


# ---------------------------------------------------------------------------
# Test 6 — validator: orphan filter key not in parent_currents
# ---------------------------------------------------------------------------


def test_validator_rejects_orphan_filter_key() -> None:
    """parent_filters key that does not appear in parent_currents raises."""
    with pytest.raises(ValueError, match=r"parent_filters key 'missing' is not in parent_currents"):
        Fjord(
            name="fjord",
            cls=FjordCls_,
            interval=1.0,
            parent_currents=["up"],
            parent_filters={"missing": ("division", operator.eq, 201)},
        )


# ---------------------------------------------------------------------------
# Test 7 — Watershed auto-derives hard-gate edges from each parent_currents name
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
    assert ("up_a", "fjord") in edge_pairs, f"Expected auto-derived edge 'up_a'→'fjord'; got {edge_pairs}"
    assert ("up_b", "fjord") in edge_pairs, f"Expected auto-derived edge 'up_b'→'fjord'; got {edge_pairs}"

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
    assert count == 1, f"Edge 'up_a'→'fjord2' must appear exactly once; found {count}"

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
