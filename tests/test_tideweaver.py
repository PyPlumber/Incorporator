"""Tests for the Tideweaver orchestration layer.

This file covers all three layers in the design:

* Shape constructors + validators on :class:`Watershed` (pure unit tests).
* Orchestration behaviour of :class:`Tideweaver` (async; mocked tick bodies).
* JSON-config loader (``load_watershed``) and the CLI verb.

Every test has a docstring stating the behaviour it proves.  No live network;
where a tick would touch I/O, the per-current ``_tick`` coroutine is stubbed
via ``monkeypatch.setattr``.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

from incorporator import Incorporator
from incorporator.observability.tideweaver import (
    Current,
    Edge,
    Export,
    Fjord,
    Stream,
    Tide,
    Tideweaver,
    Watershed,
)


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _A(Incorporator):
    """Stand-in source class A for graph-construction tests."""


class _B(Incorporator):
    """Stand-in source class B for graph-construction tests."""


class _C(Incorporator):
    """Stand-in source class C for graph-construction tests."""


class _D(Incorporator):
    """Stand-in source class D (e.g. derived fan-in class)."""


def _window() -> Tuple[datetime, datetime]:
    """Return a 1-hour future window so the scheduler treats it as open."""
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(hours=1))


def _stream(name: str, interval: float = 5.0, depends_on: List[str] | None = None) -> Stream:
    """Build a minimal Stream current with default no-op incorp params."""
    return Stream(
        name=name,
        cls={"a": _A, "b": _B, "c": _C, "d": _D}.get(name, _A),
        interval=interval,
        depends_on=depends_on or [],
        incorp_params={},
    )


# ---------------------------------------------------------------------------
# Shape constructors
# ---------------------------------------------------------------------------


def test_chain_edges() -> None:
    """``Watershed.chain([A, B, C])`` produces edges A→B, B→C with mode=hard."""
    a, b, c = _stream("a"), _stream("b"), _stream("c")
    ws = Watershed.chain(window=_window(), currents=[a, b, c])
    assert [(e.from_name, e.to_name, e.mode) for e in ws.edges] == [
        ("a", "b", "hard"),
        ("b", "c", "hard"),
    ]
    assert ws.toposort() == ["a", "b", "c"]


def test_diamond_edges() -> None:
    """``Watershed.diamond(head, [M1, M2], tail)`` produces 4 hard edges."""
    head, m1, m2, tail = _stream("a"), _stream("b"), _stream("c"), _stream("d")
    ws = Watershed.diamond(window=_window(), head=head, middle=[m1, m2], tail=tail)
    edges = {(e.from_name, e.to_name) for e in ws.edges}
    assert edges == {("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")}
    assert all(e.mode == "hard" for e in ws.edges)


def test_fanout_edges() -> None:
    """``Watershed.fanout(src, [S1, S2])`` produces one hard edge per sink."""
    src, s1, s2 = _stream("a"), _stream("b"), _stream("c")
    ws = Watershed.fanout(window=_window(), source=src, sinks=[s1, s2])
    assert {(e.from_name, e.to_name) for e in ws.edges} == {("a", "b"), ("a", "c")}


def test_parallel_no_edges() -> None:
    """``Watershed.parallel([A, B])`` produces no edges and no ordering."""
    a, b = _stream("a"), _stream("b")
    ws = Watershed.parallel(window=_window(), currents=[a, b])
    assert ws.edges == []


def test_parallel_rejects_dependency_mode() -> None:
    """``Watershed.parallel(dependency_mode=...)`` raises TypeError — no edges to mode."""
    a, b = _stream("a"), _stream("b")
    with pytest.raises(TypeError, match="dependency_mode"):
        Watershed.parallel(window=_window(), currents=[a, b], dependency_mode="hard")  # type: ignore[call-arg]


def test_soft_mode_edges() -> None:
    """``chain(..., dependency_mode='soft')`` produces edges with mode='soft'."""
    a, b, c = _stream("a"), _stream("b"), _stream("c")
    ws = Watershed.chain(window=_window(), currents=[a, b, c], dependency_mode="soft")
    assert all(e.mode == "soft" for e in ws.edges)


def test_custom_mixed_mode_edges() -> None:
    """The bare ``Watershed(...)`` constructor accepts edges with mixed modes."""
    a, b, c = _stream("a"), _stream("b"), _stream("c")
    ws = Watershed(
        window=_window(),
        currents=[a, b, c],
        edges=[
            Edge(from_name="a", to_name="b", mode="hard"),
            Edge(from_name="b", to_name="c", mode="soft"),
        ],
    )
    modes = {(e.from_name, e.to_name): e.mode for e in ws.edges}
    assert modes == {("a", "b"): "hard", ("b", "c"): "soft"}


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------


def test_watershed_rejects_duplicate_names() -> None:
    """Duplicate current names raise a clear ValueError listing the duplicates."""
    a, a2 = _stream("a"), _stream("a")
    with pytest.raises(ValueError, match="unique names"):
        Watershed.parallel(window=_window(), currents=[a, a2])


def test_watershed_rejects_inverted_window() -> None:
    """Watershed window end must be strictly after start."""
    now = datetime.now(timezone.utc)
    with pytest.raises(ValueError, match="window end"):
        Watershed.parallel(window=(now, now), currents=[_stream("a")])


def test_watershed_rejects_unknown_edge_endpoint() -> None:
    """Edges referencing an undefined current name raise a clear error."""
    a = _stream("a")
    with pytest.raises(ValueError, match="unknown current"):
        Watershed(
            window=_window(),
            currents=[a],
            edges=[Edge(from_name="a", to_name="ghost")],
        )


def test_watershed_rejects_cycle() -> None:
    """A cyclic edge set raises with the cyclic names listed."""
    a, b = _stream("a"), _stream("b")
    with pytest.raises(ValueError, match="cycle"):
        Watershed(
            window=_window(),
            currents=[a, b],
            edges=[
                Edge(from_name="a", to_name="b"),
                Edge(from_name="b", to_name="a"),
            ],
        )


def test_stream_rejects_stateful_polling_kwarg() -> None:
    """``Stream(stateful_polling=True)`` raises with a Fjord hint."""
    with pytest.raises(ValueError, match="Fjord"):
        Stream(
            name="bad",
            cls=_A,
            interval=5.0,
            incorp_params={},
            stateful_polling=True,  # type: ignore[call-arg]
        )


def test_current_depends_on_folded_into_edges() -> None:
    """A Current.depends_on value materialises as a hard edge on the watershed."""
    a = _stream("a")
    b = Stream(name="b", cls=_B, interval=5.0, incorp_params={}, depends_on=["a"])
    ws = Watershed(window=_window(), currents=[a, b])
    assert {(e.from_name, e.to_name, e.mode) for e in ws.edges} == {("a", "b", "hard")}


def test_subclass_variants_construct() -> None:
    """Stream / Fjord / Export and a bare Current all construct cleanly."""
    s = Stream(name="s", cls=_A, interval=5.0, incorp_params={"inc_url": "https://x"})
    f = Fjord(name="f", cls=_D, interval=10.0, export_params={"file_path": "out.ndjson"})
    e = Export(name="e", cls=_A, interval=15.0, export_params={"file_path": "out.csv"})
    bare = Current(name="x", cls=_A, interval=5.0)
    assert (s.name, f.name, e.name, bare.name) == ("s", "f", "e", "x")


def test_tide_log_meta_shape() -> None:
    """Tide.log_meta() produces the expected compact one-line meta string."""
    tide = Tide(tide_number=3, fired=["a"], skipped=[("b", "not_due")], duration_sec=0.123)
    assert "tide_number:3" in tide.log_meta()
    assert "fired:1" in tide.log_meta()
    assert "skipped:1" in tide.log_meta()


# ---------------------------------------------------------------------------
# Orchestration — uses tick_factory to inject deterministic stubs
# ---------------------------------------------------------------------------


def _short_window(seconds: float = 1.0) -> Tuple[datetime, datetime]:
    """Build a short future window for orchestration tests."""
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(seconds=seconds))


async def _collect_tides(tw: Tideweaver) -> List[Tide]:
    """Run a Tideweaver to completion and return its emitted Tide records."""
    return [t async for t in tw.run()]


@pytest.mark.asyncio
async def test_parallel_independent() -> None:
    """Parallel currents tick independently on their own intervals; no gating."""
    fires: List[Tuple[str, float]] = []

    async def fake_tick(current: Current) -> None:
        fires.append((current.name, time.monotonic()))

    a = _stream("a", interval=0.1)
    b = _stream("b", interval=0.1)
    ws = Watershed.parallel(window=_short_window(0.6), currents=[a, b])
    tw = Tideweaver(ws, tick_factory=fake_tick, pass_interval=0.05)
    await _collect_tides(tw)
    a_count = sum(1 for n, _ in fires if n == "a")
    b_count = sum(1 for n, _ in fires if n == "b")
    assert a_count >= 2 and b_count >= 2
    assert abs(a_count - b_count) <= 2  # roughly balanced; no inter-current gating


@pytest.mark.asyncio
async def test_dep_gating_hard() -> None:
    """In a hard chain A→B, B never fires before A's first wave lands."""
    fires: List[str] = []

    async def fake_tick(current: Current) -> None:
        fires.append(current.name)

    a = _stream("a", interval=0.1)
    b = _stream("b", interval=0.1)
    ws = Watershed.chain(window=_short_window(0.6), currents=[a, b])
    tw = Tideweaver(ws, tick_factory=fake_tick, pass_interval=0.05)
    await _collect_tides(tw)
    assert fires.count("a") > 0
    assert fires.count("b") > 0
    # B must not appear in the sequence before A's first occurrence.
    first_a = fires.index("a")
    first_b = fires.index("b")
    assert first_a < first_b


@pytest.mark.asyncio
async def test_soft_mode_no_gate() -> None:
    """In a soft chain A→B, B fires on its own interval without waiting for A's data."""
    fires: List[str] = []

    async def fake_tick(current: Current) -> None:
        fires.append(current.name)

    a = _stream("a", interval=0.1)
    b = _stream("b", interval=0.1)
    ws = Watershed.chain(window=_short_window(0.5), currents=[a, b], dependency_mode="soft")
    tw = Tideweaver(ws, tick_factory=fake_tick, pass_interval=0.05)
    await _collect_tides(tw)
    # Soft mode: B fires regardless of A's wave history.  Counts should be close.
    assert fires.count("a") > 0 and fires.count("b") > 0


@pytest.mark.asyncio
async def test_skip_ahead() -> None:
    """B skips with 'skip_ahead' when upstream A is still running > threshold * B.interval."""

    async def slow_a(current: Current) -> None:
        if current.name == "a":
            await asyncio.sleep(0.6)  # >> threshold * b.interval = 2 * 0.1 = 0.2s

    a = _stream("a", interval=0.1)
    b = _stream("b", interval=0.1)
    ws = Watershed.chain(window=_short_window(0.7), currents=[a, b])
    tw = Tideweaver(ws, tick_factory=slow_a, pass_interval=0.05)
    tides = await _collect_tides(tw)
    skip_reasons = [reason for tide in tides for _name, reason in tide.skipped]
    assert "skip_ahead" in skip_reasons


@pytest.mark.asyncio
async def test_graceful_drain() -> None:
    """An in-flight tick at window-end finishes inside drain_timeout."""
    completed = asyncio.Event()

    async def slow_tick(current: Current) -> None:
        try:
            await asyncio.sleep(0.4)
            completed.set()
        except asyncio.CancelledError:
            raise

    a = _stream("a", interval=0.1)
    ws = Watershed.parallel(
        window=_short_window(0.2),
        currents=[a],
        drain_timeout=1.0,
    )
    tw = Tideweaver(ws, tick_factory=slow_tick, pass_interval=0.05)
    await _collect_tides(tw)
    assert completed.is_set(), "in-flight tick should have completed during drain"


@pytest.mark.asyncio
async def test_drain_timeout_cancels_runaway_tick() -> None:
    """If a tick exceeds drain_timeout, it's cancelled."""
    started = asyncio.Event()
    cancelled = False

    async def runaway(current: Current) -> None:
        nonlocal cancelled
        started.set()
        try:
            await asyncio.sleep(5.0)
        except asyncio.CancelledError:
            cancelled = True
            raise

    a = _stream("a", interval=0.1)
    ws = Watershed.parallel(
        window=_short_window(0.2),
        currents=[a],
        drain_timeout=0.2,
    )
    tw = Tideweaver(ws, tick_factory=runaway, pass_interval=0.05)
    await _collect_tides(tw)
    assert started.is_set()
    assert cancelled is True


@pytest.mark.asyncio
async def test_isolate_on_error() -> None:
    """A failing 'isolate' current doesn't stop the rest of the graph."""
    fires: List[str] = []

    async def maybe_raise(current: Current) -> None:
        fires.append(current.name)
        if current.name == "boom":
            raise RuntimeError("kaboom")

    boom = Stream(name="boom", cls=_A, interval=0.1, incorp_params={}, on_error="isolate")
    ok = Stream(name="ok", cls=_B, interval=0.1, incorp_params={}, on_error="isolate")
    ws = Watershed.parallel(window=_short_window(0.5), currents=[boom, ok])
    tw = Tideweaver(ws, tick_factory=maybe_raise, pass_interval=0.05)
    await _collect_tides(tw)
    assert fires.count("ok") > 1, "the healthy current must keep firing after a sibling fails"


@pytest.mark.asyncio
async def test_tide_log_record_shape() -> None:
    """Each scheduler pass emits one Tide with fired/skipped/duration_sec populated."""

    async def noop(current: Current) -> None:
        return None

    a = _stream("a", interval=0.1)
    ws = Watershed.parallel(window=_short_window(0.3), currents=[a])
    tw = Tideweaver(ws, tick_factory=noop, pass_interval=0.05)
    tides = await _collect_tides(tw)
    assert tides, "at least one Tide must be emitted"
    for tide in tides:
        assert tide.duration_sec >= 0.0
        assert tide.tide_number >= 1
        assert isinstance(tide.fired, list)
        assert isinstance(tide.skipped, list)
    # tide_number must be strictly increasing.
    assert [t.tide_number for t in tides] == list(range(1, len(tides) + 1))


@pytest.mark.asyncio
async def test_fjord_flush_outflow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A Fjord current snapshots upstream registries, runs outflow(state), and exports."""
    monkeypatch.chdir(tmp_path)

    # Source class — populated directly by the fake upstream tick (no real fetch).
    class Lap(Incorporator):
        """Per-lap source used by the fjord flush test."""

    # Output base class — the Fjord current will materialise this dynamically.
    class State(Incorporator):
        """Derived state class produced by outflow(state)."""

    # Write outflow.py — returns one row per upstream Lap.
    outflow_py = tmp_path / "outflow.py"
    outflow_py.write_text(
        "def outflow(state):\n"
        "    laps = state.get('Lap', [])\n"
        "    return [{'lap_id': i, 'count': len(laps)} for i in range(len(laps))]\n",
        encoding="utf-8",
    )

    out_file = tmp_path / "state.ndjson"

    # Strong refs to defeat the WeakValueDictionary registry — the test owns
    # these so they survive across ticks.
    lap_strong_refs: List[Lap] = []

    async def fake_upstream(current: Current) -> None:
        """Populate Lap.inc_dict directly instead of calling stream()."""
        if current.name == "laps":
            inst = Lap()
            lap_strong_refs.append(inst)
            Lap.inc_dict[f"lap-{len(lap_strong_refs)}"] = inst

    a = Stream(name="laps", cls=Lap, interval=0.1, incorp_params={}, on_error="fail_watershed")
    f = Fjord(
        name="state",
        cls=State,
        interval=0.1,
        export_params={"file_path": str(out_file), "format": "ndjson", "if_exists": "append"},
        outflow=outflow_py,
        on_error="fail_watershed",
    )
    ws = Watershed.chain(window=_short_window(0.7), currents=[a, f])
    tw = Tideweaver(ws, tick_factory=fake_upstream, pass_interval=0.05)

    # The Fjord current uses the default _tick_fjord; only `laps` is faked.
    async def selective(current: Current) -> None:
        if isinstance(current, Fjord):
            await tw._tick_fjord(current)
        else:
            await fake_upstream(current)

    monkeypatch.setattr(tw, "_invoke_tick", selective)
    await _collect_tides(tw)
    assert out_file.exists(), "fjord flush should have written an export file"
    lines = [ln for ln in out_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert lines, "exported file should contain at least one row"
    # Each line is one JSON object with the expected fields.
    payload = json.loads(lines[-1])
    assert "lap_id" in payload and "count" in payload


@pytest.mark.asyncio
async def test_export_current_snapshots_upstream(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An Export current downstream of a Stream writes the upstream's rows to disk.

    Regression for the missing ``instance`` kwarg in ``_tick_export``: prior
    to the fix every Export tick raised ``TypeError`` and no file was ever
    written.  The fix resolves ``instance`` from the upstream's parked
    ``_tideweaver_snapshot`` (or ``inc_dict`` as a fallback) before forwarding
    the user's ``export_params``.
    """
    monkeypatch.chdir(tmp_path)

    class Tick(Incorporator):
        """Per-tick source row used by the Export regression test."""

    out_file = tmp_path / "ticks.ndjson"
    tick_strong_refs: List[Tick] = []

    async def fake_upstream(current: Current) -> None:
        """Populate Tick.inc_dict + park a snapshot, mimicking _tick_stream."""
        if current.name == "ticks":
            inst = Tick()
            tick_strong_refs.append(inst)
            Tick.inc_dict[f"t-{len(tick_strong_refs)}"] = inst
            Tick._tideweaver_snapshot = list(tick_strong_refs)  # type: ignore[attr-defined]

    src = Stream(name="ticks", cls=Tick, interval=0.1, incorp_params={}, on_error="fail_watershed")
    dump = Export(
        name="dump",
        cls=Tick,
        interval=0.1,
        export_params={"file_path": str(out_file), "format": "ndjson", "if_exists": "append"},
        on_error="fail_watershed",
    )
    ws = Watershed.chain(window=_short_window(0.7), currents=[src, dump])
    tw = Tideweaver(ws, pass_interval=0.05)

    async def selective(current: Current) -> None:
        if isinstance(current, Export):
            await tw._tick_export(current)
        else:
            await fake_upstream(current)

    monkeypatch.setattr(tw, "_invoke_tick", selective)
    await _collect_tides(tw)

    assert out_file.exists(), "Export tick must write the export file"
    lines = [ln for ln in out_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert lines, "Export tick must have written at least one row"


# ---------------------------------------------------------------------------
# JSON config loader
# ---------------------------------------------------------------------------


def _write_sidecar(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


def _write_outflow_with_classes(tmp_path: Path) -> Path:
    """Write an outflow.py that defines the source classes used by the JSON tests."""
    return _write_sidecar(
        tmp_path / "outflow.py",
        "from incorporator import Incorporator\n"
        "class LapData(Incorporator):\n    pass\n"
        "class PitStops(Incorporator):\n    pass\n"
        "class FlagEvents(Incorporator):\n    pass\n"
        "class DriverState(Incorporator):\n    pass\n"
        "def outflow(state):\n    return []\n",
    )


def _watershed_json_body(shape: str, *, with_mode: bool = True) -> Dict[str, Any]:
    body: Dict[str, Any] = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": shape,
        "outflow": "outflow.py",
        "drain_timeout": 5,
    }
    if with_mode and shape != "parallel":
        body["dependency_mode"] = "hard"
    return body


def test_json_chain_shape(tmp_path: Path) -> None:
    """``shape: 'chain'`` parses into a Watershed with the expected chain edges."""
    from incorporator.observability.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("chain")
    body["currents"] = [
        {"name": "laps", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "pits", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    ws = load_watershed(cfg)
    assert [(e.from_name, e.to_name) for e in ws.edges] == [("laps", "pits")]


def test_json_diamond_shape(tmp_path: Path) -> None:
    """``shape: 'diamond'`` parses head/middle/tail into the right 4-edge set."""
    from incorporator.observability.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("diamond")
    body["head"] = {"name": "laps", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}}
    body["middle"] = [
        {"name": "pits", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "flags", "class": "FlagEvents", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["tail"] = {
        "name": "state",
        "class": "DriverState",
        "verb": "fjord",
        "interval": 30,
        "export_params": {"file_path": "state.ndjson", "format": "ndjson"},
    }
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    ws = load_watershed(cfg)
    edges = {(e.from_name, e.to_name) for e in ws.edges}
    assert edges == {("laps", "pits"), ("laps", "flags"), ("pits", "state"), ("flags", "state")}


def test_json_fanout_shape(tmp_path: Path) -> None:
    """``shape: 'fanout'`` parses source/sinks into the expected fan-out edges."""
    from incorporator.observability.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("fanout")
    body["source"] = {"name": "laps", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}}
    body["sinks"] = [
        {"name": "pits", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "flags", "class": "FlagEvents", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    ws = load_watershed(cfg)
    assert {(e.from_name, e.to_name) for e in ws.edges} == {("laps", "pits"), ("laps", "flags")}


def test_json_parallel_shape(tmp_path: Path) -> None:
    """``shape: 'parallel'`` parses currents=[...] with no edges."""
    from incorporator.observability.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("parallel", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    ws = load_watershed(cfg)
    assert ws.edges == []


def test_json_custom_shape(tmp_path: Path) -> None:
    """``shape: 'custom'`` honors an explicit edges list with per-edge modes."""
    from incorporator.observability.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("custom", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "c", "class": "FlagEvents", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["edges"] = [
        {"from": "a", "to": "b", "mode": "hard"},
        {"from": "b", "to": "c", "mode": "soft"},
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    ws = load_watershed(cfg)
    modes = {(e.from_name, e.to_name): e.mode for e in ws.edges}
    assert modes == {("a", "b"): "hard", ("b", "c"): "soft"}


def test_json_env_interpolation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``${VAR}`` references in the JSON resolve from os.environ at load time."""
    from incorporator.observability.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    monkeypatch.setenv("TW_START", "2026-05-16T00:00:00+00:00")
    monkeypatch.setenv("TW_END", "2026-05-16T01:00:00+00:00")
    body = {
        "window": {"start": "${TW_START}", "end": "${TW_END}"},
        "shape": "parallel",
        "outflow": "outflow.py",
        "currents": [
            {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        ],
    }
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    ws = load_watershed(cfg)
    assert ws.window[0].isoformat() == "2026-05-16T00:00:00+00:00"


def test_json_bad_shape_raises(tmp_path: Path) -> None:
    """An unknown ``shape`` key raises a clear ``ValueError``."""
    from incorporator.observability.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("noodle", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError, match="Unknown shape"):
        load_watershed(cfg)


def test_json_unknown_class_raises(tmp_path: Path) -> None:
    """A ``class`` string that doesn't resolve raises a clear ``ValueError``."""
    from incorporator.observability.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("parallel", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "GhostClass", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError, match="GhostClass"):
        load_watershed(cfg)


# ---------------------------------------------------------------------------
# CLI verb
# ---------------------------------------------------------------------------


def test_cli_tideweaver_help_registered() -> None:
    """``incorporator tideweaver --help`` lists the ``run`` subcommand."""
    from typer.testing import CliRunner

    from incorporator.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["tideweaver", "--help"])
    assert result.exit_code == 0
    assert "run" in result.stdout


def test_cli_tideweaver_run_missing_config(tmp_path: Path) -> None:
    """``tideweaver run <missing>`` exits 1 with a clear error."""
    from typer.testing import CliRunner

    from incorporator.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["tideweaver", "run", str(tmp_path / "nope.json")])
    assert result.exit_code == 1
    assert "not found" in result.stdout.lower() or "not found" in (result.stderr or "").lower()


# ---------------------------------------------------------------------------
# Validate command coverage (autodetect + structural checks + CLI + pre-flight)
# ---------------------------------------------------------------------------


def _write_minimal_ws(tmp_path: Path, body_overrides: Dict[str, Any] | None = None) -> Path:
    """Write a valid baseline watershed.json (parallel shape, 1 stream current)."""
    _write_outflow_with_classes(tmp_path)
    body: Dict[str, Any] = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "parallel",
        "outflow": "outflow.py",
        "currents": [
            {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        ],
    }
    if body_overrides:
        body.update(body_overrides)
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    return cfg


def test_autodetect_identifies_tideweaver() -> None:
    """A config with top-level 'window' + 'shape' auto-detects as 'tideweaver'."""
    from incorporator.cli.validate import autodetect_type

    cfg = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "parallel",
        "currents": [],
    }
    assert autodetect_type(cfg) == "tideweaver"


def test_autodetect_still_picks_stream_and_fjord() -> None:
    """Backwards compat: stream and fjord configs still autodetect correctly."""
    from incorporator.cli.validate import autodetect_type

    assert autodetect_type({"incorp_params": {"inc_url": "https://x"}}) == "stream"
    assert autodetect_type({"outflow": "o.py", "stream_params": []}) == "fjord"


def test_validate_watershed_chain(tmp_path: Path) -> None:
    """A correct chain watershed validates cleanly (no errors)."""
    from incorporator.cli.validate import validate_watershed_config

    _write_outflow_with_classes(tmp_path)
    body = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "chain",
        "outflow": "outflow.py",
        "currents": [
            {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
            {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
        ],
    }
    assert validate_watershed_config(body, tmp_path) == []


def test_validate_rejects_bad_window(tmp_path: Path) -> None:
    """Inverted / non-ISO window timestamps surface clear errors."""
    from incorporator.cli.validate import validate_watershed_config

    _write_outflow_with_classes(tmp_path)
    body = {
        "window": {"start": "not-a-date", "end": "also-not"},
        "shape": "parallel",
        "outflow": "outflow.py",
        "currents": [
            {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        ],
    }
    errs = validate_watershed_config(body, tmp_path)
    assert any("window.start" in e for e in errs)
    assert any("window.end" in e for e in errs)


def test_validate_rejects_unknown_shape(tmp_path: Path) -> None:
    """An unrecognised 'shape' key produces a clear listing of valid shapes."""
    from incorporator.cli.validate import validate_watershed_config

    body = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "noodle",
    }
    errs = validate_watershed_config(body, tmp_path)
    assert any("'shape' must be one of" in e for e in errs)


def test_validate_rejects_bad_verb(tmp_path: Path) -> None:
    """Unknown 'verb' on a current is rejected with the valid set listed."""
    from incorporator.cli.validate import validate_watershed_config

    _write_outflow_with_classes(tmp_path)
    body = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "parallel",
        "outflow": "outflow.py",
        "currents": [{"name": "a", "class": "LapData", "verb": "spume", "interval": 30}],
    }
    errs = validate_watershed_config(body, tmp_path)
    assert any("verb" in e and "spume" in e for e in errs)


def test_validate_rejects_unknown_class(tmp_path: Path) -> None:
    """A class string that doesn't resolve in outflow.py is rejected."""
    from incorporator.cli.validate import validate_watershed_config

    _write_outflow_with_classes(tmp_path)
    body = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "parallel",
        "outflow": "outflow.py",
        "currents": [{"name": "a", "class": "GhostClass", "verb": "stream", "interval": 30}],
    }
    errs = validate_watershed_config(body, tmp_path)
    assert any("GhostClass" in e for e in errs)


def test_validate_rejects_bad_outflow_arity(tmp_path: Path) -> None:
    """An outflow(state) with the wrong arity is caught at validate time."""
    from incorporator.cli.validate import validate_watershed_config

    bad_outflow = tmp_path / "outflow.py"
    bad_outflow.write_text(
        "from incorporator import Incorporator\n"
        "class DriverState(Incorporator):\n    pass\n"
        "class LapData(Incorporator):\n    pass\n"
        "def outflow():\n    return []\n",  # arity 0 — bug
        encoding="utf-8",
    )
    body = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "chain",
        "outflow": "outflow.py",
        "currents": [
            {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
            {
                "name": "b",
                "class": "DriverState",
                "verb": "fjord",
                "interval": 30,
                "export_params": {"file_path": "out.ndjson"},
            },
        ],
    }
    errs = validate_watershed_config(body, tmp_path)
    assert any("outflow()" in e and "1 parameter" in e for e in errs)


def test_validate_rejects_parallel_with_dependency_mode(tmp_path: Path) -> None:
    """parallel shape refuses 'dependency_mode' — there are no edges to mode."""
    from incorporator.cli.validate import validate_watershed_config

    _write_outflow_with_classes(tmp_path)
    body = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "parallel",
        "outflow": "outflow.py",
        "dependency_mode": "hard",
        "currents": [{"name": "a", "class": "LapData", "verb": "stream", "interval": 30}],
    }
    errs = validate_watershed_config(body, tmp_path)
    assert any("dependency_mode" in e and "parallel" in e for e in errs)


def test_validate_rejects_custom_cycle(tmp_path: Path) -> None:
    """A cyclic edges list in shape='custom' is flagged."""
    from incorporator.cli.validate import validate_watershed_config

    _write_outflow_with_classes(tmp_path)
    body = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "custom",
        "outflow": "outflow.py",
        "currents": [
            {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
            {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
        ],
        "edges": [
            {"from": "a", "to": "b", "mode": "hard"},
            {"from": "b", "to": "a", "mode": "hard"},
        ],
    }
    errs = validate_watershed_config(body, tmp_path)
    assert any("cycle" in e for e in errs)


def test_validate_rejects_unknown_edge_endpoint(tmp_path: Path) -> None:
    """An edge that points at a non-existent current is flagged."""
    from incorporator.cli.validate import validate_watershed_config

    _write_outflow_with_classes(tmp_path)
    body = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "custom",
        "outflow": "outflow.py",
        "currents": [{"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}}],
        "edges": [{"from": "a", "to": "ghost"}],
    }
    errs = validate_watershed_config(body, tmp_path)
    assert any("ghost" in e for e in errs)


def test_validate_rejects_bad_interval(tmp_path: Path) -> None:
    """A non-positive interval is rejected."""
    from incorporator.cli.validate import validate_watershed_config

    _write_outflow_with_classes(tmp_path)
    body = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "parallel",
        "outflow": "outflow.py",
        "currents": [{"name": "a", "class": "LapData", "verb": "stream", "interval": 0}],
    }
    errs = validate_watershed_config(body, tmp_path)
    assert any("interval" in e for e in errs)


def test_top_level_validate_routes_to_tideweaver(tmp_path: Path) -> None:
    """`incorporator validate <ws.json>` auto-detects + accepts the watershed."""
    from typer.testing import CliRunner

    from incorporator.cli import app

    cfg = _write_minimal_ws(tmp_path)
    runner = CliRunner()
    result = runner.invoke(app, ["validate", str(cfg)])
    assert result.exit_code == 0, result.stdout
    assert "tideweaver" in result.stdout


def test_top_level_validate_emits_diagnostic_block(tmp_path: Path) -> None:
    """An invalid watershed produces the same diagnostic block style as stream/fjord."""
    from typer.testing import CliRunner

    from incorporator.cli import app

    cfg = _write_minimal_ws(tmp_path, {"shape": "noodle"})
    runner = CliRunner()
    result = runner.invoke(app, ["validate", str(cfg)])
    assert result.exit_code == 1
    assert "Config invalid" in result.stdout
    assert "shape" in result.stdout.lower()


def test_tideweaver_validate_subcommand(tmp_path: Path) -> None:
    """`incorporator tideweaver validate` works and emits the green banner."""
    from typer.testing import CliRunner

    from incorporator.cli import app

    cfg = _write_minimal_ws(tmp_path)
    runner = CliRunner()
    result = runner.invoke(app, ["tideweaver", "validate", str(cfg)])
    assert result.exit_code == 0, result.stdout
    assert "valid" in result.stdout


def test_tideweaver_run_calls_validate_first(tmp_path: Path) -> None:
    """`tideweaver run` runs the same validator before starting the scheduler."""
    from typer.testing import CliRunner

    from incorporator.cli import app

    cfg = _write_minimal_ws(tmp_path, {"shape": "noodle"})
    runner = CliRunner()
    result = runner.invoke(app, ["tideweaver", "run", str(cfg)])
    assert result.exit_code == 1
    assert "Config invalid" in result.stdout


def test_init_tideweaver_scaffold_validates(tmp_path: Path) -> None:
    """`init --type tideweaver` emits a watershed.json + outflow.py that pass validate_watershed_config.

    Mirrors the CLI's production flow: env-expand the raw JSON first, then validate.
    Confirms the generated scaffold is ready to ``incorporator tideweaver run`` after
    the user replaces the URL placeholders with their real source endpoints.
    """
    import json as _json

    from incorporator.cli.envexpand import expand_env
    from incorporator.cli.scaffold import write_scaffold
    from incorporator.cli.validate import autodetect_type, validate_watershed_config

    written = write_scaffold("tideweaver", tmp_path)
    names = {p.name for p in written}
    assert names == {"watershed.json", "outflow.py"}
    raw = _json.loads((tmp_path / "watershed.json").read_text(encoding="utf-8"))
    expanded = expand_env(raw)
    assert autodetect_type(expanded) == "tideweaver"
    errors = validate_watershed_config(expanded, tmp_path)
    assert errors == [], f"scaffold watershed.json should validate cleanly; got: {errors}"


# ---------------------------------------------------------------------------
# Real ``_tick_stream`` snapshot — no ``tick_factory`` override.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tick_stream_parks_snapshot_through_real_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_tick_stream`` parks a non-empty ``_tideweaver_snapshot`` via the production path.

    Regression for the WeakValueDictionary timing race: the chunked engine
    ``del``s its per-chunk ``dataset`` local after each yield, so a snapshot
    line that ran AFTER ``cls.stream()`` exited saw an empty ``inc_dict``.
    Driving the real ``_tick_stream`` (no ``tick_factory`` stub) against a
    mocked HTTP layer is the only way to catch a regression of that race.
    """
    import httpx
    from pydantic import ConfigDict

    from incorporator.io import fetch

    class StreamedPost(Incorporator):
        """Source class for the snapshot regression test."""

        model_config = ConfigDict(extra="allow")

    Incorporator.inc_dict.clear()
    if "_tideweaver_snapshot" in StreamedPost.__dict__:
        delattr(StreamedPost, "_tideweaver_snapshot")

    async def _mock(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        payload = [
            {"id": 1, "title": "first", "userId": 1},
            {"id": 2, "title": "second", "userId": 1},
        ]
        return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))

    monkeypatch.setattr(fetch, "execute_request", _mock)

    posts = Stream(
        name="posts",
        cls=StreamedPost,
        interval=0.2,
        on_error="isolate",
        incorp_params={"inc_url": "https://x/posts", "inc_code": "id"},
    )
    ws = Watershed.parallel(window=_short_window(0.6), currents=[posts])
    tw = Tideweaver(ws, pass_interval=0.05)
    await _collect_tides(tw)

    snapshot: List[Any] = list(getattr(StreamedPost, "_tideweaver_snapshot", []))
    assert snapshot, "real _tick_stream must park a non-empty snapshot — empty means the WeakValueDict race regressed"
    ids = {getattr(p, "id", None) for p in snapshot}
    assert ids == {1, 2}, f"snapshot must contain both mocked rows by id, got {ids}"


@pytest.mark.asyncio
async def test_real_stream_to_fjord_chain_sees_upstream_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real ``Stream → Fjord`` chain: outflow(state) sees rows from the parked snapshot.

    Companion to ``test_fjord_flush_outflow`` but DRIVES the production
    ``_tick_stream`` rather than bypassing it with a hand-built tick factory.
    If the snapshot-timing race regresses, the outflow's ``state['ChainedPost']``
    is empty and the exported NDJSON contains zero rows.
    """
    import httpx
    from pydantic import ConfigDict

    from incorporator.io import fetch

    monkeypatch.chdir(tmp_path)

    class ChainedPost(Incorporator):
        """Upstream Stream source for the real chain test."""

        model_config = ConfigDict(extra="allow")

    class ChainedState(Incorporator):
        """Downstream Fjord output class."""

    Incorporator.inc_dict.clear()
    for klass in (ChainedPost, ChainedState):
        if "_tideweaver_snapshot" in klass.__dict__:
            delattr(klass, "_tideweaver_snapshot")

    async def _mock(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        payload = [
            {"id": 11, "title": "alpha", "userId": 1},
            {"id": 12, "title": "beta", "userId": 1},
            {"id": 13, "title": "gamma", "userId": 2},
        ]
        return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))

    monkeypatch.setattr(fetch, "execute_request", _mock)

    outflow_py = tmp_path / "outflow.py"
    outflow_py.write_text(
        "def outflow(state):\n"
        "    posts = state.get('ChainedPost', [])\n"
        "    return [{'post_id': getattr(p, 'id', None), 'seen': len(posts)} for p in posts]\n",
        encoding="utf-8",
    )
    out_file = tmp_path / "chained_state.ndjson"

    # Stream's live HTTP/AsyncClient/pydantic stack adds noticeable per-tick
    # overhead.  Pick a Stream interval >> tick duration so a real gap opens
    # between Stream ticks; the Fjord's short interval + generous
    # skip_threshold ensures it fires inside that gap.
    posts = Stream(
        name="posts",
        cls=ChainedPost,
        interval=3.0,
        on_error="fail_watershed",
        incorp_params={"inc_url": "https://x/posts", "inc_code": "id"},
    )
    fjord = Fjord(
        name="state",
        cls=ChainedState,
        interval=0.2,
        skip_threshold=50.0,
        export_params={"file_path": str(out_file), "format": "ndjson", "if_exists": "append"},
        outflow=outflow_py,
        on_error="fail_watershed",
    )
    ws = Watershed.chain(window=_short_window(5.0), currents=[posts, fjord])
    tw = Tideweaver(ws, pass_interval=0.05)
    await _collect_tides(tw)

    assert out_file.exists(), "Fjord flush should have written an export file driven by the real Stream snapshot"
    rows = [json.loads(ln) for ln in out_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert rows, "Fjord outflow saw zero upstream rows — the snapshot race regressed"
    seen_ids = {r["post_id"] for r in rows}
    assert seen_ids >= {11, 12, 13}, f"outflow must observe every mocked post id, got {sorted(seen_ids)}"


@pytest.mark.asyncio
async def test_fjord_flush_parks_tideweaver_snapshot_on_output_class(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fjord flush parks ``_tideweaver_snapshot`` on its output class.

    Regression for the Middle-Fjord routing gap: ``_outflow.py:flush`` used to
    park its strong-ref under ``_fjord_snapshot``, but downstream tick bodies
    (``_tick_fjord``, ``_tick_export``) only read ``_tideweaver_snapshot`` —
    so a Fjord whose output fed another current saw an empty ``inc_dict`` and
    forced callers (and the routing tests) to file-reread the exported
    NDJSON to reconstruct the snapshot by hand.

    After the unification, Stream and Fjord outputs both carry
    ``_tideweaver_snapshot`` and downstream readers walk them uniformly.
    """
    import httpx
    from pydantic import ConfigDict

    from incorporator.io import fetch
    from incorporator.usercode import load_outflow_module

    monkeypatch.chdir(tmp_path)

    class SrcPost(Incorporator):
        """Upstream Stream source for the Fjord-output snapshot test."""

        model_config = ConfigDict(extra="allow")

    Incorporator.inc_dict.clear()
    if "_tideweaver_snapshot" in SrcPost.__dict__:
        delattr(SrcPost, "_tideweaver_snapshot")

    async def _mock(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        payload = [
            {"id": 101, "title": "x", "userId": 1},
            {"id": 102, "title": "y", "userId": 1},
        ]
        return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))

    monkeypatch.setattr(fetch, "execute_request", _mock)

    # The Fjord's output class must be defined IN the outflow module so
    # ``_outflow.py:flush`` picks it up via ``getattr(outflow_module,
    # derived_name, None)`` — otherwise flush would build a dynamic
    # subclass and park the snapshot there instead, defeating the
    # cross-module assertion.  Load via ``load_outflow_module`` so the
    # test and ``_tick_fjord`` share the SAME cached module instance
    # (and therefore the same ``DerivedState`` class object).
    outflow_py = tmp_path / "outflow_mod.py"
    outflow_py.write_text(
        "from pydantic import ConfigDict\n"
        "from incorporator import Incorporator\n"
        "\n"
        "class DerivedState(Incorporator):\n"
        "    model_config = ConfigDict(extra='allow')\n"
        "\n"
        "def outflow(state):\n"
        "    posts = state.get('SrcPost', [])\n"
        "    return [{'derived_id': getattr(p, 'id', None)} for p in posts]\n",
        encoding="utf-8",
    )
    _, outflow_module = load_outflow_module(outflow_py)
    DerivedState = outflow_module.DerivedState

    out_file = tmp_path / "derived_state.ndjson"

    posts = Stream(
        name="posts",
        cls=SrcPost,
        interval=3.0,
        on_error="fail_watershed",
        incorp_params={"inc_url": "https://x/posts", "inc_code": "id"},
    )
    fjord = Fjord(
        name="state",
        cls=DerivedState,
        interval=0.2,
        skip_threshold=50.0,
        export_params={"file_path": str(out_file), "format": "ndjson", "if_exists": "replace"},
        outflow=outflow_py,
        on_error="fail_watershed",
    )
    ws = Watershed.chain(window=_short_window(2.0), currents=[posts, fjord])
    tw = Tideweaver(ws, pass_interval=0.05)
    await _collect_tides(tw)

    # The headline assertion: the Fjord's output class carries a non-empty
    # ``_tideweaver_snapshot``.  Pre-fix this was None / AttributeError because
    # ``_outflow.py:flush`` parked the strong ref under ``_fjord_snapshot``.
    snapshot: List[Any] = list(getattr(DerivedState, "_tideweaver_snapshot", []))
    assert snapshot, (
        "Fjord output class must carry a non-empty _tideweaver_snapshot — "
        "empty means the Middle-Fjord snapshot-parking regressed and downstream "
        "currents will see an empty inc_dict"
    )
    derived_ids = {getattr(d, "derived_id", None) for d in snapshot}
    assert derived_ids == {101, 102}, (
        f"snapshot must contain both derived rows by derived_id, got {derived_ids}"
    )
