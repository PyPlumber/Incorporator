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
from incorporator.tideweaver import (
    Current,
    Edge,
    Export,
    Fjord,
    FlowControl,
    HardLock,
    SoftPass,
    Stream,
    SurgeBarrier,
    Tide,
    Tideweaver,
    Watershed,
    Weir,
    flow_from_mode,
)


def _gate_name(edge: Edge) -> str:
    """Return the short canal-mode name for an edge's gate ('hard', 'soft', 'weir')."""
    cls = type(edge.flow.gate).__name__
    return {"HardLock": "hard", "SoftPass": "soft", "Weir": "weir"}[cls]


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
    """``Watershed.chain([A, B, C])`` produces edges A→B, B→C with HardLock gates."""
    a, b, c = _stream("a"), _stream("b"), _stream("c")
    ws = Watershed.chain(window=_window(), currents=[a, b, c])
    assert [(e.from_name, e.to_name, _gate_name(e)) for e in ws.edges] == [
        ("a", "b", "hard"),
        ("b", "c", "hard"),
    ]
    assert ws.toposort() == ["a", "b", "c"]


def test_diamond_edges() -> None:
    """``Watershed.diamond(head, [M1, M2], tail)`` produces 4 hard-gated edges."""
    head, m1, m2, tail = _stream("a"), _stream("b"), _stream("c"), _stream("d")
    ws = Watershed.diamond(window=_window(), head=head, middle=[m1, m2], tail=tail)
    edges = {(e.from_name, e.to_name) for e in ws.edges}
    assert edges == {("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")}
    assert all(_gate_name(e) == "hard" for e in ws.edges)


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


def test_parallel_rejects_gate_mode() -> None:
    """``Watershed.parallel(gate_mode=...)`` raises TypeError — no edges to govern."""
    a, b = _stream("a"), _stream("b")
    with pytest.raises(TypeError, match="gate_mode"):
        Watershed.parallel(window=_window(), currents=[a, b], gate_mode="hard")  # type: ignore[call-arg]


def test_shape_constructors_accept_inflow_outflow_drain_kwargs(tmp_path: Path) -> None:
    """All four shape constructors hoist inflow/outflow/drain_timeout as explicit kwargs.

    Pre-v1.3.0 these worked via the trailing ``**kwargs`` forward —
    they functioned, but didn't surface in IDE autocompletion.  Per the
    senior-level audit's Improvement S3, they're now part of the
    declared signature.
    """
    a, b, c = _stream("a"), _stream("b"), _stream("c")
    inflow_path = tmp_path / "inflow.py"
    outflow_path = tmp_path / "outflow.py"
    inflow_path.write_text("", encoding="utf-8")
    outflow_path.write_text("def outflow(state):\n    return []\n", encoding="utf-8")

    chain_ws = Watershed.chain(
        window=_window(),
        currents=[a, b, c],
        inflow=inflow_path,
        outflow=outflow_path,
        drain_timeout=15.5,
    )
    assert chain_ws.inflow == inflow_path
    assert chain_ws.outflow == outflow_path
    assert chain_ws.drain_timeout == 15.5

    diamond_ws = Watershed.diamond(
        window=_window(),
        head=a,
        middle=[b],
        tail=c,
        inflow=inflow_path,
        outflow=outflow_path,
        drain_timeout=20.0,
    )
    assert diamond_ws.inflow == inflow_path
    assert diamond_ws.drain_timeout == 20.0

    fanout_ws = Watershed.fanout(
        window=_window(),
        source=a,
        sinks=[b, c],
        outflow=outflow_path,
        drain_timeout=5.0,
    )
    assert fanout_ws.outflow == outflow_path
    assert fanout_ws.drain_timeout == 5.0

    parallel_ws = Watershed.parallel(
        window=_window(),
        currents=[a, b],
        inflow=inflow_path,
        drain_timeout=45.0,
    )
    assert parallel_ws.inflow == inflow_path
    assert parallel_ws.drain_timeout == 45.0


def test_soft_mode_edges() -> None:
    """``chain(..., gate_mode='soft')`` produces edges with SoftPass gates."""
    a, b, c = _stream("a"), _stream("b"), _stream("c")
    ws = Watershed.chain(window=_window(), currents=[a, b, c], gate_mode="soft")
    assert all(_gate_name(e) == "soft" for e in ws.edges)


def test_custom_mixed_mode_edges() -> None:
    """The bare ``Watershed(...)`` constructor accepts edges with mixed gates."""
    a, b, c = _stream("a"), _stream("b"), _stream("c")
    ws = Watershed(
        window=_window(),
        currents=[a, b, c],
        edges=[
            Edge(from_name="a", to_name="b", flow=flow_from_mode("hard")),
            Edge(from_name="b", to_name="c", flow=flow_from_mode("soft")),
        ],
    )
    modes = {(e.from_name, e.to_name): _gate_name(e) for e in ws.edges}
    assert modes == {("a", "b"): "hard", ("b", "c"): "soft"}


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------


def test_watershed_name_field_defaults_to_none() -> None:
    """Watershed.name defaults to None when not supplied."""
    ws = Watershed.parallel(window=_window(), currents=[_stream("a")])
    assert ws.name is None


def test_watershed_name_field_round_trips() -> None:
    """Watershed.name='X' is stored and accessible; all four shape constructors accept it via **kwargs."""
    ws_chain = Watershed.chain(window=_window(), currents=[_stream("a"), _stream("b")], name="ChainRun")
    assert ws_chain.name == "ChainRun"

    ws_parallel = Watershed.parallel(window=_window(), currents=[_stream("a"), _stream("b")], name="ParRun")
    assert ws_parallel.name == "ParRun"

    ws_fanout = Watershed.fanout(window=_window(), source=_stream("a"), sinks=[_stream("b")], name="FanRun")
    assert ws_fanout.name == "FanRun"

    ws_diamond = Watershed.diamond(
        window=_window(), head=_stream("a"), middle=[_stream("b")], tail=_stream("c"), name="DiamRun"
    )
    assert ws_diamond.name == "DiamRun"


def test_watershed_extra_forbid_still_rejects_unknown_keys() -> None:
    """extra='forbid' continues to reject truly unknown keys after adding the name field."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Watershed(  # type: ignore[call-arg]
            window=_window(),
            currents=[_stream("a")],
            unknown_key="oops",
        )


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


def test_watershed_rejects_cls_name_collision() -> None:
    """Two currents bound to distinct classes sharing a ``cls.__name__`` raise ValueError.

    Regression for D5-05: the scheduler's fjord flush keys upstream state by
    ``dep.cls.__name__`` and parks ``_tideweaver_snapshot`` on the class itself —
    two currents whose classes happen to share a ``__name__`` (even if they are
    different class objects) silently overwrite each other's fjord state and
    snapshot. The Watershed validator must reject this at construction time,
    matching the sibling name-uniqueness / cycle / window validators in the
    same method (raise, not warn-only).
    """

    class _Dup(Incorporator):
        pass

    def _make_dup() -> type:
        class _Dup(Incorporator):  # noqa: F811 -- deliberately shadowing to build a distinct class object
            pass

        return _Dup

    other_dup = _make_dup()
    assert other_dup is not _Dup
    assert other_dup.__name__ == _Dup.__name__

    a = Stream(name="a", cls=_Dup, interval=5.0, incorp_params={})
    b = Stream(name="b", cls=other_dup, interval=5.0, incorp_params={})
    with pytest.raises(ValueError, match="cls.__name__ collision"):
        Watershed.parallel(window=_window(), currents=[a, b])


def test_watershed_allows_distinct_cls_names() -> None:
    """Two currents with distinct ``cls.__name__``s construct without error (no false positive)."""
    a = _stream("a")
    b = _stream("b")
    ws = Watershed.parallel(window=_window(), currents=[a, b])
    assert ws.currents == [a, b]


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
    """A Current.depends_on value materialises as a hard-gated edge on the watershed."""
    a = _stream("a")
    b = Stream(name="b", cls=_B, interval=5.0, incorp_params={}, depends_on=["a"])
    ws = Watershed(window=_window(), currents=[a, b])
    assert {(e.from_name, e.to_name, _gate_name(e)) for e in ws.edges} == {("a", "b", "hard")}


def test_subclass_variants_construct() -> None:
    """Stream / Fjord / Export and a bare Current all construct cleanly."""
    s = Stream(name="s", cls=_A, interval=5.0, incorp_params={"inc_url": "https://x"})
    f = Fjord(name="f", cls=_D, interval=10.0, export_params={"file_path": "out.ndjson"})
    e = Export(name="e", cls=_A, interval=15.0, export_params={"file_path": "out.csv"})
    bare = Current(name="x", cls=_A, interval=5.0)
    assert (s.name, f.name, e.name, bare.name) == ("s", "f", "e", "x")


def test_export_missing_destination_raises_at_construction() -> None:
    """(D8-02) ``Export`` with no ``file_path``/``sql_table`` in ``export_params`` raises ``ValueError``.

    Pre-fix, construction silently succeeded and every ``_tick_export``
    call misread the ``instance=<registry rows>`` list's repr as the
    destination path, raising ``IncorporatorFormatError`` on every tick
    instead of failing loudly at plan-build time.
    """
    with pytest.raises(ValueError, match="file_path.*sql_table|sql_table.*file_path"):
        Export(name="e", cls=_A, interval=15.0, export_params={})
    with pytest.raises(ValueError, match="file_path.*sql_table|sql_table.*file_path"):
        Export(name="e", cls=_A, interval=15.0)


def test_export_with_file_path_or_sql_table_constructs() -> None:
    """``Export`` with either ``file_path`` or ``sql_table`` in ``export_params`` constructs cleanly."""
    e1 = Export(name="e1", cls=_A, interval=15.0, export_params={"file_path": "out.csv"})
    e2 = Export(name="e2", cls=_A, interval=15.0, export_params={"sql_table": "my_table"})
    assert e1.export_params["file_path"] == "out.csv"
    assert e2.export_params["sql_table"] == "my_table"


def test_fjord_empty_export_params_still_constructs() -> None:
    """(D8-02 scope fence) ``Fjord`` with empty ``export_params`` is a supported config — must NOT raise.

    Only ``Export`` gets the destination-required validator; ``Fjord``
    flushes via ``outflow()`` and never falls into ``export()``'s
    in-state-mode path, so its empty-dict default stays legal.
    """
    f = Fjord(name="f", cls=_A, interval=10.0)
    assert f.export_params == {}


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
    ws = Watershed.chain(window=_short_window(0.5), currents=[a, b], gate_mode="soft")
    tw = Tideweaver(ws, tick_factory=fake_tick, pass_interval=0.05)
    await _collect_tides(tw)
    # Soft mode: B fires regardless of A's wave history.  Counts should be close.
    assert fires.count("a") > 0 and fires.count("b") > 0


@pytest.mark.asyncio
async def test_skip_ahead() -> None:
    """B skips with 'skip_ahead' when upstream A is still running > SurgeBarrier threshold.

    Default ``gate_mode="hard"`` attaches a default ``SurgeBarrier``
    (threshold_multiple=2.0, action='skip') — same effect as the old
    ``Current.skip_threshold`` field, now at the edge layer.
    """

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


# ---------------------------------------------------------------------------
# Phase 1: Weir gating + SurgeBarrier + FlowControl construction
# ---------------------------------------------------------------------------


def test_flow_from_mode_builds_hard_soft_weir_with_correct_defaults() -> None:
    """Mode shorthand: hard adds SurgeBarrier; soft/weir omit it."""
    hard = flow_from_mode("hard")
    soft = flow_from_mode("soft")
    weir = flow_from_mode("weir")
    assert isinstance(hard.gate, HardLock)
    assert isinstance(soft.gate, SoftPass)
    assert isinstance(weir.gate, Weir)
    assert isinstance(hard.surge_barrier, SurgeBarrier)
    assert hard.surge_barrier.threshold_multiple == 2.0
    assert hard.surge_barrier.action == "skip"
    assert soft.surge_barrier is None
    assert weir.surge_barrier is None


def test_watershed_rejects_both_gate_mode_and_flow() -> None:
    """chain/.diamond/.fanout raise ValueError when both gate_mode and flow are passed."""
    a, b = _stream("a"), _stream("b")
    with pytest.raises(ValueError, match="not both"):
        Watershed.chain(
            window=_window(),
            currents=[a, b],
            gate_mode="hard",
            flow=FlowControl(gate=Weir()),
        )


def test_edge_default_flow_is_hard_lock_plus_default_reservoir() -> None:
    """Edge() with no flow= kwarg builds a HardLock + DropOldest + Reservoir(depth=1)."""
    e = Edge(from_name="a", to_name="b")
    assert isinstance(e.flow.gate, HardLock)
    assert e.flow.surge_barrier is None  # bare Edge() doesn't inherit hard's SurgeBarrier
    assert e.flow.reservoir.depth == 1
    assert e.flow.penstock is None


@pytest.mark.asyncio
async def test_weir_fires_on_own_interval_after_first_upstream_wave() -> None:
    """In a weir chain A→B, B fires on its own cadence once A has emitted at least once.

    Distinguishes weir from hard: weir doesn't block on in-flight upstream;
    once A has emitted a wave the dependent hasn't consumed, B fires.
    """
    fires: List[str] = []

    async def fake_tick(current: Current) -> None:
        if current.name == "a":
            await asyncio.sleep(0.3)  # slow upstream
        fires.append(current.name)

    a = _stream("a", interval=0.8)  # rare
    b = _stream("b", interval=0.1)  # fast
    ws = Watershed.chain(window=_short_window(1.5), currents=[a, b], gate_mode="weir")
    tw = Tideweaver(ws, tick_factory=fake_tick, pass_interval=0.05)
    await _collect_tides(tw)
    # B should fire multiple times in the window (its own cadence) once A has emitted.
    assert fires.count("a") >= 1, "A must fire at least once to unlock B's gate"
    assert fires.count("b") >= 2, f"B must fire multiple times after A's first wave; got {fires.count('b')}"


@pytest.mark.asyncio
async def test_weir_waits_for_initial_upstream_emission() -> None:
    """Weir B does NOT fire BEFORE A's first wave — distinguishes weir from soft.

    A spawns on pass 1 (and is in ``tide.fired``) but takes 0.3s to
    complete.  During those 0.3s, B's interval (0.1s) elapses
    repeatedly — Weir requires a fresh upstream wave, so B must skip
    with ``awaiting_upstream`` until A's first wave lands.
    """
    fires: List[str] = []

    async def fake_tick(current: Current) -> None:
        if current.name == "a":
            await asyncio.sleep(0.3)
        fires.append(current.name)

    a = _stream("a", interval=0.8)
    b = _stream("b", interval=0.1)
    ws = Watershed.chain(window=_short_window(1.5), currents=[a, b], gate_mode="weir")
    tw = Tideweaver(ws, tick_factory=fake_tick, pass_interval=0.05)
    tides = await _collect_tides(tw)
    awaiting_skips = [
        reason for tide in tides for name, reason in tide.skipped if name == "b" and reason == "awaiting_upstream"
    ]
    assert awaiting_skips, "Weir B must skip with 'awaiting_upstream' while waiting for A's first wave"


@pytest.mark.asyncio
async def test_weir_no_skip_ahead_on_long_upstream() -> None:
    """In weir mode, slow upstream does NOT trigger skip-ahead — SurgeBarrier not auto-attached.

    Hard mode (test_skip_ahead above) emits 'skip_ahead' here.  Weir doesn't.
    """

    async def slow_a(current: Current) -> None:
        if current.name == "a":
            await asyncio.sleep(0.6)  # >> 2.0 * 0.1 — would trip default SurgeBarrier if attached

    a = _stream("a", interval=0.1)
    b = _stream("b", interval=0.1)
    ws = Watershed.chain(window=_short_window(0.7), currents=[a, b], gate_mode="weir")
    tw = Tideweaver(ws, tick_factory=slow_a, pass_interval=0.05)
    tides = await _collect_tides(tw)
    skip_reasons = [reason for tide in tides for _name, reason in tide.skipped]
    assert "skip_ahead" not in skip_reasons, (
        "Weir mode must NOT attach a default SurgeBarrier; skip_ahead leaked through"
    )


@pytest.mark.asyncio
async def test_surge_barrier_bypass_forces_pass_under_extreme_upstream() -> None:
    """SurgeBarrier(action='bypass') makes B fire even while A is in-flight beyond threshold."""
    fires: List[str] = []

    async def slow_a(current: Current) -> None:
        if current.name == "a":
            await asyncio.sleep(0.6)
        fires.append(current.name)

    a = _stream("a", interval=0.1)
    b = _stream("b", interval=0.1)
    bypass_flow = FlowControl(
        gate=HardLock(),
        surge_barrier=SurgeBarrier(threshold_multiple=2.0, action="bypass"),
    )
    ws = Watershed(
        window=_short_window(0.7),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=bypass_flow)],
    )
    tw = Tideweaver(ws, tick_factory=slow_a, pass_interval=0.05)
    await _collect_tides(tw)
    # Under bypass, B fires multiple times during A's long in-flight.
    assert fires.count("b") >= 2, f"bypass must let B fire during A's slow tick; got {fires.count('b')}"
    # Regression guard: bypass must NOT populate Tideweaver.rejects with
    # skip-class entries.  ``action="bypass"`` ``continue``s past both
    # the gate and the penstock blocks in ``_gate_reason``, so neither
    # the SkipAhead nor the SurgeHalted reject-append paths fire.  If a
    # future refactor moves the reject-append before the ``continue``,
    # this assertion catches it.
    bypass_class_rejects = [r for r in tw.rejects if r.error_kind in {"SkipAhead", "SurgeHalted"}]
    assert not bypass_class_rejects, f"bypass path must not populate skip-class rejects; got {bypass_class_rejects}"


@pytest.mark.asyncio
async def test_bypass_does_not_charge_burst_penstock() -> None:
    """SurgeBarrier(action='bypass') must not debit BurstPenstock.bucket_tokens.

    Bypass contract per scheduler._gate_reason: bypassed ticks ignore gate
    AND penstock. The finally block previously debited every upstream edge
    unconditionally — bypassed ticks paid the bucket.
    """
    from incorporator.tideweaver import BurstPenstock

    async def slow_a(current: Current) -> None:
        if current.name == "a":
            await asyncio.sleep(0.6)

    a = _stream("a", interval=0.1)
    b = _stream("b", interval=0.1)
    bypass_flow = FlowControl(
        gate=HardLock(),
        surge_barrier=SurgeBarrier(threshold_multiple=2.0, action="bypass"),
        penstock=BurstPenstock(rate_per_sec=10.0, burst=2),
    )
    ws = Watershed(
        window=_short_window(0.7),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=bypass_flow)],
    )
    tw = Tideweaver(ws, tick_factory=slow_a, pass_interval=0.05)
    await _collect_tides(tw)
    edge_state = tw._edge_state[("a", "b")]
    # bucket_tokens is None when penstock never gated (bypass skips consume_reason),
    # or equals burst cap if initialized but never debited by the finally block.
    assert edge_state.flow_state.bucket_tokens is None or edge_state.flow_state.bucket_tokens == 2.0, (
        f"BurstPenstock must not be debited on bypass; got bucket_tokens={edge_state.flow_state.bucket_tokens}"
    )


@pytest.mark.asyncio
async def test_bypass_does_not_log_window_penstock() -> None:
    """SurgeBarrier(action='bypass') must not append to WindowPenstock.window_log."""
    from incorporator.tideweaver import WindowPenstock

    async def slow_a(current: Current) -> None:
        if current.name == "a":
            await asyncio.sleep(0.6)

    a = _stream("a", interval=0.1)
    b = _stream("b", interval=0.1)
    bypass_flow = FlowControl(
        gate=HardLock(),
        surge_barrier=SurgeBarrier(threshold_multiple=2.0, action="bypass"),
        penstock=WindowPenstock(window_sec=10.0, cap=100),
    )
    ws = Watershed(
        window=_short_window(0.7),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=bypass_flow)],
    )
    tw = Tideweaver(ws, tick_factory=slow_a, pass_interval=0.05)
    await _collect_tides(tw)
    edge_state = tw._edge_state[("a", "b")]
    assert edge_state.flow_state.window_log == [], (
        f"WindowPenstock must not be appended to on bypass; got window_log={edge_state.flow_state.window_log}"
    )


@pytest.mark.asyncio
async def test_surge_barrier_halt_circuit_breaks_until_upstream_completes() -> None:
    """SurgeBarrier(action='halt') yields skip reason 'surge_halted' while A is overrun."""

    async def slow_a(current: Current) -> None:
        if current.name == "a":
            await asyncio.sleep(0.6)

    a = _stream("a", interval=0.1)
    b = _stream("b", interval=0.1)
    halt_flow = FlowControl(
        gate=HardLock(),
        surge_barrier=SurgeBarrier(threshold_multiple=2.0, action="halt"),
    )
    ws = Watershed(
        window=_short_window(0.7),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=halt_flow)],
    )
    tw = Tideweaver(ws, tick_factory=slow_a, pass_interval=0.05)
    tides = await _collect_tides(tw)
    skip_reasons = [reason for tide in tides for _name, reason in tide.skipped]
    assert "surge_halted" in skip_reasons, f"halt must surface 'surge_halted'; got {skip_reasons}"


# ---------------------------------------------------------------------------
# A-F-1: canal-layer skips populate Tideweaver.rejects (structured DLQ view)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_penstock_limited_populates_rejects() -> None:
    """SustainedPenstock-throttled edge surfaces ``PenstockLimited`` on Tideweaver.rejects.

    A→B with a 0.5 r/s SustainedPenstock — minimum gap 2s.  Inside the
    0.8s window, B's first attempt is permitted; every subsequent attempt
    on B's 0.05s interval gets ``penstock_limited``.  Each limited
    attempt populates Tideweaver.rejects with a structured RejectEntry.
    """
    from incorporator.tideweaver import SustainedPenstock

    async def fast_tick(current: Current) -> None:
        """Zero-work tick — lets the scheduler iterate freely."""

    a = _stream("a", interval=0.05)
    b = _stream("b", interval=0.05)
    throttled_flow = FlowControl(
        gate=HardLock(),
        penstock=SustainedPenstock(rate_per_sec=0.5),
    )
    ws = Watershed(
        window=_short_window(0.8),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=throttled_flow)],
    )
    tw = Tideweaver(ws, tick_factory=fast_tick, pass_interval=0.05)
    await _collect_tides(tw)

    pl_rejects = [r for r in tw.rejects if r.error_kind == "PenstockLimited"]
    assert pl_rejects, f"expected at least one PenstockLimited reject; got rejects={tw.rejects}"
    pl = pl_rejects[0]
    assert pl.source == "_B", f"source should be downstream class name; got {pl.source!r}"
    assert "edge a→b" in pl.message, f"message should name the edge; got {pl.message!r}"
    assert "penstock_limited" in pl.message
    assert pl.wave_index is not None


@pytest.mark.asyncio
async def test_surge_halted_populates_rejects() -> None:
    """SurgeBarrier(action='halt') surfaces ``SurgeHalted`` on Tideweaver.rejects.

    Mirror of :func:`test_surge_barrier_halt_circuit_breaks_until_upstream_completes`
    but asserting on the structured DLQ rather than the Tide telemetry.
    """

    async def slow_a(current: Current) -> None:
        if current.name == "a":
            await asyncio.sleep(0.6)

    a = _stream("a", interval=0.1)
    b = _stream("b", interval=0.1)
    halt_flow = FlowControl(
        gate=HardLock(),
        surge_barrier=SurgeBarrier(threshold_multiple=2.0, action="halt"),
    )
    ws = Watershed(
        window=_short_window(0.7),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=halt_flow)],
    )
    tw = Tideweaver(ws, tick_factory=slow_a, pass_interval=0.05)
    await _collect_tides(tw)

    sh_rejects = [r for r in tw.rejects if r.error_kind == "SurgeHalted"]
    assert sh_rejects, f"expected at least one SurgeHalted reject; got rejects={tw.rejects}"
    sh = sh_rejects[0]
    assert sh.source == "_B"
    assert "edge a→b" in sh.message
    assert "surge halted" in sh.message


@pytest.mark.asyncio
async def test_awaiting_upstream_does_not_populate_rejects() -> None:
    """``awaiting_upstream`` is a normal transient — it must NOT populate Tideweaver.rejects.

    A weir chain A→B with A taking 0.3s to complete its first tick;
    during those 0.3s, B fires ``awaiting_upstream`` skips many times
    (B's interval=0.05s, the gate-skip telemetry surfaces every
    attempt).  But those are normal pre-first-wave events and should
    NOT pollute the structured DLQ.
    """

    async def slow_a(current: Current) -> None:
        if current.name == "a":
            await asyncio.sleep(0.3)

    a = _stream("a", interval=0.8)
    b = _stream("b", interval=0.05)
    ws = Watershed.chain(window=_short_window(0.4), currents=[a, b], gate_mode="weir")
    tw = Tideweaver(ws, tick_factory=slow_a, pass_interval=0.05)
    tides = await _collect_tides(tw)

    # Test prerequisite: confirm awaiting_upstream skips were observed
    # in the telemetry stream (otherwise this test trivially passes).
    awaiting_skips = [reason for tide in tides for _name, reason in tide.skipped if reason == "awaiting_upstream"]
    assert awaiting_skips, "test prerequisite: must observe at least one 'awaiting_upstream' skip"

    # No canal-layer reject should have been recorded — awaiting_upstream
    # is a normal pre-first-wave state, not a failure mode.
    assert not tw.rejects, (
        f"'awaiting_upstream' is a normal transient and must NOT populate Tideweaver.rejects; got {tw.rejects}"
    )


@pytest.mark.asyncio
async def test_skip_ahead_populates_rejects() -> None:
    """SurgeBarrier(action='skip') surfaces ``SkipAhead`` on Tideweaver.rejects.

    Mirrors :func:`test_skip_ahead` (which asserts on the Tide telemetry)
    but asserts on the structured DLQ instead.  Default ``gate_mode="hard"``
    auto-attaches a :class:`SurgeBarrier` with ``action="skip"``; under a
    long-running upstream this fires the SkipAhead reject-append at the
    surge-skip branch of ``_gate_reason``.

    Regression guard: a refactor that drops the reject-append at the
    ``surge.action == "skip"`` branch (e.g. while reshuffling the bypass
    plumbing) would not be caught by ``test_skip_ahead`` alone — that
    test only checks the Tide stream, which is populated by the observer
    hook *before* the reject-append.
    """

    async def slow_a(current: Current) -> None:
        if current.name == "a":
            await asyncio.sleep(0.6)  # >> threshold * b.interval = 2 * 0.1 = 0.2s

    a = _stream("a", interval=0.1)
    b = _stream("b", interval=0.1)
    ws = Watershed.chain(window=_short_window(0.7), currents=[a, b])
    tw = Tideweaver(ws, tick_factory=slow_a, pass_interval=0.05)
    await _collect_tides(tw)

    sa_rejects = [r for r in tw.rejects if r.error_kind == "SkipAhead"]
    assert sa_rejects, f"expected at least one SkipAhead reject; got rejects={tw.rejects}"
    sa = sa_rejects[0]
    assert sa.source == "_B", f"source should be downstream class name; got {sa.source!r}"
    assert "edge a→b" in sa.message, f"message should name the edge; got {sa.message!r}"
    assert "skip-ahead" in sa.message
    assert sa.wave_index is not None


# ---------------------------------------------------------------------------
# QOL: JSON-ready discriminators + Edge gate_mode shorthand
# ---------------------------------------------------------------------------


def test_flowcontrol_round_trip_through_json_dict() -> None:
    """``FlowControl.model_validate({"gate": {"type": "weir"}, ...})`` deserializes via discriminated unions.

    This is the entry point the future JSON CLI loader will use.  No
    custom dispatch layer needed — Pydantic picks the strategy
    subclass off the ``type`` Literal on each child dict.
    """
    from incorporator.tideweaver import (
        BackpressurePenstock,
        BurstPenstock,
        ExportToArchive,
        FlowControl,
        HardLock,
        RaiseOverflow,
        SoftPass,
        SustainedPenstock,
        Weir,
        WindowPenstock,
    )

    payloads_and_expected = [
        ({"gate": {"type": "hard"}}, HardLock),
        ({"gate": {"type": "soft"}}, SoftPass),
        ({"gate": {"type": "weir"}}, Weir),
    ]
    for payload, expected_cls in payloads_and_expected:
        fc = FlowControl.model_validate(payload)
        assert isinstance(fc.gate, expected_cls), f"gate dispatch failed: {payload}"

    penstock_cases = [
        ({"type": "sustained", "rate_per_sec": 5.0}, SustainedPenstock),
        ({"type": "burst", "rate_per_sec": 2.0, "burst": 5}, BurstPenstock),
        ({"type": "window", "window_sec": 60.0, "cap": 10}, WindowPenstock),
        ({"type": "backpressure", "min_rate": 1.0, "max_rate": 10.0}, BackpressurePenstock),
    ]
    for penstock_payload, expected_cls in penstock_cases:
        fc = FlowControl.model_validate({"gate": {"type": "weir"}, "penstock": penstock_payload})
        assert isinstance(fc.penstock, expected_cls), f"penstock dispatch failed: {penstock_payload}"

    # Spillways including ExportToArchive (which carries a class reference —
    # not normally JSON-friendly, but the discriminator still works when
    # constructed from a dict at the Python layer with the class already
    # resolved).
    class _Archive(Incorporator):
        """Archive class for the JSON round-trip test."""

    fc = FlowControl.model_validate({"spillway": {"type": "raise_overflow"}})
    assert isinstance(fc.spillway, RaiseOverflow)
    fc = FlowControl.model_validate({"spillway": {"type": "export_to_archive", "archive_cls": _Archive}})
    assert isinstance(fc.spillway, ExportToArchive)


def test_edge_gate_mode_shorthand_matches_flow_from_mode() -> None:
    """``Edge(..., gate_mode="weir")`` produces the same flow as ``Edge(..., flow=flow_from_mode("weir"))``."""
    from incorporator.tideweaver import Edge, flow_from_mode

    a = Edge(from_name="x", to_name="y", gate_mode="weir")
    b = Edge(from_name="x", to_name="y", flow=flow_from_mode("weir"))
    assert _gate_name(a) == _gate_name(b) == "weir"
    # Both flows have the same shape — gate, penstock, reservoir, spillway, surge_barrier.
    assert a.flow.gate.type == b.flow.gate.type
    assert a.flow.reservoir.depth == b.flow.reservoir.depth
    assert a.flow.surge_barrier == b.flow.surge_barrier


def test_edge_rejects_both_gate_mode_and_flow() -> None:
    """``Edge(gate_mode=..., flow=...)`` raises — shorthand and full are mutually exclusive."""
    from incorporator.tideweaver import Edge, FlowControl, Weir

    with pytest.raises(ValueError, match="not both"):
        Edge(from_name="x", to_name="y", gate_mode="weir", flow=FlowControl(gate=Weir()))


def test_edge_json_aliases_from_to() -> None:
    """``Edge.model_validate({"from": "a", "to": "b"})`` works — JSON-style aliases."""
    from incorporator.tideweaver import Edge

    e = Edge.model_validate({"from": "a", "to": "b", "gate_mode": "soft"})
    assert e.from_name == "a"
    assert e.to_name == "b"
    assert _gate_name(e) == "soft"
    # Python kwargs (canonical field names) still work too.
    e2 = Edge(from_name="a", to_name="b", gate_mode="hard")
    assert e2.from_name == "a"
    assert _gate_name(e2) == "hard"


# ---------------------------------------------------------------------------
# Phase 2: Reservoir (per-edge wave buffer)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reservoir_default_depth_one_holds_one_wave() -> None:
    """Default ``Reservoir(depth=1)`` keeps only the most recent wave per edge."""
    from incorporator.tideweaver import Reservoir

    waves_seen: List[List[Any]] = []
    strong: List[_A] = []

    async def fake(current: Current) -> None:
        # Populate inc_dict via strong refs so the reservoir captures non-empty waves.
        inst = _A(inc_code=f"{current.name}-{len(strong)}")
        strong.append(inst)

    a = _stream("a", interval=0.1)
    b = _stream("b", interval=0.1)
    ws = Watershed.chain(window=_short_window(0.6), currents=[a, b])
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.05)
    await _collect_tides(tw)
    edge_state = tw._edge_state[("a", "b")]
    waves_seen.append(edge_state.waves)
    assert len(edge_state.waves) <= 1, (
        f"depth=1 reservoir must hold at most one wave; got {len(edge_state.waves)} waves"
    )


@pytest.mark.asyncio
async def test_reservoir_depth_3_holds_last_three_waves() -> None:
    """A reservoir with depth=3 holds the last 3 waves; older are displaced."""
    from incorporator.tideweaver import HardLock, Reservoir

    tick_count = {"a": 0}
    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            tick_count["a"] += 1
            inst = _A(inc_code=f"a-{tick_count['a']}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", interval=0.05)
    b = _stream("b", interval=10.0)  # never fires; just hosts the inbound edge
    deep_flow = FlowControl(gate=HardLock(), reservoir=Reservoir(depth=3))
    ws = Watershed(
        window=_short_window(0.4),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=deep_flow)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    await _collect_tides(tw)

    edge_state = tw._edge_state[("a", "b")]
    assert len(edge_state.waves) == 3, (
        f"depth=3 reservoir must hold 3 waves after many upstream ticks; got {len(edge_state.waves)}"
    )
    # The displaced overflow count = total a-ticks - 3.
    assert edge_state.overflow_count == max(0, tick_count["a"] - 3), (
        f"overflow_count must reflect displaced waves; got {edge_state.overflow_count} "
        f"vs (ticks={tick_count['a']} - depth=3)"
    )
    # Clean up class-level snapshot to avoid leaking into other tests.
    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")


@pytest.mark.asyncio
async def test_reservoir_per_edge_isolation() -> None:
    """Two outgoing edges from the same upstream have independent reservoirs."""
    from incorporator.tideweaver import HardLock, Reservoir

    class _SinkA(Incorporator):
        """Distinct sink class so this current's cls.__name__ doesn't collide with _A / _SinkB."""

    class _SinkB(Incorporator):
        """Distinct sink class so this current's cls.__name__ doesn't collide with _A / _SinkA."""

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "src":
            inst = _A(inc_code=f"src-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    src = _stream("src", interval=0.05)
    sink_a = Stream(name="sink_a", cls=_SinkA, interval=10.0, incorp_params={})
    sink_b = Stream(name="sink_b", cls=_SinkB, interval=10.0, incorp_params={})
    flow_depth_2 = FlowControl(gate=HardLock(), reservoir=Reservoir(depth=2))
    flow_depth_5 = FlowControl(gate=HardLock(), reservoir=Reservoir(depth=5))
    ws = Watershed(
        window=_short_window(0.3),
        currents=[src, sink_a, sink_b],
        edges=[
            Edge(from_name="src", to_name="sink_a", flow=flow_depth_2),
            Edge(from_name="src", to_name="sink_b", flow=flow_depth_5),
        ],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    await _collect_tides(tw)

    state_a = tw._edge_state[("src", "sink_a")]
    state_b = tw._edge_state[("src", "sink_b")]
    assert len(state_a.waves) <= 2, "sink_a reservoir bounded at depth=2"
    assert len(state_b.waves) <= 5, "sink_b reservoir bounded at depth=5"
    # Independent state: sink_b holds at least as many waves as sink_a.
    assert len(state_b.waves) >= len(state_a.waves), "deeper reservoir should hold >= waves than shallower one"
    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")


# ---------------------------------------------------------------------------
# Penstock hierarchy: 5 strategies (SustainedPenstock, BurstPenstock,
# WindowPenstock, BackpressurePenstock, SignalPenstock)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sustained_penstock_limits_consumption_rate() -> None:
    """``SustainedPenstock(rate_per_sec=5.0)`` skips downstream ticks that would exceed the rate.

    Min gap = 1/5 = 0.2s.  With dependent.interval=0.05s, most passes hit
    the penstock between consumptions and surface ``"penstock_limited"``.
    """
    from incorporator.tideweaver import HardLock, SustainedPenstock

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", interval=0.05)
    b = _stream("b", interval=0.05)
    capped_flow = FlowControl(gate=HardLock(), penstock=SustainedPenstock(rate_per_sec=5.0))
    ws = Watershed(
        window=_short_window(0.6),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=capped_flow)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    tides = await _collect_tides(tw)
    reasons = [reason for tide in tides for _name, reason in tide.skipped]
    assert "penstock_limited" in reasons, f"Penstock must surface 'penstock_limited' under tight rate; got {reasons}"
    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")


@pytest.mark.asyncio
async def test_penstock_none_means_unlimited() -> None:
    """Without a Penstock strategy, the dependent fires every interval (no rate cap)."""
    fires: List[str] = []
    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]
        fires.append(current.name)

    a = _stream("a", interval=0.05)
    b = _stream("b", interval=0.05)
    # Default flow — no penstock.
    ws = Watershed.chain(window=_short_window(0.4), currents=[a, b])
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    tides = await _collect_tides(tw)
    reasons = [reason for tide in tides for _name, reason in tide.skipped]
    assert "penstock_limited" not in reasons, f"Default (no Penstock) must NOT emit 'penstock_limited'; got {reasons}"
    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")


def test_null_penstock_validates_in_flowcontrol() -> None:
    """``FlowControl(penstock=NullPenstock())`` constructs (D8-01 narrowing-regression fix).

    ``NullPenstock`` was importable and advertised in ``flow.py``'s
    ``__all__`` but excluded from ``_PenstockUnion``, so this raised
    ``ValidationError`` before the fix.
    """
    from incorporator.io.penstock import NullPenstock

    flow = FlowControl(gate=HardLock(), penstock=NullPenstock())
    assert isinstance(flow.penstock, NullPenstock)


@pytest.mark.asyncio
async def test_null_penstock_tick_path_matches_penstock_none_control() -> None:
    """(D8-01 L4 rider) ``NullPenstock`` is inert through the full tick path, not just validation.

    Runs two otherwise-identical scheduler passes — one edge with no
    penstock, one with ``FlowControl(penstock=NullPenstock())`` — and
    asserts identical observable outcomes: equal fire counts, zero
    ``penstock_limited`` skips, zero ``PenstockLimited`` canal rejects,
    and the ``FlowState`` fields ``NullPenstock`` never touches
    (``bucket_tokens``, ``window_log``) stay at their defaults.
    """
    from incorporator.io.penstock import NullPenstock

    async def _run(penstock: Any) -> Tuple[List[str], Tideweaver]:
        fires: List[str] = []
        strong_refs: List[_A] = []

        async def fake(current: Current) -> None:
            if current.name == "a":
                inst = _A(inc_code=f"a-{len(strong_refs)}")
                strong_refs.append(inst)
                _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]
            fires.append(current.name)

        a = _stream("a", interval=0.05)
        b = _stream("b", interval=0.05)
        edge_flow = FlowControl(gate=HardLock(), penstock=penstock)
        ws = Watershed(
            window=_short_window(0.4),
            currents=[a, b],
            edges=[Edge(from_name="a", to_name="b", flow=edge_flow)],
        )
        tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
        await _collect_tides(tw)
        if "_tideweaver_snapshot" in _A.__dict__:
            delattr(_A, "_tideweaver_snapshot")
        return fires, tw

    control_fires, control_tw = await _run(None)
    null_fires, null_tw = await _run(NullPenstock())

    # Real-clock windowed scheduler: tolerate +/-1 wall-clock jitter between the
    # two sequential runs rather than asserting exact equality.
    assert abs(null_fires.count("b") - control_fires.count("b")) <= 1, (
        f"NullPenstock must fire 'b' within jitter tolerance of no-penstock control; "
        f"got null={null_fires.count('b')}, control={control_fires.count('b')}"
    )
    control_rejects = [r for r in control_tw.rejects if r.error_kind == "PenstockLimited"]
    null_rejects = [r for r in null_tw.rejects if r.error_kind == "PenstockLimited"]
    assert control_rejects == [] and null_rejects == [], (
        f"neither config should surface PenstockLimited rejects; control={control_rejects}, null={null_rejects}"
    )
    null_flow_state = null_tw._edge_state[("a", "b")].flow_state
    assert null_flow_state.bucket_tokens is None, "NullPenstock must never touch bucket_tokens"
    assert null_flow_state.window_log == [], "NullPenstock must never touch window_log"
    assert null_flow_state.last_consumed_at is not None, (
        "last_consumed_at is set by the scheduler on every successful consumption, independent of penstock type"
    )


@pytest.mark.asyncio
async def test_null_penstock_failure_gate_leaves_flow_state_untouched() -> None:
    """(D8-01 L4 rider) A failed tick with ``NullPenstock`` on the edge debits/mutates nothing.

    Reuses the existing ``_tick_raised``-gated contract (scheduler.py):
    ``post_consume`` and ``last_consumed_at`` updates are skipped
    entirely when the current's tick raises, regardless of penstock type.
    """
    from incorporator.io.penstock import NullPenstock

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code="a-1")
            _A._tideweaver_snapshot = [inst]  # type: ignore[attr-defined]
        elif current.name == "b":
            raise RuntimeError("boom")

    a = _stream("a", interval=0.05)
    b = Stream(name="b", cls=_B, interval=0.05, incorp_params={}, on_error="isolate")
    edge_flow = FlowControl(gate=HardLock(), penstock=NullPenstock())
    ws = Watershed(
        window=_short_window(0.3),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=edge_flow)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    await _collect_tides(tw)
    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")

    edge_state = tw._edge_state[("a", "b")]
    assert edge_state.flow_state.last_consumed_at is None, (
        "a permanently-failing dependent must never record a successful consumption"
    )
    assert edge_state.flow_state.bucket_tokens is None
    assert edge_state.flow_state.window_log == []


@pytest.mark.asyncio
async def test_sustained_penstock_per_edge_independent() -> None:
    """Two edges from the same upstream with different SustainedPenstocks operate independently."""
    from incorporator.tideweaver import HardLock, SustainedPenstock

    class _FastSink(Incorporator):
        """Distinct sink class so this current's cls.__name__ doesn't collide with _A / _SlowSink."""

    class _SlowSink(Incorporator):
        """Distinct sink class so this current's cls.__name__ doesn't collide with _A / _FastSink."""

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "src":
            inst = _A(inc_code=f"src-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    src = _stream("src", interval=0.05)
    fast_sink = Stream(name="fast_sink", cls=_FastSink, interval=0.05, incorp_params={})
    slow_sink = Stream(name="slow_sink", cls=_SlowSink, interval=0.05, incorp_params={})
    fast_flow = FlowControl(
        gate=HardLock(), penstock=SustainedPenstock(rate_per_sec=50.0)
    )  # cap >> tick interval; basically uncapped
    slow_flow = FlowControl(gate=HardLock(), penstock=SustainedPenstock(rate_per_sec=2.0))  # min_gap 0.5s
    ws = Watershed(
        window=_short_window(0.5),
        currents=[src, fast_sink, slow_sink],
        edges=[
            Edge(from_name="src", to_name="fast_sink", flow=fast_flow),
            Edge(from_name="src", to_name="slow_sink", flow=slow_flow),
        ],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    tides = await _collect_tides(tw)
    slow_skips = sum(
        1 for tide in tides for name, reason in tide.skipped if name == "slow_sink" and reason == "penstock_limited"
    )
    fast_skips = sum(
        1 for tide in tides for name, reason in tide.skipped if name == "fast_sink" and reason == "penstock_limited"
    )
    assert slow_skips > fast_skips, (
        f"slow_sink (cap 2/s) must skip more often than fast_sink (cap 50/s); got slow={slow_skips}, fast={fast_skips}"
    )
    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")


@pytest.mark.asyncio
async def test_burst_penstock_allows_initial_burst_then_throttles() -> None:
    """``BurstPenstock(rate_per_sec=2.0, burst=5)`` lets first 5 ticks pass fast, then throttles."""
    from incorporator.tideweaver import BurstPenstock, HardLock

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", interval=0.02)
    b = _stream("b", interval=0.02)
    # rate=2/s, burst=5 — first 5 should pass quickly, then we should hit
    # penstock_limited as the bucket drains (refill 2/sec).
    burst_flow = FlowControl(gate=HardLock(), penstock=BurstPenstock(rate_per_sec=2.0, burst=5))
    ws = Watershed(
        window=_short_window(0.5),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=burst_flow)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    tides = await _collect_tides(tw)
    skip_reasons = [reason for tide in tides for _name, reason in tide.skipped if _name == "b"]
    b_fires = sum(1 for tide in tides if "b" in tide.fired)
    # The burst lets at least the first 5 through; the bucket then drains
    # and we see penstock_limited skips.
    assert b_fires >= 5, f"BurstPenstock(burst=5) must allow >= 5 initial ticks; got {b_fires}"
    assert "penstock_limited" in skip_reasons, f"BurstPenstock should throttle after the burst; got {skip_reasons}"
    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")


@pytest.mark.asyncio
async def test_burst_penstock_refills_capped_at_burst() -> None:
    """Token bucket refills at ``rate_per_sec`` but never exceeds ``burst`` capacity.

    Manually advance the bucket state to verify the cap.
    """
    from incorporator.tideweaver import BurstPenstock

    pen = BurstPenstock(rate_per_sec=2.0, burst=3)

    class _MockEdge:
        bucket_tokens: Optional[float] = None
        bucket_last_refill_at: Optional[float] = None
        last_consumed_at: Optional[float] = None
        waves: List[Any] = []
        window_log: List[float] = []

    es = _MockEdge()
    # Initial — bucket fills to burst=3.
    assert pen.consume_reason(es, FlowControl(penstock=pen), 0.0) is None  # type: ignore[arg-type]
    assert es.bucket_tokens == 3.0
    # Idle for 100 seconds — at rate=2/s that's +200 tokens, capped at 3.
    assert pen.consume_reason(es, FlowControl(penstock=pen), 100.0) is None  # type: ignore[arg-type]
    assert es.bucket_tokens == 3.0, f"Bucket must cap at burst=3; got {es.bucket_tokens}"


@pytest.mark.asyncio
async def test_window_penstock_caps_at_window_size() -> None:
    """``WindowPenstock(window_sec=0.4, cap=3)`` allows 3 ticks within window, hard-walls 4th."""
    from incorporator.tideweaver import HardLock, WindowPenstock

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", interval=0.02)
    b = _stream("b", interval=0.02)
    win_flow = FlowControl(gate=HardLock(), penstock=WindowPenstock(window_sec=0.4, cap=3))
    ws = Watershed(
        window=_short_window(0.35),  # shorter than window_sec so the cap actually bites
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=win_flow)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    tides = await _collect_tides(tw)
    skip_reasons = [reason for tide in tides for name, reason in tide.skipped if name == "b"]
    b_fires = sum(1 for tide in tides if "b" in tide.fired)
    assert b_fires <= 3, f"WindowPenstock(cap=3) must allow at most 3 fires within the window; got {b_fires}"
    if b_fires == 3:
        assert "penstock_limited" in skip_reasons, (
            "After cap is reached, WindowPenstock must skip with 'penstock_limited'"
        )
    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")


@pytest.mark.asyncio
async def test_window_penstock_eviction_across_multiple_passes() -> None:
    """``WindowPenstock`` window slides — old entries evict and downstream fires again.

    The companion ``test_window_penstock_caps_at_window_size`` runs shorter
    than ``window_sec`` so the window never evicts — it only verifies the
    initial-cap behavior.  This test runs ``4x window_sec`` and asserts that
    downstream fires more than ``cap`` times total, which is only possible
    once early consumptions slide out of the window.
    """
    from incorporator.tideweaver import HardLock, WindowPenstock

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", interval=0.02)
    b = _stream("b", interval=0.02)
    win_flow = FlowControl(gate=HardLock(), penstock=WindowPenstock(window_sec=0.15, cap=2))
    ws = Watershed(
        window=_short_window(0.6),  # 4x window_sec — plenty of room for the window to slide
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=win_flow)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    tides = await _collect_tides(tw)
    b_total_fires = sum(1 for tide in tides if "b" in tide.fired)
    # If the window never evicted, b would be hard-capped at 2 fires total.
    # Sliding eviction across ~4 windows admits substantially more.
    assert b_total_fires > 2, (
        f"WindowPenstock(window_sec=0.15, cap=2) must allow b > 2 fires over 0.6s "
        f"once the window starts sliding (eviction frees capacity); got {b_total_fires}"
    )
    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")


@pytest.mark.asyncio
async def test_backpressure_penstock_slows_under_full_reservoir() -> None:
    """``BackpressurePenstock`` rate drops toward ``min_rate`` as the reservoir fills.

    Unit-test the rate calculation directly — interpolation between
    min_rate and max_rate based on fullness ratio.
    """
    from incorporator.tideweaver import BackpressurePenstock, Reservoir

    pen = BackpressurePenstock(min_rate=1.0, max_rate=10.0)

    class _Es:
        waves: List[Any]
        last_consumed_at: Optional[float] = None
        bucket_tokens: Optional[float] = None
        bucket_last_refill_at: Optional[float] = None
        window_log: List[float] = []

        def __init__(self, wave_count: int) -> None:
            self.waves = [None] * wave_count

    flow = FlowControl(penstock=pen, reservoir=Reservoir(depth=5))
    # Empty reservoir (0/5 fullness=0): effective_rate = 10 (max).  min_gap = 0.1s.
    # last_consumed_at None → no rate-limit yet (initial call).
    es_empty = _Es(0)
    assert pen.consume_reason(es_empty, flow, 1.0) is None  # type: ignore[arg-type]
    # Same edge, simulate consumption 0.05s ago: rate 10/s, gap < 0.1 → limited.
    es_empty.last_consumed_at = 0.95
    result_empty = pen.consume_reason(es_empty, flow, 1.0)  # type: ignore[arg-type]
    assert result_empty is not None and result_empty[0] == "penstock_limited"
    # Full reservoir (5/5 fullness=1.0): effective_rate = 1.  min_gap = 1.0s.
    es_full = _Es(5)
    es_full.last_consumed_at = 0.5  # 0.5s ago, rate=1 → min_gap=1.0 → limited.
    result_full = pen.consume_reason(es_full, flow, 1.0)  # type: ignore[arg-type]
    assert result_full is not None and result_full[0] == "penstock_limited"
    # After 1.0s+ idle on full reservoir, allowed again.
    es_full.last_consumed_at = -0.1
    assert pen.consume_reason(es_full, flow, 1.0) is None  # type: ignore[arg-type]


def test_backpressure_penstock_rejects_inverted_rates() -> None:
    """``BackpressurePenstock`` must reject ``min_rate >= max_rate`` at construction.

    The interpolation formula assumes ``min_rate < max_rate``; swapped values
    silently invert the curve (full reservoir gets a *higher* rate than
    empty).  The model_validator catches this at instantiation time.
    """
    from incorporator.tideweaver import BackpressurePenstock

    # Inverted — should raise.
    with pytest.raises(ValueError, match="min_rate < max_rate"):
        BackpressurePenstock(min_rate=10.0, max_rate=2.0)
    # Equal — degenerate (constant rate, no backpressure curve) — also rejected.
    with pytest.raises(ValueError, match="min_rate < max_rate"):
        BackpressurePenstock(min_rate=5.0, max_rate=5.0)
    # Correct ordering — accepted.
    pen = BackpressurePenstock(min_rate=1.0, max_rate=10.0)
    assert pen.min_rate == 1.0
    assert pen.max_rate == 10.0


@pytest.mark.asyncio
async def test_signal_penstock_callable_drives_rate() -> None:
    """``SignalPenstock`` rate_fn return value gates the dependent.

    Returning 0 always blocks; returning a high rate allows steady firing.
    """
    from incorporator.tideweaver import SignalPenstock

    invocations: List[float] = []

    def rate_fn(edge_state: Any, now: float) -> float:
        invocations.append(now)
        # Block until now > 1.0; then allow at 100/s (effectively uncapped).
        return 0.0 if now < 1.0 else 100.0

    pen = SignalPenstock(rate_fn=rate_fn)
    flow = FlowControl(penstock=pen)

    class _Es:
        waves: List[Any] = []
        last_consumed_at: Optional[float] = None
        bucket_tokens: Optional[float] = None
        bucket_last_refill_at: Optional[float] = None
        window_log: List[float] = []

    es = _Es()
    # rate_fn returns 0 → must block.
    result_blocked = pen.consume_reason(es, flow, 0.5)  # type: ignore[arg-type]
    assert result_blocked is not None and result_blocked[0] == "penstock_limited"
    # rate_fn returns 100 → allowed at clear rate; last_consumed_at None, so no gap to check.
    assert pen.consume_reason(es, flow, 1.5) is None  # type: ignore[arg-type]
    assert len(invocations) == 2, f"rate_fn must be called once per consume_reason; got {len(invocations)}"


# ---------------------------------------------------------------------------
# Phase 4: Spillway strategies (overflow handling)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spillway_drop_oldest_is_silent_default() -> None:
    """``DropOldest`` (the default) silently discards displaced waves — no logs, no archive."""
    from incorporator.tideweaver import DropOldest, HardLock, Reservoir

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", interval=0.05)
    b = _stream("b", interval=10.0)
    flow = FlowControl(gate=HardLock(), reservoir=Reservoir(depth=2), spillway=DropOldest())
    ws = Watershed(
        window=_short_window(0.8),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=flow)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    await _collect_tides(tw)
    state = tw._edge_state[("a", "b")]
    # Even though many waves were displaced, DropOldest leaves no trace
    # beyond the overflow_count counter and the reservoir's bounded size.
    assert state.overflow_count > 0, "depth=2 + many ticks should produce overflow"
    assert len(state.waves) == 2
    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")


@pytest.mark.asyncio
async def test_spillway_raise_overflow_logs_each_overflow(caplog: pytest.LogCaptureFixture) -> None:
    """``RaiseOverflow`` emits a WARNING log line per displaced wave."""
    import logging

    from incorporator.tideweaver import HardLock, RaiseOverflow, Reservoir

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", interval=0.05)
    b = _stream("b", interval=10.0)
    flow = FlowControl(gate=HardLock(), reservoir=Reservoir(depth=1), spillway=RaiseOverflow())
    ws = Watershed(
        window=_short_window(0.8),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=flow)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    with caplog.at_level(logging.WARNING, logger="incorporator.tideweaver.flow"):
        await _collect_tides(tw)
    overflow_logs = [r for r in caplog.records if "spillway overflow" in r.message]
    assert overflow_logs, "RaiseOverflow must emit warning logs on overflow"
    state = tw._edge_state[("a", "b")]
    assert len(overflow_logs) == state.overflow_count, (
        f"one log per overflow; got {len(overflow_logs)} logs vs {state.overflow_count} overflows"
    )
    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")


@pytest.mark.asyncio
async def test_spillway_export_to_archive_routes_displaced_waves() -> None:
    """``ExportToArchive`` appends each displaced wave's instances to ``archive_cls._spillway_backlog``."""
    from incorporator.tideweaver import ExportToArchive, HardLock, Reservoir

    class _Archive(Incorporator):
        """Backlog destination for displaced waves."""

    if "_spillway_backlog" in _Archive.__dict__:
        delattr(_Archive, "_spillway_backlog")

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", interval=0.05)
    b = _stream("b", interval=10.0)
    flow = FlowControl(
        gate=HardLock(),
        reservoir=Reservoir(depth=1),
        spillway=ExportToArchive(archive_cls=_Archive),
    )
    ws = Watershed(
        window=_short_window(0.8),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=flow)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    await _collect_tides(tw)
    state = tw._edge_state[("a", "b")]
    backlog: List[Any] = getattr(_Archive, "_spillway_backlog", [])
    assert backlog, "ExportToArchive must populate the archive backlog when overflow occurs"
    # The backlog accumulates instances across overflow events; size grows
    # at least as fast as the overflow count.
    assert len(backlog) >= state.overflow_count, (
        f"backlog must hold at least one instance per displaced wave; "
        f"got {len(backlog)} instances vs {state.overflow_count} overflows"
    )
    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")
    if "_spillway_backlog" in _Archive.__dict__:
        delattr(_Archive, "_spillway_backlog")


@pytest.mark.asyncio
async def test_export_to_archive_backlog_is_strong_ref() -> None:
    """``_spillway_backlog`` entries survive ``gc.collect()`` after all other refs drop.

    The companion ``test_spillway_export_to_archive_routes_displaced_waves``
    confirms the backlog gets populated.  This pins that the entries are
    held by strong refs — not weak-collected once the originating
    ``_tideweaver_snapshot`` and the user's strong-ref list go away.
    """
    import gc

    from incorporator.tideweaver import ExportToArchive, HardLock, Reservoir

    class _Archive(Incorporator):
        """Backlog destination for displaced waves."""

    if "_spillway_backlog" in _Archive.__dict__:
        delattr(_Archive, "_spillway_backlog")

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", interval=0.05)
    b = _stream("b", interval=10.0)  # never fires; ensures overflow on every a-tick after the first
    flow = FlowControl(
        gate=HardLock(),
        reservoir=Reservoir(depth=1),
        spillway=ExportToArchive(archive_cls=_Archive),
    )
    ws = Watershed(
        window=_short_window(0.3),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=flow)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    await _collect_tides(tw)

    initial_count = len(getattr(_Archive, "_spillway_backlog", []))
    assert initial_count > 0, "spillway must have populated the backlog"

    # Drop every external strong ref to the archived instances, force GC.
    strong_refs.clear()
    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")
    gc.collect()

    surviving = getattr(_Archive, "_spillway_backlog", [])
    assert len(surviving) == initial_count, (
        f"_spillway_backlog must hold strong refs; expected {initial_count} entries "
        f"after gc.collect(), got {len(surviving)}"
    )
    for entry in surviving:
        assert entry is not None, "backlog entry must survive GC"

    if "_spillway_backlog" in _Archive.__dict__:
        delattr(_Archive, "_spillway_backlog")


@pytest.mark.asyncio
async def test_spillway_fires_when_penstock_and_reservoir_both_active() -> None:
    """Penstock + Reservoir + Spillway all engage in one composed edge.

    Scenario: fast upstream (interval=0.05s) feeds a small Reservoir(depth=3)
    on an edge throttled by a slow ``SustainedPenstock(rate_per_sec=2.0)``
    (min_gap 0.5s).  Downstream wants to fire on every pass but the penstock
    blocks most of them; meanwhile the upstream keeps pushing waves into the
    reservoir.  When the reservoir hits depth, the ``DropOldest`` spillway
    displaces the oldest wave for every new one.

    This is the only test exercising all three primitives in a single graph;
    individual tests cover each in isolation but composition was a gap.
    """
    from incorporator.tideweaver import (
        DropOldest,
        HardLock,
        Reservoir,
        SustainedPenstock,
    )

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", interval=0.05)
    b = _stream("b", interval=0.05)
    composed = FlowControl(
        gate=HardLock(),
        penstock=SustainedPenstock(rate_per_sec=2.0),  # min_gap 0.5s
        reservoir=Reservoir(depth=3),
        spillway=DropOldest(),
    )
    ws = Watershed(
        window=_short_window(0.6),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=composed)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    tides = await _collect_tides(tw)

    reasons = [reason for tide in tides for _name, reason in tide.skipped]
    assert "penstock_limited" in reasons, (
        f"SustainedPenstock(rate=2/s) must surface 'penstock_limited' against "
        f"a 0.05s-interval downstream; got reasons={set(reasons)}"
    )

    edge_state = tw._edge_state[("a", "b")]
    assert edge_state.overflow_count > 0, (
        f"Reservoir(depth=3) with a fast upstream and a throttled downstream "
        f"must overflow; got overflow_count={edge_state.overflow_count}"
    )
    assert len(edge_state.waves) == 3, (
        f"Reservoir bounded at depth=3 post-overflow; got len(waves)={len(edge_state.waves)}"
    )

    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")


# ---------------------------------------------------------------------------
# FlowObserver — per-edge lifecycle hook (declarative telemetry channel)
# ---------------------------------------------------------------------------


def test_null_observer_is_the_flow_default() -> None:
    """``FlowControl()`` defaults to a NullObserver — no-op telemetry."""
    from incorporator.tideweaver import FlowControl, NullObserver

    fc = FlowControl()
    assert isinstance(fc.observer, NullObserver)


def test_flow_control_dump_omits_default_observer() -> None:
    """``FlowControl().model_dump()`` drops the default ``observer`` for minimal JSON.

    Senior-review m1: ``observer`` carries a default factory so user
    code can call ``.on_fire(...)`` without a None-check, but emitting
    ``"observer": {"type": "null"}`` into every serialised FlowControl
    bloated ``watershed.json``.  The ``@model_serializer`` drops the
    field when it's the default :class:`NullObserver`; round-trip is
    lossless because :meth:`model_validate` rebuilds the default.
    """
    from incorporator.tideweaver import FlowControl, LoggingObserver, NullObserver

    # Default-NullObserver path → observer key absent.
    dumped = FlowControl().model_dump()
    assert "observer" not in dumped

    # Explicit non-default observer → key present, round-trips losslessly.
    fc = FlowControl(observer=LoggingObserver(fire_level="info"))
    dumped_explicit = fc.model_dump()
    assert dumped_explicit.get("observer", {}).get("type") == "logging"

    # Round-trip: a JSON dict without observer rebuilds NullObserver via the factory.
    rebuilt = FlowControl.model_validate({})
    assert isinstance(rebuilt.observer, NullObserver)


def test_logging_observer_round_trips_via_json() -> None:
    """``observer: {type: logging, ...}`` deserialises via the discriminated union."""
    from incorporator.tideweaver import FlowControl, LoggingObserver

    fc = FlowControl.model_validate(
        {
            "observer": {
                "type": "logging",
                "fire_level": "info",
                "spillway_level": "warning",
                "reservoir_threshold": 0.75,
            },
        }
    )
    assert isinstance(fc.observer, LoggingObserver)
    assert fc.observer.fire_level == "info"
    assert fc.observer.reservoir_threshold == 0.75


@pytest.mark.asyncio
async def test_signal_observer_callback_receives_fire_events() -> None:
    """SignalObserver routes ``on_fire`` through the user callable with payload."""
    from incorporator.tideweaver import HardLock, SignalObserver

    events: List[Tuple[str, Tuple[str, str], Dict[str, Any]]] = []

    def sink(kind: str, edge: Tuple[str, str], payload: Dict[str, Any]) -> None:
        events.append((kind, edge, payload))

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", interval=0.05)
    b = _stream("b", interval=0.05)
    flow = FlowControl(gate=HardLock(), observer=SignalObserver(callback=sink))
    ws = Watershed(
        window=_short_window(0.3),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=flow)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    await _collect_tides(tw)

    fire_events = [e for e in events if e[0] == "fire"]
    assert fire_events, "SignalObserver.on_fire must fire when the dependent gates and runs"
    kind, edge, payload = fire_events[0]
    assert edge == ("a", "b")
    assert "wave_number" in payload

    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")


@pytest.mark.asyncio
async def test_observer_on_skip_fires_with_skip_reason() -> None:
    """When a gate or penstock returns a skip reason, ``on_skip`` carries it.

    Uses a HardLock chain so the dependent's first pass surfaces
    ``"awaiting_upstream"`` until A produces a wave.
    """
    from incorporator.tideweaver import HardLock, SignalObserver

    events: List[Tuple[str, Tuple[str, str], Dict[str, Any]]] = []

    def sink(kind: str, edge: Tuple[str, str], payload: Dict[str, Any]) -> None:
        events.append((kind, edge, payload))

    async def slow_a(current: Current) -> None:
        if current.name == "a":
            await asyncio.sleep(0.2)  # delay A so B's first pass sees no upstream wave

    a = _stream("a", interval=0.1)
    b = _stream("b", interval=0.05)
    flow = FlowControl(gate=HardLock(), observer=SignalObserver(callback=sink))
    ws = Watershed(
        window=_short_window(0.15),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=flow)],
    )
    tw = Tideweaver(ws, tick_factory=slow_a, pass_interval=0.02)
    await _collect_tides(tw)

    skip_events = [e for e in events if e[0] == "skip"]
    assert skip_events, "SignalObserver.on_skip must fire at least once during B's warm-up"
    reasons = {payload["reason"] for _kind, _edge, payload in skip_events}
    # Either gate-level "awaiting_upstream" or surge-barrier "skip_ahead" is acceptable;
    # the observer just needs to receive the reason that fired.
    assert reasons, "skip events must carry a reason payload"


@pytest.mark.asyncio
async def test_observer_on_spillway_fires_per_displacement() -> None:
    """One ``on_spillway`` call per displaced wave; carries the overflow_count."""
    from incorporator.tideweaver import (
        DropOldest,
        HardLock,
        Reservoir,
        SignalObserver,
    )

    events: List[Tuple[str, Tuple[str, str], Dict[str, Any]]] = []

    def sink(kind: str, edge: Tuple[str, str], payload: Dict[str, Any]) -> None:
        events.append((kind, edge, payload))

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", interval=0.05)
    b = _stream("b", interval=10.0)  # never fires; ensures overflow on every a-tick
    flow = FlowControl(
        gate=HardLock(),
        reservoir=Reservoir(depth=1),
        spillway=DropOldest(),
        observer=SignalObserver(callback=sink),
    )
    ws = Watershed(
        window=_short_window(0.3),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=flow)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    await _collect_tides(tw)

    spillway_events = [e for e in events if e[0] == "spillway"]
    assert spillway_events, "SignalObserver.on_spillway must fire at least once"
    # Count must match the edge_state's accumulated overflow_count.
    last_event_count = spillway_events[-1][2]["overflow_count"]
    edge_state = tw._edge_state[("a", "b")]
    assert last_event_count == edge_state.overflow_count

    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")


@pytest.mark.asyncio
async def test_observer_on_reservoir_level_fires_per_append() -> None:
    """``on_reservoir_level`` fires after every reservoir append with used/capacity."""
    from incorporator.tideweaver import (
        HardLock,
        Reservoir,
        SignalObserver,
    )

    events: List[Tuple[str, Tuple[str, str], Dict[str, Any]]] = []

    def sink(kind: str, edge: Tuple[str, str], payload: Dict[str, Any]) -> None:
        events.append((kind, edge, payload))

    strong_refs: List[_A] = []

    async def fake(current: Current) -> None:
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(strong_refs)}")
            strong_refs.append(inst)
            _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]

    a = _stream("a", interval=0.05)
    b = _stream("b", interval=10.0)
    flow = FlowControl(
        gate=HardLock(),
        reservoir=Reservoir(depth=3),
        observer=SignalObserver(callback=sink),
    )
    ws = Watershed(
        window=_short_window(0.3),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=flow)],
    )
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    await _collect_tides(tw)

    level_events = [e for e in events if e[0] == "reservoir_level"]
    assert level_events, "SignalObserver.on_reservoir_level must fire after every wave append"
    # capacity matches Reservoir.depth; used grows then plateaus at depth.
    assert all(payload["capacity"] == 3 for _k, _e, payload in level_events)
    final_used = level_events[-1][2]["used"]
    assert 1 <= final_used <= 3

    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")


@pytest.mark.asyncio
async def test_observer_does_not_fire_on_fire_for_bypassed_edges() -> None:
    """Bypassed edges report on_skip is NOT called for them; on_fire is NOT called either.

    The bypass contract: the tick fires ignoring this edge's gate + penstock,
    so on_fire on this edge would imply a per-edge contribution that didn't
    happen.  Bypassed edges produce no observer event for that pass.
    """
    from incorporator.tideweaver import (
        HardLock,
        SignalObserver,
        SurgeBarrier,
    )

    events: List[Tuple[str, Tuple[str, str], Dict[str, Any]]] = []

    def sink(kind: str, edge: Tuple[str, str], payload: Dict[str, Any]) -> None:
        events.append((kind, edge, payload))

    async def slow_a(current: Current) -> None:
        if current.name == "a":
            await asyncio.sleep(0.6)  # overruns 2.0 * 0.1 = 0.2s threshold

    a = _stream("a", interval=0.1)
    b = _stream("b", interval=0.1)
    flow = FlowControl(
        gate=HardLock(),
        surge_barrier=SurgeBarrier(threshold_multiple=2.0, action="bypass"),
        observer=SignalObserver(callback=sink),
    )
    ws = Watershed(
        window=_short_window(0.7),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=flow)],
    )
    tw = Tideweaver(ws, tick_factory=slow_a, pass_interval=0.05)
    await _collect_tides(tw)

    # The bypass fired (B ticked despite A still running); no on_fire for the bypassed edge.
    fire_events_on_ab = [e for e in events if e[0] == "fire" and e[1] == ("a", "b")]
    assert fire_events_on_ab == [], f"Bypassed edges must not emit on_fire events; got {fire_events_on_ab}"


# ---------------------------------------------------------------------------
# Phase 5: phase_offset_sec on Current (green-wave coordination)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_phase_offset_delays_first_tick() -> None:
    """``Current(phase_offset_sec=0.3)`` skips with 'phase_offset' until the offset elapses."""
    fires: List[Tuple[float, str]] = []

    async def fake(current: Current) -> None:
        fires.append((time.monotonic(), current.name))

    a = Stream(name="a", cls=_A, interval=0.05, phase_offset_sec=0.3, incorp_params={})
    ws = Watershed.parallel(window=_short_window(0.6), currents=[a])
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    start = time.monotonic()
    tides = await _collect_tides(tw)
    skip_reasons = [reason for tide in tides for name, reason in tide.skipped if name == "a"]
    assert "phase_offset" in skip_reasons, f"phase_offset must surface as skip reason; got {skip_reasons}"
    # First fire must be at or after the offset.
    assert fires, "current must eventually fire"
    first_fire_dt = fires[0][0] - start
    assert first_fire_dt >= 0.25, (
        f"first tick must wait phase_offset_sec=0.3 (allowing ~0.05s slop); got {first_fire_dt:.3f}s"
    )


@pytest.mark.asyncio
async def test_phase_offset_zero_matches_today() -> None:
    """``phase_offset_sec=0.0`` (default) does NOT emit 'phase_offset' — first tick runs immediately."""
    fires: List[str] = []

    async def fake(current: Current) -> None:
        fires.append(current.name)

    a = _stream("a", interval=0.05)  # default phase_offset_sec=0.0
    ws = Watershed.parallel(window=_short_window(0.2), currents=[a])
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    tides = await _collect_tides(tw)
    skip_reasons = [reason for tide in tides for _name, reason in tide.skipped]
    assert "phase_offset" not in skip_reasons, "default phase_offset_sec=0 must NOT emit 'phase_offset'"
    assert fires, "current must fire when phase_offset_sec=0"


@pytest.mark.asyncio
async def test_phase_offset_green_wave_alignment() -> None:
    """Two coupled currents with staggered phase offsets fire in the intended order.

    A fires at t=0; B fires at t=phase_offset_sec.  When A's interval (0.5s)
    is the long pole, B's phase_offset_sec=0.2s lands B's first tick squarely
    inside A's first cycle — the green-wave intuition.
    """
    fires: List[Tuple[float, str]] = []

    async def fake(current: Current) -> None:
        fires.append((time.monotonic(), current.name))

    a = Stream(name="a", cls=_A, interval=0.5, incorp_params={})  # phase 0
    b = Stream(name="b", cls=_B, interval=0.5, phase_offset_sec=0.2, incorp_params={})
    ws = Watershed.parallel(window=_short_window(0.4), currents=[a, b])
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    start = time.monotonic()
    await _collect_tides(tw)
    first_a = next((t for t, n in fires if n == "a"), None)
    first_b = next((t for t, n in fires if n == "b"), None)
    assert first_a is not None and first_b is not None
    # A fires near t=0, B fires near t=0.2.
    assert (first_a - start) < 0.1, f"A must fire promptly; got {first_a - start:.3f}s"
    assert (first_b - start) >= 0.15, f"B must wait phase_offset; got {first_b - start:.3f}s"
    assert first_b > first_a, "B's first tick must follow A's"


@pytest.mark.asyncio
async def test_phase_offset_with_hard_gated_upstream() -> None:
    """``phase_offset_sec`` on a dependent in a HardLock chain — both ``phase_offset``
    and ``awaiting_upstream`` can interleave as skip reasons during warm-up;
    the dependent must still fire after both clear.

    Distinct from ``test_phase_offset_green_wave_alignment`` which uses
    ``parallel`` (no edges).  This exercises the chain path where the gate's
    ``awaiting_upstream`` check runs alongside the phase_offset check.
    """
    fires: List[Tuple[float, str]] = []

    async def fake(current: Current) -> None:
        fires.append((time.monotonic(), current.name))

    a = Stream(name="a", cls=_A, interval=0.1, incorp_params={})
    b = Stream(name="b", cls=_B, interval=0.1, phase_offset_sec=0.2, incorp_params={})
    ws = Watershed.chain(window=_short_window(0.6), currents=[a, b])
    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    start = time.monotonic()
    tides = await _collect_tides(tw)

    b_skips = [reason for tide in tides for name, reason in tide.skipped if name == "b"]
    assert "phase_offset" in b_skips, f"b must emit 'phase_offset' during warm-up; got {b_skips}"
    # awaiting_upstream may or may not appear depending on exact scheduling —
    # either gate can be the proximate skip cause on any given early pass.
    # The contract is that b eventually fires once both clear.
    b_fires = [t for t, n in fires if n == "b"]
    assert b_fires, "b must fire after phase_offset + gate clear"
    first_b = b_fires[0] - start
    assert first_b >= 0.15, (
        f"b's first fire must be at or after phase_offset_sec=0.2 (allowing ~0.05s slop); got {first_b:.3f}s"
    )


# ---------------------------------------------------------------------------
# CustomCurrent — escape-hatch subclass for non-verb-typed tick bodies.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_custom_current_dispatches_to_user_tick() -> None:
    """The scheduler calls ``await custom.tick(scheduler)`` for CustomCurrent subclasses.

    Replaces the ``tick_factory=...`` pattern as the documented public
    path; ``tick_factory`` stays as the test-only override.
    """
    from incorporator.tideweaver import CustomCurrent

    fires: List[str] = []

    class Healthcheck(CustomCurrent):
        async def tick(self, scheduler: Any) -> None:
            fires.append(self.name)

    a = Healthcheck(name="health", cls=_A, interval=0.05)
    ws = Watershed.parallel(window=_short_window(0.15), currents=[a])
    tw = Tideweaver(ws, pass_interval=0.02)
    await _collect_tides(tw)
    assert fires, "CustomCurrent.tick must be called by the scheduler"
    assert all(name == "health" for name in fires)


@pytest.mark.asyncio
async def test_base_custom_current_raises_when_tick_not_overridden() -> None:
    """The base ``CustomCurrent.tick`` raises NotImplementedError with a guiding message."""
    from incorporator.tideweaver import CustomCurrent

    bare = CustomCurrent(name="bare", cls=_A, interval=0.05)
    with pytest.raises(NotImplementedError, match="must override async tick"):
        await bare.tick(None)


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


@pytest.mark.asyncio
async def test_export_current_sql_table_snapshots_upstream(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """(T5) An ``Export`` current configured with ``sql_table`` (no ``file_path``) constructs and ticks cleanly.

    Regression guard that the D8-02 construction-time validator only
    rejects the *missing-both-keys* case, not the legitimate
    ``sql_table``-only configuration.
    """
    monkeypatch.chdir(tmp_path)

    class TickSql(Incorporator):
        """Per-tick source row used by the Export sql_table regression test."""

    out_db = tmp_path / "ticks.sqlite"
    tick_strong_refs: List[TickSql] = []

    async def fake_upstream(current: Current) -> None:
        if current.name == "ticks":
            inst = TickSql()
            tick_strong_refs.append(inst)
            TickSql.inc_dict[f"t-{len(tick_strong_refs)}"] = inst
            TickSql._tideweaver_snapshot = list(tick_strong_refs)  # type: ignore[attr-defined]

    src = Stream(name="ticks", cls=TickSql, interval=0.1, incorp_params={}, on_error="fail_watershed")
    dump = Export(
        name="dump",
        cls=TickSql,
        interval=0.1,
        export_params={"file_path": str(out_db), "sql_table": "ticks", "if_exists": "append"},
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

    assert out_db.exists(), "Export tick with sql_table must write the sqlite file"


@pytest.mark.asyncio
async def test_fjord_empty_export_params_flushes_unchanged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """(T6) ``Fjord(export_params={})`` regression guard — constructs and flushes exactly as before D8-02.

    ``Fjord`` never gained the ``Export``-only destination validator;
    this asserts its flush-via-outflow path is untouched by the fix.
    """
    monkeypatch.chdir(tmp_path)

    class Lap2(Incorporator):
        """Per-lap source used by the Fjord empty-export_params regression test."""

    class State2(Incorporator):
        """Derived state class produced by outflow(state)."""

    outflow_py = tmp_path / "outflow.py"
    outflow_py.write_text(
        "def outflow(state):\n    laps = state.get('Lap2', [])\n    return [{'lap_id': i} for i in range(len(laps))]\n",
        encoding="utf-8",
    )

    lap_strong_refs: List[Lap2] = []

    async def fake_upstream(current: Current) -> None:
        if current.name == "laps":
            inst = Lap2()
            lap_strong_refs.append(inst)
            Lap2.inc_dict[f"lap-{len(lap_strong_refs)}"] = inst

    a = Stream(name="laps", cls=Lap2, interval=0.1, incorp_params={}, on_error="fail_watershed")
    f = Fjord(
        name="state",
        cls=State2,
        interval=0.1,
        export_params={},
        outflow=outflow_py,
        on_error="fail_watershed",
    )
    assert f.export_params == {}
    ws = Watershed.chain(window=_short_window(0.5), currents=[a, f])
    tw = Tideweaver(ws, tick_factory=fake_upstream, pass_interval=0.05)

    async def selective(current: Current) -> None:
        if isinstance(current, Fjord):
            await tw._tick_fjord(current)
        else:
            await fake_upstream(current)

    monkeypatch.setattr(tw, "_invoke_tick", selective)
    tides = await _collect_tides(tw)
    assert any("state" in tide.fired for tide in tides), "Fjord with empty export_params must still fire/flush"


def test_json_export_missing_destination_raises_at_construction(tmp_path: Path) -> None:
    """(T4 JSON variant) A watershed.json ``verb: 'export'`` entry with no ``export_params`` raises at load time.

    The loader forwards ``entry.get('export_params', {})`` straight
    into ``Export(...)``, so the same construction-time ``ValueError``
    fires for a JSON-defined plan as for the Python API.
    """
    from incorporator.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("chain", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "export", "interval": 30},
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError, match="file_path.*sql_table|sql_table.*file_path"):
        load_watershed(cfg)


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
        body["gate_mode"] = "hard"
    return body


def test_json_chain_shape(tmp_path: Path) -> None:
    """``shape: 'chain'`` parses into a Watershed with the expected chain edges."""
    from incorporator.tideweaver.config import load_watershed

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
    from incorporator.tideweaver.config import load_watershed

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
    from incorporator.tideweaver.config import load_watershed

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
    from incorporator.tideweaver.config import load_watershed

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
    from incorporator.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("custom", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "c", "class": "FlagEvents", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["edges"] = [
        {"from": "a", "to": "b", "gate_mode": "hard"},
        {"from": "b", "to": "c", "gate_mode": "soft"},
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    ws = load_watershed(cfg)
    modes = {(e.from_name, e.to_name): _gate_name(e) for e in ws.edges}
    assert modes == {("a", "b"): "hard", ("b", "c"): "soft"}


# ---------------------------------------------------------------------------
# JSON loader: rich FlowControl support (per-edge "flow" dicts, top-level "flow",
# SignalPenstock + ExportToArchive sidecar resolution)
# ---------------------------------------------------------------------------


def test_json_custom_edge_full_flowcontrol(tmp_path: Path) -> None:
    """A per-edge ``flow: {...}`` round-trips through the loader, inflating every primitive."""
    from incorporator.tideweaver import (
        BurstPenstock,
        RaiseOverflow,
        Reservoir,
        SurgeBarrier,
        Weir,
    )
    from incorporator.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("custom", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["edges"] = [
        {
            "from": "a",
            "to": "b",
            "flow": {
                "gate": {"type": "weir"},
                "penstock": {"type": "burst", "rate_per_sec": 2.0, "burst": 5},
                "reservoir": {"depth": 5},
                "spillway": {"type": "raise_overflow"},
                "surge_barrier": {"threshold_multiple": 10.0, "action": "bypass"},
            },
        }
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    ws = load_watershed(cfg)
    [edge] = ws.edges
    assert isinstance(edge.flow.gate, Weir)
    assert isinstance(edge.flow.penstock, BurstPenstock)
    assert edge.flow.penstock.rate_per_sec == 2.0
    assert edge.flow.penstock.burst == 5
    assert isinstance(edge.flow.reservoir, Reservoir) and edge.flow.reservoir.depth == 5
    assert isinstance(edge.flow.spillway, RaiseOverflow)
    assert isinstance(edge.flow.surge_barrier, SurgeBarrier)
    assert edge.flow.surge_barrier.action == "bypass"
    assert edge.flow.surge_barrier.threshold_multiple == 10.0


@pytest.mark.asyncio
async def test_json_custom_edge_null_penstock_validates_and_runs(tmp_path: Path) -> None:
    """A per-edge ``flow: {"penstock": {"type": "null"}}`` round-trips (D8-01) and ticks cleanly.

    Prior to the ``_PenstockUnion`` fix, this JSON shape raised
    ``ValidationError`` at ``load_watershed`` time.
    """
    from incorporator.io.penstock import NullPenstock
    from incorporator.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("custom", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["edges"] = [
        {"from": "a", "to": "b", "flow": {"penstock": {"type": "null"}}},
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    ws = load_watershed(cfg)
    [edge] = ws.edges
    assert isinstance(edge.flow.penstock, NullPenstock)

    # Re-window so the scheduler treats the plan as open "now", then run a pass.
    ws.window = _short_window(0.3)
    strong_refs: List[Any] = []

    async def fake(current: Current) -> None:
        cls = current.cls
        inst = cls()
        strong_refs.append(inst)
        cls._tideweaver_snapshot = list(i for i in strong_refs if isinstance(i, cls))  # type: ignore[attr-defined]

    tw = Tideweaver(ws, tick_factory=fake, pass_interval=0.02)
    tides = await _collect_tides(tw)
    reasons = [reason for tide in tides for _name, reason in tide.skipped]
    assert "penstock_limited" not in reasons, f"NullPenstock must never emit 'penstock_limited'; got {reasons}"
    for name in ("LapData", "PitStops"):
        cls_obj = next(c for c in {type(i) for i in strong_refs} if c.__name__ == name)
        if "_tideweaver_snapshot" in cls_obj.__dict__:
            delattr(cls_obj, "_tideweaver_snapshot")


def test_json_custom_edge_rejects_both_flow_and_mode(tmp_path: Path) -> None:
    """Per-edge ``flow`` + ``gate_mode`` raises a clear ValueError."""
    from incorporator.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("custom", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["edges"] = [
        {"from": "a", "to": "b", "gate_mode": "weir", "flow": {"gate": {"type": "hard"}}},
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError, match="not both"):
        load_watershed(cfg)


def test_json_chain_top_level_flow(tmp_path: Path) -> None:
    """``{"shape": "chain", "flow": {...}}`` builds a chain where every edge shares the parsed FlowControl."""
    from incorporator.tideweaver import Weir
    from incorporator.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("chain", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "c", "class": "FlagEvents", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["flow"] = {"gate": {"type": "weir"}, "reservoir": {"depth": 5}}
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    ws = load_watershed(cfg)
    assert all(isinstance(e.flow.gate, Weir) for e in ws.edges)
    assert all(e.flow.reservoir.depth == 5 for e in ws.edges)


def test_json_dependency_mode_alias_raises_after_v1_3_0(tmp_path: Path) -> None:
    """Top-level legacy ``dependency_mode`` raises ValueError with migration guidance.

    The alias was DeprecationWarning-flagged in v1.2.0 and removed in
    v1.3.0.  Passing it now produces a clear error pointing at
    ``gate_mode`` so users see the break immediately rather than
    silently dropping their intended config.
    """
    from incorporator.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("chain", with_mode=False)
    body["dependency_mode"] = "weir"
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError, match="'dependency_mode' was removed in v1.3.0"):
        load_watershed(cfg)


def test_json_edge_mode_alias_raises_after_v1_3_0(tmp_path: Path) -> None:
    """Per-edge legacy ``"mode"`` key raises ValueError with migration guidance."""
    from incorporator.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("custom", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["edges"] = [{"from": "a", "to": "b", "mode": "weir"}]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError, match="'mode' was removed in v1.3.0"):
        load_watershed(cfg)


def test_json_top_level_rejects_both_flow_and_gate_mode(tmp_path: Path) -> None:
    """Top-level ``flow`` + ``gate_mode`` on a shape constructor raises."""
    from incorporator.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("chain")  # already sets gate_mode="hard"
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["flow"] = {"gate": {"type": "weir"}}
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError, match="not both"):
        load_watershed(cfg)


def test_json_signal_penstock_resolves_sidecar_callable(tmp_path: Path) -> None:
    """``SignalPenstock.rate_fn`` resolves a bare name on the outflow sidecar."""
    from incorporator.tideweaver import SignalPenstock
    from incorporator.tideweaver.config import load_watershed

    # Outflow.py defines peak_rate alongside the required outflow() function.
    _write_sidecar(
        tmp_path / "outflow.py",
        "from incorporator import Incorporator\n"
        "class LapData(Incorporator):\n    pass\n"
        "class PitStops(Incorporator):\n    pass\n"
        "def peak_rate(edge_state, now):\n    return 5.0\n"
        "def outflow(state):\n    return []\n",
    )
    body = _watershed_json_body("custom", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["edges"] = [
        {
            "from": "a",
            "to": "b",
            "flow": {
                "gate": {"type": "weir"},
                "penstock": {"type": "signal", "rate_fn": "peak_rate"},
            },
        },
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    ws = load_watershed(cfg)
    [edge] = ws.edges
    assert isinstance(edge.flow.penstock, SignalPenstock)
    # Callable resolved + invocable.
    assert edge.flow.penstock.rate_fn(None, 0.0) == 5.0


def test_json_signal_penstock_resolves_module_path(tmp_path: Path) -> None:
    """``rate_fn: "module:fn"`` resolves via importlib (stdlib reference to avoid sidecar coupling)."""
    from incorporator.tideweaver import SignalPenstock
    from incorporator.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("custom", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["edges"] = [
        {
            "from": "a",
            "to": "b",
            "flow": {
                "penstock": {"type": "signal", "rate_fn": "json:loads"},
            },
        },
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    ws = load_watershed(cfg)
    [edge] = ws.edges
    assert isinstance(edge.flow.penstock, SignalPenstock)
    # Identity check: same callable as json.loads.
    import json as _json

    assert edge.flow.penstock.rate_fn is _json.loads


def test_json_signal_penstock_missing_callable_raises(tmp_path: Path) -> None:
    """An unknown ``rate_fn`` name surfaces a clear ValueError."""
    from incorporator.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("custom", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["edges"] = [
        {
            "from": "a",
            "to": "b",
            "flow": {
                "penstock": {"type": "signal", "rate_fn": "no_such_function"},
            },
        },
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError, match="no_such_function"):
        load_watershed(cfg)


def test_json_export_to_archive_resolves_archive_cls(tmp_path: Path) -> None:
    """``ExportToArchive.archive_cls`` resolves a class name on the outflow sidecar."""
    from incorporator.tideweaver import ExportToArchive
    from incorporator.tideweaver.config import load_watershed

    # Outflow.py defines ArchivedTrades next to the required outflow().
    _write_sidecar(
        tmp_path / "outflow.py",
        "from incorporator import Incorporator\n"
        "class LapData(Incorporator):\n    pass\n"
        "class PitStops(Incorporator):\n    pass\n"
        "class ArchivedTrades(Incorporator):\n    pass\n"
        "def outflow(state):\n    return []\n",
    )
    body = _watershed_json_body("custom", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["edges"] = [
        {
            "from": "a",
            "to": "b",
            "flow": {
                "spillway": {"type": "export_to_archive", "archive_cls": "ArchivedTrades"},
            },
        },
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    ws = load_watershed(cfg)
    [edge] = ws.edges
    assert isinstance(edge.flow.spillway, ExportToArchive)
    assert edge.flow.spillway.archive_cls.__name__ == "ArchivedTrades"


def test_json_export_to_archive_max_entries_round_trips(tmp_path: Path) -> None:
    """``ExportToArchive.max_entries`` round-trips through the JSON watershed loader."""
    from incorporator.tideweaver import ExportToArchive
    from incorporator.tideweaver.config import load_watershed

    _write_sidecar(
        tmp_path / "outflow.py",
        "from incorporator import Incorporator\n"
        "class LapData(Incorporator):\n    pass\n"
        "class PitStops(Incorporator):\n    pass\n"
        "class ArchivedTrades(Incorporator):\n    pass\n"
        "def outflow(state):\n    return []\n",
    )
    body = _watershed_json_body("custom", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["edges"] = [
        {
            "from": "a",
            "to": "b",
            "flow": {
                "spillway": {"type": "export_to_archive", "archive_cls": "ArchivedTrades", "max_entries": 100},
            },
        },
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    ws = load_watershed(cfg)
    [edge] = ws.edges
    assert isinstance(edge.flow.spillway, ExportToArchive)
    assert edge.flow.spillway.max_entries == 100


def test_json_export_to_archive_missing_class_raises(tmp_path: Path) -> None:
    """An unknown ``archive_cls`` name surfaces a clear ValueError."""
    from incorporator.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("custom", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["edges"] = [
        {
            "from": "a",
            "to": "b",
            "flow": {
                "spillway": {"type": "export_to_archive", "archive_cls": "NoSuchArchive"},
            },
        },
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError, match="NoSuchArchive"):
        load_watershed(cfg)


def test_json_reservoir_and_surge_barrier_native(tmp_path: Path) -> None:
    """``reservoir`` and ``surge_barrier`` need no resolution — pass through Pydantic natively."""
    from incorporator.tideweaver import Reservoir, SurgeBarrier
    from incorporator.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("custom", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        {"name": "b", "class": "PitStops", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    body["edges"] = [
        {
            "from": "a",
            "to": "b",
            "flow": {
                "reservoir": {"depth": 10},
                "surge_barrier": {"threshold_multiple": 5.0, "action": "halt"},
            },
        },
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    ws = load_watershed(cfg)
    [edge] = ws.edges
    assert isinstance(edge.flow.reservoir, Reservoir)
    assert edge.flow.reservoir.depth == 10
    assert isinstance(edge.flow.surge_barrier, SurgeBarrier)
    assert edge.flow.surge_barrier.action == "halt"
    assert edge.flow.surge_barrier.threshold_multiple == 5.0


def test_json_env_interpolation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``${VAR}`` references in the JSON resolve from os.environ at load time."""
    from incorporator.tideweaver.config import load_watershed

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
    from incorporator.tideweaver.config import load_watershed

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
    from incorporator.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = _watershed_json_body("parallel", with_mode=False)
    body["currents"] = [
        {"name": "a", "class": "GhostClass", "verb": "stream", "interval": 30, "incorp_params": {}},
    ]
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError, match="GhostClass"):
        load_watershed(cfg)


def test_json_relative_inc_file_resolves_to_config_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A relative ``inc_file`` in watershed.json is resolved against the config dir, not CWD.

    Proves that loading a watershed.json from a different working directory still
    resolves ``incorp_params['inc_file']`` to an absolute path under the config dir,
    not under the process CWD.  Guards the guarantee stated in the watershed.json
    comments.
    """
    from incorporator.tideweaver.config import load_watershed

    config_dir = tmp_path / "config"
    config_dir.mkdir()

    # Write a minimal fixture file co-located with the config.
    fixture = config_dir / "fixture.json"
    fixture.write_text("[]", encoding="utf-8")

    # Write a minimal outflow sidecar so class resolution succeeds.
    (config_dir / "outflow.py").write_text(
        "from incorporator import Incorporator\nclass MySource(Incorporator):\n    pass\n",
        encoding="utf-8",
    )

    body = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "parallel",
        "outflow": "outflow.py",
        "drain_timeout": 5,
        "currents": [
            {
                "name": "src",
                "class": "MySource",
                "verb": "stream",
                "interval": 30,
                "incorp_params": {"inc_file": "fixture.json", "inc_code": "id"},
            }
        ],
    }
    cfg = config_dir / "watershed.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")

    # Move CWD to tmp_path — "fixture.json" does NOT exist relative to CWD.
    monkeypatch.chdir(tmp_path)

    ws = load_watershed(cfg)
    stream_current = ws.currents[0]
    resolved_inc_file = stream_current.incorp_params["inc_file"]  # type: ignore[union-attr]

    expected = str((config_dir / "fixture.json").resolve())
    assert resolved_inc_file == expected, (
        f"inc_file should resolve to config dir, not CWD.\n  got:      {resolved_inc_file!r}\n  expected: {expected!r}"
    )
    # Also confirm CWD-relative would have been wrong.
    cwd_relative = str((tmp_path / "fixture.json").resolve())
    assert resolved_inc_file != cwd_relative, "inc_file must NOT resolve relative to CWD"


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
    """Non-ISO window timestamps surface a clear error.

    D2b: build_watershed raises on the first bad timestamp it parses;
    multi-error reports are not promised by the new validator.  Test
    one bad timestamp at a time.
    """
    from incorporator.cli.validate import validate_watershed_config

    _write_outflow_with_classes(tmp_path)
    body = {
        "window": {"start": "not-a-date", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "parallel",
        "outflow": "outflow.py",
        "currents": [
            {"name": "a", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        ],
    }
    errs = validate_watershed_config(body, tmp_path)
    assert errs, "bad window.start should produce at least one error"
    assert any("not-a-date" in e or "isoformat" in e.lower() or "window" in e.lower() for e in errs)


def test_validate_rejects_unknown_shape(tmp_path: Path) -> None:
    """An unrecognised 'shape' key produces a clear listing of valid shapes.

    D2b: build_watershed raises ``ValueError("Unknown shape: ...")`` with
    the expected-set inline; substring match is preserved.
    """
    from incorporator.cli.validate import validate_watershed_config

    body = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "noodle",
    }
    errs = validate_watershed_config(body, tmp_path)
    assert any("shape" in e.lower() for e in errs)
    assert any("noodle" in e for e in errs)


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


def test_validate_rejects_parallel_with_gate_mode(tmp_path: Path) -> None:
    """parallel shape refuses 'gate_mode' — there are no edges to govern."""
    from incorporator.cli.validate import validate_watershed_config

    _write_outflow_with_classes(tmp_path)
    body = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "parallel",
        "outflow": "outflow.py",
        "gate_mode": "hard",
        "currents": [{"name": "a", "class": "LapData", "verb": "stream", "interval": 30}],
    }
    errs = validate_watershed_config(body, tmp_path)
    assert any("gate_mode" in e and "parallel" in e for e in errs)


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
            {"from": "a", "to": "b", "gate_mode": "hard"},
            {"from": "b", "to": "a", "gate_mode": "hard"},
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


# ---------------------------------------------------------------------------
# Finding 6 — _build_current actionable error for verb="custom"
# ---------------------------------------------------------------------------


def test_build_current_custom_verb_raises_actionable_error(tmp_path: Path) -> None:
    """``verb='custom'`` in watershed.json raises a clear, actionable ValueError.

    CustomCurrent requires a Python tick() body and cannot be declared via
    JSON config.  The error message must name the current, explain the
    limitation, and direct the user to the Python API.  It must NOT produce
    the generic 'Unknown verb' fallback.
    """
    from incorporator.tideweaver.config import load_watershed

    _write_outflow_with_classes(tmp_path)
    body = {
        "window": {"start": "2026-05-16T00:00:00+00:00", "end": "2026-05-16T01:00:00+00:00"},
        "shape": "parallel",
        "outflow": "outflow.py",
        "currents": [
            {
                "name": "custom_tick",
                "class": "LapData",
                "verb": "custom",
                "interval": 30,
            }
        ],
    }
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError) as exc_info:
        load_watershed(cfg)
    msg = str(exc_info.value)
    assert "custom_tick" in msg
    assert "Python API" in msg
    # Must not fall through to the generic "Unknown verb" fallback.
    assert "Unknown verb" not in msg


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
    # between Stream ticks; the edge's loose SurgeBarrier ensures the Fjord
    # keeps firing inside that gap.
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
        export_params={"file_path": str(out_file), "format": "ndjson", "if_exists": "append"},
        outflow=outflow_py,
        on_error="fail_watershed",
    )
    # gate_mode="weir" lets the fast Fjord fire on its own cadence while the
    # slow Stream is in-flight — replaces the old skip_threshold=50.0 hack.
    ws = Watershed.chain(window=_short_window(5.0), currents=[posts, fjord], gate_mode="weir")
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

    Regression for the Middle-Fjord routing gap: ``outflow.flush`` used to
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
    # ``outflow.flush`` picks it up via ``getattr(outflow_module,
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
        export_params={"file_path": str(out_file), "format": "ndjson", "if_exists": "replace"},
        outflow=outflow_py,
        on_error="fail_watershed",
    )
    # gate_mode="weir" lets the Fjord fire on its own cadence while the
    # slow Stream is in-flight.
    ws = Watershed.chain(window=_short_window(2.0), currents=[posts, fjord], gate_mode="weir")
    tw = Tideweaver(ws, pass_interval=0.05)
    await _collect_tides(tw)

    # The headline assertion: the Fjord's output class carries a non-empty
    # ``_tideweaver_snapshot``.  Pre-fix this was None / AttributeError because
    # ``outflow.flush`` parked the strong ref under ``_fjord_snapshot``.
    snapshot: List[Any] = list(getattr(DerivedState, "_tideweaver_snapshot", []))
    assert snapshot, (
        "Fjord output class must carry a non-empty _tideweaver_snapshot — "
        "empty means the Middle-Fjord snapshot-parking regressed and downstream "
        "currents will see an empty inc_dict"
    )
    derived_ids = {getattr(d, "derived_id", None) for d in snapshot}
    assert derived_ids == {101, 102}, f"snapshot must contain both derived rows by derived_id, got {derived_ids}"


# ---------------------------------------------------------------------------
# Back-compat + new Tide fields
# ---------------------------------------------------------------------------


def test_tide_fired_and_skipped_still_populated() -> None:
    """Tide.fired and Tide.skipped remain populated alongside current_outcomes — back-compat."""
    from incorporator.tideweaver.current_outcome import CurrentOutcome

    tide = Tide(
        tide_number=1,
        fired=["a"],
        skipped=[("b", "not_due")],
        current_outcomes=[
            CurrentOutcome(name="a", status="fired"),
            CurrentOutcome(name="b", status="skipped", reason="not_due"),
        ],
        duration_sec=0.01,
    )
    # Existing consumers of fired/skipped must keep working.
    assert tide.fired == ["a"]
    assert tide.skipped == [("b", "not_due")]
    # New structured field also populated.
    assert len(tide.current_outcomes) == 2


@pytest.mark.asyncio
async def test_tide_current_outcomes_smoke() -> None:
    """current_outcomes is populated by the scheduler for each pass — basic smoke test."""
    fires: List[str] = []

    async def fake_tick(current: Current) -> None:
        fires.append(current.name)

    a = _stream("a", interval=0.1)
    ws = Watershed.parallel(window=_short_window(0.3), currents=[a])
    tw = Tideweaver(ws, tick_factory=fake_tick, pass_interval=0.05)
    tides = await _collect_tides(tw)

    # Every tide must have a current_outcomes list.
    for tide in tides:
        assert isinstance(tide.current_outcomes, list)

    # At least one tide should have "a" in its current_outcomes.
    all_names = {co.name for tide in tides for co in tide.current_outcomes}
    assert "a" in all_names


@pytest.mark.asyncio
async def test_tide_wake_reason_startup_on_first_pass() -> None:
    """The very first Tide's wake_reason is 'startup'."""
    fires: List[str] = []

    async def fake_tick(current: Current) -> None:
        fires.append(current.name)

    a = _stream("a", interval=0.1)
    ws = Watershed.parallel(window=_short_window(0.3), currents=[a])
    tw = Tideweaver(ws, tick_factory=fake_tick, pass_interval=0.05)
    tides = await _collect_tides(tw)

    assert tides, "must have emitted at least one tide"
    assert tides[0].wake_reason == "startup"


@pytest.mark.asyncio
async def test_tide_canal_rejects_from_name_to_name_populated() -> None:
    """Canal-layer rejects carry from_name and to_name identifying the edge."""

    async def slow_a(current: Current) -> None:
        if current.name == "a":
            await asyncio.sleep(0.6)

    a = _stream("a", interval=0.1)
    b = _stream("b", interval=0.1)
    ws = Watershed.chain(window=_short_window(0.7), currents=[a, b])
    tw = Tideweaver(ws, tick_factory=slow_a, pass_interval=0.05)
    await _collect_tides(tw)

    sa_rejects = [r for r in tw.rejects if r.error_kind == "SkipAhead"]
    assert sa_rejects, "expected at least one SkipAhead reject"
    # from_name and to_name must be populated on canal-layer rejects.
    assert sa_rejects[0].from_name == "a"
    assert sa_rejects[0].to_name == "b"


@pytest.mark.asyncio
async def test_tide_new_scalar_fields_populated() -> None:
    """Tide heap_depth, in_flight_count_at_start, canal_rejects_added are present on every pass."""
    fires: List[str] = []

    async def fake_tick(current: Current) -> None:
        fires.append(current.name)

    a = _stream("a", interval=0.1)
    ws = Watershed.parallel(window=_short_window(0.3), currents=[a])
    tw = Tideweaver(ws, tick_factory=fake_tick, pass_interval=0.05)
    tides = await _collect_tides(tw)

    for tide in tides:
        assert isinstance(tide.heap_depth, int)
        assert isinstance(tide.in_flight_count_at_start, int)
        assert isinstance(tide.canal_rejects_added, int)
        assert tide.canal_rejects_added >= 0


# ---------------------------------------------------------------------------
# D5-01: failed/cancelled ticks must not advertise a wave (finally-block gate)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failed_isolated_tick_does_not_advertise_wave_hard_chain() -> None:
    """A(on_error='isolate', succeeds once then always raises) -> B(HardLock): B fires exactly once.

    B's edge reservoir must not grow after A starts failing (no repeated
    stale-snapshot append), and A's BurstPenstock must not be debited for
    failed ticks.
    """
    from incorporator.tideweaver import BurstPenstock, HardLock

    strong_refs: List[_A] = []
    a_ticks = {"count": 0}

    async def fake(current: Current) -> None:
        if current.name == "a":
            a_ticks["count"] += 1
            if a_ticks["count"] == 1:
                inst = _A(inc_code="a-1")
                strong_refs.append(inst)
                _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]
            else:
                raise RuntimeError("a is down")
        # "b" tick body is a no-op fire counter handled outside.

    a = Stream(name="a", cls=_A, interval=0.05, incorp_params={}, on_error="isolate")
    b = Stream(name="b", cls=_B, interval=0.05, incorp_params={})
    edge_flow = FlowControl(gate=HardLock(), penstock=BurstPenstock(rate_per_sec=100.0, burst=5))
    ws = Watershed(
        window=_short_window(0.6),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=edge_flow)],
    )
    b_fires: List[str] = []

    async def dispatch(current: Current) -> None:
        if current.name == "b":
            b_fires.append(current.name)
            return
        await fake(current)

    tw = Tideweaver(ws, tick_factory=dispatch, pass_interval=0.03)
    await _collect_tides(tw)

    assert a_ticks["count"] > 1, "A must have ticked more than once (including failures)"
    assert len(b_fires) == 1, f"B (HardLock) must fire exactly once on A's single success; got {len(b_fires)}"

    edge_state = tw._edge_state[("a", "b")]
    assert len(edge_state.waves) == 1, f"reservoir must hold exactly A's one genuine wave; got {len(edge_state.waves)}"
    assert edge_state.overflow_count == 0, "no stale re-appends means no overflow"

    # BurstPenstock must not be debited by any of A's failed ticks: only the
    # single successful tick may consume a token (bucket starts at burst=5).
    assert edge_state.flow_state.bucket_tokens is not None
    assert edge_state.flow_state.bucket_tokens >= 4.0, (
        f"failed ticks must not debit the penstock; got bucket_tokens={edge_state.flow_state.bucket_tokens}"
    )

    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")


@pytest.mark.asyncio
async def test_diamond_tail_consumes_only_healthy_upstream_waves() -> None:
    """Diamond with one failing + one healthy middle: tail's edge reservoirs only ever hold fresh healthy waves."""
    from incorporator.tideweaver import HardLock

    healthy_refs: List[_B] = []
    failing_ticks = {"count": 0}

    async def dispatch(current: Current) -> None:
        if current.name == "head":
            return
        if current.name == "failing":
            failing_ticks["count"] += 1
            if failing_ticks["count"] == 1:
                inst = _C(inc_code="failing-1")
                _C._tideweaver_snapshot = [inst]  # type: ignore[attr-defined]
            else:
                raise RuntimeError("failing is down")
            return
        if current.name == "healthy":
            inst = _B(inc_code=f"healthy-{len(healthy_refs)}")
            healthy_refs.append(inst)
            _B._tideweaver_snapshot = list(healthy_refs)  # type: ignore[attr-defined]
            return
        # tail: no-op.

    head = Stream(name="head", cls=_A, interval=0.05, incorp_params={})
    failing = Stream(name="failing", cls=_C, interval=0.05, incorp_params={}, on_error="isolate")
    healthy = Stream(name="healthy", cls=_B, interval=0.05, incorp_params={})
    tail = Stream(name="tail", cls=_D, interval=0.05, incorp_params={})
    ws = Watershed.diamond(
        window=_short_window(0.6),
        head=head,
        middle=[failing, healthy],
        tail=tail,
        gate_mode="hard",
    )
    tw = Tideweaver(ws, tick_factory=dispatch, pass_interval=0.03)
    await _collect_tides(tw)

    assert failing_ticks["count"] > 1, "failing current must have ticked more than once"

    failing_edge = tw._edge_state[("failing", "tail")]
    healthy_edge = tw._edge_state[("healthy", "tail")]

    assert len(failing_edge.waves) == 1, (
        f"failing edge reservoir must hold only its one genuine wave; got {len(failing_edge.waves)}"
    )
    assert failing_edge.overflow_count == 0, "failed ticks must never displace a wave into overflow"
    assert len(healthy_edge.waves) >= 1, "healthy edge must have delivered at least one fresh wave"

    if "_tideweaver_snapshot" in _C.__dict__:
        delattr(_C, "_tideweaver_snapshot")
    if "_tideweaver_snapshot" in _B.__dict__:
        delattr(_B, "_tideweaver_snapshot")


@pytest.mark.asyncio
async def test_drain_cancel_sets_failed_flag_and_advertises_no_wave() -> None:
    """A tick cancelled mid-flight during drain sets the failed flag, propagates CancelledError, and advertises no wave."""
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
    state = tw._state["a"]
    assert state.last_wave_at is None, "a cancelled tick must never set last_wave_at"
    assert state.last_failed_at is not None, "a cancelled tick must set last_failed_at"


# ---------------------------------------------------------------------------
# D5-02: snapshot-wins consumed-watermark (mid-tick upstream wave delivery)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mid_tick_wave_delivery_hard_chain() -> None:
    """A(HardLock->B): a wave A emits while B's tick is in flight is not lost.

    B's first tick starts after A's first wave.  While B is still running,
    A emits a SECOND wave, then A is frozen (raises on every subsequent
    tick, advertising no further waves) -- isolating the effect to
    exactly the two waves involved.  Under the old "latest-wins"
    watermark, B's first-tick ``finally`` block would overwrite
    ``_last_consumed`` with A's CURRENT ``last_wave_at`` (the second
    wave, stamped mid-tick) even though B's tick body never read it;
    since A never emits a third wave, HardLock then blocks B forever
    (``last_consumed >= up_last_wave_at`` holds permanently) and the
    second wave is never delivered.  Under snapshot-wins, only the
    pre-tick (first-wave) snapshot is written, so the gate stays open
    for the second wave and B re-fires next pass, reading it via the
    edge reservoir's newest entry.

    MUST FAIL pre-fix: pre-fix, B fires exactly once and the second wave
    is never delivered.
    """
    from incorporator.tideweaver import HardLock

    a_ticks = {"count": 0}
    b_started = asyncio.Event()
    b_may_finish = asyncio.Event()
    b_fires: List[int] = []

    async def dispatch(current: Current) -> None:
        if current.name == "a":
            a_ticks["count"] += 1
            if a_ticks["count"] <= 2:
                inst = _A(inc_code=f"a-{a_ticks['count']}")
                _A._tideweaver_snapshot = [inst]  # type: ignore[attr-defined]
                return
            # Freeze A after its second wave -- isolates the test to the
            # exact mid-tick-second-wave scenario, no further waves ever.
            raise RuntimeError("a stops after wave 2")
        # b
        b_fires.append(a_ticks["count"])
        if len(b_fires) == 1:
            b_started.set()
            # Block B's first tick open long enough for A to emit a second
            # wave WHILE B is still in flight.
            await asyncio.wait_for(b_may_finish.wait(), timeout=2.0)

    a = Stream(name="a", cls=_A, interval=0.05, incorp_params={}, on_error="isolate")
    b = Stream(name="b", cls=_B, interval=0.05, incorp_params={})
    edge_flow = FlowControl(gate=HardLock())
    ws = Watershed(
        window=_short_window(1.0),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=edge_flow)],
    )
    tw = Tideweaver(ws, tick_factory=dispatch, pass_interval=0.03)

    async def _release_b_after_second_a_wave() -> None:
        await b_started.wait()
        start_count = a_ticks["count"]
        while a_ticks["count"] <= start_count:
            await asyncio.sleep(0.02)
        b_may_finish.set()

    releaser = asyncio.create_task(_release_b_after_second_a_wave())
    await _collect_tides(tw)
    releaser.cancel()
    try:
        await releaser
    except asyncio.CancelledError:
        pass

    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")

    assert len(b_fires) >= 2, f"B must re-fire after the mid-tick upstream wave; got fires={b_fires}"
    # The re-fire must have seen the SECOND wave (not be stuck reading the first).
    assert b_fires[1] > b_fires[0], f"B's re-fire must observe a newer A wave count than its first fire; got {b_fires}"


@pytest.mark.asyncio
async def test_no_double_fire_when_nothing_new_hard_chain() -> None:
    """A(HardLock->B): when A emits exactly one wave for the whole window, B fires exactly once.

    Regression guard for snapshot-wins: with only ONE upstream wave ever
    emitted (A's ``interval`` exceeds the window so it never re-ticks),
    B's gate must stay closed on an equal watermark after its first
    consumption — it must not reopen spuriously just because the
    snapshot-wins change deleted the post-tick ``latest`` overwrite.
    """
    from incorporator.tideweaver import HardLock

    a_ticks = {"count": 0}
    b_fires: List[str] = []

    async def dispatch(current: Current) -> None:
        if current.name == "a":
            a_ticks["count"] += 1
            inst = _A(inc_code="a-1")
            _A._tideweaver_snapshot = [inst]  # type: ignore[attr-defined]
            return
        b_fires.append(current.name)

    # A's interval exceeds the whole window, so it only ever fires once.
    a = Stream(name="a", cls=_A, interval=5.0, incorp_params={})
    b = Stream(name="b", cls=_B, interval=0.05, incorp_params={})
    edge_flow = FlowControl(gate=HardLock())
    ws = Watershed(
        window=_short_window(0.5),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=edge_flow)],
    )
    tw = Tideweaver(ws, tick_factory=dispatch, pass_interval=0.03)
    await _collect_tides(tw)

    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")

    assert a_ticks["count"] == 1, "A must tick exactly once given its long interval"
    assert len(b_fires) == 1, f"B must fire exactly once when A emits no new wave after being consumed; got {b_fires}"


@pytest.mark.asyncio
async def test_d5_01_gate_tests_stay_green_with_snapshot_wins() -> None:
    """Confirms the D5-01 failure-gate contract (not_tick_raised) is untouched by the snapshot-wins change.

    Re-runs the essential shape of ``test_failed_isolated_tick_does_not_advertise_wave_hard_chain``:
    a failing upstream must never advertise a wave regardless of which
    ``_last_consumed`` value wins on success.
    """
    from incorporator.tideweaver import BurstPenstock, HardLock

    strong_refs: List[_A] = []
    a_ticks = {"count": 0}

    async def fake(current: Current) -> None:
        if current.name == "a":
            a_ticks["count"] += 1
            if a_ticks["count"] == 1:
                inst = _A(inc_code="a-1")
                strong_refs.append(inst)
                _A._tideweaver_snapshot = list(strong_refs)  # type: ignore[attr-defined]
            else:
                raise RuntimeError("a is down")

    a = Stream(name="a", cls=_A, interval=0.05, incorp_params={}, on_error="isolate")
    b = Stream(name="b", cls=_B, interval=0.05, incorp_params={})
    edge_flow = FlowControl(gate=HardLock(), penstock=BurstPenstock(rate_per_sec=100.0, burst=5))
    ws = Watershed(
        window=_short_window(0.6),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=edge_flow)],
    )
    b_fires: List[str] = []

    async def dispatch(current: Current) -> None:
        if current.name == "b":
            b_fires.append(current.name)
            return
        await fake(current)

    tw = Tideweaver(ws, tick_factory=dispatch, pass_interval=0.03)
    await _collect_tides(tw)

    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")

    assert a_ticks["count"] > 1, "A must have ticked more than once (including failures)"
    assert len(b_fires) == 1, f"B (HardLock) must fire exactly once on A's single success; got {len(b_fires)}"


# ---------------------------------------------------------------------------
# D5-02 canal/L4: penstock debit cadence, watermark integrity, reservoir sanity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_canal_penstock_debited_once_per_fire_not_per_gate_check() -> None:
    """(T-C1) BurstPenstock tokens are debited exactly once per B FIRE, never once per gate check.

    Drives a mid-tick re-fire sequence (like T1) and asserts
    ``bucket_tokens`` only ever drops by the number of times B actually
    fired, not by the (larger) number of passes the scheduler evaluated
    the gate.
    """
    from incorporator.tideweaver import BurstPenstock, HardLock

    a_ticks = {"count": 0}
    b_started = asyncio.Event()
    b_may_finish = asyncio.Event()
    b_fires: List[int] = []

    async def dispatch(current: Current) -> None:
        if current.name == "a":
            a_ticks["count"] += 1
            inst = _A(inc_code=f"a-{a_ticks['count']}")
            _A._tideweaver_snapshot = [inst]  # type: ignore[attr-defined]
            return
        b_fires.append(a_ticks["count"])
        if len(b_fires) == 1:
            b_started.set()
            await asyncio.wait_for(b_may_finish.wait(), timeout=2.0)

    a = Stream(name="a", cls=_A, interval=0.05, incorp_params={})
    b = Stream(name="b", cls=_B, interval=0.05, incorp_params={})
    edge_flow = FlowControl(gate=HardLock(), penstock=BurstPenstock(rate_per_sec=1000.0, burst=10))
    ws = Watershed(
        window=_short_window(1.2),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=edge_flow)],
    )
    tw = Tideweaver(ws, tick_factory=dispatch, pass_interval=0.03)

    async def _release_b_after_second_a_wave() -> None:
        await b_started.wait()
        start_count = a_ticks["count"]
        while a_ticks["count"] <= start_count:
            await asyncio.sleep(0.02)
        b_may_finish.set()

    releaser = asyncio.create_task(_release_b_after_second_a_wave())
    await _collect_tides(tw)
    releaser.cancel()
    try:
        await releaser
    except asyncio.CancelledError:
        pass

    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")

    edge_state = tw._edge_state[("a", "b")]
    assert edge_state.flow_state.bucket_tokens is not None
    debited = 10.0 - edge_state.flow_state.bucket_tokens
    # Refill is near-instant at rate_per_sec=1000, so tolerate a small
    # positive slack from refill between debits without masking an
    # over-debit bug (over-debiting would show up as far more than
    # len(b_fires) tokens consumed).
    assert debited <= len(b_fires) + 0.5, (
        f"penstock must debit at most once per B fire ({len(b_fires)} fires); "
        f"observed debited={debited} tokens from bucket"
    )


@pytest.mark.asyncio
async def test_canal_watermark_reflects_pretick_snapshot_of_latest_fire() -> None:
    """(T-C2) After a re-fire, _last_consumed[edge] equals the PRE-TICK snapshot of that fire.

    Never an in-between value picked up mid-tick from the upstream.  A is
    frozen (via ``on_error='isolate'`` + a raise) right after producing
    its SECOND wave, so ``a_state.last_wave_at`` at assertion time is
    guaranteed to equal the exact wave B's re-fire snapshot must have
    consumed -- no ambiguity from A continuing to tick after the sample.
    """
    from incorporator.tideweaver import HardLock

    a_ticks = {"count": 0}
    b_started = asyncio.Event()
    b_may_finish = asyncio.Event()
    b_fires: List[int] = []

    async def dispatch(current: Current) -> None:
        if current.name == "a":
            a_ticks["count"] += 1
            if a_ticks["count"] <= 2:
                inst = _A(inc_code=f"a-{a_ticks['count']}")
                _A._tideweaver_snapshot = [inst]  # type: ignore[attr-defined]
                return
            # Freeze A after its second wave so last_wave_at stops moving.
            raise RuntimeError("a stops after wave 2")
        b_fires.append(a_ticks["count"])
        if len(b_fires) == 1:
            b_started.set()
            await asyncio.wait_for(b_may_finish.wait(), timeout=2.0)

    a = Stream(name="a", cls=_A, interval=0.05, incorp_params={}, on_error="isolate")
    b = Stream(name="b", cls=_B, interval=0.05, incorp_params={})
    edge_flow = FlowControl(gate=HardLock())
    ws = Watershed(
        window=_short_window(1.2),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=edge_flow)],
    )
    tw = Tideweaver(ws, tick_factory=dispatch, pass_interval=0.03)

    async def _release_b_after_second_a_wave() -> None:
        await b_started.wait()
        start_count = a_ticks["count"]
        while a_ticks["count"] <= start_count:
            await asyncio.sleep(0.02)
        b_may_finish.set()

    releaser = asyncio.create_task(_release_b_after_second_a_wave())
    await _collect_tides(tw)
    releaser.cancel()
    try:
        await releaser
    except asyncio.CancelledError:
        pass

    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")

    assert len(b_fires) >= 2, "test requires B to have re-fired at least once"
    watermark = tw._last_consumed[("a", "b")]
    a_state = tw._state["a"]
    # The watermark must be a real recorded wave time, never None.
    assert watermark is not None
    # A is frozen after wave 2 (further ticks raise and never advertise a
    # wave), so a_state.last_wave_at is pinned to wave 2's timestamp -- the
    # exact pre-tick snapshot B's re-fire must have consumed.
    assert watermark == a_state.last_wave_at, (
        f"watermark must reflect the pre-tick snapshot of the latest fire; "
        f"got watermark={watermark}, a.last_wave_at={a_state.last_wave_at}"
    )


@pytest.mark.asyncio
async def test_canal_backpressure_reservoir_sanity_no_spurious_rejects() -> None:
    """(T-C3) Reservoir fullness only changes via wave push + displacement; re-fires reading waves[-1] never drain it.

    Also asserts a plain HardLock+NullPenstock-free config produces no
    spurious ``penstock_limited`` canal rejects from the extra re-fire
    passes introduced by snapshot-wins.
    """
    from incorporator.tideweaver import HardLock, Reservoir

    a_ticks = {"count": 0}
    b_started = asyncio.Event()
    b_may_finish = asyncio.Event()
    b_fires: List[int] = []

    async def dispatch(current: Current) -> None:
        if current.name == "a":
            a_ticks["count"] += 1
            inst = _A(inc_code=f"a-{a_ticks['count']}")
            _A._tideweaver_snapshot = [inst]  # type: ignore[attr-defined]
            return
        b_fires.append(a_ticks["count"])
        if len(b_fires) == 1:
            b_started.set()
            await asyncio.wait_for(b_may_finish.wait(), timeout=2.0)

    a = Stream(name="a", cls=_A, interval=0.05, incorp_params={})
    b = Stream(name="b", cls=_B, interval=0.05, incorp_params={})
    edge_flow = FlowControl(gate=HardLock(), reservoir=Reservoir(depth=3))
    ws = Watershed(
        window=_short_window(1.2),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=edge_flow)],
    )
    tw = Tideweaver(ws, tick_factory=dispatch, pass_interval=0.03)

    reservoir_lens: List[int] = []

    async def _release_and_sample() -> None:
        await b_started.wait()
        start_count = a_ticks["count"]
        while a_ticks["count"] <= start_count:
            reservoir_lens.append(len(tw._edge_state[("a", "b")].waves))
            await asyncio.sleep(0.02)
        b_may_finish.set()

    releaser = asyncio.create_task(_release_and_sample())
    await _collect_tides(tw)
    releaser.cancel()
    try:
        await releaser
    except asyncio.CancelledError:
        pass

    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")

    edge_state = tw._edge_state[("a", "b")]
    # Depth=3 caps reservoir length; anything beyond that is displaced into
    # overflow, never drained by a re-fire's waves[-1] read.
    assert len(edge_state.waves) == min(a_ticks["count"], 3), (
        f"reservoir length must equal min(pushes, depth) (no drain-on-read); "
        f"got waves={len(edge_state.waves)}, a_ticks={a_ticks['count']}"
    )
    assert edge_state.overflow_count == max(0, a_ticks["count"] - 3), (
        "reservoir depth=3 overflow must only reflect pushes beyond depth, not re-fire reads"
    )
    penstock_rejects = [r for r in tw.rejects if r.error_kind == "PenstockLimited"]
    assert penstock_rejects == [], (
        f"a HardLock+NullPenstock-free config must never produce PenstockLimited rejects; got {penstock_rejects}"
    )


# ---------------------------------------------------------------------------
# D5-03: Tideweaver instance is truly reusable across sequential run() calls
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sequential_runs_reset_full_scheduler_state() -> None:
    """(T4) Two sequential run() calls on the same Tideweaver: run 2 is a fresh session.

    Asserts: tide numbering restarts at 1, phase_offset_sec is honored
    again (first tick of run 2 is gated by PHASE_OFFSET, not fired
    immediately), reservoirs/consumption-watermarks start empty (no
    stale run-1 waves delivered to run 2's first tick), and canal
    rejects/dedup-ids/due-heap do not leak across runs.

    MUST FAIL pre-fix: pre-fix, only ``_client_pool``/``_run_started_at``
    reset, so ``_tide_number`` keeps counting up, ``_last_consumed`` and
    ``_edge_state`` reservoirs from run 1 leak into run 2, and
    ``phase_offset_sec`` is silently ignored on run 2 (stale
    ``last_tick_started``).
    """
    from incorporator.tideweaver import HardLock

    fires: List[str] = []

    async def dispatch(current: Current) -> None:
        fires.append(current.name)
        if current.name == "a":
            inst = _A(inc_code=f"a-{len(fires)}")
            _A._tideweaver_snapshot = [inst]  # type: ignore[attr-defined]

    a = Stream(name="a", cls=_A, interval=0.05, phase_offset_sec=0.15)
    b = Stream(name="b", cls=_B, interval=0.05)
    edge_flow = FlowControl(gate=HardLock())
    ws = Watershed(
        window=_short_window(0.6),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=edge_flow)],
    )
    tw = Tideweaver(ws, tick_factory=dispatch, pass_interval=0.03)

    tides_1 = [t async for t in tw.run()]
    assert tides_1[0].tide_number == 1
    assert tw._tide_number == len(tides_1)
    assert tw._last_consumed.get(("a", "b")) is not None, "run 1 must have produced a consumption watermark"
    assert len(tw._edge_state[("a", "b")].waves) >= 1, "run 1 must have pushed at least one wave"

    # Simulate a run-1 reject that was already routed to the session log
    # (the dedup set that ``_reset_run_state`` must clear so run 2's
    # equivalent reject isn't silently suppressed).
    tw._routed_reject_ids.add(id(object()))
    assert tw._routed_reject_ids, "precondition: dedup set must be non-empty before reset"

    fires.clear()
    if "_tideweaver_snapshot" in _A.__dict__:
        delattr(_A, "_tideweaver_snapshot")

    # Real callers reusing a Tideweaver instance supply a fresh window per
    # run; the fixed absolute window from construction would otherwise
    # already be closed by the time run 2 starts.
    tw.watershed.window = _short_window(0.6)

    tides_2 = [t async for t in tw.run()]

    assert tw._routed_reject_ids == set(), "run 2 must clear the run-1 routed-reject dedup set"

    # Tide numbering restarts.
    assert tides_2[0].tide_number == 1, f"run 2 must restart tide numbering at 1; got {tides_2[0].tide_number}"

    # phase_offset_sec is honored again: 'a' must not appear in fires until
    # after the phase offset has elapsed, i.e. some early tides show it
    # skipped for PHASE_OFFSET before it first fires.
    first_a_tide_index = next(i for i, t in enumerate(tides_2) if "a" in t.fired)
    assert first_a_tide_index > 0, "run 2 must re-honor phase_offset_sec (a shouldn't fire on the very first pass)"
    phase_skips = [
        reason
        for t in tides_2[:first_a_tide_index]
        for name, reason in t.skipped
        if name == "a" and reason.value == "phase_offset"
    ]
    assert phase_skips, "run 2 must re-apply PHASE_OFFSET gating on 'a' (stale last_tick_started must not leak)"

    # No stale run-1 waves delivered to run 2's first tick of 'b': the
    # reservoir must have started empty and grown only from run-2 waves.
    edge_state = tw._edge_state[("a", "b")]
    assert len(edge_state.waves) <= len(tides_2), "reservoir must not carry over run-1 waves into run 2"
    assert len(fires) > 0, "run 2 must have actually ticked currents"


def test_watershed_module_docstring_does_not_overclaim_serialisability() -> None:
    """D8-03: the module docstring must not call Watershed 'serialisable' — model_dump_json() raises.

    ``Current.cls`` holds a live class object, so JSON-mode dumping fails
    and a python-mode dump-then-validate round-trip downgrades typed
    currents to bare ``Current``. Guards against the docstring drifting
    back to the over-claim.
    """
    import incorporator.tideweaver.watershed as watershed_module

    doc = watershed_module.__doc__
    assert doc is not None
    assert "serialisable description" not in doc
    assert "declarative plan" in doc
    assert "not itself JSON-dumpable" in doc
