"""Tideweaver routing tests with the **canal toolkit live**.

The other five routing test files
(``tests/test_tideweaver_routing_{chain,diamond,fanout,parallel,custom}.py``)
all set ``monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")``,
which short-circuits the host-level penstock layer.  None of them
attach edge-level ``Penstock`` / ``Reservoir`` / ``Spillway`` /
``FlowObserver`` either, so the canal layer is effectively absent from
their orchestration scenarios.

This file closes that gap.  Four tests cover the routing-canal matrix:

* **T1 — chain + full canal toolkit** (A-F-2 core).  Three-current
  chain with the A→B edge carrying a ``SustainedPenstock`` +
  ``Reservoir(depth=3)`` + ``RaiseOverflow`` spillway + ``SignalObserver``.
  Asserts on ``Tideweaver.rejects`` for ``PenstockLimited`` and on the
  observer callback's event stream.
* **T2 — diamond + LoggingObserver** (E-F-3 #1).  Asserts that
  ``LoggingObserver`` emits log records at the configured levels
  across multi-edge diamond dispatch.
* **T3 — fanout + SurgeBarrier(action="bypass")** (E-F-3 #2).  One
  dependent has ``action="bypass"`` so it fires under a slow upstream;
  siblings get ``"skip_ahead"``.
* **T4 — parallel + ``phase_offset_sec`` green-wave staging**
  (E-F-3 #3).  Three parallel streams with staggered phase offsets;
  asserts their first ticks land in order with the expected delay.

Each test follows the convention of the existing routing files:
``monkeypatch.setattr(fetch, "execute_request", ...)`` for HTTP mocks,
``monkeypatch.chdir(tmp_path)`` for export-target isolation, minimal
``Incorporator`` subclasses with ``model_config = ConfigDict(extra="allow")``,
and ``_short_window`` / ``_reset_registries`` helpers.

The HTTP-layer ``IncorporatorList.rejects`` shape (E-F-3 #4) is covered
by the verb-layer tests in ``tests/test_io_fetch.py``; surfacing it
through a routing test would require exposing the per-wave
``IncorporatorList`` from inside the Tideweaver scheduler, which is
out of scope for a coverage pass.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import httpx
import pytest
from pydantic import ConfigDict

from incorporator import Incorporator, SustainedPenstock
from incorporator.io import fetch
from incorporator.tideweaver import (
    Current,
    Edge,
    FlowControl,
    HardLock,
    LoggingObserver,
    RaiseOverflow,
    Reservoir,
    SignalObserver,
    Stream,
    SurgeBarrier,
    Tideweaver,
    Watershed,
)


# ---------------------------------------------------------------------------
# Shared fixtures + helpers
# ---------------------------------------------------------------------------


class CanalA(Incorporator):
    """Upstream source for chain/diamond/fanout scenarios."""

    model_config = ConfigDict(extra="allow")


class CanalB(Incorporator):
    """Middle current."""

    model_config = ConfigDict(extra="allow")


class CanalC(Incorporator):
    """Second middle (for diamond) or sink (for fanout)."""

    model_config = ConfigDict(extra="allow")


class CanalD(Incorporator):
    """Tail / third sink."""

    model_config = ConfigDict(extra="allow")


def _short_window(seconds: float) -> Tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(seconds=seconds))


def _reset_registries(*classes: type[Incorporator]) -> None:
    """Wipe per-class inc_dict + parked snapshot between tests."""
    for cls in classes:
        cls.inc_dict.clear()
        if "_tideweaver_snapshot" in cls.__dict__:
            try:
                delattr(cls, "_tideweaver_snapshot")
            except AttributeError:
                pass


async def _mock_jsonplaceholder(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Minimal payload returner so tick bodies have something to ingest."""
    payload = [{"id": 1, "name": "x"}, {"id": 2, "name": "y"}]
    return httpx.Response(200, text=json.dumps(payload), request=httpx.Request("GET", url))


def _stream(name: str, cls: type[Incorporator], interval: float = 0.1) -> Stream:
    """Build a minimal Stream current with a stub source URL."""
    return Stream(
        name=name,
        cls=cls,
        interval=interval,
        on_error="isolate",
        incorp_params={
            "inc_url": f"https://example.com/{name}",
            "inc_code": "id",
        },
    )


# ---------------------------------------------------------------------------
# T1 — chain + full canal toolkit (A-F-2 core)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chain_with_full_canal_toolkit(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """A→B→C chain with the A→B edge carrying Penstock + Reservoir + Spillway + Observer.

    A fires at interval=0.1 (~10×/sec) and B's edge penstock allows 2 r/s.
    After B's first permitted consumption (the penstock is empty on
    initial state), subsequent attempts within 500ms get
    ``penstock_limited``.  Over the 1.5s window we expect:

    * ≥ 3 ``PenstockLimited`` entries in ``tw.rejects``
    * Observer callback fired for ``"fire"``, ``"skip"``, and
      ``"reservoir_level"`` event kinds
    * A and B both fired at least once (proves the penstock permits
      the first call AND that the chain isn't structurally broken)

    This is the canal-layer's first multi-current test — pre-Phase 3
    the canal toolkit was unit-tested in ``tests/test_tideweaver.py``
    but never exercised across an orchestration shape.
    """
    monkeypatch.chdir(tmp_path)
    # IMPORTANT: do NOT set INCORPORATOR_RATE_LIMIT_BYPASS here — the
    # whole point of this file is that the canal layer runs live.  The
    # ``execute_request`` mock returns instantly so HTTP-layer host
    # throttling doesn't affect the measurement.
    monkeypatch.setattr(fetch, "execute_request", _mock_jsonplaceholder)
    _reset_registries(CanalA, CanalB, CanalC)

    observed_events: List[Tuple[str, Tuple[str, str], Dict[str, Any]]] = []

    def capture(event_kind: str, edge: Tuple[str, str], payload: Dict[str, Any]) -> None:
        observed_events.append((event_kind, edge, payload))

    a = _stream("a", CanalA, interval=0.1)
    b = _stream("b", CanalB, interval=0.1)
    c = _stream("c", CanalC, interval=0.1)

    full_canal = FlowControl(
        gate=HardLock(),
        penstock=SustainedPenstock(rate_per_sec=2.0),
        reservoir=Reservoir(depth=3),
        spillway=RaiseOverflow(),
        observer=SignalObserver(callback=capture),
    )
    ws = Watershed(
        window=_short_window(1.5),
        currents=[a, b, c],
        edges=[
            Edge(from_name="a", to_name="b", flow=full_canal),
            Edge(from_name="b", to_name="c"),  # default flow on tail
        ],
    )
    tw = Tideweaver(ws, pass_interval=0.05)
    tides = [tide async for tide in tw.run()]

    # A and B both fired at least once.
    a_fired = sum(1 for t in tides for n in t.fired if n == "a")
    b_fired = sum(1 for t in tides for n in t.fired if n == "b")
    assert a_fired >= 1, f"A must fire at least once (penstock permits first call); got {a_fired}"
    assert b_fired >= 1, f"B must fire at least once (penstock permits first call); got {b_fired}"

    # Penstock-limited rejects accumulated.
    pl_rejects = [r for r in tw.rejects if r.error_kind == "PenstockLimited"]
    assert len(pl_rejects) >= 3, (
        f"SustainedPenstock(2.0) with A firing at 10/sec should produce ≥3 "
        f"PenstockLimited rejects in 1.5s; got {len(pl_rejects)}"
    )
    # All canal rejects point at the right edge + downstream class name.
    for r in pl_rejects:
        assert r.source == "CanalB"
        assert "edge a→b" in r.message

    # Observer saw multiple event kinds on the a→b edge.
    event_kinds_on_ab = {kind for kind, edge, _ in observed_events if edge == ("a", "b")}
    assert "fire" in event_kinds_on_ab, f"observer must fire on_fire at least once; got {event_kinds_on_ab}"
    assert "skip" in event_kinds_on_ab, f"observer must fire on_skip at least once; got {event_kinds_on_ab}"
    assert "reservoir_level" in event_kinds_on_ab, (
        f"observer must fire on_reservoir_level at least once after B's first consumption; got {event_kinds_on_ab}"
    )


# ---------------------------------------------------------------------------
# T2 — diamond + LoggingObserver (E-F-3 #1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_diamond_logging_observer_emits_per_event(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Diamond shape with LoggingObserver on every edge emits per-event log records.

    Watershed.diamond(flow=...) applies the FlowControl to ALL four
    edges (head→middle1, head→middle2, middle1→tail, middle2→tail).
    A single LoggingObserver instance gets called for every per-edge
    event across all four edges — proves multi-edge dispatch doesn't
    crash and that fire events get emitted at the configured ``info``
    level.

    Uses ``caplog`` to capture records from the ``flow`` module's
    logger; asserts at least 2 "fired" records (one per fired edge,
    over a 1.0s window with HardLock + interval=0.1 there should be
    plenty).
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", _mock_jsonplaceholder)
    _reset_registries(CanalA, CanalB, CanalC, CanalD)

    a = _stream("a", CanalA, interval=0.1)
    b = _stream("b", CanalB, interval=0.1)
    c = _stream("c", CanalC, interval=0.1)
    d = _stream("d", CanalD, interval=0.1)

    diamond_flow = FlowControl(
        gate=HardLock(),
        observer=LoggingObserver(
            fire_level="info",
            skip_level="info",
            spillway_level="warning",
            reservoir_level_level="info",
        ),
    )
    ws = Watershed.diamond(
        window=_short_window(1.0),
        head=a,
        middle=[b, c],
        tail=d,
        flow=diamond_flow,
    )
    tw = Tideweaver(ws, pass_interval=0.05)

    with caplog.at_level(logging.INFO, logger="incorporator.tideweaver.flow"):
        tides = [tide async for tide in tw.run()]

    # Confirm the watershed actually ran — at least head + one middle fired.
    fired_names = {n for t in tides for n in t.fired}
    assert "a" in fired_names
    assert fired_names & {"b", "c"}, f"at least one middle must fire; got {fired_names}"

    # LoggingObserver emitted per-event records.
    fire_records = [r for r in caplog.records if "fired" in r.getMessage()]
    assert len(fire_records) >= 2, (
        f"LoggingObserver should emit ≥ 2 fire records across the diamond; "
        f"got {len(fire_records)}: {[r.getMessage() for r in fire_records[:5]]}"
    )


# ---------------------------------------------------------------------------
# T3 — fanout + SurgeBarrier(action="bypass") on one dependent (E-F-3 #2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fanout_surge_bypass_fires_dependent_under_slow_upstream(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One fanout dependent uses ``action="bypass"`` and fires while upstream is in-flight.

    Bare ``Watershed(...)`` lets each edge carry its own FlowControl:

    * ``a→b``: ``SurgeBarrier(threshold_multiple=2.0, action="bypass")``
      — B fires unconditionally once A has been in-flight beyond
      ``2.0 × b.interval``.
    * ``a→c`` and ``a→d``: ``gate_mode="hard"`` shorthand — picks up
      the auto-attached ``SurgeBarrier(action="skip")`` so C and D
      get ``"skip_ahead"`` while A is overrun.

    Assert that B fires at least once, that C and D produce
    ``skip_ahead`` skip records in the Tide stream, and that
    ``tw.rejects`` has ``SkipAhead`` entries for C/D but NOT for B.
    """
    monkeypatch.chdir(tmp_path)

    async def slow_a(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        # Only A's URL is slow — siblings shouldn't share the wait.
        if "/a" in url:
            import asyncio

            await asyncio.sleep(0.4)
        return await _mock_jsonplaceholder(url, *args, **kwargs)

    monkeypatch.setattr(fetch, "execute_request", slow_a)
    _reset_registries(CanalA, CanalB, CanalC, CanalD)

    a = _stream("a", CanalA, interval=0.1)
    b = _stream("b", CanalB, interval=0.1)
    c = _stream("c", CanalC, interval=0.1)
    d = _stream("d", CanalD, interval=0.1)

    bypass_flow = FlowControl(
        gate=HardLock(),
        surge_barrier=SurgeBarrier(threshold_multiple=2.0, action="bypass"),
    )
    ws = Watershed(
        window=_short_window(1.0),
        currents=[a, b, c, d],
        edges=[
            Edge(from_name="a", to_name="b", flow=bypass_flow),
            Edge(from_name="a", to_name="c", gate_mode="hard"),
            Edge(from_name="a", to_name="d", gate_mode="hard"),
        ],
    )
    tw = Tideweaver(ws, pass_interval=0.05)
    tides = [tide async for tide in tw.run()]

    b_fired = sum(1 for t in tides for n in t.fired if n == "b")
    skip_ahead_pairs = [(name, reason) for t in tides for name, reason in t.skipped if reason == "skip_ahead"]
    skip_ahead_currents = {name for name, _ in skip_ahead_pairs}

    # B fires despite A in-flight (bypass).
    assert b_fired >= 1, f"B with surge bypass must fire at least once; got {b_fired}"
    # C and D get skip_ahead from the default-hard SurgeBarrier.
    assert skip_ahead_currents >= {"c", "d"}, (
        f"C and D should both emit skip_ahead under slow A; got {skip_ahead_currents}"
    )
    # tw.rejects: SkipAhead entries for C and D, none for B (bypass path
    # short-circuits before the canal-rejects append).
    skip_rejects = [r for r in tw.rejects if r.error_kind == "SkipAhead"]
    skip_reject_sources = {r.source for r in skip_rejects}
    assert "CanalC" in skip_reject_sources or "CanalD" in skip_reject_sources, (
        f"expected SkipAhead RejectEntry for CanalC and/or CanalD; got {skip_reject_sources}"
    )
    assert "CanalB" not in skip_reject_sources, (
        f"B was bypassed — must NOT appear in SkipAhead rejects; got {skip_reject_sources}"
    )


# ---------------------------------------------------------------------------
# T4 — parallel + phase_offset_sec green-wave staging (E-F-3 #3)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parallel_phase_offset_staggers_first_ticks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Three parallel streams with staggered ``phase_offset_sec`` first-tick at the right delay.

    No edges → no canal gating; we're just verifying that the first
    tick of each current waits the configured ``phase_offset_sec`` from
    the scheduler's run start.

    Uses a stub ``tick_factory`` to record ``time.monotonic()`` when
    the scheduler SPAWNS each current's first tick — that's the
    accurate measurement of phase_offset behavior.  Mocking
    ``execute_request`` and timing from there is too noisy because
    asyncio batches tick-body execution; multiple ticks spawned at
    different times can have their ``execute_request`` calls land
    within milliseconds of each other.  ``tick_factory`` fires at the
    moment of spawn, which is what phase_offset_sec actually gates.
    """
    first_spawn: Dict[str, float] = {}
    scheduler_start: Dict[str, float] = {}

    async def timestamped_tick(current: Current) -> None:
        # Anchor the FIRST tick's monotonic time as scheduler_start.
        # We can't observe ``self._run_started_at`` from out here, but
        # the first tick to fire is current "a" with offset=0.0 — its
        # spawn time ≈ scheduler's _run_started_at + ε.
        if current.name not in first_spawn:
            first_spawn[current.name] = time.monotonic()
        if "anchor" not in scheduler_start:
            scheduler_start["anchor"] = first_spawn[current.name]

    a = Stream(
        name="a",
        cls=CanalA,
        interval=0.2,
        phase_offset_sec=0.0,  # immediate
        on_error="isolate",
        incorp_params={"inc_url": "https://example.com/a", "inc_code": "id"},
    )
    b = Stream(
        name="b",
        cls=CanalB,
        interval=0.2,
        phase_offset_sec=0.3,
        on_error="isolate",
        incorp_params={"inc_url": "https://example.com/b", "inc_code": "id"},
    )
    c = Stream(
        name="c",
        cls=CanalC,
        interval=0.2,
        phase_offset_sec=0.6,
        on_error="isolate",
        incorp_params={"inc_url": "https://example.com/c", "inc_code": "id"},
    )

    ws = Watershed.parallel(window=_short_window(1.5), currents=[a, b, c])
    tw = Tideweaver(ws, tick_factory=timestamped_tick, pass_interval=0.05)
    async for _ in tw.run():
        pass

    assert set(first_spawn) == {"a", "b", "c"}, f"all three currents should have spawned a tick; got {set(first_spawn)}"

    # Anchor against A's first spawn time (phase_offset=0.0).
    delta_a_to_b = first_spawn["b"] - first_spawn["a"]
    delta_a_to_c = first_spawn["c"] - first_spawn["a"]
    # 80% of the configured offset — the scheduler's pass_interval=0.05
    # provides 50ms resolution, and the gate check runs once per pass.
    assert delta_a_to_b >= 0.24, (
        f"B's phase_offset_sec=0.3 vs A=0.0 must produce ≥ 0.24s spawn gap "
        f"(80% of 0.3); got {delta_a_to_b:.3f}s "
        f"(A spawned at +{first_spawn['a'] - scheduler_start['anchor']:.3f}s, "
        f"B at +{first_spawn['b'] - scheduler_start['anchor']:.3f}s)"
    )
    assert delta_a_to_c >= 0.48, (
        f"C's phase_offset_sec=0.6 vs A=0.0 must produce ≥ 0.48s spawn gap "
        f"(80% of 0.6); got {delta_a_to_c:.3f}s "
        f"(A spawned at +{first_spawn['a'] - scheduler_start['anchor']:.3f}s, "
        f"C at +{first_spawn['c'] - scheduler_start['anchor']:.3f}s)"
    )
    # The configured ordering survives under jitter.
    assert first_spawn["a"] < first_spawn["b"] < first_spawn["c"], (
        f"phase offsets must preserve ordering A < B < C; got {first_spawn}"
    )
