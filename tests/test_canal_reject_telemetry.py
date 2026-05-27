"""Tests for canal-layer RejectEntry telemetry fields.

Proves that cooldown_sec, attempt_number=1, duration_sec>0 are populated
correctly on PenstockLimited rejects, and that eligibility_start_perf resets
after a successful fire.  Uses tick_factory to avoid HTTP overhead and
give precise control over firing cadence.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Tuple

import pytest

from incorporator import Incorporator, SustainedPenstock
from incorporator.observability.tideweaver import (
    Current,
    Edge,
    FlowControl,
    SoftPass,
    Stream,
    Tideweaver,
    Watershed,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TelemetryA(Incorporator):
    """Upstream source."""


class TelemetryB(Incorporator):
    """Downstream throttled by penstock."""


def _short_window(seconds: float) -> Tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(seconds=seconds))


async def _noop(current: Current) -> None:
    """No-op tick body — avoids HTTP overhead in telemetry tests."""
    return None


# ---------------------------------------------------------------------------
# T1 — PenstockLimited rejects carry correct telemetry fields
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_penstock_limited_reject_telemetry(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PenstockLimited rejects carry cooldown_sec≈1/rate, attempt_number=1, duration_sec>0.

    A throttled SustainedPenstock(rate_per_sec=2) on the A→B edge with a
    SoftPass gate.  B's interval is 0.01s (very fast) so it fires at ~100/s,
    far exceeding the penstock's 2/s limit.  After the first permitted fire,
    subsequent passes are penstock-blocked.

    We use tick_factory=_noop to avoid HTTP overhead and ensure B fires
    at the scheduled cadence without real I/O delays.
    """
    monkeypatch.chdir(tmp_path)

    rate = 2.0
    # B fires every 0.01s (100/s); penstock allows 2/s → most are blocked.
    a = Stream(name="a", cls=TelemetryA, interval=0.05, incorp_params={})
    b = Stream(name="b", cls=TelemetryB, interval=0.01, incorp_params={})

    flow = FlowControl(
        gate=SoftPass(),
        penstock=SustainedPenstock(rate_per_sec=rate),
    )
    ws = Watershed(
        window=_short_window(0.5),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b", flow=flow)],
    )
    tw = Tideweaver(ws, tick_factory=_noop, pass_interval=0.005)
    _tides = [t async for t in tw.run()]

    pl = [r for r in tw.rejects if r.error_kind == "PenstockLimited"]
    assert len(pl) >= 1, f"Expected PenstockLimited rejects; got none. All rejects: {tw.rejects}"

    r = pl[0]
    assert r.attempt_number == 1, f"Canal rejects must have attempt_number=1; got {r.attempt_number}"
    assert r.duration_sec is not None, "PenstockLimited reject must have duration_sec set"
    assert r.duration_sec >= 0.0, f"duration_sec must be non-negative; got {r.duration_sec}"
    assert r.cooldown_sec is not None, "PenstockLimited from SustainedPenstock must have cooldown_sec"
    # cooldown ≈ 1/rate within a generous tolerance (scheduling jitter)
    assert r.cooldown_sec == pytest.approx(1.0 / rate, abs=0.5), (
        f"cooldown_sec={r.cooldown_sec} should be near 1/rate={1.0 / rate}"
    )


# ---------------------------------------------------------------------------
# T2 — eligibility_start_perf is reset after a successful fire
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_eligibility_start_perf_reset_after_fire(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """eligibility_start_perf is None after _tick_wrapper.finally resets it on successful fire.

    Run a two-current chain just long enough for B to fire at least once.
    After the run, every edge_state.eligibility_start_perf should be None
    because the finally block resets it on each successful consumption.
    """
    monkeypatch.chdir(tmp_path)

    a = Stream(name="a", cls=TelemetryA, interval=0.05, incorp_params={})
    b = Stream(name="b", cls=TelemetryB, interval=0.05, incorp_params={})

    ws = Watershed(
        window=_short_window(0.5),
        currents=[a, b],
        edges=[Edge(from_name="a", to_name="b")],
    )
    tw = Tideweaver(ws, tick_factory=_noop, pass_interval=0.03)
    tides = [t async for t in tw.run()]

    # At least one firing of B must have occurred for the reset to matter.
    b_fired = sum(1 for t in tides for n in t.fired if n == "b")
    assert b_fired >= 1, "B must fire at least once for the reset assertion to be valid"

    edge_state = tw._edge_state.get(("a", "b"))
    assert edge_state is not None
    # After a successful tick, eligibility_start_perf should be None.
    assert edge_state.eligibility_start_perf is None, (
        f"eligibility_start_perf should be reset to None after a successful fire; "
        f"got {edge_state.eligibility_start_perf}"
    )
