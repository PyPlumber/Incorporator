"""Regression tests for real restart-exhaustion parking (D5-04).

Before the fix, ``on_error="restart"`` exhausting its tenacity retry budget
logged ``event_type="tick_parked"`` ("current parked") but never actually
parked anything — ``_gate_reason`` had no way to know, so the current
re-fired on its next interval and repeated the full 5-attempt exponential
backoff cycle for the rest of the window.  These tests assert the current
is genuinely skipped (``SkipReason.PARKED``) on a subsequent pass within
the same window — the actual regression-must-fail-pre-fix signal.

Covers both a CustomCurrent (Stream-shaped, no scheduler dispatch overhead)
and a real Fjord current, confirming the fix is verb-agnostic and doesn't
corrupt fjord state / the upstream snapshot.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar

import pytest

from incorporator import Incorporator
from incorporator.tideweaver import CustomCurrent, Fjord, Tideweaver, Watershed
from incorporator.tideweaver.reasons import SkipReason


class _AlwaysBoom(Incorporator):
    """Downstream class whose current always raises."""


class _FjordSink(Incorporator):
    """Fjord downstream class whose outflow always raises."""


def _short_window(ms: float = 800.0) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(milliseconds=ms))


def _reset_registries(*classes: type[Incorporator]) -> None:
    for cls in classes:
        cls.inc_dict.clear()
        if "_tideweaver_snapshot" in cls.__dict__:
            try:
                delattr(cls, "_tideweaver_snapshot")
            except AttributeError:
                pass


@pytest.mark.asyncio
async def test_restart_exhaustion_parks_custom_current(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """A CustomCurrent with on_error='restart' that always raises is really parked.

    After the retry budget is exhausted once, ``_state[name].parked`` must be
    ``True`` and a SUBSEQUENT scheduler pass within the same window must skip
    the current with ``SkipReason.PARKED`` instead of re-entering the
    5-attempt backoff cycle again.  The retry wait floor/ceiling are
    monkeypatched down so the whole exhaustion cycle finishes in well under
    the test window without depending on tenacity's random jitter landing
    small by luck.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("incorporator.tideweaver.scheduler._CANAL_OUTER_WAIT_MIN", 0.001)
    monkeypatch.setattr("incorporator.tideweaver.scheduler._CANAL_OUTER_WAIT_MAX", 0.01)
    _reset_registries(_AlwaysBoom)

    class _AlwaysRaises(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False
        attempts: ClassVar[int] = 0

        async def tick(self, scheduler: Any) -> None:
            type(self).attempts += 1
            raise RuntimeError("always fails")

    current = _AlwaysRaises(
        name="parked_current",
        cls=_AlwaysBoom,
        interval=0.01,
        on_error="restart",
    )

    ws = Watershed.parallel(window=_short_window(800), currents=[current])
    tw = Tideweaver(ws, pass_interval=0.01)

    tides = [t async for t in tw.run()]
    assert tides, "expected at least one tide"

    # Once exhausted, the flag must be real.
    assert tw._state["parked_current"].parked is True

    # A subsequent pass must report SkipReason.PARKED, not re-fire.
    parked_skips = [(name, reason) for tide in tides for name, reason in tide.skipped if reason == SkipReason.PARKED]
    assert parked_skips, (
        f"expected at least one (name, SkipReason.PARKED) skip after restart-exhaustion; "
        f"got skipped reasons across tides: {[t.skipped for t in tides]}"
    )

    # The current must not have re-entered its tick body indefinitely: bounded
    # by ONE restart cycle's attempts (5, per _CANAL_OUTER_STOP), not one full
    # cycle per pass for the whole 800ms window.
    from incorporator.tideweaver._retry_defaults import _CANAL_OUTER_STOP

    assert _AlwaysRaises.attempts <= _CANAL_OUTER_STOP, (
        f"parking must stop the current from re-entering the backoff cycle; "
        f"got {_AlwaysRaises.attempts} attempts (cap is {_CANAL_OUTER_STOP})"
    )


@pytest.mark.asyncio
async def test_restart_exhaustion_parks_fjord_current_without_corrupting_snapshot(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Fjord current with on_error='restart' that always raises is parked; upstream snapshot survives.

    Confirms the parking fix is verb-agnostic: a Fjord current whose outflow
    always raises gets parked exactly like a Stream/CustomCurrent, and the
    (independent) upstream's ``_tideweaver_snapshot`` is untouched by the
    downstream's parking — no wave is ever pushed downstream from a parked
    current because ``_gate_reason`` short-circuits before ``_spawn_tick``.
    Retry wait floor/ceiling monkeypatched down for the same reason as the
    CustomCurrent regression above.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("incorporator.tideweaver.scheduler._CANAL_OUTER_WAIT_MIN", 0.001)
    monkeypatch.setattr("incorporator.tideweaver.scheduler._CANAL_OUTER_WAIT_MAX", 0.01)
    _reset_registries(_FjordSink)

    def _boom_outflow(state: dict[str, Any]) -> list[Any]:
        raise RuntimeError("outflow always fails")

    def stub_loader(_path: Any) -> tuple[Any, Any]:
        return (_boom_outflow, None)

    monkeypatch.setattr("incorporator.usercode.load_outflow_module", stub_loader)

    fjord_current = Fjord(
        name="fjord_parked",
        cls=_FjordSink,
        interval=0.01,
        on_error="restart",
        export_params={},
    )

    ws = Watershed.parallel(window=_short_window(800), currents=[fjord_current])
    tw = Tideweaver(ws, pass_interval=0.01)

    tides = [t async for t in tw.run()]
    assert tides

    assert tw._state["fjord_parked"].parked is True

    parked_skips = [(name, reason) for tide in tides for name, reason in tide.skipped if reason == SkipReason.PARKED]
    assert parked_skips, (
        f"expected at least one (name, SkipReason.PARKED) skip for the fjord current; "
        f"got skipped reasons across tides: {[t.skipped for t in tides]}"
    )

    # A failed/parked tick never pushes a wave — no snapshot should ever be
    # parked on the (never-succeeding) Fjord output class.
    assert getattr(_FjordSink, "_tideweaver_snapshot", None) is None
