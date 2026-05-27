"""Tests for _tick_wrapper retry-statistics extraction.

Proves that _incorporator_attempt_number is set on the raised exception
with the correct value from tenacity retrying.statistics, and that
the on_error="isolate" path's retrying-is-None guard behaves correctly.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Tuple

import pytest

from incorporator import Incorporator
from incorporator.observability.tideweaver import (
    Current,
    Stream,
    Tideweaver,
    Watershed,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _RetryA(Incorporator):
    """Stand-in source class for retry telemetry tests."""


class _RetryB(Incorporator):
    """Stand-in source class B."""


def _short_window(seconds: float = 1.0) -> Tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(seconds=seconds))


async def _collect_tides(tw: Tideweaver) -> list[Any]:
    return [t async for t in tw.run()]


# ---------------------------------------------------------------------------
# T1 — on_error="restart" 5-retry exhaustion attaches attempt_number=5 to exc
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_restart_exhausted_attempt_number_on_exception(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """on_error=restart _tick_wrapper attaches _incorporator_attempt_number=5 to the exception.

    Calls _tick_wrapper directly (not through the full run loop) and captures
    the exception via a monkeypatched logger.error side-channel.  After all
    5 tenacity attempts, the except block sets e._incorporator_attempt_number=5
    on the caught exception before calling logger.error (which swallows it for
    restart policy).  We intercept at the logger.error call to inspect the exc.
    """
    monkeypatch.chdir(tmp_path)

    stream = Stream(
        name="bomber",
        cls=_RetryA,
        interval=0.05,
        on_error="restart",
        incorp_params={},
    )
    ws = Watershed(
        window=_short_window(30.0),
        currents=[stream],
        edges=[],
    )

    call_count = 0
    captured_attempt: list[int | None] = []

    async def always_fail(current: Current) -> None:
        nonlocal call_count
        call_count += 1
        raise RuntimeError("deliberate failure")

    tw = Tideweaver(ws, tick_factory=always_fail, pass_interval=0.05)
    tw._client_pool = {}
    tw._run_started_at = None

    import incorporator.observability.tideweaver.scheduler as sched_mod

    original_error = sched_mod.logger.error

    def capture_and_forward(msg: str, *args: Any, **kwargs: Any) -> None:
        # The current exception (caught in the except block) has already
        # had _incorporator_attempt_number set by the time logger.error is called.
        # We retrieve it from the asyncio current exception context.
        import sys

        exc = sys.exc_info()[1]
        captured_attempt.append(getattr(exc, "_incorporator_attempt_number", None))
        original_error(msg, *args, **kwargs)

    monkeypatch.setattr(sched_mod.logger, "error", capture_and_forward)

    await tw._tick_wrapper(stream, consumed_snapshot={})

    assert len(captured_attempt) == 1, f"logger.error must be called exactly once; got {captured_attempt}"
    assert captured_attempt[0] == 5, (
        f"Expected attempt_number=5 after stop_after_attempt(5); got {captured_attempt[0]}"
    )
    assert call_count == 5, f"Expected 5 call attempts; got {call_count}"


# ---------------------------------------------------------------------------
# T2 — on_error="isolate" path: exception swallowed; scheduler keeps running
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_isolate_path_does_not_crash_siblings(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """on_error=isolate: failing current is contained; healthy siblings keep firing.

    The isolate path catches the exception inside the per-current try/except
    before it reaches the outer except block where _incorporator_attempt_number
    would be attached.  This test asserts the containment contract: scheduler
    keeps emitting tides, and healthy siblings fire at least once despite the
    failing sibling raising every tick.
    """
    monkeypatch.chdir(tmp_path)
    fire_count: list[str] = []

    async def sometimes_fail(current: Current) -> None:
        fire_count.append(current.name)
        if current.name == "failing":
            raise RuntimeError("isolated failure")

    failing = Stream(
        name="failing",
        cls=_RetryA,
        interval=0.05,
        on_error="isolate",
        incorp_params={},
    )
    healthy = Stream(
        name="healthy",
        cls=_RetryB,
        interval=0.05,
        on_error="isolate",
        incorp_params={},
    )
    ws = Watershed(
        window=_short_window(0.4),
        currents=[failing, healthy],
        edges=[],
    )
    tw = Tideweaver(ws, tick_factory=sometimes_fail, pass_interval=0.05)
    tides = await _collect_tides(tw)

    # Scheduler must keep running despite failures.
    assert len(tides) >= 1, "Scheduler should emit tides even with isolated failures"
    healthy_fires = fire_count.count("healthy")
    assert healthy_fires >= 1, "Healthy current must fire at least once despite sibling failure"


# ---------------------------------------------------------------------------
# T3 — on_error="restart" with immediate success does not crash
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_restart_success_does_not_crash(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """on_error=restart current that succeeds on first attempt runs normally.

    Verifies that the retrying hoist change does not break the happy path —
    the retrying object is created, assigned, and the AsyncRetrying loop
    completes on the first attempt without raising.
    """
    monkeypatch.chdir(tmp_path)
    fires: list[str] = []

    async def succeed(current: Current) -> None:
        fires.append(current.name)

    stream = Stream(
        name="src",
        cls=_RetryA,
        interval=0.1,
        on_error="restart",
        incorp_params={},
    )
    ws = Watershed(
        window=_short_window(0.4),
        currents=[stream],
        edges=[],
    )
    tw = Tideweaver(ws, tick_factory=succeed, pass_interval=0.05)
    tides = await _collect_tides(tw)

    assert len(tides) >= 1
    assert fires.count("src") >= 1, "restart current must fire on happy path"
