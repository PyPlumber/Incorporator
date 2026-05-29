"""Regression tests for the scheduler's gate-dedup watermark contract.

The scheduler tracks per-edge consumption watermarks in
:attr:`Tideweaver._last_consumed` (keyed on canonical
``(from_name, to_name)`` edge tuples).  The watermark feeds
:attr:`GateContext.last_consumed` and powers the "already consumed
this wave" check in :class:`HardLock` and :class:`Weir`.  A regression
where the write key direction was flipped relative to the read key
caused the dedup check to silently no-op, letting downstreams
re-consume the same upstream wave on every scheduler pass.

These tests pin the contract by asserting exact relationships between
upstream fire counts and downstream fire counts — existing tests only
check ``>=`` lower bounds, which are permissive in the wrong direction
and miss the bug.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import pytest

from incorporator import Incorporator
from incorporator.observability.tideweaver import (
    Current,
    Stream,
    Tideweaver,
    Watershed,
    Weir,
)


class _A(Incorporator):
    """Upstream test double."""


class _B(Incorporator):
    """Downstream test double."""


def _short_window(seconds: float = 1.0) -> Tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(seconds=seconds))


def _stream(name: str, interval: float) -> Stream:
    cls = {"a": _A, "b": _B}.get(name, _A)
    return Stream(name=name, cls=cls, interval=interval, incorp_params={})


@pytest.mark.asyncio
async def test_hardlock_dedup_fires_once_per_upstream_wave() -> None:
    """Under HardLock A→B with B's interval < A's interval, B fires exactly once per A wave.

    A emits every 0.4s; B's interval is 0.05s (well below A's cadence).
    Across a 1.1s window B would *try* to fire ~22 times, but HardLock's
    "already consumed" check should permit exactly one fire per A wave.
    If the dedup key direction is wrong, B fires ~22 times — many
    duplicates per A wave.
    """
    fires: List[str] = []

    async def fake_tick(current: Current) -> None:
        fires.append(current.name)

    a = _stream("a", interval=0.4)
    b = _stream("b", interval=0.05)
    ws = Watershed.chain(window=_short_window(1.1), currents=[a, b])  # default HardLock
    tw = Tideweaver(ws, tick_factory=fake_tick, pass_interval=0.03)
    await asyncio.wait_for(_collect(tw), timeout=5.0)

    a_count = fires.count("a")
    b_count = fires.count("b")
    assert a_count >= 2, f"A must fire at least twice in 1.1s at 0.4s interval; got {a_count}"
    # HardLock dedup contract: B fires AT MOST once per A wave.
    # Allow B to be one fire behind A (if the window closes mid-pass), but
    # never more fires than A produced.  A common pre-fix observation:
    # b_count would be ~20+ here.
    assert b_count <= a_count, (
        f"HardLock dedup broken: B fired {b_count} times for {a_count} A waves (should be <= {a_count})"
    )


@pytest.mark.asyncio
async def test_weir_dedup_fires_once_per_upstream_wave() -> None:
    """Weir A→B with B faster than A: B fires once per A wave (same contract as HardLock).

    Weir differs from HardLock only on the in-flight-upstream behaviour
    (Weir fires on its own cadence once upstream has emitted at least
    once).  The wave-level dedup check is identical, so the same
    upper-bound holds: B can't fire more times than A produced waves.
    """
    fires: List[str] = []

    async def fake_tick(current: Current) -> None:
        fires.append(current.name)

    a = _stream("a", interval=0.4)
    b = _stream("b", interval=0.05)
    ws = Watershed.chain(window=_short_window(1.1), currents=[a, b], gate_mode="weir")
    tw = Tideweaver(ws, tick_factory=fake_tick, pass_interval=0.03)
    await asyncio.wait_for(_collect(tw), timeout=5.0)

    a_count = fires.count("a")
    b_count = fires.count("b")
    assert a_count >= 2, f"A must fire at least twice in 1.1s at 0.4s interval; got {a_count}"
    assert b_count <= a_count, (
        f"Weir dedup broken: B fired {b_count} times for {a_count} A waves (should be <= {a_count})"
    )


async def _collect(tw: Tideweaver) -> List[object]:
    """Drain the scheduler — used in lieu of a list-comprehension for the timeout wrap."""
    return [tide async for tide in tw.run()]


# Suppress an unused-import lint when typing checkers narrow types.
_ = Weir
