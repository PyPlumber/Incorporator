"""Benchmark: Tideweaver fjord per-tick state-assembly overhead (Stage 6c, C2/C3).

Measures the cost of ``Tideweaver._tick_fjord``'s state-assembly step —
the ``_resolve_current_snapshot`` calls (``incorporator/tideweaver/
scheduler.py``) that build ``state[dep.cls.__name__]`` from each upstream's
parked ``_tideweaver_snapshot`` before handing it to ``outflow(state)``.
Every one of those calls does an unconditional ``list(snapshot)`` copy
(module-level ``_resolve_current_snapshot``, both ``treat_empty_snapshot_
as_missing`` branches) — this benchmark is the missing measurement
instrument for evaluating whether that copy is worth removing.

Shape: 3 upstream currents x 5,000 parked rows each, one downstream
``Fjord`` with a no-op ``outflow(state)`` (``lambda state: []``) so the
benchmark isolates state ASSEMBLY, not outflow execution or export I/O.
``load_outflow_module`` is monkeypatched to skip the filesystem read (the
same stub pattern ``test_tideweaver_parent_child_fjord.py`` uses) — no
outflow.py file is ever loaded from disk.

No network is involved: ``_tick_fjord`` only snapshots parked class
registries and calls the shared ``flush()`` primitive (``incorporator/
pipeline/outflow.py``), which itself only touches ``asyncio.to_thread``
+ in-memory build/export — confirmed by reading both functions, there is
no ``io/fetch.py`` call anywhere on this path.  Upstream currents are
wired with a deliberately huge ``interval`` so they never actually tick
during the run (avoiding any real ``incorp()`` seed call); their classes
are pre-parked with rows directly.  Edges use ``gate_mode="soft"`` so the
Fjord fires on its own cadence regardless of upstream wave state
(``SoftPass`` disables in-flight/freshness/consumed checks — see
``incorporator/tideweaver/flow.py``).

This is a NEW instrument with no prior calibration history, so the floor
is deliberately generous (informational, not a tight regression gate) —
see the module docstring convention in ``test_scheduler_pass_overhead.py``.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List, Tuple

import pytest

from incorporator import Incorporator
from incorporator.tideweaver import Edge, Fjord, Stream, Tideweaver, Watershed


def _make_upstream_targets(n: int) -> List[type]:
    """Build ``n`` distinct Incorporator subclasses — Watershed's cls.__name__ collision
    validator requires every producing current to bind a distinct class.
    """
    return [type(f"_BenchFjordUpstream{i}", (Incorporator,), {}) for i in range(n)]


def _park_rows(cls: type, n_rows: int) -> None:
    """Park ``n_rows`` plain dict rows directly on ``cls._tideweaver_snapshot``.

    ``_resolve_current_snapshot`` only ever calls ``list(snapshot)`` on
    whatever is parked — a plain list is exactly what it reads; no
    ``IncorporatorList`` wrapping is required for this benchmark since
    nothing here depends on ``inc_child_path`` metadata.
    """
    cls._tideweaver_snapshot = [{"id": i, "value": i * 1.5, "label": f"row-{i}"} for i in range(n_rows)]


def _window(seconds: float) -> Tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    return (now, now + timedelta(seconds=seconds))


WINDOW_SECONDS = 2.0
N_UPSTREAMS = 3
N_ROWS = 5_000

# No prior calibration exists for this instrument — a generous floor well
# below the scheduler's ``pass_interval=0.05`` theoretical ceiling (20/s,
# see test_scheduler_pass_overhead.py) so it only fails on a gross
# regression (e.g. an accidental O(n^2) in the assembly path), not on
# ordinary machine-to-machine variance.
FLOOR_TICKS_PER_SEC = 5.0


@pytest.mark.asyncio
async def test_fjord_tick_state_assembly_overhead(monkeypatch: pytest.MonkeyPatch) -> None:
    """3 upstreams x 5,000 rows -> fjord state assembly must clear a generous floor.

    Reports ticks/sec for the fjord's real ``_tick_fjord`` state-assembly
    path (3x ``list()`` copies of a 5,000-row parked snapshot per tick).
    Informational baseline for the C2 (fjord state assembly) / C3
    (wave-push snapshot copy) perf evaluation — not a tuned regression
    gate.
    """
    monkeypatch.setattr("incorporator.usercode.load_outflow_module", lambda _path: (lambda state: [], None))

    upstream_targets = _make_upstream_targets(N_UPSTREAMS)
    for cls in upstream_targets:
        _park_rows(cls, N_ROWS)

    upstream_currents = [
        # interval huge -> never becomes due; the upstream classes are
        # pre-parked directly so the fjord always has 5,000 rows to copy
        # per upstream without any upstream current actually ticking.
        Stream(name=f"up{i}", cls=upstream_targets[i], interval=1_000_000.0, incorp_params={})
        for i in range(N_UPSTREAMS)
    ]
    fjord_out_cls = type("_BenchFjordOut", (Incorporator,), {})
    fjord = Fjord(
        name="fjord",
        cls=fjord_out_cls,
        interval=0.05,
        # Never actually read -- load_outflow_module is monkeypatched above.
        outflow=Path("_bench_unused_outflow.py"),
    )
    edges = [Edge(from_name=up.name, to_name=fjord.name, gate_mode="soft") for up in upstream_currents]

    watershed = Watershed(
        window=_window(WINDOW_SECONDS),
        currents=[*upstream_currents, fjord],
        edges=edges,
    )
    tw = Tideweaver(watershed, pass_interval=0.05)

    t0 = time.perf_counter()
    fjord_ticks = 0
    async for tide in tw.run():
        if "fjord" in tide.fired:
            fjord_ticks += 1
    elapsed = time.perf_counter() - t0

    ticks_per_sec = fjord_ticks / elapsed
    print(
        f"\n  Fjord tick state assembly ({N_UPSTREAMS}x{N_ROWS} rows): "
        f"{fjord_ticks} ticks in {elapsed:.2f}s = {ticks_per_sec:.1f} ticks/sec"
    )
    assert fjord_ticks > 0, "fjord never fired -- benchmark wiring is broken (check soft-gate edges)."
    assert ticks_per_sec >= FLOOR_TICKS_PER_SEC, (
        f"Fjord state-assembly dropped to {ticks_per_sec:.1f} ticks/sec at "
        f"{N_UPSTREAMS}x{N_ROWS} rows (floor: {FLOOR_TICKS_PER_SEC}).  "
        "Suggests a regression in _resolve_current_snapshot or the "
        "_tick_fjord state-assembly loop."
    )
