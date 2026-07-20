"""Pinning test for ``examples/appendix/nascar-tideweaver/`` (T-Tutorial).

Locks the CURRENT observable behavior of the NASCAR Tideweaver diamond ahead
of an upcoming framework refactor program. Pure regression tripwire — no live
network (all three sources are ``inc_file``-based local fixtures, so there is
nothing to mock at the ``execute_request`` layer), reconstructs
``Watershed.diamond()`` verbatim in-test with the real ``LapData``/
``PitStops``/``FlagEvents``/``DriverState``/``outflow`` objects loaded from
``outflow.py`` (never ``nascar_tideweaver.py``'s own ``main()``, which writes
into the real gitignored ``examples/appendix/nascar-tideweaver/out/``
directory with ``if_exists="append"`` — repeated test runs would accumulate
stale rows on disk).

Loading ``outflow.py`` triggers its own ``from nascar_tideweaver import
...``, which is the first-ever import of ``nascar_tideweaver`` in this
process — that import populates ``sys.modules["nascar_tideweaver"]`` with the
canonical class objects, exactly the CLI-form path (no ``sys.modules``
aliasing hack needed; that hack is only for direct ``python
nascar_tideweaver.py`` execution).

The tail Fjord's export uses ``if_exists="append"`` with fixture data that
never changes between ticks — a 15s window can fire the Fjord multiple
times, each appending the same 5-driver block. So this test pins structural
invariants (row count is a positive multiple of 5; every row matches one of
the 5 expected driver tuples) rather than an exact row count, avoiding
real-clock CI flakiness (see the README's "~20-25 rows" note).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from incorporator.schema.converters import inc
from incorporator.tideweaver import Fjord, Stream, Tideweaver, Watershed
from tests.helpers import load_sidecar

_HERE = Path(__file__).resolve()
_EXAMPLE_DIR = _HERE.parents[3] / "examples" / "appendix" / "nascar-tideweaver"
_FIXTURES = _EXAMPLE_DIR / "fixtures"

_nascar_outflow = load_sidecar(_EXAMPLE_DIR / "outflow.py", "nascar_tideweaver_appendix_outflow")

LapData = _nascar_outflow.LapData
PitStops = _nascar_outflow.PitStops
FlagEvents = _nascar_outflow.FlagEvents
DriverState = _nascar_outflow.DriverState
outflow = _nascar_outflow.outflow

_EXPECTED_ROWS = {
    ("Larson", 42, 0, "yellow"),
    ("Hamlin", 41, 1, "yellow"),
    ("Byron", 42, 1, "yellow"),
    ("Bell", 40, 1, "yellow"),
    ("Elliott", 41, 0, "yellow"),
}


def _reset_all() -> None:
    """Wipe per-class inc_dict + parked snapshots to prevent test cross-contamination."""
    for cls in (LapData, PitStops, FlagEvents, DriverState):
        cls.inc_dict.clear()
        if "_tideweaver_snapshot" in cls.__dict__:
            try:
                delattr(cls, "_tideweaver_snapshot")
            except AttributeError:
                pass


@pytest.mark.asyncio
async def test_nascar_tideweaver_diamond_produces_five_driver_states(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The nascar-tideweaver diamond fuses laps+pits+flags into 5 driver rows.

    Proves:
    - Exported NDJSON row count is a positive multiple of 5 (append-on-every-
      tick against static fixture data).
    - Every row's (driver, laps, pits, flag) tuple matches one of the 5
      expected fixture-derived driver states.
    - The set of distinct drivers across the file equals exactly the 5
      fixture drivers.
    """
    monkeypatch.chdir(tmp_path)
    _reset_all()

    out_file = tmp_path / "driver_state.ndjson"

    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(seconds=15))

    watershed = Watershed.diamond(
        window=window,
        head=Stream(
            name="laps",
            cls=LapData,
            interval=3.0,
            incorp_params={
                "inc_file": str(_FIXTURES / "laps.json"),
                "inc_code": "driver",
                "conv_dict": {"lap_number": inc(int, default=0)},
            },
        ),
        middle=[
            Stream(
                name="pits",
                cls=PitStops,
                interval=3.0,
                incorp_params={"inc_file": str(_FIXTURES / "pits.json"), "inc_code": "driver"},
            ),
            Stream(
                name="flags",
                cls=FlagEvents,
                interval=3.0,
                incorp_params={"inc_file": str(_FIXTURES / "flags.json"), "inc_code": "color"},
            ),
        ],
        tail=Fjord(
            name="state",
            cls=DriverState,
            interval=3.0,
            export_params={
                "file_path": str(out_file),
                "format": "ndjson",
                "if_exists": "append",
            },
        ),
        outflow=str(_EXAMPLE_DIR / "outflow.py"),
        drain_timeout=10.0,
    )

    [_ async for _ in Tideweaver(watershed).run()]

    assert out_file.exists(), "state Fjord must have written driver_state.ndjson"
    lines = [ln for ln in out_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert lines, "output file must not be empty"
    rows = [json.loads(ln) for ln in lines]

    assert len(rows) % 5 == 0 and len(rows) > 0, f"row count must be a positive multiple of 5, got {len(rows)}"

    for row in rows:
        tup = (row["driver"], row["laps"], row["pits"], row["flag"])
        assert tup in _EXPECTED_ROWS, f"unexpected driver-state row: {row}"

    distinct_drivers = {row["driver"] for row in rows}
    assert distinct_drivers == {t[0] for t in _EXPECTED_ROWS}, (
        f"expected exactly the 5 fixture drivers, got {distinct_drivers}"
    )
