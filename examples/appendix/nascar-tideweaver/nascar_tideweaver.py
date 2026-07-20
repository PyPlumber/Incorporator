"""
Appendix — NASCAR Tideweaver: Diamond Across a Different Domain
---------------------------------------------------------------
Companion script for ``examples/appendix/nascar-tideweaver/README.md``.

Demonstrates the same ``Watershed.diamond()`` shape that Tutorial 11
uses for its multi-exchange crypto arb scanner — applied to race
telemetry instead:

    laps + pits + flags   →   driver state (fjord flush)

The three head/middle streams refresh their per-class registries; the
tail Fjord current's tick is a *fjord flush* — snapshot upstream
registries, run ``outflow(state)``, build the dynamic ``DriverState``
class, export.

All three sources read from local JSON fixtures in ``fixtures/`` so the
example runs without network access. Real pipelines swap ``inc_file``
for ``inc_url`` against your live race-data feeds.

``LapData``/``PitStops``/``FlagEvents``/``DriverState`` and ``outflow()``
are defined ONCE, here. ``outflow.py`` re-exports them (rather than
redefining them) so the CLI's class/token resolvers see the same
canonical objects this file's own ``main()`` uses — see ``outflow.py``'s
docstring for why that matters.

Run with:
    python examples/appendix/nascar-tideweaver/nascar_tideweaver.py
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from incorporator import Fjord, Incorporator, Stream, Tideweaver, Watershed
from incorporator.schema.converters import inc

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "fixtures"
OUTFLOW_PATH = HERE / "outflow.py"
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Incorporator subclasses -- defined ONCE, here; outflow.py re-imports them.
# ---------------------------------------------------------------------------


class LapData(Incorporator):
    """Head source — per-driver lap data."""


class PitStops(Incorporator):
    """Middle source — per-driver pit-stop counts."""


class FlagEvents(Incorporator):
    """Middle source — race-flag colour timeline."""


class DriverState(Incorporator):
    """Derived output class for the tail Fjord flush -- bare row class;
    ``outflow(state)``'s returned dict keys ARE the export shape
    (``Incorporator``'s ``extra='allow'`` base means no field declarations
    are needed)."""


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Aggregate laps + pits + flags into one row per driver.

    Args:
        state: Keyed by upstream Incorporator subclass name; maps to a list
            of that class's parked ``_tideweaver_snapshot`` rows. This is a
            Tideweaver Fjord-current wave, so ``state`` values are PLAIN
            lists.

    Returns:
        One row per driver seen in laps or pits, each carrying the driver's
        max lap number, pit-stop count, and the most recent flag color (or
        None before any flag has fired).
    """
    laps = state.get("LapData", [])
    pits = state.get("PitStops", [])
    flags = state.get("FlagEvents", [])

    by_driver: dict[str, dict[str, Any]] = {}
    for lap in laps:
        row = by_driver.setdefault(lap.driver, {"driver": lap.driver, "laps": 0, "pits": 0, "flag": None})
        row["laps"] = max(row["laps"], lap.lap_number)

    for pit in pits:
        row = by_driver.setdefault(pit.driver, {"driver": pit.driver, "laps": 0, "pits": 0, "flag": None})
        row["pits"] += 1

    if flags:
        latest_color = flags[-1].color
        for row in by_driver.values():
            row["flag"] = latest_color

    return list(by_driver.values())


async def main() -> None:
    # Outputs live next to the script so you can inspect the driver-state
    # log after each run. ``examples/**/out/`` is gitignored — nothing
    # leaks into git. Delete the directory before re-running for a clean log.
    out_file = OUT / "driver_state.ndjson"

    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(seconds=15))

    watershed = Watershed.diamond(
        window=window,
        head=Stream(
            name="laps",
            cls=LapData,
            interval=3.0,
            incorp_params={
                "inc_file": str(FIXTURES / "laps.json"),
                "inc_code": "driver",
                "conv_dict": {"lap_number": inc(int, default=0)},
            },
        ),
        middle=[
            Stream(
                name="pits",
                cls=PitStops,
                interval=3.0,
                incorp_params={"inc_file": str(FIXTURES / "pits.json"), "inc_code": "driver"},
            ),
            Stream(
                name="flags",
                cls=FlagEvents,
                interval=3.0,
                incorp_params={"inc_file": str(FIXTURES / "flags.json"), "inc_code": "color"},
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
        outflow=str(OUTFLOW_PATH),
        drain_timeout=10.0,
    )

    async for tide in Tideweaver(watershed).run():
        print(
            f"Tide {tide.tide_number:3d} | fired: {','.join(tide.fired) or '-':<20} "
            f"| skipped: {len(tide.skipped):2d} | {tide.duration_sec:.3f}s"
        )

    if out_file.exists():
        rows = len(out_file.read_text(encoding="utf-8").splitlines())
        print(f"\nwrote {rows} driver-state rows to {out_file}")


if __name__ == "__main__":
    asyncio.run(main())
