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
example runs without network access.  Real pipelines swap ``inc_file``
for ``inc_url`` against your live race-data feeds.

Run with:
    python examples/appendix/nascar-tideweaver/nascar_tideweaver.py
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from incorporator import Fjord, Stream, Tideweaver, Watershed

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "fixtures"
OUTFLOW_PATH = HERE / "outflow.py"
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)

# Make the sidecar importable when this script is run via ``python -m`` or
# from a working directory other than HERE.
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

# Reuse the same class definitions + outflow() that watershed.json loads,
# so the Python and JSON entry points stay in lockstep.
from outflow import LAPDATA_CONV_DICT, DriverState, FlagEvents, LapData, PitStops  # noqa: E402


async def main() -> None:
    # Outputs live next to the script so you can inspect the driver-state
    # log after each run.  ``examples/**/out/`` is gitignored — nothing
    # leaks into git.  Delete the directory before re-running for a clean log.
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
                "conv_dict": LAPDATA_CONV_DICT,
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
