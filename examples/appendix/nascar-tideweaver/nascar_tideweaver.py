"""
Appendix — NASCAR Tideweaver: Diamond Across a Different Domain
---------------------------------------------------------------
Companion script for ``docs/appendix/nascar_tideweaver.md``.

Demonstrates the same ``Watershed.diamond()`` shape that Tutorial 7
uses for its multi-exchange crypto arb scanner — applied to race
telemetry instead:

    laps + pits + flags   →   driver state (fjord flush)

The three head/middle streams refresh their per-class registries; the
tail Fjord current's tick is a *fjord flush* — snapshot upstream
registries, run ``outflow(state)``, build the dynamic ``DriverState``
class, export.

To stay runnable without credentials, all three sources read from
local JSON files this script writes into a temp directory before
starting the watershed.  Real pipelines swap ``inc_file`` for
``inc_url`` against your live race-data feeds.

Run with:
    python examples/appendix/nascar_tideweaver.py
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from incorporator import Incorporator, Fjord, Stream, Tideweaver, Watershed


# --- Source classes -------------------------------------------------------


class Lap(Incorporator):
    """One row of per-driver lap data."""


class Pit(Incorporator):
    """One row of per-driver pit-stop data."""


class Flag(Incorporator):
    """One row of race-flag events."""


class DriverState(Incorporator):
    """Derived per-driver state — built dynamically by the fjord flush."""


# --- Outflow function -----------------------------------------------------

OUTFLOW_SOURCE = """
def outflow(state):
    laps  = state.get('Lap', [])
    pits  = state.get('Pit', [])
    flags = state.get('Flag', [])
    by_driver = {}
    for lap in laps:
        d = getattr(lap, 'driver', None) or getattr(lap, 'inc_code', None)
        if d is None:
            continue
        row = by_driver.setdefault(d, {'driver': d, 'laps': 0, 'pits': 0, 'flag': None})
        row['laps'] = max(row['laps'], int(getattr(lap, 'lap_number', 0) or 0))
    for pit in pits:
        d = getattr(pit, 'driver', None) or getattr(pit, 'inc_code', None)
        if d is None:
            continue
        row = by_driver.setdefault(d, {'driver': d, 'laps': 0, 'pits': 0, 'flag': None})
        row['pits'] += 1
    if flags:
        latest = flags[-1]
        for row in by_driver.values():
            row['flag'] = getattr(latest, 'color', None)
    return list(by_driver.values())
"""


def _seed_source_files(tmpdir: Path) -> dict[str, Path]:
    """Write toy JSON source files so the example runs without network access."""
    laps_path = tmpdir / "laps.json"
    pits_path = tmpdir / "pits.json"
    flags_path = tmpdir / "flags.json"
    laps_path.write_text(
        json.dumps(
            [
                {"driver": "Larson", "lap_number": 42},
                {"driver": "Hamlin", "lap_number": 41},
                {"driver": "Byron", "lap_number": 42},
            ]
        ),
        encoding="utf-8",
    )
    pits_path.write_text(
        json.dumps([{"driver": "Hamlin", "stop": 1}, {"driver": "Byron", "stop": 2}]),
        encoding="utf-8",
    )
    flags_path.write_text(json.dumps([{"color": "green", "lap": 1}, {"color": "yellow", "lap": 42}]), encoding="utf-8")
    return {"laps": laps_path, "pits": pits_path, "flags": flags_path}


def _write_outflow(tmpdir: Path) -> Path:
    path = tmpdir / "outflow.py"
    path.write_text(OUTFLOW_SOURCE, encoding="utf-8")
    return path


async def main() -> None:
    # Outputs live next to the script (``examples/appendix/nascar-tideweaver/out/``)
    # so you can inspect the driver-state log after each run.
    # ``examples/**/out/`` is gitignored — nothing leaks into git.  Delete
    # the directory before re-running for a clean log.
    here = Path(__file__).resolve().parent
    out_dir = here / "out"
    out_dir.mkdir(exist_ok=True)
    files = _seed_source_files(out_dir)
    outflow_path = _write_outflow(out_dir)
    out_file = out_dir / "driver_state.ndjson"

    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(seconds=15))

    watershed = Watershed.diamond(
        window=window,
        head=Stream(
            name="laps",
            cls=Lap,
            interval=3.0,
            incorp_params={"inc_file": str(files["laps"]), "inc_code": "driver"},
        ),
        middle=[
            Stream(
                name="pits",
                cls=Pit,
                interval=3.0,
                incorp_params={"inc_file": str(files["pits"]), "inc_code": "driver"},
            ),
            Stream(
                name="flags",
                cls=Flag,
                interval=3.0,
                incorp_params={"inc_file": str(files["flags"]), "inc_code": "color"},
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
        outflow=outflow_path,
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
