***

> 📎 **Appendix — Same diamond, different domain.**  The crypto-spine
> Tutorial 11 builds a multi-exchange arbitrage scanner via
> `Watershed.diamond()`.  This appendix re-runs the same shape against
> a completely different domain — NASCAR race telemetry — so the
> reader can confirm the orchestrator is domain-agnostic.  No new
> framework concepts here; read [Tutorial 11](../../11-tideweaver/README.md) first.

***

# 🏁 NASCAR Tideweaver: Diamond Across a Different Domain

Race telemetry is a natural fit for `Watershed.diamond()`:

* **Laps** update every few seconds (per-driver position, lap number, speed).
* **Pit reports** update every 30 seconds or so (per-driver stop count, duration).
* **Flag events** fire on no fixed cadence (green / yellow / red / white).
* **Driver state** — the fused output — wants to combine all three on a steady
  interval so a downstream dashboard sees a coherent per-driver snapshot.

Three Stream currents feed one Fjord tail.  Same shape as the crypto arb scanner
in [Tutorial 11](../../11-tideweaver/README.md); different sources, different outflow logic, same
five-name vocabulary.

---

## The diamond

```python
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from incorporator import Incorporator, Fjord, Stream, Tideweaver, Watershed


class Lap(Incorporator):
    """One row of per-driver lap data."""


class Pit(Incorporator):
    """One row of per-driver pit-stop data."""


class Flag(Incorporator):
    """One row of race-flag events."""


class DriverState(Incorporator):
    """Derived per-driver state — built dynamically by the fjord flush."""


async def main() -> None:
    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(seconds=15))

    watershed = Watershed.diamond(
        window=window,
        head=Stream(
            name="laps",
            cls=Lap,
            interval=3.0,
            incorp_params={"inc_file": "examples/appendix/nascar-tideweaver/fixtures/laps.json",
                           "inc_code": "driver"},
        ),
        middle=[
            Stream(
                name="pits",
                cls=Pit,
                interval=3.0,
                incorp_params={"inc_file": "examples/appendix/nascar-tideweaver/fixtures/pits.json",
                               "inc_code": "driver"},
            ),
            Stream(
                name="flags",
                cls=Flag,
                interval=3.0,
                incorp_params={"inc_file": "examples/appendix/nascar-tideweaver/fixtures/flags.json",
                               "inc_code": "color"},
            ),
        ],
        tail=Fjord(
            name="state",
            cls=DriverState,
            interval=3.0,
            export_params={
                "file_path": "data/driver_state.ndjson",
                "format": "ndjson",
                "if_exists": "append",
            },
        ),
        outflow="examples/appendix/nascar-tideweaver/race_outflow.py",
        drain_timeout=10.0,
    )

    async for tide in Tideweaver(watershed).run():
        print(
            f"Tide {tide.tide_number:3d} | fired: {','.join(tide.fired) or '-':<20} "
            f"| skipped: {len(tide.skipped):2d} | {tide.duration_sec:.3f}s"
        )


if __name__ == "__main__":
    asyncio.run(main())
```

A runnable version with local-file fixtures, an inlined outflow, and a tempdir output
target lives at [`examples/appendix/nascar-tideweaver/nascar_tideweaver.py`](../../examples/appendix/nascar-tideweaver/nascar_tideweaver.py).

---

## The outflow function

The `DriverState` Fjord current's `outflow(state)` joins laps + pits + flags into one
row per driver:

```python
# examples/appendix/nascar-tideweaver/race_outflow.py
def outflow(state):
    laps  = state.get("LapData", [])
    pits  = state.get("PitStops", [])
    flags = state.get("FlagEvents", [])

    by_driver = {}
    for lap in laps:
        d = getattr(lap, "driver", None) or getattr(lap, "inc_code", None)
        if d is None:
            continue
        row = by_driver.setdefault(d, {"driver": d, "laps": 0, "pits": 0, "flag": None})
        row["laps"] = max(row["laps"], int(getattr(lap, "lap_number", 0) or 0))

    for pit in pits:
        d = getattr(pit, "driver", None) or getattr(pit, "inc_code", None)
        if d is None:
            continue
        row = by_driver.setdefault(d, {"driver": d, "laps": 0, "pits": 0, "flag": None})
        row["pits"] += 1

    if flags:
        latest = flags[-1]
        for row in by_driver.values():
            row["flag"] = getattr(latest, "color", None)

    return list(by_driver.values())
```

Same structure as Tutorial 11's `arb_outflow.outflow()` — snapshot upstream registries,
build a per-key composite, return as a list of dicts.

---

## CLI form

`examples/appendix/nascar-tideweaver/watershed.json` ships alongside the example:

```bash
incorporator validate examples/appendix/nascar-tideweaver/watershed.json
incorporator tideweaver run examples/appendix/nascar-tideweaver/watershed.json --json-output
```

Run from the repo root so the relative `inc_file` / `outflow` paths resolve.

---

## Why this domain works well for Tideweaver

* **Bounded race window** — the orchestrator runs for the race duration and exits
  clean.  No daemon to babysit between sessions.
* **Mixed cadences** — laps are fast, pits are medium, flags are bursty.  Per-current
  intervals match the source's actual update rhythm.
* **One coherent fused output** — the dashboard wants one driver-state record per N
  seconds carrying the latest from all three sources.  The Fjord tail does exactly
  that via `outflow(state)`.

---

## Where to Go Next

| Goal | Read |
|---|---|
| See the crypto-spine version of the same diamond | [Tutorial 11 — Tideweaver](../../11-tideweaver/README.md) |
| Run the non-Tideweaver fjord variant against NASCAR data | [Appendix — NASCAR Fantasy Fjord](../nascar-fantasy-fjord/README.md) |
| Land columnar artifacts at window close | [Appendix — Parquet Snapshots in a Tideweaver Window](../tideweaver-parquet-snapshots/README.md) |
| Pick between in-process Tideweaver and cloud schedulers | [Appendix — Tideweaver vs. Prefect](../tideweaver-vs-prefect/README.md) |
| Configure this watershed for the CLI | [CLI & Configuration §9](../../../docs/cli_and_configuration.md#9-the-tideweaver-subcommand--windowed-orchestration) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/appendix/nascar-tideweaver/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
