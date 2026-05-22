***

> 📎 **Appendix — Same diamond, different domain.** Tutorial 11
> builds a crypto-spine multi-exchange arb scanner via
> `Watershed.diamond()`. This appendix re-runs the same shape against
> NASCAR race telemetry so the reader can confirm the orchestrator
> is domain-agnostic. No new framework concepts here; read
> [Tutorial 11](../../11-tideweaver/README.md) first.

***

# 🏁 NASCAR Tideweaver: Diamond Across a Different Domain

Race-day telemetry has three concurrent feeds — lap-by-lap times, pit stops, and yellow / green / red flag transitions — that converge on one fused "driver state" view updated every few seconds. T11's `Watershed.diamond()` is the right shape; this appendix runs the same orchestration mechanics against the NASCAR race-control feeds.

The cadence map:

* **Laps** update every few seconds (per-driver position, lap number, speed).
* **Pit reports** update every ~30 seconds (per-driver stop count, duration).
* **Flag events** fire on no fixed cadence (green / yellow / red / white).
* **Driver state** — the fused output — wants all three combined on a steady interval so a downstream dashboard sees a coherent per-driver snapshot.

Three Stream currents feed one Fjord tail. Same shape as the crypto arb scanner; different sources, different outflow logic, same five-name vocabulary. Verified: a 15-second window emits ~15 Tides and writes 100 driver-state rows to NDJSON.

> **Two entry points in this directory.** `nascar_tideweaver.py` is the
> standalone Python runner — defines `Lap` / `Pit` / `Flag` source
> classes plus an inline `outflow(state)` that reads
> `state.get("Lap")` / `state.get("Pit")` / `state.get("Flag")`.
> `race_outflow.py` is the CLI sidecar referenced from
> `watershed.json` and uses different class names — `LapData` /
> `PitStops` / `FlagEvents` — to keep the CLI-side declarations
> distinct from the Python script's. The walkthrough below tracks
> the Python runner (the simpler entry). The CLI form at the end
> uses the sidecar.

---

## The diamond (Python form)

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


HERE = Path(__file__).resolve().parent
OUT = HERE / "out"


async def main() -> None:
    OUT.mkdir(exist_ok=True)
    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(seconds=15))

    watershed = Watershed.diamond(
        window=window,
        head=Stream(
            name="laps",
            cls=Lap,
            interval=3.0,
            incorp_params={"inc_file": str(HERE / "fixtures/laps.json"),
                           "inc_code": "driver"},
        ),
        middle=[
            Stream(
                name="pits",
                cls=Pit,
                interval=3.0,
                incorp_params={"inc_file": str(HERE / "fixtures/pits.json"),
                               "inc_code": "driver"},
            ),
            Stream(
                name="flags",
                cls=Flag,
                interval=3.0,
                incorp_params={"inc_file": str(HERE / "fixtures/flags.json"),
                               "inc_code": "color"},
            ),
        ],
        tail=Fjord(
            name="state",
            interval=3.0,
            export_params={
                "file_path": str(OUT / "driver_state.ndjson"),
                "format": "ndjson",
                "if_exists": "append",
            },
        ),
        outflow=str(HERE / "_inline_outflow.py"),    # see below
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

The runnable version at [`examples/appendix/nascar-tideweaver/nascar_tideweaver.py`](./nascar_tideweaver.py) inlines the outflow into the same file via a tempdir trick rather than a sibling `.py`; the code above splits them for narrative clarity.

> **Don't pre-declare `DriverState` records.** The tail Fjord current's output class is built dynamically by the engine from the `outflow(state)` return rows. Never instantiate the output class yourself; let the Fjord build it.

---

## The outflow function

The tail Fjord current's `outflow(state)` joins laps + pits + flags into one row per driver. The keys of `state` are the upstream **class names** (`"Lap"`, `"Pit"`, `"Flag"`):

```python
def outflow(state):
    laps  = state.get("Lap", [])
    pits  = state.get("Pit", [])
    flags = state.get("Flag", [])

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

> **Guard against missing keys.** `outflow(state)` is called every Fjord tick — the first tick may fire before pits or flags have populated. Every `state.get(...)` defaults to `[]`, and every per-record read uses `getattr(..., None)` with an explicit fallback. Reading `state["Pit"]` directly would `KeyError` on the first tide.

Same structure as Tutorial 11's `arb_outflow.outflow()` — snapshot upstream registries, build a per-key composite, return as a list of dicts.

---

## CLI form

The CLI version uses a SEPARATE sidecar — [`race_outflow.py`](./race_outflow.py) — with different class names (`LapData` / `PitStops` / `FlagEvents`) referenced from `watershed.json`. The two forms are intentionally independent; pick whichever fits your deployment shape:

```bash
incorporator validate examples/appendix/nascar-tideweaver/watershed.json
incorporator tideweaver run examples/appendix/nascar-tideweaver/watershed.json --json-output
```

Run from the repo root so the relative `inc_file` / `outflow` paths resolve.

---

## Per-edge telemetry via `LoggingObserver`

The `watershed.json` above wires `"observer": {"type": "logging", ...}` on every
diamond edge.  That's the `LoggingObserver` from the canal toolkit's
`FlowObserver` hierarchy — declarative per-edge events that route through
Python `logging` without you wiring callbacks.  Four hooks fire as the
scheduler runs:

| Hook | Fires when | Default level (configurable) |
|---|---|---|
| `on_fire` | Edge consumed an upstream wave and the downstream tick fired | `info` |
| `on_skip(reason)` | Edge skipped (e.g. `"penstock_limited"`, `"hard_lock"`) | `debug` |
| `on_spillway(displaced, count)` | Reservoir overflowed and the spillway evicted a wave | `warning` |
| `on_reservoir_level(used, capacity)` | After each successful consume — useful for capacity tracking | `debug` |

Swap `"type": "logging"` for `"type": "signal"` and add a `callback=` field
to forward the same four events to a metrics pipeline (Prometheus, StatsD)
instead.  Hooks must stay synchronous — heavy work should be queued
off-thread inside the observer.

---

## Why this domain works well for Tideweaver

* **Bounded race window** — the orchestrator runs for the race duration and exits clean. No daemon to babysit between sessions.
* **Mixed cadences** — laps are fast, pits are medium, flags are bursty. Per-current intervals match the source's actual update rhythm.
* **One coherent fused output** — the dashboard wants one driver-state record per N seconds carrying the latest from all three sources. The Fjord tail does exactly that via `outflow(state)`.

---

## Where to Go Next

| Goal | Read |
|---|---|
| See the crypto-spine version of the same diamond | [Tutorial 11 — Tideweaver](../../11-tideweaver/README.md) |
| Run the non-Tideweaver fjord variant against NASCAR data | [Tutorial 9 — NASCAR Fantasy Fjord](../../09-nascar-fantasy-fjord/README.md) |
| Land columnar artifacts at window close | [Appendix — Parquet Snapshots in a Tideweaver Window](../tideweaver-parquet-snapshots/README.md) |
| Pick between in-process Tideweaver and cloud schedulers | [Appendix — Tideweaver vs. Prefect](../tideweaver-vs-prefect/README.md) |
| Configure this watershed for the CLI | [CLI & Configuration §9](../../../docs/cli_and_configuration.md#9-the-tideweaver-subcommand--windowed-orchestration) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/appendix/nascar-tideweaver/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
