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

Three Stream currents feed one Fjord tail. Same shape as the crypto arb scanner; different sources, different outflow logic, same five-name vocabulary. The fixture demo uses one uniform 3-second interval across all four currents for simplicity — a real telemetry feed would tune each source's interval to its own update rhythm. Verified: a 15-second window emits ~15 Tides and writes ~20-25 driver-state rows to NDJSON, identically from both entry forms.

> **Classes live in the main entry file.** `LapData`, `PitStops`,
> `FlagEvents`, `DriverState`, and `outflow()` are defined ONCE, in
> `nascar_tideweaver.py`. The `outflow.py` sidecar only re-imports
> them (plus the CLI-only `window_start`/`window_end` tokens) so the
> CLI's class/token resolvers see the same canonical objects the
> Python runner's own `main()` uses. `watershed.json` references the
> sidecar via `"outflow": "outflow.py"`. Both entry points stay in
> lockstep — a change to `nascar_tideweaver.py` is reflected in both
> immediately.

---

## The diamond (Python form)

```python
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from incorporator import Fjord, Incorporator, Stream, Tideweaver, Watershed
from incorporator.schema.converters import inc

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "fixtures"
OUTFLOW_PATH = HERE / "outflow.py"
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)


class LapData(Incorporator):
    """Head source — per-driver lap data."""


class PitStops(Incorporator):
    """Middle source — per-driver pit-stop counts."""


class FlagEvents(Incorporator):
    """Middle source — race-flag colour timeline."""


class DriverState(Incorporator):
    """Derived output class for the tail Fjord flush -- bare row class."""


async def main() -> None:
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
```

The runnable version is at [`examples/appendix/nascar-tideweaver/nascar_tideweaver.py`](./nascar_tideweaver.py).

> **`DriverState` is a bare row class — don't instantiate it.** The tail Fjord's output class is declared once in `nascar_tideweaver.py` and passed via `cls=DriverState`. `flush()` infers the output class fields from the dict keys that `outflow(state)` returns — the declaration gives the class its name, but the schema comes from the rows. `DriverState` carries no declared fields (`Incorporator`'s base is `extra='allow'`), so the rows' keys ARE the export shape. Never call `DriverState(...)` yourself; let the Fjord build the records from the `outflow(state)` return rows.

---

## The outflow function

The tail Fjord current's `outflow(state)` joins laps + pits + flags into one row per driver. The keys of `state` are the upstream **class names** (`"LapData"`, `"PitStops"`, `"FlagEvents"`):

```python
def outflow(state):
    laps = state.get("LapData", [])
    pits = state.get("PitStops", [])
    flags = state.get("FlagEvents", [])

    by_driver = {}
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
```

> **Build-time coercion.** `lap.lap_number` reads as a plain `int` above because
> the `laps` Stream's own `conv_dict` — `{"lap_number": inc(int, default=0)}` —
> coerces it at build time; `outflow()` never needs a read-time `int(...)` call.
> `lap.driver`, `pit.driver`, and `flags[-1].color` are plain fixture fields,
> always present on every row, so a plain dot read is correct — no defensive
> fallback is guarding a case that can't occur.

Same structure as Tutorial 11's `outflow.outflow()` — snapshot upstream registries, build a per-key composite, return as a list of dicts.

---

## Per-edge telemetry via `LoggingObserver`

The `watershed.json` above wires `"observer": {"type": "logging", ...}` on every
diamond edge.  That's the `LoggingObserver` from the canal toolkit's
`FlowObserver` hierarchy — declarative per-edge events that route through
Python `logging` without you wiring callbacks.  Four hooks fire as the
scheduler runs:

| Hook | Fires when | Default level (configurable) |
|---|---|---|
| `on_fire` | Edge consumed an upstream wave and the downstream tick fired | `debug` (this `watershed.json` raises it to `info`) |
| `on_skip(reason)` | Edge skipped (e.g. `"penstock_limited"`, `"awaiting_upstream"`) | `debug` |
| `on_spillway(displaced, count)` | Reservoir overflowed and the spillway evicted a wave | `warning` |
| `on_reservoir_level(used, capacity)` | After each successful consume — useful for capacity tracking | `debug` |

Swap `"type": "logging"` for `"type": "signal"` and add a `callback=` field
to forward the same four events to a metrics pipeline (Prometheus, StatsD)
instead.  Hooks must stay synchronous — heavy work should be queued
off-thread inside the observer.

When the three feeds drift apart, some passes skip rather than fire.  Read
`tw.rejects` after the run for a `list[RejectEntry]` whose `error_kind` is
one of `"PenstockLimited"`, `"SurgeHalted"`, `"SkipAhead"`, `"GateBlocked"`
— each record carries `from_name` / `to_name` / `cooldown_sec` so you can
group per-driver / per-edge to see which feed caused the skip.

---

## Why this domain works well for Tideweaver

* **Bounded race window** — the orchestrator runs for the race duration and exits clean. No daemon to babysit between sessions.
* **Mixed cadences in a real feed** — laps are fast, pits are medium, flags are bursty; a live pipeline would tune each Stream's `interval` to match. This fixture demo uses one uniform interval across all four currents for simplicity.
* **One coherent fused output** — the dashboard wants one driver-state record per N seconds carrying the latest from all three sources. The Fjord tail does exactly that via `outflow(state)`.

---

## Run it

```bash
# Python entry
python examples/appendix/nascar-tideweaver/nascar_tideweaver.py

# Same diamond, from the CLI
cd examples/appendix/nascar-tideweaver
incorporator tideweaver run watershed.json --json-output
```

Run the CLI form from this directory (not the repo root) so
`out/driver_state.ndjson` lands in `examples/appendix/nascar-tideweaver/out/`
— its `file_path` is resolved relative to the current working directory, not
`watershed.json`'s own location.

Also runs in Docker via the [central mount pattern](../../README.md#running-a-tutorial-in-docker) (not run or verified). Both entry points now describe the identical watershed — same 15-second window, same 3.0s intervals on every current, same 10.0s `drain_timeout` — so row counts match between the two forms.

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
