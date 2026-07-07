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

Three Stream currents feed one Fjord tail. Same shape as the crypto arb scanner; different sources, different outflow logic, same five-name vocabulary. Verified: a 15-second window emits ~15 Tides and writes ~20-25 driver-state rows to NDJSON.

> **One shared sidecar.** Both `nascar_tideweaver.py` (Python runner)
> and `watershed.json` (CLI entry) load their class definitions and
> `outflow(state)` logic from the same `outflow.py` sidecar. The
> Python runner imports `LapData`, `PitStops`, `FlagEvents`, and
> `DriverState` directly; `watershed.json` references the file via
> `"outflow": "outflow.py"`. Both entry points stay in lockstep — a
> change to `outflow.py` is reflected in both immediately.

---

## The diamond (Python form)

```python
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

if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from outflow import LAPDATA_CONV_DICT, DriverState, FlagEvents, LapData, PitStops  # noqa: E402


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
```

The runnable version is at [`examples/appendix/nascar-tideweaver/nascar_tideweaver.py`](./nascar_tideweaver.py).

> **`DriverState` is declared in the sidecar — don't instantiate it.** The tail Fjord's output class lives in `outflow.py` and is passed via `cls=DriverState`. `flush()` infers the output class fields from the dict keys that `outflow(state)` returns — the declaration gives the class its name, but the schema comes from the rows. `DriverState` is a bare class (no declared fields, no `extra='allow'`), so if the rows carry undeclared keys the framework emits one WARNING and falls through to inference, preserving every field. Never call `DriverState(...)` yourself; let the Fjord build the records from the `outflow(state)` return rows.

---

## The outflow function

The tail Fjord current's `outflow(state)` joins laps + pits + flags into one row per driver. The keys of `state` are the upstream **class names** (`"LapData"`, `"PitStops"`, `"FlagEvents"`):

```python
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
        row["laps"] = max(row["laps"], lap.lap_number)

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

> **Guard against missing keys.** `outflow(state)` is called every Fjord tick — the first tick may fire before pits or flags have populated. Every `state.get(...)` defaults to `[]`, and every per-record read uses `getattr(..., None)` with an explicit fallback. Reading `state["PitStops"]` directly would `KeyError` on the first tide.

> **Build-time coercion, read-time selection.** `lap.lap_number` reads as a plain
> `int` above because the `laps` Stream's own `conv_dict` — `LAPDATA_CONV_DICT =
> {"lap_number": inc(int, default=0)}` in `outflow.py` — coerces it at build
> time; `outflow()` no longer needs `int(getattr(lap, "lap_number", 0) or 0)`.
>
> **Why the driver-or-inc_code fallback stays read-time.** `getattr(lap,
> "driver", None) or getattr(lap, "inc_code", None)` is field SELECTION, not
> coercion — there's no earlier point at which a conv_dict callable could read
> "what `inc_code` will resolve to," because `inc_code` is the framework-assigned
> PK bound from `incorp_params={"inc_code": "driver"}` *after* conv_dict
> resolution runs. This is a genuine framework boundary (same shape as
> mlb-pulse's honest-boundary note on its cross-current join), not a missed DX
> opportunity — so it stays a plain read-time expression.

Same structure as Tutorial 11's `outflow.outflow()` — snapshot upstream registries, build a per-key composite, return as a list of dicts.

---

## CLI form

The CLI entry uses [`watershed.json`](./watershed.json) with `"outflow": "outflow.py"` — the same sidecar the Python runner imports. Class names (`LapData` / `PitStops` / `FlagEvents` / `DriverState`) are shared:

```bash
incorporator validate examples/appendix/nascar-tideweaver/watershed.json
incorporator tideweaver run examples/appendix/nascar-tideweaver/watershed.json --json-output
```

The CLI resolves `inc_file`, `inflow`, and `outflow` relative to `watershed.json`'s directory, so these commands work from any working directory. `export_params.file_path` (`"data/driver_state.ndjson"`) is CWD-relative — the output file lands in `<your working directory>/data/`, not alongside the config.

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
