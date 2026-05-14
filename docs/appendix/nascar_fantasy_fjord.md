***

> 📎 **Advanced fjord pattern.**  Builds on Tutorial 7 (the two-source
> crypto-spread fjord).  Read that one first if you haven't — this
> appendix assumes you know what `outflow(state)` does and why the
> dynamic class name comes from the filename stem.

***

# 🕸️ Graph-Map Fjord: NASCAR Fantasy League (6 sources, 2 outputs, 1 config)

Tutorial 7's crypto-spread example is the *minimum viable fjord*: two
co-equal sources, one outflow, one export file.  Real production
joins are messier.  You have **dependent sources** (one source's
foreign keys reference another), **multiple analytical views** to
publish from the same fused state, and **sentinel values** in raw
APIs that need filtering at the graph boundary.

This appendix walks all three by porting a working NASCAR fantasy
league ETL to a single fjord pipeline.

* **6 sources** seeded concurrently — `Track`, `Driver`, `Race`,
  `CupStanding`, `BuschStanding`, `TruckStanding`.
* **State-aware inflow** — `Race.track_id` and
  `Race.pole_winner_driver_id` resolve to live `Track` and `Driver`
  Pydantic instances via `link_to(state["…"])` at the inflow layer.
* **Multi-output outflow** — one `outflow(state)` call emits **two**
  derived classes (`MonthlyRaceSchedule`, `FantasyTeam`), each
  written to its own NDJSON file.
* **Sentinel filter** — NASCAR's API returns `pole_winner_driver_id = 0`
  for races whose qualifying hasn't happened yet.  Driver ID 0
  happens to be a real driver in the registry ("Kris Wright"), so a
  naked `link_to` resolves every future race to the same phantom
  pole winner.  We dodge it with a one-line `extractor=` on the inflow's
  `link_to` call — exactly the escape hatch `link_to`'s designer had
  in mind.

By the end you'll know how to wire a relational join across N
sources in one pipeline.json (or one async-for loop), produce
multiple analytical views from one fused state, and filter sentinel
IDs at the graph boundary instead of the consumer.

Runnable code lives at
[`examples/fjord_code/nascar_fantasy.py`](../../examples/fjord_code/nascar_fantasy.py)
and [`examples/nascar_fantasy_fjord.py`](../../examples/nascar_fantasy_fjord.py).

---

## 🎯 The Goal

For the current NASCAR Cup, Busch, and Truck seasons:

1. Build a **monthly race schedule** for the Cup series, with each
   race's track name and pole-winner name resolved from the live
   driver/track registries.  Skip future races' pole winners
   (qualifying hasn't happened yet — show "TBD").
2. Compute a **fantasy-league scoreboard** for 8 hardcoded teams.
   Each team picks one Cup driver, one Busch driver, six more Cup
   drivers, etc.  Sum their season points by series, rank teams by
   grand total, emit one row per team with their full resolved
   roster (driver name, team, wins, top-10s, points-by-series,
   percentages).
3. Export both views as NDJSON, in **one** fjord call.

Six APIs, one outflow function, two output files, no daemons, no
manual joins.

---

## 🧱 The Sources

The naive Python ETL hits six endpoints and reassembles the graph by
hand:

```text
https://cf.nascar.com/cacher/tracks.json                    →  Track
https://cf.nascar.com/cacher/drivers.json                   →  Driver
https://cf.nascar.com/cacher/{YEAR}/race_list_basic.json    →  Race
https://cf.nascar.com/data/cacher/production/{YEAR}/1/...   →  CupStanding
https://cf.nascar.com/data/cacher/production/{YEAR}/2/...   →  BuschStanding
https://cf.nascar.com/data/cacher/production/{YEAR}/3/...   →  TruckStanding
```

`Race` has foreign keys into both `Track` (`track_id`) and `Driver`
(`pole_winner_driver_id`).  The three standings endpoints share the
same response shape but **must** be distinct classes so their
registries don't collide on a shared `inc_dict`.

Fjord's parallel-seed phase loads any source whose `inflow(state)`
return doesn't reference its peers; only the ones that do reference
peers wait.  `Track`, `Driver`, and the three Standings classes load
concurrently; `Race` waits for `Track` + `Driver` to land so its
`link_to(state["Track"])` and `link_to(state["Driver"])` resolve
correctly.

---

## 🔧 Step 1: The Outflow Sidecar

`examples/fjord_code/nascar_fantasy.py` defines the six source
classes, the league rosters, the `inflow(state)` wiring, and the
`outflow(state)` function.  The file is the entire ETL — fjord
imports it, registers the classes, and drives the pipeline.

```python
from datetime import datetime
from typing import Any, Dict, List, Tuple
from incorporator import Incorporator, calc, inc, link_to


# 1. Six source classes — one IncorporatorList per registry.
class Track(Incorporator):         pass
class Driver(Incorporator):        pass
class Race(Incorporator):          pass
class CupStanding(Incorporator):   pass
class BuschStanding(Incorporator): pass
class TruckStanding(Incorporator): pass


_DATE_FIELDS = ("date_scheduled", "race_date", "qualifying_date", "tunein_date")


# 2. Fantasy-league rosters — keyed by team handle, values are
#    (series_id_float, driver_id) tuples.  Series 1 = Cup, 2 = Busch,
#    3 = Truck.  Pure data, lives next to the outflow it feeds.
LEAGUE_TEAMS_RAW: Dict[str, List[Tuple[float, int]]] = {
    "Queen":     [(3.0, 4235), (2.0, 4441), (1.0, 3989), ...],
    "Intim'tor": [(3.0, 4312), (2.0, 34),   (1.0, 4030), ...],
    # ... 6 more teams
}
```

### The Sentinel Filter

NASCAR's race-list API returns `pole_winner_driver_id = 0` for races
whose pole qualifying hasn't happened yet (or was rained out).  Run
this query against the live API to confirm:

```python
>>> json.loads(urlopen("https://cf.nascar.com/cacher/2026/race_list_basic.json").read())["series_1"]
# 5597  2026-02-22  Autotrader 400         | pole_id = 0     ← sentinel
# 5596  2026-02-15  DAYTONA 500            | pole_id = 454   ← real driver
```

A naked `link_to(state["Driver"])` would happily look up ID 0 in the
driver registry — and because NASCAR's auto-numbered driver list
includes a low-budget driver at exactly ID 0 (currently "Kris
Wright"), every future race's pole winner would resolve to that one
incidental name.  The user-facing report would say "Kris Wright is
the pole winner for every race in May".

The fix is a 3-line helper used as `link_to`'s `extractor=`:

```python
def _pole_id_or_none(raw: Any) -> Any:
    """0 / None / "" → None so link_to short-circuits the registry lookup."""
    return raw if raw else None
```

`link_to` runs the extractor on the raw column value **before** it
hits the registry.  When the extractor returns `None`, the lookup is
skipped and the field stays `None` — and the outflow's existing
`getattr(pole, "Full_Name", "TBD") if pole else "TBD"` branch fires
naturally for every "no pole yet" race.

> 💡 **The pattern generalises.**  Any third-party API with sentinel
> IDs (Discord's `-1`-as-deleted-user, Twitter's `0`-as-anon-author,
> a SQL `NULL` foreign-key) lands at the same boundary: write a tiny
> extractor that converts the sentinel to `None` and let
> `link_to`'s short-circuit do the rest.  No per-call guard in
> consumer code.

### State-aware inflow

```python
def inflow(state: Dict[str, Any]) -> Dict[str, Any]:
    """Wire Race's foreign keys against the live Track + Driver registries.

    Inflow runs BEFORE each source's incorp().  On the first calls
    (Track / Driver / Standings) ``state`` is empty / partial, so
    Race's override only emits once its peers exist — fjord then
    re-applies it on every refresh tick so the link_to closures see
    fresh peer snapshots.
    """
    overrides: Dict[str, Any] = {}
    if "Track" in state and "Driver" in state:
        overrides["Race"] = {
            "conv_dict": {
                "track_id":              link_to(state["Track"]),
                "pole_winner_driver_id": link_to(state["Driver"], extractor=_pole_id_or_none),
                **{key: inc(datetime) for key in _DATE_FIELDS},
            }
        }
    return overrides
```

### Multi-output outflow

`outflow(state)` returns a `dict[ClassName, list[dict]]` and fjord
builds one dynamic Incorporator subclass per dict key:

```python
def outflow(state: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    drivers = state.get("Driver")
    races   = state.get("Race")
    if drivers is None or races is None:
        return {}

    points_standings = {
        1: state.get("CupStanding"),
        2: state.get("BuschStanding"),
        3: state.get("TruckStanding"),
    }

    # ── View 1: Cup schedule for the current month ──────────────
    now = datetime.now()
    monthly = []
    for race in races:
        dt = getattr(race, "date_scheduled", None)
        if dt is None or dt.month != now.month or dt.year != now.year:
            continue
        pole  = getattr(race, "pole_winner_driver_id", None)
        track = getattr(race, "track", None) or getattr(race, "track_id", None)
        monthly.append({
            "race_id":     race.inc_code,
            "date":        dt.strftime("%Y-%m-%d"),
            "race_name":   getattr(race, "race_name", "TBD"),
            "track":       getattr(track, "inc_name", "Unknown") if track else "Unknown",
            "pole_winner": getattr(pole, "Full_Name", "TBD") if pole else "TBD",
            "cars":        getattr(race, "number_of_cars_in_field", 0),
        })
    monthly.sort(key=lambda r: r["date"])

    # ── View 2: Fantasy-league scoreboard ────────────────────────
    # (full code in examples/fjord_code/nascar_fantasy.py)
    fantasy = _build_scoreboard(drivers, points_standings)

    return {
        "MonthlyRaceSchedule": monthly,
        "FantasyTeam":         fantasy,
    }
```

Two keys in the return dict → two derived classes
(`MonthlyRaceSchedule`, `FantasyTeam`) → two output files.

---

## 🔧 Step 2: The Driver Script

`examples/nascar_fantasy_fjord.py` calls fjord with the six sources
+ per-class export targets:

```python
import asyncio
from pathlib import Path
from incorporator import Incorporator, calc

from examples.fjord_code.nascar_fantasy import (
    BuschStanding, CupStanding, Driver, Race, Track, TruckStanding,
)

HERE = Path(__file__).parent
DATA = HERE.parent / "data"

CURRENT_YEAR = 2026
CFC_BASE   = "https://cf.nascar.com/cacher"
PROD_BASE  = f"https://cf.nascar.com/data/cacher/production/{CURRENT_YEAR}"
STANDINGS  = "racinginsights-points-feed.json"


async def main() -> None:
    DATA.mkdir(exist_ok=True)

    async for wave in Incorporator.fjord(
        stream_params=[
            {"cls": Track,         "incorp_params": {...}, "refresh_params": None},
            {"cls": Driver,        "incorp_params": {...}, "refresh_params": None},
            {"cls": Race,          "incorp_params": {...}, "refresh_params": None},
            {"cls": CupStanding,   "incorp_params": {...}, "refresh_params": None},
            {"cls": BuschStanding, "incorp_params": {...}, "refresh_params": None},
            {"cls": TruckStanding, "incorp_params": {...}, "refresh_params": None},
        ],
        inflow=str(HERE  / "fjord_code/nascar_fantasy.py"),
        outflow=str(HERE / "fjord_code/nascar_fantasy.py"),
        export_params={                                       # ← multi-output: nested dict
            "MonthlyRaceSchedule": {"file_path": str(DATA / "nascar_monthly_schedule.ndjson")},
            "FantasyTeam":         {"file_path": str(DATA / "nascar_fantasy_scoreboard.ndjson")},
        },
        # refresh_interval={...}  ← uncomment for long-running daemon mode
    ):
        op = wave.operation
        print(f"✅ {op:35s} chunk {wave.chunk_index}: {wave.rows_processed} rows")
```

> **Same-file inflow + outflow.**  Both `inflow=` and `outflow=`
> point at the same Python file because `inflow(state)` and
> `outflow(state)` live side-by-side in `nascar_fantasy.py`.  Fjord
> loads the module once via `importlib`'s cache, so the second
> import is free.

> **`refresh_params=None` everywhere = single-tick test mode.**  Drop
> those lines (refresh defaults to on with a 60s interval) and the
> daemons stay alive — perfect for production but blocks the
> `async for` loop indefinitely.  Mix and match: leave `Track`'s
> refresh off (tracks never change) while letting standings refresh
> every 5 minutes.

---

## 🏁 The Run

A single tick against the live NASCAR endpoints:

```
🏁 Initiating NASCAR Data Gateway (fjord)...

✅ fjord_incorp:Track                  chunk 1: 49 rows
✅ fjord_incorp:Driver                 chunk 1: 917 rows
✅ fjord_incorp:Race                   chunk 1: 40 rows
✅ fjord_incorp:CupStanding            chunk 1: 39 rows
✅ fjord_incorp:BuschStanding          chunk 1: 59 rows
✅ fjord_incorp:TruckStanding          chunk 1: 61 rows
✅ outflow:MonthlyRaceSchedule         chunk 1: 5 rows
✅ outflow:FantasyTeam                 chunk 1: 8 rows

✅ Pipeline complete.
```

`data/nascar_monthly_schedule.ndjson` — verified pole-winner fix:

```jsonc
{"date":"2026-05-03", "race_name":"Würth 400 …",          "pole_winner":"Carson Hocevar"}     // past, real
{"date":"2026-05-10", "race_name":"Go Bowling at The Glen", "pole_winner":"Shane van Gisbergen"} // past, real
{"date":"2026-05-17", "race_name":"NASCAR All-Star Race",   "pole_winner":"TBD"}                // future
{"date":"2026-05-24", "race_name":"Coca-Cola 600",          "pole_winner":"TBD"}                // future
{"date":"2026-05-31", "race_name":"Cracker Barrel 400",     "pole_winner":"TBD"}                // future
```

`data/nascar_fantasy_scoreboard.ndjson` — one row per team, sorted
by total score descending:

```jsonc
{"team_id":"Intim'tor", "total_score":3058, "roster":[
    {"series":"Cup", "car_idx":1, "name":"Kyle Larson", "car":"5",
     "team":"Hendrick Motorsports", "wins":0, "t10":6, "points":332},
    ...
]}
```

---

## 🧠 What This Demonstrates

| Pattern | Where to look |
|---|---|
| **Concurrent seed of N independent sources** | The six `stream_params` entries — all incorp calls fire via `asyncio.gather` |
| **Sequential seed when state matters** | `Race` waits for `Track` + `Driver` because its `inflow(state)` return references them; the others stay parallel |
| **Live foreign-key resolution** | `link_to(state["Track"])` / `link_to(state["Driver"])` in the inflow's `conv_dict` |
| **Sentinel-ID filter** | `extractor=_pole_id_or_none` short-circuits ID 0 to `None` at the graph boundary |
| **Multi-output dict return** | `outflow(state) -> {"MonthlyRaceSchedule": ..., "FantasyTeam": ...}` → two derived classes |
| **Per-class export config** | Top-level `export_params` keyed by class name |
| **Single-tick test mode** | `refresh_params=None` on every entry, no `export_interval` → the pipeline exits after one outflow tick |
| **Pure-data outflow function** | The 100-line `outflow(state)` is a normal Python function — no async, no daemon plumbing, no lock acquisition. Fjord takes care of all that |

---

## 📚 See Also

* **[Tutorial 7 — Multi-Source Fjord](../7_multi_source_fjord.md)** —
  the simpler two-source crypto-spread fjord that introduces the
  basic vocabulary.  Read it first if you haven't.
* **[Tutorial 4 — Parent-Child Drilling](../4_parent_child_drilling.md)** —
  HATEOAS pattern (parent records → child URLs).  Often pairs with
  the graph-map pattern above when a source's foreign key is a URL
  rather than an ID.
* **[Crypto Graph Mapping (static)](./crypto_graph_mapping.md)** —
  the pure-Python version of the same join pattern, without the
  daemon scaffolding.  Reach for it when you need a one-shot ETL
  rather than a long-running fjord.
* **[CLI & Configuration Guide](../cli_and_configuration.md)** —
  the same pipeline expressed as `pipeline.json` and run from the
  CLI.  Multi-output `export_params` shape works in JSON too.
* **[Library reference](../library_reference.md)** —
  full signatures for `fjord()`, `link_to()`, `inflow(state)`.
