***

> 📎 **Advanced fjord pattern.**  Builds on Tutorial 7 (the two-source
> crypto-spread fjord).  Read that one first if you haven't — this
> appendix assumes you know what `outflow(state)` does and why the
> dynamic class name comes from the filename stem.

***

# 🕸️ Graph-Map Fjord: NASCAR Fantasy League (7 sources, 3 outputs, 1 config)

Tutorial 7's crypto-spread example is the *minimum viable fjord*: two
co-equal sources, one outflow, one export file.  Real production
joins are messier.  You have **dependent sources** (one source's
foreign keys reference another), **mixed API + local-file inputs**
(market data on the wire, business config in version control),
**multiple analytical views** from the same fused state, and
**sentinel values** in raw APIs that need filtering at the graph
boundary.

This appendix walks all four by porting a working NASCAR fantasy
league ETL to a single fjord pipeline.

* **7 sources** — 6 NASCAR APIs (`Track`, `Driver`, `Race`,
  `CupStanding`, `BuschStanding`, `TruckStanding`) + 1 local JSON
  file (`LeagueRoster`).  All seven seed concurrently; Race waits on
  Track + Driver via inflow's declared state dependency.
* **State-aware inflow** — `Race.track_id`,
  `Race.pole_winner_driver_id`, and `Race.winner_driver_id` resolve
  to live `Track` and `Driver` Pydantic instances via
  `link_to(state["…"])` at the inflow layer.
* **API + file source mixing** — `LeagueRoster` is loaded with
  `inc_file=str(HERE / "league_teams.json")`; every other source
  uses `inc_url=`.  Fjord's handler dispatch routes both through the
  same code path — no special casing.
* **Multi-output outflow** — one `outflow(state)` call emits **three**
  derived classes (`MonthlyRaceSchedule`, `FantasyTeam`,
  `ManufacturerLeaderboard`), each written to its own NDJSON file.
* **Field harvesting** — every output column traces back to a field
  already pulled in the seed.  Adding `track_type`, `manufacturer`,
  `winner`, `top_5`, `laps_led` cost zero extra HTTP calls — payoff
  of the framework's eager-fetch + centralised-state model.
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
   race's track name, **track type**, **track length**, **city/state**,
   pole winner, **pole speed**, **race winner** (past races), car count,
   **TV broadcaster**, and **playoff flag** all resolved from the live
   registries.  Future races' pole / winner / speed columns are
   `null` (qualifying / race hasn't happened yet).
2. Compute a **fantasy-league scoreboard** for the 8 teams in
   `league_teams.json`.  Sum each team's season points by series,
   rank teams by grand total, emit one row per team with their full
   resolved roster — every per-driver row includes **manufacturer**,
   **hometown**, **current series rank**, **wins**, **top-5s**,
   **top-10s**, **laps led**, **points**, and **points back**.  The
   team summary also carries a **manufacturer mix** counter (Chevy
   vs Ford vs Toyota distribution) and a total-wins tally.
3. Compute a **manufacturer leaderboard** — Chevrolet / Ford /
   Toyota — with driver count, total points, total wins, playoff
   seats, and the top driver per make.  Demonstrates a third
   analytical view that's "free" because the Standings data is
   already in memory.
4. Export all three views as NDJSON, in **one** fjord call.

Seven sources (six API + one local JSON), one outflow function,
three output files, no daemons, no manual joins.

---

## 🧱 The Sources

Six HTTP endpoints + one local JSON file:

```text
https://cf.nascar.com/cacher/tracks.json                    →  Track          (49 rows)
https://cf.nascar.com/cacher/drivers.json                   →  Driver         (917 rows)
https://cf.nascar.com/cacher/{YEAR}/race_list_basic.json    →  Race           (40 rows)
https://cf.nascar.com/data/cacher/production/{YEAR}/1/...   →  CupStanding    (39 rows)
https://cf.nascar.com/data/cacher/production/{YEAR}/2/...   →  BuschStanding  (59 rows)
https://cf.nascar.com/data/cacher/production/{YEAR}/3/...   →  TruckStanding  (61 rows)
examples/fjord_code/league_teams.json                       →  LeagueRoster   (8 rows)
```

**`Race` has three foreign keys** into the registries — `track_id`
into `Track`, `pole_winner_driver_id` and `winner_driver_id` into
`Driver`.  The three standings endpoints share the same response
shape but **must** be distinct classes so their registries don't
collide on a shared `inc_dict`.

**`LeagueRoster`** is the seventh source — a hand-curated JSON file
that lives next to the outflow code.  Fjord's handler dispatch
routes the `inc_file=` and `inc_url=` paths through the same code,
so the file source registers as a normal `Incorporator` subclass
indistinguishable from the API-fed ones.

Fjord's parallel-seed phase loads any source whose `inflow(state)`
return doesn't reference its peers; only the ones that do reference
peers wait.  Six of the seven (`Track`, `Driver`, `LeagueRoster`,
and the three Standings classes) load concurrently; `Race` waits
for `Track` + `Driver` so its three `link_to(state["…"])` resolvers
land correctly.

---

## 🔧 Step 1: The Outflow Sidecar

`examples/fjord_code/nascar_fantasy.py` defines the six source
classes, the league rosters, the `inflow(state)` wiring, and the
`outflow(state)` function.  The file is the entire ETL — fjord
imports it, registers the classes, and drives the pipeline.

```python
from datetime import datetime
from typing import Any, Dict, List
from incorporator import Incorporator, inc, link_to


# Seven source classes — one IncorporatorList per registry.
# LeagueRoster is the seventh: seeded from a local JSON file via
# inc_file= instead of inc_url=.  Same handler dispatch; no special casing.
class Track(Incorporator):         pass
class Driver(Incorporator):        pass
class Race(Incorporator):          pass
class CupStanding(Incorporator):   pass
class BuschStanding(Incorporator): pass
class TruckStanding(Incorporator): pass
class LeagueRoster(Incorporator):  pass


_DATE_FIELDS = ("date_scheduled", "race_date", "qualifying_date", "tunein_date")
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
    re-applies it on every refresh wave so the link_to closures see
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

> **`refresh_params=None` everywhere = single-wave test mode.**  Drop
> those lines (refresh defaults to on with a 60s interval) and the
> daemons stay alive — perfect for production but blocks the
> `async for` loop indefinitely.  Mix and match: leave `Track`'s
> refresh off (tracks never change) while letting standings refresh
> every 5 minutes.

---

## 🏁 The Run

A single wave against the live NASCAR endpoints:

```
🏁 Initiating NASCAR Data Gateway (fjord)...

✅ fjord_incorp:Track                  chunk 1: 49 rows
✅ fjord_incorp:Driver                 chunk 1: 917 rows
✅ fjord_incorp:Race                   chunk 1: 40 rows
✅ fjord_incorp:CupStanding            chunk 1: 39 rows
✅ fjord_incorp:BuschStanding          chunk 1: 59 rows
✅ fjord_incorp:TruckStanding          chunk 1: 61 rows
✅ fjord_incorp:LeagueRoster           chunk 1: 8 rows
✅ outflow:MonthlyRaceSchedule         chunk 1: 5 rows
✅ outflow:FantasyTeam                 chunk 1: 8 rows
✅ outflow:ManufacturerLeaderboard     chunk 1: 3 rows

✅ Pipeline complete.
```

### Output 1 — `nascar_monthly_schedule.ndjson`

Per-race row with resolved track + driver context.  Past races have
real `pole_winner`, `pole_speed`, and `winner`; future races have
all three as `null` (sentinel filter + outflow `getattr` fallbacks):

```jsonc
{
  "race_id":     5606,
  "date":        "2026-05-03",
  "race_name":   "Würth 400 presented by LIQUI MOLY",
  "track":       "Texas Motor Speedway",
  "track_type":  "Intermediate",
  "track_miles": 1.5,
  "track_loc":   "Fort Worth, TX",
  "pole_winner": "Carson Hocevar",
  "pole_speed":  191.34,
  "winner":      "Chase Elliott",        // ← past race: known winner
  "cars":        38,
  "tv":          "FS1",
  "playoff":     false
}
{
  "race_id":     5611,
  "date":        "2026-05-31",
  "race_name":   "Cracker Barrel 400",
  "track":       "Nashville Superspeedway",
  "track_type":  "Intermediate",
  "track_miles": 1.333,
  "track_loc":   "Lebanon, TN",
  "pole_winner": null,                   // ← future race
  "pole_speed":  null,
  "winner":      null,
  "cars":        40,
  "tv":          "PRIME VIDEO",
  "playoff":     false
}
```

### Output 2 — `nascar_fantasy_scoreboard.ndjson`

One row per team, sorted by total score.  Each roster row carries
the driver's manufacturer, hometown, current series rank, and the
full season stats (top-5, top-10, laps led, points, gap to leader).
The team summary block carries the manufacturer mix and total wins:

```jsonc
{
  "team_id":          "Intim'tor",
  "total_score":      3058,
  "total_wins":       10,
  "manufacturer_mix": {"Chevrolet": 3, "Ford": 3, "Toyota": 2},
  "roster": [
    {
      "series":       "Cup",
      "car_idx":      1,
      "name":         "Kyle Larson",
      "car":          "5",
      "team":         "Hendrick Motorsports",
      "manufacturer": "Chevrolet",
      "hometown":     "Elk Grove, California",
      "rank":         8,
      "wins":         0,
      "t10":          6,
      "top_5":        3,
      "laps_led":     499,
      "points":       332,
      "points_back":  235
    },
    // ... 7 more roster rows
  ],
  "points": [...]
}
```

### Output 3 — `nascar_manufacturer_leaderboard.ndjson`

Cup-series manufacturer rollup — one row per Chevrolet / Ford /
Toyota, sorted by total points:

```jsonc
{"manufacturer":"Chevrolet", "drivers":20, "total_points":3941, "total_wins":4, "playoff_seats":20, "top_driver":"Chase Elliott", "top_points":422}
{"manufacturer":"Toyota",    "drivers":9,  "total_points":2867, "total_wins":7, "playoff_seats":9,  "top_driver":"Tyler Reddick", "top_points":567}
{"manufacturer":"Ford",      "drivers":10, "total_points":2677, "total_wins":1, "playoff_seats":10, "top_driver":"Ryan Blaney",   "top_points":405}
```

Note how **Toyota has fewer drivers than Chevrolet (9 vs 20) but
nearly twice the wins (7 vs 4)** — that's the kind of insight a
manufacturer leaderboard surfaces and the legacy ETL hid in memory.

---

## 🐳 Run It From the CLI

The same seven-source pipeline expressed as a JSON config — no
Python wrapper required.  The CLI loader resolves `cls_name` by
importing the `outflow=` file and looking up each class by name,
which is how the multi-source registration stays JSON-serialisable.

### `config/pipeline.json`

```json
{
  "inflow":  "examples/fjord_code/nascar_fantasy.py",
  "outflow": "examples/fjord_code/nascar_fantasy.py",
  "stream_params": [
    {
      "cls_name": "Track",
      "incorp_params": {
        "inc_url":   "https://cf.nascar.com/cacher/tracks.json",
        "rec_path":  "items",
        "inc_code":  "track_id",
        "inc_name":  "track_name"
      },
      "refresh_params": null
    },
    {
      "cls_name": "Driver",
      "incorp_params": {
        "inc_url":   "https://cf.nascar.com/cacher/drivers.json",
        "rec_path":  "response",
        "inc_code":  "Nascar_Driver_ID",
        "inc_name":  "Full_Name",
        "excl_lst":  ["Series_Logo", "Short_Name", "Description", "Hobbies",
                      "Children", "Residing_City", "Residing_State",
                      "Residing_Country", "Image_Transparent", "SecondaryImage",
                      "Career_Stats", "Age", "Rank", "Points", "Points_Behind",
                      "No_Wins", "Poles", "Top5", "Top10", "Laps_Led",
                      "Stage_Wins", "Playoff_Points", "Playoff_Rank",
                      "Integrated_Sponsor_Name", "Integrated_Sponsor",
                      "Integrated_Sponsor_URL", "Silly_Season_Change",
                      "Silly_Season_Change_Description", "Driver_Post_Status",
                      "Driver_Part_Time"]
      }
    },
    {
      "cls_name": "Race",
      "incorp_params": {
        "inc_url":   "https://cf.nascar.com/cacher/2026/race_list_basic.json",
        "rec_path":  "series_1",
        "inc_code":  "race_id",
        "inc_name":  "race_name",
        "excl_lst":  ["schedule", "track_name"],
        "name_chg":  [["track_id", "track"]]
      }
    },
    {
      "cls_name": "CupStanding",
      "incorp_params": {
        "inc_url":   "https://cf.nascar.com/data/cacher/production/2026/1/racinginsights-points-feed.json",
        "inc_code":  "driver_id",
        "inc_name":  "driver_name",
        "excl_lst":  ["is_clinch", "driver_first_name", "driver_last_name",
                      "driver_suffix", "playoff_stage_wins"],
        "conv_dict": {
          "points":   "calc(int, default=0, target_type=int)",
          "wins":     "calc(int, default=0, target_type=int)",
          "top_10":   "calc(int, default=0, target_type=int)",
          "top_5":    "calc(int, default=0, target_type=int)",
          "laps_led": "calc(int, default=0, target_type=int)",
          "position": "calc(int, default=0, target_type=int)"
        }
      }
    },
    {
      "cls_name": "BuschStanding",
      "incorp_params": { "...same shape as CupStanding, /2/ endpoint": "..." }
    },
    {
      "cls_name": "TruckStanding",
      "incorp_params": { "...same shape as CupStanding, /3/ endpoint": "..." }
    },
    {
      "cls_name": "LeagueRoster",
      "incorp_params": {
        "inc_file":  "config/league_teams.json",
        "inc_code":  "team_id",
        "inc_name":  "team_id"
      },
      "refresh_params": null
    }
  ],
  "export_params": {
    "MonthlyRaceSchedule":     {"file_path": "data/nascar_monthly_schedule.ndjson"},
    "FantasyTeam":             {"file_path": "data/nascar_fantasy_scoreboard.ndjson"},
    "ManufacturerLeaderboard": {"file_path": "data/nascar_manufacturer_leaderboard.ndjson"}
  },
  "refresh_interval": {
    "Driver":        3600,
    "Race":          600,
    "CupStanding":   300,
    "BuschStanding": 300,
    "TruckStanding": 300
  },
  "export_interval": 60
}
```

A few JSON-specific notes:

* **`refresh_params: null`** is the JSON spelling of Python's
  `refresh_params=None` — opts the source out of the refresh
  daemon.  Used here for `Track` (tracks never change) and
  `LeagueRoster` (rosters change rarely; restart the daemon when
  you edit the file).
* **`conv_dict` values are quoted strings.**  The token resolver in
  `cli/tokens.py` parses them at config-load time and substitutes
  the real callables.  `calc(int, default=0, target_type=int)`
  becomes the actual converter; same for `inc(datetime)` etc.
  `link_to(state["…"])` calls live in `inflow(state)` — not in the
  JSON — because they need the runtime registry handle.
* **`name_chg` uses arrays not tuples.**  JSON has no tuple
  literal; `["track_id", "track"]` deserialises to the same shape
  the Python code uses.
* **`refresh_interval` as a dict** keyed by class name, exactly
  like Python — JSON-friendly out of the box.
* **`export_params` keyed by output class** — multi-output detection
  is "is there a top-level `file_path` key?  No → multi-output."

### Validate + run

```bash
incorporator validate config/pipeline.json
incorporator fjord    config/pipeline.json --logs
```

`--logs` routes every Wave through the `LoggedIncorporator` queue
handler into `logs/api.log` (success) and `logs/error.log` (failures
with redacted URLs).  Add `--heartbeat-file /tmp/inc.beat` to pair
with Docker's `HEALTHCHECK`.

---

## 🐳 Run It in Docker

The repo's `docker-compose.yml` and `Dockerfile` already work for
this pipeline.  Three host folders bind-mount into the container:

| Host | Container | What goes here |
|---|---|---|
| `./config` | `/app/config` *(read-only)* | `pipeline.json` **and** `league_teams.json` (the `inc_file` source) |
| `./data` | `/app/data` | Three NDJSON outputs land here |
| `./logs` | `/app/logs` | Rotating JSON log files (when `--logs` is set) |

The key wrinkle compared to a single-source fjord: **the
`league_teams.json` file must live where the container can read it**.
Easiest pattern is to drop it next to `pipeline.json` in `config/`
and reference it with a container-relative path:

```bash
# 1. Lay out the host folders.
mkdir -p config data logs
cp examples/fjord_code/league_teams.json config/league_teams.json

# 2. Write config/pipeline.json (see the JSON above) with the
#    container path:
#         "inc_file": "config/league_teams.json"
#    NOT  "inc_file": "examples/fjord_code/league_teams.json"
#    — the container only sees /app/config and /app/data.

# 3. Also drop the inflow/outflow sidecar in config/ so the
#    container can import it:
cp examples/fjord_code/nascar_fantasy.py config/nascar_fantasy.py
#    and point pipeline.json at the container-side path:
#         "inflow":  "config/nascar_fantasy.py",
#         "outflow": "config/nascar_fantasy.py"

# 4. Validate + launch.
incorporator validate config/pipeline.json
docker compose up -d
docker compose logs -f
```

`docker compose up -d` starts the long-running fjord daemon with the
`refresh_interval` + `export_interval` cadences above.  The
container's `HEALTHCHECK` watches the heartbeat file (touched after
every Wave); a stalled daemon is auto-restarted by the orchestrator
(compose / swarm / k8s).

### Refresh schedule for NASCAR's update cadence

The defaults in the JSON above are tuned for live-season operation:

| Source | Cadence | Rationale |
|---|---:|---|
| `Track` | refresh off | Tracks never change mid-season |
| `Driver` | 1 h | Crew-chief / sponsor swaps occasionally |
| `Race` | 10 min | Pole winner finalises Saturday; race winner Sunday |
| `CupStanding` / `BuschStanding` / `TruckStanding` | 5 min | Live points update during/after each race |
| `LeagueRoster` | refresh off | Edit the JSON + restart the daemon |
| Outflow wave | 60 s | Fused export every minute |

For an off-season demo (one-shot run with no refresh), set every
`refresh_params: null` and drop `refresh_interval` / `export_interval`
entirely — the pipeline exits cleanly after one outflow wave.

### Secrets aren't required

This pipeline calls *only* public NASCAR endpoints — no API keys, no
auth.  The `${API_KEY}` / `${file:/run/secrets/...}` patterns
documented in the
[deployment guide](../deployment.md#secrets--local-vs-production)
apply if you ever swap one of the sources for a paid feed.

---

## 🧠 What This Demonstrates

| Pattern | Where to look |
|---|---|
| **Concurrent seed of N independent sources** | The seven `stream_params` entries — six API + one file all start in parallel via `asyncio.gather` |
| **API + file source mixing** | `LeagueRoster` uses `inc_file=`; the other six use `inc_url=`.  Same handler dispatch routes both transparently |
| **Sequential seed when state matters** | `Race` waits for `Track` + `Driver` because its `inflow(state)` return references them; the others stay parallel |
| **Live foreign-key resolution** | `link_to(state["Track"])` / `link_to(state["Driver"], extractor=…)` in the inflow's `conv_dict` |
| **Sentinel-ID filter** | `extractor=_driver_id_or_none` short-circuits ID 0 to `None` at the graph boundary — applied to BOTH `pole_winner_driver_id` and `winner_driver_id` |
| **Multi-output dict return** | `outflow(state) -> {"MonthlyRaceSchedule": …, "FantasyTeam": …, "ManufacturerLeaderboard": …}` → three derived classes, three files |
| **Per-class export config** | Top-level `export_params` keyed by class name |
| **Field harvesting** | Every output column traces back to a field already pulled in the seed; no extra API call to add `track_type` / `manufacturer` / `winner`.  Payoff for the framework's eager-fetch / centralised-state model |
| **Config externalisation** | Fantasy rosters live in `league_teams.json`, not Python — editing the league no longer requires touching code |
| **Single-wave test mode** | `refresh_params=None` on every entry, no `export_interval` → the pipeline exits after one outflow wave |
| **Pure-data outflow function** | The `outflow(state)` is a normal Python function — no async, no daemon plumbing, no lock acquisition. Fjord takes care of all that |

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
