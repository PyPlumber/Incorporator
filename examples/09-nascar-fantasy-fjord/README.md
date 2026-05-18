***

# 🕸️ Tutorial 9 — NASCAR Fantasy Fjord: 7 sources, 3 outputs, 1 config

**Prerequisites:** [Tutorial 8 — Streaming Daemons](../08-streaming-daemon/README.md) (the `stream()` shape and `outflow(state)` mechanics).

You're the commissioner of an 8-team NASCAR fantasy league.  Every Sunday morning before the green flag drops you need three derived analytical views on the same desk: **this month's Cup schedule** (so the group chat knows what's worth tuning in for), **the league scoreboard** (so trash talk is correctly calibrated), and **the manufacturer leaderboard** (so the Chevy-vs-Ford bet has a settled score).  All three live behind the same six NASCAR APIs and one hand-maintained roster file.  Wired naively this is an eight-script crontab.  Wired through fjord it's a single async-for loop and one outflow function.

T8 introduced streaming polling on single-source registries.  T9 walks the *full* multi-source production shape — concurrent seed of independent sources, sequential seed where state matters, sentinel-ID filtering at the graph boundary, and a single outflow returning a `dict` whose keys become three derived classes, three NDJSON files.  T10 will introduce `fjord()` formally on the minimum-viable two-source case.  You're getting the production shape first, the abstraction next.

---

## 🎯 The Goal

For the current NASCAR Cup, Busch, and Truck seasons, in **one** fjord call:

1. **`MonthlyRaceSchedule`** — current-month Cup races with resolved track name, **track type**, **track length**, **city/state**, pole winner, **pole speed**, **race winner** (past races), car count, **TV broadcaster**, and **playoff flag**.  Future races' pole / winner / speed columns land as `null` (qualifying / race hasn't happened yet).
2. **`FantasyTeam`** — the 8-team league scoreboard sorted by total points.  One row per team with their full resolved roster; each per-driver row carries **manufacturer**, **hometown**, **current series rank**, **wins**, **top-5s**, **top-10s**, **laps led**, **points**, and **points back**.  Per-team summary block carries a **manufacturer mix** counter (Chevy vs Ford vs Toyota) and a total-wins tally.
3. **`ManufacturerLeaderboard`** — Cup-series rollup, one row per Chevrolet / Ford / Toyota, with driver count, total points, total wins, playoff seats, and the top driver per make.

All three views exported as NDJSON.  Seven sources, one outflow, three output files, no daemons, no manual joins, ~330 lines of inflow/outflow + ~70 lines of driver.

---

## 🧱 The Sources

Six HTTP endpoints + one local JSON file:

```text
https://cf.nascar.com/cacher/tracks.json                    →  Track          (~49 rows)
https://cf.nascar.com/cacher/drivers.json                   →  Driver         (~917 rows)
https://cf.nascar.com/cacher/{YEAR}/race_list_basic.json    →  Race           (~40 rows)
https://cf.nascar.com/data/cacher/production/{YEAR}/1/...   →  CupStanding    (~39 rows)
https://cf.nascar.com/data/cacher/production/{YEAR}/2/...   →  BuschStanding  (~59 rows)
https://cf.nascar.com/data/cacher/production/{YEAR}/3/...   →  TruckStanding  (~61 rows)
fixtures/league_teams.json                                  →  LeagueRoster   (8 rows)
```

**`Race` has three foreign keys** into the registries — `track_id` into `Track`, `pole_winner_driver_id` and `winner_driver_id` into `Driver`.  The three standings endpoints share the same response shape but **must** be distinct classes so their registries don't collide on a shared `inc_dict`.

**`LeagueRoster`** is the seventh source — a hand-curated JSON file that lives next to the outflow code.  Fjord's handler dispatch routes `inc_file=` and `inc_url=` through the same code path, so the file source registers as a normal `Incorporator` subclass indistinguishable from the API-fed ones.

Fjord's parallel-seed phase loads any source whose `inflow(state)` return doesn't reference its peers; the ones that do reference peers wait.  Six of the seven (`Track`, `Driver`, `LeagueRoster`, and the three Standings classes) load concurrently; `Race` waits for `Track` + `Driver` so its three `link_to(state["…"])` resolvers land correctly.

---

## 🗂️ Project Layout

By the end of this tutorial you'll have laid down three files:

```text
examples/09-nascar-fantasy-fjord/
├── fixtures/
│   └── league_teams.json     ← Step 1 — the roster
├── outflow.py                ← Step 2 — source classes + inflow + outflow
└── nascar_fantasy.py         ← Step 3 — the runner
```

The output directory (`out/`) is created at runtime; you don't need to make it.

---

## 🔧 Step 1: The Roster Fixture

`fixtures/league_teams.json` is the **only** piece of business logic that isn't either an API or framework wiring — it's the league commissioner's source of truth.  Eight teams, each with a roster of 8 drivers (1 Truck pick, 1 Busch pick, 6 Cup picks).  Lay it down verbatim:

```bash
mkdir -p examples/09-nascar-fantasy-fjord/fixtures
cat > examples/09-nascar-fantasy-fjord/fixtures/league_teams.json <<'EOF'
[
  {
    "team_id": "Queen",
    "roster": [
      {"series_id": 3, "driver_id": 4235},
      {"series_id": 2, "driver_id": 4441},
      {"series_id": 1, "driver_id": 3989},
      {"series_id": 1, "driver_id": 4062},
      {"series_id": 1, "driver_id": 4123},
      {"series_id": 1, "driver_id": 4272},
      {"series_id": 1, "driver_id": 3859},
      {"series_id": 1, "driver_id": 4481}
    ]
  },
  {
    "team_id": "Intim'tor",
    "roster": [
      {"series_id": 3, "driver_id": 4312},
      {"series_id": 2, "driver_id": 34},
      {"series_id": 1, "driver_id": 4030},
      {"series_id": 1, "driver_id": 4023},
      {"series_id": 1, "driver_id": 3989},
      {"series_id": 1, "driver_id": 4153},
      {"series_id": 1, "driver_id": 4065},
      {"series_id": 1, "driver_id": 4481}
    ]
  },
  {
    "team_id": "WonderBoy",
    "roster": [
      {"series_id": 3, "driver_id": 4235},
      {"series_id": 2, "driver_id": 4133},
      {"series_id": 1, "driver_id": 4153},
      {"series_id": 1, "driver_id": 4030},
      {"series_id": 1, "driver_id": 1816},
      {"series_id": 1, "driver_id": 4065},
      {"series_id": 1, "driver_id": 3859},
      {"series_id": 1, "driver_id": 4481}
    ]
  },
  {
    "team_id": "AlabamaG",
    "roster": [
      {"series_id": 3, "driver_id": 4446},
      {"series_id": 2, "driver_id": 34},
      {"series_id": 1, "driver_id": 4030},
      {"series_id": 1, "driver_id": 454},
      {"series_id": 1, "driver_id": 4023},
      {"series_id": 1, "driver_id": 4153},
      {"series_id": 1, "driver_id": 4065},
      {"series_id": 1, "driver_id": 4481}
    ]
  },
  {
    "team_id": "Jaws",
    "roster": [
      {"series_id": 3, "driver_id": 4446},
      {"series_id": 2, "driver_id": 34},
      {"series_id": 1, "driver_id": 4065},
      {"series_id": 1, "driver_id": 4030},
      {"series_id": 1, "driver_id": 4153},
      {"series_id": 1, "driver_id": 3859},
      {"series_id": 1, "driver_id": 4001},
      {"series_id": 1, "driver_id": 4481}
    ]
  },
  {
    "team_id": "Seven",
    "roster": [
      {"series_id": 3, "driver_id": 4235},
      {"series_id": 2, "driver_id": 4133},
      {"series_id": 1, "driver_id": 1816},
      {"series_id": 1, "driver_id": 454},
      {"series_id": 1, "driver_id": 4062},
      {"series_id": 1, "driver_id": 1361},
      {"series_id": 1, "driver_id": 3859},
      {"series_id": 1, "driver_id": 4481}
    ]
  },
  {
    "team_id": "Cale",
    "roster": [
      {"series_id": 3, "driver_id": 4427},
      {"series_id": 2, "driver_id": 4133},
      {"series_id": 1, "driver_id": 4023},
      {"series_id": 1, "driver_id": 4001},
      {"series_id": 1, "driver_id": 4153},
      {"series_id": 1, "driver_id": 4030},
      {"series_id": 1, "driver_id": 4065},
      {"series_id": 1, "driver_id": 4481}
    ]
  },
  {
    "team_id": "Confused",
    "roster": [
      {"series_id": 3, "driver_id": 4235},
      {"series_id": 2, "driver_id": 34},
      {"series_id": 1, "driver_id": 4023},
      {"series_id": 1, "driver_id": 3989},
      {"series_id": 1, "driver_id": 4062},
      {"series_id": 1, "driver_id": 4153},
      {"series_id": 1, "driver_id": 4469},
      {"series_id": 1, "driver_id": 4481}
    ]
  }
]
EOF
```

**Schema rationale.**  Each team has a `team_id` (used as the registry key — `inc_code=team_id` in the driver script) and a `roster` array.  Each roster row is a `(series_id, driver_id)` pair where `series_id` is `1` (Cup), `2` (Busch), or `3` (Truck) and `driver_id` joins to `Driver.Nascar_Driver_ID`.  The outflow uses `series_id` to pick which Standings registry to look the driver up in, and `driver_id` to pull the live `Driver` instance for hometown / team / manufacturer.

---

## 🔧 Step 2: The Inflow / Outflow Sidecar

`outflow.py` is the entire ETL.  It defines the seven source classes, the sentinel-filter helper, the `inflow(state)` callable that wires `Race`'s foreign keys against `Track` + `Driver`, and the `outflow(state)` function that emits the three derived views.  Fjord imports it, registers the classes, and drives the pipeline — no other Python is required apart from the runner in Step 3.  The filename matches the framework's CLI-scaffold convention (`incorporator init --type fjord` generates a `pipeline.json` + `outflow.py` pair).

Lay the file down whole; we'll walk it in chunks below.

### 2a. Source classes

Each fjord source needs its own subclass so the three Standings don't share `inc_dict`.  `LeagueRoster` is the seventh — fed by a local JSON file, demonstrating that fjord mixes API + filesystem sources without any special casing.

```python
"""Outflow sidecar for the NASCAR fantasy-league fjord pipeline."""

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List

from incorporator import Incorporator, inc, link_to


# ── Source classes ─────────────────────────────────────────────────


class Track(Incorporator):
    pass


class Driver(Incorporator):
    pass


class Race(Incorporator):
    pass


class CupStanding(Incorporator):
    pass


class BuschStanding(Incorporator):
    pass


class TruckStanding(Incorporator):
    pass


class LeagueRoster(Incorporator):
    """League membership read from ``league_teams.json``.  Keyed by
    ``team_id``; each instance carries a ``roster`` list of
    ``{series_id, driver_id}`` picks."""
```

> ⚠️ **Do not pre-declare fields on these classes.**  The framework builds Pydantic schemas dynamically from the incoming JSON; if you stub `track_id: int = None` on `Track`, fjord stops inferring and you lose half your columns to silent drops.  The classes are deliberately bare.

### 2b. Constants

```python
_DATE_FIELDS = ("date_scheduled", "race_date", "qualifying_date", "tunein_date")

_SERIES_LIST = ("Cup", "Busch", "Truck")
```

`_DATE_FIELDS` lists every column in the Race payload that ships as an ISO string — the inflow's `conv_dict` adds `inc(datetime)` for each so they arrive as real `datetime` instances in the outflow.  `_SERIES_LIST` is the human-readable label table; series IDs `1/2/3` map to indices `0/1/2`.

### 2c. The sentinel filter

NASCAR's API returns `pole_winner_driver_id = 0` for races whose pole qualifying hasn't happened yet (or was rained out) — and `winner_driver_id = 0` for races that haven't been run yet.  Driver ID `0` coincidentally resolves to a real entry in the driver registry, so a naked `link_to` makes every future race resolve to the same incidental name.  The fix is a 3-line `extractor=`:

```python
# ── Sentinel filter for link_to ────────────────────────────────────


def _driver_id_or_none(raw: Any) -> Any:
    """NASCAR returns ``0`` for any driver-ID field whose underlying
    event hasn't happened yet (qualifying not held, race not run,
    rain-out).  Driver ID 0 coincidentally resolves to a real driver
    in the registry, so without this filter every future race's
    pole/winner column would show the same incidental name.  Mapping
    falsy values (``0``, ``None``, ``""``) to ``None`` lets ``link_to``
    short-circuit and downstream consumers see ``None``.
    """
    return raw if raw else None
```

`link_to` runs the extractor on the raw column value **before** it hits the registry; returning `None` short-circuits the lookup and the field stays `None`.  The outflow's existing `getattr(pole, "Full_Name", None) if pole else None` branch then fires naturally for every "no pole yet" race.

> 💡 **The pattern generalises.**  Any third-party API with sentinel IDs (Discord's `-1`-as-deleted-user, Twitter's `0`-as-anon-author, a SQL `NULL` foreign-key) lands at the same boundary: write a tiny extractor that converts the sentinel to `None` and let `link_to`'s short-circuit do the rest.  No per-call guard in consumer code.

### 2d. State-aware inflow

`inflow(state)` is called **once per source in seed order**, with a progressively-populated `state` dict.  The first call gets an empty `state` (no sources loaded yet); the second gets one entry; and so on.  Guard against missing keys with `if "Track" in state and "Driver" in state:` — your override only emits once its dependencies are present, and fjord re-applies it on every refresh wave so the `link_to` closures see fresh peer snapshots.

```python
# ── State-aware inflow — wires Race.conv_dict against live peers ────


def inflow(state: Dict[str, Any]) -> Dict[str, Any]:
    """Build per-source ``conv_dict`` overrides from sibling registries.

    Inflow is called before each source's ``incorp()``.  On the early
    calls (Track / Driver / Standings / LeagueRoster) ``state`` is
    empty or partial, so we only emit Race's override once its peers
    exist — fjord then re-applies it on every refresh wave so Race's
    ``track_id``, ``pole_winner_driver_id``, and ``winner_driver_id``
    resolve to live ``Track`` / ``Driver`` instances rather than raw
    integers.
    """
    overrides: Dict[str, Any] = {}
    if "Track" in state and "Driver" in state:
        overrides["Race"] = {
            "conv_dict": {
                "track_id":              link_to(state["Track"]),
                "pole_winner_driver_id": link_to(state["Driver"], extractor=_driver_id_or_none),
                "winner_driver_id":      link_to(state["Driver"], extractor=_driver_id_or_none),
                **{key: inc(datetime) for key in _DATE_FIELDS},
            }
        }
    return overrides
```

Returning `{}` for a source = "no overrides, use the `incorp_params` as-declared".  Returning `{"Race": {"conv_dict": …}}` = "when fjord goes to seed `Race`, merge this `conv_dict` into its `incorp_params`".  The `link_to(state["Track"])` call captures the **live** `Track` registry, so when `Race`'s rows incorporate, every `track_id` integer is swapped for the matching `Track` Pydantic instance.

**Foreign-key resolution is one-time, not lazy.**  Once a Race row is incorporated, `race.track_id` is the `Track` instance itself — `race.track_id.inc_name`, `race.track_id.city`, `race.track_id.length` all work directly.  No re-lookup in the outflow.  (The runner does `name_chg=[("track_id", "track")]` purely for readability — the field arrives renamed to `track` in the Race instance.)

### 2e. Helpers

Two small string-composition helpers used by the outflow.  Pure functions, no state.

```python
# ── Helpers ────────────────────────────────────────────────────────


def _hometown(driver: Any) -> str:
    """Compose ``City, ST`` from the driver's hometown fields, or
    ``Unknown`` if either piece is missing.
    """
    city = getattr(driver, "Hometown_City", "") or ""
    state = getattr(driver, "Hometown_State", "") or ""
    city = city.strip()
    state = state.strip()
    if city and state:
        return f"{city}, {state}"
    return city or state or "Unknown"


def _track_loc(track: Any) -> str:
    """Compose ``City, ST`` for a track."""
    if track is None:
        return "Unknown"
    city = (getattr(track, "city", "") or "").strip()
    state = (getattr(track, "state", "") or "").strip()
    if city and state:
        return f"{city}, {state}"
    return city or state or "Unknown"
```

### 2f. Outflow — three derived views in one function

`outflow(state)` returns `dict[ClassName, list[dict]]` and fjord builds **one dynamic Incorporator subclass per dict key** at first emit.  This is the multi-output contract: three keys in the return → three derived classes → three NDJSON files.

> ⚠️ **Do not pre-declare the output classes.**  `MonthlyRaceSchedule`, `FantasyTeam`, and `ManufacturerLeaderboard` are conspicuously absent from the source-class block at the top of the file.  Fjord builds them dynamically from the dict keys returned here and infers fields from the first emitted row.  A bare `class MonthlyRaceSchedule(Incorporator): pass` would suppress field inference and the export would land empty.

The function reads from `state["Driver"]`, `state["Race"]`, `state["LeagueRoster"]`, and the three Standings registries.  If any of the three required dependencies are missing (first wave hasn't completed yet), return `{}` to skip the emit.

```python
# ── Outflow — three derived views ──────────────────────────────────


def outflow(state: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Compute three derived views from the fused state.  Each dict
    key becomes a derived Incorporator subclass and is written to its
    matching ``export_params`` file by fjord's multi-output contract.
    """
    drivers = state.get("Driver")
    races = state.get("Race")
    league = state.get("LeagueRoster")
    if drivers is None or races is None or league is None:
        return {}

    points_standings = {
        1: state.get("CupStanding"),
        2: state.get("BuschStanding"),
        3: state.get("TruckStanding"),
    }

    now = datetime.now()
```

#### View 1 — `MonthlyRaceSchedule`

Current-month Cup races with resolved track + driver context.  This is where the inflow's `link_to` work pays off: `race.track_id` is already a live `Track` instance (we renamed it to `race.track` via `name_chg` in the runner, but we fall back to the original name too), and `race.pole_winner_driver_id` / `race.winner_driver_id` are live `Driver` instances or `None` thanks to the sentinel filter.  Zero re-lookups in the outflow.

```python
    # ════════════════════════════════════════════════════════════════
    # View 1 — MonthlyRaceSchedule
    # ════════════════════════════════════════════════════════════════
    monthly: List[Dict[str, Any]] = []
    for race in races:
        dt = getattr(race, "date_scheduled", None)
        if dt is None or dt.month != now.month or dt.year != now.year:
            continue
        pole = getattr(race, "pole_winner_driver_id", None)
        winner = getattr(race, "winner_driver_id", None)
        track = getattr(race, "track", None) or getattr(race, "track_id", None)
        playoff_round = getattr(race, "playoff_round", 0) or 0

        monthly.append({
            "race_id":     race.inc_code,
            "date":        dt.strftime("%Y-%m-%d"),
            "race_name":   getattr(race, "race_name", "TBD"),
            "track":       getattr(track, "inc_name", "Unknown") if track else "Unknown",
            "track_type":  getattr(track, "track_type", "Unknown") if track else "Unknown",
            "track_miles": getattr(track, "length", None) if track else None,
            "track_loc":   _track_loc(track),
            "pole_winner": getattr(pole, "Full_Name", None) if pole else None,
            "pole_speed":  (getattr(race, "pole_winner_speed", None) or None) if pole else None,
            "winner":      getattr(winner, "Full_Name", None) if winner else None,
            "cars":        getattr(race, "number_of_cars_in_field", 0),
            "tv":          getattr(race, "television_broadcaster", "TBD") or "TBD",
            "playoff":     bool(playoff_round),
        })
    monthly.sort(key=lambda r: r["date"])
```

Note the navigation idioms:

* **`race.inc_code`** — every Incorporator instance exposes its primary key via `inc_code` and its display name via `inc_name`.  This is how you get back at the original ID after fjord's field renaming.
* **`getattr(race, "track", None) or getattr(race, "track_id", None)`** — fjord renames `track_id` → `track` per the runner's `name_chg`, but we accept either spelling defensively.
* **`getattr(pole, "Full_Name", None) if pole else None`** — `pole` is a live `Driver` Pydantic instance whose schema came from the API; `Full_Name` is the API's actual field name and reaches us unchanged.

#### View 2 — `FantasyTeam`

Per-team scoreboard.  For each team's roster pick `{series_id, driver_id}`:

1. Look up the driver in `state["Driver"].inc_dict` by `driver_id` → live `Driver` instance.
2. Look up that same driver in the matching series Standings (`state["CupStanding"]` / `BuschStanding` / `TruckStanding`) by **the same key** — `Driver` and `*Standing` are keyed on the same `driver_id`, so `standings.inc_dict.get(driver.inc_code)` works.
3. Pull manufacturer / wins / position / points off the Standings row, and hometown / team / car number off the Driver row.

`.inc_dict.get(key)` is the framework's O(1) primary-key lookup on the registry — every `IncorporatorList` exposes it.

```python
    # ════════════════════════════════════════════════════════════════
    # View 2 — FantasyTeam
    # ════════════════════════════════════════════════════════════════
    league_teams: Dict[str, Dict[int, List[Any]]] = {}
    for team in league:
        team_cd = team.team_id
        league_teams[team_cd] = {}
        for pick in (team.roster or []):
            sid = int(getattr(pick, "series_id", 0))
            did = int(getattr(pick, "driver_id", 0))
            driver_obj = drivers.inc_dict.get(did)
            if driver_obj is not None and sid in (1, 2, 3):
                league_teams[team_cd].setdefault(sid, []).append(driver_obj)
        for sid in (1, 2, 3):
            if sid in league_teams[team_cd]:
                league_teams[team_cd][sid].sort(
                    key=lambda d: int(getattr(d, "Badge", 0) or 0)
                )

    fantasy: List[Dict[str, Any]] = []
    for team_cd, roster in league_teams.items():
        team_obj: Dict[str, Any] = {
            "team_id":          team_cd,
            "roster":           [],
            "points":           [],
            "manufacturer_mix": {},
            "total_wins":       0,
            "total_score":      0,
        }
        team_score = 0
        per_series: Dict[int, int] = {}
        mfg_counter: Counter = Counter()
        total_wins = 0

        for series_id, series_name in enumerate(_SERIES_LIST, start=1):
            per_series[series_id] = 0
            if series_id not in roster:
                continue
            series_cls = points_standings.get(series_id)
            for car_idx, driver in enumerate(roster[series_id], start=1):
                stnd = series_cls.inc_dict.get(driver.inc_code) if series_cls else None
                pts = getattr(stnd, "points", 0) if stnd else 0
                wins = getattr(stnd, "wins", 0) if stnd else 0
                per_series[series_id] += pts
                total_wins += wins

                mfg = (getattr(stnd, "manufacturer", "") if stnd else "") or \
                      getattr(driver, "Manufacturer", "") or "Unknown"
                mfg = mfg.strip() or "Unknown"
                mfg_counter[mfg] += 1

                team_obj["roster"].append({
                    "series":       series_name,
                    "car_idx":      car_idx,
                    "name":         getattr(driver, "inc_name", "Unknown").strip(),
                    "car":          getattr(driver, "Badge", "N/A"),
                    "team":         (getattr(driver, "Team", "") or "Unknown").strip(),
                    "manufacturer": mfg,
                    "hometown":     _hometown(driver),
                    "rank":         getattr(stnd, "position", None) if stnd else None,
                    "wins":         wins,
                    "t10":          getattr(stnd, "top_10", 0) if stnd else 0,
                    "top_5":        getattr(stnd, "top_5", 0) if stnd else 0,
                    "laps_led":     getattr(stnd, "laps_led", 0) if stnd else 0,
                    "points":       pts,
                    "points_back":  abs(getattr(stnd, "delta_leader", 0) or 0) if stnd else None,
                })
            team_score += per_series[series_id]

        for series_id, series_name in enumerate(_SERIES_LIST, start=1):
            pts = per_series[series_id]
            team_obj["points"].append({
                "series":     series_name,
                "points":     pts,
                "percentage": round(pts / team_score, 4) if team_score else 0,
            })
        team_obj["points"].append({"series": "GRAND TOTAL", "points": team_score, "percentage": 1.0})
        team_obj["total_score"] = team_score
        team_obj["total_wins"] = total_wins
        team_obj["manufacturer_mix"] = dict(mfg_counter.most_common())
        fantasy.append(team_obj)

    fantasy.sort(key=lambda t: -t["total_score"])
```

#### View 3 — `ManufacturerLeaderboard`

A third analytical view that's effectively "free" because the Cup standings are already in memory.  Bucket Cup drivers by `manufacturer`, sum points / wins / playoff seats per make, find the top driver per make by points.

```python
    # ════════════════════════════════════════════════════════════════
    # View 3 — ManufacturerLeaderboard
    # ════════════════════════════════════════════════════════════════
    cup = points_standings[1]
    mfg_buckets: Dict[str, List[Any]] = defaultdict(list)
    if cup is not None:
        for stnd in cup:
            mfg = (getattr(stnd, "manufacturer", "") or "").strip() or "Unknown"
            mfg_buckets[mfg].append(stnd)

    manufacturer_rows: List[Dict[str, Any]] = []
    for mfg, rows in mfg_buckets.items():
        if mfg == "Unknown":
            continue
        top = max(rows, key=lambda s: getattr(s, "points", 0))
        manufacturer_rows.append({
            "manufacturer":  mfg,
            "drivers":       len(rows),
            "total_points":  sum(getattr(s, "points", 0) for s in rows),
            "total_wins":    sum(getattr(s, "wins", 0) for s in rows),
            "playoff_seats": sum(1 for s in rows if getattr(s, "playoff_eligible", 0)),
            "top_driver":    getattr(top, "inc_name", "Unknown"),
            "top_points":    getattr(top, "points", 0),
        })
    manufacturer_rows.sort(key=lambda r: -r["total_points"])

    return {
        "MonthlyRaceSchedule":     monthly,
        "FantasyTeam":             fantasy,
        "ManufacturerLeaderboard": manufacturer_rows,
    }
```

The three dict keys land verbatim as fjord-built class names and match the keys in the runner's `export_params`.

> 💡 **One outflow, N views.**  Adding a fourth view ("DriverHotStreaks" — drivers who scored top-10s in their last three Cup races) means **adding one more key to the return dict and one more entry to `export_params`**.  No new file, no new daemon, no extra HTTP call — every column you need is already in fused state.

---

## 🔧 Step 3: The Driver Script

`nascar_fantasy.py` is the runner.  It declares the seven sources (six API + one local file), points fjord at `outflow.py` for both inflow and outflow, and configures the three export targets.  `refresh_params=None` on every source = single-wave test mode; the pipeline exits cleanly after one outflow wave.

```python
"""NASCAR fantasy league as a multi-output fjord pipeline."""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

from incorporator import Incorporator, calc

HERE = Path(__file__).resolve().parent
DATA = HERE / "out"

if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from outflow import (  # noqa: E402
    BuschStanding,
    CupStanding,
    Driver,
    LeagueRoster,
    Race,
    Track,
    TruckStanding,
)

CURRENT_YEAR = datetime.now().year
CFC_BASE = "https://cf.nascar.com/cacher"
PROD_BASE = f"https://cf.nascar.com/data/cacher/production/{CURRENT_YEAR}"
STANDINGS_BASE = "racinginsights-points-feed.json"

_STANDINGS_EXCL = [
    "is_clinch",
    "driver_first_name", "driver_last_name", "driver_suffix",
    "playoff_stage_wins",
]
_STANDINGS_CONV = {
    "points":   calc(int, default=0, target_type=int),
    "wins":     calc(int, default=0, target_type=int),
    "top_10":   calc(int, default=0, target_type=int),
    "top_5":    calc(int, default=0, target_type=int),
    "laps_led": calc(int, default=0, target_type=int),
    "position": calc(int, default=0, target_type=int),
}
_DRIVER_EXCL = [
    "Series_Logo", "Short_Name", "Description", "Hobbies", "Children",
    "Residing_City", "Residing_State", "Residing_Country", "Image_Transparent",
    "SecondaryImage", "Career_Stats", "Age", "Rank", "Points", "Points_Behind",
    "No_Wins", "Poles", "Top5", "Top10", "Laps_Led", "Stage_Wins",
    "Playoff_Points", "Playoff_Rank", "Integrated_Sponsor_Name",
    "Integrated_Sponsor", "Integrated_Sponsor_URL", "Silly_Season_Change",
    "Silly_Season_Change_Description", "Driver_Post_Status", "Driver_Part_Time",
]


async def main() -> None:
    print("Initiating NASCAR Data Gateway (fjord)...\n")
    DATA.mkdir(exist_ok=True)

    async for wave in Incorporator.fjord(
        stream_params=[
            {"cls": Track, "incorp_params": {"inc_url": f"{CFC_BASE}/tracks.json", "rec_path": "items", "inc_code": "track_id", "inc_name": "track_name"}, "refresh_params": None},
            {"cls": Driver, "incorp_params": {"inc_url": f"{CFC_BASE}/drivers.json", "rec_path": "response", "inc_code": "Nascar_Driver_ID", "inc_name": "Full_Name", "excl_lst": _DRIVER_EXCL}, "refresh_params": None},
            {"cls": Race, "incorp_params": {"inc_url": f"{CFC_BASE}/{CURRENT_YEAR}/race_list_basic.json", "rec_path": "series_1", "inc_code": "race_id", "inc_name": "race_name", "excl_lst": ["schedule", "track_name"], "name_chg": [("track_id", "track")]}, "refresh_params": None},
            {"cls": CupStanding, "incorp_params": {"inc_url": f"{PROD_BASE}/1/{STANDINGS_BASE}", "inc_code": "driver_id", "inc_name": "driver_name", "excl_lst": _STANDINGS_EXCL, "conv_dict": _STANDINGS_CONV}, "refresh_params": None},
            {"cls": BuschStanding, "incorp_params": {"inc_url": f"{PROD_BASE}/2/{STANDINGS_BASE}", "inc_code": "driver_id", "inc_name": "driver_name", "excl_lst": _STANDINGS_EXCL, "conv_dict": _STANDINGS_CONV}, "refresh_params": None},
            {"cls": TruckStanding, "incorp_params": {"inc_url": f"{PROD_BASE}/3/{STANDINGS_BASE}", "inc_code": "driver_id", "inc_name": "driver_name", "excl_lst": _STANDINGS_EXCL, "conv_dict": _STANDINGS_CONV}, "refresh_params": None},
            {"cls": LeagueRoster, "incorp_params": {"inc_file": str(HERE / "fixtures/league_teams.json"), "inc_code": "team_id", "inc_name": "team_id"}, "refresh_params": None},
        ],
        inflow=str(HERE / "outflow.py"),
        outflow=str(HERE / "outflow.py"),
        export_params={
            "MonthlyRaceSchedule":     {"file_path": str(DATA / "nascar_monthly_schedule.ndjson")},
            "FantasyTeam":             {"file_path": str(DATA / "nascar_fantasy_scoreboard.ndjson")},
            "ManufacturerLeaderboard": {"file_path": str(DATA / "nascar_manufacturer_leaderboard.ndjson")},
        },
    ):
        op = wave.operation
        if wave.failed_sources:
            print(f"WARN  {op:35s} chunk {wave.chunk_index}: {wave.failed_sources}")
        else:
            print(f"OK    {op:35s} chunk {wave.chunk_index}: {wave.rows_processed} rows")


if __name__ == "__main__":
    asyncio.run(main())
```

Notable wiring:

* **Same-file inflow + outflow.**  Both `inflow=` and `outflow=` point at `outflow.py` because both callables live there.  Fjord loads the module once via `importlib`'s cache, so the second import is free.
* **`refresh_params=None` everywhere = single-wave test mode.**  Drop those lines (refresh defaults on at 60s) and the daemons stay alive — perfect for production but blocks the `async for` loop indefinitely.  Mix and match: leave `Track`'s refresh off (tracks never change) while letting standings refresh every 5 minutes.
* **`export_params` is keyed by output class name.**  Each key matches a key returned by `outflow(state)`; fjord's multi-output detection is "is there a top-level `file_path`?  No → multi-output."

---

## 🏁 The Run

```bash
cd examples/09-nascar-fantasy-fjord
python nascar_fantasy.py
```

Expected console output (numbers depend on which races have been run this season):

```text
Initiating NASCAR Data Gateway (fjord)...

OK    fjord_incorp:Track                  chunk 1: 49 rows
OK    fjord_incorp:Driver                 chunk 1: 917 rows
OK    fjord_incorp:CupStanding            chunk 1: 39 rows
OK    fjord_incorp:BuschStanding          chunk 1: 59 rows
OK    fjord_incorp:TruckStanding          chunk 1: 61 rows
OK    fjord_incorp:LeagueRoster           chunk 1: 8 rows
OK    fjord_incorp:Race                   chunk 1: 40 rows
OK    outflow:MonthlyRaceSchedule         chunk 1: 5 rows
OK    outflow:FantasyTeam                 chunk 1: 8 rows
OK    outflow:ManufacturerLeaderboard     chunk 1: 3 rows
```

Notice `Race` lands **after** `Track` and `Driver` even though all seven sources started in the same `stream_params` list — that's the inflow's `if "Track" in state and "Driver" in state:` guard back-pressuring `Race`'s seed until its peers have published their registries.  The other five sources race to completion in parallel.

### Output 1 — `out/nascar_monthly_schedule.ndjson`

Per-race row with resolved track + driver context.  Past races have real `pole_winner`, `pole_speed`, and `winner`; future races have all three as `null` (sentinel filter + outflow `getattr` fallbacks):

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

### Output 2 — `out/nascar_fantasy_scoreboard.ndjson`

One row per team, sorted by total score.  Each roster row carries the driver's manufacturer, hometown, current series rank, and the full season stats (top-5, top-10, laps led, points, gap to leader).  The team summary block carries the manufacturer mix and total wins:

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
    }
    // ... 7 more roster rows
  ],
  "points": [
    {"series": "Cup",         "points": 2598, "percentage": 0.8496},
    {"series": "Busch",       "points":  310, "percentage": 0.1014},
    {"series": "Truck",       "points":  150, "percentage": 0.0490},
    {"series": "GRAND TOTAL", "points": 3058, "percentage": 1.0}
  ]
}
```

### Output 3 — `out/nascar_manufacturer_leaderboard.ndjson`

Cup-series manufacturer rollup — one row per Chevrolet / Ford / Toyota, sorted by total points:

```jsonc
{"manufacturer":"Chevrolet", "drivers":20, "total_points":3941, "total_wins":4, "playoff_seats":20, "top_driver":"Chase Elliott", "top_points":422}
{"manufacturer":"Toyota",    "drivers":9,  "total_points":2867, "total_wins":7, "playoff_seats":9,  "top_driver":"Tyler Reddick", "top_points":567}
{"manufacturer":"Ford",      "drivers":10, "total_points":2677, "total_wins":1, "playoff_seats":10, "top_driver":"Ryan Blaney",   "top_points":405}
```

Note how **Toyota has fewer drivers than Chevrolet (9 vs 20) but nearly twice the wins (7 vs 4)** — that's the kind of insight a manufacturer leaderboard surfaces and the legacy ETL hid in memory.

---

## 🐳 Run It From the CLI

The same seven-source pipeline expressed as a JSON config — no Python wrapper required.  The CLI loader resolves `cls_name` by importing the `outflow=` file and looking up each class by name, which is how the multi-source registration stays JSON-serialisable.

### `config/pipeline.json`

```json
{
  "inflow":  "examples/09-nascar-fantasy-fjord/outflow.py",
  "outflow": "examples/09-nascar-fantasy-fjord/outflow.py",
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

* **`refresh_params: null`** is the JSON spelling of Python's `refresh_params=None` — opts the source out of the refresh daemon.  Used here for `Track` (tracks never change) and `LeagueRoster` (rosters change rarely; restart the daemon when you edit the file).
* **`conv_dict` values are quoted strings.**  The token resolver in `cli/tokens.py` parses them at config-load time and substitutes the real callables.  `calc(int, default=0, target_type=int)` becomes the actual converter; same for `inc(datetime)` etc.  `link_to(state["…"])` calls live in `inflow(state)` — not in the JSON — because they need the runtime registry handle.
* **`name_chg` uses arrays not tuples.**  JSON has no tuple literal; `["track_id", "track"]` deserialises to the same shape the Python code uses.
* **`refresh_interval` as a dict** keyed by class name, exactly like Python — JSON-friendly out of the box.
* **`export_params` keyed by output class** — multi-output detection is "is there a top-level `file_path` key?  No → multi-output."

### Validate + run

```bash
incorporator validate config/pipeline.json
incorporator fjord    config/pipeline.json --logs
```

`--logs` routes every Wave through the `LoggedIncorporator` queue handler into `logs/api.log` (success) and `logs/error.log` (failures with redacted URLs).  Add `--heartbeat-file /tmp/inc.beat` to pair with Docker's `HEALTHCHECK`.

### Docker

The repo's `docker-compose.yml` and `Dockerfile` work for this pipeline as-is.  Three host folders bind-mount into the container:

| Host | Container | What goes here |
|---|---|---|
| `./config` | `/app/config` *(read-only)* | `pipeline.json`, `outflow.py`, and `league_teams.json` |
| `./data` | `/app/data` | Three NDJSON outputs land here |
| `./logs` | `/app/logs` | Rotating JSON log files (when `--logs` is set) |

The wrinkle compared to a single-source fjord: **the `league_teams.json` file must live where the container can read it**.  Easiest pattern is to drop it next to `pipeline.json` in `config/` and reference it with a container-relative path (`"inc_file": "config/league_teams.json"` — *not* the `examples/...` path the host uses).  Same for `outflow.py`: copy it into `config/` and point both `inflow` and `outflow` at `"config/outflow.py"`.

```bash
mkdir -p config data logs
cp examples/09-nascar-fantasy-fjord/fixtures/league_teams.json config/league_teams.json
cp examples/09-nascar-fantasy-fjord/outflow.py                 config/outflow.py
# edit config/pipeline.json so inflow/outflow/inc_file point at config/* paths
incorporator validate config/pipeline.json
docker compose up -d
docker compose logs -f
```

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

For an off-season demo (one-shot run with no refresh), set every `refresh_params: null` and drop `refresh_interval` / `export_interval` entirely — the pipeline exits cleanly after one outflow wave.  This pipeline calls *only* public NASCAR endpoints — no API keys, no auth.  The `${API_KEY}` / `${file:/run/secrets/...}` patterns documented in the [deployment guide](../../docs/deployment.md#secrets--local-vs-production) apply if you ever swap one of the sources for a paid feed.

---

## 🧠 What This Demonstrates

| Pattern | Where to look |
|---|---|
| **Concurrent seed of N independent sources** | The seven `stream_params` entries — six API + one file all start in parallel via `asyncio.gather` |
| **API + file source mixing** | `LeagueRoster` uses `inc_file=`; the other six use `inc_url=`.  Same handler dispatch routes both transparently |
| **Sequential seed when state matters** | `Race` waits for `Track` + `Driver` because its `inflow(state)` return references them; the others stay parallel |
| **Live foreign-key resolution** | `link_to(state["Track"])` / `link_to(state["Driver"], extractor=…)` in the inflow's `conv_dict` — and in the outflow, `race.track_id` is a live `Track` Pydantic instance with no re-lookup |
| **Sentinel-ID filter** | `extractor=_driver_id_or_none` short-circuits ID 0 to `None` at the graph boundary — applied to BOTH `pole_winner_driver_id` and `winner_driver_id` |
| **`stream()` vs `fjord()` vs `refresh()`** | `stream()` is paginated bulk-export chunking; `refresh()` is manual one-shot; `fjord()` (this tutorial) is the stateful multi-source daemon |
| **Empty-state contract in `inflow(state)`** | First call arrives with `state == {}`; guard with `if "Track" in state and "Driver" in state:` and let fjord re-call you once peers exist |
| **Dynamic output classes** | The three derived classes (`MonthlyRaceSchedule`, `FantasyTeam`, `ManufacturerLeaderboard`) are **not** pre-declared in `outflow.py` — fjord builds one Pydantic class per dict key returned from `outflow(state)`.  Bare pre-declarations would suppress field inference |
| **Multi-output dict return** | `outflow(state) -> {"MonthlyRaceSchedule": …, "FantasyTeam": …, "ManufacturerLeaderboard": …}` → three derived classes, three files |
| **Per-class export config** | Top-level `export_params` keyed by class name |
| **Registry navigation** | `state["Cls"]` is an `IncorporatorList`; `.inc_dict.get(key)` is O(1) primary-key lookup; iteration yields live Pydantic instances |
| **Field harvesting** | Every output column traces back to a field already pulled in the seed; no extra API call to add `track_type` / `manufacturer` / `winner`.  Payoff for the framework's eager-fetch / centralised-state model |
| **Config externalisation** | Fantasy rosters live in `fixtures/league_teams.json`, not Python — editing the league no longer requires touching code |
| **Single-wave test mode** | `refresh_params=None` on every entry, no `export_interval` → the pipeline exits after one outflow wave |
| **Pure-data outflow function** | The `outflow(state)` is a normal Python function — no async, no daemon plumbing, no lock acquisition. Fjord takes care of all that |

---

## Where to Go Next

> 👉 **Up next: [Tutorial 10 — Multi-Source Fjord](../10-multi-source-fjord/README.md).**  T9 walked the *full* fjord shape — seven sources, state-aware inflow, three outputs.  T10 introduces `fjord()` formally on its minimum viable form (two co-equal sources, one outflow) on the crypto-spread pattern.

| Goal | Read |
|---|---|
| Master the two-source fjord pattern abstraction | [Tutorial 10 — Multi-Source Fjord](../10-multi-source-fjord/README.md) |
| Drill parent records before fusing | [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md) |
| Orchestrate the same multi-source join in a windowed graph | [Tutorial 11 — Tideweaver](../11-tideweaver/README.md) |
| Run the diamond shape across NASCAR race telemetry | [Appendix — NASCAR Tideweaver](../appendix/nascar-tideweaver/README.md) |
| Configure this pipeline as a CLI fjord run | [CLI & Configuration Guide](../../docs/cli_and_configuration.md) |
| Revisit chunking & streaming fundamentals | [Tutorial 8 — Streaming Daemons](../08-streaming-daemon/README.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/09-nascar-fantasy-fjord/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
