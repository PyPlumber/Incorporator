***

# 🕸️ Tutorial 9 — NASCAR Fantasy Fjord: 8 sources, 3 outputs, 1 config

**Prerequisites:** [Tutorial 8 — Streaming Daemons](../08-streaming-daemon/README.md) (the `stream()` shape and `outflow(state)` mechanics).

You're the commissioner of an 8-team NASCAR fantasy league.  Every Sunday morning before the green flag drops you need three derived analytical views on the same desk: **this month's Cup schedule** (so the group chat knows what's worth tuning in for), **the league scoreboard** (so trash talk is correctly calibrated), and **the manufacturer leaderboard** (so the Chevy-vs-Ford bet has a settled score).  All three live behind the same seven NASCAR APIs and one hand-maintained roster file.  Wired naively this is a nine-script crontab.  Wired through fjord it's a single async-for loop and one outflow function.

T8 introduced streaming polling on single-source registries.  T9 walks the *full* multi-source production shape — schema-free ingestion of seven heterogeneous JSON endpoints and one local file into eight bare `class Foo(Incorporator): pass` subclasses (no field declarations, no Pydantic schemas hand-written), tiered-parallel seed via `depends_on`, sentinel-ID filtering at the graph boundary, and a single outflow returning a `dict` whose keys become three derived classes, three NDJSON files.  T10 will introduce `fjord()` formally on the minimum-viable two-source case.  You're getting the production shape first, the abstraction next.

---

## 🎯 The Goal

For the current NASCAR Cup, Busch, and Truck seasons, in **one** fjord call:

1. **`MonthlyRaceSchedule`** — current-month Cup races with resolved track name, **track type**, **track length**, **city/state**, pole winner, **pole speed**, **race winner** (past races), car count, **TV broadcaster**, and **playoff flag**.  Future races' pole / winner / speed columns land as `null` (qualifying / race hasn't happened yet).
2. **`FantasyTeam`** — the 8-team league scoreboard sorted by total points.  One row per team with their full resolved roster; each per-driver row carries **manufacturer**, **hometown**, **current series rank**, **wins**, **top-5s**, **top-10s**, **laps led**, **points**, and **points back**.  Per-team summary block carries a **manufacturer mix** counter (Chevy vs Ford vs Toyota) and a total-wins tally.
3. **`ManufacturerLeaderboard`** — Cup-series rollup, one row per Chevrolet / Ford / Toyota, with driver count, total points, total wins, playoff seats, and the top driver per make.

All three views exported as NDJSON.  Eight sources, one outflow, three output files, no daemons, no manual joins, ~400 lines of inflow/outflow + ~70 lines of driver.

---

## 🧱 The Sources

Seven HTTP endpoints + one local JSON file:

```text
https://cf.nascar.com/cacher/tracks.json                    →  Track              (~49 rows)
https://cf.nascar.com/cacher/drivers.json                   →  Driver             (~917 rows)
https://cf.nascar.com/cacher/{YEAR}/race_list_basic.json    →  Race               (~40 rows)
https://cf.nascar.com/data/cacher/production/{YEAR}/1/...   →  CupStanding        (~39 rows)
https://cf.nascar.com/data/cacher/production/{YEAR}/2/...   →  BuschStanding      (~59 rows)
https://cf.nascar.com/data/cacher/production/{YEAR}/3/...   →  TruckStanding      (~61 rows)
https://cf.nascar.com/cacher/{YEAR}/1/final/1-owners-points.json  →  CupOwnerStanding  (~46 rows)
fixtures/league_teams.json                                  →  LeagueRoster       (8 rows)
```

**`Race` has three foreign keys** into the registries — `track_id` into `Track`, `pole_winner_driver_id` and `winner_driver_id` into `Driver`.  The three standings endpoints share the same response shape but **must** be distinct classes so their registries don't collide on a shared `inc_dict`.

**`CupOwnerStanding`** is the eighth source — the Cup series owner-entry points feed.  It is keyed by `vehicle_number` (a string: `'133'`, `'3'`, `'33'`, …) rather than `owner_id` because `owner_id` 553 repeats across all three RCR entries.  This source is used by the outflow's `OWNER_SCORED` map to route deceased-driver picks to owner-entry points (see the Kyle Busch section below).

**`LeagueRoster`** is the only local-file source — a hand-curated JSON file that lives next to the outflow code.  Fjord's handler dispatch routes `inc_file=` and `inc_url=` through the same code path, so the file source registers as a normal `Incorporator` subclass indistinguishable from the API-fed ones.

With `inflow=` set, fjord seeds sources sequentially by default — each source calls `inflow(state)` and the state dict grows one entry at a time.  Adding `depends_on=["Track", "Driver"]` to the `Race` entry switches the engine into **tiered-parallel seed**: tier 0 contains all sources with no declared dependencies (`Track`, `Driver`, `LeagueRoster`, the three Standings, and `CupOwnerStanding`) and they all fire concurrently via `asyncio.gather`; tier 1 contains `Race` alone, and it waits for tier 0 to populate state before its `inflow(state)` call runs, so the `link_to(state["Track"])` and `link_to(state["Driver"])` closures resolve against live registries.

---

## 🗂️ Project Layout

By the end of this tutorial you'll have laid down four files:

```text
examples/09-nascar-fantasy-fjord/
├── fixtures/
│   └── league_teams.json     ← Step 1 — the roster
├── inflow.py                 ← Step 2a — incoming-data manipulation (inflow seed hook + conv_dict converters)
├── outflow.py                ← Step 2b — source classes + output-assembly policy + outflow
└── nascar_fantasy.py         ← Step 3 — the runner
```

The two sidecars are split by **direction of data flow**: anything that shapes a value *as it's ingested* (the `inflow(state)` seed hook, `conv_dict` converters/extractors) lives in `inflow.py`; anything that shapes the *output* (the `outflow(state)` views, their row helpers, the source classes) lives in `outflow.py`.

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

## 🔧 Step 2: The Sidecars — `inflow.py` and `outflow.py`

The ETL lives in two sibling sidecars, split by **direction of data flow** so the file layout self-documents:

* **`inflow.py` — incoming-data manipulation.**  The `inflow(state)` seed hook that wires `Race`'s foreign keys against `Track` + `Driver`, plus the `conv_dict` converter/extractor helpers that shape values *as they're ingested* (`_driver_id_or_none`, `_mfg_from_logo_url`) and the ingestion constant `_DATE_FIELDS`.
* **`outflow.py` — output shaping.**  The eight source classes, the output-assembly policy (`OWNER_SCORED`), the output-row helpers (`_hometown`, `_track_loc`, `_SERIES_LIST`), and the `outflow(state)` function that emits the three derived views.

The runner points `inflow=` at `inflow.py` and `outflow=` at `outflow.py`.  A converter used in a `conv_dict` is incoming-data manipulation even though the runner imports it directly — so it belongs in `inflow.py`, not `outflow.py`.

Lay both files down whole; we'll walk them in chunks below.

### 2a. Source classes

Each fjord source needs its own subclass so the Standings classes don't share `inc_dict`.  `LeagueRoster` is the only local-file source — fed by a hand-curated JSON file, demonstrating that fjord mixes API + filesystem sources without any special casing.  `CupOwnerStanding` is the eighth source — see the Kyle Busch section below.

```python
"""Outflow sidecar for the NASCAR fantasy-league fjord pipeline."""

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from incorporator import Incorporator


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


class CupOwnerStanding(Incorporator):
    """Owner-entry standings for the Cup series.  Keyed by vehicle_number
    (string: '133', '3', '33') — see the Kyle Busch / owner-seat section."""


# Owner-seat routing map: driver_id → vehicle_number (string).
# The per-pick scoring loop reads this at O(1) to route picks for
# deceased / released Cup drivers to CupOwnerStanding instead of CupStanding.
# Scoring policy only — conv_dict lives inline in the runner (nascar_fantasy.py).
OWNER_SCORED: dict[int, str] = {454: "133"}
```

> ⚠️ **Do not pre-declare fields on these classes.**  The framework builds Pydantic schemas dynamically from the incoming JSON; if you stub `track_id: int = None` on `Track`, fjord stops inferring and you lose half your columns to silent drops.  The classes are deliberately bare — source classes are seeded by dynamic schema inference from the incoming JSON, so a bare `pass` body is exactly right here.  (The framework does emit a one-time WARNING for a bare *output* class — see the §2g callout — but that path is the derived-class emit, not source seeding.)

### 2b. Constants

These two constants live in separate files because they serve different directions of data flow.

**`inflow.py`** (`inflow.py:28`):

```python
_DATE_FIELDS = ("date_scheduled", "race_date", "qualifying_date", "tunein_date")
```

`_DATE_FIELDS` lists every column in the Race payload that ships as an ISO string — the inflow's `conv_dict` adds `inc(datetime)` for each so they arrive as real `datetime` instances in the outflow.

**`outflow.py`** (`outflow.py:103`):

```python
_SERIES_LIST = ("Cup", "Busch", "Truck")
```

`_SERIES_LIST` is the human-readable label table used in `outflow(state)`; series IDs `1/2/3` map to indices `0/1/2`.

### 2c. The sentinel filter (`inflow.py`)

NASCAR's API returns `pole_winner_driver_id = 0` for races whose pole qualifying hasn't happened yet (or was rained out) — and `winner_driver_id = 0` for races that haven't been run yet.  Driver ID `0` coincidentally resolves to a real entry in the driver registry, so a naked `link_to` makes every future race resolve to the same incidental name.  The fix is a 3-line `extractor=` in `inflow.py`:

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

### 2d. The manufacturer helper (`inflow.py`)

NASCAR's `drivers.json` delivers `Manufacturer` as a logo-image URL — for example:

```
https://www.nascar.com/.../Chevrolet_2025-330x140.png
```

Without a converter, `driver.Manufacturer` holds that raw CDN URL.  Two downstream paths break silently when that happens:

1. **Owner-seat fallback in `outflow.py`.**  When `did in OWNER_SCORED`, the outflow reads `driver.Manufacturer` because the owner standings carry no manufacturer field.  The URL string would appear verbatim in the fantasy scoreboard's `manufacturer` column.
2. **`ManufacturerLeaderboard`.**  `CupStanding` rows carry a clean make name from the standings feed.  But any driver whose standings row is missing gets bucketed under the raw URL string, producing spurious manufacturer entries.

`_mfg_from_logo_url` in `inflow.py` (`inflow.py:49-63`) parses the make from the URL basename by stripping the path, removing the extension, and splitting on underscores and hyphens:

```python
def _mfg_from_logo_url(url: str) -> str:
    """Parse a NASCAR manufacturer logo URL into the make name.

    'https://www.nascar.com/.../Chevrolet_2025-330x140.png' -> 'Chevrolet'
    'https://www.nascar.com/.../Ford-Logo-1-320x180.png'   -> 'Ford'
    'https://www.nascar.com/.../Toyota-180x180.png'         -> 'Toyota'
    'https://www.nascar.com/.../Ram-330x115.png'            -> 'Ram'

    Splits the basename on underscores and hyphens; first token is the make.
    is_garbage_value pre-handles empty / None inputs — no defensive guard needed.
    """
    basename = url.rsplit("/", 1)[-1]  # 'Chevrolet_2025-330x140.png'
    stem = basename.split(".")[0]  # 'Chevrolet_2025-330x140'
    token = stem.replace("-", "_").split("_")[0]  # 'Chevrolet'
    return token
```

It is wired in the runner's `Driver` stream entry via `calc()`:

```python
"conv_dict": {
    "Manufacturer": calc(_mfg_from_logo_url, "Manufacturer", default="Unknown", target_type=str),
}
```

`calc()` handles empty or `None` inputs via `is_garbage_value` before the callable runs; those land as `default="Unknown"`.  The helper belongs in `inflow.py` because it shapes a value *as it is ingested* — it is a `conv_dict` converter, not an output-assembly helper.

### 2e. State-aware inflow

`inflow(state)` is called before each source seeds.  With `depends_on=["Track", "Driver"]` declared on `Race`, the engine splits sources into topo tiers and calls `inflow(state)` per tier — tier 0 sources (`Track`, `Driver`, the three Standings, `LeagueRoster`) each receive the current partial state as peers publish; `Race` (tier 1) sees a fully-populated state when `inflow` is called for it.  The `if "Track" in state and "Driver" in state:` guard is still necessary for the **refresh-wave** path — a peer refresh failure could leave the state incomplete, and without the guard `inflow` would emit a `link_to()` resolver pointing at a stale or missing registry.  Fjord re-applies `inflow(state)` on every refresh so the closures always see the latest snapshots.

```python
# ── State-aware inflow — wires Race.conv_dict against live peers ────


def inflow(state: dict[str, Any]) -> dict[str, Any]:
    """Build per-source ``conv_dict`` overrides from sibling registries.

    Inflow is called before each source's ``incorp()``.  With tiered-
    parallel seed (``depends_on=["Track", "Driver"]`` on Race), ``state``
    is fully populated with tier-0 registries by the time this fires for
    Race — no partial-state guards needed for the tier-1 entry.  The guard
    below is still correct for the refresh-wave path where state may briefly
    be incomplete if a peer refresh fails.
    """
    overrides: dict[str, Any] = {}
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

> **Keep the guard for refresh waves.**  During refresh, a peer source may temporarily fail — `Track` might be offline — which means `state["Track"]` is stale or absent.  The `if "Track" in state and "Driver" in state:` check lets `inflow` return `{}` safely, so `Race` re-uses its last-good `conv_dict` instead of getting a `KeyError`.  `depends_on` guarantees ordering at seed time; it does not suppress failures at refresh time.  See [Tutorial 10's seed-empty abort callout](../10-multi-source-fjord/README.md) for more on inflow failure handling.

**Foreign-key resolution is one-time, not lazy.**  Once a Race row is incorporated, `race.track_id` is the `Track` instance itself — `race.track_id.inc_name`, `race.track_id.city`, `race.track_id.length` all work directly.  No re-lookup in the outflow.  (The runner does `name_chg=[("track_id", "track")]` purely for readability — the field arrives renamed to `track` in the Race instance.)

### 2f. Helpers

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

### 2g. Outflow — three derived views in one function

`outflow(state)` returns `dict[ClassName, list[dict]]` and fjord builds **one dynamic Incorporator subclass per dict key** at first emit.  This is the multi-output contract: three keys in the return → three derived classes → three NDJSON files.

> ⚠️ **Do not pre-declare the output classes.**  `MonthlyRaceSchedule`, `FantasyTeam`, and `ManufacturerLeaderboard` are conspicuously absent from the source-class block at the top of the file.  Fjord builds them dynamically from the dict keys returned here and infers fields from the first emitted row.  A bare `class MonthlyRaceSchedule(Incorporator): pass` would suppress field inference and the export would land empty.

The function reads from `state["Driver"]`, `state["Race"]`, `state["LeagueRoster"]`, and the three Standings registries.  If any of the three required dependencies are missing (first wave hasn't completed yet), return `{}` to skip the emit.

```python
# ── Outflow — three derived views ──────────────────────────────────


def outflow(state: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Compute three derived views from the fused state.  Each dict
    key becomes a derived Incorporator subclass and is written to its
    matching ``export_params`` file by fjord's multi-output contract.
    """
    drivers = state.get("Driver")
    races = state.get("Race")
    league = state.get("LeagueRoster")
    if drivers is None or races is None or league is None:
        return {}

    # CupOwnerStanding is an optional eighth source — if it fails to load the
    # outflow degrades gracefully (owner-scored picks score 0 pts) rather than
    # aborting.  The owner-seat branch in View 2 reads this handle.
    owner_standings = state.get("CupOwnerStanding")

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
    monthly: list[dict[str, Any]] = []
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
    league_teams: dict[str, dict[int, list[Any]]] = {}
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

    fantasy: list[dict[str, Any]] = []
    for team_cd, roster in league_teams.items():
        team_obj: dict[str, Any] = {
            "team_id":          team_cd,
            "roster":           [],
            "points":           [],
            "manufacturer_mix": {},
            "total_wins":       0,
            "total_score":      0,
        }
        team_score = 0
        per_series: dict[int, int] = {}
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
    mfg_buckets: dict[str, list[Any]] = defaultdict(list)
    if cup is not None:
        for stnd in cup:
            mfg = (getattr(stnd, "manufacturer", "") or "").strip() or "Unknown"
            mfg_buckets[mfg].append(stnd)

    manufacturer_rows: list[dict[str, Any]] = []
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

`nascar_fantasy.py` is the runner.  It declares the eight sources (seven API + one local file), points `inflow=` at `inflow.py` and `outflow=` at `outflow.py`, and configures the three export targets.  The `Race` entry carries `depends_on=["Track", "Driver"]`, which opts the seed phase into tiered-parallel mode: the seven co-equal tier-0 sources fire concurrently, and `Race` seeds in tier 1 once those registries are ready.  `refresh_params=None` on every source = single-wave test mode; with no `export_interval` set, the pipeline exits cleanly after one outflow wave.

```python
"""NASCAR fantasy league as a multi-output fjord pipeline."""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

from incorporator import Incorporator, calc, inc

HERE = Path(__file__).resolve().parent
DATA = HERE / "out"  # examples/09-nascar-fantasy-fjord/out/

# Sibling sidecar import — Python only auto-adds HERE to sys.path for the
# bare ``python <script>`` invocation; explicit insert covers ``python -m``
# and other launch paths.
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from inflow import _mfg_from_logo_url  # noqa: E402
from outflow import (  # noqa: E402
    BuschStanding,
    CupOwnerStanding,
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

# Owner-standings exclusion list — drop the redundant name-component
# fields; ``owner_name`` is already the ``inc_name`` and is sufficient.
_OWNER_EXCL = ["owner_first_name", "owner_last_name", "owner_suffix"]

# Standings exclusion list — drop only the genuinely-noisy fields.
# Keep ``position``, ``top_5``, ``laps_led``, ``delta_leader``,
# ``poles``, ``starts``, ``manufacturer``, and ``playoff_eligible``:
# FantasyTeam scoring and ManufacturerLeaderboard both need them.
_STANDINGS_EXCL = [
    "is_clinch",
    "driver_first_name",
    "driver_last_name",
    "driver_suffix",
    "playoff_stage_wins",
]
# Driver exclusion list — keep ``Manufacturer``, ``Hometown_City``,
# ``Hometown_State`` (used by the enriched FantasyTeam roster).
_DRIVER_EXCL = [
    "Series_Logo",
    "Short_Name",
    "Description",
    "Hobbies",
    "Children",
    "Residing_City",
    "Residing_State",
    "Residing_Country",
    "Image_Transparent",
    "SecondaryImage",
    "Career_Stats",
    "Age",
    "Rank",
    "Points",
    "Points_Behind",
    "No_Wins",
    "Poles",
    "Top5",
    "Top10",
    "Laps_Led",
    "Stage_Wins",
    "Playoff_Points",
    "Playoff_Rank",
    "Integrated_Sponsor_Name",
    "Integrated_Sponsor",
    "Integrated_Sponsor_URL",
    "Silly_Season_Change",
    "Silly_Season_Change_Description",
    "Driver_Post_Status",
    "Driver_Part_Time",
]


async def main() -> None:
    print("🏁 Initiating NASCAR Data Gateway (fjord)...\n")
    DATA.mkdir(exist_ok=True)

    async for wave in Incorporator.fjord(
        stream_params=[
            # ── Static reference data — never refresh ──
            {
                "cls": Track,
                "incorp_params": {
                    "inc_url": f"{CFC_BASE}/tracks.json",
                    "rec_path": "items",
                    "inc_code": "track_id",
                    "inc_name": "track_name",
                },
                "refresh_params": None,  # tracks never change
            },
            # ── Drivers refresh occasionally ──
            {
                "cls": Driver,
                "incorp_params": {
                    "inc_url": f"{CFC_BASE}/drivers.json",
                    "rec_path": "response",
                    "inc_code": "Nascar_Driver_ID",
                    "inc_name": "Full_Name",
                    "excl_lst": _DRIVER_EXCL,
                    "conv_dict": {
                        # drivers.json carries Manufacturer as a logo-image URL
                        # (e.g. 'https://.../Chevrolet_2025-330x140.png').  Parse
                        # the make name from the URL basename so that owner-seat
                        # fallback in outflow.py yields a clean text string.
                        # Empty Manufacturer fields are handled by is_garbage_value
                        # before the callable runs and land as default='Unknown'.
                        "Manufacturer": calc(_mfg_from_logo_url, "Manufacturer", default="Unknown", target_type=str),
                    },
                },
                "refresh_params": None,
            },
            # ── Race schedule — depends on Track + Driver via inflow ──
            # depends_on enables tiered-parallel seed: Track + Driver +
            # the three Standings + LeagueRoster all fire concurrently in
            # tier 0; Race fires in tier 1 once its peers' registries are
            # available for link_to() resolution.
            {
                "cls": Race,
                "incorp_params": {
                    "inc_url": f"{CFC_BASE}/{CURRENT_YEAR}/race_list_basic.json",
                    "rec_path": "series_1",
                    "inc_code": "race_id",
                    "inc_name": "race_name",
                    "excl_lst": ["schedule", "track_name"],
                    "name_chg": [("track_id", "track")],
                },
                "depends_on": ["Track", "Driver"],
                "refresh_params": None,
            },
            # ── Live standings, one source per series ──
            {
                "cls": CupStanding,
                "incorp_params": {
                    "inc_url": f"{PROD_BASE}/1/{STANDINGS_BASE}",
                    "inc_code": "driver_id",
                    "inc_name": "driver_name",
                    "excl_lst": _STANDINGS_EXCL,
                    "conv_dict": {
                        "points": inc(int, default=0),
                        "wins": inc(int, default=0),
                        "top_10": inc(int, default=0),
                        "top_5": inc(int, default=0),
                        "laps_led": inc(int, default=0),
                        "position": inc(int, default=0),
                    },
                },
                "refresh_params": None,
            },
            {
                "cls": BuschStanding,
                "incorp_params": {
                    "inc_url": f"{PROD_BASE}/2/{STANDINGS_BASE}",
                    "inc_code": "driver_id",
                    "inc_name": "driver_name",
                    "excl_lst": _STANDINGS_EXCL,
                    "conv_dict": {
                        "points": inc(int, default=0),
                        "wins": inc(int, default=0),
                        "top_10": inc(int, default=0),
                        "top_5": inc(int, default=0),
                        "laps_led": inc(int, default=0),
                        "position": inc(int, default=0),
                    },
                },
                "refresh_params": None,
            },
            {
                "cls": TruckStanding,
                "incorp_params": {
                    "inc_url": f"{PROD_BASE}/3/{STANDINGS_BASE}",
                    "inc_code": "driver_id",
                    "inc_name": "driver_name",
                    "excl_lst": _STANDINGS_EXCL,
                    "conv_dict": {
                        "points": inc(int, default=0),
                        "wins": inc(int, default=0),
                        "top_10": inc(int, default=0),
                        "top_5": inc(int, default=0),
                        "laps_led": inc(int, default=0),
                        "position": inc(int, default=0),
                    },
                },
                "refresh_params": None,
            },
            # ── Owner-entry standings — Cup series ──
            # Keyed by vehicle_number (string: '133', '3', '33') rather
            # than owner_id because owner_id 553 repeats across all three
            # RCR entries.  Used by outflow.OWNER_SCORED to score roster
            # spots where a deceased/released Cup driver's pick is routed
            # to the team's owner-entry points instead.
            {
                "cls": CupOwnerStanding,
                "incorp_params": {
                    "inc_url": f"{CFC_BASE}/{CURRENT_YEAR}/1/final/1-owners-points.json",
                    "inc_code": "vehicle_number",
                    "inc_name": "owner_name",
                    "excl_lst": _OWNER_EXCL,
                    "conv_dict": {
                        "points": inc(int, default=0),
                        "wins": inc(int, default=0),
                        "top_5": inc(int, default=0),
                        "top_10": inc(int, default=0),
                        "starts": inc(int, default=0),
                        "position": inc(int, default=0),
                        "dnf": inc(int, default=0),
                        "winnings": inc(float, default=0),
                    },
                },
                "refresh_params": None,
            },
            # ── Local-file source: the fantasy league rosters ──
            # ``inc_file=`` routes through the same handler dispatch
            # as the API sources above — JSON format is inferred from
            # the file extension.  Rosters rarely change, so refresh
            # is opted out.
            {
                "cls": LeagueRoster,
                "incorp_params": {
                    "inc_file": str(HERE / "fixtures/league_teams.json"),
                    "inc_code": "team_id",
                    "inc_name": "team_id",
                },
                "refresh_params": None,
            },
        ],
        # The state-aware inflow sidecar (inflow.py) and output sidecar (outflow.py).
        inflow=str(HERE / "inflow.py"),
        outflow=str(HERE / "outflow.py"),
        # Per-class export_params — one entry per dict-key returned
        # by outflow(state).  Detection: nested dict shape = multi-output.
        export_params={
            "MonthlyRaceSchedule": {"file_path": str(DATA / "nascar_monthly_schedule.ndjson")},
            "FantasyTeam": {"file_path": str(DATA / "nascar_fantasy_scoreboard.ndjson")},
            "ManufacturerLeaderboard": {"file_path": str(DATA / "nascar_manufacturer_leaderboard.ndjson")},
        },
        # This is a one-shot test run — every source has
        # ``refresh_params=None`` above so no refresh daemon spawns
        # and the pipeline exits after a single outflow wave.
        #
        # For a production long-running daemon, drop the
        # ``refresh_params=None`` lines and uncomment the cadence
        # block below (per-class dict by name).
        #
        # refresh_interval={
        #     "Driver":        3600,   # 1 h
        #     "Race":          600,    # 10 min (pole finalises Sat)
        #     "CupStanding":   300,    # 5 min on race day
        #     "BuschStanding": 300,
        #     "TruckStanding": 300,
        # },
        # export_interval=60,
    ):
        op = wave.operation
        if wave.failed_sources:
            print(f"⚠️  {op:35s} chunk {wave.chunk_index}: {wave.failed_sources}")
        else:
            print(f"✅ {op:35s} chunk {wave.chunk_index}: {wave.rows_processed} rows")

    print("\n✅ Pipeline complete.")
    print(f"   • {DATA / 'nascar_monthly_schedule.ndjson'}")
    print(f"   • {DATA / 'nascar_fantasy_scoreboard.ndjson'}")
    print(f"   • {DATA / 'nascar_manufacturer_leaderboard.ndjson'}")


if __name__ == "__main__":
    asyncio.run(main())
```

Notable wiring:

* **Split-file inflow + outflow.**  `inflow=` points at `inflow.py` (the incoming-data manipulation sidecar: `inflow(state)`, `_mfg_from_logo_url`, `_driver_id_or_none`, `_DATE_FIELDS`) and `outflow=` points at `outflow.py` (the output-shaping sidecar: source classes, `outflow(state)`, `OWNER_SCORED`).  The two files are split by direction of data flow.
* **`refresh_params=None` everywhere = single-wave test mode.**  With no `export_interval` set either, the pipeline exits cleanly after one outflow wave.  Drop the `refresh_params=None` lines (refresh defaults on at 60s) and add `export_interval=60` to keep daemons alive for a production run.  Mix and match: leave `Track`'s refresh off (tracks never change) while letting standings refresh every 5 minutes.
* **`export_params` is keyed by output class name.**  Each key matches a key returned by `outflow(state)`; fjord's multi-output detection is "is there a top-level `file_path`?  No → multi-output."
* **Structured error surface.**  `wave.failed_sources` carries the bare-string view of any seed or refresh failures.  For structured per-source access use `LoggedIncorporator.get_error()` post-run — each record carries the source name, exception type, message, and HTTP retry metadata.  Honour any `retry_after` value and backoff on 429s without re-parsing the message string.

---

## 🏁 The Run

```bash
cd examples/09-nascar-fantasy-fjord
python nascar_fantasy.py
```

Expected console output (numbers depend on which races have been run this season):

```text
🏁 Initiating NASCAR Data Gateway (fjord)...

✅ fjord_incorp:Track                  chunk 1: 49 rows
✅ fjord_incorp:Driver                 chunk 1: 917 rows
✅ fjord_incorp:CupStanding            chunk 1: 39 rows
✅ fjord_incorp:BuschStanding          chunk 1: 59 rows
✅ fjord_incorp:TruckStanding          chunk 1: 61 rows
✅ fjord_incorp:CupOwnerStanding       chunk 1: 46 rows
✅ fjord_incorp:LeagueRoster           chunk 1: 8 rows
✅ fjord_incorp:Race                   chunk 1: 40 rows
✅ outflow:MonthlyRaceSchedule         chunk 1: 5 rows
✅ outflow:FantasyTeam                 chunk 1: 8 rows
✅ outflow:ManufacturerLeaderboard     chunk 1: 3 rows

✅ Pipeline complete.
   • examples/09-nascar-fantasy-fjord/out/nascar_monthly_schedule.ndjson
   • examples/09-nascar-fantasy-fjord/out/nascar_fantasy_scoreboard.ndjson
   • examples/09-nascar-fantasy-fjord/out/nascar_manufacturer_leaderboard.ndjson
```

Notice `Race` lands **after** `Track` and `Driver` — that's `depends_on=["Track", "Driver"]` doing its job.  The engine groups sources into topo tiers: tier 0 (`Track`, `Driver`, `CupStanding`, `BuschStanding`, `TruckStanding`, `CupOwnerStanding`, `LeagueRoster`) fires concurrently via `asyncio.gather`; tier 1 (`Race`) starts only after all tier-0 registries are in state, so the `link_to()` closures in `inflow(state)` resolve against live data on every run.

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

### Owner-seat scoring: Kyle Busch (RCR #8 → #133)

Kyle Busch (driver_id 454, RCR #8) died mid-season.  League rules say the roster spot stays, but scoring pivots from the driver's Cup points to the **RCR #133 owner-entry points** — the same car, renamed from the old #33 entry at the time of the switch.

The fix is entirely inside the outflow — no roster file changes, no second pass.  `OWNER_SCORED = {454: "133"}` is a module-level dict.  In the per-pick scoring loop a single O(1) branch detects the deceased driver and routes to `CupOwnerStanding.inc_dict['133']` instead of `CupStanding.inc_dict[454]`:

```python
if did in OWNER_SCORED and series_id == 1:
    owner_vnum = OWNER_SCORED[did]          # "133" — string key, not int
    stnd = owner_standings.inc_dict.get(owner_vnum) if owner_standings else None
    owner_seat = owner_vnum
else:
    stnd = series_cls.inc_dict.get(driver.inc_code) if series_cls else None
    owner_seat = None
```

The affected roster row is relabelled in the output:

```jsonc
{
  "series":     "Cup",
  "name":       "Kyle Busch [owner seat: RCR #133]",
  "car":        "8",
  "rank":       27,
  "points":     237,
  "wins":       0,
  "t10":        0,
  "top_5":      0,
  "laps_led":   0,
  "points_back": 420,
  "owner_seat": "133"
}
```

`laps_led` is emitted as `0` because the owner-entry feed does not track laps led.  All other fields (`manufacturer`, `hometown`, `team`, `car`) still read from the `Driver` registry — the car number and team name on the driver record remain intact.

**Adding a new entry** to `OWNER_SCORED` (e.g., `{456: "17"}` for a future hypothetical) is the only change required to route another pick.  The `CupOwnerStanding` feed carries all 46 owner entries so the new vehicle number is already in the registry.

> **Why `vehicle_number` and not `owner_id`?**  All three RCR entries (#3, #133, #33) share `owner_id = 553`.  Using `owner_id` as the PK would collapse them into one registry row and the downstream lookup `inc_dict.get('133')` would silently return the wrong record.  `vehicle_number` is unique across entries.

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

The same eight-source pipeline expressed as a JSON config — no Python wrapper required.  The CLI loader resolves `cls_name` by importing the `outflow=` file and looking up each class by name, which is how the multi-source registration stays JSON-serialisable.

### `config/pipeline.json`

```json
{
  "inflow":  "examples/09-nascar-fantasy-fjord/inflow.py",
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
      },
      "depends_on": ["Track", "Driver"]
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
          "points":   "inc(int, default=0)",
          "wins":     "inc(int, default=0)",
          "top_10":   "inc(int, default=0)",
          "top_5":    "inc(int, default=0)",
          "laps_led": "inc(int, default=0)",
          "position": "inc(int, default=0)"
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
      "cls_name": "CupOwnerStanding",
      "incorp_params": {
        "inc_url":   "https://cf.nascar.com/cacher/2026/1/final/1-owners-points.json",
        "inc_code":  "vehicle_number",
        "inc_name":  "owner_name",
        "excl_lst":  ["owner_first_name", "owner_last_name", "owner_suffix"],
        "conv_dict": {
          "points":   "inc(int, default=0)",
          "wins":     "inc(int, default=0)",
          "top_5":    "inc(int, default=0)",
          "top_10":   "inc(int, default=0)",
          "starts":   "inc(int, default=0)",
          "position": "inc(int, default=0)",
          "dnf":      "inc(int, default=0)",
          "winnings": "inc(float, default=0)"
        }
      },
      "refresh_params": null
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
    "TruckStanding": 300,
    "CupOwnerStanding": 300
  },
  "export_interval": 60
}
```

A few JSON-specific notes:

* **`refresh_params: null`** is the JSON spelling of Python's `refresh_params=None` — opts the source out of the refresh daemon.  Used here for `Track` (tracks never change) and `LeagueRoster` (rosters change rarely; restart the daemon when you edit the file).
* **`conv_dict` values are quoted strings.**  The token resolver in `cli/tokens.py` parses them at config-load time and substitutes the real callables.  `inc(int, default=0)` becomes the actual converter; same for `inc(datetime)` etc.  `link_to(state["…"])` calls live in `inflow(state)` — not in the JSON — because they need the runtime registry handle.
* **`name_chg` uses arrays not tuples.**  JSON has no tuple literal; `["track_id", "track"]` deserialises to the same shape the Python code uses.
* **`refresh_interval` as a dict** keyed by class name, exactly like Python — the JSON config accepts the same dict shape.
* **`export_params` keyed by output class** — multi-output detection is "is there a top-level `file_path` key?  No → multi-output."
* **Known limitation — `_mfg_from_logo_url` is not wired in the JSON form.**  The token resolver (`incorporator/usercode.py:121`) builds its allow-list from `extract_public_names`, which excludes any name beginning with an underscore (`not n.startswith("_")`).  Omitting the `conv_dict` for `Manufacturer` in the Driver entry means the field retains its raw CDN URL in the CLI form.  If you attempt to reference `_mfg_from_logo_url` by name in a JSON `conv_dict` string, the token resolver raises `TokenResolutionError` because the name is private.  To enable the converter in the CLI form, rename the helper to `mfg_from_logo_url` (drop the leading underscore) in `inflow.py` and reference it as `"calc(mfg_from_logo_url, 'Manufacturer', default='Unknown', target_type=str)"` in the Driver `conv_dict`.  The Python runner (`nascar_fantasy.py`) imports it directly and is unaffected.

### Validate + run

```bash
incorporator validate config/pipeline.json
incorporator fjord    config/pipeline.json --logs
```

`--logs` routes every Wave through the `LoggedIncorporator` queue handler into per-class log files: `logs/<ClassName>_api.log` (HTTP audit traces) and `logs/<ClassName>_error.log` (chunk waves; failures redacted).  An eight-source fjord produces up to 16 log files.  Add `--heartbeat-file /tmp/inc.beat` to pair with Docker's `HEALTHCHECK`.

### Docker

The repo's `docker-compose.yml` and `Dockerfile` work for this pipeline as-is.  Three host folders bind-mount into the container:

| Host | Container | What goes here |
|---|---|---|
| `./config` | `/app/config` *(read-only)* | `pipeline.json`, `inflow.py`, `outflow.py`, and `league_teams.json` |
| `./data` | `/app/data` | Three NDJSON outputs land here |
| `./logs` | `/app/logs` | Rotating JSON log files (when `--logs` is set) |

The wrinkle compared to a single-source fjord: **both sidecar files and `league_teams.json` must live where the container can read them**.  Easiest pattern is to drop them next to `pipeline.json` in `config/` and reference each with a container-relative path.  The two sidecars are now separate files — `inflow` and `outflow` each get their own copy:

```bash
mkdir -p config data logs
cp examples/09-nascar-fantasy-fjord/fixtures/league_teams.json config/league_teams.json
cp examples/09-nascar-fantasy-fjord/inflow.py                  config/inflow.py
cp examples/09-nascar-fantasy-fjord/outflow.py                 config/outflow.py
# edit config/pipeline.json so inflow points at config/inflow.py,
# outflow points at config/outflow.py, and inc_file points at config/league_teams.json
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
| `CupOwnerStanding` | 5 min | Owner points update on same cadence as driver points |
| `LeagueRoster` | refresh off | Edit the JSON + restart the daemon |
| Outflow wave | 60 s | Fused export every minute |

For an off-season demo (one-shot run with no refresh), set every `refresh_params: null` and drop `refresh_interval` / `export_interval` entirely — the pipeline exits cleanly after one outflow wave.  This pipeline calls *only* public NASCAR endpoints — no API keys, no auth.  The `${API_KEY}` / `${file:/run/secrets/...}` patterns documented in the [deployment guide](../../docs/deployment.md#secrets--local-vs-production) apply if you ever swap one of the sources for a paid feed.

---

## 🧠 What This Demonstrates

| Pattern | Where to look |
|---|---|
| **Tiered-parallel seed** | `depends_on=["Track", "Driver"]` on `Race` opts the engine into topo-tier mode: seven independent sources fire concurrently in tier 0 via `asyncio.gather`; `Race` seeds in tier 1 once all tier-0 registries are ready |
| **API + file source mixing** | `LeagueRoster` uses `inc_file=`; the other seven use `inc_url=`.  Same handler dispatch routes both transparently |
| **Sequential seed when state matters** | Without `depends_on`, `inflow=` triggers declaration-order sequential seeding; adding `depends_on` keeps the ordering guarantee while restoring within-tier parallelism |
| **Live foreign-key resolution** | `link_to(state["Track"])` / `link_to(state["Driver"], extractor=…)` in the inflow's `conv_dict` — and in the outflow, `race.track_id` is a live `Track` Pydantic instance with no re-lookup |
| **Sentinel-ID filter** | `extractor=_driver_id_or_none` short-circuits ID 0 to `None` at the graph boundary — applied to BOTH `pole_winner_driver_id` and `winner_driver_id` |
| **`stream()` vs `fjord()` vs `refresh()`** | `stream()` is paginated bulk-export chunking; `refresh()` is manual one-shot; `fjord()` (this tutorial) is the stateful multi-source daemon |
| **Empty-state contract in `inflow(state)`** | With tiered seed, tier-0 sources receive `state == {}`; tier-1 `Race` receives a fully-populated tier-0 state.  The `if "Track" in state and "Driver" in state:` guard still matters on subsequent refresh waves where a peer refresh might fail |
| **Deceased-driver owner-seat routing** | `OWNER_SCORED = {454: "133"}` in `outflow.py` is an O(1) map; the per-pick scoring loop has a single `if did in OWNER_SCORED` branch that routes `CupOwnerStanding.inc_dict['133']` instead of `CupStanding.inc_dict[driver_id]`.  No second pass, no roster-file change, no extra callsite |
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

> 👉 **Up next: [Tutorial 10 — Multi-Source Fjord](../10-multi-source-fjord/README.md).**  T9 walked the *full* fjord shape — eight sources, state-aware inflow, three outputs.  T10 introduces `fjord()` formally on its minimum viable form (two co-equal sources, one outflow) on the crypto-spread pattern.

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
