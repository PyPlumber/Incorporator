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

> 💡 **Read-time DX rule: coerce + join at build time; outflow reads plain attributes.**
> Every `getattr(x, "field", default) or fallback` guard in an `outflow()` function exists for one of two reasons: (1) the field hasn't been *coerced* yet (raw JSON string where you want an `int`/`float`/`bool`), or (2) the field hasn't been *joined* yet (a raw FK where you want the actual related object).  Both belong in the `conv_dict` at each source's own build time — `inc()`/`calc()` for coercion, `link_to()`/`link_to_list()` for joins — not in `outflow()`.  The framework's `is_garbage_value` null contract already does the defensive work once, at construction; a second defensive read at export time is pure duplication.
>
> This tutorial's `Standing` classes, `Track`, and `Driver` all coerce their own numeric/string fields in `nascar_fantasy.py`'s `conv_dict`s; `Race`'s three foreign keys and its `0.0`/`0`-sentinel fields resolve in `inflow.py`.  `outflow.py`'s three views read almost everything as plain attributes as a result.
>
> **Two joins stay deliberately read-time** — not because build-time is impossible in general, but because *this specific model* can't express them as a static per-field `conv_dict` entry:
> 1. **Roster → Driver** (`drivers.inc_dict.get(driver_id)`) — `LeagueRoster.roster` is a list of `{series_id, driver_id}` dicts, not a flat FK field; `link_to()` resolves one scalar field per conv_dict entry, it doesn't fan out a nested list-of-dicts.
> 2. **Per-pick Standings lookup** (`series_cls.inc_dict.get(...)` / `owner_standings.inc_dict.get(...)`) — the *target dataset itself* is chosen per-row at runtime (`series_id` picks Cup/Busch/Truck; `OWNER_SCORED` membership picks Owner vs Cup).  `link_to()` binds to ONE dataset per conv_dict entry; it can't branch between three datasets based on another field's runtime value.  This is dynamic dispatch, not a static FK — the honest boundary, not a shortcut.
>
> See `docs/api_atlas.md`'s "Build-time vs read-time: where coercion + joins belong" section for the general rule and why a read-time `inc_dict` registry accessor is deliberately **not** being added as a framework primitive.

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

* **`inflow.py` — incoming-data manipulation.**  The `inflow(state)` seed hook that wires `Race`'s foreign keys against `Track` + `Driver`, plus the `conv_dict` converter/extractor helpers that shape values *as they're ingested* (`_driver_id_or_none`, `mfg_from_logo_url`) and the ingestion constant `_DATE_FIELDS`.
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

`mfg_from_logo_url` in `inflow.py` (`inflow.py:67-81`) parses the make from the URL basename by stripping the path, removing the extension, and splitting on underscores and hyphens:

```python
def mfg_from_logo_url(url: str) -> str:
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
    "Manufacturer": calc(mfg_from_logo_url, "Manufacturer", default="Unknown", target_type=str),
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
                # Build-time coercion so outflow.py reads these as plain
                # attributes instead of getattr(race, "...", default).
                "number_of_cars_in_field": inc(int, default=0),
                "television_broadcaster": inc(str, default="TBD"),
                "playoff_round": inc(int, default=0),
                # 0.0-as-missing sentinel, same shape as the driver-ID
                # fields above but for calc() since there's no dataset to
                # join against -- just a float re-mapped to None.
                "pole_winner_speed": calc(_speed_or_none, "pole_winner_speed"),
            }
        }
    return overrides
```

Returning `{}` for a source = "no overrides, use the `incorp_params` as-declared".  Returning `{"Race": {"conv_dict": …}}` = "when fjord goes to seed `Race`, merge this `conv_dict` into its `incorp_params`".  The `link_to(state["Track"])` call captures the **live** `Track` registry, so when `Race`'s rows incorporate, every `track_id` integer is swapped for the matching `Track` Pydantic instance.

**The `_speed_or_none` helper** mirrors `_driver_id_or_none`'s shape but for a plain float field instead of a joined dataset:

```python
def _speed_or_none(raw: Any) -> float | None:
    """NASCAR returns 0.0 for pole_winner_speed on races whose pole hasn't
    been set yet (same sentinel pattern as the driver-ID fields above).
    Casts to float inline (rather than via calc()'s target_type=) so a
    genuine None result doesn't hit float(None) and log a per-row
    coercion warning.
    """
    return float(raw) if raw else None
```

`calc(_speed_or_none, "pole_winner_speed")` promotes the `0.0`-as-missing sentinel to `None` at build time — `outflow.py` then reads `race.pole_winner_speed` directly with no `if pole else None` guard.

> **Keep the guard for refresh waves.**  During refresh, a peer source may temporarily fail — `Track` might be offline — which means `state["Track"]` is stale or absent.  The `if "Track" in state and "Driver" in state:` check lets `inflow` return `{}` safely, so `Race` re-uses its last-good `conv_dict` instead of getting a `KeyError`.  `depends_on` guarantees ordering at seed time; it does not suppress failures at refresh time.  See [Tutorial 10's seed-empty abort callout](../10-multi-source-fjord/README.md) for more on inflow failure handling.

**Foreign-key resolution is one-time, not lazy.**  Once a Race row is incorporated, `race.track_id` is the `Track` instance itself — `race.track_id.inc_name`, `race.track_id.city`, `race.track_id.length` all work directly.  No re-lookup in the outflow.  (The runner does `name_chg=[("track_id", "track")]` purely for readability — the field arrives renamed to `track` in the Race instance.)

### 2f. Helpers

Two small string-composition helpers used by the outflow.  Pure functions, no state.

```python
# ── Helpers ────────────────────────────────────────────────────────


def _hometown(driver: Any) -> str:
    """Compose ``City, ST`` from the driver's hometown fields, or
    ``Unknown`` if either piece is missing.

    Hometown_City / Hometown_State are coerced to plain strings at
    Driver's own build time (inc(str, default="") in nascar_fantasy.py)
    -- no getattr(..., "") or "" guard needed here.  The ``city and
    state`` composition is business logic (how to format two strings
    together), not a null guard, so it stays.
    """
    city = driver.Hometown_City.strip()
    state = driver.Hometown_State.strip()
    if city and state:
        return f"{city}, {state}"
    return city or state or "Unknown"


def _track_loc(track: Any) -> str:
    """Compose ``City, ST`` for a track.

    ``track`` itself can be None (a Race whose Track FK didn't resolve)
    -- that "is there a related object at all" check is a legitimate
    null-object guard on the join result, not a field-coercion guard,
    and stays.  Track's city/state fields are not build-time coerced
    (tracks.json ships them as plain strings already), so the local
    ``or ""`` guard remains as defense against a genuinely missing key.
    """
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

Current-month Cup races with resolved track + driver context.  This is where both build-time mechanisms pay off: `race.track_id` is already a live `Track` instance (renamed to `race.track` via `name_chg` in the runner, but we fall back to the original name too), `race.pole_winner_driver_id` / `race.winner_driver_id` are live `Driver` instances or `None` thanks to the sentinel filter, and `race.pole_winner_speed` / `race.number_of_cars_in_field` / `race.television_broadcaster` / `race.playoff_round` are already coerced by `inflow.py`'s extended `conv_dict`.  Zero re-lookups, zero defensive coercion, in the outflow.

```python
    # ════════════════════════════════════════════════════════════════
    # View 1 — MonthlyRaceSchedule
    # ════════════════════════════════════════════════════════════════
    monthly: list[dict[str, Any]] = []
    for race in races:
        # date_scheduled arrives via inflow.py's inc(datetime) -- a Race
        # with a genuinely missing schedule date is a null-object case
        # (dt is None), not a coercion gap, so this guard stays.
        dt = getattr(race, "date_scheduled", None)
        if dt is None or dt.month != now.month or dt.year != now.year:
            continue
        # pole / winner / track can each be None -- the FK didn't resolve
        # (link_to's sentinel-aware extractor for driver IDs; a Race whose
        # track_id had no Track match) -- a null-object guard on the JOIN
        # result, not a field-coercion guard, so `if track else` etc. stay.
        pole = race.pole_winner_driver_id
        winner = race.winner_driver_id
        track = getattr(race, "track", None) or getattr(race, "track_id", None)

        monthly.append({
            "race_id":     race.inc_code,
            "date":        dt.strftime("%Y-%m-%d"),
            "race_name":   getattr(race, "race_name", "TBD"),
            "track":       getattr(track, "inc_name", "Unknown") if track else "Unknown",
            "track_type":  track.track_type if track else "Unknown",
            "track_miles": track.length if track else None,
            "track_loc":   _track_loc(track),
            "pole_winner": getattr(pole, "Full_Name", None) if pole else None,
            # inflow.py's _speed_or_none already promotes NASCAR's
            # 0.0-as-missing sentinel to None at build time.
            "pole_speed":  race.pole_winner_speed,
            "winner":      getattr(winner, "Full_Name", None) if winner else None,
            "cars":        race.number_of_cars_in_field,
            "tv":          race.television_broadcaster,
            "playoff":     bool(race.playoff_round),
        })
    monthly.sort(key=lambda r: r["date"])
```

Note the navigation idioms:

* **`race.inc_code`** — every Incorporator instance exposes its primary key via `inc_code` and its display name via `inc_name`.  This is how you get back at the original ID after fjord's field renaming.
* **`getattr(race, "track", None) or getattr(race, "track_id", None)`** — fjord renames `track_id` → `track` per the runner's `name_chg`, but we accept either spelling defensively.
* **`getattr(pole, "Full_Name", None) if pole else None`** — `pole` is a live `Driver` Pydantic instance whose schema came from the API; `Full_Name` is the API's actual field name and reaches us unchanged.
* **`race.pole_winner_speed` (plain attribute, no guard)** — `inflow.py`'s `_speed_or_none` converter already ran at Race's build time; the field is either a real float or `None`, never the raw `0.0` sentinel.

#### View 2 — `FantasyTeam`

Per-team scoreboard.  For each team's roster pick `{series_id, driver_id}`:

1. Look up the driver in `state["Driver"].inc_dict` by `driver_id` → live `Driver` instance.
2. Look up that same driver in the matching series Standings (`state["CupStanding"]` / `BuschStanding` / `TruckStanding`) — or, for a Kyle-Busch-style owner-seat pick, in `state["CupOwnerStanding"]` instead (see the Kyle Busch section below).
3. Pull manufacturer / wins / position / points off the Standings row (all build-time coerced — plain attribute reads), and hometown / team / car number off the Driver row (also build-time coerced).

`.inc_dict.get(key)` is the framework's O(1) primary-key lookup on the registry — every `IncorporatorList` exposes it.  This is the honest read-time boundary described above: **both** lookups here are joins whose target dataset can't be pinned to one `conv_dict` entry, so both stay read-time — everything downstream of them is a plain attribute read.

```python
    # ════════════════════════════════════════════════════════════════
    # View 2 — FantasyTeam
    # ════════════════════════════════════════════════════════════════
    # This roster -> Driver lookup stays read-time: LeagueRoster.roster is
    # a list of {series_id, driver_id} dicts (not a flat FK field), and
    # Driver seeds in the same tier as LeagueRoster with no ordering
    # guarantee between tier-0 siblings -- link_to() can't fan out a
    # nested list-of-dicts at build time, so this is the honest boundary.
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
                league_teams[team_cd][sid].sort(key=lambda d: int(d.Badge))

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
                did = int(driver.inc_code)
                # Conditional join whose TARGET dataset is chosen per-row at
                # runtime (series_id picks Cup/Busch/Truck; OWNER_SCORED
                # membership picks Owner vs Cup) -- link_to() binds to ONE
                # dataset per conv_dict entry and can't branch between three
                # datasets on another field's runtime value.  Stays read-time.
                if did in OWNER_SCORED and series_id == 1:
                    owner_vnum = OWNER_SCORED[did]
                    stnd = owner_standings.inc_dict.get(owner_vnum) if owner_standings else None
                    owner_seat: str | None = owner_vnum
                else:
                    stnd = series_cls.inc_dict.get(driver.inc_code) if series_cls else None
                    owner_seat = None

                # stnd itself is a null-object guard on the join result (a
                # driver with no standings row) -- every field READ off
                # stnd below is a plain attribute because the Standing
                # classes' own conv_dict (nascar_fantasy.py) coerced them.
                pts = stnd.points if stnd else 0
                wins = stnd.wins if stnd else 0
                per_series[series_id] += pts
                total_wins += wins

                mfg = (stnd.manufacturer if stnd and owner_seat is None else "") or driver.Manufacturer or "Unknown"
                mfg = mfg.strip() or "Unknown"
                mfg_counter[mfg] += 1

                driver_name = getattr(driver, "inc_name", "Unknown").strip()
                row: dict[str, Any] = {
                    "series":       series_name,
                    "car_idx":      car_idx,
                    "name":         f"{driver_name} [owner seat: RCR #{owner_seat}]" if owner_seat else driver_name,
                    "car":          driver.Badge,
                    "team":         driver.Team.strip() or "Unknown",
                    "manufacturer": mfg,
                    "hometown":     _hometown(driver),
                    "rank":         stnd.position if stnd else None,
                    "wins":         wins,
                    "t10":          stnd.top_10 if stnd else 0,
                    "top_5":        stnd.top_5 if stnd else 0,
                    # laps_led is not tracked in owner standings; emit 0.
                    "laps_led":     stnd.laps_led if stnd and owner_seat is None else 0,
                    "points":       pts,
                    # CupOwnerStanding doesn't carry delta_leader either --
                    # same honest-boundary reason as laps_led above.
                    "points_back":  abs(stnd.delta_leader) if stnd and owner_seat is None else None,
                }
                if owner_seat is not None:
                    row["owner_seat"] = owner_seat
                team_obj["roster"].append(row)
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

Every field read off `stnd` or `driver` above is a plain attribute — no `getattr(..., default) or fallback` — because `nascar_fantasy.py`'s `conv_dict`s guarantee they're always present.  Only `stnd` itself (does this driver have a standings row at all?) and `owner_seat is None` (is this an owner-seated pick, whose source class lacks certain fields?) remain as guards — both null-object / cross-source-shape checks, not coercion gaps.

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
            mfg = stnd.manufacturer.strip() or "Unknown"
            mfg_buckets[mfg].append(stnd)

    manufacturer_rows: list[dict[str, Any]] = []
    for mfg, rows in mfg_buckets.items():
        if mfg == "Unknown":
            continue
        top = max(rows, key=lambda s: s.points)
        manufacturer_rows.append({
            "manufacturer":  mfg,
            "drivers":       len(rows),
            "total_points":  sum(s.points for s in rows),
            "total_wins":    sum(s.wins for s in rows),
            "playoff_seats": sum(1 for s in rows if s.playoff_eligible),
            "top_driver":    getattr(top, "inc_name", "Unknown"),
            "top_points":    top.points,
        })
    manufacturer_rows.sort(key=lambda r: -r["total_points"])

    return {
        "MonthlyRaceSchedule":     monthly,
        "FantasyTeam":             fantasy,
        "ManufacturerLeaderboard": manufacturer_rows,
    }
```

`stnd.manufacturer` / `stnd.points` / `stnd.wins` / `stnd.playoff_eligible` are plain attributes here too — `CupStanding`'s own `conv_dict` in `nascar_fantasy.py` coerces all four (`manufacturer` defaults to `""`, `playoff_eligible` defaults to `False`).  No null-object guard is needed at all in this view: every `stnd` came from iterating `cup` directly (a real `CupStanding` instance, never `None`).

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

from inflow import mfg_from_logo_url  # noqa: E402
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
    print("Initiating NASCAR Data Gateway (fjord)...\n")
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
                    "conv_dict": {
                        "track_type": inc(str, default="Unknown"),
                        "length": inc(float, default=None),
                    },
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
                        "Manufacturer": calc(mfg_from_logo_url, "Manufacturer", default="Unknown", target_type=str),
                        "Hometown_City": inc(str, default=""),
                        "Hometown_State": inc(str, default=""),
                        "Team": inc(str, default=""),
                        # Badge is a numeric-string car number ("5", "8") in
                        # the raw feed; keep the "0" default numeric-string
                        # (not "N/A") so outflow.py's sort-by-car-number
                        # int(driver.Badge) never breaks on a missing badge.
                        "Badge": inc(str, default="0"),
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
                        "delta_leader": inc(int, default=0),
                        "manufacturer": inc(str, default=""),
                        "playoff_eligible": inc(bool, default=False),
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
                        "delta_leader": inc(int, default=0),
                        "manufacturer": inc(str, default=""),
                        "playoff_eligible": inc(bool, default=False),
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
                        "delta_leader": inc(int, default=0),
                        "manufacturer": inc(str, default=""),
                        "playoff_eligible": inc(bool, default=False),
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
            print(f"WARN  {op:35s} chunk {wave.chunk_index}: {wave.failed_sources}")
        else:
            print(f"OK    {op:35s} chunk {wave.chunk_index}: {wave.rows_processed} rows")

    print("\nPipeline complete.")
    print(f"   - {DATA / 'nascar_monthly_schedule.ndjson'}")
    print(f"   - {DATA / 'nascar_fantasy_scoreboard.ndjson'}")
    print(f"   - {DATA / 'nascar_manufacturer_leaderboard.ndjson'}")


if __name__ == "__main__":
    asyncio.run(main())
```

Notable wiring:

* **Split-file inflow + outflow.**  `inflow=` points at `inflow.py` (the incoming-data manipulation sidecar: `inflow(state)`, `mfg_from_logo_url`, `_driver_id_or_none`, `_DATE_FIELDS`) and `outflow=` points at `outflow.py` (the output-shaping sidecar: source classes, `outflow(state)`, `OWNER_SCORED`).  The two files are split by direction of data flow.
* **`refresh_params=None` everywhere = single-wave test mode.**  With no `export_interval` set either, the pipeline exits cleanly after one outflow wave.  Drop the `refresh_params=None` lines (refresh defaults on at 60s) and add `export_interval=60` to keep daemons alive for a production run.  Mix and match: leave `Track`'s refresh off (tracks never change) while letting standings refresh every 5 minutes.
* **`export_params` is keyed by output class name.**  Each key matches a key returned by `outflow(state)`; fjord's multi-output detection is "is there a top-level `file_path`?  No → multi-output."
* **Structured error surface.**  `wave.failed_sources` carries the bare-string view of any seed or refresh failures.  For structured per-source access use `LoggedIncorporator.get_rejects()` post-run — it unions `_api.log` + `_error.log` and returns records with a top-level `"reject"` key.  Each entry carries `source`, `error_kind`, `is_url_traffic_error` (bool — `True` for HTTP/network failures, `False` for parse failures), and `retry_after`.  Use `get_api()` for URL-traffic failures only, `get_error()` for codebase failures only.

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
  "points_back": null,
  "owner_seat": "133"
}
```

`laps_led` is emitted as `0` and `points_back` as `null` because the owner-entry feed does not track laps led or delta-to-leader — both fields are `CupStanding`-only, gated on `owner_seat is None`.  All other fields (`manufacturer`, `hometown`, `team`, `car`) still read from the `Driver` registry — the car number and team name on the driver record remain intact.

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

## Run it from the CLI

The same eight-source pipeline expressed as a JSON config — no Python wrapper
required — ships next to `nascar_fantasy.py` as [`pipeline.json`](pipeline.json),
with [`inflow.py`](inflow.py) and [`outflow.py`](outflow.py) as its sidecars
(the same two files `nascar_fantasy.py` imports). No inline JSON duplicate
here; the CLI loader resolves each `cls_name` by importing the `outflow=`
file and looking up the class by name, which is how the multi-source
registration stays JSON-serialisable. Full run instructions, the one
CLI-vs-Python behavioural difference (`Driver.Manufacturer`), and Docker
are in the addendum at the bottom of this page.

The `--logs` flag routes every Wave through the `LoggedIncorporator` queue
handler into one **unified session**, not one per source class:
`logs/LoggedIncorporator_api.log` (URL/internet-traffic errors — HTTP
4xx/5xx, timeouts), `logs/LoggedIncorporator_error.log` (successful waves,
parse failures, schema errors), `logs/LoggedIncorporator_debug.log`
(superset), and `logs/LoggedIncorporator_tide.log` (per-wave summary) —
four files total, regardless of source count. `LoggedIncorporator.fjord()`'s
own docstring documents this as intentional: every source's waves and every
outflow emission land under the one class the CLI invoked `fjord()` on, "so
one `get_error` call returns the full pipeline's error history." (Per-class
routing is a `LoggedIncorporator.stream()` behaviour — genuine when you
subclass per source — not a `fjord()` one.) Use `get_rejects()` to read all
failures across both routing files; use `entry["reject"]["is_url_traffic_error"]`
to classify each one. Add `--heartbeat-file /tmp/inc.beat` to pair with
Docker's `HEALTHCHECK`.

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

## 🐳 Run It From the CLI (+ Docker)

Reference material — three ways to run the exact same eight-source pipeline, in order.

**1. Python entry** (what every section above walked through):

```bash
cd examples/09-nascar-fantasy-fjord
python nascar_fantasy.py
```

**2. CLI form** — [`pipeline.json`](pipeline.json) ships next to the entry
script, with [`inflow.py`](inflow.py) / [`outflow.py`](outflow.py) as its
sidecars — the same two files the Python entry imports. No inline JSON
duplicate here (see it drift once, trust it forever).

```bash
cd examples/09-nascar-fantasy-fjord
incorporator validate pipeline.json
incorporator fjord pipeline.json --logs
```

> **Run from inside this directory.** Every `export_params` entry
> (`"out/nascar_monthly_schedule.ndjson"`, etc.) is CWD-relative, not
> config-dir-relative. Running `incorporator fjord
> examples/09-nascar-fantasy-fjord/pipeline.json` from the repo root
> silently writes to `<repo-root>/out/` instead.
>
> **One-shot, not a daemon.** Every `stream_params` entry sets
> `"refresh_params": null` and the config declares no top-level
> `refresh_interval` / `export_interval`, so the pipeline seeds all eight
> sources once, outflows once, and exits — matching
> `nascar_fantasy.py`'s "one-shot test run" design (see the commented-out
> production `refresh_interval` block in that file for the cadences a
> live daemon would use: `Driver` 1 h, `Race` 10 min, the three standings
> 5 min each, outflow every 60 s).
>
> **`Driver.Manufacturer` is wired identically in both forms.** The JSON
> config's `Driver` entry carries
> `"Manufacturer": "calc(mfg_from_logo_url, 'Manufacturer', default='Unknown', target_type=str)"`
> — the same converter `nascar_fantasy.py` calls directly. The token
> resolver's public-name allow-list (built from `inflow.py`'s exports)
> excludes any identifier starting with an underscore, which is why the
> helper in `inflow.py` is named `mfg_from_logo_url` (no leading
> underscore) rather than a private name.

**3. Docker** — reasoned from the `Dockerfile`/`docker-compose.yml`, **NOT
run or verified** (no Docker available in this pass — confirm before
relying on it):

```bash
# Reasoned, unverified.
docker run --rm \
  --user "$(id -u):$(id -g)" \
  -v "$(pwd)/examples/09-nascar-fantasy-fjord:/app/config:ro" \
  -v "$(pwd)/examples/09-nascar-fantasy-fjord/out:/app/out" \
  incorporator:latest \
  fjord /app/config/pipeline.json --logs
```

The image's `WORKDIR` is `/app`, and every `export_params.file_path` is
CWD-relative (never rebased against the config's directory) — so
`pipeline.json`'s `"out/..."` targets resolve to `/app/out/...` inside
the container. The mount target must therefore be `/app/out`, not one of
the three paths the `Dockerfile` prepares (`/app/config`, `/app/data`,
`/app/logs`). Mounting the whole example directory read-only at
`/app/config` carries `pipeline.json`, `inflow.py`, `outflow.py`, and
`fixtures/league_teams.json` together in one mount — no `cp`-into-`config/`
staging step needed, unlike an earlier revision of this section that
predated bare, config-dir-relative sidecar refs. `--user` lets the
non-root `appuser` write to the separately-mounted `/app/out`.
`docker compose up -d` isn't used here — `docker-compose.yml`'s volumes
anchor at repo-root `./config` / `./data`, not at this tutorial's own
directory, and editing the compose file is out of scope for this pass.

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/09-nascar-fantasy-fjord/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
