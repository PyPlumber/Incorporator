***

# 🕸️ Tutorial 9 — NASCAR Fantasy Fjord: 8 sources, 3 outputs, 1 config

**Prerequisites:** [Tutorial 8 — Streaming Daemons](../08-streaming-daemon/README.md) (the `stream()` shape and `outflow(state)` mechanics).

You're the commissioner of an 8-team NASCAR fantasy league.  Every Sunday morning before the green flag drops you need three derived analytical views on the same desk: **this month's Cup schedule** (so the group chat knows what's worth tuning in for), **the league scoreboard** (so trash talk is correctly calibrated), and **the manufacturer leaderboard** (so the Chevy-vs-Ford bet has a settled score).  All three live behind the same seven NASCAR APIs and one hand-maintained roster file.  Wired naively this is a nine-script crontab.  Wired through fjord it's a single async-for loop and one outflow function.

T8 introduced streaming polling on single-source registries.  T9 walks the *full* multi-source production shape — schema-free ingestion of seven heterogeneous JSON endpoints and one local file into eight bare `class Foo(Incorporator): pass` subclasses (no field declarations, no Pydantic schemas hand-written), parallel seed, read-time cross-source joins with sentinel-ID guards, and a single outflow returning a `dict` whose keys become three derived classes, three NDJSON files.  T10 will introduce `fjord()` formally on the minimum-viable two-source case.  You're getting the production shape first, the abstraction next.

---

## 🎯 The Goal

For the current NASCAR Cup, Busch, and Truck seasons, in **one** fjord call:

1. **`MonthlyRaceSchedule`** — current-month Cup races with resolved track name, **track type**, **track length**, **city/state**, pole winner, **pole speed**, **race winner** (past races), car count, **TV broadcaster**, and **playoff flag**.  Future races' pole / winner / speed columns land as `null` (qualifying / race hasn't happened yet).
2. **`FantasyTeam`** — the 8-team league scoreboard sorted by total points.  One row per team with their full resolved roster; each per-driver row carries **manufacturer**, **hometown**, **current series rank**, **wins**, **top-5s**, **top-10s**, **laps led**, **points**, and **points back**.  Per-team summary block carries a **manufacturer mix** counter (Chevy vs Ford vs Toyota) and a total-wins tally.
3. **`ManufacturerLeaderboard`** — Cup-series rollup, one row per Chevrolet / Ford / Toyota, with driver count, total points, total wins, playoff seats, and the top driver per make.

All three views exported as NDJSON.  Eight sources, one outflow, three output files, no daemons — one sidecar file for the whole ETL.

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

None of the eight sources needs another source's registry to build itself — each coerces only its own fields at its own build time.  All eight seed in **parallel** via `asyncio.gather`, one wave per source.  `Race`'s three foreign keys resolve later, at read time, inside `outflow(state)` — see the read-time DX rule below.

---

## 🗂️ Project Layout

By the end of this tutorial you'll have laid down three files:

```text
examples/09-nascar-fantasy-fjord/
├── fixtures/
│   └── league_teams.json     ← Step 1 — the roster
├── outflow.py                ← Step 2 — source classes + output-assembly policy + outflow
└── nascar_fantasy.py         ← Step 3 — the runner
```

`outflow.py` is the only sidecar this pipeline needs: the eight source classes, the output-assembly policy (`OWNER_SCORED`), the row helpers, and `outflow(state)` — including the three cross-source FK joins, resolved read-time against the live snapshot `outflow(state)` receives each wave.

The output directory (`out/`) is created at runtime; you don't need to make it.

> 💡 **Read-time DX rule: each source coerces its own fields; joins happen where the peer data actually lives.**
> If you ever find yourself reaching for `getattr(x, "field", default) or fallback` inside an `outflow()` function, it's a sign one of two things hasn't happened yet: (1) the field hasn't been *coerced* (raw JSON string where you want an `int`/`float`/`bool`), or (2) the field hasn't been *joined* (a raw FK where you want the actual related object).  Coercion belongs in each source's own `conv_dict`, at its own build time — `inc()` for a plain type conversion, `calc()` for one that needs a function (like `Race`'s `speed_or_none` sentinel guard).  A cross-source join belongs in `outflow(state)`, at read time: `fjord()` hands `outflow()` a live snapshot of every source, taken under its own shared lock, every wave — `state["Track"].inc_dict.get(race.track_id)` is a safe, cheap, O(1) lookup, no different in cost or safety from a build-time join.  This tutorial's `outflow.py` has zero `getattr(` calls precisely because both boundaries are honored.
>
> This tutorial's `Standing` classes, `Track`, `Driver`, and `Race` all coerce their own numeric/string/date fields in `nascar_fantasy.py`'s `conv_dict`s.  `outflow.py`'s three views then read those coerced fields as plain attributes.  Every cross-source lookup — `Race`'s three FK joins (`track_id` → `Track`, `pole_winner_driver_id` / `winner_driver_id` → `Driver`), the roster → Driver fan-out, and the per-pick Standings dispatch — happens in `outflow(state)`, uniformly, via `.inc_dict.get(...)`.  There's one join mechanism in this tutorial, not two: no `link_to()`, no `depends_on` tiering, no state-aware `inflow(state)` sidecar.  (`inflow(state)` still exists in the framework for the rarer case where a *build-time* join is required — a downstream source's own `conv_dict` needs the resolved object before it can finish building.  Nothing in this pipeline needs that.)

---

## 🔧 Step 1: The Roster Fixture

`fixtures/league_teams.json` is the **only** piece of business logic that isn't either an API or framework wiring — it's the league commissioner's source of truth.  Eight teams, each with a roster of 8 drivers (1 Truck pick, 1 Busch pick, 6 Cup picks).  Here's the shape (first team shown; all eight ship in the file):

```json
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
  }
]
```

> The full file is [`fixtures/league_teams.json`](fixtures/league_teams.json) (106 lines, 8 teams) — this walkthrough excerpts one team to show the shape.

**Schema rationale.**  Each team has a `team_id` (used as the registry key — `inc_code=team_id` in the driver script) and a `roster` array.  Each roster row is a `(series_id, driver_id)` pair where `series_id` is `1` (Cup), `2` (Busch), or `3` (Truck) and `driver_id` joins to `Driver.Nascar_Driver_ID`.  The outflow uses `series_id` to pick which Standings registry to look the driver up in, and `driver_id` to pull the live `Driver` instance for hometown / team / manufacturer.

---

## 🔧 Step 2: `outflow.py`

The ETL lives in one sidecar: `outflow.py` declares the eight source classes, the output-assembly policy (`OWNER_SCORED`), the row helpers (`_hometown`, `_track_loc`, `_SERIES_LIST`), the two shared `conv_dict` converters (`mfg_from_logo_url`, `speed_or_none` — public so `pipeline.json`'s token form can resolve them too), and the `outflow(state)` function that emits the three derived views, including the read-time FK joins.

The runner points `outflow=` at `outflow.py`.  It ships next to this README; we'll walk the parts that matter below.

### 2a. Source classes

Each fjord source needs its own subclass so the Standings classes don't share `inc_dict`.  `LeagueRoster` is the only local-file source — fed by a hand-curated JSON file, demonstrating that fjord mixes API + filesystem sources without any special casing.  `CupOwnerStanding` is the eighth source — see the Kyle Busch section below.

```python
from incorporator import Incorporator


# ── Source classes ─────────────────────────────────────────────────


class Track(Incorporator):
    pass


# Driver, Race, CupStanding, BuschStanding, and TruckStanding follow the
# identical bare-``pass`` shape as Track above.


class LeagueRoster(Incorporator):
    """League membership read from ``league_teams.json``.  Keyed by
    ``team_id``; each instance carries a ``roster`` list of
    ``{series_id, driver_id}`` picks."""


class CupOwnerStanding(Incorporator):
    """Owner-entry standings for the Cup series.

    Keyed by ``vehicle_number`` (a string: '133', '3', '33', …) rather than
    ``owner_id`` because owner_id 553 repeats across all three RCR entries
    (#3, #133, #33).  The RCR #133 row (position 27, 237 pts) is the
    owner-seat substitute for Kyle Busch (driver_id 454) after his
    mid-season death.  The car was renumbered from #33 to #133 at the
    same time.
    """


# ── Deceased-driver owner-seat routing ────────────────────────────
# Map driver_id → vehicle_number (string) for picks that must score
# from the owner standings instead of the driver standings.
# Adding a new entry here is sufficient to route any future deceased /
# released driver; no other code changes are required.
# Scoring policy only — conv_dict lives inline in the runner (nascar_fantasy.py).
OWNER_SCORED: dict[int, str] = {454: "133"}
```

> The full file is [`outflow.py`](outflow.py) (~380 lines) — this walkthrough excerpts the parts that matter.

> ⚠️ **Do not pre-declare fields on these classes.**  The framework builds Pydantic schemas dynamically from the incoming JSON; if you stub `track_id: int = None` on `Track`, fjord stops inferring and you lose half your columns to silent drops.  The classes are deliberately bare — source classes are seeded by dynamic schema inference from the incoming JSON, so a bare `pass` body is exactly right here.  (The framework does emit a one-time WARNING for a bare *output* class — see the §2f callout — but that path is the derived-class emit, not source seeding.)

### 2b. Constants

```python
_SERIES_LIST = ("Cup", "Busch", "Truck")
```

`_SERIES_LIST` is the human-readable label table used in `outflow(state)`; series IDs `1/2/3` map to indices `0/1/2`.  (`_DATE_FIELDS` — the Race columns that ship as ISO date strings — lives in `nascar_fantasy.py` instead, right next to the static `conv_dict` that uses it; see Step 3.)

### 2c. The manufacturer helper (`outflow.py`)

NASCAR's `drivers.json` delivers `Manufacturer` as a logo-image URL — for example:

```
https://www.nascar.com/.../Chevrolet_2025-330x140.png
```

Without a converter, `driver.Manufacturer` holds that raw CDN URL.  Two downstream paths break silently when that happens:

1. **Owner-seat fallback in `outflow.py`.**  When `did in OWNER_SCORED`, the outflow reads `driver.Manufacturer` because the owner standings carry no manufacturer field.  The URL string would appear verbatim in the fantasy scoreboard's `manufacturer` column.
2. **`ManufacturerLeaderboard`.**  `CupStanding` rows carry a clean make name from the standings feed.  But any driver whose standings row is missing gets bucketed under the raw URL string, producing spurious manufacturer entries.

`mfg_from_logo_url` parses the make from the URL basename by stripping the path, removing the extension, and splitting on underscores and hyphens:

```python
def mfg_from_logo_url(url: str) -> str:
    """Parse a NASCAR manufacturer logo URL into the make name.

    'https://www.nascar.com/.../Chevrolet_2025-330x140.png' -> 'Chevrolet'
    'https://www.nascar.com/.../Ford-Logo-1-320x180.png'   -> 'Ford'
    'https://www.nascar.com/.../Toyota-180x180.png'         -> 'Toyota'
    'https://www.nascar.com/.../Ram-330x115.png'            -> 'Ram'

    Splits the basename on underscores and hyphens; first token is the make.
    is_garbage_value pre-handles empty / None inputs — no defensive guard needed.
    Public (no leading underscore) so pipeline.json's conv_dict token
    ``calc(mfg_from_logo_url, ...)`` resolves against this sidecar's
    public namespace.
    """
    basename = url.rsplit("/", 1)[-1]  # 'Chevrolet_2025-330x140.png'
    stem = basename.split(".")[0]  # 'Chevrolet_2025-330x140'
    token = stem.replace("-", "_").split("_")[0]  # 'Chevrolet'
    return token
```

`nascar_fantasy.py` imports it from `outflow.py` (alongside the eight source classes) and wires it in the `Driver` stream entry via `calc()`:

```python
"conv_dict": {
    "Manufacturer": calc(mfg_from_logo_url, "Manufacturer", default="Unknown", target_type=str),
}
```

`calc()` handles empty or `None` inputs via `is_garbage_value` before the callable runs; those land as `default="Unknown"`.

### 2d. Read-time joins — Race's three foreign keys

`Race` carries three foreign keys — `track_id` into `Track`, `pole_winner_driver_id` and `winner_driver_id` into `Driver` — and resolves all three the same way every other cross-source lookup in this outflow does: read time, via `.inc_dict.get(...)` against the live snapshot `outflow(state)` receives each wave.  `Race` seeds with no knowledge of its peers; it doesn't need any.

```python
tracks = state.get("Track")
```

Inside the `MonthlyRaceSchedule` loop:

```python
track = tracks.inc_dict.get(race.track_id) if tracks else None
pole = drivers.inc_dict.get(race.pole_winner_driver_id) if race.pole_winner_driver_id else None
winner = drivers.inc_dict.get(race.winner_driver_id) if race.winner_driver_id else None
```

`race.track_id` / `race.pole_winner_driver_id` / `race.winner_driver_id` are Race's own raw FK ints — nothing renames or resolves them at build time, so they arrive at `outflow(state)` exactly as NASCAR's API sent them.

**The 0-as-missing sentinel.**  NASCAR returns `pole_winner_driver_id = 0` for races whose pole qualifying hasn't happened yet (or was rained out), and `winner_driver_id = 0` for races that haven't been run yet.  Driver ID `0` coincidentally resolves to a real entry in the driver registry, so a naked `.inc_dict.get(0)` would make every future race resolve to the same incidental name.  The `if race.pole_winner_driver_id else None` guard (falsy `0`/`None`/`""` short-circuits to `None` before the lookup runs) is the fix — one line, right where the join happens.

> 💡 **The pattern generalises.**  Any third-party API with sentinel IDs (Discord's `-1`-as-deleted-user, Twitter's `0`-as-anon-author, a SQL `NULL` foreign-key) lands at the same boundary: guard the sentinel to `None` before the lookup runs.  No per-call guard anywhere else in consumer code.

`race.pole_winner_speed` needs no read-time guard — Race's own `conv_dict` (Step 3) already promotes NASCAR's `0.0`-as-missing sentinel to `None` at Race's build time via `speed_or_none()`:

```python
def speed_or_none(raw: Any) -> float | None:
    """NASCAR returns 0.0 for pole_winner_speed on races whose pole hasn't
    been set yet -- same 0-as-missing sentinel pattern as the driver-ID
    fields above, but there's no dataset to join against here, just a
    float re-mapped to None, so this stays a build-time conv_dict entry
    instead of a read-time guard. Casts to float inline (rather than via
    calc()'s target_type=) so a genuine None result doesn't hit
    float(None) and log a per-row coercion warning.
    """
    return float(raw) if raw else None
```

`calc(speed_or_none, "pole_winner_speed")` in Race's `conv_dict` promotes the sentinel once, at build time — `outflow.py` then reads `race.pole_winner_speed` directly with no guard at all.

### 2e. Helpers

Two small string-composition helpers used by the outflow.  Pure functions, no state.  `_track_loc` shows the pattern; `_hometown` follows the identical shape for a driver's hometown fields (minus the null-object guard, since a resolved `Driver` instance always exists by this point):

```python
def _track_loc(track: Any) -> str:
    """Compose ``City, ST`` for a track, or ``Unknown``.

    ``track`` can be ``None`` (a Race whose Track FK didn't resolve) —
    that's a null-object guard on the join result, not a coercion gap.
    """
    if track is None:
        return "Unknown"
    city = (track.city or "").strip()
    state = (track.state or "").strip()
    if city and state:
        return f"{city}, {state}"
    return city or state or "Unknown"
```

> The full file is [`outflow.py`](outflow.py) (~380 lines) — this walkthrough excerpts the parts that matter.

### 2f. Outflow — three derived views in one function

`outflow(state)` returns `dict[ClassName, list[dict]]` and fjord builds **one dynamic Incorporator subclass per dict key** at first emit.  This is the multi-output contract: three keys in the return → three derived classes → three NDJSON files.

> ⚠️ **Do not pre-declare the output classes.**  `MonthlyRaceSchedule`, `FantasyTeam`, and `ManufacturerLeaderboard` are conspicuously absent from the source-class block at the top of the file.  Fjord builds them dynamically from the dict keys returned here and infers fields from the first emitted row.  A bare `class MonthlyRaceSchedule(Incorporator): pass` would suppress field inference and the export would land empty.

The function reads from `state["Driver"]`, `state["Race"]`, `state["LeagueRoster"]`, and the three Standings registries.  If any of the three required dependencies are missing (first wave hasn't completed yet), return `{}` to skip the emit.

```python
def outflow(state: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Compute three derived views from the fused state.  Each dict
    key becomes a derived Incorporator subclass and is written to its
    matching ``export_params`` file by fjord's multi-output contract.
    """
    drivers = state.get("Driver")
    races = state.get("Race")
    league = state.get("LeagueRoster")
    tracks = state.get("Track")
    if drivers is None or races is None or league is None:
        return {}
```

`owner_standings` (the optional eighth `CupOwnerStanding` source, degrading gracefully if absent) and `points_standings` (a `{series_id: Standing_registry}` lookup dict for `CupStanding`/`BuschStanding`/`TruckStanding`) are also pulled from `state` here — see [`outflow.py`](outflow.py) for the exact lines.

#### View 1 — `MonthlyRaceSchedule`

Current-month Cup races with resolved track + driver context.  Every field here is either a plain attribute read (Race's own static `conv_dict` already coerced the dates and defaults) or the read-time join walked in §2d above.  Zero build-time re-lookups, zero magic-number leakage.

```python
    # ════════════════════════════════════════════════════════════════
    # View 1 — MonthlyRaceSchedule
    # ════════════════════════════════════════════════════════════════
    monthly: list[dict[str, Any]] = []
    for race in races:
        # A race with no schedule date is a null-object case, not a coercion gap.
        dt = race.date_scheduled
        if dt is None or dt.month != now.month or dt.year != now.year:
            continue
        # track / pole / winner are read-time joins against live sibling
        # snapshots -- see §2d. `if race.pole_winner_driver_id else None`
        # is the 0-as-missing sentinel guard, applied before the lookup.
        track = tracks.inc_dict.get(race.track_id) if tracks else None
        pole = drivers.inc_dict.get(race.pole_winner_driver_id) if race.pole_winner_driver_id else None
        winner = drivers.inc_dict.get(race.winner_driver_id) if race.winner_driver_id else None

        monthly.append({
            "race_id":     race.inc_code,
            "date":        dt.strftime("%Y-%m-%d"),
            "race_name":   race.race_name or "TBD",
            "track":       track.inc_name if track else "Unknown",
            "track_type":  track.track_type if track else "Unknown",
            "track_miles": track.length if track else None,
            "track_loc":   _track_loc(track),
            "pole_winner": pole.Full_Name if pole else None,
            # Race's own conv_dict (Step 3) already promotes NASCAR's
            # 0.0-as-missing sentinel to None at Race's build time.
            "pole_speed":  race.pole_winner_speed,
            "winner":      winner.Full_Name if winner else None,
            "cars":        race.number_of_cars_in_field,
            "tv":          race.television_broadcaster,
            "playoff":     bool(race.playoff_round),
        })
    monthly.sort(key=lambda r: r["date"])
```

Note the navigation idioms:

* **`race.inc_code`** — every Incorporator instance exposes its primary key via `inc_code` and its display name via `inc_name`.  This is how you get back at the original ID.
* **`tracks.inc_dict.get(race.track_id)`** — the framework's O(1) primary-key lookup on the live `Track` snapshot; `race.track_id` is Race's own raw FK field, untouched by any rename.
* **`pole.Full_Name if pole else None`** — `pole` is a live `Driver` Pydantic instance whose schema came from the API; `Full_Name` is the API's actual field name and reaches us unchanged.
* **`race.pole_winner_speed` (plain attribute, no guard)** — Race's own `speed_or_none` converter already ran at build time; the field is either a real float or `None`, never the raw `0.0` sentinel.

#### View 2 — `FantasyTeam`

Per-team scoreboard.  For each team's roster pick `{series_id, driver_id}`:

1. Look up the driver in `state["Driver"].inc_dict` by `driver_id` → live `Driver` instance.
2. Look up that same driver in the matching series Standings (`state["CupStanding"]` / `BuschStanding` / `TruckStanding`) — or, for a Kyle-Busch-style owner-seat pick, in `state["CupOwnerStanding"]` instead (see the Kyle Busch section below).
3. Pull manufacturer / wins / position / points off the Standings row (all build-time coerced — plain attribute reads), and hometown / team / car number off the Driver row (also build-time coerced).

`.inc_dict.get(key)` is the framework's O(1) primary-key lookup on the registry — every `IncorporatorList` exposes it.  Same read-time-join mechanism as View 1's Track/Driver lookups above: the *target dataset* here is chosen per-row at runtime (`series_id` picks Cup/Busch/Truck; `OWNER_SCORED` membership picks Owner vs Cup), which a static `conv_dict` entry can't express — dynamic dispatch, not a static FK.

```python
    # Materialise each team's roster by series, sorted by car number.
    # roster -> Driver stays read-time: LeagueRoster.roster is a list of
    # {series_id, driver_id} dicts, not a flat FK field a static conv_dict can join.
    league_teams: dict[str, dict[int, list[Any]]] = {}
    for team in league:
        team_cd = team.team_id
        league_teams[team_cd] = {}
        for pick in team.roster or []:
            sid = int(pick.series_id)
            did = int(pick.driver_id)
            driver_obj = drivers.inc_dict.get(did)
            if driver_obj is not None and sid in (1, 2, 3):
                league_teams[team_cd].setdefault(sid, []).append(driver_obj)
        for sid in (1, 2, 3):
            if sid in league_teams[team_cd]:
                league_teams[team_cd][sid].sort(key=lambda d: int(d.Badge))
```

From here the scoring loop walks each team's `league_teams` entries series-by-series, looks up each driver's Standings row (or, for an owner-seated pick, the `CupOwnerStanding` row instead — the same `OWNER_SCORED` branch walked in the **Kyle Busch** section below), and builds one `row` dict per driver — its exact shape is the same one shown in **Output 2**'s sample JSON above.  Team totals (`total_score`, `total_wins`, `manufacturer_mix`) accumulate alongside the per-driver rows; the finished team list sorts by descending `total_score`.

> The full file is [`outflow.py`](outflow.py) (~380 lines) — this walkthrough excerpts the parts that matter.

Every field read off `stnd` or `driver` in the full scoring loop is a plain attribute — no defensive default fallback — because `nascar_fantasy.py`'s `conv_dict`s guarantee they're always present.  Only `stnd` itself (does this driver have a standings row at all?) and `owner_seat is None` (is this an owner-seated pick, whose source class lacks certain fields?) remain as guards in `outflow.py` — both null-object / cross-source-shape checks, not coercion gaps.

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
            "top_driver":    top.inc_name,
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

`nascar_fantasy.py` is the runner.  It declares all eight co-equal sources (seven API + one local file), points `outflow=` at `outflow.py`, and configures the three export targets.  All eight seed in parallel — nothing declares `depends_on`, since no source needs a peer's registry to build itself.  `refresh_params=None` on every source = single-wave test mode; with no `export_interval` set, the pipeline exits cleanly after one outflow wave.

> The full file is [`nascar_fantasy.py`](nascar_fantasy.py) — this walkthrough excerpts the parts that matter.  The module's own docstring (enumerating the advanced fjord capabilities this tutorial demonstrates) is worth reading directly in the file rather than duplicated here.

Three exclusion lists (`_OWNER_EXCL`, `_STANDINGS_EXCL`, `_DRIVER_EXCL`) drop noisy API fields before `conv_dict` runs — see the file for the full lists.

Static reference data that never refreshes:

```python
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
```

`Driver` carries the `calc(mfg_from_logo_url, ...)` wiring already walked in Step 2c above:

```python
{
    "cls": Driver,
    "incorp_params": {
        "inc_url": f"{CFC_BASE}/drivers.json",
        "rec_path": "response",
        "inc_code": "Nascar_Driver_ID",
        "inc_name": "Full_Name",
        "excl_lst": _DRIVER_EXCL,
        "conv_dict": {
            "Manufacturer": calc(mfg_from_logo_url, "Manufacturer", default="Unknown", target_type=str),
            "Hometown_City": inc(str, default=""),
            "Hometown_State": inc(str, default=""),
            "Team": inc(str, default=""),
            "Badge": inc(str, default="0"),
        },
    },
    "refresh_params": None,
},
```

`Race` is a co-equal source too — its static `conv_dict` carries the six coercions walked in §2d above (`_DATE_FIELDS` lives right here, next to the entry that uses it), and its three raw FKs pass through untouched for `outflow.py` to resolve read-time:

```python
{
    "cls": Race,
    "incorp_params": {
        "inc_url": f"{CFC_BASE}/{CURRENT_YEAR}/race_list_basic.json",
        "rec_path": "series_1",
        "inc_code": "race_id",
        "inc_name": "race_name",
        "excl_lst": ["schedule", "track_name"],
        "conv_dict": {
            **{key: inc(datetime) for key in _DATE_FIELDS},
            "number_of_cars_in_field": inc(int, default=0),
            "television_broadcaster": inc(str, default="TBD"),
            "playoff_round": inc(int, default=0),
            # 0.0-as-missing sentinel: NASCAR reports 0.0 for a
            # pole speed that hasn't been set yet.
            "pole_winner_speed": calc(speed_or_none, "pole_winner_speed"),
        },
    },
    "refresh_params": None,
},
```

One `stream_params` entry per Standings series.  `BuschStanding` and `TruckStanding` follow the identical shape, swapping only the series number in the URL and the class name:

```python
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
```

`CupOwnerStanding` — the eighth source, uniquely shaped and keyed by `vehicle_number` (see the Kyle Busch section below):

```python
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
```

`LeagueRoster` — the file-source demo (API + file source mixing):

```python
{
    "cls": LeagueRoster,
    "incorp_params": {
        "inc_file": str(HERE / "fixtures/league_teams.json"),
        "inc_code": "team_id",
        "inc_name": "team_id",
    },
    "refresh_params": None,
},
```

The `fjord()` call's `outflow=` / `export_params=` wiring:

```python
outflow=str(HERE / "outflow.py"),
export_params={
    "MonthlyRaceSchedule": {"file_path": str(DATA / "nascar_monthly_schedule.ndjson")},
    "FantasyTeam": {"file_path": str(DATA / "nascar_fantasy_scoreboard.ndjson")},
    "ManufacturerLeaderboard": {"file_path": str(DATA / "nascar_manufacturer_leaderboard.ndjson")},
},
```

Every source above ships `refresh_params=None` for single-wave test mode; the file's commented-out block shows what a production daemon would uncomment instead:

```python
# refresh_interval={
#     "Driver":        3600,   # 1 h
#     "Race":          600,    # 10 min (pole finalises Sat)
#     "CupStanding":   300,    # 5 min on race day
#     "BuschStanding": 300,
#     "TruckStanding": 300,
# },
# export_interval=60,
```

Each wave prints its own outcome:

```python
op = wave.operation
if wave.failed_sources:
    print(f"WARN  {op:35s} chunk {wave.chunk_index}: {wave.failed_sources}")
else:
    print(f"OK    {op:35s} chunk {wave.chunk_index}: {wave.rows_processed} rows")
```

Notable wiring:

* **One sidecar.**  `outflow=` points at `outflow.py` — source classes, `outflow(state)` (including Race's read-time FK joins), `OWNER_SCORED`, and the two shared `conv_dict` helpers.  No `inflow=` needed: every source coerces its own fields, and the only cross-source work (the joins) belongs to `outflow(state)` anyway.
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
Initiating NASCAR Data Gateway (fjord)...

OK    fjord_incorp:Track                  chunk 1: 49 rows
OK    fjord_incorp:Driver                 chunk 1: 918 rows
OK    fjord_incorp:Race                   chunk 1: 40 rows
OK    fjord_incorp:CupStanding            chunk 1: 40 rows
OK    fjord_incorp:BuschStanding          chunk 1: 64 rows
OK    fjord_incorp:TruckStanding          chunk 1: 76 rows
OK    fjord_incorp:CupOwnerStanding       chunk 1: 47 rows
OK    fjord_incorp:LeagueRoster           chunk 1: 8 rows
OK    outflow:MonthlyRaceSchedule         chunk 1: 4 rows
OK    outflow:FantasyTeam                 chunk 1: 8 rows
OK    outflow:ManufacturerLeaderboard     chunk 1: 3 rows

Pipeline complete.
   - examples/09-nascar-fantasy-fjord/out/nascar_monthly_schedule.ndjson
   - examples/09-nascar-fantasy-fjord/out/nascar_fantasy_scoreboard.ndjson
   - examples/09-nascar-fantasy-fjord/out/nascar_manufacturer_leaderboard.ndjson
```

All eight source waves fire concurrently via `asyncio.gather` — `Race`'s position in this list isn't ordering, just the order `asyncio.gather` happened to resolve its coroutines this run.  No source depends on another to seed itself; `Race`'s three FK joins resolve later, read-time, inside `outflow(state)`.

### Output 1 — `out/nascar_monthly_schedule.ndjson`

Per-race row with resolved track + driver context.  Past races have real `pole_winner`, `pole_speed`, and `winner`; future races have all three as `null` (the read-time 0-as-missing sentinel guard short-circuits the join to `None` before the lookup runs):

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

## 🧠 What This Demonstrates

| Pattern | Where to look |
|---|---|
| **Parallel seed** | All eight sources seed concurrently via `asyncio.gather` — none declares `depends_on`, since none needs a peer's registry to build itself |
| **API + file source mixing** | `LeagueRoster` uses `inc_file=`; the other seven use `inc_url=`.  Same handler dispatch routes both transparently |
| **Read-time cross-source joins** | `state["Track"].inc_dict.get(race.track_id)` / `state["Driver"].inc_dict.get(...)` inside `outflow(state)` — the same mechanism already used for the roster→Driver fan-out and the per-pick Standings dispatch, applied uniformly to Race's three FKs |
| **Sentinel-ID guard, applied at the join** | `if race.pole_winner_driver_id else None` short-circuits ID 0 to `None` before the read-time lookup runs — applied to BOTH `pole_winner_driver_id` and `winner_driver_id` |
| **`stream()` vs `fjord()` vs `refresh()`** | `stream()` is paginated bulk-export chunking; `refresh()` is manual one-shot; `fjord()` (this tutorial) is the stateful multi-source daemon |
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

## Run it

```bash
# Python entry — resolves out/ absolutely (HERE / "out"), runs from anywhere
python examples/09-nascar-fantasy-fjord/nascar_fantasy.py

# Same fjord, from the CLI — cd first: pipeline.json's export_params.file_path
# entries are CWD-relative, not config-relative, so running from repo root
# would silently write out/ next to your shell's cwd instead of next to the config
cd examples/09-nascar-fantasy-fjord
incorporator validate pipeline.json && incorporator fjord pipeline.json --logs
```

Also runs in Docker via the [central mount pattern](../README.md#running-a-tutorial-in-docker) (not run or verified). Verified live: both forms fuse the same live NASCAR feed into identical row counts across all eleven waves and identical manufacturer-leaderboard totals (counts drift week to week — live standings, see [`pipeline.json`](pipeline.json)).

---

## Where to Go Next

> 👉 **Up next: [Tutorial 10 — Multi-Source Fjord](../10-multi-source-fjord/README.md).**  T9 walked the *full* fjord shape — eight sources, read-time joins, three outputs.  T10 introduces `fjord()` formally on its minimum viable form (two co-equal sources, one outflow) on the crypto-spread pattern.

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
