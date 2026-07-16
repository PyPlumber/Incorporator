"""Outflow sidecar for the NASCAR fantasy-league fjord pipeline.

Defines EIGHT source classes (Track, Driver, Race, three Standings,
CupOwnerStanding, plus LeagueRoster from a local JSON file) and the
``outflow(state)`` function that emits THREE derived classes from one
fused state:

* ``MonthlyRaceSchedule`` — current-month Cup races with resolved
  track, pole-winner, race-winner, and watchability metadata.
* ``FantasyTeam`` — the league scoreboard with enriched per-driver
  rows (manufacturer, hometown, season rank, top-5s, laps-led, gap
  to leader) and a per-team manufacturer-mix summary.
* ``ManufacturerLeaderboard`` — Chevrolet / Ford / Toyota leaderboard
  with driver counts, total points, total wins, playoff seats, and
  the top driver per make.

Each derived class gets its own export file via fjord's multi-output
contract.

Incoming-data manipulation (``_DATE_FIELDS``, ``_driver_id_or_none``,
``mfg_from_logo_url``, and the ``inflow(state)`` seed hook) lives in
the sibling ``inflow.py``.

Kyle Busch died mid-season; ``OWNER_SCORED`` routes his roster pick to
the RCR #133 owner-entry feed instead of driver points — see
``CupOwnerStanding`` below.
"""

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from incorporator import Incorporator

# ── Source classes ─────────────────────────────────────────────────
# Each fjord source needs its own subclass so the Standings classes
# don't share ``inc_dict``.  LeagueRoster is the only one fed by a
# local JSON file, demonstrating that fjord mixes API + filesystem
# sources without any special casing.


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


# ── Constants ──────────────────────────────────────────────────────

_SERIES_LIST = ("Cup", "Busch", "Truck")


# ── Helpers ────────────────────────────────────────────────────────


def _hometown(driver: Any) -> str:
    """Compose ``City, ST`` from the driver's hometown fields, falling
    back to ``Unknown``. Fields are already coerced to ``str`` at
    Driver's build time, so this is composition, not a null guard.
    """
    city = driver.Hometown_City.strip()
    state = driver.Hometown_State.strip()
    if city and state:
        return f"{city}, {state}"
    return city or state or "Unknown"


def _track_loc(track: Any) -> str:
    """Compose ``City, ST`` for a track, or ``Unknown``.

    ``track`` can be ``None`` (a Race whose Track FK didn't resolve) —
    that's a null-object guard on the join result, not a coercion gap.
    """
    if track is None:
        return "Unknown"
    city = (getattr(track, "city", "") or "").strip()
    state = (getattr(track, "state", "") or "").strip()
    if city and state:
        return f"{city}, {state}"
    return city or state or "Unknown"


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

    # CupOwnerStanding is an optional eighth source — if it fails to load
    # the outflow degrades gracefully (owner-scored picks score 0 pts)
    # rather than aborting the entire run.
    owner_standings = state.get("CupOwnerStanding")

    points_standings = {
        1: state.get("CupStanding"),
        2: state.get("BuschStanding"),
        3: state.get("TruckStanding"),
    }

    now = datetime.now()

    # ════════════════════════════════════════════════════════════════
    # View 1 — MonthlyRaceSchedule
    # ════════════════════════════════════════════════════════════════
    monthly: list[dict[str, Any]] = []
    for race in races:
        # A race with no schedule date is a null-object case, not a coercion gap.
        dt = getattr(race, "date_scheduled", None)
        if dt is None or dt.month != now.month or dt.year != now.year:
            continue
        # pole / winner / track can each be None if the FK didn't resolve --
        # a null-object guard on the join result, so `if track else` etc. stay.
        pole = race.pole_winner_driver_id
        winner = race.winner_driver_id
        track = getattr(race, "track", None) or getattr(race, "track_id", None)

        monthly.append(
            {
                "race_id": race.inc_code,
                "date": dt.strftime("%Y-%m-%d"),
                "race_name": getattr(race, "race_name", "TBD"),
                "track": getattr(track, "inc_name", "Unknown") if track else "Unknown",
                "track_type": track.track_type if track else "Unknown",
                "track_miles": track.length if track else None,
                "track_loc": _track_loc(track),
                "pole_winner": getattr(pole, "Full_Name", None) if pole else None,
                # inflow.py's _speed_or_none already promotes NASCAR's
                # 0.0-as-missing sentinel to None at build time.
                "pole_speed": race.pole_winner_speed,
                "winner": getattr(winner, "Full_Name", None) if winner else None,
                "cars": race.number_of_cars_in_field,
                "tv": race.television_broadcaster,
                "playoff": bool(race.playoff_round),
            }
        )
    monthly.sort(key=lambda r: r["date"])

    # ════════════════════════════════════════════════════════════════
    # View 2 — FantasyTeam
    # ════════════════════════════════════════════════════════════════
    # Materialise each team's roster by series, sorted by car number.
    # roster -> Driver stays read-time: LeagueRoster.roster is a list of
    # {series_id, driver_id} dicts, not a flat FK field link_to() can join.
    league_teams: dict[str, dict[int, list[Any]]] = {}
    for team in league:
        team_cd = team.team_id
        league_teams[team_cd] = {}
        for pick in team.roster or []:
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
            "team_id": team_cd,
            "roster": [],
            "points": [],
            "manufacturer_mix": {},
            "total_wins": 0,
            "total_score": 0,
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
                # Which dataset to join against is chosen per-row at runtime
                # (series + OWNER_SCORED membership) -- link_to() can't branch
                # between three datasets like this, so it stays read-time.
                if did in OWNER_SCORED and series_id == 1:
                    owner_vnum = OWNER_SCORED[did]  # '133' — must be string key
                    stnd = owner_standings.inc_dict.get(owner_vnum) if owner_standings else None
                    owner_seat: str | None = owner_vnum
                else:
                    stnd = series_cls.inc_dict.get(driver.inc_code) if series_cls else None
                    owner_seat = None

                # stnd is a null-object guard (a driver with no standings row);
                # fields read off it are plain attrs, already build-time coerced.
                pts = stnd.points if stnd else 0
                wins = stnd.wins if stnd else 0
                per_series[series_id] += pts
                total_wins += wins

                # Prefer the season-current standings copy of manufacturer,
                # falling back to the driver record (owner standings don't
                # carry it, so owner-seated picks always fall back).
                mfg = (stnd.manufacturer if stnd and owner_seat is None else "") or driver.Manufacturer or "Unknown"
                mfg = mfg.strip() or "Unknown"
                mfg_counter[mfg] += 1

                driver_name = getattr(driver, "inc_name", "Unknown").strip()
                row: dict[str, Any] = {
                    "series": series_name,
                    "car_idx": car_idx,
                    "name": f"{driver_name} [owner seat: RCR #{owner_seat}]" if owner_seat else driver_name,
                    "car": driver.Badge,
                    "team": driver.Team.strip() or "Unknown",
                    "manufacturer": mfg,
                    "hometown": _hometown(driver),
                    "rank": stnd.position if stnd else None,
                    "wins": wins,
                    "t10": stnd.top_10 if stnd else 0,
                    "top_5": stnd.top_5 if stnd else 0,
                    # laps_led is not tracked in owner standings; emit 0.
                    "laps_led": stnd.laps_led if stnd and owner_seat is None else 0,
                    "points": pts,
                    # delta_leader is signed (negative = behind); abs() reads
                    # as "points behind leader". Owner standings don't carry it.
                    "points_back": abs(stnd.delta_leader) if stnd and owner_seat is None else None,
                }
                if owner_seat is not None:
                    row["owner_seat"] = owner_seat
                team_obj["roster"].append(row)
            team_score += per_series[series_id]

        for series_id, series_name in enumerate(_SERIES_LIST, start=1):
            pts = per_series[series_id]
            team_obj["points"].append(
                {
                    "series": series_name,
                    "points": pts,
                    "percentage": round(pts / team_score, 4) if team_score else 0,
                }
            )
        team_obj["points"].append({"series": "GRAND TOTAL", "points": team_score, "percentage": 1.0})
        team_obj["total_score"] = team_score
        team_obj["total_wins"] = total_wins
        team_obj["manufacturer_mix"] = dict(mfg_counter.most_common())
        fantasy.append(team_obj)

    fantasy.sort(key=lambda t: -t["total_score"])

    # ════════════════════════════════════════════════════════════════
    # View 3 — ManufacturerLeaderboard
    # ════════════════════════════════════════════════════════════════
    # Group the Cup standings by manufacturer.  ``CupStanding`` is the
    # canonical season-points feed; ``BuschStanding`` and
    # ``TruckStanding`` could be added by changing the source — left
    # as Cup-only here because the Cup series is what fantasy plays
    # care about most.
    cup = points_standings[1]
    mfg_buckets: dict[str, list[Any]] = defaultdict(list)
    if cup is not None:
        for stnd in cup:
            mfg = stnd.manufacturer.strip() or "Unknown"
            mfg_buckets[mfg].append(stnd)

    manufacturer_rows: list[dict[str, Any]] = []
    for mfg, rows in mfg_buckets.items():
        if mfg == "Unknown":
            continue  # skip the catch-all bucket
        top = max(rows, key=lambda s: s.points)
        manufacturer_rows.append(
            {
                "manufacturer": mfg,
                "drivers": len(rows),
                "total_points": sum(s.points for s in rows),
                "total_wins": sum(s.wins for s in rows),
                "playoff_seats": sum(1 for s in rows if s.playoff_eligible),
                "top_driver": getattr(top, "inc_name", "Unknown"),
                "top_points": top.points,
            }
        )
    manufacturer_rows.sort(key=lambda r: -r["total_points"])

    return {
        "MonthlyRaceSchedule": monthly,
        "FantasyTeam": fantasy,
        "ManufacturerLeaderboard": manufacturer_rows,
    }
