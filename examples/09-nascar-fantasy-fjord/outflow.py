"""Outflow sidecar for the NASCAR fantasy-league fjord pipeline.

Defines SEVEN source classes (Track, Driver, Race, three Standings,
plus LeagueRoster from a local JSON file), the ``inflow(state)``
callable that wires Race's foreign-key fields against the
already-loaded Track + Driver registries, and the ``outflow(state)``
function that emits THREE derived classes from one fused state:

* ``MonthlyRaceSchedule`` — current-month Cup races with resolved
  track, pole-winner, race-winner, and watchability metadata.
* ``FantasyTeam`` — the league scoreboard with enriched per-driver
  rows (manufacturer, hometown, season rank, top-5s, laps-led, gap
  to leader) and a per-team manufacturer-mix summary.
* ``ManufacturerLeaderboard`` — Chevrolet / Ford / Toyota leaderboard
  with driver counts, total points, total wins, playoff seats, and
  the top driver per make.

Each derived class gets its own export file via fjord's multi-output
contract.  No daemon plumbing, no lock acquisition, no per-class
fanout — fjord handles all of it.
"""

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from incorporator import Incorporator, inc, link_to


# ── Source classes ─────────────────────────────────────────────────
# Each fjord source needs its own subclass so the three Standings
# don't share ``inc_dict``.  LeagueRoster is the seventh source —
# the only one fed by a local JSON file, demonstrating that fjord
# mixes API + filesystem sources without any special casing.


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


# ── Constants ──────────────────────────────────────────────────────

_DATE_FIELDS = ("date_scheduled", "race_date", "qualifying_date", "tunein_date")

_SERIES_LIST = ("Cup", "Busch", "Truck")


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


# ── State-aware inflow — wires Race.conv_dict against live peers ────


def inflow(state: dict[str, Any]) -> dict[str, Any]:
    """Build per-source ``conv_dict`` overrides from sibling registries.

    Inflow is called before each source's ``incorp()``.  On the early
    calls (Track / Driver / Standings / LeagueRoster) ``state`` is
    empty or partial, so we only emit Race's override once its peers
    exist — fjord then re-applies it on every refresh wave so Race's
    ``track_id``, ``pole_winner_driver_id``, and ``winner_driver_id``
    resolve to live ``Track`` / ``Driver`` instances rather than raw
    integers.
    """
    overrides: dict[str, Any] = {}
    if "Track" in state and "Driver" in state:
        overrides["Race"] = {
            "conv_dict": {
                "track_id": link_to(state["Track"]),
                "pole_winner_driver_id": link_to(state["Driver"], extractor=_driver_id_or_none),
                "winner_driver_id": link_to(state["Driver"], extractor=_driver_id_or_none),
                **{key: inc(datetime) for key in _DATE_FIELDS},
            }
        }
    return overrides


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
        dt = getattr(race, "date_scheduled", None)
        if dt is None or dt.month != now.month or dt.year != now.year:
            continue
        pole = getattr(race, "pole_winner_driver_id", None)
        winner = getattr(race, "winner_driver_id", None)
        track = getattr(race, "track", None) or getattr(race, "track_id", None)
        playoff_round = getattr(race, "playoff_round", 0) or 0

        monthly.append(
            {
                "race_id": race.inc_code,
                "date": dt.strftime("%Y-%m-%d"),
                "race_name": getattr(race, "race_name", "TBD"),
                "track": getattr(track, "inc_name", "Unknown") if track else "Unknown",
                "track_type": getattr(track, "track_type", "Unknown") if track else "Unknown",
                "track_miles": getattr(track, "length", None) if track else None,
                "track_loc": _track_loc(track),
                "pole_winner": getattr(pole, "Full_Name", None) if pole else None,
                # NASCAR returns 0.0 for races whose pole hasn't been set
                # (same sentinel pattern as pole_winner_driver_id) — promote
                # to None so consumers can show "TBD" without checking magic
                # numbers.
                "pole_speed": (getattr(race, "pole_winner_speed", None) or None) if pole else None,
                "winner": getattr(winner, "Full_Name", None) if winner else None,
                "cars": getattr(race, "number_of_cars_in_field", 0),
                "tv": getattr(race, "television_broadcaster", "TBD") or "TBD",
                "playoff": bool(playoff_round),
            }
        )
    monthly.sort(key=lambda r: r["date"])

    # ════════════════════════════════════════════════════════════════
    # View 2 — FantasyTeam
    # ════════════════════════════════════════════════════════════════
    # Materialise each team's roster by series, sorted by car number.
    # Read from state["LeagueRoster"] instead of a hardcoded constant.
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
                league_teams[team_cd][sid].sort(key=lambda d: int(getattr(d, "Badge", 0) or 0))

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
                stnd = series_cls.inc_dict.get(driver.inc_code) if series_cls else None
                pts = getattr(stnd, "points", 0) if stnd else 0
                wins = getattr(stnd, "wins", 0) if stnd else 0
                per_series[series_id] += pts
                total_wins += wins

                # Manufacturer can live on either the driver record or
                # the standings row.  Prefer the standings copy (it's
                # season-current) and fall back to the driver record.
                mfg = (
                    (getattr(stnd, "manufacturer", "") if stnd else "")
                    or getattr(driver, "Manufacturer", "")
                    or "Unknown"
                )
                mfg = mfg.strip() or "Unknown"
                mfg_counter[mfg] += 1

                team_obj["roster"].append(
                    {
                        "series": series_name,
                        "car_idx": car_idx,
                        "name": getattr(driver, "inc_name", "Unknown").strip(),
                        "car": getattr(driver, "Badge", "N/A"),
                        "team": (getattr(driver, "Team", "") or "Unknown").strip(),
                        "manufacturer": mfg,
                        "hometown": _hometown(driver),
                        "rank": getattr(stnd, "position", None) if stnd else None,
                        "wins": wins,
                        "t10": getattr(stnd, "top_10", 0) if stnd else 0,
                        "top_5": getattr(stnd, "top_5", 0) if stnd else 0,
                        "laps_led": getattr(stnd, "laps_led", 0) if stnd else 0,
                        "points": pts,
                        # ``delta_leader`` is signed in the raw feed (negative
                        # for drivers behind the leader, 0 for the leader).
                        # Fantasy UX wants "points behind leader" as a
                        # positive number — abs() makes the column intuitive
                        # without altering the underlying truth.
                        "points_back": abs(getattr(stnd, "delta_leader", 0) or 0) if stnd else None,
                    }
                )
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
            mfg = (getattr(stnd, "manufacturer", "") or "").strip() or "Unknown"
            mfg_buckets[mfg].append(stnd)

    manufacturer_rows: list[dict[str, Any]] = []
    for mfg, rows in mfg_buckets.items():
        if mfg == "Unknown":
            continue  # skip the catch-all bucket
        top = max(rows, key=lambda s: getattr(s, "points", 0))
        manufacturer_rows.append(
            {
                "manufacturer": mfg,
                "drivers": len(rows),
                "total_points": sum(getattr(s, "points", 0) for s in rows),
                "total_wins": sum(getattr(s, "wins", 0) for s in rows),
                "playoff_seats": sum(1 for s in rows if getattr(s, "playoff_eligible", 0)),
                "top_driver": getattr(top, "inc_name", "Unknown"),
                "top_points": getattr(top, "points", 0),
            }
        )
    manufacturer_rows.sort(key=lambda r: -r["total_points"])

    return {
        "MonthlyRaceSchedule": monthly,
        "FantasyTeam": fantasy,
        "ManufacturerLeaderboard": manufacturer_rows,
    }
