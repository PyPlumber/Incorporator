"""Outflow sidecar for the NASCAR fantasy-league fjord pipeline.

Defines six source classes (Track, Driver, Race, CupStanding,
BuschStanding, TruckStanding), the ``inflow(state)`` callable that
wires Race's foreign-key fields against the already-loaded Track +
Driver registries, and the ``outflow(state)`` function that emits
TWO derived classes from one fused state:

* ``MonthlyRaceSchedule`` — current-month Cup races with resolved
  pole-winner and track names.
* ``FantasyTeam`` — the league scoreboard (team rosters + per-series
  points + grand total).

Each derived class gets its own export file via fjord's multi-output
contract.  No daemon plumbing, no lock acquisition, no per-class
fanout — fjord handles all of it.
"""

from datetime import datetime
from typing import Any, Dict, List, Tuple

from incorporator import Incorporator, calc, inc, link_to


# ── Source classes ─────────────────────────────────────────────────
# Each fjord source needs its own subclass so the three Standings
# don't share ``inc_dict``.  Track / Driver / Race / three standings
# = six independent registries seeded concurrently (Track + Driver +
# all three standings load in parallel; Race waits on Track + Driver
# via inflow's declared dependency).


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


# ── Constants ──────────────────────────────────────────────────────

_DATE_FIELDS = ("date_scheduled", "race_date", "qualifying_date", "tunein_date")

_SERIES_LIST = ("Cup", "Busch", "Truck")

# Hardcoded fantasy rosters — keyed by team handle, each entry is
# (series_id, nascar_driver_id) pairs.  Series 1 = Cup, 2 = Busch,
# 3 = Truck.  Lives here (not in the driver script) so the outflow
# function can read it directly during the join.
LEAGUE_TEAMS_RAW: Dict[str, List[Tuple[float, int]]] = {
    "Queen":     [(3.0, 4235), (2.0, 4441), (1.0, 3989), (1.0, 4062), (1.0, 4123), (1.0, 4272), (1.0, 3859), (1.0, 4481)],
    "Intim'tor": [(3.0, 4312), (2.0, 34),   (1.0, 4030), (1.0, 4023), (1.0, 3989), (1.0, 4153), (1.0, 4065), (1.0, 4481)],
    "WonderBoy": [(3.0, 4235), (2.0, 4133), (1.0, 4153), (1.0, 4030), (1.0, 1816), (1.0, 4065), (1.0, 3859), (1.0, 4481)],
    "AlabamaG":  [(3.0, 4446), (2.0, 34),   (1.0, 4030), (1.0, 454),  (1.0, 4023), (1.0, 4153), (1.0, 4065), (1.0, 4481)],
    "Jaws":      [(3.0, 4446), (2.0, 34),   (1.0, 4065), (1.0, 4030), (1.0, 4153), (1.0, 3859), (1.0, 4001), (1.0, 4481)],
    "Seven":     [(3.0, 4235), (2.0, 4133), (1.0, 1816), (1.0, 454),  (1.0, 4062), (1.0, 1361), (1.0, 3859), (1.0, 4481)],
    "Cale":      [(3.0, 4427), (2.0, 4133), (1.0, 4023), (1.0, 4001), (1.0, 4153), (1.0, 4030), (1.0, 4065), (1.0, 4481)],
    "Confused":  [(3.0, 4235), (2.0, 34),   (1.0, 4023), (1.0, 3989), (1.0, 4062), (1.0, 4153), (1.0, 4469), (1.0, 4481)],
}


# ── State-aware inflow — wires Race.conv_dict against live peers ────


def _pole_id_or_none(raw: Any) -> Any:
    """NASCAR returns ``pole_winner_driver_id = 0`` for races whose pole
    qualifying hasn't happened yet (or was rained out).  Driver ID 0
    coincidentally resolves to a real driver in the registry, so
    without this filter every future race shows the same name.  Mapping
    0 → None lets ``link_to`` short-circuit and downstream consumers
    see ``race.pole_winner_driver_id is None``.
    """
    return raw if raw else None


def inflow(state: Dict[str, Any]) -> Dict[str, Any]:
    """Build per-source ``conv_dict`` overrides from sibling registries.

    Inflow is called before each source's ``incorp()``.  On the early
    calls (Track / Driver / Standings) ``state`` is empty or partial,
    so we only emit Race's override once its peers exist — fjord then
    re-applies it on every refresh tick so Race's ``track_id`` and
    ``pole_winner_driver_id`` resolve to live ``Track`` / ``Driver``
    instances rather than raw integers.
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


# ── Outflow — multi-output: schedule + league scoreboard ───────────


def outflow(state: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Compute two derived views from the fused state and return them
    keyed by the desired derived-class name.  Fjord builds one dynamic
    Incorporator subclass per key and writes each to its own file via
    the multi-output ``export_params`` contract.
    """
    drivers = state.get("Driver")
    races = state.get("Race")
    if drivers is None or races is None:
        return {}

    points_standings = {
        1: state.get("CupStanding"),
        2: state.get("BuschStanding"),
        3: state.get("TruckStanding"),
    }

    # ─── View 1: current-month Cup race schedule ─────────────────
    now = datetime.now()
    monthly: List[Dict[str, Any]] = []
    for race in races:
        dt = getattr(race, "date_scheduled", None)
        if dt is None or dt.month != now.month or dt.year != now.year:
            continue
        pole = getattr(race, "pole_winner_driver_id", None)
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

    # ─── View 2: fantasy league scoreboard ───────────────────────
    # Resolve every (series_id, driver_id) the league cares about
    # into a points lookup.
    league_roster = {player for roster in LEAGUE_TEAMS_RAW.values() for player in roster}
    league_scores: Dict[Tuple[float, int], int] = {}
    for series_id_float, driver_id in league_roster:
        series_cls = points_standings.get(int(series_id_float))
        standing = series_cls.inc_dict.get(driver_id) if series_cls else None
        league_scores[(series_id_float, driver_id)] = (
            getattr(standing, "points", 0) if standing else 0
        )

    # Materialise each team's roster by series, sorted by car number.
    league_teams: Dict[str, Dict[int, List[Any]]] = {}
    for team_cd, roster in LEAGUE_TEAMS_RAW.items():
        league_teams[team_cd] = {}
        for series_id_float, driver_id in roster:
            series_id = int(series_id_float)
            driver_obj = drivers.inc_dict.get(driver_id)
            if driver_obj is not None:
                league_teams[team_cd].setdefault(series_id, []).append(driver_obj)
        for series_id in range(1, 4):
            if series_id in league_teams[team_cd]:
                league_teams[team_cd][series_id].sort(
                    key=lambda d: int(getattr(d, "Badge", 0) or 0)
                )

    fantasy: List[Dict[str, Any]] = []
    for team_cd, roster in league_teams.items():
        team_obj: Dict[str, Any] = {"team_id": team_cd, "roster": [], "points": [], "total_score": 0}
        team_score = 0
        per_series: Dict[int, int] = {}
        for series_id, series_name in enumerate(_SERIES_LIST, start=1):
            per_series[series_id] = 0
            if series_id in roster:
                for car_idx, driver in enumerate(roster[series_id], start=1):
                    pts = league_scores[(float(series_id), driver.inc_code)]
                    per_series[series_id] += pts
                    series_cls = points_standings.get(series_id)
                    stnd = series_cls.inc_dict.get(driver.inc_code) if series_cls else None
                    team_obj["roster"].append({
                        "series":  series_name,
                        "car_idx": car_idx,
                        "name":    getattr(driver, "inc_name", "Unknown").strip(),
                        "car":     getattr(driver, "Badge", "N/A"),
                        "team":    getattr(driver, "Team", "Unknown").strip(),
                        "wins":    getattr(stnd, "wins", 0) if stnd else 0,
                        "t10":     getattr(stnd, "top_10", 0) if stnd else 0,
                        "points":  pts,
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
        fantasy.append(team_obj)

    fantasy.sort(key=lambda t: -t["total_score"])

    return {
        "MonthlyRaceSchedule": monthly,
        "FantasyTeam":         fantasy,
    }
