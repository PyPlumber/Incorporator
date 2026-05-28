"""Outflow logic and class definitions for the MLB AL East Pulse Tideweaver diamond.

Defines the six ``Incorporator`` subclasses and two ``CustomCurrent`` subclasses
referenced from ``watershed.json`` and ``mlb_pulse.py``, plus named module-level
helpers and the ``outflow(state)`` function the tail Fjord calls each tick.

Imported by both the Python entry (``mlb_pulse.py``) and the CLI form
(``incorporator tideweaver run watershed.json``), so host-throttle registration
lives here as well as in the entry script.  Both import paths must register the
penstock independently.

Run from repo root:

    incorporator validate examples/appendix/mlb-pulse/watershed.json
    incorporator tideweaver run examples/appendix/mlb-pulse/watershed.json
"""

from __future__ import annotations

import operator
from typing import Any

from incorporator import Incorporator, SustainedPenstock, register_host_penstock
from incorporator.observability.tideweaver import CustomCurrent
from incorporator.schema.converters import calc, inc

# ---------------------------------------------------------------------------
# Host throttle — 1 req/sec = 60 req/min, well under any undocumented MLB cap.
# Registered at module-top so both the Python entry and the CLI form impose
# the same constraint regardless of import order.
# ---------------------------------------------------------------------------

register_host_penstock("statsapi.mlb.com", SustainedPenstock(rate_per_sec=1.0))

# ---------------------------------------------------------------------------
# AL East division ID (MLB Stats API constant, does not change season-to-season)
# ---------------------------------------------------------------------------

_AL_EAST_DIVISION_ID = 201

# ---------------------------------------------------------------------------
# Incorporator subclasses — one per stream node + two derived output classes
# ---------------------------------------------------------------------------


class MLBSchedule(Incorporator):
    """Today's game schedule from /api/v1/schedule — rec_path 'dates.0.games'."""


class MLBAllTeam(Incorporator):
    """All MLB teams from /api/v1/teams — rec_path 'teams'."""


class MLBStandings(Incorporator):
    """AL East standings record from /api/v1/standings — rec_path 'records'.

    Live probe confirmed ``inc_code='division.id'`` produces ``inc_code=201``
    for the AL East record.  The ``teamRecords`` list is accessed directly from
    the raw instance attribute in ``outflow()``; no conv_dict entry needed.
    """


class MLBHitting(Incorporator):
    """Per-team season hitting stats — populated by HittingDrillCurrent T5 drills."""


class MLBPitching(Incorporator):
    """Per-team season pitching stats — populated by PitchingDrillCurrent T5 drills."""


class TeamPulseCard(Incorporator):
    """Derived AL East Pulse Card — one row per team, produced by outflow(state)."""


# ---------------------------------------------------------------------------
# Module-level conv_dict constants — passed as incorp_params kwargs, not class
# attributes (Pydantic V2 rejects unannotated class attrs on Incorporator subs).
# ---------------------------------------------------------------------------

# schedule conv_dict — passed as incorp_params["conv_dict"] in Stream/incorp calls.
# abbr_lower note: ordering matters for MLBAllTeam — see SCHEDULE_CONV comment.
SCHEDULE_CONV: dict[str, Any] = {
    "home_team_id": calc(int, "teams.home.team.id", default=0, target_type=int),
    "away_team_id": calc(int, "teams.away.team.id", default=0, target_type=int),
    "home_team_name": calc(str, "teams.home.team.name", default="", target_type=str),
    "away_team_name": calc(str, "teams.away.team.name", default="", target_type=str),
    "game_date": calc(str, "gameDate", default="", target_type=str),
    "game_status": calc(str, "status.detailedState", default="", target_type=str),
}

# all_teams conv_dict — abbr_lower MUST come after abbreviation so inc(str) has
# already coerced the field before str.lower reads it (conv_dict insertion order).
ALL_TEAMS_CONV: dict[str, Any] = {
    "name": inc(str, default=""),
    "abbreviation": inc(str, default=""),
    "abbr_lower": calc(str.lower, "abbreviation", default="", target_type=str),
    "division_id": calc(int, "division.id", default=0, target_type=int),
    "league_id": calc(int, "league.id", default=0, target_type=int),
    "team_name": calc(str, "teamName", default="", target_type=str),
    "short_name": calc(str, "shortName", default="", target_type=str),
}

STANDINGS_CONV: dict[str, Any] = {
    "last_updated": inc(str, default=""),
}


# ---------------------------------------------------------------------------
# Named module-level helpers (lambda-free, per AGENTS.md H3 idiom)
# ---------------------------------------------------------------------------


def derive_power_index(ops: float, era: float, mean_ops: float, mean_era: float) -> float:
    """Peer-relative composite metric: (OPS / mean_OPS) × (mean_ERA / ERA).

    Higher is better.  Teams with OPS above average AND ERA below average score
    above 1.0.  Division-mean normalisation makes the metric comparable across
    different scoring environments and seasons.
    """
    ops_ratio = ops / mean_ops if mean_ops > 0 else 0.0
    era_ratio = mean_era / era if era > 0 else 0.0
    return round(ops_ratio * era_ratio, 4)


def derive_pythag(runs_scored: float, runs_allowed: float) -> float:
    """Bill James Pythagorean win expectation: RS^2 / (RS^2 + RA^2).

    Returns 0.5 when both are zero (no data) to avoid division by zero.
    Exponent 2 is the classic form; advanced models use 1.83 but the
    integer form is accurate enough for single-season leaderboards.
    """
    if runs_scored <= 0 and runs_allowed <= 0:
        return 0.5
    denom = runs_scored**2 + runs_allowed**2
    return round(runs_scored**2 / denom, 4) if denom > 0 else 0.5


def _format_wl(wins: int, losses: int) -> str:
    """Return 'W-L' string for the console leaderboard."""
    return f"{wins}-{losses}"


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Coerce a value to float; return ``default`` on failure or None."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    """Coerce a value to int; return ``default`` on failure or None."""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# CustomCurrent subclasses — T5 parent-child drill nodes
# ---------------------------------------------------------------------------

_HITTING_URL = "https://statsapi.mlb.com/api/v1/teams/{}/stats?group=hitting&stats=season&season=2026"
_PITCHING_URL = "https://statsapi.mlb.com/api/v1/teams/{}/stats?group=pitching&stats=season&season=2026"

_HITTING_CONV: dict[str, Any] = {
    "ops": calc(float, "stat.ops", default=0.0, target_type=float),
    "obp": calc(float, "stat.obp", default=0.0, target_type=float),
    "slg": calc(float, "stat.slg", default=0.0, target_type=float),
    "avg": calc(float, "stat.avg", default=0.0, target_type=float),
    "home_runs": calc(int, "stat.homeRuns", default=0, target_type=int),
    "rbi": calc(int, "stat.rbi", default=0, target_type=int),
    "strikeouts": calc(int, "stat.strikeOuts", default=0, target_type=int),
    "walks": calc(int, "stat.baseOnBalls", default=0, target_type=int),
}

_PITCHING_CONV: dict[str, Any] = {
    # default=9.99 for era/whip so garbage rows sort to bottom of Power Index
    "era": calc(float, "stat.era", default=9.99, target_type=float),
    "whip": calc(float, "stat.whip", default=9.99, target_type=float),
    "wins": calc(int, "stat.wins", default=0, target_type=int),
    "losses": calc(int, "stat.losses", default=0, target_type=int),
    "strikeouts": calc(int, "stat.strikeOuts", default=0, target_type=int),
    "walks": calc(int, "stat.baseOnBalls", default=0, target_type=int),
    "innings_pitched": calc(str, "stat.inningsPitched", default="0", target_type=str),
    "earned_runs": calc(int, "stat.earnedRuns", default=0, target_type=int),
}


class HittingDrillCurrent(CustomCurrent):
    """T5 drill: reads MLBAllTeam snapshot, filters to AL East, fires 5 hitting-stat fetches.

    Must park ``_tideweaver_snapshot`` after the incorp call so the tail
    Fjord's ``outflow(state)`` can read ``state['MLBHitting']``.  Without
    the explicit park the WeakValueDictionary GC race empties the registry
    between ticks.
    """

    async def tick(self, scheduler: Any) -> None:
        """Filter upstream MLBAllTeam to AL East; fire T5 hitting drills; park snapshot."""
        snapshot = getattr(MLBAllTeam, "_tideweaver_snapshot", None)
        if not snapshot:
            return  # all_teams hasn't fired yet — skip silently
        al_east = [t for t in snapshot if getattr(t, "division_id", 0) == _AL_EAST_DIVISION_ID]
        if not al_east:
            return
        result = await MLBHitting.incorp(
            inc_parent=al_east,
            inc_child="inc_code",
            inc_url=_HITTING_URL,
            rec_path="stats.0.splits.0",
            inc_code="team.id",
            conv_dict=_HITTING_CONV,
        )
        MLBHitting._tideweaver_snapshot = (
            list(result)
            if isinstance(result, list)
            # type: ignore[attr-defined]
            else ([result] if result is not None else [])
        )


class PitchingDrillCurrent(CustomCurrent):
    """T5 drill: reads MLBAllTeam snapshot, filters to AL East, fires 5 pitching-stat fetches.

    Same snapshot-parking pattern as HittingDrillCurrent — required so
    ``state['MLBPitching']`` is non-empty when the tail Fjord flushes.
    """

    async def tick(self, scheduler: Any) -> None:
        """Filter upstream MLBAllTeam to AL East; fire T5 pitching drills; park snapshot."""
        snapshot = getattr(MLBAllTeam, "_tideweaver_snapshot", None)
        if not snapshot:
            return  # all_teams hasn't fired yet — skip silently
        al_east = [t for t in snapshot if getattr(t, "division_id", 0) == _AL_EAST_DIVISION_ID]
        if not al_east:
            return
        result = await MLBPitching.incorp(
            inc_parent=al_east,
            inc_child="inc_code",
            inc_url=_PITCHING_URL,
            rec_path="stats.0.splits.0",
            inc_code="team.id",
            conv_dict=_PITCHING_CONV,
        )
        MLBPitching._tideweaver_snapshot = (
            list(result)
            if isinstance(result, list)
            # type: ignore[attr-defined]
            else ([result] if result is not None else [])
        )


# ---------------------------------------------------------------------------
# Outflow function — joins 4 upstream graph maps into ranked Pulse Cards
# ---------------------------------------------------------------------------


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Join standings + hitting + pitching + team metadata into one Pulse Card per AL East team.

    Args:
        state: Keyed by upstream ``Incorporator`` subclass name; maps to a list
            of that class's current registry instances held alive between ticks
            via ``_tideweaver_snapshot`` strong-refs.

    Returns:
        Five rows sorted by ``power_index`` descending, one per AL East team,
        or an empty list when any required upstream hasn't fired yet.
    """
    teams_by_id = {
        t.inc_code: t for t in state.get("MLBAllTeam", []) if getattr(t, "division_id", 0) == _AL_EAST_DIVISION_ID
    }
    hitting_by_id = {h.inc_code: h for h in state.get("MLBHitting", [])}
    pitching_by_id = {p.inc_code: p for p in state.get("MLBPitching", [])}

    # Guard: CustomCurrents may not have fired on the first few ticks.
    if not hitting_by_id or not pitching_by_id:
        return []

    standings_row = next(
        (r for r in state.get("MLBStandings", []) if r.inc_code == _AL_EAST_DIVISION_ID),
        None,
    )
    if standings_row is None:
        return []

    # teamRecords may be a list attribute (Stream materialised it) or missing.
    team_records: list[Any] = getattr(standings_row, "teamRecords", None) or []

    rows: list[dict[str, Any]] = []
    for tr in team_records:
        # teamRecord.team may be a sub-Incorporator instance OR a raw dict
        # depending on how MLBStandings' nested list is materialised.
        team_sub = getattr(tr, "team", None)
        if team_sub is not None:
            # Instance attribute path
            team_id = getattr(team_sub, "id", None)
            if team_id is None and isinstance(team_sub, dict):
                team_id = team_sub.get("id")
        else:
            # Dict access path
            raw_tr = tr if isinstance(tr, dict) else {}
            team_id = raw_tr.get("team", {}).get("id")

        if team_id is None or team_id not in teams_by_id:
            continue

        team = teams_by_id[team_id]
        hit = hitting_by_id.get(team_id)
        pit = pitching_by_id.get(team_id)
        if hit is None or pit is None:
            continue

        # Extract W-L from teamRecord; path depends on materialisation form.
        if isinstance(tr, dict):
            wins = _safe_int(tr.get("wins", tr.get("leagueRecord", {}).get("wins", 0)))
            losses = _safe_int(tr.get("losses", tr.get("leagueRecord", {}).get("losses", 0)))
            win_pct_raw = tr.get("winningPercentage", tr.get("leagueRecord", {}).get("pct", "0"))
            games_back_raw = tr.get("gamesBack", "0")
            runs_scored = _safe_float(tr.get("runsScored", 0))
            runs_allowed = _safe_float(tr.get("runsAllowed", 0))
        else:
            league_rec = getattr(tr, "leagueRecord", None) or {}
            wins = _safe_int(
                getattr(
                    tr,
                    "wins",
                    getattr(league_rec, "wins", 0) if not isinstance(league_rec, dict) else league_rec.get("wins", 0),
                )
            )
            losses = _safe_int(
                getattr(
                    tr,
                    "losses",
                    getattr(league_rec, "losses", 0)
                    if not isinstance(league_rec, dict)
                    else league_rec.get("losses", 0),
                )
            )
            win_pct_raw = getattr(
                tr,
                "winningPercentage",
                getattr(league_rec, "pct", "0") if not isinstance(league_rec, dict) else league_rec.get("pct", "0"),
            )
            games_back_raw = getattr(tr, "gamesBack", "0")
            runs_scored = _safe_float(getattr(tr, "runsScored", 0))
            runs_allowed = _safe_float(getattr(tr, "runsAllowed", 0))

        win_pct = _safe_float(win_pct_raw)
        games_back_str = str(games_back_raw) if games_back_raw is not None else "0"
        games_back = 0.0 if games_back_str in ("-", "") else _safe_float(games_back_str)

        ops = _safe_float(getattr(hit, "ops", 0.0))
        era = _safe_float(getattr(pit, "era", 9.99), default=9.99)
        pythag = derive_pythag(runs_scored, runs_allowed)

        rows.append(
            {
                "inc_code": team_id,
                "team_name": getattr(team, "name", ""),
                "wins": wins,
                "losses": losses,
                "win_pct": win_pct,
                "games_back": games_back,
                "ops": ops,
                "era": era,
                "power_index": 0.0,  # filled after division means computed
                "pythag": pythag,
                "pythag_delta": 0.0,  # filled after power_index pass
                "power_rank": 0,  # filled after sort
            }
        )

    if not rows:
        return []

    # Compute division means for Power Index normalisation.
    mean_ops = sum(r["ops"] for r in rows) / len(rows)
    mean_era = sum(r["era"] for r in rows) / len(rows)

    for r in rows:
        r["power_index"] = derive_power_index(r["ops"], r["era"], mean_ops, mean_era)
        r["pythag_delta"] = round(r["pythag"] - r["win_pct"], 4)

    rows.sort(key=operator.itemgetter("power_index"), reverse=True)

    for rank, r in enumerate(rows, start=1):
        r["power_rank"] = rank

    return rows
