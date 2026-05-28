"""Outflow logic for the MLB AL East Pulse Tideweaver diamond.

Defines the six ``Incorporator`` subclasses the diamond uses (one per
Stream / CustomCurrent + the tail's output class) plus the
``outflow(state)`` function the ``Fjord`` tail current calls each tick.

The outflow joins four upstream graph maps from state — `MLBAllTeam`
(filtered to AL East via ``division_id == 201``), `MLBHitting`,
`MLBPitching`, `MLBStandings` — plus today's schedule, and computes
two independent composite metrics side-by-side for each team:

  * Power Index — peer-relative composite: ``(team_OPS / league_avg_OPS)
    × (league_avg_ERA / team_ERA) × win_pct``.  Higher = stronger
    all-around team relative to division peers.
  * Pythagorean win expectation — classic sabermetric:
    ``runs_scored² / (runs_scored² + runs_allowed²)``.  ``pythag_delta =
    pythag - win_pct`` exposes over-/under-performing teams.

Output rows are pre-sorted by ``power_index`` descending.
"""

from __future__ import annotations

from operator import itemgetter
from typing import Any

from pydantic import ConfigDict

from incorporator import Incorporator


# ---------------------------------------------------------------------------
# Source classes (one per upstream Stream / CustomCurrent + tail output)
# All use extra="allow" so conv_dict-derived fields are preserved alongside
# the auto-inferred fields.
# Matches the pattern in tests/test_tideweaver_routing_diamond.py.
# ---------------------------------------------------------------------------


class MLBSchedule(Incorporator):
    """Today's MLB game schedule (head Stream)."""

    model_config = ConfigDict(extra="allow")


class MLBAllTeam(Incorporator):
    """All 30 MLB teams + their league / division / venue (middle Stream)."""

    model_config = ConfigDict(extra="allow")


class MLBHitting(Incorporator):
    """Team-level season hitting stats (populated by HittingDrillCurrent)."""

    model_config = ConfigDict(extra="allow")


class MLBPitching(Incorporator):
    """Team-level season pitching stats (populated by PitchingDrillCurrent)."""

    model_config = ConfigDict(extra="allow")


class MLBStandings(Incorporator):
    """Live division standings (middle Stream, refreshes during the run)."""

    model_config = ConfigDict(extra="allow")


class TeamPulseCard(Incorporator):
    """Derived per-team Power Card — built by the fjord flush each tick."""

    model_config = ConfigDict(extra="allow")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


AL_EAST_DIVISION_ID: int = 201


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_al_east(team: Any) -> bool:
    """Filter predicate matching the team's `division_id` against AL East (201).

    `MLBAllTeam` carries the full 30-team registry; the outflow narrows it
    down to the 5 AL East teams here (same filter the CustomCurrents apply
    upstream for their T5 drills).
    """
    return getattr(team, "division_id", None) == AL_EAST_DIVISION_ID


def _mean(values: list[float]) -> float:
    """Defensive mean — returns 0.0 on an empty list (caller pre-filtered)."""
    real = [v for v in values if v is not None]
    return sum(real) / len(real) if real else 0.0


def _power_index(team_ops: float, team_era: float, win_pct: float, league_ops: float, league_era: float) -> float:
    """Composite peer-relative team strength.

    `team_OPS` normalized against the division peer mean × inverted
    `team_ERA` peer ratio × actual `win_pct`.  Higher = stronger team.
    Returns 0.0 if any divisor is missing/zero (defensive).
    """
    if not league_ops or not team_era:
        return 0.0
    return (team_ops / league_ops) * (league_era / team_era) * win_pct


def _pythag(runs_scored: int, runs_allowed: int) -> float:
    """Pythagorean win expectation — classic sabermetric.

    Returns the expected winning percentage given a team's run differential.
    `runs² / (runs² + ra²)`.  Returns 0.0 when both inputs are zero
    (off-season / no games played).
    """
    rs2 = runs_scored**2
    ra2 = runs_allowed**2
    denom = rs2 + ra2
    if not denom:
        return 0.0
    return rs2 / denom


# ---------------------------------------------------------------------------
# outflow(state) — called by the Fjord tail current every flush
# ---------------------------------------------------------------------------


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Join the four upstream registries + today's schedule into per-team Pulse Cards.

    Reads from state (transitive-upstream snapshot):
      * `state["MLBAllTeam"]`     — 30 teams; we filter to AL East
      * `state["MLBHitting"]`     — 5 team-level hitting rows
      * `state["MLBPitching"]`    — 5 team-level pitching rows
      * `state["MLBStandings"]`   — 5 live standings rows
      * `state["MLBSchedule"]`    — today's MLB games (we filter to AL East matchups)

    Produces sorted Power Cards: each AL East team gets one card with
    standing, hitting, pitching, today's game (if any), Power Index, and
    Pythagorean expectation.  Rows pre-sorted by Power Index descending.
    """
    all_teams = list(state.get("MLBAllTeam", []) or [])
    hitting_rows = list(state.get("MLBHitting", []) or [])
    pitching_rows = list(state.get("MLBPitching", []) or [])
    standings_rows = list(state.get("MLBStandings", []) or [])
    schedule_rows = list(state.get("MLBSchedule", []) or [])

    # Narrow MLBAllTeam to AL East via division_id filter.
    al_east_teams = [t for t in all_teams if _is_al_east(t)]
    if len(al_east_teams) < 5:
        # Diamond hasn't fully populated yet; emit nothing this flush.
        return []

    al_east_team_ids = {t.inc_code for t in al_east_teams}

    # Index the upstream rows by team id for O(1) join.
    hitting_by_tid = {getattr(h, "team_id", None): h for h in hitting_rows}
    pitching_by_tid = {getattr(p, "team_id", None): p for p in pitching_rows}
    standings_by_tid = {getattr(s, "team_id", None): s for s in standings_rows}

    # Today's schedule filtered to AL East matchups.
    al_east_games = [
        g
        for g in schedule_rows
        if getattr(g, "home_team_id", None) in al_east_team_ids
        or getattr(g, "away_team_id", None) in al_east_team_ids
    ]

    # League averages for Power Index normalization (only over teams we have hitting/pitching for).
    league_ops_mean = _mean([float(getattr(h, "ops", 0.0) or 0.0) for h in hitting_rows])
    league_era_mean = _mean([float(getattr(p, "era", 0.0) or 0.0) for p in pitching_rows])

    cards: list[dict[str, Any]] = []
    for team in al_east_teams:
        tid = team.inc_code
        h = hitting_by_tid.get(tid)
        p = pitching_by_tid.get(tid)
        s = standings_by_tid.get(tid)

        win_pct = float(getattr(s, "win_pct", 0.0) or 0.0) if s else 0.0
        team_ops = float(getattr(h, "ops", 0.0) or 0.0) if h else 0.0
        team_era = float(getattr(p, "era", 0.0) or 0.0) if p else 0.0
        runs_scored = int(getattr(s, "runs_scored", 0) or 0) if s else 0
        runs_allowed = int(getattr(s, "runs_allowed", 0) or 0) if s else 0

        power_index = _power_index(team_ops, team_era, win_pct, league_ops_mean, league_era_mean)
        pythag = _pythag(runs_scored, runs_allowed)
        pythag_delta = pythag - win_pct

        # Find today's game for this team (if any).
        team_game = None
        for g in al_east_games:
            home_id = getattr(g, "home_team_id", None)
            away_id = getattr(g, "away_team_id", None)
            if home_id == tid or away_id == tid:
                # Resolve opponent name from MLBAllTeam registry.
                opponent_id = away_id if home_id == tid else home_id
                opponent_team = next((t for t in all_teams if t.inc_code == opponent_id), None)
                team_game = {
                    "opponent": getattr(opponent_team, "name", None) if opponent_team else None,
                    "home": home_id == tid,
                    "status": getattr(g, "game_status", None),
                    "time": getattr(g, "game_time", None),
                }
                break

        cards.append(
            {
                "inc_code": tid,
                "team": getattr(team, "name", None),
                "abbr": getattr(team, "abbreviation", None),
                "abbr_lower": getattr(team, "abbr_lower", None),
                "venue": getattr(team, "venue_name", None),
                "league": getattr(team, "league_name", None),
                "standing": {
                    "wins": getattr(s, "wins", None) if s else None,
                    "losses": getattr(s, "losses", None) if s else None,
                    "win_pct": win_pct,
                    "games_back": getattr(s, "games_back", None) if s else None,
                    "streak": getattr(s, "streak", None) if s else None,
                    "runs_scored": runs_scored,
                    "runs_allowed": runs_allowed,
                    "over_500": getattr(s, "over_500", None) if s else None,
                },
                "hitting": {
                    "avg": getattr(h, "avg", None) if h else None,
                    "obp": getattr(h, "obp", None) if h else None,
                    "slg": getattr(h, "slg", None) if h else None,
                    "ops": team_ops or None,
                    "home_runs": getattr(h, "home_runs", None) if h else None,
                },
                "pitching": {
                    "era": team_era or None,
                    "whip": getattr(p, "whip", None) if p else None,
                    "k_per_9": getattr(p, "strikeouts_per9inn", None) if p else None,
                    "saves": getattr(p, "saves", None) if p else None,
                    "power_pitchers": getattr(p, "power_pitchers", None) if p else None,
                },
                "todays_game": team_game,
                "power_index": round(power_index, 3),
                "pythag": round(pythag, 3),
                "pythag_delta": round(pythag_delta, 3),
            }
        )

    # Pre-sort by Power Index descending so the NDJSON is ranked.
    cards.sort(key=itemgetter("power_index"), reverse=True)
    return cards
