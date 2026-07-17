"""Outflow logic and class definitions for the MLB AL Pulse Tideweaver diamond.

Defines the six ``Incorporator`` subclasses referenced from ``watershed.json``
and ``mlb_pulse.py``, plus named module-level helpers and the ``outflow(state)``
function the tail Fjord calls each tick.

Imported by both the Python entry (``mlb_pulse.py``) and the CLI form
(``incorporator tideweaver run watershed.json``), so host-throttle registration
lives here as well as in the entry script.  Both import paths must register the
penstock independently.

Row filtering: the parent ``al_teams`` Stream uses URL-level filtering
(``?sportId=1&leagueId=103``) to scope to the 15 American League teams
server-side. No post-fetch row filter is applied here — the outflow joins
across whatever the parent's scope produced.

Relative paths in the config resolve against its directory, so it runs from
any directory. See the README's "Run it" section for the run commands.
"""

from __future__ import annotations

import operator
from datetime import datetime, timedelta, timezone
from typing import Any

from incorporator import Incorporator, register_host_penstock
from incorporator.schema.converters import calc, is_garbage_value

# ---------------------------------------------------------------------------
# Host throttle — 1 req/sec = 60 req/min, well under any undocumented MLB cap.
# Registered at module-top so both the Python entry and the CLI form impose
# the same constraint regardless of import order.
# ---------------------------------------------------------------------------

register_host_penstock("statsapi.mlb.com", rate_per_sec=1.0)

# ---------------------------------------------------------------------------
# Dateless window: watershed.json's "window" references these public names
# via the "@window_start" / "@window_end" sigil. Mirrors mlb_pulse.py's own
# `_run()` window exactly (25s), keeping both entry forms byte-identical in
# timing. Stretch this if the MLB API's ~31 calls @ 1 req/sec run long --
# mirrors the README's rate-limit note.
# ---------------------------------------------------------------------------

window_start = datetime.now(timezone.utc)
window_end = window_start + timedelta(seconds=25)

# ---------------------------------------------------------------------------
# Named module-level helpers (lambda-free, per AGENTS.md H3 idiom)
# ---------------------------------------------------------------------------


def _coerce_int(value: Any, default: int) -> int:
    """Reuse the framework's garbage-value contract; only try int() on real data."""
    if is_garbage_value(value):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any, default: float) -> float:
    """Reuse the framework's garbage-value contract; only try float() on real data."""
    if is_garbage_value(value):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_games_back(value: Any) -> float:
    """MLB's league-leader sentinel is the literal string '-', not covered by
    ``is_garbage_value``'s GARBAGE_VALUES set — explicit sentinel maps to 0.0.
    """
    if value in ("-", "", None):
        return 0.0
    return _coerce_float(value, 0.0)


def flatten_team_records(team_records: list[dict]) -> list[dict]:
    """Flatten + coerce one division's raw ``teamRecords`` list into clean rows.

    Runs once per MLBStandings row at build time (inside ``calc``), so
    ``outflow()`` reads plain dict keys with no getattr/isinstance branching.
    Falls back top-level field -> ``leagueRecord`` sub-dict, matching the
    live MLB Stats API's occasional omission of the top-level duplicate.
    """
    rows: list[dict] = []
    for tr in team_records or []:
        league_rec = tr.get("leagueRecord") or {}
        team_id = (tr.get("team") or {}).get("id")
        if team_id is None:
            continue
        wins = tr.get("wins", league_rec.get("wins", 0))
        losses = tr.get("losses", league_rec.get("losses", 0))
        win_pct_raw = tr.get("winningPercentage", league_rec.get("pct", "0"))
        gb_raw = tr.get("gamesBack", "0")
        win_pct = _coerce_float(win_pct_raw, 0.0)
        runs_scored = _coerce_float(tr.get("runsScored", 0), 0.0)
        runs_allowed = _coerce_float(tr.get("runsAllowed", 0), 0.0)
        pythag = derive_pythag(runs_scored, runs_allowed)
        rows.append(
            {
                "team_id": _coerce_int(team_id, 0),
                "wins": _coerce_int(wins, 0),
                "losses": _coerce_int(losses, 0),
                "win_pct": win_pct,
                "games_back": _coerce_games_back(gb_raw),
                "runs_scored": runs_scored,
                "runs_allowed": runs_allowed,
                "pythag": pythag,
                "pythag_delta": round(pythag - win_pct, 4),
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Incorporator subclasses — one per stream node + two derived output classes
# ---------------------------------------------------------------------------


class MLBSchedule(Incorporator):
    """Today's game schedule from /api/v1/schedule — rec_path 'dates.0.games'."""


class MLBAllTeam(Incorporator):
    """American League teams from /api/v1/teams?sportId=1&leagueId=103 — rec_path 'teams'.

    URL-level filtering (``?leagueId=103``) scopes the parent to the 15 AL teams
    server-side. This is the framework's preferred row-filter primitive; see the
    README sidebar for the row-filter decision tree.
    """


# Build-time flattening: teamRecords[] nests team.id + a top-level/leagueRecord
# W-L/pct dual-path per row.  Coercing + flattening once here (rather than at
# outflow read-time) means outflow() reads plain dict keys off ``team_rows``
# with zero getattr/isinstance branching.  See the README's honest-boundary
# note for why the team/hitting/pitching JOIN itself still happens read-time.
#
# ``flatten_team_records`` is public (no leading underscore) so both entry
# forms resolve it identically: the Python entry imports it directly via
# this dict, and the CLI's watershed.json references it by name in its own
# "standings" conv_dict entry — both paths merge outflow.py's public names
# into the same token-resolver allow-list (usercode.merge_sidecar_extra_names).
MLBSTANDINGS_CONV_DICT = {
    "team_rows": calc(flatten_team_records, "teamRecords", default=[]),
}


class MLBStandings(Incorporator):
    """AL division standings record from /api/v1/standings?leagueId=103 — rec_path 'records'.

    Returns one record per AL division (East/Central/West). Each carries its
    own ``teamRecords`` list, flattened + coerced at build time into
    ``team_rows`` (see ``MLBSTANDINGS_CONV_DICT``). ``outflow()`` iterates
    ALL records to build a league-wide leaderboard.
    """


class MLBHitting(Incorporator):
    """Per-team season hitting stats — populated by Stream(parent_current='al_teams') T5 drills."""


class MLBPitching(Incorporator):
    """Per-team season pitching stats — populated by Stream(parent_current='al_teams') T5 drills."""


class TeamPulseCard(Incorporator):
    """Derived AL Pulse Card — one row per team, produced by outflow(state)."""


def derive_power_index(ops: float, era: float, mean_ops: float, mean_era: float) -> float:
    """Peer-relative composite metric: (OPS / mean_OPS) × (mean_ERA / ERA).

    Higher is better.  Teams with OPS above average AND ERA below average score
    above 1.0.  League-mean normalisation makes the metric comparable across
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


# ---------------------------------------------------------------------------
# Outflow function — joins 4 upstream graph maps into ranked Pulse Cards
# ---------------------------------------------------------------------------


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Join standings + hitting + pitching + team metadata into one Pulse Card per AL team.

    A Tideweaver ``diamond`` has no cross-current inflow hook, so this join
    stays read-time (see the README's honest-boundary note) — but every
    field it reads off either side is now a plain, pre-coerced attribute or
    dict key sourced from each current's own build-time ``conv_dict``.

    Args:
        state: Keyed by upstream ``Incorporator`` subclass name; maps to a list
            of that class's current registry instances held alive between ticks
            via ``_tideweaver_snapshot`` strong-refs.

    Returns:
        Up to 15 rows sorted by ``power_index`` descending, one per AL team,
        or an empty list when any required upstream hasn't fired yet.

    Raises:
        RuntimeError: If a ``MLBStandings`` instance lacks ``team_rows`` — this
            signals the ``standings`` current was built without its
            ``team_rows`` conv_dict entry (see ``flatten_team_records`` /
            ``MLBSTANDINGS_CONV_DICT`` in this file), which both the Python
            entry (``mlb_pulse.py``) and the CLI ``watershed.json`` form now
            populate identically. Fails loud on the first offending row
            instead of letting the join silently degrade to zero rows.
    """
    standings_records = state.get("MLBStandings", [])
    for standings_row in standings_records:
        if not hasattr(standings_row, "team_rows"):
            raise RuntimeError(
                "MLBStandings instance is missing 'team_rows' -- every "
                "MLBStandings instance must carry it. The 'standings' current's "
                "conv_dict (MLBSTANDINGS_CONV_DICT in mlb_pulse.py; the "
                "equivalent JSON conv_dict entry in watershed.json) is "
                "responsible for populating it via flatten_team_records."
            )

    teams_by_id = {t.inc_code: t for t in state.get("MLBAllTeam", [])}
    hitting_by_id = {h.inc_code: h for h in state.get("MLBHitting", [])}
    pitching_by_id = {p.inc_code: p for p in state.get("MLBPitching", [])}

    # Guard: parent-current Streams may not have fired on the first few ticks.
    if not hitting_by_id or not pitching_by_id:
        return []

    if not standings_records:
        return []

    rows: list[dict[str, Any]] = []
    # Iterate ALL AL division standings (East/Central/West) — the al_teams URL
    # already scoped the parent set, so any team appearing in standings + teams
    # + hitting + pitching is in scope.
    for standings_row in standings_records:
        # ``team_rows`` is a calc()-computed list[dict]; the schema builder still
        # promotes each dict into a nested dynamic sub-model (list[dict] fields
        # get the same dict->submodel treatment as raw JSON), so reads here are
        # plain attributes, not dict subscripts.
        for tr_row in standings_row.team_rows:
            team_id = tr_row.team_id
            team = teams_by_id.get(team_id)
            hit = hitting_by_id.get(team_id)
            pit = pitching_by_id.get(team_id)
            if team is None or hit is None or pit is None:
                continue
            rows.append(
                {
                    "inc_code": team_id,
                    "team_name": team.inc_name,
                    "wins": tr_row.wins,
                    "losses": tr_row.losses,
                    "win_pct": tr_row.win_pct,
                    "games_back": tr_row.games_back,
                    "ops": hit.ops,
                    "era": pit.era,
                    "power_index": 0.0,  # filled after league means computed
                    "pythag": tr_row.pythag,
                    "pythag_delta": tr_row.pythag_delta,
                    "power_rank": 0,  # filled after sort
                }
            )

    if not rows:
        return []

    # League-wide means for Power Index normalisation (spans all returned AL teams).
    mean_ops = sum(r["ops"] for r in rows) / len(rows)
    mean_era = sum(r["era"] for r in rows) / len(rows)

    for r in rows:
        r["power_index"] = derive_power_index(r["ops"], r["era"], mean_ops, mean_era)

    rows.sort(key=operator.itemgetter("power_index"), reverse=True)

    for rank, r in enumerate(rows, start=1):
        r["power_rank"] = rank

    return rows
