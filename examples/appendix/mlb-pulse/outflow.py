"""Sidecar for the MLB AL Pulse Tideweaver -- a pure name-bag.

This file exists only because the CLI needs an importable module to point
``watershed.json``'s ``"outflow"`` key at; otherwise ``outflow(state)`` below
would just sit at the bottom of ``mlb_pulse.py``, as the return-twin of
``print_leaderboard()``'s loop -- same fields, same join keys, returned as
dicts instead of printed as table rows.

``MLBAllTeam``/``MLBStandings``/``MLBHitting``/``MLBPitching``/``TeamPulseCard``
and the named helpers (``parse_games_back``/``derive_pythag``/
``derive_power_index``) are defined ONCE, in ``mlb_pulse.py``. This module
only re-exports them (via a plain ``import``) plus the CLI-only tokens the
JSON config needs (``window_start``/``window_end``) and the fjord's
``outflow(state)`` fusion hook -- the team/hitting/pitching join happens
READ-TIME, once per wave, directly against the live class-level graph maps
(``MLBAllTeam.inc_dict`` / ``MLBHitting.inc_dict`` / ``MLBPitching.inc_dict``),
no intermediate link ops.
"""

from __future__ import annotations

import operator
from datetime import datetime, timedelta, timezone
from typing import Any

from mlb_pulse import (
    MLBAllTeam,
    MLBHitting,
    MLBPitching,
    MLBStandings,
    TeamPulseCard,
    derive_power_index,
    derive_pythag,
    parse_games_back,
)

__all__ = [
    "MLBAllTeam",
    "MLBStandings",
    "MLBHitting",
    "MLBPitching",
    "TeamPulseCard",
    "derive_power_index",
    "derive_pythag",
    "parse_games_back",
    "window_start",
    "window_end",
    "outflow",
]

# Dateless window: watershed.json's "window" references these public names
# via the "@window_start" / "@window_end" sigil (resolve_tokens, extended
# with this sidecar's public names by merge_sidecar_extra_names). Evaluated
# once at import time, mirroring mlb_pulse.py's own main() window duration.
window_start = datetime.now(timezone.utc)
window_end = window_start + timedelta(seconds=50)


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Join standings + team + hitting + pitching into one Pulse Card per AL team, read-time.

    Args:
        state: Keyed by upstream ``Incorporator`` subclass name; maps to a
            list of that class's parked ``_tideweaver_snapshot`` rows. This
            is a Tideweaver Fjord-current wave (a ``Watershed``/diamond run,
            not a ``cls.fjord()`` daemon), so ``state`` values are PLAIN
            lists -- ``MLBAllTeam``/``MLBHitting``/``MLBPitching`` are read
            via their class-level ``inc_dict`` graph map instead.

    Returns:
        Up to 15 rows, one per AL team, sorted by ``power_index`` descending
        with ``power_rank`` 1-15 -- or an empty list before ``standings`` has
        fired, or if every team is still missing a hitting/pitching/roster
        match (early ticks before the T5 drills complete).

    ``MLBStandings.teamRecords`` auto-promotes to nested submodels with plain
    dotted attribute access (``tr.team.id``, ``tr.wins``, ``tr.gamesBack``,
    ...) -- no conv_dict on ``MLBStandings`` at all; the flatten + derive
    happens here, at the diamond's join point, exactly where the framework's
    own guidance puts export shaping. ``ops``/``era`` arrive as real floats
    (each source pre-coerces its own field via a one-entry ``conv_dict``);
    ``win_pct``/``games_back`` are coerced here because ``winningPercentage``/
    ``gamesBack`` are per-element of a nested list, which conv_dict has no
    primitive for. ``gamesBack``'s division-leader sentinel is the literal
    string ``"-"`` (not covered by ``is_garbage_value``'s GARBAGE_VALUES set)
    -- routing it through a ``calc()`` exception-fallback would log a
    warning on every division leader every tick; ``parse_games_back()``
    handles it directly with zero warnings.
    """
    standings = state.get("MLBStandings", [])
    if not standings:
        return []

    rows: list[dict[str, Any]] = []
    for division in standings:
        for tr in division.teamRecords:
            team_id = tr.team.id
            team = MLBAllTeam.inc_dict.get(team_id)
            hit = MLBHitting.inc_dict.get(team_id)
            pit = MLBPitching.inc_dict.get(team_id)
            if team is None or hit is None or pit is None:
                continue
            win_pct = float(tr.winningPercentage)
            pythag = derive_pythag(tr.runsScored, tr.runsAllowed)
            rows.append(
                {
                    "inc_code": team_id,
                    "team_name": team.inc_name,
                    "wins": tr.wins,
                    "losses": tr.losses,
                    "win_pct": win_pct,
                    "games_back": parse_games_back(tr.gamesBack),
                    "ops": hit.ops,
                    "era": pit.era,
                    "power_index": 0.0,  # filled after league means computed
                    "pythag": pythag,
                    "pythag_delta": round(pythag - win_pct, 4),
                    "power_rank": 0,  # filled after sort
                }
            )

    if not rows:
        return []

    mean_ops = sum(r["ops"] for r in rows) / len(rows)
    mean_era = sum(r["era"] for r in rows) / len(rows)
    for r in rows:
        r["power_index"] = derive_power_index(r["ops"], r["era"], mean_ops, mean_era)

    rows.sort(key=operator.itemgetter("power_index"), reverse=True)
    for rank, r in enumerate(rows, start=1):
        r["power_rank"] = rank

    return rows
