"""Outflow sidecar for the ESPN league-history one-shot fjord pipeline.

Defines the six source classes (``Season``, ``Owner``, ``Standing``,
``Matchup``, ``DraftPick``, ``PlayerName``) and the six DERIVED view classes
(``FranchiseCard``, ``SeasonTimelineRow``, ``RivalryRow``, ``RecordRow``,
``DraftTendencyRow``, ``SettingsRow``), all declared BARE (docstring only).
``flush()`` infers each view's schema from the rows ``outflow()`` returns for
it, using the bare class itself as the base -- every row field a view returns
is kept, and the built instances register into that class's own ``inc_dict``,
exactly like a bare source class under ``incorp()``. ``espn_league_history.py``'s
console report reads these classes' `inc_dict` back AFTER the fjord loop ends
-- and ``outflow(state)`` is the return-twin of that same print loop. Every
field a view needs is either build-time-coerced on its own source's
``conv_dict`` (inline in ``espn_league_history.py``'s ``incorp_params``) or
resolved read-time here, directly off the live ``state["X"]`` snapshot fjord
hands this function each wave -- ``state`` values are live
``IncorporatorList``s in this ``cls.fjord()`` daemon path (not the
Tideweaver-plain-list form; see ``espn_league_history.py``'s module docstring
for the two-path split).

Cross-row aggregation (per-franchise rollups, the rivalry matrix, the
records-book extremes, draft tendencies) is plain Python: ``defaultdict``
buckets, ``Counter``, and ``max()``/``min()`` picks -- the same shape
``examples/09-nascar-fantasy-fjord/outflow.py`` and
``examples/appendix/mlb-pulse/outflow.py`` already use. No ``calc_all``, no
group-collapsing second registration of a source under an alternate
``inc_code`` -- one team-game melt over ``state["Matchup"]`` builds
everything team/rivalry-related in a single pass.
"""

from __future__ import annotations

import operator
from collections import Counter, defaultdict
from typing import Any

from incorporator import Incorporator

TOP_N_MOST_DRAFTED = 15


class Season(Incorporator):
    """One ESPN league-season response. `roster_slots` is declared (not
    inferred) so its digit-string keys ("0", "2", ...) survive
    `infer_dynamic_schema`'s cross-record key union untouched -- the schema
    builder skips inference for any field already present on the base class."""

    roster_slots: dict[str, int] | None = None


class Owner(Incorporator):
    """One league member (GUID + display name), batched network-free off every season's `members`."""


class Standing(Incorporator):
    """One team-season, `inc_code="team_key"` (a bare ESPN `team.id` repeats every season)."""


class Matchup(Incorporator):
    """One scheduled game, `inc_code="id"`. Home/away owner GUIDs are threaded onto the raw
    payload pre-fjord (in `espn_league_history.py`, from a sibling source with no seeding-order
    guarantee) -- everything else `outflow()` needs is a plain, unconverted payload field."""


class DraftPick(Incorporator):
    """One draft pick, batched network-free off every season's `draftDetail.picks`."""


class PlayerName(Incorporator):
    """Resolved player name + position -- the one genuinely-networked fjord source."""


class FranchiseCard(Incorporator):
    """View 1's derived row -- one all-time rollup per owner."""


class SeasonTimelineRow(Incorporator):
    """View 2's derived row -- one row per franchise-season."""


class RivalryRow(Incorporator):
    """View 3's derived row -- one row per all-time franchise pair."""


class RecordRow(Incorporator):
    """View 4's derived row -- ten all-time record kinds. Bare inference locks `value`'s
    type from the first-sampled row (a float), so the two trailing streak kinds render
    `10.0`/`9.0` via Pydantic's lax int-to-float coercion -- expected, not a regression."""


class DraftTendencyRow(Incorporator):
    """View 5's derived row -- one class, three row shapes (union of all fields, nullable
    where a given kind omits it): `round1_position_mix`, `most_drafted`, `first_overall`."""


class SettingsRow(Incorporator):
    """View 6's derived row -- one row per season. `roster_slots` keeps its digit-string
    keys as plain data; a season whose key-set is smaller than another season sharing the
    same flush wave gets its missing keys null-padded by the cross-row dict-submodel
    inference -- accepted, not a data loss (the season's own real values stay intact)."""


def outflow(state: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """The `fjord(outflow=)` contract -- the framework calls this by name once
    every source has seeded; its return dict's keys map to the six derived
    view classes."""
    seasons, owners, standings = state.get("Season"), state.get("Owner"), state.get("Standing")
    if seasons is None or owners is None or standings is None:
        return {}
    matchups = state.get("Matchup")
    draft_picks = state.get("DraftPick")
    player_names = state.get("PlayerName")
    names = {o.inc_code: o.display_name for o in owners}

    # ── Team-game melt: one fold over every decided matchup builds team_games,
    # rivalry_games, and playoff_seasons_by_owner in a single pass. Replaces the
    # old TeamGame/RivalryPair build-time classes entirely.
    team_games: list[dict[str, Any]] = []
    rivalry_games: list[dict[str, Any]] = []
    playoff_seasons_by_owner: dict[str, set[int]] = defaultdict(set)
    for m in matchups or []:
        if m.winner == "UNDECIDED":
            continue
        home_owner, away_owner = m.home_owner_guid, m.away_owner_guid
        home_score = float(m.home.totalPoints)
        away_score = float(m.away.totalPoints) if m.away else 0.0
        margin = round(abs(home_score - away_score), 2)
        if home_owner is not None:
            team_games.append(
                {
                    "season": m.season,
                    "week": m.matchupPeriodId,
                    "tier": m.playoffTierType,
                    "owner_guid": home_owner,
                    "opp_owner_guid": away_owner,
                    "score": home_score,
                    "result": "T" if m.winner not in ("HOME", "AWAY") else ("W" if m.winner == "HOME" else "L"),
                    "margin": margin,
                }
            )
        if away_owner is not None:
            team_games.append(
                {
                    "season": m.season,
                    "week": m.matchupPeriodId,
                    "tier": m.playoffTierType,
                    "owner_guid": away_owner,
                    "opp_owner_guid": home_owner,
                    "score": away_score,
                    "result": "T" if m.winner not in ("HOME", "AWAY") else ("W" if m.winner == "AWAY" else "L"),
                    "margin": margin,
                }
            )
        if home_owner is not None and away_owner is not None and home_owner != away_owner:
            owner_a, owner_b = (away_owner, home_owner) if away_owner < home_owner else (home_owner, away_owner)
            a_won = (m.winner == "AWAY") if owner_a == away_owner else (m.winner == "HOME")
            rivalry_games.append(
                {
                    "owner_a": owner_a,
                    "owner_b": owner_b,
                    "a_won": a_won,
                    "margin": margin,
                    "season": m.season,
                    "week": m.matchupPeriodId,
                    "is_playoff": m.playoffTierType != "NONE",
                }
            )
        if m.playoffTierType == "WINNERS_BRACKET":
            if home_owner is not None:
                playoff_seasons_by_owner[home_owner].add(m.season)
            if away_owner is not None:
                playoff_seasons_by_owner[away_owner].add(m.season)

    # ── All-play: bucket (season, week) -> {owner: score} for regular-season games,
    # then each row earns 1 win per opponent it outscored that week, 0.5 for a tie.
    week_scores: dict[tuple[int, int], dict[str, float]] = defaultdict(dict)
    for tg in team_games:
        if tg["tier"] == "NONE":
            week_scores[(tg["season"], tg["week"])][tg["owner_guid"]] = tg["score"]
    all_play_wins: dict[tuple[int, str], float] = defaultdict(float)
    for tg in team_games:
        if tg["tier"] != "NONE":
            continue
        others = [v for owner, v in week_scores[(tg["season"], tg["week"])].items() if owner != tg["owner_guid"]]
        all_play_wins[(tg["season"], tg["owner_guid"])] += sum(1 for v in others if tg["score"] > v) + 0.5 * sum(
            1 for v in others if tg["score"] == v
        )

    # ── Streaks: one chronological pass, tracking each owner's running win/loss run;
    # keep only the single best (streak, owner, season, week) tuple seen so far.
    win_run: dict[str, int] = defaultdict(int)
    loss_run: dict[str, int] = defaultdict(int)
    best_win: tuple[int, str | None, int | None, int | None] = (0, None, None, None)
    best_loss: tuple[int, str | None, int | None, int | None] = (0, None, None, None)
    for tg in sorted(team_games, key=operator.itemgetter("season", "week")):
        owner = tg["owner_guid"]
        win_run[owner] = win_run[owner] + 1 if tg["result"] == "W" else 0
        loss_run[owner] = loss_run[owner] + 1 if tg["result"] == "L" else 0
        if win_run[owner] > best_win[0]:
            best_win = (win_run[owner], owner, tg["season"], tg["week"])
        if loss_run[owner] > best_loss[0]:
            best_loss = (loss_run[owner], owner, tg["season"], tg["week"])

    # ── View 1 — FranchiseCard: bucket standings by owner (and, separately, by
    # season, for a last-place lookup); one literal per owner.
    standings_by_owner: dict[str, list[Any]] = defaultdict(list)
    standings_by_season: dict[int, list[Any]] = defaultdict(list)
    for s in standings:
        standings_by_owner[s.primaryOwner].append(s)
        standings_by_season[s.season].append(s)
    worst_rank_by_season: dict[int, int] = {}
    for season, rows in standings_by_season.items():
        positive_ranks = [r.final_rank for r in rows if r.final_rank > 0]
        if positive_ranks:
            worst_rank_by_season[season] = max(positive_ranks)

    franchise_cards: list[dict[str, Any]] = []
    for owner_guid, rows in standings_by_owner.items():
        wins, losses, ties = sum(s.wins for s in rows), sum(s.losses for s in rows), sum(s.ties for s in rows)
        played = wins + losses + ties
        seasons_played = len(rows)
        playoff_appearances = len(playoff_seasons_by_owner.get(owner_guid, set()))
        franchise_cards.append(
            {
                "owner_guid": owner_guid,
                "display_name": names.get(owner_guid, "Unknown"),
                "wins": wins,
                "losses": losses,
                "ties": ties,
                "win_pct": round(wins / played, 3) if played else 0.0,
                "points_for": round(sum(s.points_for for s in rows), 2),
                "points_against": round(sum(s.points_against for s in rows), 2),
                "seasons_played": seasons_played,
                "average_finish": round(sum(s.final_rank for s in rows) / seasons_played, 2),
                "championships": sum(s.is_champion for s in rows),
                "runner_ups": sum(s.is_runner_up for s in rows),
                "last_places": sum(
                    1 for s in rows if s.final_rank > 0 and s.final_rank == worst_rank_by_season.get(s.season)
                ),
                "playoff_appearances": playoff_appearances,
                "playoff_rate": round(playoff_appearances / seasons_played, 3) if seasons_played else 0.0,
            }
        )

    # ── View 2 — SeasonTimelineRow: one literal per franchise-season.
    season_timeline = []
    for s in standings:
        expected_wins = round(all_play_wins.get((s.season, s.primaryOwner), 0.0), 2)
        season_timeline.append(
            {
                "owner_guid": s.primaryOwner,
                "display_name": names.get(s.primaryOwner, "Unknown"),
                "season": s.season,
                "team_id": s.id,
                "team_name": s.name,
                "division_id": s.division_id,
                "seed": s.playoff_seed,
                "final_rank": s.final_rank,
                "wins": s.wins,
                "losses": s.losses,
                "ties": s.ties,
                "points_for": s.points_for,
                "points_against": s.points_against,
                "all_play_expected_wins": expected_wins,
                "luck_delta": round(s.wins + 0.5 * s.ties - expected_wins, 2),
            }
        )

    # ── View 3 — RivalryRow: bucket rivalry_games by (owner_a, owner_b).
    rivalry_by_pair: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for g in rivalry_games:
        rivalry_by_pair[(g["owner_a"], g["owner_b"])].append(g)

    rivalry_matrix = []
    for (owner_a, owner_b), games in rivalry_by_pair.items():
        blowout = max(games, key=operator.itemgetter("margin"))
        closest = min(games, key=operator.itemgetter("margin"))
        rivalry_matrix.append(
            {
                "owner_guid_a": owner_a,
                "owner_guid_b": owner_b,
                "display_name_a": names.get(owner_a, "Unknown"),
                "display_name_b": names.get(owner_b, "Unknown"),
                "meetings": len(games),
                "wins_a": sum(1 for g in games if g["a_won"]),
                "wins_b": sum(1 for g in games if not g["a_won"]),
                "playoff_meetings": sum(1 for g in games if g["is_playoff"]),
                "biggest_blowout_margin": blowout["margin"],
                "biggest_blowout_season": blowout["season"],
                "biggest_blowout_week": blowout["week"],
                "closest_game_margin": closest["margin"],
                "closest_game_season": closest["season"],
                "closest_game_week": closest["week"],
            }
        )

    # ── View 4 — RecordRow: ten kinds, each a max()/min() pick over team_games
    # or standings, guarded so a missing/empty source degrades to fewer kinds
    # rather than crashing.
    records_book: list[dict[str, Any]] = []
    if team_games:
        hi = max(team_games, key=operator.itemgetter("score"))
        lo = min(team_games, key=operator.itemgetter("score"))
        records_book += [
            {
                "kind": "highest_single_week_score",
                "value": hi["score"],
                "owner_guid": hi["owner_guid"],
                "display_name": names.get(hi["owner_guid"], "Unknown"),
                "season": hi["season"],
                "week": hi["week"],
                "detail": f"vs {names.get(hi['opp_owner_guid'], 'Unknown')}",
            },
            {
                "kind": "lowest_single_week_score",
                "value": lo["score"],
                "owner_guid": lo["owner_guid"],
                "display_name": names.get(lo["owner_guid"], "Unknown"),
                "season": lo["season"],
                "week": lo["week"],
                "detail": f"vs {names.get(lo['opp_owner_guid'], 'Unknown')}",
            },
        ]
    win_rows = [tg for tg in team_games if tg["result"] == "W"]
    if win_rows:
        big = max(win_rows, key=operator.itemgetter("margin"))
        narrow = min(win_rows, key=operator.itemgetter("margin"))
        records_book += [
            {
                "kind": "largest_margin_of_victory",
                "value": big["margin"],
                "owner_guid": big["owner_guid"],
                "display_name": names.get(big["owner_guid"], "Unknown"),
                "season": big["season"],
                "week": big["week"],
                "detail": f"beat {names.get(big['opp_owner_guid'], 'Unknown')} by {big['margin']}",
            },
            {
                "kind": "narrowest_margin_of_victory",
                "value": narrow["margin"],
                "owner_guid": narrow["owner_guid"],
                "display_name": names.get(narrow["owner_guid"], "Unknown"),
                "season": narrow["season"],
                "week": narrow["week"],
                "detail": f"beat {names.get(narrow['opp_owner_guid'], 'Unknown')} by {narrow['margin']}",
            },
        ]
    played_seasons = [s for s in standings if s.wins + s.losses + s.ties > 0]
    if played_seasons:
        best = max(played_seasons, key=operator.attrgetter("win_pct_equiv"))
        worst = min(played_seasons, key=operator.attrgetter("win_pct_equiv"))
        high_pf = max(played_seasons, key=operator.attrgetter("points_for"))
        low_pf = min(played_seasons, key=operator.attrgetter("points_for"))
        records_book += [
            {
                "kind": "best_season_record",
                "value": best.win_pct_equiv,
                "owner_guid": best.primaryOwner,
                "display_name": names.get(best.primaryOwner, "Unknown"),
                "season": best.season,
                "week": None,
                "detail": f"{best.wins}-{best.losses}-{best.ties}",
            },
            {
                "kind": "worst_season_record",
                "value": worst.win_pct_equiv,
                "owner_guid": worst.primaryOwner,
                "display_name": names.get(worst.primaryOwner, "Unknown"),
                "season": worst.season,
                "week": None,
                "detail": f"{worst.wins}-{worst.losses}-{worst.ties}",
            },
            {
                "kind": "highest_season_points_for",
                "value": high_pf.points_for,
                "owner_guid": high_pf.primaryOwner,
                "display_name": names.get(high_pf.primaryOwner, "Unknown"),
                "season": high_pf.season,
                "week": None,
                "detail": f"{high_pf.points_for:.2f} points",
            },
            {
                "kind": "lowest_season_points_for",
                "value": low_pf.points_for,
                "owner_guid": low_pf.primaryOwner,
                "display_name": names.get(low_pf.primaryOwner, "Unknown"),
                "season": low_pf.season,
                "week": None,
                "detail": f"{low_pf.points_for:.2f} points",
            },
        ]
    if best_win[1] is not None:
        records_book.append(
            {
                "kind": "longest_win_streak",
                "value": best_win[0],
                "owner_guid": best_win[1],
                "display_name": names.get(best_win[1], "Unknown"),
                "season": best_win[2],
                "week": best_win[3],
                "detail": f"{best_win[0]} straight wins",
            }
        )
    if best_loss[1] is not None:
        records_book.append(
            {
                "kind": "longest_loss_streak",
                "value": best_loss[0],
                "owner_guid": best_loss[1],
                "display_name": names.get(best_loss[1], "Unknown"),
                "season": best_loss[2],
                "week": best_loss[3],
                "detail": f"{best_loss[0]} straight losses",
            }
        )

    # ── View 5 — DraftTendencyRow: Counter accumulation, three kinds.
    draft_tendencies: list[dict[str, Any]] = []
    round1_picks = [p for p in draft_picks or [] if p.roundId == 1 and p.owner_guid]
    position_counts = Counter(
        (p.owner_guid, player.position if player else "UNKNOWN")
        for p in round1_picks
        for player in [player_names.inc_dict.get(p.playerId) if player_names else None]
    )
    for (owner_guid, position), count in position_counts.items():
        draft_tendencies.append(
            {
                "kind": "round1_position_mix",
                "owner_guid": owner_guid,
                "display_name": names.get(owner_guid, "Unknown"),
                "position": position,
                "count": count,
            }
        )

    draft_counts = Counter(p.playerId for p in draft_picks) if draft_picks else Counter()
    for rank, (player_id, times_drafted) in enumerate(draft_counts.most_common(TOP_N_MOST_DRAFTED), start=1):
        player = player_names.inc_dict.get(player_id) if player_names else None
        draft_tendencies.append(
            {
                "kind": "most_drafted",
                "rank": rank,
                "player_id": player_id,
                "player_name": player.fullName if player else "Unknown",
                "position": player.position if player else "UNKNOWN",
                "times_drafted": times_drafted,
            }
        )

    for p in draft_picks or []:
        if p.overallPickNumber != 1:
            continue
        player = player_names.inc_dict.get(p.playerId) if player_names else None
        draft_tendencies.append(
            {
                "kind": "first_overall",
                "season": p.season,
                "owner_guid": p.owner_guid,
                "display_name": names.get(p.owner_guid, "Unknown"),
                "player_id": p.playerId,
                "player_name": player.fullName if player else "Unknown",
                "position": player.position if player else "UNKNOWN",
            }
        )

    # ── View 6 — SettingsRow: one literal per season, off Season's own build-time fields.
    settings_evolution = [
        {
            "season": s.season,
            "league_size": s.league_size,
            "playoff_team_count": s.playoff_team_count,
            "playoff_seeding_rule": s.playoff_seeding_rule,
            "ppr_points": s.ppr_points,
            "division_names": [d.name for d in s.settings.scheduleSettings.divisions],
            "division_count": len(s.settings.scheduleSettings.divisions),
            "roster_slots": s.roster_slots,
        }
        for s in seasons
    ]

    return {
        "FranchiseCard": franchise_cards,
        "SeasonTimelineRow": season_timeline,
        "RivalryRow": rivalry_matrix,
        "RecordRow": records_book,
        "DraftTendencyRow": draft_tendencies,
        "SettingsRow": settings_evolution,
    }
