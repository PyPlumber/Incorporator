"""ESPN Fantasy Football League History: Six-View Franchise Almanac.

Companion script for `README.md` in this directory.

A one-shot script (no Watershed) -- every ESPN season is fetched exactly
once, ever, and the six output views are cardinality-reducing group-bys
(per-owner rollups across N seasons, per-pair rivalry aggregation, top-N
draft counts) that `conv_dict`/`calc`/`calc_all` cannot express within a
single `incorp()` call. That reduction happens in plain Python
`build_*` helpers, called inline in `main()` right before each view's own
`Cls.incorp(payload_list=...)` + `Cls.export(...)` -- the return-twin of a
Fjord's `outflow(state)`, without a Fjord.

Two auth modes, one pipeline:
- PUBLIC (default): no cookies, demo league 899513, unauthenticated floor
  season 2020 (earlier seasons 401 without cookies and are skipped with a
  printed note).
- PRIVATE: set `ESPN_S2` / `ESPN_SWID` (browser cookies) to unlock a
  private league and the cookie-gated pre-2018 `leagueHistory` endpoint.

Season discovery is server-declared, not brute-forced: one bootstrap fetch
of the current calendar-year season reads `status.previousSeasons` off
the response and unions it with the current year -- that IS the season
list, no floor/ceiling guessing.

Run with:
    python examples/appendix/espn-league-history/espn_league_history.py
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import operator
import os
import warnings
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any

from incorporator import Incorporator, calc, inc, register_host_penstock

# lm-api-reads.fantasy.espn.com has no known_host_rates() entry -- register
# a polite 1 req/sec throttle for the ~15-20 calls this walkthrough makes.
register_host_penstock("lm-api-reads.fantasy.espn.com", rate_per_sec=1.0)

BASE = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl"
MODERN_URL = BASE + "/seasons/{season}/segments/0/leagues/{league_id}"
HISTORY_URL = BASE + "/leagueHistory/{league_id}"
PLAYERS_URL = BASE + "/seasons/{season}/players"
VIEWS = ["mTeam", "mMatchupScore", "mSettings", "mDraftDetail"]

DEMO_LEAGUE_ID = "899513"

# Standard ESPN fantasy-football defaultPositionId enumeration (live-verified
# against Davante Adams=3=WR and a D/ST row=16); unknown ids fall back to a
# labelled placeholder rather than crashing.
POSITION_MAP = {1: "QB", 2: "RB", 3: "WR", 4: "TE", 5: "K", 16: "D/ST"}

TOP_N_MOST_DRAFTED = 15


def position_name(position_id: int) -> str:
    return POSITION_MAP.get(position_id, f"POS_{position_id}")


def cookie_headers(espn_s2: str | None, espn_swid: str | None) -> dict[str, str]:
    """Hand-rolled Cookie header -- `incorp()` has no native `cookies=` kwarg."""
    if espn_s2 and espn_swid:
        return {"Cookie": f"espn_s2={espn_s2}; SWID={espn_swid}"}
    return {}


@contextlib.contextmanager
def quiet_expected_reject():
    """Suppress the framework's per-call reject `UserWarning` + WARNING-level
    log line for a season-discovery probe this loop already handles via its
    own `if not rows:` skip/fallback branch -- narrowly scoped to ONE
    incorp() call, never a blanket warnings.filterwarnings()."""
    fetch_logger = logging.getLogger("incorporator")
    prev_level = fetch_logger.level
    fetch_logger.setLevel(logging.ERROR)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            yield
    finally:
        fetch_logger.setLevel(prev_level)


class Season(Incorporator):
    """One ESPN league-season response -- modern dict-root or historical
    list-root (`rec_path="0"`), same conv_dict either way. Discarded after
    its four sub-collections (teams/schedule/members/draft_picks) and its
    settings dict are read off; never looked up via `inc_dict`."""


class Standing(Incorporator):
    """One team's season-long record, built network-free off `season.teams`
    via `payload_list=`."""


class Matchup(Incorporator):
    """One scheduled/played game, built network-free off `season.schedule`."""


class Owner(Incorporator):
    """One league member (GUID + display name), built network-free off
    `season.members`."""


class DraftPick(Incorporator):
    """One draft pick, built network-free off `season.draft_picks`."""


class PlayerName(Incorporator):
    """Resolved player name + position, batched per season via ESPN's
    `players_wl` endpoint (season-matched calls are required -- old ids
    don't resolve against modern player universes)."""


class FranchiseCard(Incorporator):
    """All-time per-franchise rollup (view 1). Bare -- `build_franchise_cards`'s
    returned dict keys are its export shape."""


class SeasonTimelineRow(Incorporator):
    """One franchise-season (view 2). Bare, no `inc_code` -- owner+season is
    a composite with no consumer that needs a lookup key."""


class RivalryRow(Incorporator):
    """One franchise pair, all-time (view 3). Bare, no `inc_code` -- the pair
    key is a plain Python tuple used only for aggregation."""


class RecordRow(Incorporator):
    """One records-book entry, one of ten kinds (view 4). Bare, no
    `inc_code` -- `kind` is a plain field, not a synthesized PK."""


class DraftTendencyRow(Incorporator):
    """One draft-tendency entry, one of three kinds (view 5). Bare, no
    `inc_code` -- heterogeneous per-kind shape, list-scanned only."""


class SettingsRow(Incorporator):
    """One season's league settings snapshot (view 6). `inc_code="season"`
    -- an existing, naturally-unique field."""


def build_franchise_cards(
    all_standings: list[Any], all_matchups: list[Any], owner_display_name: dict[str, str], season_team_owner: dict
) -> list[dict[str, Any]]:
    """All-time per-owner rollup: record, PF/PA, average finish,
    championships/runner-ups/last-places, playoff appearances/rate."""
    owners = sorted({s.primaryOwner for s in all_standings if s.primaryOwner})
    rows = []
    for owner_guid in owners:
        standings = [s for s in all_standings if s.primaryOwner == owner_guid]
        wins = sum(s.wins for s in standings)
        losses = sum(s.losses for s in standings)
        ties = sum(s.ties for s in standings)
        points_for = sum(s.points_for for s in standings)
        points_against = sum(s.points_against for s in standings)
        seasons_played = len({s.season for s in standings})

        finished = [s.final_rank for s in standings if s.final_rank > 0]
        average_finish = sum(finished) / len(finished) if finished else 0.0
        championships = sum(1 for s in standings if s.final_rank == 1)
        runner_ups = sum(1 for s in standings if s.final_rank == 2)

        last_places = 0
        for s in standings:
            season_ranks = [x.final_rank for x in all_standings if x.season == s.season and x.final_rank > 0]
            if season_ranks and s.final_rank == max(season_ranks):
                last_places += 1

        playoff_seasons = {
            m.season
            for m in all_matchups
            if m.playoffTierType == "WINNERS_BRACKET"
            and (
                season_team_owner.get((m.season, m.home_team_id)) == owner_guid
                or season_team_owner.get((m.season, m.away_team_id)) == owner_guid
            )
        }
        played = wins + losses + ties

        rows.append(
            {
                "owner_guid": owner_guid,
                "display_name": owner_display_name.get(owner_guid, "Unknown"),
                "wins": wins,
                "losses": losses,
                "ties": ties,
                "win_pct": round(wins / played, 3) if played else 0.0,
                "points_for": round(points_for, 2),
                "points_against": round(points_against, 2),
                "seasons_played": seasons_played,
                "average_finish": round(average_finish, 2),
                "championships": championships,
                "runner_ups": runner_ups,
                "last_places": last_places,
                "playoff_appearances": len(playoff_seasons),
                "playoff_rate": round(len(playoff_seasons) / seasons_played, 3) if seasons_played else 0.0,
            }
        )
    return rows


def build_season_timeline(
    all_standings: list[Any], all_matchups: list[Any], owner_display_name: dict[str, str]
) -> list[dict[str, Any]]:
    """One row per franchise-season: seed -> final rank, record, PF/PA,
    division, and an all-play expected-wins / luck delta pair. All-play
    compares each team's regular-season weekly score against every other
    team's score THAT week -- the "how many teams would I have beaten if
    I'd played everyone" expectation."""
    rows = []
    for s in all_standings:
        season_matchups = [
            m for m in all_matchups if m.season == s.season and m.playoffTierType == "NONE" and m.winner != "UNDECIDED"
        ]
        weekly_scores: dict[int, dict[int, float]] = defaultdict(dict)
        for m in season_matchups:
            weekly_scores[m.matchupPeriodId][m.home_team_id] = m.home_score
            weekly_scores[m.matchupPeriodId][m.away_team_id] = m.away_score

        all_play_wins = 0.0
        for scores in weekly_scores.values():
            if s.id not in scores:
                continue
            my_score = scores[s.id]
            others = [v for team_id, v in scores.items() if team_id != s.id]
            if not others:
                continue
            all_play_wins += sum(1 for v in others if my_score > v) + 0.5 * sum(1 for v in others if my_score == v)

        actual_win_equivalent = s.wins + 0.5 * s.ties
        rows.append(
            {
                "owner_guid": s.primaryOwner,
                "display_name": owner_display_name.get(s.primaryOwner, "Unknown"),
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
                "all_play_expected_wins": round(all_play_wins, 2),
                "luck_delta": round(actual_win_equivalent - all_play_wins, 2),
            }
        )
    return rows


def build_rivalry_matrix(
    all_matchups: list[Any], owner_display_name: dict[str, str], season_team_owner: dict
) -> list[dict[str, Any]]:
    """One row per franchise pair, all-time: W-L, meetings, playoff
    meetings, biggest blowout, closest game. Pair key is sorted
    (owner_guid_a < owner_guid_b) so each pair is counted once."""
    pairs: dict[tuple[str, str], dict[str, Any]] = {}
    for m in all_matchups:
        if m.winner == "UNDECIDED":
            continue
        owner_home = season_team_owner.get((m.season, m.home_team_id))
        owner_away = season_team_owner.get((m.season, m.away_team_id))
        if not owner_home or not owner_away or owner_home == owner_away:
            continue

        if owner_home < owner_away:
            owner_a, owner_b, score_a, score_b, a_won = (
                owner_home,
                owner_away,
                m.home_score,
                m.away_score,
                (m.winner == "HOME"),
            )
        else:
            owner_a, owner_b, score_a, score_b, a_won = (
                owner_away,
                owner_home,
                m.away_score,
                m.home_score,
                (m.winner == "AWAY"),
            )

        stat = pairs.setdefault(
            (owner_a, owner_b),
            {
                "meetings": 0,
                "wins_a": 0,
                "wins_b": 0,
                "playoff_meetings": 0,
                "biggest_blowout_margin": -1.0,
                "biggest_blowout_season": None,
                "biggest_blowout_week": None,
                "closest_game_margin": None,
                "closest_game_season": None,
                "closest_game_week": None,
            },
        )
        stat["meetings"] += 1
        stat["wins_a" if a_won else "wins_b"] += 1
        if m.playoffTierType != "NONE":
            stat["playoff_meetings"] += 1

        margin = abs(score_a - score_b)
        if margin > stat["biggest_blowout_margin"]:
            stat["biggest_blowout_margin"] = margin
            stat["biggest_blowout_season"] = m.season
            stat["biggest_blowout_week"] = m.matchupPeriodId
        if stat["closest_game_margin"] is None or margin < stat["closest_game_margin"]:
            stat["closest_game_margin"] = margin
            stat["closest_game_season"] = m.season
            stat["closest_game_week"] = m.matchupPeriodId

    rows = []
    for (owner_a, owner_b), stat in pairs.items():
        rows.append(
            {
                "owner_guid_a": owner_a,
                "display_name_a": owner_display_name.get(owner_a, "Unknown"),
                "owner_guid_b": owner_b,
                "display_name_b": owner_display_name.get(owner_b, "Unknown"),
                "meetings": stat["meetings"],
                "wins_a": stat["wins_a"],
                "wins_b": stat["wins_b"],
                "playoff_meetings": stat["playoff_meetings"],
                "biggest_blowout_margin": round(stat["biggest_blowout_margin"], 2),
                "biggest_blowout_season": stat["biggest_blowout_season"],
                "biggest_blowout_week": stat["biggest_blowout_week"],
                "closest_game_margin": round(stat["closest_game_margin"], 2),
                "closest_game_season": stat["closest_game_season"],
                "closest_game_week": stat["closest_game_week"],
            }
        )
    return rows


def build_records_book(
    all_standings: list[Any], all_matchups: list[Any], owner_display_name: dict[str, str], season_team_owner: dict
) -> list[dict[str, Any]]:
    """Ten all-time record kinds. Every kind is filtered to decided matchups
    (`winner != "UNDECIDED"`) first -- byes and in-progress weeks never
    register, but a genuine 0-point week or a tiebreak-decided tie
    (`winner` still HOME/AWAY) does."""
    decided = [m for m in all_matchups if m.winner != "UNDECIDED"]
    rows: list[dict[str, Any]] = []

    def record(
        kind: str, value: Any, owner_guid: str | None, season: int | None, week: int | None, detail: str
    ) -> None:
        rows.append(
            {
                "kind": kind,
                "value": round(value, 2) if isinstance(value, float) else value,
                "owner_guid": owner_guid,
                "display_name": owner_display_name.get(owner_guid, "Unknown") if owner_guid else "Unknown",
                "season": season,
                "week": week,
                "detail": detail,
            }
        )

    # 1-2: highest/lowest single-week score (each decided matchup contributes
    # one team-week entry per side).
    weekly_entries = []
    for m in decided:
        owner_home = season_team_owner.get((m.season, m.home_team_id))
        owner_away = season_team_owner.get((m.season, m.away_team_id))
        if owner_home:
            weekly_entries.append((m.home_score, m.season, m.matchupPeriodId, owner_home, owner_away))
        if owner_away:
            weekly_entries.append((m.away_score, m.season, m.matchupPeriodId, owner_away, owner_home))

    if weekly_entries:
        highest = max(weekly_entries, key=operator.itemgetter(0))
        record(
            "highest_single_week_score",
            highest[0],
            highest[3],
            highest[1],
            highest[2],
            f"vs {owner_display_name.get(highest[4], 'Unknown')}",
        )
        lowest = min(weekly_entries, key=operator.itemgetter(0))
        record(
            "lowest_single_week_score",
            lowest[0],
            lowest[3],
            lowest[1],
            lowest[2],
            f"vs {owner_display_name.get(lowest[4], 'Unknown')}",
        )

    # 3-4: largest/narrowest margin of victory, recorded from the winner's side.
    margin_entries = []
    for m in decided:
        owner_home = season_team_owner.get((m.season, m.home_team_id))
        owner_away = season_team_owner.get((m.season, m.away_team_id))
        winner_owner = owner_home if m.winner == "HOME" else owner_away
        loser_owner = owner_away if m.winner == "HOME" else owner_home
        if not winner_owner:
            continue
        margin_entries.append(
            (abs(m.home_score - m.away_score), m.season, m.matchupPeriodId, winner_owner, loser_owner)
        )

    if margin_entries:
        biggest = max(margin_entries, key=operator.itemgetter(0))
        record(
            "largest_margin_of_victory",
            biggest[0],
            biggest[3],
            biggest[1],
            biggest[2],
            f"beat {owner_display_name.get(biggest[4], 'Unknown')} by {round(biggest[0], 2)}",
        )
        narrowest = min(margin_entries, key=operator.itemgetter(0))
        record(
            "narrowest_margin_of_victory",
            narrowest[0],
            narrowest[3],
            narrowest[1],
            narrowest[2],
            f"beat {owner_display_name.get(narrowest[4], 'Unknown')} by {round(narrowest[0], 2)}",
        )

    # 5-8: best/worst season record, highest/lowest season points-for --
    # only seasons a franchise actually played at least one decided game.
    played_standings = [s for s in all_standings if (s.wins + s.losses + s.ties) > 0]
    if played_standings:

        def win_pct(s: Any) -> float:
            return (s.wins + 0.5 * s.ties) / (s.wins + s.losses + s.ties)

        best = max(played_standings, key=win_pct)
        record(
            "best_season_record",
            win_pct(best),
            best.primaryOwner,
            best.season,
            None,
            f"{best.wins}-{best.losses}-{best.ties}",
        )
        worst = min(played_standings, key=win_pct)
        record(
            "worst_season_record",
            win_pct(worst),
            worst.primaryOwner,
            worst.season,
            None,
            f"{worst.wins}-{worst.losses}-{worst.ties}",
        )

        highest_pf = max(played_standings, key=operator.attrgetter("points_for"))
        record(
            "highest_season_points_for",
            highest_pf.points_for,
            highest_pf.primaryOwner,
            highest_pf.season,
            None,
            f"{highest_pf.points_for:.2f} points",
        )
        lowest_pf = min(played_standings, key=operator.attrgetter("points_for"))
        record(
            "lowest_season_points_for",
            lowest_pf.points_for,
            lowest_pf.primaryOwner,
            lowest_pf.season,
            None,
            f"{lowest_pf.points_for:.2f} points",
        )

    # 9-10: longest win/loss streak -- a franchise-history streak,
    # chronological across ALL seasons, not reset per season.
    owner_games: dict[str, list[tuple[int, int, str]]] = defaultdict(list)
    for m in decided:
        owner_home = season_team_owner.get((m.season, m.home_team_id))
        owner_away = season_team_owner.get((m.season, m.away_team_id))
        if owner_home:
            owner_games[owner_home].append((m.season, m.matchupPeriodId, "W" if m.winner == "HOME" else "L"))
        if owner_away:
            owner_games[owner_away].append((m.season, m.matchupPeriodId, "W" if m.winner == "AWAY" else "L"))

    best_win_streak: tuple[int, str | None, int | None, int | None] = (0, None, None, None)
    best_loss_streak: tuple[int, str | None, int | None, int | None] = (0, None, None, None)
    for owner_guid, games in owner_games.items():
        games.sort(key=operator.itemgetter(0, 1))
        win_run = loss_run = 0
        for season, week, result in games:
            if result == "W":
                win_run += 1
                loss_run = 0
                if win_run > best_win_streak[0]:
                    best_win_streak = (win_run, owner_guid, season, week)
            else:
                loss_run += 1
                win_run = 0
                if loss_run > best_loss_streak[0]:
                    best_loss_streak = (loss_run, owner_guid, season, week)

    if best_win_streak[1] is not None:
        record(
            "longest_win_streak",
            best_win_streak[0],
            best_win_streak[1],
            best_win_streak[2],
            best_win_streak[3],
            f"{best_win_streak[0]} straight wins",
        )
    if best_loss_streak[1] is not None:
        record(
            "longest_loss_streak",
            best_loss_streak[0],
            best_loss_streak[1],
            best_loss_streak[2],
            best_loss_streak[3],
            f"{best_loss_streak[0]} straight losses",
        )

    return rows


def build_draft_tendencies(
    all_draft_picks: list[Any],
    player_names: dict[int, Any],
    owner_display_name: dict[str, str],
    season_team_owner: dict,
) -> list[dict[str, Any]]:
    """Three draft-tendency kinds: round-1 position mix per franchise,
    all-time most-drafted players, and the first-overall honor roll."""
    rows: list[dict[str, Any]] = []

    position_counts: dict[tuple[str, str], int] = defaultdict(int)
    for p in all_draft_picks:
        if p.roundId != 1:
            continue
        owner_guid = season_team_owner.get((p.season, p.teamId))
        if not owner_guid:
            continue
        player = player_names.get(p.playerId)
        position_counts[(owner_guid, position_name(player.defaultPositionId) if player else "UNKNOWN")] += 1

    for (owner_guid, position), count in position_counts.items():
        rows.append(
            {
                "kind": "round1_position_mix",
                "owner_guid": owner_guid,
                "display_name": owner_display_name.get(owner_guid, "Unknown"),
                "position": position,
                "count": count,
            }
        )

    draft_counts = Counter(p.playerId for p in all_draft_picks)
    for rank, (player_id, times_drafted) in enumerate(draft_counts.most_common(TOP_N_MOST_DRAFTED), start=1):
        player = player_names.get(player_id)
        rows.append(
            {
                "kind": "most_drafted",
                "rank": rank,
                "player_id": player_id,
                "player_name": player.fullName if player else "Unknown",
                "position": position_name(player.defaultPositionId) if player else "UNKNOWN",
                "times_drafted": times_drafted,
            }
        )

    for p in all_draft_picks:
        if p.overallPickNumber != 1:
            continue
        owner_guid = season_team_owner.get((p.season, p.teamId))
        player = player_names.get(p.playerId)
        rows.append(
            {
                "kind": "first_overall",
                "season": p.season,
                "owner_guid": owner_guid,
                "display_name": owner_display_name.get(owner_guid, "Unknown") if owner_guid else "Unknown",
                "player_id": p.playerId,
                "player_name": player.fullName if player else "Unknown",
                "position": position_name(player.defaultPositionId) if player else "UNKNOWN",
            }
        )

    return rows


def build_settings_evolution(all_seasons: list[Any]) -> list[dict[str, Any]]:
    """One row per season: league size, playoff format, PPR adoption,
    roster slots, division eras. `pointsOverrides` may be null OR the key
    entirely absent -- both are guarded."""
    rows = []
    for season in all_seasons:
        # Season's own conv_dict auto-promotes nested dicts into submodels
        # too; model_dump(by_alias=True) flattens back to plain dicts while
        # preserving lineupSlotCounts' numeric-STRING keys (by_alias=False
        # would sanitize "0"/"2"/... into "field_0"/"field_2"/...).
        settings = season.settings.model_dump(by_alias=True)
        schedule_settings = settings.get("scheduleSettings") or {}
        roster_settings = settings.get("rosterSettings") or {}
        scoring_items = (settings.get("scoringSettings") or {}).get("scoringItems") or []

        ppr_item = next((item for item in scoring_items if item.get("statId") == 53), None)
        if ppr_item is not None:
            overrides = ppr_item.get("pointsOverrides") or {}
            ppr_points = overrides.get("16", ppr_item.get("points", 0.0))
        else:
            ppr_points = 0.0

        division_names = [d.get("name", "") for d in (schedule_settings.get("divisions") or [])]

        rows.append(
            {
                "season": season.season,
                "league_size": len(season.teams),
                "playoff_team_count": schedule_settings.get("playoffTeamCount", 0),
                "playoff_seeding_rule": schedule_settings.get("playoffSeedingRule", ""),
                "ppr_points": ppr_points,
                "roster_slots": roster_settings.get("lineupSlotCounts") or {},
                "division_names": division_names,
                "division_count": len(division_names),
            }
        )
    return rows


def ascii_safe(text: str) -> str:
    return text.encode("ascii", errors="replace").decode("ascii")


def print_franchise_board(franchise_rows: list[dict[str, Any]]) -> None:
    ranked = sorted(franchise_rows, key=operator.itemgetter("win_pct"), reverse=True)
    print("\nFRANCHISE CARDS (all-time, sorted by win%)")
    header = f"{'FRANCHISE':<24}{'W-L-T':<12}{'WIN%':>7}{'SEASONS':>9}{'TITLES':>8}{'PLAYOFF%':>10}"
    print(header)
    print("-" * len(header))
    for row in ranked:
        name = ascii_safe(str(row["display_name"]))[:23]
        record = f"{row['wins']}-{row['losses']}-{row['ties']}"
        print(
            f"{name:<24}{record:<12}{row['win_pct']:>7.3f}{row['seasons_played']:>9}"
            f"{row['championships']:>8}{row['playoff_rate']:>10.1%}"
        )


def print_records_book(record_rows: list[dict[str, Any]]) -> None:
    print("\nRECORDS BOOK")
    header = f"{'KIND':<28}{'VALUE':>10}{'FRANCHISE':<20}{'SEASON':>8}{'DETAIL'}"
    print(header)
    print("-" * len(header))
    for row in record_rows:
        name = ascii_safe(str(row["display_name"]))[:19]
        season = row["season"] if row["season"] is not None else "-"
        print(f"{row['kind']:<28}{row['value']:>10}{name:<20}{season!s:>8}  {ascii_safe(str(row['detail']))}")


def print_honor_roll(tendency_rows: list[dict[str, Any]]) -> None:
    honor_roll = sorted((r for r in tendency_rows if r["kind"] == "first_overall"), key=operator.itemgetter("season"))
    print("\nFIRST-OVERALL DRAFT HONOR ROLL")
    header = f"{'SEASON':<8}{'FRANCHISE':<22}{'PLAYER':<24}{'POS'}"
    print(header)
    print("-" * len(header))
    for row in honor_roll:
        franchise = ascii_safe(str(row["display_name"]))[:21]
        player = ascii_safe(str(row["player_name"]))[:23]
        print(f"{row['season']:<8}{franchise:<22}{player:<24}{row['position']}")


async def main() -> None:
    league_id = os.environ.get("ESPN_LEAGUE_ID", DEMO_LEAGUE_ID)
    espn_s2 = os.environ.get("ESPN_S2")
    espn_swid = os.environ.get("ESPN_SWID")
    auth_headers = cookie_headers(espn_s2, espn_swid)
    has_cookies = bool(auth_headers)

    out_dir = Path(__file__).resolve().parent / "out"
    out_dir.mkdir(exist_ok=True)

    print(f"League {league_id} -- {'private (cookies present)' if has_cookies else 'public (no cookies)'}")
    print("Discovering reachable seasons via status.previousSeasons ...")

    current_year = date.today().year
    pending = [current_year]
    seen_years: set[int] = set()
    all_seasons: list[Any] = []

    while pending:
        year = pending.pop(0)
        if year in seen_years:
            continue
        seen_years.add(year)

        with quiet_expected_reject():
            rows = await Season.incorp(
                inc_url=MODERN_URL.format(season=year, league_id=league_id),
                headers=auth_headers,
                params={"view": VIEWS},
                conv_dict={
                    "teams": calc(list, "teams", default=[], target_type=list),
                    "schedule": calc(list, "schedule", default=[], target_type=list),
                    "members": calc(list, "members", default=[], target_type=list),
                    "draft_picks": calc(list, "draftDetail.picks", default=[], target_type=list),
                    "settings": calc(dict, "settings", default={}, target_type=dict),
                    "previous_seasons": calc(list, "status.previousSeasons", default=[], target_type=list),
                },
            )
        if not rows:
            # A failed fetch surfaces as an empty IncorporatorList carrying
            # structured RejectEntry records, not a raised exception. With
            # cookies present, a failed modern fetch retries against the
            # cookie-gated leagueHistory endpoint regardless of status code --
            # ESPN returns 404 (not 401) for a season outside the modern
            # endpoint's own coverage window, so the status code alone can't
            # gate the retry.
            if has_cookies:
                with quiet_expected_reject():
                    rows = await Season.incorp(
                        inc_url=HISTORY_URL.format(league_id=league_id),
                        headers=auth_headers,
                        params={"view": VIEWS, "seasonId": year},
                        rec_path="0",
                        conv_dict={
                            "teams": calc(list, "teams", default=[], target_type=list),
                            "schedule": calc(list, "schedule", default=[], target_type=list),
                            "members": calc(list, "members", default=[], target_type=list),
                            "draft_picks": calc(list, "draftDetail.picks", default=[], target_type=list),
                            "settings": calc(dict, "settings", default={}, target_type=dict),
                            "previous_seasons": calc(list, "status.previousSeasons", default=[], target_type=list),
                        },
                    )
                if not rows:
                    print(f"  season {year}: unavailable via historical endpoint too -- skipping")
                    continue
            else:
                print(f"  season {year}: unavailable (no cookies) -- skipping")
                continue

        season_row = rows[0]
        season_row.season = year
        all_seasons.append(season_row)
        print(f"  season {year}: OK ({len(season_row.teams)} teams, {len(season_row.schedule)} matchups)")

        if year == current_year:
            pending.extend(y for y in season_row.previous_seasons if y not in seen_years)

    all_seasons.sort(key=operator.attrgetter("season"))
    print(f"\nFetched {len(all_seasons)} season(s): {[s.season for s in all_seasons]}")

    all_standings: list[Any] = []
    all_matchups: list[Any] = []
    all_owners: list[Any] = []
    all_draft_picks: list[Any] = []

    for season in all_seasons:
        standings = await Standing.incorp(
            # Season's own conv_dict promoted `teams` into typed submodels
            # (auto-nested-model promotion) -- model_dump() flattens each
            # back to a plain dict for this payload-only passthrough.
            payload_list=[t.model_dump(by_alias=True) for t in season.teams],
            conv_dict={
                "id": inc(int, default=0),
                "primaryOwner": inc(str, default=""),
                "name": inc(str, default="Unknown"),
                "division_id": calc(int, "divisionId", default=0, target_type=int),
                "wins": calc(int, "record.overall.wins", default=0, target_type=int),
                "losses": calc(int, "record.overall.losses", default=0, target_type=int),
                "ties": calc(int, "record.overall.ties", default=0, target_type=int),
                "points_for": calc(float, "record.overall.pointsFor", default=0.0, target_type=float),
                "points_against": calc(float, "record.overall.pointsAgainst", default=0.0, target_type=float),
                "playoff_seed": calc(int, "playoffSeed", default=0, target_type=int),
                "final_rank": calc(int, "rankCalculatedFinal", default=0, target_type=int),
            },
        )
        for s in standings:
            s.season = season.season
        all_standings.extend(standings)

        matchups = await Matchup.incorp(
            payload_list=[m.model_dump(by_alias=True) for m in season.schedule],
            conv_dict={
                "id": inc(int, default=0),
                "matchupPeriodId": inc(int, default=0),
                "playoffTierType": inc(str, default="NONE"),
                "winner": inc(str, default="UNDECIDED"),
                "home_team_id": calc(int, "home.teamId", default=0, target_type=int),
                "home_score": calc(float, "home.totalPoints", default=0.0, target_type=float),
                "away_team_id": calc(int, "away.teamId", default=0, target_type=int),
                "away_score": calc(float, "away.totalPoints", default=0.0, target_type=float),
            },
        )
        for m in matchups:
            m.season = season.season
        all_matchups.extend(matchups)

        owners = await Owner.incorp(
            payload_list=[o.model_dump(by_alias=True) for o in season.members],
            conv_dict={
                "id": inc(str, default=""),
                "display_name": calc(str, "displayName", default="Unknown", target_type=str),
            },
        )
        all_owners.extend(owners)

        picks = await DraftPick.incorp(
            payload_list=[p.model_dump(by_alias=True) for p in season.draft_picks],
            conv_dict={
                "roundId": inc(int, default=0),
                "roundPickNumber": inc(int, default=0),
                "overallPickNumber": inc(int, default=0),
                "playerId": inc(int, default=0),
                "teamId": inc(int, default=0),
                "keeper": inc(bool, default=False),
            },
        )
        for p in picks:
            p.season = season.season
        all_draft_picks.extend(picks)

    owner_display_name: dict[str, str] = {o.id: o.display_name for o in all_owners}
    season_team_owner: dict[tuple[int, int], str] = {(s.season, s.id): s.primaryOwner for s in all_standings}

    print(
        f"Loaded {len(all_standings)} standings, {len(all_matchups)} matchups, "
        f"{len(all_owners)} owner records, {len(all_draft_picks)} draft picks."
    )

    # Player names: one batched call per season for that season's round-1
    # picks, plus a second targeted pass (grouped by most-recent drafted
    # season) for any top-N most-drafted playerId not already resolved.
    print("Resolving player names for round-1 picks + top drafted players ...")
    player_names: dict[int, Any] = {}

    for season in all_seasons:
        round1_ids = sorted({p.playerId for p in all_draft_picks if p.season == season.season and p.roundId == 1})
        missing_ids = [pid for pid in round1_ids if pid not in player_names]
        if not missing_ids:
            continue
        names = await PlayerName.incorp(
            inc_url=PLAYERS_URL.format(season=season.season),
            headers={**auth_headers, "X-Fantasy-Filter": json.dumps({"filterIds": {"value": missing_ids}})},
            params={"view": "players_wl"},
            inc_code="id",
            inc_name="fullName",
            conv_dict={"defaultPositionId": inc(int, default=0)},
        )
        for n in names:
            player_names[n.id] = n

    most_recent_season: dict[int, int] = {}
    for p in all_draft_picks:
        most_recent_season[p.playerId] = p.season

    top_drafted = Counter(p.playerId for p in all_draft_picks).most_common(TOP_N_MOST_DRAFTED)
    missing_by_season: dict[int, list[int]] = defaultdict(list)
    for player_id, _count in top_drafted:
        if player_id not in player_names:
            missing_by_season[most_recent_season[player_id]].append(player_id)

    for season_year, player_ids in missing_by_season.items():
        names = await PlayerName.incorp(
            inc_url=PLAYERS_URL.format(season=season_year),
            headers={**auth_headers, "X-Fantasy-Filter": json.dumps({"filterIds": {"value": player_ids}})},
            params={"view": "players_wl"},
            inc_code="id",
            inc_name="fullName",
            conv_dict={"defaultPositionId": inc(int, default=0)},
        )
        for n in names:
            player_names[n.id] = n

    print(f"Resolved {len(player_names)} player names.")

    # --- View 1: Franchise Cards ---
    franchise_rows = build_franchise_cards(all_standings, all_matchups, owner_display_name, season_team_owner)
    franchise_cards = await FranchiseCard.incorp(
        payload_list=franchise_rows, inc_code="owner_guid", inc_name="display_name"
    )
    await FranchiseCard.export(
        instance=franchise_cards, file_path=out_dir / "franchise_cards.ndjson", if_exists="replace"
    )

    # --- View 2: Season Timeline ---
    timeline_rows = build_season_timeline(all_standings, all_matchups, owner_display_name)
    season_timeline = await SeasonTimelineRow.incorp(payload_list=timeline_rows)
    await SeasonTimelineRow.export(
        instance=season_timeline, file_path=out_dir / "season_timeline.ndjson", if_exists="replace"
    )

    # --- View 3: Rivalry Matrix ---
    rivalry_rows = build_rivalry_matrix(all_matchups, owner_display_name, season_team_owner)
    rivalry_matrix = await RivalryRow.incorp(payload_list=rivalry_rows)
    await RivalryRow.export(instance=rivalry_matrix, file_path=out_dir / "rivalry_matrix.ndjson", if_exists="replace")

    # --- View 4: Records Book ---
    records_rows = build_records_book(all_standings, all_matchups, owner_display_name, season_team_owner)
    records_book = await RecordRow.incorp(payload_list=records_rows)
    await RecordRow.export(instance=records_book, file_path=out_dir / "records_book.ndjson", if_exists="replace")

    # --- View 5: Draft Tendencies ---
    tendency_rows = build_draft_tendencies(all_draft_picks, player_names, owner_display_name, season_team_owner)
    draft_tendencies = await DraftTendencyRow.incorp(payload_list=tendency_rows)
    await DraftTendencyRow.export(
        instance=draft_tendencies, file_path=out_dir / "draft_tendencies.ndjson", if_exists="replace"
    )

    # --- View 6: Settings Evolution ---
    settings_rows = build_settings_evolution(all_seasons)
    settings_evolution = await SettingsRow.incorp(payload_list=settings_rows, inc_code="season")
    await SettingsRow.export(
        instance=settings_evolution, file_path=out_dir / "settings_evolution.ndjson", if_exists="replace"
    )

    print(f"\nWrote 6 views to {out_dir}:")
    print(f"  franchise_cards.ndjson    {len(franchise_cards)} rows")
    print(f"  season_timeline.ndjson    {len(season_timeline)} rows")
    print(f"  rivalry_matrix.ndjson     {len(rivalry_matrix)} rows")
    print(f"  records_book.ndjson       {len(records_book)} rows")
    print(f"  draft_tendencies.ndjson   {len(draft_tendencies)} rows")
    print(f"  settings_evolution.ndjson {len(settings_evolution)} rows")

    print_franchise_board(franchise_rows)
    print_records_book(records_rows)
    print_honor_roll(tendency_rows)


if __name__ == "__main__":
    asyncio.run(main())
