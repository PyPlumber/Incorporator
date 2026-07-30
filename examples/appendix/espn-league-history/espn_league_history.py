"""ESPN Fantasy Football League History: Six-View Franchise Almanac.

Companion script for `README.md` in this directory.

A one-shot `Incorporator.fjord()` pipeline -- every ESPN season is fetched
exactly once, ever. Season discovery is a genuine two-phase dependency
(bootstrap fetch -> learn the reachable-years list -> fan out the rest) that
runs as plain pre-fjord Python, since every `fjord()` `stream_params`
entry's `incorp_params` must be fully static before `fjord()` is called.
Everything downstream of discovery -- `Owner`/`Standing`/`Matchup`/
`DraftPick`/`TeamGame` -- becomes a network-free `payload_list=` fjord
source, and `PlayerName` is the ONE genuinely-networked fjord source: a
single `inc_url=[...]` fan-out across every discovered season, sharing one
`X-Fantasy-Filter` header carrying the union of every wanted player id.
Every row-shaped value each source's OWN `conv_dict` can compute from data it
already carries -- canonical rivalry a/b orientation on `Matchup`, rounding,
`Season`'s settings-evolution fields, `TeamGame.luck_delta` -- is computed
there, at `incorp()` time (owner GUIDs are threaded onto the raw schedule/
pick rows in this plain pre-fjord step, the same pattern `team_game_rows`
already used, so those `conv_dict`s never need a build-time `link_to`
between unordered sibling sources). `outflow.py` still fuses the seven
sources read-time (`state["Peer"].inc_dict.get(key)`) for the joins that
genuinely need it: resolving an owner GUID to `Owner.display_name`, and the
per-row-count-changing folds (rivalry pairs, records-book max/min, draft
position mix) that aren't representable as a `conv_dict` at all. No
build-time `link_to` is used anywhere -- `Standing`/`Matchup`/`DraftPick`/
`TeamGame`/`Owner` are sibling `stream_params` entries with no ordering
guarantee between them.

A failed source never raises -- `IncorporatorList.rejects` / `.failed_sources`
plus the framework's own WARNING-level log line ARE the failure report.

Two auth modes, one pipeline:
- PUBLIC (default): no cookies, demo league 899513, unauthenticated floor
  season 2020 (earlier seasons fail without cookies and are left out of the
  final season list -- public mode is modern-endpoint-only).
- PRIVATE: set `ESPN_S2` / `ESPN_SWID` (browser cookies) to unlock a
  private league and the cookie-gated `leagueHistory` endpoint, which
  serves every completed season directly.

Season discovery is server-declared, not brute-forced: one bootstrap fetch
of the current calendar-year season reads `status.previousSeasons` off the
response -- that IS the season list, no floor/ceiling guessing. Which
endpoint each remaining year fetches from is decided once, up front, from
`has_cookies` alone (see Section 4 of the README).

Run with:
    python examples/appendix/espn-league-history/espn_league_history.py
"""

from __future__ import annotations

import asyncio
import json
import operator
import os
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from incorporator import Incorporator, IncorporatorList, calc, calc_all, inc, register_host_penstock

# lm-api-reads.fantasy.espn.com has no known_host_rates() entry -- register a
# polite 1 req/sec throttle for the handful of calls this pipeline makes.
register_host_penstock("lm-api-reads.fantasy.espn.com", rate_per_sec=1.0)

BASE = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl"
MODERN_URL = BASE + "/seasons/{season}/segments/0/leagues/{league_id}"
HISTORY_URL = BASE + "/leagueHistory/{league_id}"
PLAYERS_URL = BASE + "/seasons/{season}/players"
VIEWS = ["mTeam", "mMatchupScore", "mSettings", "mDraftDetail"]

DEMO_LEAGUE_ID = "899513"

HERE = Path(__file__).resolve().parent

# Bring the SOURCE classes (the ones referenced as "cls": X below) + shared
# domain-calc helpers into scope so fjord() can register them -- see
# outflow.py's own docstring.
from outflow import (  # noqa: E402
    TOP_N_MOST_DRAFTED,
    DraftPick,
    Matchup,
    Owner,
    PlayerName,
    Season,
    Standing,
    TeamGame,
    abs_diff,
    all_play_broadcast,
    canonical_a_won,
    canonical_owner_a,
    canonical_owner_b,
    canonical_score_a,
    canonical_score_b,
    count_distinct_by_group,
    count_true_by_group,
    division_names_from_raw,
    is_champion,
    is_group_max_positive,
    is_runner_up,
    luck_delta_fn,
    make_streak_broadcast,
    mean_positive_by_group,
    perspective_result,
    position_name,
    ppr_points_from_scoring,
    roster_slots_from_raw,
    round2,
    round3,
    sum_by_group,
    team_key,
    win_pct_equiv,
    win_pct_from_totals,
)


def cookie_headers(espn_s2: str | None, espn_swid: str | None) -> dict[str, str]:
    """Hand-rolled Cookie header -- `incorp()` has no native `cookies=` kwarg."""
    if espn_s2 and espn_swid:
        return {"Cookie": f"espn_s2={espn_s2}; SWID={espn_swid}"}
    return {}


class ViewRow(Incorporator):
    pass


class RecordViewRow(Incorporator):
    """Records Book read-back needs the same declared `value` field as
    `outflow.py`'s `RecordRow`: its ten kinds share one `value` key across
    float measurements and int streak counts, and a schema inferred fresh
    from a bare class types that key from its first-sampled row only."""

    value: int | float | None = None


async def read_view(path: Path, row_cls: type[Incorporator] = ViewRow) -> IncorporatorList[Any]:
    if not path.exists():
        return IncorporatorList([])
    return await row_cls.incorp(inc_file=path)


def print_franchise_board(franchise_cards: IncorporatorList[Any]) -> None:
    ranked = sorted(franchise_cards, key=operator.attrgetter("win_pct"), reverse=True)
    print("\nFRANCHISE CARDS (all-time, sorted by win%)")
    header = f"{'FRANCHISE':<24}{'W-L-T':<12}{'WIN%':>7}{'SEASONS':>9}{'TITLES':>8}{'PLAYOFF%':>10}"
    print(header)
    print("-" * len(header))
    for row in ranked:
        name = str(row.display_name).encode("ascii", errors="replace").decode("ascii")[:23]
        record = f"{row.wins}-{row.losses}-{row.ties}"
        print(
            f"{name:<24}{record:<12}{row.win_pct:>7.3f}{row.seasons_played:>9}"
            f"{row.championships:>8}{row.playoff_rate:>10.1%}"
        )


def print_records_book(records_book: IncorporatorList[Any]) -> None:
    print("\nRECORDS BOOK")
    header = f"{'KIND':<28}{'VALUE':>10}  {'FRANCHISE':<20}{'SEASON':>8}  {'DETAIL'}"
    print(header)
    print("-" * len(header))
    for row in records_book:
        name = str(row.display_name).encode("ascii", errors="replace").decode("ascii")[:19]
        detail = str(row.detail).encode("ascii", errors="replace").decode("ascii")
        print(f"{row.kind:<28}{row.value:>10}  {name:<20}{row.season!s:>8}  {detail}")


def print_honor_roll(draft_tendencies: IncorporatorList[Any]) -> None:
    honor_roll = sorted((r for r in draft_tendencies if r.kind == "first_overall"), key=operator.attrgetter("season"))
    print("\nFIRST-OVERALL DRAFT HONOR ROLL")
    header = f"{'SEASON':<8}{'FRANCHISE':<22}{'PLAYER':<24}{'POS'}"
    print(header)
    print("-" * len(header))
    for row in honor_roll:
        franchise = str(row.display_name).encode("ascii", errors="replace").decode("ascii")[:21]
        player = str(row.player_name).encode("ascii", errors="replace").decode("ascii")[:23]
        print(f"{row.season:<8}{franchise:<22}{player:<24}{row.position}")


async def main() -> None:
    league_id = os.environ.get("ESPN_LEAGUE_ID", DEMO_LEAGUE_ID)
    espn_s2 = os.environ.get("ESPN_S2")
    espn_swid = os.environ.get("ESPN_SWID")
    auth_headers = cookie_headers(espn_s2, espn_swid)
    has_cookies = bool(auth_headers)

    out_dir = HERE / "out"
    out_dir.mkdir(exist_ok=True)

    print(f"League {league_id} -- {'private (cookies present)' if has_cookies else 'public (no cookies)'}")
    print("Discovering reachable seasons via status.previousSeasons ...")

    current_year = date.today().year

    season_conv_dict = {
        "teams": calc(list, "teams", default=[], target_type=list),
        "schedule": calc(list, "schedule", default=[], target_type=list),
        "members": calc(list, "members", default=[], target_type=list),
        "draft_picks": calc(list, "draftDetail.picks", default=[], target_type=list),
        "previous_seasons": calc(list, "status.previousSeasons", default=[], target_type=list),
        # calc(), not inc(): the output key ("season") differs from the
        # source key ("seasonId").
        "season": calc(int, "seasonId", default=0, target_type=int),
        # View 6 (Settings Evolution) build-time fields -- raw "settings.*"
        # paths still resolve against the untouched source row even with no
        # standalone "settings" conv_dict entry (apply_etl_transformations
        # only overwrites keys it's told to; the raw nested dict stays put).
        "league_size": calc(len, "teams", default=0, target_type=int),
        "playoff_team_count": calc(int, "settings.scheduleSettings.playoffTeamCount", default=0, target_type=int),
        "playoff_seeding_rule": calc(str, "settings.scheduleSettings.playoffSeedingRule", default="", target_type=str),
        "division_names": calc(
            division_names_from_raw, "settings.scheduleSettings.divisions", default=[], target_type=list
        ),
        # calc_all not needed: len() over the just-computed "division_names"
        # field, insertion-order dependent on the entry above.
        "division_count": calc(len, "division_names", default=0, target_type=int),
        # default is numeric, not [] -- an all-garbage input skips func and
        # feeds `default` straight to target_type, so a list default would
        # make round2() raise and assign a list to a numeric field.
        "ppr_points": calc(
            ppr_points_from_scoring, "settings.scoringSettings.scoringItems", default=0.0, target_type=round2
        ),
        # Declared on Season below (roster_slots) so infer_dynamic_schema
        # doesn't promote this digit-string-keyed dict into a mangled
        # nested submodel. roster_slots_from_raw (not plain dict()) because
        # this entry re-runs against the fjord's own reseeded Season rows,
        # not just the original raw ESPN payload -- see its docstring.
        "roster_slots": calc(
            roster_slots_from_raw, "settings.rosterSettings.lineupSlotCounts", default={}, target_type=dict
        ),
    }

    # --- Bootstrap: one call, learns the reachable-years list.
    bootstrap = await Season.incorp(
        inc_url=MODERN_URL.format(season=current_year, league_id=league_id),
        headers=auth_headers,
        params={"view": VIEWS},
        conv_dict=season_conv_dict,
    )
    candidate_years = set(bootstrap[0].previous_seasons) if bootstrap else set()
    remaining_years = sorted(y for y in candidate_years if y != current_year)

    # --- Fan-out: every remaining year, ONE call, concurrent on one client.
    # Endpoint choice is deterministic, decided once from has_cookies alone --
    # no probe, no retry (see README Section 4).
    fanout_rows: Any = []
    if remaining_years:
        if has_cookies:
            # leagueHistory serves every completed season once cookies unlock
            # it (live-verified 2010-2025 for this league) -- go straight there.
            fanout_urls = [f"{HISTORY_URL.format(league_id=league_id)}?seasonId={y}" for y in remaining_years]
            fanout_rows = await Season.incorp(
                inc_url=fanout_urls,
                headers=auth_headers,
                params={"view": VIEWS},
                rec_path="0",
                conv_dict=season_conv_dict,
            )
        else:
            # No cookies: leagueHistory always 401s, so public mode is
            # modern-only; a modern-endpoint miss on an old season is
            # terminal, never retried.
            fanout_urls = [MODERN_URL.format(season=y, league_id=league_id) for y in remaining_years]
            fanout_rows = await Season.incorp(
                inc_url=fanout_urls,
                headers=auth_headers,
                params={"view": VIEWS},
                conv_dict=season_conv_dict,
            )

    all_seasons = sorted([*bootstrap, *fanout_rows], key=operator.attrgetter("season"))
    resolved_years = {s.season for s in all_seasons}
    unresolved_years = sorted((candidate_years | {current_year}) - resolved_years)
    print(f"\nFetched {len(all_seasons)} season(s): {[s.season for s in all_seasons]}")
    if unresolved_years:
        print(f"  unresolved seasons (no data available): {unresolved_years}")

    # --- Flatten every season's sub-collections into raw row lists --
    # network-free payloads for the fjord's Owner/Standing/Matchup/
    # DraftPick/TeamGame sources below. ESPN repeats every member row per
    # season, so Owner's rows are deduped by id before batching.
    all_owner_rows = [o.model_dump(by_alias=True) for s in all_seasons for o in s.members]
    deduped_owner_rows = list({o["id"]: o for o in all_owner_rows}.values())
    all_team_rows = [{**t.model_dump(by_alias=True), "season": s.season} for s in all_seasons for t in s.teams]

    # Thread each team's owner onto its sibling schedule/pick rows now, while
    # still plain pre-fjord Python -- the same pattern team_game_rows already
    # uses for owner_guid below, extended to Matchup and DraftPick so their
    # own conv_dicts can build canonical a/b fields and join keys at incorp()
    # time instead of outflow() read time.
    owner_by_team_key_raw = {team_key(r["season"], r["id"]): r["primaryOwner"] for r in all_team_rows}
    all_schedule_rows = [
        {
            **m.model_dump(by_alias=True),
            "season": s.season,
            "home_owner_guid": owner_by_team_key_raw.get(team_key(s.season, m.home.teamId)),
            "away_owner_guid": owner_by_team_key_raw.get(team_key(s.season, m.away.teamId)) if m.away else None,
        }
        for s in all_seasons
        for m in s.schedule
    ]
    all_pick_rows = [
        {
            **p.model_dump(by_alias=True),
            "season": s.season,
            "owner_guid": owner_by_team_key_raw.get(team_key(s.season, p.teamId)),
        }
        for s in all_seasons
        for p in s.draft_picks
    ]

    standing_conv_dict = {
        "id": inc(int, default=0),
        "primaryOwner": inc(str, default=""),
        "name": inc(str, default="Unknown"),
        "season": inc(int, default=0),
        "team_key": calc(team_key, "season", "id", target_type=str),
        "division_id": calc(int, "divisionId", default=0, target_type=int),
        "wins": calc(int, "record.overall.wins", default=0, target_type=int),
        "losses": calc(int, "record.overall.losses", default=0, target_type=int),
        "ties": calc(int, "record.overall.ties", default=0, target_type=int),
        "points_for": calc(float, "record.overall.pointsFor", default=0.0, target_type=round2),
        "points_against": calc(float, "record.overall.pointsAgainst", default=0.0, target_type=round2),
        "playoff_seed": calc(int, "playoffSeed", default=0, target_type=int),
        "final_rank": calc(int, "rankCalculatedFinal", default=0, target_type=int),
        "win_pct_equiv": calc(win_pct_equiv, "wins", "losses", "ties", target_type=round2),
        "is_champion": calc(is_champion, "final_rank", default=False, target_type=bool),
        "is_runner_up": calc(is_runner_up, "final_rank", default=False, target_type=bool),
        # --- calc_all broadcasts: whole-column pass, once, across every
        # fetched season.
        "owner_wins_total": calc_all(sum_by_group, "primaryOwner", "wins", target_type=int),
        "owner_losses_total": calc_all(sum_by_group, "primaryOwner", "losses", target_type=int),
        "owner_ties_total": calc_all(sum_by_group, "primaryOwner", "ties", target_type=int),
        "owner_points_for_total": calc_all(sum_by_group, "primaryOwner", "points_for", target_type=round2),
        "owner_points_against_total": calc_all(sum_by_group, "primaryOwner", "points_against", target_type=round2),
        "owner_seasons_played": calc_all(count_distinct_by_group, "primaryOwner", "season", target_type=int),
        "owner_championships": calc_all(count_true_by_group, "primaryOwner", "is_champion", target_type=int),
        "owner_runner_ups": calc_all(count_true_by_group, "primaryOwner", "is_runner_up", target_type=int),
        "owner_average_finish": calc_all(mean_positive_by_group, "primaryOwner", "final_rank", target_type=round2),
        "season_is_last_place": calc_all(is_group_max_positive, "season", "final_rank", target_type=bool),
        "owner_last_places_total": calc_all(
            count_true_by_group, "primaryOwner", "season_is_last_place", target_type=int
        ),
        "owner_win_pct": calc(
            win_pct_from_totals, "owner_wins_total", "owner_losses_total", "owner_ties_total", target_type=round3
        ),
    }
    matchup_conv_dict = {
        "id": inc(int, default=0),
        "matchupPeriodId": inc(int, default=0),
        "playoffTierType": inc(str, default="NONE"),
        "winner": inc(str, default="UNDECIDED"),
        "season": inc(int, default=0),
        "home_team_key": calc(team_key, "season", "home.teamId", default=None, target_type=str),
        "away_team_key": calc(team_key, "season", "away.teamId", default=None, target_type=str),
        # Canonical a/b orientation (lower owner_guid string sorts first),
        # built once here instead of per-row inside outflow()'s rivalry fold.
        "home_owner_guid": inc(str, default=None),
        "away_owner_guid": inc(str, default=None),
        "owner_a": calc(canonical_owner_a, "home_owner_guid", "away_owner_guid", target_type=str),
        "owner_b": calc(canonical_owner_b, "home_owner_guid", "away_owner_guid", target_type=str),
        "score_a": calc(
            canonical_score_a,
            "home_owner_guid",
            "away_owner_guid",
            "home.totalPoints",
            "away.totalPoints",
            default=0.0,
            target_type=round2,
        ),
        "score_b": calc(
            canonical_score_b,
            "home_owner_guid",
            "away_owner_guid",
            "home.totalPoints",
            "away.totalPoints",
            default=0.0,
            target_type=round2,
        ),
        "a_won": calc(canonical_a_won, "home_owner_guid", "away_owner_guid", "winner", default=False, target_type=bool),
        "margin": calc(abs_diff, "score_a", "score_b", target_type=round2),
    }

    # --- Pre-fjord builds: Standing and Matchup need real typed instances
    # here so team_game_rows can traverse submodels (m.home / m.away)
    # instead of raw-dict .get() chains. Both are re-registered below as
    # payload_list= fjord sources too, so outflow(state) can read
    # state["Standing"] / state["Matchup"] -- the same two-phase pattern
    # Season itself uses.
    standings = await Standing.incorp(payload_list=all_team_rows, inc_code="team_key", conv_dict=standing_conv_dict)
    matchups = await Matchup.incorp(payload_list=all_schedule_rows, conv_dict=matchup_conv_dict)

    standing_by_team_key = {s.team_key: s for s in standings}
    team_game_rows: list[dict[str, Any]] = []
    for m in matchups:
        if m.winner == "UNDECIDED":
            continue
        for side, team, opp_team, tk, opp_tk in (
            ("home", m.home, m.away, m.home_team_key, m.away_team_key),
            ("away", m.away, m.home, m.away_team_key, m.home_team_key),
        ):
            if team is None:  # away-perspective row skipped on a playoff bye
                continue
            standing = standing_by_team_key.get(tk)
            opp_standing = standing_by_team_key.get(opp_tk) if opp_tk else None
            team_game_rows.append(
                {
                    "season": m.season,
                    "week": m.matchupPeriodId,
                    "tier": m.playoffTierType,
                    "team_key": tk,
                    "owner_guid": standing.primaryOwner if standing else None,
                    # Threaded onto the row so TeamGame's own conv_dict can
                    # build luck_delta at incorp() time instead of outflow()
                    # read time.
                    "team_wins": standing.wins if standing else 0,
                    "team_ties": standing.ties if standing else 0,
                    "opponent_team_key": opp_tk,
                    "opponent_owner_guid": opp_standing.primaryOwner if opp_standing else None,
                    "score": team.totalPoints,
                    "opponent_score": opp_team.totalPoints if opp_team else 0.0,
                    "side": side,
                    "winner": m.winner,
                }
            )

    # --- Player-name wanted-id union: round-1 picks + all-time top-N most
    # drafted, ONE union set shared by every discovered season's fan-out URL.
    round1_ids = {p["playerId"] for p in all_pick_rows if p["roundId"] == 1}
    draft_counts = Counter(p["playerId"] for p in all_pick_rows)
    top_drafted_ids = {pid for pid, _times in draft_counts.most_common(TOP_N_MOST_DRAFTED)}
    wanted_ids = sorted(round1_ids | top_drafted_ids)

    print("Running one-shot fjord: Owner/Standing/Matchup/DraftPick/TeamGame network-free, PlayerName fanned out ...")

    async for wave in Incorporator.fjord(
        stream_params=[
            {
                "cls": Season,
                "incorp_params": {
                    "payload_list": [s.model_dump(by_alias=True) for s in all_seasons],
                    "conv_dict": season_conv_dict,
                },
                "refresh_params": None,
            },
            {
                "cls": Owner,
                "incorp_params": {
                    "payload_list": deduped_owner_rows,
                    "inc_code": "id",
                    "inc_name": "display_name",
                    "conv_dict": {
                        "id": inc(str, default=""),
                        "display_name": calc(str, "displayName", default="Unknown", target_type=str),
                    },
                },
                "refresh_params": None,
            },
            {
                "cls": Standing,
                "incorp_params": {
                    "payload_list": all_team_rows,
                    "inc_code": "team_key",
                    "conv_dict": standing_conv_dict,
                },
                "refresh_params": None,
            },
            {
                "cls": Matchup,
                "incorp_params": {
                    "payload_list": all_schedule_rows,
                    "conv_dict": matchup_conv_dict,
                },
                "refresh_params": None,
            },
            {
                "cls": DraftPick,
                "incorp_params": {
                    "payload_list": all_pick_rows,
                    "conv_dict": {
                        "roundId": inc(int, default=0),
                        "roundPickNumber": inc(int, default=0),
                        "overallPickNumber": inc(int, default=0),
                        "playerId": inc(int, default=0),
                        "teamId": inc(int, default=0),
                        "keeper": inc(bool, default=False),
                        "season": inc(int, default=0),
                        "team_key": calc(team_key, "season", "teamId", target_type=str),
                        "owner_guid": inc(str, default=None),
                    },
                },
                "refresh_params": None,
            },
            {
                "cls": TeamGame,
                "incorp_params": {
                    "payload_list": team_game_rows,
                    "inc_code": "team_key",
                    "conv_dict": {
                        # calc(), not inc(): a real transform (rounding)
                        # applies even though output key == source key.
                        "score": calc(float, "score", default=0.0, target_type=round2),
                        "opponent_score": calc(float, "opponent_score", default=0.0, target_type=round2),
                        "result": calc(perspective_result, "winner", "side", default="T", target_type=str),
                        "margin": calc(abs_diff, "score", "opponent_score", target_type=round2),
                        "all_play_expected_wins": calc_all(
                            all_play_broadcast, "season", "team_key", "week", "score", "tier", target_type=round2
                        ),
                        "longest_win_streak": calc_all(
                            make_streak_broadcast("W"), "owner_guid", "season", "week", "result", target_type=int
                        ),
                        "longest_loss_streak": calc_all(
                            make_streak_broadcast("L"), "owner_guid", "season", "week", "result", target_type=int
                        ),
                        # Placed last: reads team_wins/team_ties (threaded
                        # onto the row above) + this same conv_dict's own
                        # all_play_expected_wins output.
                        "luck_delta": calc(
                            luck_delta_fn,
                            "team_wins",
                            "team_ties",
                            "all_play_expected_wins",
                            default=0.0,
                            target_type=round2,
                        ),
                    },
                },
                "refresh_params": None,
            },
            {
                "cls": PlayerName,
                "incorp_params": {
                    "inc_url": [PLAYERS_URL.format(season=s.season) for s in all_seasons],
                    "headers": {**auth_headers, "X-Fantasy-Filter": json.dumps({"filterIds": {"value": wanted_ids}})},
                    "params": {"view": "players_wl"},
                    "inc_code": "id",
                    "inc_name": "fullName",
                    "conv_dict": {
                        "defaultPositionId": inc(int, default=0),
                        "position": calc(position_name, "defaultPositionId", default="UNKNOWN", target_type=str),
                    },
                },
                "refresh_params": None,
            },
        ],
        outflow=str(HERE / "outflow.py"),
        export_params={
            "FranchiseCard": {"file_path": str(out_dir / "franchise_cards.ndjson")},
            "SeasonTimelineRow": {"file_path": str(out_dir / "season_timeline.ndjson")},
            "RivalryRow": {"file_path": str(out_dir / "rivalry_matrix.ndjson")},
            "RecordRow": {"file_path": str(out_dir / "records_book.ndjson")},
            "DraftTendencyRow": {"file_path": str(out_dir / "draft_tendencies.ndjson")},
            "SettingsRow": {"file_path": str(out_dir / "settings_evolution.ndjson")},
        },
        # Every stream_params entry above sets refresh_params=None and no
        # top-level export_interval is given -- fjord seeds once, flushes
        # outflow(state) once, and exits.
    ):
        if wave.failed_sources:
            print(f"WARN  {wave.operation}: {wave.failed_sources}")

    franchise_cards = await read_view(out_dir / "franchise_cards.ndjson")
    season_timeline = await read_view(out_dir / "season_timeline.ndjson")
    rivalry_matrix = await read_view(out_dir / "rivalry_matrix.ndjson")
    records_book = await read_view(out_dir / "records_book.ndjson", RecordViewRow)
    draft_tendencies = await read_view(out_dir / "draft_tendencies.ndjson")
    settings_evolution = await read_view(out_dir / "settings_evolution.ndjson")

    print(f"\nWrote 6 views to {out_dir}:")
    print(f"  franchise_cards.ndjson    {len(franchise_cards)} rows")
    print(f"  season_timeline.ndjson    {len(season_timeline)} rows")
    print(f"  rivalry_matrix.ndjson     {len(rivalry_matrix)} rows")
    print(f"  records_book.ndjson       {len(records_book)} rows")
    print(f"  draft_tendencies.ndjson   {len(draft_tendencies)} rows")
    print(f"  settings_evolution.ndjson {len(settings_evolution)} rows")

    print_franchise_board(franchise_cards)
    print_records_book(records_book)
    print_honor_roll(draft_tendencies)


if __name__ == "__main__":
    asyncio.run(main())
