"""ESPN Fantasy Football League History: Six-View Franchise Almanac.

Companion script for `README.md` in this directory.

A one-shot `Incorporator.fjord()` pipeline -- every ESPN season is fetched
exactly once, ever. Season discovery is a genuine two-phase dependency
(bootstrap fetch -> learn the reachable-years list -> fan out the rest) that
has to run as plain pre-fjord Python, since every `fjord()` `stream_params`
entry's `incorp_params` must be fully static before `fjord()` is called.
Everything downstream of discovery -- `Owner`/`Standing`/`Matchup`/
`DraftPick`/`TeamGame` -- becomes a network-free `payload_list=` fjord
source (one dict list built once in `main()`, never inside a loop), and
`PlayerName` is the ONE genuinely-networked fjord source: a single
`inc_url=[...]` fan-out across every discovered season, sharing one
`X-Fantasy-Filter` header carrying the union of every wanted player id.
`outflow.py` fuses all seven sources into the six views entirely READ-TIME
(`state["Peer"].inc_dict.get(key)`) -- no build-time `link_to` anywhere,
since `Standing`/`Matchup`/`DraftPick`/`TeamGame` are sibling `stream_params`
entries with no ordering guarantee between them.

A failed source never raises -- `IncorporatorList.rejects` / `.failed_sources`
plus the framework's own WARNING-level log line ARE the failure report. The
season-discovery fan-out's `.failed_sources` drives the historical-endpoint
retry (a data check, not a print); the fjord wave loop's
`if wave.failed_sources: print(...)` is the one user-facing failure line.

Two auth modes, one pipeline:
- PUBLIC (default): no cookies, demo league 899513, unauthenticated floor
  season 2020 (earlier seasons fail without cookies and are left out of the
  final season list -- no historical retry without cookies).
- PRIVATE: set `ESPN_S2` / `ESPN_SWID` (browser cookies) to unlock a
  private league and the cookie-gated pre-2018 `leagueHistory` endpoint.

Season discovery is server-declared, not brute-forced: one bootstrap fetch
of the current calendar-year season reads `status.previousSeasons` off the
response -- that IS the season list, no floor/ceiling guessing.

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

from incorporator import Incorporator, calc, calc_all, inc, register_host_penstock

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
# outflow.py's own docstring. The six DERIVED view classes are deliberately
# NOT imported here (matching examples/09-nascar-fantasy-fjord/'s own
# pattern): fjord()'s outflow= loads outflow.py through a separate cache key
# from this file's own `import outflow`, so a class imported here and the
# class flush() actually builds instances through would be two distinct,
# non-interchangeable objects -- read the export files back instead (see
# read_ndjson() below).
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
    cookie_headers,
    count_distinct_by_group,
    count_true_by_group,
    is_champion,
    is_group_max_positive,
    is_runner_up,
    make_streak_broadcast,
    mean_positive_by_group,
    position_name,
    sum_by_group,
    team_key,
    win_pct_equiv,
    win_pct_from_totals,
)


def ascii_safe(text: str) -> str:
    return text.encode("ascii", errors="replace").decode("ascii")


def read_ndjson(path: Path) -> list[dict[str, Any]]:
    """Read one just-exported view back off disk for the console board below.

    The board reads from the EXPORT FILE, not `SomeDerivedClass.inc_dict`:
    `outflow.py`'s six view classes are resolved by `fjord()`'s internal
    `load_user_module()` against a synthetic cache key distinct from this
    file's own `from outflow import (...)` -- two separate class objects,
    live-verified 2026-07-27 (export is correct; a post-loop
    `FranchiseCard.inc_dict` read here is always empty)."""
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def print_franchise_board(franchise_cards: list[dict[str, Any]]) -> None:
    ranked = sorted(franchise_cards, key=operator.itemgetter("win_pct"), reverse=True)
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


def print_records_book(records_book: list[dict[str, Any]]) -> None:
    print("\nRECORDS BOOK")
    header = f"{'KIND':<28}{'VALUE':>10}  {'FRANCHISE':<20}{'SEASON':>8}  {'DETAIL'}"
    print(header)
    print("-" * len(header))
    for row in records_book:
        name = ascii_safe(str(row["display_name"]))[:19]
        print(f"{row['kind']:<28}{row['value']:>10}  {name:<20}{row['season']!s:>8}  {ascii_safe(str(row['detail']))}")


def print_honor_roll(draft_tendencies: list[dict[str, Any]]) -> None:
    honor_roll = sorted(
        (r for r in draft_tendencies if r["kind"] == "first_overall"), key=operator.itemgetter("season")
    )
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
        "settings": calc(dict, "settings", default={}, target_type=dict),
        "previous_seasons": calc(list, "status.previousSeasons", default=[], target_type=list),
        # calc(), not inc(): the output key ("season") differs from the
        # source key ("seasonId").
        "season": calc(int, "seasonId", default=0, target_type=int),
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
    fanout_rows: Any = []
    url_to_year: dict[str, int] = {}
    if remaining_years:
        fanout_urls = [MODERN_URL.format(season=y, league_id=league_id) for y in remaining_years]
        url_to_year = dict(zip(fanout_urls, remaining_years, strict=True))
        fanout_rows = await Season.incorp(
            inc_url=fanout_urls,
            headers=auth_headers,
            params={"view": VIEWS},
            conv_dict=season_conv_dict,
        )

    # --- Historical retry (private mode only): whichever fan-out years
    # failed, refetch as ONE more fan-out against the cookie-gated
    # leagueHistory endpoint. The failed-source check drives control flow
    # (which years to retry), not a print -- see module docstring.
    failed_years = sorted(url_to_year[u] for u in fanout_rows.failed_sources) if remaining_years else []
    historical_rows: Any = []
    if has_cookies and failed_years:
        historical_urls = [f"{HISTORY_URL.format(league_id=league_id)}?seasonId={y}" for y in failed_years]
        historical_rows = await Season.incorp(
            inc_url=historical_urls,
            headers=auth_headers,
            params={"view": VIEWS},
            rec_path="0",
            conv_dict=season_conv_dict,
        )

    all_seasons = sorted([*bootstrap, *fanout_rows, *historical_rows], key=operator.attrgetter("season"))
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
    all_schedule_rows = [{**m.model_dump(by_alias=True), "season": s.season} for s in all_seasons for m in s.schedule]
    all_pick_rows = [{**p.model_dump(by_alias=True), "season": s.season} for s in all_seasons for p in s.draft_picks]

    # --- TeamGame's raw payload rows: one per team per DECIDED matchup.
    # Built off the RAW schedule dicts (Matchup hasn't been built yet -- it's
    # a sibling fjord source with no ordering guarantee), so this reads dict
    # keys (`m["home"]["totalPoints"]`), not `.home`/`.away` attributes.
    # owner_by_team_key is a composite-key -> raw-string lookup (not an
    # FK->object dict) built from Standing's own raw rows, needed only
    # because Standing and TeamGame are sibling sources seeded with no
    # build-time ordering guarantee between them.
    owner_by_team_key = {team_key(t["season"], t["id"]): t.get("primaryOwner") for t in all_team_rows}
    team_game_rows: list[dict[str, Any]] = []
    for m in all_schedule_rows:
        winner = m.get("winner", "UNDECIDED")
        if winner == "UNDECIDED":
            continue
        season = m["season"]
        home = m.get("home") or {}
        away = m.get("away")
        home_key = team_key(season, home.get("teamId"))
        away_key = team_key(season, away.get("teamId")) if away else None
        home_score = home.get("totalPoints", 0.0)
        away_score = away.get("totalPoints", 0.0) if away else 0.0
        week = m.get("matchupPeriodId", 0)
        tier = m.get("playoffTierType", "NONE")
        team_game_rows.append(
            {
                "season": season,
                "week": week,
                "tier": tier,
                "team_key": home_key,
                "owner_guid": owner_by_team_key.get(home_key),
                "opponent_team_key": away_key,
                "opponent_owner_guid": owner_by_team_key.get(away_key) if away_key else None,
                "score": home_score,
                "opponent_score": away_score,
                "result": "W" if winner == "HOME" else ("L" if winner == "AWAY" else "T"),
            }
        )
        if away:
            team_game_rows.append(
                {
                    "season": season,
                    "week": week,
                    "tier": tier,
                    "team_key": away_key,
                    "owner_guid": owner_by_team_key.get(away_key),
                    "opponent_team_key": home_key,
                    "opponent_owner_guid": owner_by_team_key.get(home_key),
                    "score": away_score,
                    "opponent_score": home_score,
                    "result": "W" if winner == "AWAY" else ("L" if winner == "HOME" else "T"),
                }
            )

    # --- Player-name wanted-id union: round-1 picks + all-time top-N most
    # drafted, ONE union set shared by every discovered season's fan-out URL.
    round1_ids = {p["playerId"] for p in all_pick_rows if p["roundId"] == 1}
    draft_counts = Counter(p["playerId"] for p in all_pick_rows)
    top_drafted_ids = {pid for pid, _times in draft_counts.most_common(TOP_N_MOST_DRAFTED)}
    wanted_ids = sorted(round1_ids | top_drafted_ids)

    player_name_conv_dict = {
        "defaultPositionId": inc(int, default=0),
        "position": calc(position_name, "defaultPositionId", default="UNKNOWN", target_type=str),
    }

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
                    "conv_dict": {
                        "id": inc(int, default=0),
                        "primaryOwner": inc(str, default=""),
                        "name": inc(str, default="Unknown"),
                        "season": inc(int, default=0),
                        "team_key": calc(team_key, "season", "id", target_type=str),
                        "division_id": calc(int, "divisionId", default=0, target_type=int),
                        "wins": calc(int, "record.overall.wins", default=0, target_type=int),
                        "losses": calc(int, "record.overall.losses", default=0, target_type=int),
                        "ties": calc(int, "record.overall.ties", default=0, target_type=int),
                        "points_for": calc(float, "record.overall.pointsFor", default=0.0, target_type=float),
                        "points_against": calc(float, "record.overall.pointsAgainst", default=0.0, target_type=float),
                        "playoff_seed": calc(int, "playoffSeed", default=0, target_type=int),
                        "final_rank": calc(int, "rankCalculatedFinal", default=0, target_type=int),
                        "win_pct_equiv": calc(win_pct_equiv, "wins", "losses", "ties", target_type=float),
                        "is_champion": calc(is_champion, "final_rank", default=False, target_type=bool),
                        "is_runner_up": calc(is_runner_up, "final_rank", default=False, target_type=bool),
                        # --- calc_all broadcasts: whole-column pass, once,
                        # across every fetched season.
                        "owner_wins_total": calc_all(sum_by_group, "primaryOwner", "wins", target_type=int),
                        "owner_losses_total": calc_all(sum_by_group, "primaryOwner", "losses", target_type=int),
                        "owner_ties_total": calc_all(sum_by_group, "primaryOwner", "ties", target_type=int),
                        "owner_points_for_total": calc_all(
                            sum_by_group, "primaryOwner", "points_for", target_type=float
                        ),
                        "owner_points_against_total": calc_all(
                            sum_by_group, "primaryOwner", "points_against", target_type=float
                        ),
                        "owner_seasons_played": calc_all(
                            count_distinct_by_group, "primaryOwner", "season", target_type=int
                        ),
                        "owner_championships": calc_all(
                            count_true_by_group, "primaryOwner", "is_champion", target_type=int
                        ),
                        "owner_runner_ups": calc_all(
                            count_true_by_group, "primaryOwner", "is_runner_up", target_type=int
                        ),
                        "owner_average_finish": calc_all(
                            mean_positive_by_group, "primaryOwner", "final_rank", target_type=float
                        ),
                        "season_is_last_place": calc_all(
                            is_group_max_positive, "season", "final_rank", target_type=bool
                        ),
                        "owner_last_places_total": calc_all(
                            count_true_by_group, "primaryOwner", "season_is_last_place", target_type=int
                        ),
                        "owner_win_pct": calc(
                            win_pct_from_totals,
                            "owner_wins_total",
                            "owner_losses_total",
                            "owner_ties_total",
                            target_type=float,
                        ),
                    },
                },
                "refresh_params": None,
            },
            {
                "cls": Matchup,
                "incorp_params": {
                    "payload_list": all_schedule_rows,
                    "conv_dict": {
                        "id": inc(int, default=0),
                        "matchupPeriodId": inc(int, default=0),
                        "playoffTierType": inc(str, default="NONE"),
                        "winner": inc(str, default="UNDECIDED"),
                        "season": inc(int, default=0),
                        "home_team_key": calc(team_key, "season", "home.teamId", default=None, target_type=str),
                        "away_team_key": calc(team_key, "season", "away.teamId", default=None, target_type=str),
                    },
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
                        "margin": calc(abs_diff, "score", "opponent_score", target_type=float),
                        "all_play_expected_wins": calc_all(
                            all_play_broadcast, "season", "team_key", "week", "score", "tier", target_type=float
                        ),
                        "longest_win_streak": calc_all(
                            make_streak_broadcast("W"), "owner_guid", "season", "week", "result", target_type=int
                        ),
                        "longest_loss_streak": calc_all(
                            make_streak_broadcast("L"), "owner_guid", "season", "week", "result", target_type=int
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
                    "conv_dict": player_name_conv_dict,
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

    franchise_cards = read_ndjson(out_dir / "franchise_cards.ndjson")
    season_timeline = read_ndjson(out_dir / "season_timeline.ndjson")
    rivalry_matrix = read_ndjson(out_dir / "rivalry_matrix.ndjson")
    records_book = read_ndjson(out_dir / "records_book.ndjson")
    draft_tendencies = read_ndjson(out_dir / "draft_tendencies.ndjson")
    settings_evolution = read_ndjson(out_dir / "settings_evolution.ndjson")

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
