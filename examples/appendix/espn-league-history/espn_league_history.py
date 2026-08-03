"""ESPN Fantasy Football League History: Six-View Franchise Almanac.

Companion script for `README.md` in this directory.

One-shot `Incorporator.fjord()` pipeline: two `Season.incorp` calls discover
every reachable season, seeding network-free Owner/Standing/Matchup/DraftPick
sources; `PlayerName` is the one genuinely-networked drill. Rollups and
records-book extremes are plain Python in `outflow.py`'s `outflow(state)`.
Two auth modes -- PUBLIC (default) or PRIVATE (`ESPN_S2`/`ESPN_SWID`
cookies) -- see README Section 1. Classes live in `outflow.py`, loaded via
`load_outflow_module` (README Section 2 explains why).

Run with:
    python examples/appendix/espn-league-history/espn_league_history.py
"""

from __future__ import annotations

import asyncio
import functools
import json
import operator
import os
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from incorporator import Incorporator, calc, pluck, register_host_penstock
from incorporator.usercode import load_outflow_module

register_host_penstock("lm-api-reads.fantasy.espn.com", rate_per_sec=1.0)

BASE = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl"
MODERN_URL = BASE + "/seasons/{season}/segments/0/leagues/{league_id}"
HISTORY_URL = BASE + "/leagueHistory/{league_id}"
VIEWS = ["mTeam", "mMatchupScore", "mSettings", "mDraftDetail"]

DEMO_LEAGUE_ID = "899513"

POSITION_MAP = {1: "QB", 2: "RB", 3: "WR", 4: "TE", 5: "K", 16: "D/ST"}

HERE = Path(__file__).resolve().parent
_outflow_path = str(HERE / "outflow.py")
_, _outflow_mod = load_outflow_module(_outflow_path)
Season, Owner, Standing, Matchup, DraftPick, PlayerName = (
    _outflow_mod.Season,
    _outflow_mod.Owner,
    _outflow_mod.Standing,
    _outflow_mod.Matchup,
    _outflow_mod.DraftPick,
    _outflow_mod.PlayerName,
)
FranchiseCard, SeasonTimelineRow, RivalryRow, RecordRow, DraftTendencyRow, SettingsRow = (
    _outflow_mod.FranchiseCard,
    _outflow_mod.SeasonTimelineRow,
    _outflow_mod.RivalryRow,
    _outflow_mod.RecordRow,
    _outflow_mod.DraftTendencyRow,
    _outflow_mod.SettingsRow,
)
TOP_N_MOST_DRAFTED = _outflow_mod.TOP_N_MOST_DRAFTED


def win_pct_equiv(wins: int, losses: int, ties: int) -> float:
    """Win percentage with each tie counted as half a win."""
    games = wins + losses + ties
    return round((wins + 0.5 * ties) / games, 2) if games else 0.0


def position_name(position_id: int) -> str:
    """Position abbreviation for a `defaultPositionId`, or `POS_<id>` if unmapped."""
    return POSITION_MAP.get(position_id, f"POS_{position_id}")


def ppr_points_from_scoring(scoring_items: list[dict[str, Any]]) -> float:
    """PPR (reception) scoring value for a season, from its scoringItems list."""
    ppr_item = next((item for item in scoring_items if item.get("statId") == 53), None)
    if ppr_item is None:
        return 0.0
    overrides = ppr_item.get("pointsOverrides") or {}
    return overrides.get("16", ppr_item.get("points", 0.0))


async def main() -> None:
    """Entry point: season discovery, then the fjord, then the console report."""
    league_id = os.environ.get("ESPN_LEAGUE_ID", DEMO_LEAGUE_ID)
    espn_s2 = os.environ.get("ESPN_S2")
    espn_swid = os.environ.get("ESPN_SWID")
    auth_headers = {"Cookie": f"espn_s2={espn_s2}; SWID={espn_swid}"} if espn_s2 and espn_swid else {}
    has_cookies = bool(auth_headers)

    out_dir = HERE / "out"
    out_dir.mkdir(exist_ok=True)

    print(f"League {league_id} -- {'private (cookies present)' if has_cookies else 'public (no cookies)'}")
    print("Discovering reachable seasons via status.previousSeasons ...")

    current_year = date.today().year

    season_conv_dict = {
        "draft_picks": pluck("draftDetail.picks"),
        "previous_seasons": pluck("status.previousSeasons"),
        "season": calc(int, "seasonId", default=0, target_type=int),
        # `default=True` covers the cookie-gated leagueHistory response, whose
        # per-season objects may omit `status` entirely -- every season it
        # carries besides the current one is already complete by construction.
        "is_complete": calc(
            operator.lt, "status.finalScoringPeriod", "status.latestScoringPeriod", default=True, target_type=bool
        ),
        "league_size": calc(len, "teams", default=0, target_type=int),
        "playoff_team_count": calc(int, "settings.scheduleSettings.playoffTeamCount", default=0, target_type=int),
        "playoff_seeding_rule": calc(str, "settings.scheduleSettings.playoffSeedingRule", default="", target_type=str),
        "ppr_points": calc(
            ppr_points_from_scoring, "settings.scoringSettings.scoringItems", default=0.0, target_type=float
        ),
        # Declared on Season (roster_slots) so infer_dynamic_schema doesn't promote this
        # digit-string-keyed dict into a mangled nested submodel.
        "roster_slots": calc(dict, "settings.rosterSettings.lineupSlotCounts", default={}, target_type=dict),
    }

    # current_season: the in-progress current-year season; also learns the
    # candidate-years list off status.previousSeasons (public branch only).
    current_season = await Season.incorp(
        inc_url=MODERN_URL.format(season=current_year, league_id=league_id),
        headers=auth_headers,
        params={"view": VIEWS},
        conv_dict=season_conv_dict,
    )
    candidate_years = set(current_season[0].previous_seasons) if current_season else set()

    # History/fan-out: private mode's leagueHistory endpoint (no seasonId) returns
    # every OTHER completed season as one list; public mode drills the modern
    # endpoint per entry of current_season's own previous_seasons field instead.
    completed_kwargs = (
        {"inc_url": HISTORY_URL.format(league_id=league_id)}
        if has_cookies
        else {
            "inc_parent": current_season,
            "inc_child": "previous_seasons",
            "inc_url": f"{BASE}/seasons/{{}}/segments/0/leagues/{league_id}",
        }
    )
    completed_seasons = await Season.incorp(
        headers=auth_headers, params={"view": VIEWS}, conv_dict=season_conv_dict, **completed_kwargs
    )

    all_seasons = sorted([*current_season, *completed_seasons], key=operator.attrgetter("season"))
    resolved_years = {s.season for s in all_seasons}
    unresolved_years = sorted((candidate_years | {current_year}) - resolved_years)
    print(f"\nFetched {len(all_seasons)} season(s): {[s.season for s in all_seasons]}")
    if unresolved_years:
        print(f"  unresolved seasons (no data available): {unresolved_years}")

    # Flatten every season's sub-collections into raw row lists -- network-free payloads.
    all_owner_rows = [{"id": o.id, "displayName": o.displayName} for s in all_seasons for o in s.members]
    deduped_owner_rows = list({o["id"]: o for o in all_owner_rows}.values())

    all_team_rows = []
    for s in all_seasons:
        for t in s.teams:
            rec = t.record.overall if t.record else None
            all_team_rows.append(
                {
                    "season": s.season,
                    "id": t.id,
                    "primaryOwner": t.primaryOwner,
                    "name": t.name,
                    "divisionId": t.divisionId,
                    "playoffSeed": t.playoffSeed,
                    "rankCalculatedFinal": t.rankCalculatedFinal,
                    "wins": rec.wins if rec else 0,
                    "losses": rec.losses if rec else 0,
                    "ties": rec.ties if rec else 0,
                    "pointsFor": rec.pointsFor if rec else 0.0,
                    "pointsAgainst": rec.pointsAgainst if rec else 0.0,
                }
            )

    owner_by_team_key_raw = {f"{r['season']}:{r['id']}": r["primaryOwner"] for r in all_team_rows}
    all_schedule_rows = [
        {
            "id": m.id,
            "season": s.season,
            "matchupPeriodId": m.matchupPeriodId,
            "playoffTierType": m.playoffTierType,
            "winner": m.winner,
            "home_owner_guid": owner_by_team_key_raw.get(f"{s.season}:{m.home.teamId}"),
            "away_owner_guid": owner_by_team_key_raw.get(f"{s.season}:{m.away.teamId}") if m.away else None,
            "home_points": m.home.totalPoints,
            "away_points": m.away.totalPoints if m.away else None,
        }
        for s in all_seasons
        for m in s.schedule
    ]
    all_pick_rows = [
        {
            "playerId": p.playerId,
            "roundId": p.roundId,
            "overallPickNumber": p.overallPickNumber,
            "season": s.season,
            "owner_guid": owner_by_team_key_raw.get(f"{s.season}:{p.teamId}"),
        }
        for s in all_seasons
        for p in s.draft_picks
        # ESPN's vacant-pick sentinel (playerId == -1) -- see README Section 7.
        if p.playerId != -1
    ]

    round1_ids = {p["playerId"] for p in all_pick_rows if p["roundId"] == 1}
    draft_counts = Counter(p["playerId"] for p in all_pick_rows)
    top_drafted_ids = {pid for pid, _times in draft_counts.most_common(TOP_N_MOST_DRAFTED)}
    wanted_ids = sorted(round1_ids | top_drafted_ids)

    print("Running one-shot fjord: Owner/Standing/Matchup/DraftPick network-free, PlayerName fanned out ...")

    async for wave in Incorporator.fjord(
        stream_params=[
            {
                "cls": Season,
                "incorp_params": {
                    "payload_list": [
                        {
                            "season": s.season,
                            "is_complete": s.is_complete,
                            "league_size": s.league_size,
                            "playoff_team_count": s.playoff_team_count,
                            "playoff_seeding_rule": s.playoff_seeding_rule,
                            "ppr_points": s.ppr_points,
                            "roster_slots": s.roster_slots,
                            "division_names": [d.name for d in s.settings.scheduleSettings.divisions],
                        }
                        for s in all_seasons
                    ],
                    "inc_code": "season",
                },
                "refresh_params": None,
            },
            {
                "cls": Owner,
                "incorp_params": {
                    "payload_list": deduped_owner_rows,
                    "inc_code": "id",
                    "inc_name": "display_name",
                    "name_chg": [("displayName", "display_name")],
                },
                "refresh_params": None,
            },
            {
                "cls": Standing,
                "incorp_params": {
                    "payload_list": all_team_rows,
                    "inc_code": "team_key",
                    "inc_name": "name",
                    "conv_dict": {
                        "team_key": calc("{}:{}".format, "season", "id", target_type=str),
                        "division_id": calc(int, "divisionId", default=0, target_type=int),
                        "wins": calc(int, "wins", default=0, target_type=int),
                        "losses": calc(int, "losses", default=0, target_type=int),
                        "ties": calc(int, "ties", default=0, target_type=int),
                        # ESPN's point totals carry floating-point summation drift;
                        # round(..., ndigits=2) via functools.partial constant-binding.
                        "points_for": calc(
                            functools.partial(round, ndigits=2),
                            "pointsFor",
                            default=0.0,
                            target_type=float,
                        ),
                        "points_against": calc(
                            functools.partial(round, ndigits=2),
                            "pointsAgainst",
                            default=0.0,
                            target_type=float,
                        ),
                        "playoff_seed": calc(int, "playoffSeed", default=0, target_type=int),
                        "final_rank": calc(int, "rankCalculatedFinal", default=0, target_type=int),
                        # Must come after wins/losses/ties/final_rank -- conv_dict order matters.
                        "win_pct_equiv": calc(win_pct_equiv, "wins", "losses", "ties", target_type=float),
                        "is_champion": calc((1).__eq__, "final_rank", target_type=bool),
                        "is_runner_up": calc((2).__eq__, "final_rank", target_type=bool),
                    },
                },
                "refresh_params": None,
            },
            {
                "cls": Matchup,
                "incorp_params": {
                    # Owner GUIDs + points are already threaded onto each row; rest auto-infers.
                    "payload_list": all_schedule_rows,
                    "inc_code": "id",
                },
                "refresh_params": None,
            },
            {
                "cls": DraftPick,
                "incorp_params": {
                    # owner_guid/season are already threaded onto each row; rest auto-infers.
                    "payload_list": all_pick_rows,
                },
                "refresh_params": None,
            },
            {
                "cls": PlayerName,
                "incorp_params": {
                    "inc_parent": all_seasons,
                    "inc_child": "season",
                    "inc_url": f"{BASE}/seasons/{{}}/players",
                    "headers": {**auth_headers, "X-Fantasy-Filter": json.dumps({"filterIds": {"value": wanted_ids}})},
                    "params": {"view": "players_wl"},
                    "inc_code": "id",
                    "inc_name": "fullName",
                    "conv_dict": {
                        "position": calc(position_name, "defaultPositionId", default="UNKNOWN", target_type=str)
                    },
                },
                "refresh_params": None,
            },
        ],
        outflow=_outflow_path,
        export_params={
            "FranchiseCard": {"file_path": str(out_dir / "franchise_cards.ndjson")},
            "SeasonTimelineRow": {"file_path": str(out_dir / "season_timeline.ndjson")},
            "RivalryRow": {"file_path": str(out_dir / "rivalry_matrix.ndjson")},
            "RecordRow": {"file_path": str(out_dir / "records_book.ndjson")},
            "DraftTendencyRow": {"file_path": str(out_dir / "draft_tendencies.ndjson")},
            "SettingsRow": {"file_path": str(out_dir / "settings_evolution.ndjson")},
        },
    ):
        if wave.failed_sources:
            print(f"WARN  {wave.operation}: {wave.failed_sources}")

    print(f"\nWrote 6 views to {out_dir}:")
    print(f"  franchise_cards.ndjson    {len(FranchiseCard.inc_dict)} rows")
    print(f"  season_timeline.ndjson    {len(SeasonTimelineRow.inc_dict)} rows")
    print(f"  rivalry_matrix.ndjson     {len(RivalryRow.inc_dict)} rows")
    print(f"  records_book.ndjson       {len(RecordRow.inc_dict)} rows")
    print(f"  draft_tendencies.ndjson   {len(DraftTendencyRow.inc_dict)} rows")
    print(f"  settings_evolution.ndjson {len(SettingsRow.inc_dict)} rows")

    ranked = sorted(FranchiseCard.inc_dict.values(), key=operator.attrgetter("win_pct"), reverse=True)
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

    print("\nRECORDS BOOK")
    header = f"{'KIND':<28}{'VALUE':>10}  {'FRANCHISE':<20}{'SEASON':>8}  {'DETAIL'}"
    print(header)
    print("-" * len(header))
    for row in RecordRow.inc_dict.values():
        name = str(row.display_name).encode("ascii", errors="replace").decode("ascii")[:19]
        detail = str(row.detail).encode("ascii", errors="replace").decode("ascii")
        print(f"{row.kind:<28}{row.value:>10}  {name:<20}{row.season!s:>8}  {detail}")

    honor_roll = sorted(
        (r for r in DraftTendencyRow.inc_dict.values() if r.kind == "first_overall"),
        key=operator.attrgetter("season"),
    )
    print("\nFIRST-OVERALL DRAFT HONOR ROLL")
    header = f"{'SEASON':<8}{'FRANCHISE':<22}{'PLAYER':<24}{'POS'}"
    print(header)
    print("-" * len(header))
    for row in honor_roll:
        franchise = str(row.display_name).encode("ascii", errors="replace").decode("ascii")[:21]
        player = str(row.player_name).encode("ascii", errors="replace").decode("ascii")[:23]
        print(f"{row.season:<8}{franchise:<22}{player:<24}{row.position}")


if __name__ == "__main__":
    asyncio.run(main())
