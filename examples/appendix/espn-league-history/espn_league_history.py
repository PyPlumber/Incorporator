"""ESPN Fantasy Football League History: Six-View Franchise Almanac.

Companion script for `README.md` in this directory.

A one-shot `Incorporator.fjord()` pipeline -- every ESPN season is fetched
exactly once, ever. Season discovery is a genuine two-phase dependency
(bootstrap fetch -> learn the reachable-years list -> fan out the rest) that
runs as plain pre-fjord Python, since every `fjord()` `stream_params`
entry's `incorp_params` must be fully static before `fjord()` is called.
`Standing`/`Matchup`/`DraftPick` are network-free `payload_list=` fjord
sources built straight off the flattened season rows below; `PlayerName` is
the one genuinely-networked fan-out. Every rollup, rivalry stat, and
records-book extreme is plain Python inside `outflow.py`'s `outflow(state)`
-- `defaultdict` buckets, `Counter`, and `max()`/`min()` picks over the live
fjord snapshot, the same shape `examples/09-nascar-fantasy-fjord/outflow.py`
already uses.

Two auth modes, one pipeline:
- PUBLIC (default): no cookies, demo league 899513, unauthenticated floor
  season 2020 (earlier seasons fail without cookies and are left out of the
  final season list -- public mode is modern-endpoint-only).
- PRIVATE: set `ESPN_S2` / `ESPN_SWID` (browser cookies) to unlock a
  private league and the cookie-gated `leagueHistory` endpoint, which
  serves every completed season directly.

Season discovery is server-declared, not brute-forced: one bootstrap fetch
of the current calendar-year season reads `status.previousSeasons` off the
response -- that IS the season list, no floor/ceiling guessing.

This is a `cls.fjord()` daemon example -- the six source classes AND the
`outflow(state)` function are defined in `outflow.py`, and this entry file
imports them back out of it (the CLI resolves `cls_name` against the
sidecar's namespace, so this direction is load-bearing; a Watershed example
would flip it).

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

register_host_penstock("lm-api-reads.fantasy.espn.com", rate_per_sec=1.0)

BASE = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl"
MODERN_URL = BASE + "/seasons/{season}/segments/0/leagues/{league_id}"
HISTORY_URL = BASE + "/leagueHistory/{league_id}"
PLAYERS_URL = BASE + "/seasons/{season}/players"
VIEWS = ["mTeam", "mMatchupScore", "mSettings", "mDraftDetail"]

DEMO_LEAGUE_ID = "899513"

HERE = Path(__file__).resolve().parent

from outflow import (  # noqa: E402
    TOP_N_MOST_DRAFTED,
    DraftPick,
    Matchup,
    Owner,
    PlayerName,
    Season,
    Standing,
    position_name,
    ppr_points_from_scoring,
    win_pct_equiv,
)


class ViewRow(Incorporator):
    """Generic read-back class for re-reading an already-written NDJSON view -- used only
    for the console report below, not for any of the six views' own build/export shape."""


async def main() -> None:
    """Entry point -- the linear season-discovery -> fjord -> report path lives
    inline here, T9's shape (exactly one `def` at this level)."""
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
    if remaining_years:
        if has_cookies:
            fanout_urls = [f"{HISTORY_URL.format(league_id=league_id)}?seasonId={y}" for y in remaining_years]
            fanout_rows = await Season.incorp(
                inc_url=fanout_urls,
                headers=auth_headers,
                params={"view": VIEWS},
                rec_path="0",
                conv_dict=season_conv_dict,
            )
        else:
            fanout_urls = [MODERN_URL.format(season=y, league_id=league_id) for y in remaining_years]
            fanout_rows = await Season.incorp(
                inc_url=fanout_urls, headers=auth_headers, params={"view": VIEWS}, conv_dict=season_conv_dict
            )

    all_seasons = sorted([*bootstrap, *fanout_rows], key=operator.attrgetter("season"))
    resolved_years = {s.season for s in all_seasons}
    unresolved_years = sorted((candidate_years | {current_year}) - resolved_years)
    print(f"\nFetched {len(all_seasons)} season(s): {[s.season for s in all_seasons]}")
    if unresolved_years:
        print(f"  unresolved seasons (no data available): {unresolved_years}")

    # --- Flatten every season's sub-collections into raw row lists -- network-free payloads.
    all_owner_rows = [o.model_dump(by_alias=True) for s in all_seasons for o in s.members]
    deduped_owner_rows = list({o["id"]: o for o in all_owner_rows}.values())
    all_team_rows = [{**t.model_dump(by_alias=True), "season": s.season} for s in all_seasons for t in s.teams]

    owner_by_team_key_raw = {f"{r['season']}:{r['id']}": r["primaryOwner"] for r in all_team_rows}
    all_schedule_rows = [
        {
            **m.model_dump(by_alias=True),
            "season": s.season,
            "home_owner_guid": owner_by_team_key_raw.get(f"{s.season}:{m.home.teamId}"),
            "away_owner_guid": owner_by_team_key_raw.get(f"{s.season}:{m.away.teamId}") if m.away else None,
        }
        for s in all_seasons
        for m in s.schedule
    ]
    all_pick_rows = [
        {
            **p.model_dump(by_alias=True),
            "season": s.season,
            "owner_guid": owner_by_team_key_raw.get(f"{s.season}:{p.teamId}"),
        }
        for s in all_seasons
        for p in s.draft_picks
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
                    "payload_list": [s.model_dump(by_alias=True, exclude_unset=True) for s in all_seasons],
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
                        "wins": calc(int, "record.overall.wins", default=0, target_type=int),
                        "losses": calc(int, "record.overall.losses", default=0, target_type=int),
                        "ties": calc(int, "record.overall.ties", default=0, target_type=int),
                        # ESPN's own cumulative point totals carry floating-point summation drift
                        # (live-verified: e.g. 1946.3600000000001) -- round(..., ndigits=2) via the
                        # same functools.partial constant-binding form used below for is_champion.
                        "points_for": calc(
                            functools.partial(round, ndigits=2),
                            "record.overall.pointsFor",
                            default=0.0,
                            target_type=float,
                        ),
                        "points_against": calc(
                            functools.partial(round, ndigits=2),
                            "record.overall.pointsAgainst",
                            default=0.0,
                            target_type=float,
                        ),
                        "playoff_seed": calc(int, "playoffSeed", default=0, target_type=int),
                        "final_rank": calc(int, "rankCalculatedFinal", default=0, target_type=int),
                        # win_pct_equiv/is_champion/is_runner_up read wins/losses/ties/final_rank
                        # back, so they must come after those four entries in dict order.
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
                    # home_owner_guid/away_owner_guid are already threaded onto each row above;
                    # season/matchupPeriodId/playoffTierType/winner/home/away auto-infer.
                    "payload_list": all_schedule_rows,
                    "inc_code": "id",
                },
                "refresh_params": None,
            },
            {
                "cls": DraftPick,
                "incorp_params": {
                    # owner_guid/season are already threaded onto each row above;
                    # roundId/overallPickNumber/playerId/teamId/keeper auto-infer.
                    "payload_list": all_pick_rows,
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
                        "position": calc(position_name, "defaultPositionId", default="UNKNOWN", target_type=str)
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
    ):
        if wave.failed_sources:
            print(f"WARN  {wave.operation}: {wave.failed_sources}")

    franchise_cards = await ViewRow.incorp(inc_file=out_dir / "franchise_cards.ndjson")
    season_timeline = await ViewRow.incorp(inc_file=out_dir / "season_timeline.ndjson")
    rivalry_matrix = await ViewRow.incorp(inc_file=out_dir / "rivalry_matrix.ndjson")
    records_book = await ViewRow.incorp(inc_file=out_dir / "records_book.ndjson")
    draft_tendencies = await ViewRow.incorp(inc_file=out_dir / "draft_tendencies.ndjson")
    settings_evolution = await ViewRow.incorp(inc_file=out_dir / "settings_evolution.ndjson")

    print(f"\nWrote 6 views to {out_dir}:")
    print(f"  franchise_cards.ndjson    {len(franchise_cards)} rows")
    print(f"  season_timeline.ndjson    {len(season_timeline)} rows")
    print(f"  rivalry_matrix.ndjson     {len(rivalry_matrix)} rows")
    print(f"  records_book.ndjson       {len(records_book)} rows")
    print(f"  draft_tendencies.ndjson   {len(draft_tendencies)} rows")
    print(f"  settings_evolution.ndjson {len(settings_evolution)} rows")

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

    print("\nRECORDS BOOK")
    header = f"{'KIND':<28}{'VALUE':>10}  {'FRANCHISE':<20}{'SEASON':>8}  {'DETAIL'}"
    print(header)
    print("-" * len(header))
    for row in records_book:
        name = str(row.display_name).encode("ascii", errors="replace").decode("ascii")[:19]
        detail = str(row.detail).encode("ascii", errors="replace").decode("ascii")
        print(f"{row.kind:<28}{row.value:>10}  {name:<20}{row.season!s:>8}  {detail}")

    honor_roll = sorted((r for r in draft_tendencies if r.kind == "first_overall"), key=operator.attrgetter("season"))
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
