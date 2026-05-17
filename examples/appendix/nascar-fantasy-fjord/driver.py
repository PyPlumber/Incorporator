"""NASCAR fantasy league as a multi-output fjord pipeline.

Six-source fjord pipeline showcasing advanced fjord capabilities via
the fjord engine.  Demonstrates three advanced fjord capabilities in
one config:

1. **State-aware inflow** — ``Race.track_id`` and
   ``Race.pole_winner_driver_id`` resolve to live ``Track`` and
   ``Driver`` instances via ``inflow(state)`` in
   ``nascar_fantasy.py`` (sibling sidecar).  Track + Driver +
   three standings classes seed in parallel; Race waits for its
   peers and gets state-wired conv_dict on every refresh wave.

2. **Multi-output outflow** — ``outflow(state)`` returns
   ``dict[ClassName, list[dict]]``; fjord builds TWO derived classes
   (``MonthlyRaceSchedule`` and ``FantasyTeam``) and writes each to
   its own file.

3. **Per-source refresh cadence via dict** — Tracks never refresh
   (``refresh_params=None``), drivers and the race schedule refresh
   slowly, standings refresh frequently.

Run with:
    python examples/appendix/nascar-fantasy-fjord/driver.py

Outputs (in ``out/`` next to this script — gitignored, inspect with any tool):
    examples/appendix/nascar-fantasy-fjord/out/nascar_monthly_schedule.ndjson
    examples/appendix/nascar-fantasy-fjord/out/nascar_fantasy_scoreboard.ndjson
    examples/appendix/nascar-fantasy-fjord/out/nascar_manufacturer_leaderboard.ndjson
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

from incorporator import Incorporator, calc

HERE = Path(__file__).resolve().parent
DATA = HERE / "out"  # examples/appendix/nascar-fantasy-fjord/out/

# Sibling sidecar import — Python only auto-adds HERE to sys.path for the
# bare ``python <script>`` invocation; explicit insert covers ``python -m``
# and other launch paths.
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from nascar_fantasy import (  # noqa: E402
    BuschStanding,
    CupStanding,
    Driver,
    LeagueRoster,
    Race,
    Track,
    TruckStanding,
)

CURRENT_YEAR = datetime.now().year
CFC_BASE = "https://cf.nascar.com/cacher"
PROD_BASE = f"https://cf.nascar.com/data/cacher/production/{CURRENT_YEAR}"
STANDINGS_BASE = "racinginsights-points-feed.json"

# Standings exclusion list — drop only the genuinely-noisy fields.
# Keep ``position``, ``top_5``, ``laps_led``, ``delta_leader``,
# ``poles``, ``starts``, ``manufacturer``, and ``playoff_eligible``:
# FantasyTeam scoring and ManufacturerLeaderboard both need them.
_STANDINGS_EXCL = [
    "is_clinch",
    "driver_first_name", "driver_last_name", "driver_suffix",
    "playoff_stage_wins",
]
_STANDINGS_CONV = {
    "points":   calc(int, default=0, target_type=int),
    "wins":     calc(int, default=0, target_type=int),
    "top_10":   calc(int, default=0, target_type=int),
    "top_5":    calc(int, default=0, target_type=int),
    "laps_led": calc(int, default=0, target_type=int),
    "position": calc(int, default=0, target_type=int),
}
# Driver exclusion list — keep ``Manufacturer``, ``Hometown_City``,
# ``Hometown_State`` (used by the enriched FantasyTeam roster).
_DRIVER_EXCL = [
    "Series_Logo", "Short_Name", "Description", "Hobbies", "Children",
    "Residing_City", "Residing_State", "Residing_Country", "Image_Transparent",
    "SecondaryImage", "Career_Stats", "Age", "Rank", "Points", "Points_Behind",
    "No_Wins", "Poles", "Top5", "Top10", "Laps_Led", "Stage_Wins",
    "Playoff_Points", "Playoff_Rank", "Integrated_Sponsor_Name",
    "Integrated_Sponsor", "Integrated_Sponsor_URL", "Silly_Season_Change",
    "Silly_Season_Change_Description", "Driver_Post_Status", "Driver_Part_Time",
]


async def main() -> None:
    print("🏁 Initiating NASCAR Data Gateway (fjord)...\n")
    DATA.mkdir(exist_ok=True)

    async for wave in Incorporator.fjord(
        stream_params=[
            # ── Static reference data — never refresh ──
            {
                "cls": Track,
                "incorp_params": {
                    "inc_url": f"{CFC_BASE}/tracks.json",
                    "rec_path": "items",
                    "inc_code": "track_id",
                    "inc_name": "track_name",
                },
                "refresh_params": None,           # tracks never change
            },
            # ── Drivers refresh occasionally ──
            {
                "cls": Driver,
                "incorp_params": {
                    "inc_url": f"{CFC_BASE}/drivers.json",
                    "rec_path": "response",
                    "inc_code": "Nascar_Driver_ID",
                    "inc_name": "Full_Name",
                    "excl_lst": _DRIVER_EXCL,
                },
                "refresh_params": None,
            },
            # ── Race schedule — depends on Track + Driver via inflow ──
            {
                "cls": Race,
                "incorp_params": {
                    "inc_url": f"{CFC_BASE}/{CURRENT_YEAR}/race_list_basic.json",
                    "rec_path": "series_1",
                    "inc_code": "race_id",
                    "inc_name": "race_name",
                    "excl_lst": ["schedule", "track_name"],
                    "name_chg": [("track_id", "track")],
                },
                "refresh_params": None,
            },
            # ── Live standings, one source per series ──
            {
                "cls": CupStanding,
                "incorp_params": {
                    "inc_url": f"{PROD_BASE}/1/{STANDINGS_BASE}",
                    "inc_code": "driver_id",
                    "inc_name": "driver_name",
                    "excl_lst": _STANDINGS_EXCL,
                    "conv_dict": _STANDINGS_CONV,
                },
                "refresh_params": None,
            },
            {
                "cls": BuschStanding,
                "incorp_params": {
                    "inc_url": f"{PROD_BASE}/2/{STANDINGS_BASE}",
                    "inc_code": "driver_id",
                    "inc_name": "driver_name",
                    "excl_lst": _STANDINGS_EXCL,
                    "conv_dict": _STANDINGS_CONV,
                },
                "refresh_params": None,
            },
            {
                "cls": TruckStanding,
                "incorp_params": {
                    "inc_url": f"{PROD_BASE}/3/{STANDINGS_BASE}",
                    "inc_code": "driver_id",
                    "inc_name": "driver_name",
                    "excl_lst": _STANDINGS_EXCL,
                    "conv_dict": _STANDINGS_CONV,
                },
                "refresh_params": None,
            },
            # ── Local-file source: the fantasy league rosters ──
            # ``inc_file=`` routes through the same handler dispatch
            # as the API sources above — JSON format is inferred from
            # the file extension.  Rosters rarely change, so refresh
            # is opted out.
            {
                "cls": LeagueRoster,
                "incorp_params": {
                    "inc_file": str(HERE / "fixtures/league_teams.json"),
                    "inc_code": "team_id",
                    "inc_name": "team_id",
                },
                "refresh_params": None,
            },
        ],

        # The state-aware inflow + outflow sidecar.
        inflow=str(HERE / "nascar_fantasy.py"),
        outflow=str(HERE / "nascar_fantasy.py"),

        # Per-class export_params — one entry per dict-key returned
        # by outflow(state).  Detection: nested dict shape = multi-output.
        export_params={
            "MonthlyRaceSchedule":     {"file_path": str(DATA / "nascar_monthly_schedule.ndjson")},
            "FantasyTeam":             {"file_path": str(DATA / "nascar_fantasy_scoreboard.ndjson")},
            "ManufacturerLeaderboard": {"file_path": str(DATA / "nascar_manufacturer_leaderboard.ndjson")},
        },

        # This is a one-shot test run — every source has
        # ``refresh_params=None`` above so no refresh daemon spawns
        # and the pipeline exits after a single outflow wave.
        #
        # For a production long-running daemon, drop the
        # ``refresh_params=None`` lines and uncomment the cadence
        # block below (per-class dict by name).
        #
        # refresh_interval={
        #     "Driver":        3600,   # 1 h
        #     "Race":          600,    # 10 min (pole finalises Sat)
        #     "CupStanding":   300,    # 5 min on race day
        #     "BuschStanding": 300,
        #     "TruckStanding": 300,
        # },
        # export_interval=60,
    ):
        op = wave.operation
        if wave.failed_sources:
            print(f"⚠️  {op:35s} chunk {wave.chunk_index}: {wave.failed_sources}")
        else:
            print(f"✅ {op:35s} chunk {wave.chunk_index}: {wave.rows_processed} rows")

    print("\n✅ Pipeline complete.")
    print(f"   • {DATA / 'nascar_monthly_schedule.ndjson'}")
    print(f"   • {DATA / 'nascar_fantasy_scoreboard.ndjson'}")
    print(f"   • {DATA / 'nascar_manufacturer_leaderboard.ndjson'}")


if __name__ == "__main__":
    asyncio.run(main())
