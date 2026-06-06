"""NASCAR fantasy league as a multi-output fjord pipeline.

Eight-source fjord pipeline showcasing advanced fjord capabilities via
the fjord engine.  Demonstrates three advanced fjord capabilities in
one config:

1. **State-aware inflow** — ``Race.track_id`` and
   ``Race.pole_winner_driver_id`` resolve to live ``Track`` and
   ``Driver`` instances via ``inflow(state)`` in
   ``inflow.py`` (sibling sidecar).  Track + Driver +
   three standings classes seed in parallel; Race waits for its
   peers and gets state-wired conv_dict on every refresh wave.

2. **Multi-output outflow** — ``outflow(state)`` returns
   ``dict[ClassName, list[dict]]``; fjord builds THREE derived classes
   (``MonthlyRaceSchedule``, ``FantasyTeam``, and
   ``ManufacturerLeaderboard``) and writes each to its own file.

3. **Per-source refresh cadence via dict** — Tracks never refresh
   (``refresh_params=None``), drivers and the race schedule refresh
   slowly, standings refresh frequently.

Run with:
    python examples/09-nascar-fantasy-fjord/nascar_fantasy.py

Outputs (in ``out/`` next to this script — gitignored, inspect with any tool):
    examples/09-nascar-fantasy-fjord/out/nascar_monthly_schedule.ndjson
    examples/09-nascar-fantasy-fjord/out/nascar_fantasy_scoreboard.ndjson
    examples/09-nascar-fantasy-fjord/out/nascar_manufacturer_leaderboard.ndjson
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

from incorporator import Incorporator, calc, inc

HERE = Path(__file__).resolve().parent
DATA = HERE / "out"  # examples/09-nascar-fantasy-fjord/out/

# Sibling sidecar import — Python only auto-adds HERE to sys.path for the
# bare ``python <script>`` invocation; explicit insert covers ``python -m``
# and other launch paths.
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from inflow import _mfg_from_logo_url  # noqa: E402
from outflow import (  # noqa: E402
    BuschStanding,
    CupOwnerStanding,
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

# Owner-standings exclusion list — drop the redundant name-component
# fields; ``owner_name`` is already the ``inc_name`` and is sufficient.
_OWNER_EXCL = ["owner_first_name", "owner_last_name", "owner_suffix"]

# Standings exclusion list — drop only the genuinely-noisy fields.
# Keep ``position``, ``top_5``, ``laps_led``, ``delta_leader``,
# ``poles``, ``starts``, ``manufacturer``, and ``playoff_eligible``:
# FantasyTeam scoring and ManufacturerLeaderboard both need them.
_STANDINGS_EXCL = [
    "is_clinch",
    "driver_first_name",
    "driver_last_name",
    "driver_suffix",
    "playoff_stage_wins",
]
# Driver exclusion list — keep ``Manufacturer``, ``Hometown_City``,
# ``Hometown_State`` (used by the enriched FantasyTeam roster).
_DRIVER_EXCL = [
    "Series_Logo",
    "Short_Name",
    "Description",
    "Hobbies",
    "Children",
    "Residing_City",
    "Residing_State",
    "Residing_Country",
    "Image_Transparent",
    "SecondaryImage",
    "Career_Stats",
    "Age",
    "Rank",
    "Points",
    "Points_Behind",
    "No_Wins",
    "Poles",
    "Top5",
    "Top10",
    "Laps_Led",
    "Stage_Wins",
    "Playoff_Points",
    "Playoff_Rank",
    "Integrated_Sponsor_Name",
    "Integrated_Sponsor",
    "Integrated_Sponsor_URL",
    "Silly_Season_Change",
    "Silly_Season_Change_Description",
    "Driver_Post_Status",
    "Driver_Part_Time",
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
                "refresh_params": None,  # tracks never change
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
                    "conv_dict": {
                        # drivers.json carries Manufacturer as a logo-image URL
                        # (e.g. 'https://.../Chevrolet_2025-330x140.png').  Parse
                        # the make name from the URL basename so that owner-seat
                        # fallback in outflow.py yields a clean text string.
                        # Empty Manufacturer fields are handled by is_garbage_value
                        # before the callable runs and land as default='Unknown'.
                        "Manufacturer": calc(_mfg_from_logo_url, "Manufacturer", default="Unknown", target_type=str),
                    },
                },
                "refresh_params": None,
            },
            # ── Race schedule — depends on Track + Driver via inflow ──
            # depends_on enables tiered-parallel seed: Track + Driver +
            # the three Standings + LeagueRoster all fire concurrently in
            # tier 0; Race fires in tier 1 once its peers' registries are
            # available for link_to() resolution.
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
                "depends_on": ["Track", "Driver"],
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
                    "conv_dict": {
                        "points": inc(int, default=0),
                        "wins": inc(int, default=0),
                        "top_10": inc(int, default=0),
                        "top_5": inc(int, default=0),
                        "laps_led": inc(int, default=0),
                        "position": inc(int, default=0),
                    },
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
                    "conv_dict": {
                        "points": inc(int, default=0),
                        "wins": inc(int, default=0),
                        "top_10": inc(int, default=0),
                        "top_5": inc(int, default=0),
                        "laps_led": inc(int, default=0),
                        "position": inc(int, default=0),
                    },
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
                    "conv_dict": {
                        "points": inc(int, default=0),
                        "wins": inc(int, default=0),
                        "top_10": inc(int, default=0),
                        "top_5": inc(int, default=0),
                        "laps_led": inc(int, default=0),
                        "position": inc(int, default=0),
                    },
                },
                "refresh_params": None,
            },
            # ── Owner-entry standings — Cup series ──
            # Keyed by vehicle_number (string: '133', '3', '33') rather
            # than owner_id because owner_id 553 repeats across all three
            # RCR entries.  Used by outflow.OWNER_SCORED to score roster
            # spots where a deceased/released Cup driver's pick is routed
            # to the team's owner-entry points instead.
            {
                "cls": CupOwnerStanding,
                "incorp_params": {
                    "inc_url": f"{CFC_BASE}/{CURRENT_YEAR}/1/final/1-owners-points.json",
                    "inc_code": "vehicle_number",
                    "inc_name": "owner_name",
                    "excl_lst": _OWNER_EXCL,
                    "conv_dict": {
                        "points": inc(int, default=0),
                        "wins": inc(int, default=0),
                        "top_5": inc(int, default=0),
                        "top_10": inc(int, default=0),
                        "starts": inc(int, default=0),
                        "position": inc(int, default=0),
                        "dnf": inc(int, default=0),
                        "winnings": inc(float, default=0),
                    },
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
        # The state-aware inflow sidecar (inflow.py) and output sidecar (outflow.py).
        inflow=str(HERE / "inflow.py"),
        outflow=str(HERE / "outflow.py"),
        # Per-class export_params — one entry per dict-key returned
        # by outflow(state).  Detection: nested dict shape = multi-output.
        export_params={
            "MonthlyRaceSchedule": {"file_path": str(DATA / "nascar_monthly_schedule.ndjson")},
            "FantasyTeam": {"file_path": str(DATA / "nascar_fantasy_scoreboard.ndjson")},
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
