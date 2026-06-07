"""Integration tests for Concurrent Orchestration, React JSON Pipeline, and Null-Safe Math."""

import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from incorporator import Incorporator
from incorporator.io import fetch
from incorporator.schema.converters import inc

# ── outflow sidecar loader ──────────────────────────────────────────────────────
# Loaded via importlib with a unique sys.modules key so concurrent pytest sessions
# that also load other examples/*/outflow.py files never receive the wrong module.
_EXAMPLE_DIR = Path(__file__).resolve().parents[3] / "examples" / "09-nascar-fantasy-fjord"
_OUTFLOW_CACHE_KEY = "nascar_fantasy_outflow"
_outflow_spec = importlib.util.spec_from_file_location(_OUTFLOW_CACHE_KEY, _EXAMPLE_DIR / "outflow.py")
_nascar_outflow = importlib.util.module_from_spec(_outflow_spec)
sys.modules[_OUTFLOW_CACHE_KEY] = _nascar_outflow
_outflow_spec.loader.exec_module(_nascar_outflow)


# --- EXPLICIT SUBCLASSING ---
class Track(Incorporator):
    pass


class Driver(Incorporator):
    pass


class Race(Incorporator):
    pass


class Standing(Incorporator):
    pass


# Dedicated subclass for testing the React export pipeline
class FantasyTeam(Incorporator):
    pass


# --- MOCK NETWORK SETUP ---
async def mock_execute_get(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    if "tracks.json" in url:
        payload = {"items": [{"track_id": 1, "track_name": "Daytona International Speedway"}]}
    elif "drivers.json" in url:
        payload = {
            "response": [
                {
                    "Nascar_Driver_ID": 3989,
                    "Full_Name": "Kyle Larson",
                    "Badge": "5",
                    "Team": "Hendrick",
                },
                {"Nascar_Driver_ID": 4441, "Full_Name": "Sammy Smith", "Badge": "8", "Team": "JRM"},
                {
                    "Nascar_Driver_ID": 4235,
                    "Full_Name": "Corey Heim",
                    "Badge": "11",
                    "Team": "Tricon",
                },
            ]
        }
    elif "/1/racinginsights" in url:
        payload = [
            {
                "driver_id": 3989,
                "driver_name": "Kyle Larson",
                "points": 1050,
                "wins": 3,
                "top_10": 15,
            }
        ]
    elif "/2/racinginsights" in url:
        payload = [{"driver_id": 4441, "driver_name": "Sammy Smith", "points": 850, "top_10": 10}]
    elif "/3/racinginsights" in url:
        payload = [{"driver_id": 4235, "driver_name": "Corey Heim", "points": 920, "wins": 4, "top_10": 12}]
    else:
        payload = {}
    return httpx.Response(200, text=json.dumps(payload))


# --- TESTS ---
@pytest.mark.asyncio
async def test_nascar_react_export_pipeline(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Validates data fetching, scoring, and the complete JSON React export pipeline."""

    monkeypatch.setattr(fetch, "execute_request", mock_execute_get)
    BASE = "https://api.nascar.com"

    # ==========================================
    # 1. FETCH DATA CONCURRENTLY
    # ==========================================
    tracks, drivers = await asyncio.gather(
        Track.incorp(
            inc_url=f"{BASE}/tracks.json",
            rec_path="items",
            inc_code="track_id",
            inc_name="track_name",
        ),
        Driver.incorp(
            inc_url=f"{BASE}/drivers.json",
            rec_path="response",
            inc_code="Nascar_Driver_ID",
            inc_name="Full_Name",
        ),
    )

    cup_st, busch_st, truck_st = await asyncio.gather(
        Standing.incorp(
            inc_url=f"{BASE}/1/racinginsights-points-feed.json",
            inc_code="driver_id",
            inc_name="driver_name",
            conv_dict={
                "points": inc(int, default=0),
                "wins": inc(int, default=0),
                "top_10": inc(int, default=0),
            },
        ),
        Standing.incorp(
            inc_url=f"{BASE}/2/racinginsights-points-feed.json",
            inc_code="driver_id",
            inc_name="driver_name",
            conv_dict={
                "points": inc(int, default=0),
                "wins": inc(int, default=0),
                "top_10": inc(int, default=0),
            },
        ),
        Standing.incorp(
            inc_url=f"{BASE}/3/racinginsights-points-feed.json",
            inc_code="driver_id",
            inc_name="driver_name",
            conv_dict={
                "points": inc(int, default=0),
                "wins": inc(int, default=0),
                "top_10": inc(int, default=0),
            },
        ),
    )

    # ==========================================
    # 2. SCORING & RAW JSON GENERATION
    # ==========================================
    points_standings = {1: cup_st, 2: busch_st, 3: truck_st}

    mock_league = {"TeamAlpha": [(1, 3989), (2, 4441)], "TeamBeta": [(3, 4235)]}

    raw_react_data = []
    for team_name, roster in mock_league.items():
        total_score = 0
        for series_id, driver_id in roster:
            standing = points_standings[series_id].inc_dict.get(driver_id)
            if standing:
                total_score += getattr(standing, "points", 0)

        raw_react_data.append({"team_id": team_name, "total_score": total_score, "active_drivers": len(roster)})

    # ==========================================
    # 3. THE INCORPORATOR REACT PIPELINE
    # ==========================================
    temp_feed = tmp_path / "temp_react_feed.json"
    final_feed = tmp_path / "final_react_feed.json"

    temp_feed.write_text(json.dumps(raw_react_data))

    react_teams = await FantasyTeam.incorp(inc_file=str(temp_feed), inc_code="team_id", inc_name="team_id")

    react_teams = await FantasyTeam.refresh(instance=react_teams, new_file=str(temp_feed))

    await FantasyTeam.export(instance=react_teams, file_path=str(final_feed))

    # ==========================================
    # 4. ASSERTIONS & SORTING
    # ==========================================
    assert final_feed.exists()

    exported_payload = json.loads(final_feed.read_text())
    assert len(exported_payload) == 2

    react_teams.sort(key=lambda t: getattr(t, "total_score", 0), reverse=True)

    # FIXED: Check the actual dynamic API attribute (team_id) instead of inc_name
    assert react_teams[0].team_id == "TeamAlpha"
    assert react_teams[0].total_score == 1900

    assert react_teams[1].team_id == "TeamBeta"
    assert react_teams[1].total_score == 920

    # ==========================================
    # 5. SHOWCASE TABLE
    # ==========================================
    print("\n\n" + "=" * 80)
    print(" 🏆 TABLE 1: FANTASY LEADERBOARD (Sorted by Total Score)")
    print("=" * 80)
    print(f"{'TEAM NAME':<25} | {'TOTAL SCORE':<15} | {'ACTIVE DRIVERS'}")
    print("-" * 80)
    for team in react_teams:
        # FIXED: Check the actual dynamic API attribute (team_id) instead of inc_name
        t_name = str(getattr(team, "team_id", "Unknown"))
        t_score = getattr(team, "total_score", 0)
        t_drivers = getattr(team, "active_drivers", 0)

        print(f"{t_name:<25} | {t_score:<15} | {t_drivers}")
    print("=" * 80 + "\n")


# ── OWNER_SCORED routing tests ──────────────────────────────────────────────────


def test_owner_scored_constant() -> None:
    """OWNER_SCORED maps Kyle Busch (driver_id 454) to RCR #133 (string key).

    Proves: the constant exists, has the correct key/value types,
    and the vehicle_number is a string (not an int) so that
    CupOwnerStanding.inc_dict.get('133') uses the correct key.
    """
    OWNER_SCORED = _nascar_outflow.OWNER_SCORED

    assert 454 in OWNER_SCORED, "Kyle Busch driver_id 454 must be in OWNER_SCORED"
    vehicle_num = OWNER_SCORED[454]
    assert isinstance(vehicle_num, str), "vehicle_number must be a string, not int"
    assert vehicle_num == "133", "RCR owner entry after renumber is #133"


@pytest.mark.asyncio
async def test_outflow_owner_seat_routing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """outflow(state) routes Kyle Busch (driver_id 454) to CupOwnerStanding.

    Proves: for teams holding driver_id 454 as a Cup pick, the outflow reads
    points from CupOwnerStanding.inc_dict['133'] (237 pts) rather than
    CupStanding.inc_dict[454] (which would be missing / 0), and the
    roster row carries the '[owner seat: RCR #133]' label plus owner_seat field.
    The team total includes the 237 owner pts.  All other picks are unaffected.
    """
    monkeypatch.chdir(tmp_path)

    of_mod = _nascar_outflow

    # Build minimal mock IncorporatorList objects using MagicMock.
    # Each mock exposes .inc_dict and is iterable (for the ManufacturerLeaderboard loop).

    def _mock_list(inc_dict: dict) -> MagicMock:
        m = MagicMock()
        m.inc_dict = inc_dict
        m.__iter__ = lambda self: iter([])  # ManufacturerLeaderboard iterates cup
        return m

    # Driver registry: Kyle Busch (454) and one normal Cup driver (3989 = Kyle Larson)
    kyle_busch = MagicMock()
    kyle_busch.inc_code = 454  # driver inc_code
    kyle_busch.inc_name = "Kyle Busch"
    kyle_busch.Badge = "8"
    kyle_busch.Team = "Richard Childress Racing"
    kyle_busch.Manufacturer = "Chevrolet"
    kyle_busch.Hometown_City = "Las Vegas"
    kyle_busch.Hometown_State = "NV"

    larson = MagicMock()
    larson.inc_code = 3989
    larson.inc_name = "Kyle Larson"
    larson.Badge = "5"
    larson.Team = "Hendrick Motorsports"
    larson.Manufacturer = "Chevrolet"
    larson.Hometown_City = "Elk Grove"
    larson.Hometown_State = "CA"

    driver_reg = _mock_list({454: kyle_busch, 3989: larson})

    # CupStanding: only Larson has Cup points (Busch has no entry — he's dead)
    larson_cup = MagicMock()
    larson_cup.points = 500
    larson_cup.wins = 2
    larson_cup.top_5 = 5
    larson_cup.top_10 = 10
    larson_cup.laps_led = 200
    larson_cup.position = 3
    larson_cup.delta_leader = -100
    larson_cup.manufacturer = "Chevrolet"

    cup_reg = _mock_list({3989: larson_cup})

    # CupOwnerStanding: RCR #133 row (vehicle_number = '133')
    rcr_133 = MagicMock()
    rcr_133.points = 237
    rcr_133.wins = 0
    rcr_133.top_5 = 0
    rcr_133.top_10 = 0
    rcr_133.laps_led = 0  # owner feed doesn't carry laps_led
    rcr_133.position = 27
    rcr_133.delta_leader = -420

    owner_reg = _mock_list({"133": rcr_133})

    # LeagueRoster: one team holding both Larson and Kyle Busch as Cup picks
    pick_larson = MagicMock()
    pick_larson.series_id = 1
    pick_larson.driver_id = 3989

    pick_busch = MagicMock()
    pick_busch.series_id = 1
    pick_busch.driver_id = 454

    team = MagicMock()
    team.team_id = "AlabamaG"
    team.roster = [pick_larson, pick_busch]

    league_reg = _mock_list({})
    league_reg.__iter__ = lambda self: iter([team])

    # Race + BuschStanding + TruckStanding: minimal empty mocks (not under test here)
    empty_reg = _mock_list({})
    race_reg = _mock_list({})
    race_reg.__iter__ = lambda self: iter([])  # no races this month

    state = {
        "Driver": driver_reg,
        "Race": race_reg,
        "LeagueRoster": league_reg,
        "CupStanding": cup_reg,
        "BuschStanding": empty_reg,
        "TruckStanding": empty_reg,
        "CupOwnerStanding": owner_reg,
    }

    result = of_mod.outflow(state)

    assert "FantasyTeam" in result
    teams = result["FantasyTeam"]
    assert len(teams) == 1

    team_row = teams[0]
    assert team_row["team_id"] == "AlabamaG"

    roster = team_row["roster"]
    # Both Cup picks should appear
    assert len(roster) == 2

    # Find Kyle Busch's row
    busch_row = next((r for r in roster if "Kyle Busch" in r["name"]), None)
    assert busch_row is not None, "Kyle Busch roster row must be present"
    assert "owner seat: RCR #133" in busch_row["name"], "name must carry owner-seat label"
    assert busch_row.get("owner_seat") == "133", "owner_seat field must be '133' (string)"
    assert busch_row["points"] == 237, "owner-seat points must come from CupOwnerStanding (237)"
    assert busch_row["rank"] == 27, "rank must come from owner standings (position 27)"
    assert busch_row["laps_led"] == 0, "laps_led must be 0 for owner-seated picks"

    # Find Larson's row
    larson_row = next((r for r in roster if "Kyle Larson" in r["name"]), None)
    assert larson_row is not None, "Larson roster row must be present"
    assert "owner seat" not in larson_row["name"], "normal picks must not carry owner-seat label"
    assert larson_row.get("owner_seat") is None or "owner_seat" not in larson_row
    assert larson_row["points"] == 500, "Larson points must come from CupStanding (500)"

    # Team total must include both contributions
    assert team_row["total_score"] == 237 + 500, "team total must include owner-seat pts + Larson pts"
