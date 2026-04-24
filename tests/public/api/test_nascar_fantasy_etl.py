"""Integration tests for Concurrent Orchestration, React JSON Pipeline, and Null-Safe Math."""

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from incorporator import Incorporator, to_date, to_int, link_to


# --- EXPLICIT SUBCLASSING ---
class Track(Incorporator): pass


class Driver(Incorporator): pass


class Race(Incorporator): pass


class Standing(Incorporator): pass


# Dedicated subclass for testing the React export pipeline
class FantasyTeam(Incorporator): pass


# --- MOCK NETWORK SETUP ---
async def mock_execute_get(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    if "tracks.json" in url:
        payload = {"items": [{"track_id": 1, "track_name": "Daytona International Speedway"}]}
    elif "drivers.json" in url:
        payload = {
            "response": [
                {"Nascar_Driver_ID": 3989, "Full_Name": "Kyle Larson", "Badge": "5", "Team": "Hendrick"},
                {"Nascar_Driver_ID": 4441, "Full_Name": "Sammy Smith", "Badge": "8", "Team": "JRM"},
                {"Nascar_Driver_ID": 4235, "Full_Name": "Corey Heim", "Badge": "11", "Team": "Tricon"}
            ]
        }
    elif "race_list_basic.json" in url:
        payload = {"series_1": [
            {"race_id": 5333, "race_name": "Daytona 500", "track_id": 1, "pole_winner_driver_id": 3989,
             "date_scheduled": "2026-02-15T14:30:00Z"}]}
    elif "/1/racinginsights" in url:
        payload = [{"driver_id": 3989, "driver_name": "Kyle Larson", "points": 1050, "wins": 3, "top_10": 15}]
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

    monkeypatch.setattr("incorporator.methods.network._execute_get", mock_execute_get)
    BASE = "https://api.nascar.com"

    # ==========================================
    # 1. FETCH DATA CONCURRENTLY
    # ==========================================
    tracks, drivers = await asyncio.gather(
        Track.incorp(inc_url=f"{BASE}/tracks.json", rec_path="items", inc_code="track_id", inc_name="track_name"),
        Driver.incorp(inc_url=f"{BASE}/drivers.json", rec_path="response", inc_code="Nascar_Driver_ID",
                      inc_name="Full_Name")
    )

    cup_races = await Race.incorp(
        inc_url=f"{BASE}/2026/race_list_basic.json", rec_path="series_1",
        inc_code="race_id", inc_name="race_name",
        conv_dict={'track_id': link_to(tracks), 'pole_winner_driver_id': link_to(drivers), 'date_scheduled': to_date},
        name_chg=[('track_id', 'track')]
    )

    standings_conv = {'points': to_int(default=0), 'wins': to_int(default=0), 'top_10': to_int(default=0)}
    cup_st, busch_st, truck_st = await asyncio.gather(
        Standing.incorp(inc_url=f"{BASE}/1/racinginsights-points-feed.json", inc_code="driver_id",
                        inc_name="driver_name", conv_dict=standings_conv),
        Standing.incorp(inc_url=f"{BASE}/2/racinginsights-points-feed.json", inc_code="driver_id",
                        inc_name="driver_name", conv_dict=standings_conv),
        Standing.incorp(inc_url=f"{BASE}/3/racinginsights-points-feed.json", inc_code="driver_id",
                        inc_name="driver_name", conv_dict=standings_conv)
    )

    # ==========================================
    # 2. SCORING & RAW JSON GENERATION
    # ==========================================
    points_standings = {1: cup_st, 2: busch_st, 3: truck_st}

    mock_league = {
        "TeamAlpha": [(1, 3989), (2, 4441)],
        "TeamBeta": [(3, 4235)]
    }

    raw_react_data = []
    for team_name, roster in mock_league.items():
        total_score = 0
        for series_id, driver_id in roster:
            standing = points_standings[series_id].codeDict.get(driver_id)
            if standing:
                total_score += getattr(standing, "points", 0)

        raw_react_data.append({
            "team_id": team_name,
            "total_score": total_score,
            "active_drivers": len(roster)
        })

    # ==========================================
    # 3. THE INCORPORATOR REACT PIPELINE
    # ==========================================
    temp_feed = tmp_path / "temp_react_feed.json"
    final_feed = tmp_path / "final_react_feed.json"

    temp_feed.write_text(json.dumps(raw_react_data))

    react_teams = await FantasyTeam.incorp(
        inc_file=str(temp_feed),
        inc_code="team_id",
        inc_name="team_id"
    )

    react_teams = await FantasyTeam.refresh(
        instance=react_teams,
        new_file=str(temp_feed)
    )

    await FantasyTeam.export(
        instance=react_teams,
        file_path=str(final_feed)
    )

    # ==========================================
    # 4. ASSERTIONS & SORTING
    # ==========================================
    assert final_feed.exists()

    exported_payload = json.loads(final_feed.read_text())
    assert len(exported_payload) == 2

    react_teams.sort(key=lambda t: getattr(t, "total_score", 0), reverse=True)

    # FIXED: Check the actual dynamic API attribute (team_id) instead of inc_name
    assert getattr(react_teams[0], "team_id") == "TeamAlpha"
    assert getattr(react_teams[0], "total_score") == 1900

    assert getattr(react_teams[1], "team_id") == "TeamBeta"
    assert getattr(react_teams[1], "total_score") == 920

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
        t_name = str(getattr(team, 'team_id', 'Unknown'))
        t_score = getattr(team, 'total_score', 0)
        t_drivers = getattr(team, 'active_drivers', 0)

        print(f"{t_name:<25} | {t_score:<15} | {t_drivers}")
    print("=" * 80 + "\n")