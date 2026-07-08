"""Mocked end-to-end smoke test for Tutorial 6 (city_sports.py).

Loads the actual tutorial entry script via `load_sidecar` (unique importlib
key) rather than duplicating its conv_dict logic, so this test exercises the
exact shipped code path: dotted `inc_code="team.uid"` disambiguating a
numeric `team.id` collision across leagues, a single-instance `inc_parent`
roster drill, the `salary_per_year` / `turned_pro_at` derived metrics with no
`target_type=` warning spam, the MLB active-roster filter, and the
`birth_city` + `birth_state` hometown-heroes guard.
"""

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from incorporator.io import fetch
from tests.helpers import load_sidecar

_EXAMPLE_DIR = Path(__file__).resolve().parents[3] / "examples" / "06-city-sports"
city_sports = load_sidecar(_EXAMPLE_DIR / "city_sports.py", "city_sports_target")

Team = city_sports.Team
Player = city_sports.Player


def _teams_envelope(teams: list[dict[str, Any]]) -> dict[str, Any]:
    return {"sports": [{"leagues": [{"teams": teams}]}]}


TEAM_LIST_PAYLOADS: dict[str, dict[str, Any]] = {
    "football/nfl": _teams_envelope(
        [
            {
                "team": {
                    "id": "13",
                    "uid": "s:20~l:28~t:13",
                    "location": "Los Angeles",
                    "displayName": "Los Angeles Chargers",
                    "abbreviation": "LAC",
                }
            },
            {
                "team": {
                    "id": "99",
                    "uid": "s:20~l:28~t:99",
                    "location": "Dallas",
                    "displayName": "Dallas Cowboys",
                    "abbreviation": "DAL",
                }
            },
        ]
    ),
    "basketball/nba": _teams_envelope(
        [
            {
                # Numeric id collides with the NFL Chargers above -- only
                # team.uid disambiguates the two across Team.inc_dict.
                "team": {
                    "id": "13",
                    "uid": "s:40~l:46~t:13",
                    "location": "Los Angeles",
                    "displayName": "Los Angeles Lakers",
                    "abbreviation": "LAL",
                }
            },
            {
                # ESPN's one abbreviated location label across all four
                # leagues -- CITY_ALIASES must map "Los Angeles" to it or
                # this team silently vanishes from the city (verified live).
                "team": {
                    "id": "12",
                    "uid": "s:40~l:46~t:12",
                    "location": "LA",
                    "displayName": "LA Clippers",
                    "abbreviation": "LAC",
                }
            },
            # A real ESPN league lists 30+ teams; a single-team fixture would
            # trip incorp()'s len(source_list) <= 1 single-record mode and
            # hand back a bare instance instead of an IncorporatorList.
            {
                "team": {
                    "id": "88",
                    "uid": "s:40~l:46~t:88",
                    "location": "Boston",
                    "displayName": "Boston Celtics",
                    "abbreviation": "BOS",
                }
            },
        ]
    ),
    "baseball/mlb": _teams_envelope(
        [
            {
                "team": {
                    "id": "3",
                    "uid": "s:1~l:10~t:3",
                    "location": "Los Angeles",
                    "displayName": "Los Angeles Angels",
                    "abbreviation": "LAA",
                }
            },
            {
                "team": {
                    "id": "77",
                    "uid": "s:1~l:10~t:77",
                    "location": "Boston",
                    "displayName": "Boston Red Sox",
                    "abbreviation": "BOS",
                }
            },
        ]
    ),
    "hockey/nhl": _teams_envelope(
        [
            {
                "team": {
                    "id": "8",
                    "uid": "s:70~l:90~t:8",
                    "location": "Los Angeles",
                    "displayName": "Los Angeles Kings",
                    "abbreviation": "LAK",
                }
            },
            {
                "team": {
                    "id": "66",
                    "uid": "s:70~l:90~t:66",
                    "location": "Boston",
                    "displayName": "Boston Bruins",
                    "abbreviation": "BOS",
                }
            },
        ]
    ),
}

ROSTER_PAYLOADS: dict[tuple[str, str], dict[str, Any]] = {
    ("football/nfl", "13"): {
        "team": {
            "athletes": [
                {
                    "id": "p1",
                    "fullName": "Joe Alt",
                    "active": True,
                    "position": {"abbreviation": "OT"},
                    "contract": {"salary": 3809632},
                    "experience": {"years": 3},
                    "age": 23,
                    "birthPlace": {"city": "North Oaks", "state": "MN"},
                },
                {
                    # No salary, no age -- proves both stay None without raising.
                    "id": "p2",
                    "fullName": "Rookie No Age",
                    "active": True,
                    "position": {"abbreviation": "WR"},
                    "contract": {},
                    "experience": {"years": 0},
                    "birthPlace": {"city": "Somewhere", "state": "TX"},
                },
            ]
        }
    },
    ("basketball/nba", "13"): {
        "team": {
            # A real roster has 15+ athletes; a single-athlete fixture would
            # trip incorp()'s len(source_list) <= 1 single-record mode and
            # hand back a bare instance instead of an IncorporatorList.
            "athletes": [
                {
                    "id": "p3",
                    "fullName": "Luka Doncic",
                    "active": True,
                    "position": {"abbreviation": "G"},
                    "contract": {"salary": 54126450},
                    "experience": {"years": 7},
                    "age": 26,
                    "birthPlace": {"city": "Los Angeles", "state": "CA"},
                },
                {
                    "id": "p3b",
                    "fullName": "Bench Guard",
                    "active": True,
                    "position": {"abbreviation": "G"},
                    "contract": {},
                    "experience": {"years": 2},
                    "age": 24,
                    "birthPlace": {"city": "Denver", "state": "CO"},
                },
            ]
        }
    },
    ("basketball/nba", "12"): {
        "team": {
            "athletes": [
                {
                    "id": "p7",
                    "fullName": "Clipper Guard",
                    "active": True,
                    "position": {"abbreviation": "G"},
                    "contract": {"salary": 20000000},
                    "experience": {"years": 4},
                    "age": 26,
                    "birthPlace": {"city": "Toronto", "state": "ON"},
                },
                {
                    "id": "p7b",
                    "fullName": "Clipper Wing",
                    "active": True,
                    "position": {"abbreviation": "F"},
                    "contract": {},
                    "experience": {"years": 1},
                    "age": 21,
                    "birthPlace": {"city": "Phoenix", "state": "AZ"},
                },
            ]
        }
    },
    ("baseball/mlb", "3"): {
        "team": {
            # Whole-org quirk: one active roster player, one inactive
            # minor-leaguer that must not survive the active filter.
            "athletes": [
                {
                    "id": "p4",
                    "fullName": "Active Angel",
                    "active": True,
                    "position": {"abbreviation": "OF"},
                    "contract": {},
                    "experience": {"years": 5},
                    "age": 28,
                    # Same metro city NAME as the LA metro set but the WRONG
                    # state -- the birth_state guard must exclude this one.
                    "birthPlace": {"city": "Long Beach", "state": "NY"},
                },
                {
                    "id": "p5",
                    "fullName": "Minor Leaguer",
                    "active": False,
                    "position": {"abbreviation": "1B"},
                    "contract": {},
                    "experience": {"years": 1},
                    "age": 22,
                    "birthPlace": {"city": "Nowhere", "state": "TX"},
                },
            ]
        }
    },
    ("hockey/nhl", "8"): {
        "team": {
            "athletes": [
                {
                    "id": "p6",
                    "fullName": "Vet Defenseman",
                    "active": True,
                    "position": {"abbreviation": "D"},
                    "contract": {},
                    "experience": {"years": 15},
                    "age": 33,
                    "birthPlace": {"city": "Northridge", "state": "CA"},
                },
                {
                    "id": "p6b",
                    "fullName": "Young Winger",
                    "active": True,
                    "position": {"abbreviation": "W"},
                    "contract": {},
                    "experience": {"years": 1},
                    "age": 20,
                    "birthPlace": {"city": "Toronto", "state": "ON"},
                },
            ]
        }
    },
}


async def mock_espn_execute_request(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Serves the four team-list feeds, then per-(sport, id) roster drills."""
    req = httpx.Request("GET", url)
    if "enable=roster" not in url:
        for sport, payload in TEAM_LIST_PAYLOADS.items():
            if f"/sports/{sport}/teams" in url:
                return httpx.Response(200, text=json.dumps(payload), request=req)
        return httpx.Response(200, text=json.dumps(_teams_envelope([])), request=req)

    for (sport, team_id), payload in ROSTER_PAYLOADS.items():
        if f"/sports/{sport}/teams/{team_id}?enable=roster" in url:
            return httpx.Response(200, text=json.dumps(payload), request=req)
    return httpx.Response(200, text=json.dumps({"team": {"athletes": []}}), request=req)


@pytest.mark.asyncio
async def test_city_sports_discover_drill_and_filter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """Discover -> drill -> board-filter pipeline, end to end, for Los Angeles."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", mock_espn_execute_request)

    city_teams = await city_sports.discover_city_teams("Los Angeles")

    # City filter picked all five LA teams -- including the Clippers, whose
    # ESPN location is the abbreviated "LA" and only matches via CITY_ALIASES.
    # Dallas (NFL) and the Boston teams were correctly dropped.
    assert len(city_teams) == 5
    leagues_found = {league for league, _sport, _team in city_teams}
    assert leagues_found == {"NFL", "NBA", "MLB", "NHL"}
    clippers = Team.inc_dict["s:40~l:46~t:12"]
    assert clippers.inc_name == "LA Clippers"
    assert clippers.location == "LA"

    # uid disambiguates the NFL/NBA numeric id=13 collision -- both teams
    # registered distinctly in Team.inc_dict under their own uid.
    chargers = Team.inc_dict["s:20~l:28~t:13"]
    lakers = Team.inc_dict["s:40~l:46~t:13"]
    assert chargers.inc_name == "Los Angeles Chargers"
    assert lakers.inc_name == "Los Angeles Lakers"
    assert chargers.team_id == "13" == lakers.team_id
    assert chargers is not lakers

    rosters = await asyncio.gather(
        *(city_sports.drill_roster(league, sport, team) for league, sport, team in city_teams)
    )

    all_players = []
    for _league, _team, active, _failed in rosters:
        all_players.extend(active)

    # Inactive MLB org player is excluded (whole-org roster quirk).
    assert "Minor Leaguer" not in [p.inc_name for p in all_players]
    assert len(all_players) == 9  # 10 fetched across 5 rosters, 1 inactive dropped

    by_name = {p.inc_name: p for p in all_players}

    # Salary present (NFL) -> pluck + calc produce both fields.
    joe_alt = by_name["Joe Alt"]
    assert joe_alt.salary == 3809632
    assert joe_alt.tenure == 3
    assert joe_alt.turned_pro_at == 20
    assert joe_alt.salary_per_year == pytest.approx(3809632 / 3)

    # Salary present (NBA).
    doncic = by_name["Luka Doncic"]
    assert doncic.salary == 54126450
    assert doncic.salary_per_year == pytest.approx(54126450 / 7)

    # Missing age -> turned_pro_at is None, not a crash; missing salary -> None too.
    rookie = by_name["Rookie No Age"]
    assert rookie.salary is None
    assert rookie.salary_per_year is None
    assert rookie.turned_pro_at is None

    # Salary absent (MLB/NHL-style) -> both salary and salary_per_year None.
    active_angel = by_name["Active Angel"]
    assert active_angel.salary is None
    assert active_angel.salary_per_year is None

    vet = by_name["Vet Defenseman"]
    assert vet.salary is None
    assert vet.salary_per_year is None
    assert vet.tenure == 15

    # No target_type= on the derived-metric calc()s -> no coercion-failure
    # warning spam for the rows with a legitimate None output.
    assert "type coercion failed" not in caplog.text

    # Hometown filter: city+state match passes (Los Angeles CA, Northridge CA);
    # same-metro-city-NAME-but-wrong-state (Long Beach, NY) is excluded.
    state = city_sports.CITY_STATE["Los Angeles"]
    metro = city_sports.HOMETOWN_METRO["Los Angeles"]
    heroes = [p for p in all_players if p.birth_city in metro and p.birth_state == state]
    hero_names = {p.inc_name for p in heroes}
    assert hero_names == {"Luka Doncic", "Vet Defenseman"}
    assert "Active Angel" not in hero_names
