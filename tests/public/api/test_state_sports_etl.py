"""Mocked end-to-end smoke test for Tutorial 6 (state_sports.py).

Loads the actual tutorial entry script via `load_sidecar` (unique importlib
key) rather than duplicating its conv_dict logic, so this test exercises the
exact shipped code path: the T5-style whole-list `inc_parent` detail
fan-out that reads `franchise.venue.address.state`, `to_state_code`'s
full-name -> 2-letter normalization (MLB reports "California" / Canadian
MLB reports "Ontario" where NHL/NBA already say "CA" / "ON"), the
no-venue-address exclusion path, the join back to the original `Team`
instance for the roster drill, the `salary_per_year` / `turned_pro_at`
derived metrics with no `target_type=` warning spam, the MLB active-roster
filter, and the `birth_state` homegrown-board equality filter.
"""

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from incorporator import IncorporatorList
from incorporator.io import fetch
from tests.helpers import load_sidecar

_EXAMPLE_DIR = Path(__file__).resolve().parents[3] / "examples" / "06-state-sports"
state_sports = load_sidecar(_EXAMPLE_DIR / "state_sports.py", "state_sports_target")

Team = state_sports.Team
TeamDetail = state_sports.TeamDetail
Player = state_sports.Player


def _teams_envelope(teams: list[dict[str, Any]]) -> dict[str, Any]:
    return {"sports": [{"leagues": [{"teams": teams}]}]}


def _team_entry(team_id: str, uid: str, display_name: str, abbreviation: str) -> dict[str, Any]:
    return {
        "team": {
            "id": team_id,
            "uid": uid,
            "location": display_name.rsplit(" ", 1)[0],
            "displayName": display_name,
            "abbreviation": abbreviation,
        }
    }


TEAM_LIST_PAYLOADS: dict[str, dict[str, Any]] = {
    "football/nfl": _teams_envelope(
        [
            _team_entry("13", "s:20~l:28~t:13", "Los Angeles Chargers", "LAC"),
            _team_entry("99", "s:20~l:28~t:99", "Dallas Cowboys", "DAL"),
            # No franchise/venue in its detail payload -- proves the
            # no-venue exclusion path and its WARN counter.
            _team_entry("50", "s:20~l:28~t:50", "Ghost Team", "GHT"),
        ]
    ),
    "basketball/nba": _teams_envelope(
        [
            # Numeric id collides with the NFL Chargers above -- only
            # team.uid disambiguates the two across Team.inc_dict.
            _team_entry("13", "s:40~l:46~t:13", "Los Angeles Lakers", "LAL"),
            _team_entry("12", "s:40~l:46~t:12", "LA Clippers", "LAC"),
            _team_entry("88", "s:40~l:46~t:88", "Boston Celtics", "BOS"),
        ]
    ),
    "baseball/mlb": _teams_envelope(
        [
            _team_entry("3", "s:1~l:10~t:3", "Los Angeles Angels", "LAA"),
            _team_entry("77", "s:1~l:10~t:77", "Boston Red Sox", "BOS"),
        ]
    ),
    # Deliberately a single team: post-4c595ff, incorp() always returns an
    # IncorporatorList regardless of row count -- this is the "welcome new
    # test case" the OVERRIDE calls for, proving the whole-list detail
    # fan-out still works (and still returns a real list) off a 1-row parent.
    "hockey/nhl": _teams_envelope(
        [
            _team_entry("8", "s:70~l:90~t:8", "Los Angeles Kings", "LAK"),
        ]
    ),
}


def _detail_payload(uid: str, display_name: str, address: dict[str, Any] | None) -> dict[str, Any]:
    team: dict[str, Any] = {"uid": uid, "displayName": display_name}
    if address is not None:
        team["franchise"] = {"venue": {"address": address}}
    return {"team": team}


TEAM_DETAIL_PAYLOADS: dict[tuple[str, str], dict[str, Any]] = {
    ("football/nfl", "13"): _detail_payload(
        "s:20~l:28~t:13", "Los Angeles Chargers", {"city": "Carson", "state": "CA", "zipCode": "90746"}
    ),
    ("football/nfl", "99"): _detail_payload(
        "s:20~l:28~t:99", "Dallas Cowboys", {"city": "Arlington", "state": "TX", "zipCode": "76011"}
    ),
    # No `franchise` key at all -- venue_state resolves to None via pluck's
    # missing-path-segment handling, not a crash.
    ("football/nfl", "50"): _detail_payload("s:20~l:28~t:50", "Ghost Team", None),
    ("basketball/nba", "13"): _detail_payload(
        "s:40~l:46~t:13", "Los Angeles Lakers", {"city": "Los Angeles", "state": "CA"}
    ),
    ("basketball/nba", "12"): _detail_payload("s:40~l:46~t:12", "LA Clippers", {"city": "Los Angeles", "state": "CA"}),
    ("basketball/nba", "88"): _detail_payload("s:40~l:46~t:88", "Boston Celtics", {"city": "Boston", "state": "MA"}),
    # MLB reports the full US state name, not "CA" -- proves to_state_code's
    # normalization fires.
    ("baseball/mlb", "3"): _detail_payload(
        "s:1~l:10~t:3", "Los Angeles Angels", {"city": "Anaheim", "state": "California", "zipCode": "92806"}
    ),
    ("baseball/mlb", "77"): _detail_payload(
        "s:1~l:10~t:77", "Boston Red Sox", {"city": "Boston", "state": "Massachusetts", "zipCode": "02215"}
    ),
    ("hockey/nhl", "8"): _detail_payload("s:70~l:90~t:8", "Los Angeles Kings", {"city": "Los Angeles", "state": "CA"}),
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
            # trip incorp()'s pre-4c595ff single-record mode.
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
                    "birthPlace": {"city": "Los Angeles", "state": "CA"},
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
                    # Same metro city NAME as a real CA city but the WRONG
                    # state -- the birth_state equality guard must exclude
                    # this one from the homegrown board.
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
    """Serves the four team-list feeds, per-(sport, id) detail drills, then roster drills."""
    req = httpx.Request("GET", url)

    if "enable=roster" in url:
        for (sport, team_id), payload in ROSTER_PAYLOADS.items():
            if f"/sports/{sport}/teams/{team_id}?enable=roster" in url:
                return httpx.Response(200, text=json.dumps(payload), request=req)
        return httpx.Response(200, text=json.dumps({"team": {"athletes": []}}), request=req)

    for sport, payload in TEAM_LIST_PAYLOADS.items():
        if url.endswith(f"/sports/{sport}/teams"):
            return httpx.Response(200, text=json.dumps(payload), request=req)

    for (sport, team_id), payload in TEAM_DETAIL_PAYLOADS.items():
        if url.endswith(f"/sports/{sport}/teams/{team_id}"):
            return httpx.Response(200, text=json.dumps(payload), request=req)

    return httpx.Response(200, text=json.dumps({"team": {}}), request=req)


def test_to_state_code_normalizes_full_names_and_passes_through_codes() -> None:
    """MLB-style full names normalize; already-abbreviated codes pass through unchanged."""
    assert state_sports.to_state_code("California") == "CA"
    assert state_sports.to_state_code("District of Columbia") == "DC"
    assert state_sports.to_state_code("Ontario") == "ON"
    assert state_sports.to_state_code("CA") == "CA"
    assert state_sports.to_state_code("ON") == "ON"


@pytest.mark.asyncio
async def test_state_sports_discover_drill_and_filter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """Discover -> whole-list detail fan-out -> state-equality filter -> roster drill, for CA."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", mock_espn_execute_request)

    state_teams, no_venue_total = await state_sports.discover_state_teams("CA")

    # State filter picked all five CA teams -- including the Clippers (whose
    # venue address is CA regardless of the deleted city-brand string), and
    # the MLB Angels (whose raw feed says "California", not "CA" -- proves
    # to_state_code fired). Dallas (TX), Boston (MA x2), and the venue-less
    # Ghost Team were correctly excluded.
    assert len(state_teams) == 5
    assert no_venue_total == 1
    leagues_found = {league for league, _sport, _team in state_teams}
    assert leagues_found == {"NFL", "NBA", "MLB", "NHL"}

    clippers = Team.inc_dict["s:40~l:46~t:12"]
    assert clippers.inc_name == "LA Clippers"

    # uid disambiguates the NFL/NBA numeric id=13 collision -- both teams
    # registered distinctly in Team.inc_dict under their own uid.
    chargers = Team.inc_dict["s:20~l:28~t:13"]
    lakers = Team.inc_dict["s:40~l:46~t:13"]
    assert chargers.inc_name == "Los Angeles Chargers"
    assert lakers.inc_name == "Los Angeles Lakers"
    assert chargers is not lakers

    rosters = await asyncio.gather(
        *(state_sports.drill_roster(league, sport, team) for league, sport, team in state_teams)
    )

    all_players: list[Any] = []
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

    # Homegrown board: pure birth_state == region equality, no brand tables.
    # "Bench Guard" (CO) and "Active Angel" (Long Beach, but state=NY -- same
    # metro city NAME as a real CA city, wrong state) are both excluded.
    heroes = [p for p in all_players if p.birth_state == "CA"]
    hero_names = {p.inc_name for p in heroes}
    assert hero_names == {"Luka Doncic", "Clipper Wing", "Vet Defenseman"}
    assert "Active Angel" not in hero_names
    assert "Bench Guard" not in hero_names


@pytest.mark.asyncio
async def test_team_detail_whole_list_fan_out_normalizes_venue_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """T5's whole-list `inc_parent` fan-out drills every NBA/MLB team's venue in one call each.

    Holds strong local references to the built `TeamDetail` rows -- `Team.inc_dict` /
    `TeamDetail.inc_dict` are `WeakValueDictionary`s, so a row with no surviving strong
    reference (e.g. read back from the registry after the building function returned)
    can already be garbage-collected; this test reads the `IncorporatorList` directly
    instead of round-tripping through the class registry.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", mock_espn_execute_request)

    nba_teams = await Team.incorp(
        inc_url="https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams",
        rec_path="sports.0.leagues.0.teams",
        inc_code="team.uid",
        inc_name="team.displayName",
        timeout=8,
    )
    nba_details = await TeamDetail.incorp(
        inc_url="https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{}",
        inc_parent=nba_teams,
        inc_child="team.id",
        rec_path="team",
        inc_code="uid",
        inc_name="displayName",
        conv_dict={
            "venue_city": state_sports.pluck("franchise.venue.address.city"),
            "venue_state": state_sports.pluck("franchise.venue.address.state", chain=state_sports.to_state_code),
        },
        timeout=8,
    )
    assert isinstance(nba_details, IncorporatorList)
    assert len(nba_details) == 3  # whole-list fan-out over all 3 NBA fixture teams

    by_uid = {d.inc_code: d for d in nba_details}
    # Regression: the Clippers, the team that started this pivot away from
    # brand-string matching -- a data attribute, not "location": "LA".
    assert by_uid["s:40~l:46~t:12"].venue_state == "CA"
    assert by_uid["s:40~l:46~t:12"].venue_city == "Los Angeles"
    assert by_uid["s:40~l:46~t:88"].venue_state == "MA"

    mlb_teams = await Team.incorp(
        inc_url="https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams",
        rec_path="sports.0.leagues.0.teams",
        inc_code="team.uid",
        inc_name="team.displayName",
        timeout=8,
    )
    mlb_details = await TeamDetail.incorp(
        inc_url="https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{}",
        inc_parent=mlb_teams,
        inc_child="team.id",
        rec_path="team",
        inc_code="uid",
        inc_name="displayName",
        conv_dict={
            "venue_state": state_sports.pluck("franchise.venue.address.state", chain=state_sports.to_state_code),
        },
        timeout=8,
    )
    # MLB's raw feed says "California" -- proves to_state_code's full-name
    # normalization fires, not just pass-through of an already-short code.
    angels_detail = next(d for d in mlb_details if d.inc_code == "s:1~l:10~t:3")
    assert angels_detail.venue_state == "CA"


@pytest.mark.asyncio
async def test_state_sports_excludes_no_venue_team(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """A team whose detail record has no `franchise` key at all is excluded and counted, not crashed."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", mock_espn_execute_request)

    state_teams, no_venue_total = await state_sports.discover_state_teams("CA")

    ghost_present = any(team.inc_name == "Ghost Team" for _league, _sport, team in state_teams)
    assert ghost_present is False
    assert no_venue_total == 1


@pytest.mark.asyncio
async def test_team_detail_incorp_returns_incorporator_list_for_single_row_league(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """Post-4c595ff: a single-team league's team-list build still yields a real IncorporatorList."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", mock_espn_execute_request)

    teams = await Team.incorp(
        inc_url="https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams",
        rec_path="sports.0.leagues.0.teams",
        inc_code="team.uid",
        inc_name="team.displayName",
        timeout=8,
    )
    assert isinstance(teams, IncorporatorList)
    assert len(teams) == 1
