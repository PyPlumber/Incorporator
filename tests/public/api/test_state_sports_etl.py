"""Mocked end-to-end smoke test for Tutorial 6 (state_sports.py).

Loads the actual tutorial entry script via `load_sidecar` (unique importlib
key) rather than duplicating its logic, so this test exercises the exact
shipped code path: the live CountriesNow reference-map fetch (one multi-URL
`incorp()` call, its fail-fast path, and the PARTIAL-failure fail-fast
check), Drill 1 (`League.incorp()` -> `Team.incorp(inc_parent=leagues,
inc_child="team_paths.path", ...)`, T5's whole-list `inc_parent`/`inc_child`
fan-out), the no-venue-address exclusion path, and Drill 2
(`TeamRoster.incorp(inc_parent=matched, ...)`) whose `conv_dict` showcases
all four converters -- `link_to` (the build-time join back to Drill 1's
`Team` instances), `inc` (pure type coercion), `calc` (array reductions +
league derivation), and `pluck` (nested venue lifts).

There is no Watershed, no `Fjord`, no exported file anywhere in this
tutorial -- every assertion below reads the `IncorporatorList` /
`Incorporator` instances `incorp()` returns directly.
"""

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from incorporator import IncorporatorList, calc, inc, link_to, pluck
from incorporator.io import fetch
from tests.helpers import load_sidecar

_EXAMPLE_DIR = Path(__file__).resolve().parents[3] / "examples" / "06-state-sports"
state_sports = load_sidecar(_EXAMPLE_DIR / "state_sports.py", "state_sports_target")

StateRef = state_sports.StateRef
League = state_sports.League
Team = state_sports.Team
TeamRoster = state_sports.TeamRoster

# A small literal map -- exercises the same normalization the live
# CountriesNow fetch would produce, without a network call.
TEST_STATE_CODE_MAP: dict[str, str] = {
    "California": "CA",
    "Massachusetts": "MA",
    "District of Columbia": "DC",
    "Ontario": "ON",
}


def _league_envelope(sport_slug: str, league_slug: str, abbreviation: str, team_ids: list[str]) -> dict[str, Any]:
    return {
        "sports": [
            {
                "slug": sport_slug,
                "leagues": [
                    {
                        "slug": league_slug,
                        "abbreviation": abbreviation,
                        "teams": [{"team": {"id": tid}} for tid in team_ids],
                    }
                ],
            }
        ]
    }


TEAM_LIST_PAYLOADS: dict[str, dict[str, Any]] = {
    "football/nfl": _league_envelope("football", "nfl", "NFL", ["13", "99", "50"]),
    "basketball/nba": _league_envelope("basketball", "nba", "NBA", ["13", "12", "88"]),
    "baseball/mlb": _league_envelope("baseball", "mlb", "MLB", ["3", "77"]),
    # Single team -- proves the whole-list `inc_parent` fan-out still works
    # (and still returns a real `IncorporatorList`) off a 1-row parent.
    "hockey/nhl": _league_envelope("hockey", "nhl", "NHL", ["8"]),
}


def _detail_team(
    league_slug: str, team_id: str, uid: str, display_name: str, address: dict[str, Any] | None
) -> dict[str, Any]:
    team: dict[str, Any] = {
        "id": team_id,
        "uid": uid,
        "displayName": display_name,
        # `league_from_links` reads links[0]["href"]'s 4th path segment --
        # stable across every league (verified live 2026-07-08).
        "links": [{"href": f"https://www.espn.com/{league_slug}/team/_/name/xx/team"}],
    }
    if address is not None:
        team["franchise"] = {"venue": {"address": address, "fullName": f"{display_name} Arena"}}
    return {"team": team}


TEAM_DETAIL_PAYLOADS: dict[tuple[str, str], dict[str, Any]] = {
    ("football/nfl", "13"): _detail_team(
        "nfl", "13", "s:20~l:28~t:13", "Los Angeles Chargers", {"city": "Carson", "state": "CA", "zipCode": "90746"}
    ),
    ("football/nfl", "99"): _detail_team(
        "nfl", "99", "s:20~l:28~t:99", "Dallas Cowboys", {"city": "Arlington", "state": "TX", "zipCode": "76011"}
    ),
    # No `franchise` key at all -- `venue_state` resolves to `None` via
    # `pluck`'s missing-path-segment handling, not a crash.
    ("football/nfl", "50"): _detail_team("nfl", "50", "s:20~l:28~t:50", "Ghost Team", None),
    ("basketball/nba", "13"): _detail_team(
        "nba", "13", "s:40~l:46~t:13", "Los Angeles Lakers", {"city": "Los Angeles", "state": "CA"}
    ),
    ("basketball/nba", "12"): _detail_team(
        "nba", "12", "s:40~l:46~t:12", "LA Clippers", {"city": "Los Angeles", "state": "CA"}
    ),
    ("basketball/nba", "88"): _detail_team(
        "nba", "88", "s:40~l:46~t:88", "Boston Celtics", {"city": "Boston", "state": "MA"}
    ),
    # MLB reports the full US state name, not "CA" -- proves `to_state_code`'s
    # normalization fires.
    ("baseball/mlb", "3"): _detail_team(
        "mlb", "3", "s:1~l:10~t:3", "Los Angeles Angels", {"city": "Anaheim", "state": "California", "zipCode": "92806"}
    ),
    ("baseball/mlb", "77"): _detail_team(
        "mlb", "77", "s:1~l:10~t:77", "Boston Red Sox", {"city": "Boston", "state": "Massachusetts"}
    ),
    ("hockey/nhl", "8"): _detail_team(
        "nhl", "8", "s:70~l:90~t:8", "Los Angeles Kings", {"city": "Los Angeles", "state": "CA"}
    ),
}


def _roster_team(league_slug: str, uid: str, display_name: str, athletes: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "team": {
            "uid": uid,
            "displayName": display_name,
            "links": [{"href": f"https://www.espn.com/{league_slug}/team/_/name/xx/team"}],
            "franchise": {"venue": {"fullName": f"{display_name} Arena"}},
            "athletes": athletes,
        }
    }


# Fictional player names throughout -- these fixtures don't need to track a
# real, ever-changing roster.
ROSTER_PAYLOADS: dict[tuple[str, str], dict[str, Any]] = {
    ("football/nfl", "13"): _roster_team(
        "nfl",
        "s:20~l:28~t:13",
        "Los Angeles Chargers",
        [
            {
                "id": "p1",
                "fullName": "Ridge Falcone",
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
                "fullName": "Wyatt Kessler",
                "active": True,
                "position": {"abbreviation": "WR"},
                "contract": {},
                "experience": {"years": 0},
                "birthPlace": {"city": "Somewhere", "state": "TX"},
            },
        ],
    ),
    ("basketball/nba", "13"): _roster_team(
        "nba",
        "s:40~l:46~t:13",
        "Los Angeles Lakers",
        [
            {
                "id": "p3",
                "fullName": "Teo Marsh",
                "active": True,
                "position": {"abbreviation": "G"},
                "contract": {"salary": 54126450},
                "experience": {"years": 7},
                "age": 26,
                "birthPlace": {"city": "Los Angeles", "state": "CA"},
            }
        ],
    ),
    ("basketball/nba", "12"): _roster_team(
        "nba",
        "s:40~l:46~t:12",
        "LA Clippers",
        [
            {
                "id": "p7",
                "fullName": "Rio Delgado",
                "active": True,
                "position": {"abbreviation": "G"},
                "contract": {"salary": 20000000},
                "experience": {"years": 4},
                "age": 26,
                "birthPlace": {"city": "Toronto", "state": "ON"},
            }
        ],
    ),
    ("baseball/mlb", "3"): _roster_team(
        "mlb",
        "s:1~l:10~t:3",
        "Los Angeles Angels",
        [
            # Whole-org quirk: one active roster player, one inactive
            # minor-leaguer that must not survive the active filter.
            {
                "id": "p4",
                "fullName": "Wells Bramante",
                "active": True,
                "position": {"abbreviation": "OF"},
                "contract": {},
                "experience": {"years": 5},
                "age": 28,
                "birthPlace": {"city": "Long Beach", "state": "NY"},
            },
            {
                "id": "p5",
                "fullName": "Reed Calloway",
                "active": False,
                "position": {"abbreviation": "1B"},
                "contract": {},
                "experience": {"years": 1},
                "age": 22,
                "birthPlace": {"city": "Nowhere", "state": "TX"},
            },
        ],
    ),
    ("hockey/nhl", "8"): _roster_team(
        "nhl",
        "s:70~l:90~t:8",
        "Los Angeles Kings",
        [
            {
                "id": "p6",
                "fullName": "Otto Kwan",
                "active": True,
                "position": {"abbreviation": "D"},
                "contract": {},
                "experience": {"years": 15},
                "age": 33,
                "birthPlace": {"city": "Northridge", "state": "CA"},
            }
        ],
    ),
}

# CountriesNow's real payload shape: {"error": bool, "data": {"states": [...]}}.
# `rec_path="data.states"` drills straight into this.
COUNTRIESNOW_PAYLOADS: dict[str, dict[str, Any]] = {
    "United%20States": {
        "error": False,
        "data": {"states": [{"name": "California", "state_code": "CA"}, {"name": "Massachusetts", "state_code": "MA"}]},
    },
    "Canada": {
        "error": False,
        "data": {"states": [{"name": "Ontario", "state_code": "ON"}]},
    },
}


async def mock_espn_execute_request(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Serves CountriesNow reference data, the four league team-lists, per-team
    detail drills, then roster drills -- routed purely off URL shape."""
    req = httpx.Request("GET", url)

    if "countriesnow.space" in url:
        for key, payload in COUNTRIESNOW_PAYLOADS.items():
            if f"country={key}" in url:
                return httpx.Response(200, text=json.dumps(payload), request=req)
        return httpx.Response(200, text=json.dumps({"error": True, "data": {}}), request=req)

    if "enable=roster" in url:
        for (sport_league, team_id), payload in ROSTER_PAYLOADS.items():
            if f"/sports/{sport_league}/teams/{team_id}?enable=roster" in url:
                return httpx.Response(200, text=json.dumps(payload), request=req)
        return httpx.Response(200, text=json.dumps({"team": {"athletes": []}}), request=req)

    for sport_league, payload in TEAM_LIST_PAYLOADS.items():
        if url.endswith(f"/sports/{sport_league}/teams"):
            return httpx.Response(200, text=json.dumps(payload), request=req)

    for (sport_league, team_id), payload in TEAM_DETAIL_PAYLOADS.items():
        if url.endswith(f"/sports/{sport_league}/teams/{team_id}"):
            return httpx.Response(200, text=json.dumps(payload), request=req)

    return httpx.Response(200, text=json.dumps({"team": {}}), request=req)


async def mock_countriesnow_unreachable(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Every CountriesNow call comes back with an empty `states` list -- the fail-fast path."""
    req = httpx.Request("GET", url)
    return httpx.Response(200, text=json.dumps({"error": True, "data": {}}), request=req)


async def mock_countriesnow_partial_failure(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """The US call succeeds; the Canada call comes back empty -- the combined
    `states` list is non-empty, so `if not states` alone would miss this. The
    fail-fast check must also catch a missing representative entry (Ontario)."""
    req = httpx.Request("GET", url)
    if "country=United%20States" in url:
        return httpx.Response(200, text=json.dumps(COUNTRIESNOW_PAYLOADS["United%20States"]), request=req)
    return httpx.Response(200, text=json.dumps({"error": True, "data": {}}), request=req)


def _reset_registries(*classes: Any) -> None:
    """Wipe per-class inc_dict between tests -- a WeakValueDictionary that
    would otherwise leak matches from an earlier test's fixture uids."""
    for cls in classes:
        cls.inc_dict.clear()


def test_to_state_code_normalizes_full_names_and_passes_through_codes() -> None:
    """MLB-style full names normalize via a literal mapping; already-abbreviated codes pass through."""
    test_map = {"California": "CA", "District of Columbia": "DC", "Ontario": "ON"}
    assert state_sports.to_state_code(test_map, "California") == "CA"
    assert state_sports.to_state_code(test_map, "District of Columbia") == "DC"
    assert state_sports.to_state_code(test_map, "Ontario") == "ON"
    assert state_sports.to_state_code(test_map, "CA") == "CA"
    assert state_sports.to_state_code(test_map, "ON") == "ON"


@pytest.mark.asyncio
async def test_reference_api_failure_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """An unreachable/empty CountriesNow response prints one ASCII error line and exits non-zero."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", mock_countriesnow_unreachable)
    _reset_registries(StateRef)

    with pytest.raises(SystemExit) as exc_info:
        await state_sports.fetch_state_code_map()

    assert exc_info.value.code != 0

    captured = capsys.readouterr()
    assert state_sports.REFERENCE_API_ERROR in captured.out
    assert captured.out.strip().isascii()


@pytest.mark.asyncio
async def test_reference_api_partial_failure_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """One of the two countries in the single multi-URL `incorp()` call resolves; the
    other comes back empty. The fail-fast check must still catch the missing
    country rather than treat a non-empty combined `states` list as full success."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", mock_countriesnow_partial_failure)
    _reset_registries(StateRef)

    with pytest.raises(SystemExit) as exc_info:
        await state_sports.fetch_state_code_map()

    assert exc_info.value.code != 0

    captured = capsys.readouterr()
    assert state_sports.REFERENCE_API_ERROR in captured.out
    assert captured.out.strip().isascii()


def test_build_team_paths_produces_dict_per_team_not_bare_strings() -> None:
    """`build_team_paths` must return `list[dict]`, not `list[str]` -- a
    bare-string leaf silently collapses into a list-of-lists under
    `extract_parent_data`'s BFS (a list-valued leaf read directly off a
    non-list parent node doesn't fan out on its own segment)."""
    leagues_array = [{"slug": "nfl", "teams": [{"team": {"id": "13"}}, {"team": {"id": "99"}}]}]
    paths = state_sports.build_team_paths("football", leagues_array)
    assert paths == [{"path": "football/nfl/teams/13"}, {"path": "football/nfl/teams/99"}]

    # Array-shape safety, not a defensive None-guard: garbage input degrades
    # to an empty list rather than raising.
    assert state_sports.build_team_paths("football", None) == []
    assert state_sports.build_team_paths("football", []) == []
    assert state_sports.build_team_paths("football", [{"slug": "nfl", "teams": "not-a-list"}]) == []


def test_league_from_links_derives_label_across_all_four_leagues() -> None:
    """Every league's own `links[0]['href']` embeds the league slug at the
    same path position -- no lookup table needed."""
    assert state_sports.league_from_links([{"href": "https://www.espn.com/nfl/team/_/name/xx"}]) == "NFL"
    assert state_sports.league_from_links([{"href": "https://www.espn.com/nba/team/_/name/xx"}]) == "NBA"
    assert state_sports.league_from_links([{"href": "https://www.espn.com/mlb/team/_/name/xx"}]) == "MLB"
    assert state_sports.league_from_links([{"href": "https://www.espn.com/nhl/team/_/name/xx"}]) == "NHL"
    assert state_sports.league_from_links(None) is None
    assert state_sports.league_from_links([]) is None


def test_build_roster_path_resolves_the_closed_sport_slug_map() -> None:
    """`build_roster_path` reads the row's OWN already-computed `league` value
    (conv_dict insertion order) and resolves it to ESPN's fixed URL sport
    segment -- URL-taxonomy plumbing, not a brand/location alias table."""
    assert state_sports.build_roster_path("NFL", "13") == "football/nfl/teams/13?enable=roster"
    assert state_sports.build_roster_path("NBA", "12") == "basketball/nba/teams/12?enable=roster"
    assert state_sports.build_roster_path("MLB", "3") == "baseball/mlb/teams/3?enable=roster"
    assert state_sports.build_roster_path("NHL", "8") == "hockey/nhl/teams/8?enable=roster"


def test_extract_active_players_filters_inactive_and_derives_fields() -> None:
    """The array-reduction workhorse: active-only filter, embedded league/team_name,
    and the salary_per_year / turned_pro_at derivations -- with no crash on missing
    salary or age."""
    athletes = [
        {
            "id": "p1",
            "fullName": "Ridge Falcone",
            "active": True,
            "position": {"abbreviation": "OT"},
            "contract": {"salary": 3000000},
            "experience": {"years": 3},
            "age": 23,
            "birthPlace": {"city": "North Oaks", "state": "MN"},
        },
        {
            "id": "p2",
            "fullName": "Bench Warmer",
            "active": False,  # inactive -- must be dropped
            "position": {"abbreviation": "QB"},
            "contract": {"salary": 999999},
            "experience": {"years": 1},
            "age": 25,
            "birthPlace": {"city": "Nowhere", "state": "TX"},
        },
        {
            "id": "p3",
            "fullName": "No Salary No Age",
            "active": True,
            "position": {"abbreviation": "WR"},
            "contract": {},
            "experience": {"years": 0},
            "birthPlace": {"city": "Somewhere", "state": "TX"},
        },
    ]
    players = state_sports.extract_active_players(athletes, "NFL", "Los Angeles Chargers")

    assert len(players) == 2  # inactive player dropped
    assert all(p["team_name"] == "Los Angeles Chargers" for p in players)
    assert all(p["league"] == "NFL" for p in players)

    ridge = next(p for p in players if p["name"] == "Ridge Falcone")
    assert ridge["salary"] == 3000000
    assert ridge["tenure"] == 3
    assert ridge["salary_per_year"] == pytest.approx(3000000 / 3)
    assert ridge["turned_pro_at"] == 20

    no_salary = next(p for p in players if p["name"] == "No Salary No Age")
    assert no_salary["salary"] is None
    assert no_salary["salary_per_year"] is None
    assert no_salary["turned_pro_at"] is None

    # Garbage input degrades to an empty list, no crash.
    assert state_sports.extract_active_players(None, "NFL", "X") == []


def test_team_summary_reducers_cover_active_only() -> None:
    """`team_payroll` / `team_salary_known_count` / `team_active_count` all
    ignore inactive players (MLB's org-roster quirk)."""
    athletes = [
        {"active": True, "contract": {"salary": 1000000}},
        {"active": True, "contract": {}},
        {"active": False, "contract": {"salary": 9999999}},  # inactive -- excluded from every reducer
    ]
    assert state_sports.team_payroll(athletes) == 1000000.0
    assert state_sports.team_salary_known_count(athletes) == 1
    assert state_sports.team_active_count(athletes) == 2
    assert state_sports.team_payroll(None) == 0.0
    assert state_sports.team_salary_known_count(None) == 0
    assert state_sports.team_active_count(None) == 0


@pytest.mark.asyncio
async def test_discover_teams_whole_list_drill_normalizes_venue_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """Drill 1 -- `League.incorp()` -> `Team.incorp(inc_parent=leagues,
    inc_child="team_paths.path", ...)` -- fans out across all four leagues in
    one whole-list `inc_parent` call and normalizes `venue_state` via the
    fetched CountriesNow map."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", mock_espn_execute_request)
    _reset_registries(League, Team)

    teams = await state_sports.discover_teams(TEST_STATE_CODE_MAP)

    assert isinstance(teams, IncorporatorList)
    assert len(teams) == 9  # 3 + 3 + 2 + 1 across the four leagues

    by_uid = {t.inc_code: t for t in teams}

    # Regression: the Clippers, the team that started this pivot away from
    # brand-string matching -- a data attribute, not "location": "LA".
    clippers = by_uid["s:40~l:46~t:12"]
    assert clippers.venue_state == "CA"
    assert clippers.venue_city == "Los Angeles"
    assert clippers.league == "NBA"
    assert clippers.roster_path == "basketball/nba/teams/12?enable=roster"

    # uid disambiguates the NFL/NBA numeric id=13 collision.
    chargers = by_uid["s:20~l:28~t:13"]
    lakers = by_uid["s:40~l:46~t:13"]
    assert chargers.inc_name == "Los Angeles Chargers"
    assert lakers.inc_name == "Los Angeles Lakers"
    assert chargers.league == "NFL"
    assert lakers.league == "NBA"

    # MLB's raw feed says "California" -- proves to_state_code's full-name
    # normalization fires, not just pass-through of an already-short code.
    angels = by_uid["s:1~l:10~t:3"]
    assert angels.venue_state == "CA"

    # No `franchise` key at all -- excluded via `venue_state is None`, not a crash.
    ghost = by_uid["s:20~l:28~t:50"]
    assert ghost.venue_state is None

    no_venue_total = sum(1 for t in teams if t.venue_state is None)
    assert no_venue_total == 1

    matched = [t for t in teams if t.venue_state == "CA"]
    assert len(matched) == 5
    leagues_found = {t.league for t in matched}
    assert leagues_found == {"NFL", "NBA", "MLB", "NHL"}


@pytest.mark.asyncio
async def test_roster_drill_links_back_to_matched_team_and_flattens_active_players(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """Drill 2 -- `TeamRoster.incorp(inc_parent=matched, ...)` -- showcases all
    four `conv_dict` converters: `link_to` (build-time join back to Drill 1's
    `Team` instances), `calc` (league-through-the-join + array reductions),
    `inc` (pure type coercion on `id`), and `pluck` (the nested venue name).
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", mock_espn_execute_request)
    _reset_registries(StateRef, League, Team, TeamRoster)

    teams = await state_sports.discover_teams(TEST_STATE_CODE_MAP)
    matched = [t for t in teams if t.venue_state == "CA"]
    assert len(matched) == 5

    rosters = await TeamRoster.incorp(
        inc_parent=matched,
        inc_child="roster_path",
        inc_url="https://site.api.espn.com/apis/site/v2/sports/{}",
        rec_path="team",
        inc_code="uid",
        inc_name="displayName",
        conv_dict={
            "team_ref": calc(link_to(matched), "uid"),
            "league": calc(state_sports.league_from_team_ref, "team_ref", default=None, target_type=str),
            "venue_name": pluck("franchise.venue.fullName"),
            "id": inc(int, default=0),
            "payroll": calc(state_sports.team_payroll, "athletes", default=0.0, target_type=float),
            "salary_known": calc(state_sports.team_salary_known_count, "athletes", default=0, target_type=int),
            "active_count": calc(state_sports.team_active_count, "athletes", default=0, target_type=int),
            "athletes": calc(state_sports.extract_active_players, "athletes", "league", "displayName", default=[]),
        },
        name_chg=[("athletes", "players")],
        excl_lst=["record", "logos", "nextEvent", "standingSummary"],
        timeout=10,
    )

    assert isinstance(rosters, IncorporatorList)
    assert len(rosters) == 5

    by_uid = {r.inc_code: r for r in rosters}

    # link_to(): the Chargers' TeamRoster row's team_ref resolves to the
    # SAME Team instance discover_teams() built -- not a re-fetch, not a
    # re-derivation.
    chargers_roster = by_uid["s:20~l:28~t:13"]
    chargers_team = next(t for t in matched if t.inc_code == "s:20~l:28~t:13")
    assert chargers_roster.team_ref is chargers_team
    assert chargers_roster.team_ref.venue_state == "CA"

    # calc() reading THROUGH team_ref: league lands correctly without a
    # second league_from_links derivation against the roster payload.
    assert chargers_roster.league == "NFL"

    # inc(): id coerced to a real int, not a JSON string.
    assert isinstance(chargers_roster.id, int)

    # pluck(): venue_name is a genuinely new nested field.
    assert chargers_roster.venue_name == "Los Angeles Chargers Arena"

    # calc() array reductions: payroll/salary_known/active_count read the
    # RAW athletes array (computed before "athletes" is overwritten in
    # place by the final conv_dict entry -- insertion order).
    assert chargers_roster.payroll == 3809632.0
    assert chargers_roster.salary_known == 1
    assert chargers_roster.active_count == 2

    # name_chg renames the in-place-computed "athletes" to "players" -- a
    # re-inferred nested Pydantic sub-model list, attribute access only.
    assert len(chargers_roster.players) == 2
    ridge = next(p for p in chargers_roster.players if p.name == "Ridge Falcone")
    assert ridge.salary == 3809632
    assert ridge.tenure == 3
    assert ridge.turned_pro_at == 20
    assert ridge.salary_per_year == pytest.approx(3809632 / 3)
    assert ridge.league == "NFL"
    assert ridge.team_name == "Los Angeles Chargers"

    # MLB's whole-org quirk: the inactive minor-leaguer never reaches `players`.
    angels_roster = by_uid["s:1~l:10~t:3"]
    assert len(angels_roster.players) == 1
    assert angels_roster.players[0].name == "Wells Bramante"

    all_players = [p for team in rosters for p in team.players]
    assert len(all_players) == 6  # 2 + 1 + 1 + 1 + 1 across the five CA teams

    # Homegrown board precondition: birth_state is embedded per player,
    # comparable directly against the region with zero brand-string tables.
    heroes = {p.name for p in all_players if p.birth_state == "CA"}
    assert heroes == {"Teo Marsh", "Otto Kwan"}
    assert "Wells Bramante" not in heroes  # Long Beach, but state="NY" -- wrong state
