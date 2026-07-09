"""Mocked end-to-end smoke test for Tutorial 6 (state_sports.py).

Loads the actual tutorial entry script via `load_sidecar` (unique importlib
key) rather than duplicating its logic, so this test exercises the exact
shipped code path: the live CountriesNow reference-map fetch (one multi-URL
`incorp()` call, its fail-fast path, and the PARTIAL-failure fail-fast
check), Drill 1 (`League.incorp()` -> `Team.incorp(inc_parent=leagues,
inc_child="team_paths.path", ...)`, T5's whole-list `inc_parent`/`inc_child`
fan-out), the no-venue-address exclusion path, Drill 2
(`TeamRoster.incorp(inc_parent=matched, ...)`), and the THIRD, in-memory
`Player.incorp(payload_list=roster_payload)` passthrough -- the
"Build rows from memory" recipe in `docs/api_atlas.md`.

There is no Watershed, no `Fjord`, no exported file anywhere in this
tutorial -- every assertion below reads the `IncorporatorList` /
`Incorporator` instances `incorp()` returns directly, or the printed board
output captured via `capsys`.

`main()` is now fully inline (no `fetch_state_code_map` / `discover_teams`
phase functions to call directly) -- the full-pipeline tests below drive it
through `sys.argv` + `capsys`, mirroring how a user actually runs the
script.
"""

import json
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest

from incorporator import IncorporatorList, calc
from incorporator.io import fetch
from tests.helpers import load_sidecar

_EXAMPLE_DIR = Path(__file__).resolve().parents[3] / "examples" / "06-state-sports"
state_sports = load_sidecar(_EXAMPLE_DIR / "state_sports.py", "state_sports_target")

StateRef = state_sports.StateRef
League = state_sports.League
Team = state_sports.Team
TeamRoster = state_sports.TeamRoster
Player = state_sports.Player

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


def _athlete(
    uid: str,
    full_name: str,
    active: bool,
    pos: str,
    salary: int | None,
    tenure: int,
    age: int | None,
    birth_city: str,
    birth_state: str,
) -> dict[str, Any]:
    """One ESPN-shaped athlete row -- `uid` is globally unique across leagues
    (verified live 2026-07-09), the collision-safe PK `Player.incorp` binds
    directly with no league-qualifying calc needed."""
    return {
        "uid": uid,
        "fullName": full_name,
        "active": active,
        "position": {"abbreviation": pos},
        "contract": {"salary": salary} if salary is not None else {},
        "experience": {"years": tenure},
        "age": age,
        "birthPlace": {"city": birth_city, "state": birth_state},
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
            _athlete("s:20~l:28~a:p1", "Ridge Falcone", True, "OT", 3809632, 3, 23, "North Oaks", "MN"),
            # No salary, no age -- proves both stay build-time-defaulted
            # ("-") without raising.
            _athlete("s:20~l:28~a:p2", "Wyatt Kessler", True, "WR", None, 0, None, "Somewhere", "TX"),
        ],
    ),
    ("basketball/nba", "13"): _roster_team(
        "nba",
        "s:40~l:46~t:13",
        "Los Angeles Lakers",
        [_athlete("s:40~l:46~a:p3", "Teo Marsh", True, "G", 54126450, 7, 26, "Los Angeles", "CA")],
    ),
    ("basketball/nba", "12"): _roster_team(
        "nba",
        "s:40~l:46~t:12",
        "LA Clippers",
        [_athlete("s:40~l:46~a:p7", "Rio Delgado", True, "G", 20000000, 4, 26, "Toronto", "ON")],
    ),
    ("baseball/mlb", "3"): _roster_team(
        "mlb",
        "s:1~l:10~t:3",
        "Los Angeles Angels",
        [
            # Whole-org quirk: one active roster player, one inactive
            # minor-leaguer that must not survive the active filter.
            _athlete("s:1~l:10~a:p4", "Wells Bramante", True, "OF", None, 5, 28, "Long Beach", "NY"),
            _athlete("s:1~l:10~a:p5", "Reed Calloway", False, "1B", None, 1, 22, "Nowhere", "TX"),
        ],
    ),
    ("hockey/nhl", "8"): _roster_team(
        "nhl",
        "s:70~l:90~t:8",
        "Los Angeles Kings",
        [_athlete("s:70~l:90~a:p6", "Otto Kwan", True, "D", None, 15, 33, "Northridge", "CA")],
    ),
    ("football/nfl", "99"): _roster_team(
        "nfl",
        "s:20~l:28~t:99",
        "Dallas Cowboys",
        [_athlete("s:20~l:28~a:p8", "Marcus Fielding", True, "LB", 1200000, 2, 24, "Waco", "TX")],
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


def test_build_team_paths_produces_dict_per_team_not_bare_strings() -> None:
    """`build_team_paths` must return `list[dict]`, not `list[str]` -- a
    bare-string leaf silently collapses into a list-of-lists under
    `extract_parent_data`'s BFS (a list-valued leaf read directly off a
    non-list parent node doesn't fan out on its own segment). The bare
    comprehension has no isinstance ladder -- malformed input is `calc()`'s
    exception-fallback's job (`default=[]`), not this function's."""
    leagues_array = [{"slug": "nfl", "teams": [{"team": {"id": "13"}}, {"team": {"id": "99"}}]}]
    paths = state_sports.build_team_paths("football", leagues_array)
    assert paths == [{"path": "football/nfl/teams/13"}, {"path": "football/nfl/teams/99"}]


def test_league_from_links_derives_label_across_all_four_leagues() -> None:
    """Every league's own `links[0]['href']` embeds the league slug at the
    same path position -- no lookup table needed."""
    assert state_sports.league_from_links([{"href": "https://www.espn.com/nfl/team/_/name/xx"}]) == "NFL"
    assert state_sports.league_from_links([{"href": "https://www.espn.com/nba/team/_/name/xx"}]) == "NBA"
    assert state_sports.league_from_links([{"href": "https://www.espn.com/mlb/team/_/name/xx"}]) == "MLB"
    assert state_sports.league_from_links([{"href": "https://www.espn.com/nhl/team/_/name/xx"}]) == "NHL"


def test_build_roster_path_resolves_the_closed_sport_slug_map() -> None:
    """`build_roster_path` reads the row's OWN already-computed `league` value
    (conv_dict insertion order) and resolves it to ESPN's fixed URL sport
    segment -- URL-taxonomy plumbing, not a brand/location alias table."""
    assert state_sports.build_roster_path("NFL", "13") == "football/nfl/teams/13?enable=roster"
    assert state_sports.build_roster_path("NBA", "12") == "basketball/nba/teams/12?enable=roster"
    assert state_sports.build_roster_path("MLB", "3") == "baseball/mlb/teams/3?enable=roster"
    assert state_sports.build_roster_path("NHL", "8") == "hockey/nhl/teams/8?enable=roster"


def test_salary_per_year_formats_display_string_and_guards_missing_salary() -> None:
    """`salary_per_year` is a preformatted display string (never sorted or
    aggregated) -- 3809632/3 = 1269877.33 -> `,.0f` -> "$1,269,877",
    matching the acceptance spot-check. A missing salary returns "-" via
    the function's own guard, not `calc()`'s exception-fallback (tenure is
    real, non-garbage data, so `calc()` would otherwise invoke the function
    and log a warning on every MLB/NHL row -- verified against a live run)."""
    assert state_sports.salary_per_year(3809632, 3) == "$1,269,877"
    assert state_sports.salary_per_year(None, 5) == "-"
    assert state_sports.salary_per_year(1000000, 0) == "$1,000,000"  # tenure=0 -> "or 1" fallback


def test_turned_pro_at_formats_display_string_and_guards_missing_age() -> None:
    """age=23, tenure=3 -> "20", matching the acceptance spot-check. A
    missing age returns "-" via the function's own guard."""
    assert state_sports.turned_pro_at(23, 3) == "20"
    assert state_sports.turned_pro_at(None, 5) == "-"


@pytest.mark.asyncio
async def test_player_payload_passthrough_conv_dict(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """`Player.incorp(payload_list=...)` -- the network-free in-memory
    passthrough -- exercises every conv_dict entry: `calc(TYPE, "nested.path",
    default=..., target_type=TYPE)` for salary/tenure/pos/birth_city/
    birth_state, and the two named-callable `calc()` entries for the
    preformatted display strings. `pluck()` has no `default=` (verified
    `incorporator/schema/extractors.py:308`), so every one of these fields
    uses `calc(TYPE, ...)` instead -- never a bare `pluck()`."""
    monkeypatch.chdir(tmp_path)
    _reset_registries(Player)

    payload = [
        {
            "uid": "s:20~l:28~a:p1",
            "fullName": "Ridge Falcone",
            "league": "NFL",
            "team_name": "Los Angeles Chargers",
            "contract": {"salary": 3809632},
            "experience": {"years": 3},
            "position": {"abbreviation": "OT"},
            "birthPlace": {"city": "North Oaks", "state": "MN"},
            "age": 23,
        },
        {
            # No salary, no age, no position, no birthplace -- every field
            # must resolve to its build-time default, not crash.
            "uid": "s:20~l:28~a:p2",
            "fullName": "Wyatt Kessler",
            "league": "NFL",
            "team_name": "Los Angeles Chargers",
            "contract": {},
            "experience": {"years": 0},
            "position": {},
            "birthPlace": {},
            "age": None,
        },
    ]
    players = await Player.incorp(
        payload_list=payload,
        inc_code="uid",
        inc_name="fullName",
        conv_dict={
            "salary": calc(float, "contract.salary", default=0.0, target_type=float),
            "tenure": calc(int, "experience.years", default=0, target_type=int),
            "pos": calc(str, "position.abbreviation", default="-", target_type=str),
            "birth_city": calc(str, "birthPlace.city", default="-", target_type=str),
            "birth_state": calc(str, "birthPlace.state", default="-", target_type=str),
            "salary_per_year": calc(
                state_sports.salary_per_year, "contract.salary", "experience.years", default="-", target_type=str
            ),
            "turned_pro_at": calc(state_sports.turned_pro_at, "age", "experience.years", default="-", target_type=str),
        },
    )

    assert isinstance(players, IncorporatorList)
    assert len(players) == 2

    ridge = next(p for p in players if p.inc_name == "Ridge Falcone")
    assert ridge.salary == 3809632.0
    assert ridge.tenure == 3
    assert ridge.pos == "OT"
    assert ridge.birth_city == "North Oaks"
    assert ridge.birth_state == "MN"
    assert ridge.salary_per_year == "$1,269,877"  # 3809632 / 3 -> ",.0f"
    assert ridge.turned_pro_at == "20"  # 23 - 3

    wyatt = next(p for p in players if p.inc_name == "Wyatt Kessler")
    assert wyatt.salary == 0.0
    assert wyatt.tenure == 0
    assert wyatt.pos == "-"
    assert wyatt.birth_city == "-"
    assert wyatt.birth_state == "-"
    assert wyatt.salary_per_year == "-"
    assert wyatt.turned_pro_at == "-"


@pytest.mark.asyncio
async def test_reference_api_failure_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """An unreachable/empty CountriesNow response calls `sys.exit(REFERENCE_API_ERROR)`.
    `sys.exit(str)` only prints to stderr when it propagates uncaught to the
    real interpreter top-level (verified empirically) -- inside a
    `pytest.raises(SystemExit)` block nothing is written to stderr by the
    exception itself, so the load-bearing assertion is on `exc_info.value.code`."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", mock_countriesnow_unreachable)
    monkeypatch.setattr(sys, "argv", ["state_sports.py"])
    _reset_registries(StateRef)

    with pytest.raises(SystemExit) as exc_info:
        await state_sports.main()

    assert exc_info.value.code == state_sports.REFERENCE_API_ERROR

    captured = capsys.readouterr()
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
    monkeypatch.setattr(sys, "argv", ["state_sports.py"])
    _reset_registries(StateRef)

    with pytest.raises(SystemExit) as exc_info:
        await state_sports.main()

    assert exc_info.value.code == state_sports.REFERENCE_API_ERROR

    captured = capsys.readouterr()
    assert captured.out.strip().isascii()


@pytest.mark.asyncio
async def test_full_run_ca_default_prints_all_boards(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """End-to-end: default argv ("CA") drives the whole inline `main()` --
    reference fetch, Drill 1, the no-venue exclusion, Drill 2, and the
    THIRD in-memory `Player.incorp(payload_list=...)` call -- with zero
    stderr and ASCII-only stdout, exactly like the live acceptance run."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", mock_espn_execute_request)
    monkeypatch.setattr(sys, "argv", ["state_sports.py"])
    _reset_registries(StateRef, League, Team, TeamRoster, Player)

    await state_sports.main()

    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out.isascii()

    assert "WARN: 1 team(s) had no reachable venue address" in captured.out
    assert "OK: Found 5 CA team(s)" in captured.out
    assert "OK: Loaded 6 active players across 5 teams." in captured.out
    assert "PAYCHECK BOARD" in captured.out
    assert "VETERANS BOARD" in captured.out
    assert "HOMEGROWN BOARD (CA-born players on a CA team)" in captured.out

    # Teo Marsh (Lakers, CA-born) and Otto Kwan (Kings, CA-born) surface;
    # Wells Bramante (Angels, born in NY) must not.
    assert "Teo Marsh" in captured.out
    assert "Otto Kwan" in captured.out
    assert "Wells Bramante" not in captured.out.split("HOMEGROWN BOARD")[1]


@pytest.mark.asyncio
async def test_full_run_single_team_region(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """A region matching exactly one fixture team (TX -> Dallas Cowboys)
    still runs the whole pipeline, including the third in-memory
    `Player.incorp(payload_list=...)` call, off a single-team roster."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", mock_espn_execute_request)
    monkeypatch.setattr(sys, "argv", ["state_sports.py", "TX"])
    _reset_registries(StateRef, League, Team, TeamRoster, Player)

    await state_sports.main()

    captured = capsys.readouterr()
    assert captured.err == ""
    assert "OK: Found 1 TX team(s): NFL Dallas Cowboys" in captured.out
    assert "OK: Loaded 1 active players across 1 teams." in captured.out
    assert "Marcus Fielding" in captured.out


@pytest.mark.asyncio
async def test_full_run_no_matching_region_returns_gracefully(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """A region with no matching fixture team prints the "no teams found"
    guidance and returns -- no `SystemExit`, no crash, no roster/player
    calls attempted."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", mock_espn_execute_request)
    monkeypatch.setattr(sys, "argv", ["state_sports.py", "ON"])
    _reset_registries(StateRef, League, Team, TeamRoster, Player)

    await state_sports.main()

    captured = capsys.readouterr()
    assert captured.err == ""
    assert "No ON teams found" in captured.out
    assert "OK: Loaded" not in captured.out
