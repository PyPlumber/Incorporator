"""Mocked end-to-end smoke test for Tutorial 6 (state_sports.py).

Loads the actual tutorial entry script via `load_sidecar` (unique importlib
key) rather than duplicating its logic, so this test exercises the exact
shipped code path: the live CountriesNow reference-map fetch (one multi-URL
`incorp()` call, its fail-fast path, and the PARTIAL-failure fail-fast
check), Drill 1 (`League.incorp()` -> a per-league loop of
`Team.incorp(inc_parent=lg, inc_child="leagues.teams.team.id", ...)`, T5's
`inc_parent`/`inc_child` shape reused once per league), the no-venue-address
exclusion path, Drill 2 (a per-league-group loop of
`TeamRoster.incorp(inc_parent=group, ...)`), and the THIRD, in-memory
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
import logging
import operator
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
        # Vestigial: the per-league loop stamps `league` off the loop
        # variable itself now (see state_sports.py's Drill 1), not off a
        # row's own `links` array -- kept here only because it's harmless
        # shape-fidelity with the real ESPN payload, not because anything
        # still reads it.
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
    # MLB reports the full US state name, not "CA" -- proves the
    # identity-augmented `state_code_map` normalization fires.
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
        "data": {
            "states": [
                {"name": "California", "state_code": "CA"},
                {"name": "Massachusetts", "state_code": "MA"},
                # Dallas Cowboys' venue reports the already-abbreviated "TX"
                # directly (no full-name normalization needed) -- Texas
                # still needs its own identity entry in the fetched map,
                # same as the real CountriesNow feed (which lists all 50
                # states): `chain=state_code_map.get` has no
                # `mapping.get(value, value)` passthrough fallback, so an
                # omitted-but-valid code would resolve to `None`, not itself.
                {"name": "Texas", "state_code": "TX"},
            ]
        },
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


@pytest.mark.asyncio
async def test_player_payload_passthrough_conv_dict(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """`Player.incorp(payload_list=...)` -- the network-free in-memory
    passthrough -- exercises every primitive-only conv_dict entry:
    `calc(TYPE, "nested.path", default=..., target_type=TYPE)` for
    salary/tenure/age/pos/birth_city/birth_state, then `calc(operator.sub,
    "age", "tenure", ...)` for `turned_pro_at`, reading the already-mutated,
    pre-coerced (never-None) `age`/`tenure` entries (insertion order).
    `salary_per_year` is no longer a conv_dict entry at all -- it's a
    one-line post-build `max()`-guarded stamp, exercised separately below."""
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
            "salary": calc(int, "contract.salary", default=0, target_type=int),
            "tenure": calc(int, "experience.years", default=0, target_type=int),
            "age": calc(int, "age", default=0, target_type=int),
            "pos": calc(str, "position.abbreviation", default="-", target_type=str),
            "birth_city": calc(str, "birthPlace.city", default="-", target_type=str),
            "birth_state": calc(str, "birthPlace.state", default="-", target_type=str),
            "turned_pro_at": calc(operator.sub, "age", "tenure", default=0, target_type=int),
        },
    )
    for p in players:
        p.salary_per_year = p.salary / max(p.tenure, 1)

    assert isinstance(players, IncorporatorList)
    assert len(players) == 2

    ridge = next(p for p in players if p.inc_name == "Ridge Falcone")
    assert ridge.salary == 3809632
    assert ridge.tenure == 3
    assert ridge.pos == "OT"
    assert ridge.birth_city == "North Oaks"
    assert ridge.birth_state == "MN"
    assert ridge.salary_per_year == 3809632 / 3  # 1269877.33...
    assert ridge.turned_pro_at == 20  # 23 - 3

    wyatt = next(p for p in players if p.inc_name == "Wyatt Kessler")
    assert wyatt.salary == 0
    assert wyatt.tenure == 0
    assert wyatt.pos == "-"
    assert wyatt.birth_city == "-"
    assert wyatt.birth_state == "-"
    assert wyatt.salary_per_year == 0.0  # 0 / max(0, 1)
    assert wyatt.turned_pro_at == 0  # missing age defaults to 0; 0 - 0 = 0 sentinel


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
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """End-to-end: default argv ("CA") drives the whole inline `main()` --
    reference fetch, Drill 1 (per-league loop), the no-venue exclusion,
    Drill 2 (per-league-group loop), and the THIRD in-memory
    `Player.incorp(payload_list=...)` call -- with zero stderr and
    ASCII-only stdout, exactly like the live acceptance run. Also asserts
    zero coercion-warning spam: every derived field reads a pre-coerced,
    never-garbage input (age/tenure default to real ints before
    `turned_pro_at`'s `calc(operator.sub, ...)` runs), mirroring the live
    logging-handler check performed against the real 580-player pipeline."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", mock_espn_execute_request)
    monkeypatch.setattr(sys, "argv", ["state_sports.py"])
    _reset_registries(StateRef, League, Team, TeamRoster, Player)

    with caplog.at_level(logging.WARNING):
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

    warning_records = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert warning_records == []


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
