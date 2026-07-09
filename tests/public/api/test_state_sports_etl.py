"""Mocked end-to-end smoke test for Tutorial 6 (state_sports.py).

Loads the actual tutorial entry script via `load_sidecar` (unique importlib
key) rather than duplicating its logic, so this test exercises the exact
shipped code path: the live CountriesNow reference-map fetch (one multi-URL
`incorp()` call, its fail-fast path, and the PARTIAL-failure fail-fast
check), the T5-style whole-list `inc_parent` detail fan-out that reads
`franchise.venue.address.state`, `to_state_code`'s full-name -> 2-letter
normalization, the no-venue-address exclusion path, the plain-dict
matched-team rows built straight off `TeamDetail` (no `Team.inc_dict`
join-back), and the 3-current linear Watershed (file-mode `matched_teams`
Stream -> `parent_current` `rosters` Stream -> `boards` Fjord) that joins
the two snapshots, flattens active players, and exports tagged NDJSON rows.
"""

import functools
import json
from datetime import datetime, timedelta, timezone
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
MatchedTeam = state_sports.MatchedTeam
TeamRoster = state_sports.TeamRoster

# A small literal map -- exercises the same normalization the live
# CountriesNow fetch would produce, without a network call. Phase 1's tests
# below thread this through `discover_state_teams` directly rather than
# going through `fetch_state_code_map()`.
TEST_STATE_CODE_MAP: dict[str, str] = {
    "California": "CA",
    "Massachusetts": "MA",
    "District of Columbia": "DC",
    "Ontario": "ON",
}


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
    # Deliberately a single team: incorp() always returns an
    # IncorporatorList regardless of row count -- proves the whole-list
    # detail fan-out still works (and still returns a real list) off a
    # 1-row parent.
    "hockey/nhl": _teams_envelope(
        [
            _team_entry("8", "s:70~l:90~t:8", "Los Angeles Kings", "LAK"),
        ]
    ),
}


def _detail_payload(team_id: str, uid: str, display_name: str, address: dict[str, Any] | None) -> dict[str, Any]:
    # `id` sits alongside `uid` at the top level of the raw "team" envelope --
    # `rec_path="team"` in `discover_state_teams` reads `detail.id` directly
    # (no `Team.inc_dict` join-back), so the fixture must carry it too.
    team: dict[str, Any] = {"id": team_id, "uid": uid, "displayName": display_name}
    if address is not None:
        team["franchise"] = {"venue": {"address": address}}
    return {"team": team}


TEAM_DETAIL_PAYLOADS: dict[tuple[str, str], dict[str, Any]] = {
    ("football/nfl", "13"): _detail_payload(
        "13", "s:20~l:28~t:13", "Los Angeles Chargers", {"city": "Carson", "state": "CA", "zipCode": "90746"}
    ),
    ("football/nfl", "99"): _detail_payload(
        "99", "s:20~l:28~t:99", "Dallas Cowboys", {"city": "Arlington", "state": "TX", "zipCode": "76011"}
    ),
    # No `franchise` key at all -- venue_state resolves to None via pluck's
    # missing-path-segment handling, not a crash.
    ("football/nfl", "50"): _detail_payload("50", "s:20~l:28~t:50", "Ghost Team", None),
    ("basketball/nba", "13"): _detail_payload(
        "13", "s:40~l:46~t:13", "Los Angeles Lakers", {"city": "Los Angeles", "state": "CA"}
    ),
    ("basketball/nba", "12"): _detail_payload(
        "12", "s:40~l:46~t:12", "LA Clippers", {"city": "Los Angeles", "state": "CA"}
    ),
    ("basketball/nba", "88"): _detail_payload(
        "88", "s:40~l:46~t:88", "Boston Celtics", {"city": "Boston", "state": "MA"}
    ),
    # MLB reports the full US state name, not "CA" -- proves to_state_code's
    # normalization fires.
    ("baseball/mlb", "3"): _detail_payload(
        "3", "s:1~l:10~t:3", "Los Angeles Angels", {"city": "Anaheim", "state": "California", "zipCode": "92806"}
    ),
    ("baseball/mlb", "77"): _detail_payload(
        "77", "s:1~l:10~t:77", "Boston Red Sox", {"city": "Boston", "state": "Massachusetts", "zipCode": "02215"}
    ),
    ("hockey/nhl", "8"): _detail_payload(
        "8", "s:70~l:90~t:8", "Los Angeles Kings", {"city": "Los Angeles", "state": "CA"}
    ),
}


def _roster_payload(uid: str, display_name: str, athletes: list[dict[str, Any]]) -> dict[str, Any]:
    """`rec_path="team"` roster shape -- `uid`/`displayName` travel with `athletes`
    in the SAME sub-object, unlike the old `rec_path="team.athletes"` design this
    tutorial replaced."""
    return {"team": {"uid": uid, "displayName": display_name, "athletes": athletes}}


# Fictional player names throughout -- these fixtures don't need to track a
# real, ever-changing roster.
ROSTER_PAYLOADS: dict[tuple[str, str], dict[str, Any]] = {
    ("football/nfl", "13"): _roster_payload(
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
    ("basketball/nba", "13"): _roster_payload(
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
            },
            {
                "id": "p3b",
                "fullName": "Devon Wray",
                "active": True,
                "position": {"abbreviation": "G"},
                "contract": {},
                "experience": {"years": 2},
                "age": 24,
                "birthPlace": {"city": "Denver", "state": "CO"},
            },
        ],
    ),
    ("basketball/nba", "12"): _roster_payload(
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
            },
            {
                "id": "p7b",
                "fullName": "Milo Okafor",
                "active": True,
                "position": {"abbreviation": "F"},
                "contract": {},
                "experience": {"years": 1},
                "age": 21,
                "birthPlace": {"city": "Los Angeles", "state": "CA"},
            },
        ],
    ),
    ("baseball/mlb", "3"): _roster_payload(
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
                # Same metro city NAME as a real CA city but the WRONG
                # state -- the birth_state equality guard must exclude
                # this one from the homegrown board.
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
    ("hockey/nhl", "8"): _roster_payload(
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
            },
            {
                "id": "p6b",
                "fullName": "Finn Baptiste",
                "active": True,
                "position": {"abbreviation": "W"},
                "contract": {},
                "experience": {"years": 1},
                "age": 20,
                "birthPlace": {"city": "Toronto", "state": "ON"},
            },
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
    """Serves CountriesNow reference data, the four team-list feeds, per-(sport, id) detail
    drills, then roster drills."""
    req = httpx.Request("GET", url)

    if "countriesnow.space" in url:
        for key, payload in COUNTRIESNOW_PAYLOADS.items():
            if f"country={key}" in url:
                return httpx.Response(200, text=json.dumps(payload), request=req)
        return httpx.Response(200, text=json.dumps({"error": True, "data": {}}), request=req)

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
    """Wipe per-class inc_dict + parked snapshot between tests."""
    for cls in classes:
        cls.inc_dict.clear()
        if "_tideweaver_snapshot" in cls.__dict__:
            try:
                delattr(cls, "_tideweaver_snapshot")
            except AttributeError:
                pass


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
    _reset_registries(state_sports.StateRef)

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
    _reset_registries(state_sports.StateRef)

    with pytest.raises(SystemExit) as exc_info:
        await state_sports.fetch_state_code_map()

    assert exc_info.value.code != 0

    captured = capsys.readouterr()
    assert state_sports.REFERENCE_API_ERROR in captured.out
    assert captured.out.strip().isascii()


@pytest.mark.asyncio
async def test_discover_state_teams_filters_by_venue_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """Discover -> whole-list detail fan-out -> state-equality filter builds plain
    matched-team dicts, with no `Team.inc_dict` join-back involved."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fetch, "execute_request", mock_espn_execute_request)

    matched_teams, no_venue_total = await state_sports.discover_state_teams("CA", TEST_STATE_CODE_MAP)

    # State filter picked all five CA teams -- including the Clippers (whose
    # venue address is CA regardless of any brand string), and the MLB
    # Angels (whose raw feed says "California", not "CA" -- proves
    # to_state_code fired). Dallas (TX), Boston (MA x2), and the venue-less
    # Ghost Team were correctly excluded.
    assert len(matched_teams) == 5
    assert no_venue_total == 1
    leagues_found = {team["league"] for team in matched_teams}
    assert leagues_found == {"NFL", "NBA", "MLB", "NHL"}

    by_uid = {team["uid"]: team for team in matched_teams}
    clippers = by_uid["s:40~l:46~t:12"]
    assert clippers["team_name"] == "LA Clippers"
    assert clippers["roster_path"] == "basketball/nba/teams/12?enable=roster"

    # uid disambiguates the NFL/NBA numeric id=13 collision -- both teams
    # appear distinctly, keyed by their own uid.
    chargers = by_uid["s:20~l:28~t:13"]
    lakers = by_uid["s:40~l:46~t:13"]
    assert chargers["team_name"] == "Los Angeles Chargers"
    assert lakers["team_name"] == "Los Angeles Lakers"
    assert chargers["league"] == "NFL"
    assert lakers["league"] == "NBA"


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
    to_code = functools.partial(state_sports.to_state_code, TEST_STATE_CODE_MAP)

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
            "venue_state": state_sports.pluck("franchise.venue.address.state", chain=to_code),
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
            "venue_state": state_sports.pluck("franchise.venue.address.state", chain=to_code),
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

    matched_teams, no_venue_total = await state_sports.discover_state_teams("CA", TEST_STATE_CODE_MAP)

    ghost_present = any(team["team_name"] == "Ghost Team" for team in matched_teams)
    assert ghost_present is False
    assert no_venue_total == 1


@pytest.mark.asyncio
async def test_team_detail_incorp_returns_incorporator_list_for_single_row_league(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    """A single-team league's team-list build still yields a real IncorporatorList."""
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


@pytest.mark.asyncio
async def test_roster_watershed_produces_tagged_ndjson_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """The 3-current chain (file-mode `matched_teams` Stream -> `parent_current`
    `rosters` Stream -> `boards` Fjord) joins the two snapshots, flattens active
    players, and exports them to NDJSON with league/team_name attribution intact.

    Asserts against the exported NDJSON file, not `Roster._tideweaver_snapshot`
    -- the Fjord flush parks that snapshot on the `Roster` class object its OWN
    `outflow.py` load resolves (a distinct Python class from the one imported
    into this test's `state_sports` module), so the file is the only
    cross-module-safe read point (same pattern `examples/11-tideweaver/
    arb_scanner.py` uses).
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", mock_espn_execute_request)
    _reset_registries(Team, TeamDetail, MatchedTeam, TeamRoster, state_sports.Roster)

    matched_teams, _no_venue_total = await state_sports.discover_state_teams("CA", TEST_STATE_CODE_MAP)
    assert len(matched_teams) == 5

    matched_teams_file = tmp_path / "matched_teams.json"
    matched_teams_file.write_text(json.dumps(matched_teams), encoding="utf-8")

    out_file = tmp_path / "roster.ndjson"
    matched_teams_current = state_sports.Stream(
        name="matched_teams",
        cls=MatchedTeam,
        interval=60.0,
        on_error="isolate",
        incorp_params={"inc_file": str(matched_teams_file), "inc_code": "uid"},
    )
    rosters = state_sports.Stream(
        name="rosters",
        cls=TeamRoster,
        interval=60.0,
        on_error="isolate",
        parent_current="matched_teams",
        incorp_params={
            "inc_url": "https://site.api.espn.com/apis/site/v2/sports/{}",
            "inc_child": "roster_path",
            "rec_path": "team",
            "inc_code": "uid",
            "conv_dict": {
                "team_name": state_sports.pluck("displayName"),
                "athletes": state_sports.pluck("athletes"),
            },
            "timeout": 10,
        },
    )
    boards = state_sports.Fjord(
        name="boards",
        cls=state_sports.Roster,
        parent_currents=["rosters"],
        interval=60.0,
        on_error="isolate",
        export_params={"file_path": str(out_file), "format": "ndjson", "if_exists": "replace"},
    )

    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(seconds=8.0))
    ws = state_sports.Watershed.chain(
        window=window,
        currents=[matched_teams_current, rosters, boards],
        gate_mode="weir",
        outflow=str(state_sports.OUTFLOW_PATH),
        drain_timeout=8.0,
    )
    tw = state_sports.Tideweaver(ws, pass_interval=0.05)
    tides = [tide async for tide in tw.run()]

    fired_names = {name for tide in tides for name in tide.fired}
    assert fired_names == {"matched_teams", "rosters", "boards"}, f"all three currents must fire, got {fired_names}"

    assert out_file.exists(), "boards Fjord must have written the NDJSON export"
    lines = [ln for ln in out_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
    rows = [json.loads(ln) for ln in lines]
    assert len(rows) == 9  # 10 fetched across 5 rosters, 1 inactive MLB org player dropped

    by_name = {r["inc_name"]: r for r in rows}
    assert "Reed Calloway" not in by_name  # inactive MLB org player excluded

    ridge = by_name["Ridge Falcone"]
    assert ridge["league"] == "NFL"
    assert ridge["team_name"] == "Los Angeles Chargers"
    assert ridge["salary"] == 3809632
    assert ridge["tenure"] == 3
    assert ridge["turned_pro_at"] == 20
    assert ridge["salary_per_year"] == pytest.approx(3809632 / 3)
    assert ridge["inc_code"] == "NFL:p1"  # league-qualified inc_code

    teo = by_name["Teo Marsh"]
    assert teo["league"] == "NBA"
    assert teo["team_name"] == "Los Angeles Lakers"
    assert teo["salary"] == 54126450
    assert teo["salary_per_year"] == pytest.approx(54126450 / 7)

    # Missing age -> turned_pro_at is None, not a crash; missing salary -> None too.
    wyatt = by_name["Wyatt Kessler"]
    assert wyatt["salary"] is None
    assert wyatt["salary_per_year"] is None
    assert wyatt["turned_pro_at"] is None

    # Salary absent (MLB/NHL-style) -> both salary and salary_per_year None.
    wells = by_name["Wells Bramante"]
    assert wells["salary"] is None
    assert wells["salary_per_year"] is None

    otto = by_name["Otto Kwan"]
    assert otto["salary"] is None
    assert otto["salary_per_year"] is None
    assert otto["tenure"] == 15

    # Homegrown board: pure birth_state == region equality, no brand tables.
    # "Devon Wray" (CO) and "Wells Bramante" (Long Beach, but state=NY -- same
    # metro city NAME as a real CA city, wrong state) are both excluded.
    heroes = {r["inc_name"] for r in rows if r["birth_state"] == "CA"}
    assert heroes == {"Teo Marsh", "Milo Okafor", "Otto Kwan"}
    assert "Wells Bramante" not in heroes
    assert "Devon Wray" not in heroes

    assert all(":" in r["inc_code"] for r in rows)
