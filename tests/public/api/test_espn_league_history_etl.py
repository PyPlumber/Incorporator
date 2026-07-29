"""T-Tutorial mocked smoke test for the ESPN league-history appendix.

Loads `espn_league_history.py` via `load_sidecar` (unique importlib key) and
drives `main()` end-to-end against hand-faked ESPN payloads, zero network,
via `monkeypatch.setattr(fetch, "execute_request", ...)`. The pipeline is a
one-shot `Incorporator.fjord()`: `main()` runs the pre-fjord season-discovery
calls, then `fjord()` seeds six network-free `payload_list=` sources plus one
genuinely-networked `PlayerName` fan-out, then flushes `outflow.py`'s
`outflow(state)` once into the six NDJSON views.

Two scenarios, one shared fixture set. Endpoint choice is decided once, up
front, from `has_cookies` alone -- no probe, no retry:

- `test_public_run_...`: no `ESPN_S2`/`ESPN_SWID` env vars. Every remaining
  year fans out against the modern endpoint; OLD_YEAR 401s there (no-cookie
  behavior, live-verified) and is left out of the final season list --
  public mode never touches `leagueHistory` at all.
- `test_private_run_...`: `ESPN_S2`/`ESPN_SWID` set. Every remaining year
  (PREVIOUS_YEAR and OLD_YEAR) fans out directly against the cookie-gated
  `leagueHistory` list-root endpoint (`rec_path="0"`, `seasonId` embedded in
  each URL's own query string since `params=`/`headers=` are shared across
  a fan-out call, not per-URL) -- the modern endpoint is never probed for a
  non-current year in private mode. Both years resolve on the first try.

Both scenarios exercise: a playoff bye (home-only, no `away` key, permanently
`UNDECIDED`), a tiebreak-decided tie (`winner` still HOME/AWAY, margin can be
0), a negative D/ST `playerId` that resolves fine through `players_wl`, and
both `pointsOverrides` guard branches (key entirely absent / value `null` /
value present).

The public scenario also asserts that OLD_YEAR's expected 401 reject
surfaces NORMALLY through the framework's own two channels -- a
`warnings.warn(UserWarning, ...)` in `incorporator/base.py` and a
`logger.warning(...)` on the `incorporator.io.fetch` logger -- instead of
being suppressed: the pipeline defines no `warnings`/logger-suppression
ceremony, so a failed source's reject is exactly as visible here as
anywhere else in the examples tree. The private scenario has no failed
source to observe (both years resolve on the first try), so it does not
assert the warning channel.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from incorporator.io import fetch
from tests.helpers import load_sidecar

_HERE = Path(__file__).resolve()
_EXAMPLE_DIR = _HERE.parents[3] / "examples" / "appendix" / "espn-league-history"
espn_history = load_sidecar(_EXAMPLE_DIR / "espn_league_history.py", "espn_league_history_target")

# ---------------------------------------------------------------------------
# Fixture years, anchored off the real calendar so the test never rots.
# ---------------------------------------------------------------------------

CURRENT_YEAR = date.today().year
PREVIOUS_YEAR = CURRENT_YEAR - 1
OLD_YEAR = CURRENT_YEAR - 6

OWNER_A = "{OWNER-AAAA}"
OWNER_B = "{OWNER-BBBB}"
FAKE_LEAGUE_ID = "555000"

PLAYER_DB: dict[int, dict[str, Any]] = {
    1001: {"id": 1001, "fullName": "Wanda Reyes", "defaultPositionId": 3, "proTeamId": 9},
    -16003: {"id": -16003, "fullName": "Bears D/ST", "defaultPositionId": 16, "proTeamId": 3},
    2001: {"id": 2001, "fullName": "Quinn Napier", "defaultPositionId": 1, "proTeamId": 1},
    2002: {"id": 2002, "fullName": "Remy Cole", "defaultPositionId": 2, "proTeamId": 5},
    3001: {"id": 3001, "fullName": "Sasha Vale", "defaultPositionId": 4, "proTeamId": 7},
    4001: {"id": 4001, "fullName": "Otis Vance", "defaultPositionId": 5, "proTeamId": 12},
}


def _team(
    team_id: int, owner: str, name: str, wins: int, losses: int, pf: float, pa: float, seed: int, rank: int
) -> dict:
    return {
        "id": team_id,
        "primaryOwner": owner,
        "name": name,
        "divisionId": 0,
        "record": {"overall": {"wins": wins, "losses": losses, "ties": 0, "pointsFor": pf, "pointsAgainst": pa}},
        "playoffSeed": seed,
        "rankCalculatedFinal": rank,
    }


def _matchup(
    matchup_id: int,
    week: int,
    tier: str,
    winner: str,
    home_team: int,
    home_score: float,
    away_team: int | None = None,
    away_score: float | None = None,
) -> dict:
    row: dict[str, Any] = {
        "id": matchup_id,
        "matchupPeriodId": week,
        "playoffTierType": tier,
        "winner": winner,
        "home": {"teamId": home_team, "totalPoints": home_score},
    }
    if away_team is not None:
        row["away"] = {"teamId": away_team, "totalPoints": away_score}
    return row


def _pick(round_id: int, round_pick: int, overall: int, player_id: int, team_id: int, keeper: bool = False) -> dict:
    return {
        "roundId": round_id,
        "roundPickNumber": round_pick,
        "overallPickNumber": overall,
        "playerId": player_id,
        "teamId": team_id,
        "keeper": keeper,
    }


_MEMBERS = [
    {"id": OWNER_A, "displayName": "Alice Alpha"},
    {"id": OWNER_B, "displayName": "Bob Beta"},
]

_CURRENT_PAYLOAD = {
    "seasonId": CURRENT_YEAR,
    "teams": [
        _team(1, OWNER_A, "Team Alpha", 8, 5, 1200.5, 1100.25, 1, 1),
        _team(2, OWNER_B, "Team Beta", 5, 8, 1000.0, 1150.75, 2, 2),
    ],
    "schedule": [
        _matchup(1, 1, "NONE", "HOME", 1, 120.5, 2, 95.25),
        # Tiebreak-decided tie -- score is 100-100 but `winner` still resolves
        # to a side; margin is legitimately 0.
        _matchup(2, 2, "NONE", "AWAY", 1, 100.0, 2, 100.0),
        # Playoff bye -- home-only, no `away` key, permanently UNDECIDED.
        _matchup(3, 3, "NONE", "UNDECIDED", 1, 0.0),
        _matchup(4, 3, "WINNERS_BRACKET", "HOME", 1, 130.0, 2, 90.0),
        # Not-yet-played future week -- both sides 0.0, UNDECIDED.
        _matchup(5, 4, "NONE", "UNDECIDED", 1, 0.0, 2, 0.0),
    ],
    "members": _MEMBERS,
    "draftDetail": {
        "picks": [
            _pick(1, 1, 1, 1001, 1),
            _pick(1, 2, 2, -16003, 2),  # negative D/ST playerId
            _pick(2, 1, 11, 3001, 1),
        ]
    },
    "settings": {
        "scoringSettings": {
            # statId 53 (receptions) present but `pointsOverrides` key is
            # entirely absent -- one of the two guard branches.
            "scoringItems": [{"statId": 53, "points": 0.5}]
        },
        "scheduleSettings": {
            "playoffTeamCount": 4,
            "playoffSeedingRule": "TOTAL_POINTS_SCORED",
            "divisions": [{"id": 0, "name": "League"}],
        },
        "rosterSettings": {"lineupSlotCounts": {"0": 1, "2": 2, "3": 2, "4": 1, "5": 1, "20": 6, "21": 2, "23": 1}},
    },
    "status": {"previousSeasons": [PREVIOUS_YEAR, OLD_YEAR]},
}

_PREVIOUS_PAYLOAD = {
    "seasonId": PREVIOUS_YEAR,
    "teams": [
        _team(1, OWNER_A, "Team Alpha", 10, 3, 1400.0, 1250.0, 1, 1),
        _team(2, OWNER_B, "Team Beta", 3, 10, 980.0, 1200.0, 2, 2),
    ],
    "schedule": [
        _matchup(1, 1, "NONE", "HOME", 1, 150.0, 2, 60.0),
        _matchup(2, 2, "NONE", "HOME", 2, 85.0, 1, 80.0),
    ],
    "members": _MEMBERS,
    "draftDetail": {
        "picks": [
            _pick(1, 1, 1, 2001, 1),
            _pick(1, 2, 2, 2002, 2, keeper=True),
            _pick(3, 1, 25, 3001, 1),
            # Round-2 re-draft of the same player CURRENT_YEAR drafted round-1 --
            # proves times_drafted aggregates across seasons and the
            # already-resolved player is not re-fetched.
            _pick(2, 1, 12, 1001, 2),
        ]
    },
    "settings": {
        "scoringSettings": {
            # statId 53 present with `pointsOverrides` explicitly null --
            # the second guard branch.
            "scoringItems": [{"statId": 53, "points": 0.5, "pointsOverrides": None}]
        },
        "scheduleSettings": {"playoffTeamCount": 4, "playoffSeedingRule": "TOTAL_POINTS_SCORED", "divisions": []},
        "rosterSettings": {"lineupSlotCounts": {"0": 1, "2": 2, "3": 2, "4": 1, "5": 1, "20": 6, "21": 2, "23": 1}},
    },
    "status": {"previousSeasons": []},
}

# Historical (leagueHistory) list-root payload for OLD_YEAR -- only reachable
# with cookies present; `pointsOverrides` present WITH an override this time
# (the third guard branch).
_OLD_PAYLOAD = [
    {
        "seasonId": OLD_YEAR,
        "teams": [
            _team(1, OWNER_A, "Team Alpha", 2, 1, 300.0, 250.0, 1, 1),
            _team(2, OWNER_B, "Team Beta", 1, 2, 250.0, 300.0, 2, 2),
        ],
        "schedule": [_matchup(1, 1, "NONE", "HOME", 1, 100.0, 2, 80.0)],
        "members": _MEMBERS,
        "draftDetail": {"picks": [_pick(1, 1, 1, 4001, 1)]},
        "settings": {
            "scoringSettings": {"scoringItems": [{"statId": 53, "points": 0.5, "pointsOverrides": {"16": 1.0}}]},
            "scheduleSettings": {"playoffTeamCount": 2, "playoffSeedingRule": "TOTAL_POINTS_SCORED", "divisions": []},
            "rosterSettings": {"lineupSlotCounts": {"0": 1, "2": 1, "3": 1}},
        },
        "status": {"previousSeasons": []},
    }
]

# In private mode EVERY remaining year fans out against leagueHistory
# directly (not just a failed year) -- one list-root payload per season_id.
_HISTORY_PAYLOADS: dict[int, list[dict[str, Any]]] = {
    PREVIOUS_YEAR: [_PREVIOUS_PAYLOAD],
    OLD_YEAR: _OLD_PAYLOAD,
}

_SEASON_RE = re.compile(r"/seasons/(\d+)/segments/0/leagues/")
_PLAYERS_RE = re.compile(r"/seasons/(\d+)/players")


def _players_response(headers: httpx.Headers, req: httpx.Request) -> httpx.Response:
    filt = json.loads(headers.get("X-Fantasy-Filter", "{}"))
    ids = filt.get("filterIds", {}).get("value", [])
    rows = [PLAYER_DB[pid] for pid in ids if pid in PLAYER_DB]
    return httpx.Response(200, text=json.dumps(rows), request=req)


def _make_mock(allow_cookies: bool):
    """Build a mock `execute_request` -- `allow_cookies` selects which
    endpoint family the pipeline is expected to reach. With cookies present
    every remaining year fans out directly against `leagueHistory` (never
    touching the modern endpoint for a non-current year at all); with no
    cookies every remaining year fans out against the modern endpoint,
    where OLD_YEAR 401s (an auth failure, live-verified) and is left
    unresolved.

    `execute_request`'s real signature has no `headers=` kwarg -- headers are
    baked onto the `httpx.AsyncClient` at build time
    (`HTTPClientBuilder.build_client(headers=...)`) and forwarded here as the
    keyword-only `client=` argument, so this mock reads `kwargs["client"].headers`
    rather than a (nonexistent) `headers` kwarg.
    """

    async def mock_espn(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
        req = httpx.Request("GET", url)
        client: httpx.AsyncClient | None = kwargs.get("client")
        headers = client.headers if client is not None else httpx.Headers()

        players_match = _PLAYERS_RE.search(url)
        if players_match:
            return _players_response(headers, req)

        season_match = _SEASON_RE.search(url)
        if season_match:
            season = int(season_match.group(1))
            if season == CURRENT_YEAR:
                return httpx.Response(200, text=json.dumps(_CURRENT_PAYLOAD), request=req)
            if season == PREVIOUS_YEAR:
                return httpx.Response(200, text=json.dumps(_PREVIOUS_PAYLOAD), request=req)
            # Public mode never resolves a year outside the modern
            # endpoint's own coverage window -- OLD_YEAR 401s (an auth
            # failure, live-verified) and is never retried.
            resp = httpx.Response(401, text="unauthorized", request=req)
            raise httpx.HTTPStatusError("401", request=req, response=resp)

        if "leagueHistory" in url:
            # Private mode fans out directly against leagueHistory for
            # EVERY remaining year, not just a failed one -- the per-URL
            # seasonId differentiator is embedded directly in each URL's
            # own query string since params=/headers= are shared across a
            # fan-out call, not per-URL.
            query = parse_qs(urlsplit(url).query)
            season_id = int(query["seasonId"][0]) if "seasonId" in query else None
            if allow_cookies and headers.get("Cookie") and season_id in _HISTORY_PAYLOADS:
                return httpx.Response(200, text=json.dumps(_HISTORY_PAYLOADS[season_id]), request=req)
            resp = httpx.Response(404, text="not found", request=req)
            raise httpx.HTTPStatusError("404", request=req, response=resp)

        return httpx.Response(200, text="{}", request=req)

    return mock_espn


def _reset_all() -> None:
    # The six derived view classes (FranchiseCard, ...) are declared in
    # outflow.py and never imported into espn_league_history.py's own
    # namespace (see that module's import-block comment) -- fjord()'s
    # internal flush() always clears+rebuilds their inc_dict on every run
    # regardless, so only the seven SOURCE classes need resetting here.
    for cls in (
        espn_history.Season,
        espn_history.Standing,
        espn_history.Matchup,
        espn_history.Owner,
        espn_history.DraftPick,
        espn_history.PlayerName,
        espn_history.TeamGame,
    ):
        cls.inc_dict.clear()


@pytest.mark.asyncio
async def test_public_run_skips_401_season_and_builds_six_views(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    capsys: pytest.CaptureFixture[str],
    recwarn: pytest.WarningsRecorder,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No cookies: OLD_YEAR 401s on the modern endpoint and is left out of
    the final season list, never retried against `leagueHistory`.
    CURRENT_YEAR + PREVIOUS_YEAR still fetch fine, producing all six views.

    Also proves the OLD_YEAR fan-out reject surfaces through the framework's
    own two channels -- a `UserWarning` (base.py) and an `incorporator.io.fetch`
    WARNING log line -- since the pipeline no longer defines any suppression
    ceremony around it (Complaint 1's removal)."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ESPN_S2", raising=False)
    monkeypatch.delenv("ESPN_SWID", raising=False)
    monkeypatch.setenv("ESPN_LEAGUE_ID", FAKE_LEAGUE_ID)
    monkeypatch.setattr(fetch, "execute_request", _make_mock(allow_cookies=False))
    _reset_all()

    with caplog.at_level(logging.WARNING, logger="incorporator"):
        await espn_history.main()

    assert any(issubclass(w.category, UserWarning) for w in recwarn.list)
    assert any(r.levelno == logging.WARNING for r in caplog.records)

    captured = capsys.readouterr()
    assert captured.out.isascii()
    assert f"unresolved seasons (no data available): [{OLD_YEAR}]" in captured.out
    assert f"Fetched 2 season(s): [{PREVIOUS_YEAR}, {CURRENT_YEAR}]" in captured.out

    out_dir = _EXAMPLE_DIR / "out"
    franchise_rows = [json.loads(ln) for ln in (out_dir / "franchise_cards.ndjson").read_text().splitlines() if ln]
    assert {r["owner_guid"] for r in franchise_rows} == {OWNER_A, OWNER_B}
    for row in franchise_rows:
        assert row["seasons_played"] == 2

    records_rows = [json.loads(ln) for ln in (out_dir / "records_book.ndjson").read_text().splitlines() if ln]
    kinds = {r["kind"] for r in records_rows}
    assert kinds == {
        "highest_single_week_score",
        "lowest_single_week_score",
        "largest_margin_of_victory",
        "narrowest_margin_of_victory",
        "best_season_record",
        "worst_season_record",
        "highest_season_points_for",
        "lowest_season_points_for",
        "longest_win_streak",
        "longest_loss_streak",
    }
    narrowest = next(r for r in records_rows if r["kind"] == "narrowest_margin_of_victory")
    assert narrowest["value"] == 0.0  # the tiebreak-decided tie

    tendency_rows = [json.loads(ln) for ln in (out_dir / "draft_tendencies.ndjson").read_text().splitlines() if ln]
    tendency_kinds = {r["kind"] for r in tendency_rows}
    assert tendency_kinds == {"round1_position_mix", "most_drafted", "first_overall"}

    dst_pick = next(r for r in tendency_rows if r["kind"] == "first_overall" and r["season"] == CURRENT_YEAR)
    assert dst_pick["player_name"] == "Wanda Reyes"

    most_drafted = {r["player_id"]: r["times_drafted"] for r in tendency_rows if r["kind"] == "most_drafted"}
    assert most_drafted[1001] == 2  # drafted round-1 CURRENT_YEAR + round-2 PREVIOUS_YEAR

    settings_rows = [json.loads(ln) for ln in (out_dir / "settings_evolution.ndjson").read_text().splitlines() if ln]
    by_season = {r["season"]: r for r in settings_rows}
    assert by_season[CURRENT_YEAR]["ppr_points"] == 0.5  # pointsOverrides key entirely absent
    assert by_season[PREVIOUS_YEAR]["ppr_points"] == 0.5  # pointsOverrides explicitly null


@pytest.mark.asyncio
async def test_private_run_resolves_old_year_via_league_history(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Cookies present: PREVIOUS_YEAR and OLD_YEAR both fan out directly
    against the cookie-gated `leagueHistory` list-root endpoint
    (`rec_path="0"`, `seasonId` embedded in each URL's own query string
    since the fan-out shares `params=`/`headers=` across every URL) --
    the modern endpoint is never probed for either year in private mode.
    Both resolve on the first try, pulling in a third season alongside the
    CURRENT_YEAR bootstrap. No source fails in this scenario, so there is
    no warning channel to assert (see `test_public_run_...` for that
    regression guard)."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ESPN_S2", "fake-s2-value")
    monkeypatch.setenv("ESPN_SWID", "{FAKE-SWID}")
    monkeypatch.setenv("ESPN_LEAGUE_ID", FAKE_LEAGUE_ID)
    monkeypatch.setattr(fetch, "execute_request", _make_mock(allow_cookies=True))
    _reset_all()

    await espn_history.main()

    captured = capsys.readouterr()
    assert captured.out.isascii()
    assert f"Fetched 3 season(s): [{OLD_YEAR}, {PREVIOUS_YEAR}, {CURRENT_YEAR}]" in captured.out

    out_dir = _EXAMPLE_DIR / "out"
    franchise_rows = [json.loads(ln) for ln in (out_dir / "franchise_cards.ndjson").read_text().splitlines() if ln]
    for row in franchise_rows:
        assert row["seasons_played"] == 3

    settings_rows = [json.loads(ln) for ln in (out_dir / "settings_evolution.ndjson").read_text().splitlines() if ln]
    by_season = {r["season"]: r for r in settings_rows}
    assert by_season[OLD_YEAR]["ppr_points"] == 1.0  # pointsOverrides present with an override

    tendency_rows = [json.loads(ln) for ln in (out_dir / "draft_tendencies.ndjson").read_text().splitlines() if ln]
    old_year_pick = next(r for r in tendency_rows if r["kind"] == "first_overall" and r["season"] == OLD_YEAR)
    assert old_year_pick["player_name"] == "Otis Vance"
