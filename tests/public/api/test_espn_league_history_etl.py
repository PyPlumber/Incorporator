"""T-Tutorial mocked smoke test for the ESPN league-history appendix.

Loads `espn_league_history.py` via `load_sidecar` (unique importlib key) and
drives `main()` end-to-end against hand-faked ESPN payloads, zero network,
via `monkeypatch.setattr(fetch, "execute_request", ...)`.

Two scenarios, one shared fixture set:

- `test_public_run_...`: no `ESPN_S2`/`ESPN_SWID` env vars. The bootstrap
  season's `status.previousSeasons` includes an OLD_YEAR that 401s on the
  modern endpoint (no-cookie behavior, live-verified); with no cookies
  present that season is skipped with a printed note (never retried against
  `leagueHistory`).
- `test_private_run_...`: `ESPN_S2`/`ESPN_SWID` set. OLD_YEAR now 404s on the
  modern endpoint (cookies-present behavior, live-verified -- ESPN returns
  404, not 401, when a season predates the modern endpoint's own coverage
  window), and the retry against the cookie-gated `leagueHistory` list-root
  endpoint (`rec_path="0"`) succeeds regardless of that status code.

Both scenarios exercise: a playoff bye (home-only, no `away` key, permanently
`UNDECIDED`), a tiebreak-decided tie (`winner` still HOME/AWAY, margin can be
0), a negative D/ST `playerId` that resolves fine through `players_wl`, and
both `pointsOverrides` guard branches (key entirely absent / value `null` /
value present).

Both scenarios also assert that the OLD_YEAR probe's expected reject (401 or
404, per scenario) produces NO stderr noise: `quiet_expected_reject()` must
suppress BOTH independent framework channels -- the `warnings.warn(UserWarning,
...)` in `incorporator/base.py` AND the `logger.warning(...)` on the
`incorporator.io.fetch` logger -- around the two deliberately-probed
`Season.incorp()` calls in the discovery loop. A test that only checked the
printed skip message would still pass even if `quiet_expected_reject()` were
accidentally removed; `recwarn`/`caplog` are the actual regression guard.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any

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

_SEASON_RE = re.compile(r"/seasons/(\d+)/segments/0/leagues/")
_PLAYERS_RE = re.compile(r"/seasons/(\d+)/players")


def _players_response(headers: httpx.Headers, req: httpx.Request) -> httpx.Response:
    filt = json.loads(headers.get("X-Fantasy-Filter", "{}"))
    ids = filt.get("filterIds", {}).get("value", [])
    rows = [PLAYER_DB[pid] for pid in ids if pid in PLAYER_DB]
    return httpx.Response(200, text=json.dumps(rows), request=req)


def _make_mock(allow_cookies: bool):
    """Build a mock `execute_request` -- `allow_cookies` controls both the
    modern-endpoint failure status for `OLD_YEAR` and whether the historical
    `leagueHistory` fallback succeeds, mirroring ESPN's live-verified split:
    with cookies present the modern endpoint 404s (a season outside its own
    coverage window); with no cookies it 401s (an auth failure). Either way
    the historical retry is gated on `has_cookies` alone, not on which status
    code came back -- `allow_cookies=True` proves the fallback fires after a
    404 (private mode); `allow_cookies=False` proves the no-cookies branch is
    unaffected by a 401 (public mode).

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
            # OLD_YEAR (and anything else): 404 with cookies present (a season
            # outside the modern endpoint's own coverage window), 401 with no
            # cookies (an auth failure) -- both live-verified.
            if allow_cookies:
                resp = httpx.Response(404, text="not found", request=req)
                raise httpx.HTTPStatusError("404", request=req, response=resp)
            resp = httpx.Response(401, text="unauthorized", request=req)
            raise httpx.HTTPStatusError("401", request=req, response=resp)

        if "leagueHistory" in url:
            params = kwargs.get("params") or {}
            season_id = params.get("seasonId")
            if allow_cookies and season_id == OLD_YEAR and headers.get("Cookie"):
                return httpx.Response(200, text=json.dumps(_OLD_PAYLOAD), request=req)
            resp = httpx.Response(404, text="not found", request=req)
            raise httpx.HTTPStatusError("404", request=req, response=resp)

        return httpx.Response(200, text="{}", request=req)

    return mock_espn


def _reset_all() -> None:
    for cls in (
        espn_history.Season,
        espn_history.Standing,
        espn_history.Matchup,
        espn_history.Owner,
        espn_history.DraftPick,
        espn_history.PlayerName,
        espn_history.FranchiseCard,
        espn_history.SeasonTimelineRow,
        espn_history.RivalryRow,
        espn_history.RecordRow,
        espn_history.DraftTendencyRow,
        espn_history.SettingsRow,
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
    """No cookies: OLD_YEAR 401s on the modern endpoint and is skipped with a
    printed note, never retried against `leagueHistory`. CURRENT_YEAR +
    PREVIOUS_YEAR still fetch fine, producing all six views.

    Also proves `quiet_expected_reject()` suppresses BOTH the reject
    `UserWarning` (base.py) and the `incorporator.io.fetch` WARNING log line
    around the OLD_YEAR probe -- a printed-message-only assertion would not
    catch a regression that dropped the logging half of the suppression."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ESPN_S2", raising=False)
    monkeypatch.delenv("ESPN_SWID", raising=False)
    monkeypatch.setenv("ESPN_LEAGUE_ID", FAKE_LEAGUE_ID)
    monkeypatch.setattr(fetch, "execute_request", _make_mock(allow_cookies=False))
    _reset_all()

    with caplog.at_level(logging.WARNING, logger="incorporator"):
        await espn_history.main()

    assert [w for w in recwarn.list if issubclass(w.category, UserWarning)] == []
    assert caplog.records == []

    captured = capsys.readouterr()
    assert captured.out.isascii()
    assert f"season {OLD_YEAR}: unavailable (no cookies) -- skipping" in captured.out
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
    recwarn: pytest.WarningsRecorder,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Cookies present: OLD_YEAR now 404s on the modern endpoint (the
    live-verified cookies-present failure mode), but the retry against the
    cookie-gated `leagueHistory` list-root (`rec_path="0"`) succeeds
    regardless of that status code, pulling in a third season.

    Also proves `quiet_expected_reject()` suppresses BOTH the reject
    `UserWarning` and the `incorporator.io.fetch` WARNING log line around
    the OLD_YEAR modern-endpoint 404 probe, even though the retry itself
    succeeds."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ESPN_S2", "fake-s2-value")
    monkeypatch.setenv("ESPN_SWID", "{FAKE-SWID}")
    monkeypatch.setenv("ESPN_LEAGUE_ID", FAKE_LEAGUE_ID)
    monkeypatch.setattr(fetch, "execute_request", _make_mock(allow_cookies=True))
    _reset_all()

    with caplog.at_level(logging.WARNING, logger="incorporator"):
        await espn_history.main()

    assert [w for w in recwarn.list if issubclass(w.category, UserWarning)] == []
    assert caplog.records == []

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
