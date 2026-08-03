"""T-Tutorial mocked smoke test for the ESPN league-history appendix.

Loads `espn_league_history.py` via `load_sidecar` (unique importlib key) and
drives `main()` end-to-end against hand-faked ESPN payloads, zero network,
via `monkeypatch.setattr(fetch, "execute_request", ...)`. The pipeline is a
one-shot `Incorporator.fjord()`: `main()` runs the pre-fjord season-discovery
calls (exactly two `Season.incorp` sites), then `fjord()` seeds six
network-free `payload_list=` sources plus one genuinely-networked
`PlayerName` fan-out, then flushes `outflow.py`'s `outflow(state)` once into
the six NDJSON views -- all six bare view classes, read back via
`espn_history.FranchiseCard.inc_dict` etc. after `main()` returns.

Two scenarios, one shared fixture set. Endpoint choice is decided once, up
front, from `has_cookies` alone -- no probe, no retry:

- `test_public_run_...`: no `ESPN_S2`/`ESPN_SWID` env vars. Every remaining
  year fans out against the modern endpoint; OLD_YEAR 401s there (no-cookie
  behavior, live-verified) and is left out of the final season list --
  public mode never touches `leagueHistory` at all.
- `test_private_run_...`: `ESPN_S2`/`ESPN_SWID` set. ONE call fans out
  directly against the cookie-gated `leagueHistory` endpoint with no
  `seasonId` query param -- its top-level JSON list response IS every OTHER
  completed season (PREVIOUS_YEAR and OLD_YEAR) in one body, no `rec_path`
  drilling -- the modern endpoint is never probed for a non-current year in
  private mode. Both years resolve on the first try.

Both scenarios exercise: a playoff bye (home-only, no `away` key, permanently
`UNDECIDED`), a tiebreak-decided tie (`winner` still HOME/AWAY, margin can be
0), a negative D/ST `playerId` that resolves fine through `players_wl`, ESPN's
own vacant/placeholder-pick sentinel (`playerId == -1, teamId == -1`, occupying
PREVIOUS_YEAR's actual pick-1 slot) proving the sentinel is filtered at the
source rather than falling through to an `Unknown` row, and both
`pointsOverrides` guard branches (key entirely absent / value `null` /
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
OWNER_C = "{OWNER-CCCC}"
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
    {"id": OWNER_C, "displayName": "Cleo Gamma"},
]

_CURRENT_PAYLOAD = {
    "seasonId": CURRENT_YEAR,
    "teams": [
        _team(1, OWNER_A, "Team Alpha", 8, 5, 1200.5, 1100.25, 1, 1),
        _team(2, OWNER_B, "Team Beta", 5, 8, 1000.0, 1150.75, 2, 2),
        # Owner C fields a team ONLY in the in-progress season -- exercises
        # the zero-division guard once seasons_played excludes it entirely.
        _team(3, OWNER_C, "Team Gamma", 0, 0, 0.0, 0.0, 0, 0),
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
    # latestScoringPeriod < finalScoringPeriod -- in-progress, mirrors the
    # live-probed 2026 preseason shape (0 < 17, real comparison not the
    # garbage-value default path).
    "status": {"previousSeasons": [PREVIOUS_YEAR, OLD_YEAR], "latestScoringPeriod": 0, "finalScoringPeriod": 17},
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
            # ESPN's own vacant/placeholder-pick sentinel occupying this season's
            # actual pick-1 slot (playerId == -1, teamId == -1) -- proves the
            # sentinel is filtered before it ever becomes a DraftPick row: no
            # `Unknown` first_overall row for PREVIOUS_YEAR, no -1 in most_drafted,
            # and no -1 in the X-Fantasy-Filter header sent to the players mock.
            _pick(1, 1, 1, -1, -1),
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
    # latestScoringPeriod > finalScoringPeriod -- complete. Reused verbatim by
    # both the public modern-endpoint fetch and the private leagueHistory list.
    "status": {"previousSeasons": [], "latestScoringPeriod": 18, "finalScoringPeriod": 17},
}

# Historical (leagueHistory) season payload for OLD_YEAR -- only reachable
# with cookies present; `pointsOverrides` present WITH an override this time
# (the third guard branch).
_OLD_SEASON_PAYLOAD = {
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
    # No scoring-period fields -- only reachable via cookie-gated leagueHistory
    # in this twin, exercising is_complete's default=True fallback for real.
    "status": {"previousSeasons": []},
}

# In private mode, ONE leagueHistory call (no seasonId query param) returns
# every OTHER completed season as ONE top-level JSON list -- no per-season
# fan-out, no rec_path drilling.
_HISTORY_PAYLOAD_LIST: list[dict[str, Any]] = [_PREVIOUS_PAYLOAD, _OLD_SEASON_PAYLOAD]

_SEASON_RE = re.compile(r"/seasons/(\d+)/segments/0/leagues/")
_PLAYERS_RE = re.compile(r"/seasons/(\d+)/players")


def _players_response(
    headers: httpx.Headers, req: httpx.Request, captured_filter_ids: list[int] | None = None
) -> httpx.Response:
    filt = json.loads(headers.get("X-Fantasy-Filter", "{}"))
    ids = filt.get("filterIds", {}).get("value", [])
    if captured_filter_ids is not None:
        captured_filter_ids.extend(ids)
    rows = [PLAYER_DB[pid] for pid in ids if pid in PLAYER_DB]
    return httpx.Response(200, text=json.dumps(rows), request=req)


def _make_mock(allow_cookies: bool, captured_filter_ids: list[int] | None = None):
    """Build a mock `execute_request` -- `allow_cookies` selects which
    endpoint family the pipeline is expected to reach. With cookies present,
    ONE `leagueHistory` call (no `seasonId` query param) returns every other
    completed season as one JSON list body (never touching the modern
    endpoint for a non-current year at all); with no cookies every remaining
    year fans out against the modern endpoint, where OLD_YEAR 401s (an auth
    failure, live-verified) and is left unresolved.

    `captured_filter_ids`, if given, collects every `playerId` the pipeline
    ever placed in the `X-Fantasy-Filter` header sent to the players endpoint --
    used to prove the vacant-pick sentinel (-1) never reaches `wanted_ids`.

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
            return _players_response(headers, req, captured_filter_ids)

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
            # Private mode fans out ONCE against leagueHistory with no
            # seasonId query param -- the endpoint's own top-level list IS
            # every other completed season (excluding the in-progress
            # current year).
            if allow_cookies and headers.get("Cookie"):
                return httpx.Response(200, text=json.dumps(_HISTORY_PAYLOAD_LIST), request=req)
            resp = httpx.Response(404, text="not found", request=req)
            raise httpx.HTTPStatusError("404", request=req, response=resp)

        return httpx.Response(200, text="{}", request=req)

    return mock_espn


def _reset_all() -> None:
    # fjord()'s internal flush() always clears+rebuilds every derived view
    # class's inc_dict on every run (FranchiseCard, ...), so only the six
    # SOURCE classes need resetting here between the two test runs.
    for cls in (
        espn_history.Season,
        espn_history.Owner,
        espn_history.Standing,
        espn_history.Matchup,
        espn_history.DraftPick,
        espn_history.PlayerName,
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
    captured_filter_ids: list[int] = []
    monkeypatch.setattr(
        fetch, "execute_request", _make_mock(allow_cookies=False, captured_filter_ids=captured_filter_ids)
    )
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
    assert {r["owner_guid"] for r in franchise_rows} == {OWNER_A, OWNER_B, OWNER_C}
    by_owner = {r["owner_guid"]: r for r in franchise_rows}
    # Only PREVIOUS_YEAR is complete -- CURRENT_YEAR is excluded from every
    # season-counting field even though it still produced rows everywhere else.
    assert by_owner[OWNER_A]["seasons_played"] == 1
    assert by_owner[OWNER_B]["seasons_played"] == 1
    assert by_owner[OWNER_A]["average_finish"] == 1.0
    assert by_owner[OWNER_B]["average_finish"] == 2.0
    # playoff_appearances is unfiltered by design -- CURRENT_YEAR's own decided
    # WINNERS_BRACKET matchup still counts (a real result, kept inclusive), so
    # against a seasons_played denominator of 1 this reads 100%.
    assert by_owner[OWNER_A]["playoff_rate"] == 1.0
    assert by_owner[OWNER_B]["playoff_rate"] == 1.0
    assert by_owner[OWNER_A]["has_current_season"] is True
    # Owner C fields a team ONLY in the in-progress season -- seasons_played
    # divides by zero without the guard this same edit adds.
    assert by_owner[OWNER_C]["seasons_played"] == 0
    assert by_owner[OWNER_C]["average_finish"] == 0.0
    assert by_owner[OWNER_C]["playoff_rate"] == 0.0
    assert by_owner[OWNER_C]["has_current_season"] is True

    season_timeline_rows = [
        json.loads(ln) for ln in (out_dir / "season_timeline.ndjson").read_text().splitlines() if ln
    ]
    assert all(r["is_complete"] is False for r in season_timeline_rows if r["season"] == CURRENT_YEAR)
    assert all(r["is_complete"] is True for r in season_timeline_rows if r["season"] == PREVIOUS_YEAR)

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
    assert dst_pick["player_name"] == "Wanda Reyes"  # the -16003 D/ST trap stays armed

    # PREVIOUS_YEAR's own pick-1 is ESPN's vacant/placeholder sentinel (playerId == -1)
    # -- it's filtered before ever becoming a DraftPick row, so PREVIOUS_YEAR simply
    # has NO first_overall row (not an `Unknown` one).
    first_overall_seasons = {r["season"] for r in tendency_rows if r["kind"] == "first_overall"}
    assert PREVIOUS_YEAR not in first_overall_seasons
    assert not any(r["kind"] == "first_overall" and r["player_name"] == "Unknown" for r in tendency_rows)

    most_drafted = {r["player_id"]: r["times_drafted"] for r in tendency_rows if r["kind"] == "most_drafted"}
    assert most_drafted[1001] == 2  # drafted round-1 CURRENT_YEAR + round-2 PREVIOUS_YEAR
    assert -1 not in most_drafted  # the sentinel never reaches most_drafted

    assert -1 not in captured_filter_ids  # the sentinel never reaches the X-Fantasy-Filter header

    settings_rows = [json.loads(ln) for ln in (out_dir / "settings_evolution.ndjson").read_text().splitlines() if ln]
    by_season = {r["season"]: r for r in settings_rows}
    assert by_season[CURRENT_YEAR]["ppr_points"] == 0.5  # pointsOverrides key entirely absent
    assert by_season[PREVIOUS_YEAR]["ppr_points"] == 0.5  # pointsOverrides explicitly null
    assert by_season[CURRENT_YEAR]["is_complete"] is False
    assert by_season[PREVIOUS_YEAR]["is_complete"] is True


@pytest.mark.asyncio
async def test_private_run_resolves_old_year_via_league_history(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Cookies present: ONE call against the cookie-gated `leagueHistory`
    endpoint (no `seasonId` query param) returns PREVIOUS_YEAR and OLD_YEAR
    together as one top-level JSON list -- the modern endpoint is never
    probed for either year in private mode. Both resolve on the first try,
    pulling in a third season alongside the CURRENT_YEAR bootstrap. No
    source fails in this scenario, so there is no warning channel to assert
    (see `test_public_run_...` for that regression guard)."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ESPN_S2", "fake-s2-value")
    monkeypatch.setenv("ESPN_SWID", "{FAKE-SWID}")
    monkeypatch.setenv("ESPN_LEAGUE_ID", FAKE_LEAGUE_ID)
    captured_filter_ids: list[int] = []
    monkeypatch.setattr(
        fetch, "execute_request", _make_mock(allow_cookies=True, captured_filter_ids=captured_filter_ids)
    )
    _reset_all()

    await espn_history.main()

    captured = capsys.readouterr()
    assert captured.out.isascii()
    assert f"Fetched 3 season(s): [{OLD_YEAR}, {PREVIOUS_YEAR}, {CURRENT_YEAR}]" in captured.out

    out_dir = _EXAMPLE_DIR / "out"
    franchise_rows = [json.loads(ln) for ln in (out_dir / "franchise_cards.ndjson").read_text().splitlines() if ln]
    by_owner = {r["owner_guid"]: r for r in franchise_rows}
    # PREVIOUS_YEAR + OLD_YEAR are complete; CURRENT_YEAR is excluded --
    # OLD_YEAR's completeness comes from is_complete's default=True fallback
    # (leagueHistory's status block carries no scoring-period fields).
    assert by_owner[OWNER_A]["seasons_played"] == 2
    assert by_owner[OWNER_B]["seasons_played"] == 2
    assert by_owner[OWNER_A]["average_finish"] == 1.0
    assert by_owner[OWNER_B]["average_finish"] == 2.0
    assert by_owner[OWNER_A]["playoff_rate"] == 0.5
    assert by_owner[OWNER_B]["playoff_rate"] == 0.5
    # Owner C only ever fielded a team in the in-progress season here too.
    assert by_owner[OWNER_C]["seasons_played"] == 0
    assert by_owner[OWNER_C]["average_finish"] == 0.0
    assert by_owner[OWNER_C]["playoff_rate"] == 0.0
    assert all(by_owner[g]["has_current_season"] is True for g in (OWNER_A, OWNER_B, OWNER_C))

    settings_rows = [json.loads(ln) for ln in (out_dir / "settings_evolution.ndjson").read_text().splitlines() if ln]
    by_season = {r["season"]: r for r in settings_rows}
    assert by_season[OLD_YEAR]["ppr_points"] == 1.0  # pointsOverrides present with an override
    assert by_season[CURRENT_YEAR]["is_complete"] is False
    assert by_season[PREVIOUS_YEAR]["is_complete"] is True
    assert by_season[OLD_YEAR]["is_complete"] is True  # default=True fallback, no status fields present

    # OLD_YEAR's lineupSlotCounts (3 keys) and PREVIOUS_YEAR's (8 keys) both land in
    # SettingsRow's one flush wave. The bare SettingsRow class infers its
    # `roster_slots` submodel from the UNION of keys across every row in that wave
    # (infer_dynamic_schema's cross-row dict-key-union merge), so OLD_YEAR's smaller
    # row gets null-padded with the 5 extra keys PREVIOUS_YEAR carries -- accepted
    # inference behavior (see this session's brief / CHANGELOG entry), not data loss:
    # OLD_YEAR's own 3 real values stay present and correct.
    old_year_slots = by_season[OLD_YEAR]["roster_slots"]
    assert all(old_year_slots[k] == v for k, v in {"0": 1, "2": 1, "3": 1}.items())
    padded_keys = set(old_year_slots) - {"0", "2", "3"}
    assert padded_keys and all(old_year_slots[k] is None for k in padded_keys)

    tendency_rows = [json.loads(ln) for ln in (out_dir / "draft_tendencies.ndjson").read_text().splitlines() if ln]
    old_year_pick = next(r for r in tendency_rows if r["kind"] == "first_overall" and r["season"] == OLD_YEAR)
    assert old_year_pick["player_name"] == "Otis Vance"

    # PREVIOUS_YEAR's own pick-1 is ESPN's vacant/placeholder sentinel (playerId == -1)
    # -- filtered before ever becoming a DraftPick row, so PREVIOUS_YEAR has NO
    # first_overall row (not an `Unknown` one) here either.
    first_overall_seasons = {r["season"] for r in tendency_rows if r["kind"] == "first_overall"}
    assert PREVIOUS_YEAR not in first_overall_seasons
    assert not any(r["kind"] == "first_overall" and r["player_name"] == "Unknown" for r in tendency_rows)

    most_drafted = {r["player_id"]: r["times_drafted"] for r in tendency_rows if r["kind"] == "most_drafted"}
    assert -1 not in most_drafted  # the sentinel never reaches most_drafted

    assert -1 not in captured_filter_ids  # the sentinel never reaches the X-Fantasy-Filter header
