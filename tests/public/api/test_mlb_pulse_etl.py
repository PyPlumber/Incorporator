"""T-Tutorial smoke test for the MLB AL Pulse Tideweaver appendix.

Patches ``execute_request`` to return canned MLB Stats API payloads and drives
the diamond end-to-end (head + 4 middles + tail Fjord). hitting_stream and
pitching_stream use ``Stream(parent_current='al_teams')`` for T5 fan-out.
Row filtering is server-side at the al_teams URL (``?leagueId=103``), so no
post-fetch filter is applied — the parent's 15 AL teams ARE the scope.

Asserts:
- 15 output rows (one per AL team) on the final flush
- Sorted by power_index descending
- All inc_codes in the AL team set
- Presence of power_index, pythag, pythag_delta fields
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from incorporator.io import fetch
from incorporator.tideweaver import (
    Current,
    Export,
    Fjord,
    Stream,
    Tideweaver,
    Watershed,
)
from incorporator.schema.converters import calc
from tests.helpers import load_sidecar

# ---------------------------------------------------------------------------
# Resolve sidecar path and load outflow.py via importlib with a unique
# sys.modules key so concurrent pytest sessions that also load other
# examples/*/outflow.py files (e.g. test_nascar_fantasy_etl.py) never
# receive the wrong module.
# ---------------------------------------------------------------------------

_HERE = Path(__file__).resolve()
_SIDECAR_DIR = _HERE.parents[3] / "examples" / "appendix" / "mlb-pulse"

_mlb_outflow = load_sidecar(_SIDECAR_DIR / "outflow.py", "mlb_pulse_outflow")

MLBAllTeam = _mlb_outflow.MLBAllTeam
MLBHitting = _mlb_outflow.MLBHitting
MLBPitching = _mlb_outflow.MLBPitching
MLBSchedule = _mlb_outflow.MLBSchedule
MLBStandings = _mlb_outflow.MLBStandings
TeamPulseCard = _mlb_outflow.TeamPulseCard

# ---------------------------------------------------------------------------
# AL team IDs by division (canonical, season-stable)
# ---------------------------------------------------------------------------

_AL_EAST_IDS = {110, 111, 139, 141, 147}  # BAL, BOS, TBR, TOR, NYY
_AL_CENTRAL_IDS = {114, 116, 118, 142, 145}  # CLE, DET, KCR, MIN, CWS
_AL_WEST_IDS = {108, 117, 133, 136, 140}  # LAA, HOU, OAK, SEA, TEX
_AL_TEAM_IDS = _AL_EAST_IDS | _AL_CENTRAL_IDS | _AL_WEST_IDS

# ---------------------------------------------------------------------------
# Canned response payloads
# ---------------------------------------------------------------------------

_SCHEDULE_PAYLOAD = {
    "dates": [
        {
            "games": [
                {
                    "gamePk": 1001,
                    "gameDate": "2026-05-28",
                    "status": {"detailedState": "Scheduled"},
                    "teams": {
                        "home": {"team": {"id": 147, "name": "New York Yankees"}},
                        "away": {"team": {"id": 111, "name": "Boston Red Sox"}},
                    },
                }
            ]
        }
    ]
}


def _make_team(team_id: int, name: str, abbr: str, short: str, division_id: int) -> dict:
    return {
        "id": team_id,
        "name": name,
        "abbreviation": abbr,
        "teamName": name.split()[-1],
        "shortName": short,
        "division": {"id": division_id},
        "league": {"id": 103},
    }


_TEAMS_PAYLOAD = {
    "teams": [
        # AL East (201)
        _make_team(110, "Baltimore Orioles", "BAL", "Baltimore", 201),
        _make_team(111, "Boston Red Sox", "BOS", "Boston", 201),
        _make_team(139, "Tampa Bay Rays", "TB", "Tampa Bay", 201),
        _make_team(141, "Toronto Blue Jays", "TOR", "Toronto", 201),
        _make_team(147, "New York Yankees", "NYY", "NY Yankees", 201),
        # AL Central (202)
        _make_team(114, "Cleveland Guardians", "CLE", "Cleveland", 202),
        _make_team(116, "Detroit Tigers", "DET", "Detroit", 202),
        _make_team(118, "Kansas City Royals", "KCR", "Kansas City", 202),
        _make_team(142, "Minnesota Twins", "MIN", "Minnesota", 202),
        _make_team(145, "Chicago White Sox", "CWS", "Chicago", 202),
        # AL West (200)
        _make_team(108, "Los Angeles Angels", "LAA", "LA Angels", 200),
        _make_team(117, "Houston Astros", "HOU", "Houston", 200),
        _make_team(133, "Athletics", "ATH", "Athletics", 200),
        _make_team(136, "Seattle Mariners", "SEA", "Seattle", 200),
        _make_team(140, "Texas Rangers", "TEX", "Texas", 200),
    ]
}


def _team_rec(team_id: int, name: str, wins: int, losses: int, gb: float, rs: int, ra: int) -> dict:
    pct = f"{wins / max(wins + losses, 1):.3f}"
    return {
        "team": {"id": team_id, "name": name},
        "wins": wins,
        "losses": losses,
        "winningPercentage": pct,
        "gamesBack": f"{gb:.1f}",
        "runsScored": rs,
        "runsAllowed": ra,
        "leagueRecord": {"wins": wins, "losses": losses, "pct": pct},
    }


_STANDINGS_PAYLOAD = {
    "records": [
        {
            "division": {"id": 201, "name": "American League East"},
            "lastUpdated": "2026-05-28T12:00:00Z",
            "teamRecords": [
                _team_rec(147, "New York Yankees", 32, 18, 0.0, 280, 195),
                _team_rec(110, "Baltimore Orioles", 28, 22, 4.0, 240, 210),
                _team_rec(111, "Boston Red Sox", 25, 25, 7.0, 220, 220),
                _team_rec(139, "Tampa Bay Rays", 24, 26, 8.0, 200, 215),
                _team_rec(141, "Toronto Blue Jays", 20, 30, 12.0, 185, 230),
            ],
        },
        {
            "division": {"id": 202, "name": "American League Central"},
            "lastUpdated": "2026-05-28T12:00:00Z",
            "teamRecords": [
                _team_rec(114, "Cleveland Guardians", 30, 20, 0.0, 260, 200),
                _team_rec(142, "Minnesota Twins", 27, 23, 3.0, 235, 215),
                _team_rec(116, "Detroit Tigers", 24, 26, 6.0, 210, 225),
                _team_rec(118, "Kansas City Royals", 22, 28, 8.0, 195, 230),
                _team_rec(145, "Chicago White Sox", 18, 32, 12.0, 170, 250),
            ],
        },
        {
            "division": {"id": 200, "name": "American League West"},
            "lastUpdated": "2026-05-28T12:00:00Z",
            "teamRecords": [
                _team_rec(117, "Houston Astros", 31, 19, 0.0, 270, 200),
                _team_rec(136, "Seattle Mariners", 28, 22, 3.0, 245, 215),
                _team_rec(140, "Texas Rangers", 26, 24, 5.0, 230, 220),
                _team_rec(108, "Los Angeles Angels", 22, 28, 9.0, 200, 235),
                _team_rec(133, "Athletics", 19, 31, 12.0, 180, 245),
            ],
        },
    ]
}


# Distinguishable OPS/ERA per team so power_index ordering is unambiguous.
_TEAM_STATS: dict[int, tuple[str, str]] = {
    # AL East
    147: ("0.770", "3.14"),
    110: ("0.773", "3.87"),
    111: ("0.764", "4.21"),
    139: ("0.701", "4.05"),
    141: ("0.741", "4.42"),
    # AL Central
    114: ("0.755", "3.50"),
    142: ("0.742", "3.78"),
    116: ("0.720", "4.10"),
    118: ("0.705", "4.30"),
    145: ("0.688", "4.65"),
    # AL West
    117: ("0.768", "3.32"),
    136: ("0.751", "3.65"),
    140: ("0.735", "3.95"),
    108: ("0.712", "4.15"),
    133: ("0.690", "4.55"),
}


def _hitting_payload(team_id: int) -> dict:
    """Return a canned hitting-stats payload for the given team ID."""
    ops, _era = _TEAM_STATS.get(team_id, ("0.700", "5.00"))
    s = {
        "ops": ops,
        "obp": "0.330",
        "slg": "0.410",
        "avg": "0.250",
        "homeRuns": 80,
        "rbi": 260,
        "strikeOuts": 510,
        "baseOnBalls": 180,
    }
    return {"stats": [{"splits": [{"season": "2026", "team": {"id": team_id}, "stat": s}]}]}


def _pitching_payload(team_id: int) -> dict:
    """Return a canned pitching-stats payload for the given team ID."""
    _ops, era = _TEAM_STATS.get(team_id, ("0.700", "5.00"))
    s = {
        "era": era,
        "whip": "1.25",
        "wins": 25,
        "losses": 25,
        "strikeOuts": 430,
        "baseOnBalls": 170,
        "inningsPitched": "440.0",
        "earnedRuns": 200,
    }
    return {"stats": [{"splits": [{"season": "2026", "team": {"id": team_id}, "stat": s}]}]}


# ---------------------------------------------------------------------------
# Mock execute_request
# ---------------------------------------------------------------------------


async def _mock_mlb_pulse(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Return canned MLB Stats API responses keyed on URL pattern."""
    if "schedule" in url:
        payload: Any = _SCHEDULE_PAYLOAD
    elif "/teams?sportId" in url:
        payload = _TEAMS_PAYLOAD
    elif "standings" in url:
        payload = _STANDINGS_PAYLOAD
    elif "group=hitting" in url:
        try:
            team_id = int(url.split("/teams/")[1].split("/")[0])
        except (IndexError, ValueError):
            team_id = 147
        payload = _hitting_payload(team_id)
    elif "group=pitching" in url:
        try:
            team_id = int(url.split("/teams/")[1].split("/")[0])
        except (IndexError, ValueError):
            team_id = 147
        payload = _pitching_payload(team_id)
    else:
        payload = {}
    req = httpx.Request("GET", url)
    return httpx.Response(200, text=json.dumps(payload), request=req)


# ---------------------------------------------------------------------------
# Registry reset helper
# ---------------------------------------------------------------------------


def _reset_all() -> None:
    """Wipe per-class inc_dict + parked snapshots to prevent test cross-contamination."""
    for cls in (MLBSchedule, MLBAllTeam, MLBStandings, MLBHitting, MLBPitching, TeamPulseCard):
        cls.inc_dict.clear()
        if "_tideweaver_snapshot" in cls.__dict__:
            try:
                delattr(cls, "_tideweaver_snapshot")
            except AttributeError:
                pass


# ---------------------------------------------------------------------------
# Tick dispatcher (mirrors test_tideweaver_routing_diamond.py pattern)
# ---------------------------------------------------------------------------


def _make_pulse_tick(tw: Tideweaver, strong_refs: dict, currents_by_name: dict[str, Any]) -> Any:
    """Build a tick dispatcher that handles Stream and Fjord nodes.

    For Stream nodes with ``parent_current`` set, mirrors the real scheduler's
    ``_tick_stream`` logic (scheduler.py): resolve parent snapshot, call
    ``cls.incorp(inc_parent=<snapshot>, ...)``. No post-fetch filter — the
    parent's URL already scoped the row set.

    For the Fjord branch: propagates strong-ref snapshots onto class attributes
    so ``_tick_fjord`` can find them when parent-current Streams haven't been
    scheduled yet this pass.
    """

    async def tick(current: Current) -> None:
        if isinstance(current, Stream):
            if current.parent_current is not None:
                parent_current = currents_by_name.get(current.parent_current)
                pre_snap = getattr(parent_current.cls, "_tideweaver_snapshot", None) if parent_current else None
                if not pre_snap:
                    return
                result = await current.cls.incorp(inc_parent=list(pre_snap), **current.incorp_params)
            else:
                result = await current.cls.incorp(**current.incorp_params)
            if isinstance(result, list):
                strong_refs[current.cls] = list(result)
            elif result is not None:
                strong_refs[current.cls] = [result]
            current.cls._tideweaver_snapshot = list(strong_refs.get(current.cls, []))  # type: ignore[attr-defined]
        elif isinstance(current, Fjord):
            # Propagate any pre-primed strong-refs onto class snapshots so the
            # real _tick_fjord can find them when parent-current Streams haven't
            # been scheduled this pass yet.
            for cls_ref, rows in strong_refs.items():
                if not getattr(cls_ref, "_tideweaver_snapshot", None) and rows:
                    cls_ref._tideweaver_snapshot = list(rows)  # type: ignore[attr-defined]
            await tw._tick_fjord(current)
            snapshot = getattr(current.cls, "_tideweaver_snapshot", None)
            if snapshot:
                strong_refs[current.cls] = list(snapshot)
        elif isinstance(current, Export):
            instance = strong_refs.get(current.cls, []) or list(current.cls.inc_dict.values())
            if not instance:
                return
            params = dict(current.export_params)
            params.setdefault("instance", instance)
            await current.cls.export(**params)

    return tick


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mlb_pulse_etl_produces_fifteen_ranked_al_cards(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """MLB AL Pulse diamond produces 15 ranked Pulse Cards sorted by power_index desc.

    Proves:
    - 15 output rows (one per AL team) on the final flush
    - Sorted by power_index descending
    - All inc_codes in the AL team set (5 East + 5 Central + 5 West)
    - power_index, pythag, and pythag_delta fields present and numeric
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", _mock_mlb_pulse)
    _reset_all()
    strong_refs: dict = {}

    out_file = tmp_path / "al_pulse.ndjson"
    outflow_path = _SIDECAR_DIR / "outflow.py"

    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(seconds=12.0))

    schedule_stream = Stream(
        name="schedule",
        cls=MLBSchedule,
        interval=0.5,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://statsapi.mlb.com/api/v1/schedule?sportId=1",
            "rec_path": "dates.0.games",
            "inc_code": "gamePk",
            "inc_name": "teams.home.team.name",
        },
    )
    al_teams_stream = Stream(
        name="al_teams",
        cls=MLBAllTeam,
        interval=0.5,
        on_error="isolate",
        # No conv_dict — URL filter (?leagueId=103) scopes server-side.
        incorp_params={
            "inc_url": "https://statsapi.mlb.com/api/v1/teams?sportId=1&leagueId=103",
            "rec_path": "teams",
            "inc_code": "id",
            "inc_name": "name",
        },
    )
    standings_stream = Stream(
        name="standings",
        cls=MLBStandings,
        interval=0.5,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://statsapi.mlb.com/api/v1/standings?leagueId=103",
            "rec_path": "records",
            "inc_code": "division.id",
            "inc_name": "division.name",
        },
    )
    hitting_stream = Stream(
        name="hitting",
        cls=MLBHitting,
        interval=0.3,
        on_error="isolate",
        parent_current="al_teams",
        incorp_params={
            "inc_url": "https://statsapi.mlb.com/api/v1/teams/{}/stats?group=hitting&stats=season&season=2026",
            "inc_child": "inc_code",
            "rec_path": "stats.0.splits.0",
            "inc_code": "team.id",
            "conv_dict": {"ops": calc(float, "stat.ops", default=0.0, target_type=float)},
        },
    )
    pitching_stream = Stream(
        name="pitching",
        cls=MLBPitching,
        interval=0.3,
        on_error="isolate",
        parent_current="al_teams",
        incorp_params={
            "inc_url": "https://statsapi.mlb.com/api/v1/teams/{}/stats?group=pitching&stats=season&season=2026",
            "inc_child": "inc_code",
            "rec_path": "stats.0.splits.0",
            "inc_code": "team.id",
            "conv_dict": {"era": calc(float, "stat.era", default=9.99, target_type=float)},
        },
    )
    pulse_fjord = Fjord(
        name="pulse",
        cls=TeamPulseCard,
        interval=0.2,
        on_error="isolate",
        export_params={
            "file_path": str(out_file),
            "format": "ndjson",
            "if_exists": "replace",
        },
    )

    ws = Watershed.diamond(
        window=window,
        head=schedule_stream,
        middle=[al_teams_stream, standings_stream, hitting_stream, pitching_stream],
        tail=pulse_fjord,
        outflow=outflow_path,
        gate_mode="weir",
        drain_timeout=8.0,
    )
    tw = Tideweaver(ws, pass_interval=0.05)
    currents_by_name = {c.name: c for c in ws.currents}
    monkeypatch.setattr(tw, "_invoke_tick", _make_pulse_tick(tw, strong_refs, currents_by_name))

    # The scheduler drives all 5 upstream Stream ticks under the Weir gate's
    # freshness watermark (last_wave_at set inside _tick_wrapper, scheduler.py:761).
    # Pre-priming via direct Stream.cls.incorp() was removed because it does not
    # set last_wave_at and therefore does not satisfy the Weir gate — see
    # flow.py:122-127 + scheduler.py:557-558/617/761/796.
    [_ async for _ in tw.run()]

    # --- Assertions ---

    # 1. Output file written
    assert out_file.exists(), "pulse Fjord must have written al_pulse.ndjson"
    lines = [ln for ln in out_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert lines, "output file must not be empty"
    rows = [json.loads(ln) for ln in lines]

    # The Fjord may flush multiple ticks; take the final 15 rows (final flush).
    assert len(rows) >= 15, f"expected at least 15 rows, got {len(rows)}"
    last_fifteen = rows[-15:]

    # 2. Exactly 15 rows in the final flush
    assert len(last_fifteen) == 15, f"final flush must have 15 rows, got {len(last_fifteen)}"

    # 3. All inc_codes in the AL team set
    codes = {r["inc_code"] for r in last_fifteen}
    assert codes == _AL_TEAM_IDS, f"inc_codes must be exactly the AL team set, got {codes}"

    # 4. Sorted by power_index descending
    power_indices = [r["power_index"] for r in last_fifteen]
    assert power_indices == sorted(power_indices, reverse=True), (
        f"rows must be sorted by power_index desc, got {power_indices}"
    )

    # 5. Required derived fields present and numeric
    for r in last_fifteen:
        assert isinstance(r.get("power_index"), float), f"power_index must be float in {r}"
        assert isinstance(r.get("pythag"), float), f"pythag must be float in {r}"
        assert isinstance(r.get("pythag_delta"), float), f"pythag_delta must be float in {r}"
        assert r.get("power_rank") in range(1, 16), f"power_rank must be 1-15 in {r}"
