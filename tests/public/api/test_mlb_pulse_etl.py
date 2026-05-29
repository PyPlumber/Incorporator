"""T-Tutorial smoke test for the MLB AL East Pulse Tideweaver appendix.

Patches ``execute_request`` to return canned MLB Stats API payloads and drives
the diamond end-to-end (head + 4 middles + tail Fjord).  hitting_stream and
pitching_stream use Stream(parent_current='all_teams', parent_filter=...) —
the mock tick dispatcher resolves parent snapshots the same way the real
scheduler does.

Asserts:
- Exactly 5 output rows (one per AL East team)
- Sorted by power_index descending
- All 5 inc_codes in {139, 141, 147, 110, 111}
- Presence of power_index, pythag, pythag_delta fields
"""

from __future__ import annotations

import json
import operator
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest

from incorporator.io import fetch
from incorporator.observability.tideweaver import (
    Current,
    Export,
    Fjord,
    Stream,
    Tideweaver,
    Watershed,
)

# ---------------------------------------------------------------------------
# Resolve sidecar path so pulse_outflow.py imports cleanly even when pytest
# is invoked from the repo root (not from examples/appendix/mlb-pulse/).
# ---------------------------------------------------------------------------

_HERE = Path(__file__).resolve()
_SIDECAR_DIR = _HERE.parents[3] / "examples" / "appendix" / "mlb-pulse"

if str(_SIDECAR_DIR) not in sys.path:
    sys.path.insert(0, str(_SIDECAR_DIR))

from pulse_outflow import (  # noqa: E402
    _AL_EAST_DIVISION_ID,
    MLBAllTeam,
    MLBHitting,
    MLBPitching,
    MLBSchedule,
    MLBStandings,
    TeamPulseCard,
)

from incorporator.schema.converters import calc  # noqa: E402

# ---------------------------------------------------------------------------
# AL East team IDs the brief specifies
# ---------------------------------------------------------------------------

_AL_EAST_IDS = {139, 141, 147, 110, 111}

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

_TEAMS_PAYLOAD = {
    "teams": [
        {
            "id": 139,
            "name": "Tampa Bay Rays",
            "abbreviation": "TB",
            "teamName": "Rays",
            "shortName": "Tampa Bay",
            "division": {"id": 201},
            "league": {"id": 103},
        },
        {
            "id": 141,
            "name": "Toronto Blue Jays",
            "abbreviation": "TOR",
            "teamName": "Blue Jays",
            "shortName": "Toronto",
            "division": {"id": 201},
            "league": {"id": 103},
        },
        {
            "id": 147,
            "name": "New York Yankees",
            "abbreviation": "NYY",
            "teamName": "Yankees",
            "shortName": "NY Yankees",
            "division": {"id": 201},
            "league": {"id": 103},
        },
        {
            "id": 110,
            "name": "Baltimore Orioles",
            "abbreviation": "BAL",
            "teamName": "Orioles",
            "shortName": "Baltimore",
            "division": {"id": 201},
            "league": {"id": 103},
        },
        {
            "id": 111,
            "name": "Boston Red Sox",
            "abbreviation": "BOS",
            "teamName": "Red Sox",
            "shortName": "Boston",
            "division": {"id": 201},
            "league": {"id": 103},
        },
        # Non-AL-East team (should be filtered out in outflow)
        {
            "id": 116,
            "name": "Detroit Tigers",
            "abbreviation": "DET",
            "teamName": "Tigers",
            "shortName": "Detroit",
            "division": {"id": 205},
            "league": {"id": 103},
        },
    ]
}

_STANDINGS_PAYLOAD = {
    "records": [
        {
            "division": {"id": 201, "name": "American League East"},
            "lastUpdated": "2026-05-28T12:00:00Z",
            "teamRecords": [
                {
                    "team": {"id": 147, "name": "New York Yankees"},
                    "wins": 32,
                    "losses": 18,
                    "winningPercentage": ".640",
                    "gamesBack": "0.0",
                    "runsScored": 280,
                    "runsAllowed": 195,
                    "leagueRecord": {"wins": 32, "losses": 18, "pct": ".640"},
                },
                {
                    "team": {"id": 110, "name": "Baltimore Orioles"},
                    "wins": 28,
                    "losses": 22,
                    "winningPercentage": ".560",
                    "gamesBack": "4.0",
                    "runsScored": 240,
                    "runsAllowed": 210,
                    "leagueRecord": {"wins": 28, "losses": 22, "pct": ".560"},
                },
                {
                    "team": {"id": 111, "name": "Boston Red Sox"},
                    "wins": 25,
                    "losses": 25,
                    "winningPercentage": ".500",
                    "gamesBack": "7.0",
                    "runsScored": 220,
                    "runsAllowed": 220,
                    "leagueRecord": {"wins": 25, "losses": 25, "pct": ".500"},
                },
                {
                    "team": {"id": 139, "name": "Tampa Bay Rays"},
                    "wins": 24,
                    "losses": 26,
                    "winningPercentage": ".480",
                    "gamesBack": "8.0",
                    "runsScored": 200,
                    "runsAllowed": 215,
                    "leagueRecord": {"wins": 24, "losses": 26, "pct": ".480"},
                },
                {
                    "team": {"id": 141, "name": "Toronto Blue Jays"},
                    "wins": 20,
                    "losses": 30,
                    "winningPercentage": ".400",
                    "gamesBack": "12.0",
                    "runsScored": 185,
                    "runsAllowed": 230,
                    "leagueRecord": {"wins": 20, "losses": 30, "pct": ".400"},
                },
            ],
        },
        {
            "division": {"id": 202, "name": "American League Central"},
            "lastUpdated": "2026-05-28T12:00:00Z",
            "teamRecords": [],
        },
    ]
}


def _hitting_payload(team_id: int) -> dict:
    """Return a canned hitting-stats payload for the given team ID."""
    stats_by_team = {
        147: {
            "ops": "0.770",
            "obp": "0.340",
            "slg": "0.430",
            "avg": "0.255",
            "homeRuns": 85,
            "rbi": 280,
            "strikeOuts": 510,
            "baseOnBalls": 195,
        },
        110: {
            "ops": "0.773",
            "obp": "0.342",
            "slg": "0.431",
            "avg": "0.257",
            "homeRuns": 88,
            "rbi": 283,
            "strikeOuts": 505,
            "baseOnBalls": 192,
        },
        111: {
            "ops": "0.764",
            "obp": "0.337",
            "slg": "0.427",
            "avg": "0.253",
            "homeRuns": 80,
            "rbi": 265,
            "strikeOuts": 520,
            "baseOnBalls": 185,
        },
        139: {
            "ops": "0.701",
            "obp": "0.315",
            "slg": "0.386",
            "avg": "0.240",
            "homeRuns": 65,
            "rbi": 230,
            "strikeOuts": 540,
            "baseOnBalls": 165,
        },
        141: {
            "ops": "0.741",
            "obp": "0.328",
            "slg": "0.413",
            "avg": "0.248",
            "homeRuns": 72,
            "rbi": 248,
            "strikeOuts": 530,
            "baseOnBalls": 175,
        },
    }
    s = stats_by_team.get(
        team_id,
        {
            "ops": "0.700",
            "obp": "0.310",
            "slg": "0.390",
            "avg": "0.240",
            "homeRuns": 60,
            "rbi": 220,
            "strikeOuts": 550,
            "baseOnBalls": 160,
        },
    )
    return {
        "stats": [
            {
                "splits": [
                    {
                        "season": "2026",
                        "team": {"id": team_id},
                        "stat": s,
                    }
                ]
            }
        ]
    }


def _pitching_payload(team_id: int) -> dict:
    """Return a canned pitching-stats payload for the given team ID."""
    stats_by_team = {
        147: {
            "era": "3.14",
            "whip": "1.12",
            "wins": 32,
            "losses": 18,
            "strikeOuts": 480,
            "baseOnBalls": 155,
            "inningsPitched": "446.0",
            "earnedRuns": 156,
        },
        110: {
            "era": "3.87",
            "whip": "1.25",
            "wins": 28,
            "losses": 22,
            "strikeOuts": 440,
            "baseOnBalls": 170,
            "inningsPitched": "440.0",
            "earnedRuns": 190,
        },
        111: {
            "era": "4.21",
            "whip": "1.32",
            "wins": 25,
            "losses": 25,
            "strikeOuts": 415,
            "baseOnBalls": 180,
            "inningsPitched": "435.0",
            "earnedRuns": 204,
        },
        139: {
            "era": "4.05",
            "whip": "1.28",
            "wins": 24,
            "losses": 26,
            "strikeOuts": 420,
            "baseOnBalls": 175,
            "inningsPitched": "438.0",
            "earnedRuns": 197,
        },
        141: {
            "era": "4.42",
            "whip": "1.38",
            "wins": 20,
            "losses": 30,
            "strikeOuts": 395,
            "baseOnBalls": 190,
            "inningsPitched": "428.0",
            "earnedRuns": 210,
        },
    }
    s = stats_by_team.get(
        team_id,
        {
            "era": "5.00",
            "whip": "1.50",
            "wins": 18,
            "losses": 32,
            "strikeOuts": 380,
            "baseOnBalls": 200,
            "inningsPitched": "420.0",
            "earnedRuns": 233,
        },
    )
    return {
        "stats": [
            {
                "splits": [
                    {
                        "season": "2026",
                        "team": {"id": team_id},
                        "stat": s,
                    }
                ]
            }
        ]
    }


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
        # Extract team_id from URL: /api/v1/teams/{team_id}/stats?...
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
    ``_tick_stream`` logic (scheduler.py:942-958): resolve parent snapshot,
    apply parent_filter, call ``cls.incorp(inc_parent=filtered, ...)``.

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
                if isinstance(current.parent_filter, tuple):
                    attr, op, value = current.parent_filter
                    filtered = [r for r in pre_snap if op(getattr(r, attr, None), value)]
                elif callable(current.parent_filter):
                    filtered = [r for r in pre_snap if current.parent_filter(r)]
                else:
                    filtered = list(pre_snap)
                if not filtered:
                    return
                result = await current.cls.incorp(inc_parent=filtered, **current.incorp_params)
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


async def _prime_upstreams(
    strong_refs: dict,
    schedule_stream: Stream,
    all_teams_stream: Stream,
    standings_stream: Stream,
    hitting_stream: Stream,
    pitching_stream: Stream,
) -> None:
    """Prime all upstream snapshots before the scheduler run.

    Drives Stream.incorp() calls directly so that when the scheduler's first
    Fjord flush fires, the state dict is already populated with real data.
    This avoids the race where the Fjord fires before the parent-current
    Streams have populated MLBHitting/MLBPitching.
    """
    # 1. Simple streams — incorp + park snapshot
    for stream in (schedule_stream, all_teams_stream, standings_stream):
        result = await stream.cls.incorp(**stream.incorp_params)
        rows = list(result) if isinstance(result, list) else ([result] if result is not None else [])
        strong_refs[stream.cls] = rows
        stream.cls._tideweaver_snapshot = rows  # type: ignore[attr-defined]

    # 2. Parent-current Streams — resolve upstream snapshot, apply parent_filter, fan-out.
    # Lookup table mirrors scheduler._tick_stream's self._currents_by_name[current.parent_current]
    # (incorporator/observability/tideweaver/scheduler.py:944) — KeyError surfaces if a future
    # child Stream names an unknown parent, instead of silently shadowing onto all_teams.
    by_name = {
        "schedule": schedule_stream,
        "all_teams": all_teams_stream,
        "standings": standings_stream,
    }
    for stream in (hitting_stream, pitching_stream):
        upstream = by_name[stream.parent_current]
        pre_snap = getattr(upstream.cls, "_tideweaver_snapshot", None)
        if not pre_snap:
            continue
        if isinstance(stream.parent_filter, tuple):
            attr, op, value = stream.parent_filter
            filtered = [r for r in pre_snap if op(getattr(r, attr, None), value)]
        elif callable(stream.parent_filter):
            filtered = [r for r in pre_snap if stream.parent_filter(r)]
        else:
            filtered = list(pre_snap)
        if not filtered:
            continue
        result = await stream.cls.incorp(inc_parent=filtered, **stream.incorp_params)
        rows = list(result) if isinstance(result, list) else ([result] if result is not None else [])
        strong_refs[stream.cls] = rows
        stream.cls._tideweaver_snapshot = rows  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mlb_pulse_etl_produces_five_ranked_al_east_cards(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """MLB AL East Pulse diamond produces exactly 5 ranked Pulse Cards sorted by power_index desc.

    Proves:
    - Exactly 5 output rows (one per AL East team)
    - Sorted by power_index descending
    - All 5 inc_codes belong to {139, 141, 147, 110, 111}
    - power_index, pythag, and pythag_delta fields are present and numeric
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INCORPORATOR_RATE_LIMIT_BYPASS", "1")
    monkeypatch.setattr(fetch, "execute_request", _mock_mlb_pulse)
    _reset_all()
    strong_refs: dict = {}

    out_file = tmp_path / "al_east_pulse.ndjson"
    outflow_path = _SIDECAR_DIR / "pulse_outflow.py"

    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(seconds=12.0))

    schedule_stream = Stream(
        name="schedule",
        cls=MLBSchedule,
        interval=0.5,
        on_error="isolate",
        # No conv_dict — outflow() does not read any MLBSchedule field.
        incorp_params={
            "inc_url": "https://statsapi.mlb.com/api/v1/schedule?sportId=1",
            "rec_path": "dates.0.games",
            "inc_code": "gamePk",
            "inc_name": "teams.home.team.name",
        },
    )
    all_teams_stream = Stream(
        name="all_teams",
        cls=MLBAllTeam,
        interval=0.5,
        on_error="isolate",
        incorp_params={
            "inc_url": "https://statsapi.mlb.com/api/v1/teams?sportId=1",
            "rec_path": "teams",
            "inc_code": "id",
            "inc_name": "name",
            # Only division_id needs coercion for the AL East filter.
            "conv_dict": {"division_id": calc(int, "division.id", default=0, target_type=int)},
        },
    )
    standings_stream = Stream(
        name="standings",
        cls=MLBStandings,
        interval=0.5,
        on_error="isolate",
        # No conv_dict — outflow() reads teamRecords via local _safe_* helpers.
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
        parent_current="all_teams",
        parent_filter=("division_id", operator.eq, _AL_EAST_DIVISION_ID),
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
        parent_current="all_teams",
        parent_filter=("division_id", operator.eq, _AL_EAST_DIVISION_ID),
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
        middle=[all_teams_stream, standings_stream, hitting_stream, pitching_stream],
        tail=pulse_fjord,
        outflow=outflow_path,
        gate_mode="weir",
        drain_timeout=8.0,
    )
    tw = Tideweaver(ws, pass_interval=0.05)
    currents_by_name = {c.name: c for c in ws.currents}
    monkeypatch.setattr(tw, "_invoke_tick", _make_pulse_tick(tw, strong_refs, currents_by_name))

    # Pre-prime all upstream snapshots so the first Fjord flush has data.
    # Without this, the Fjord (interval=0.2) fires before the parent-current
    # Streams (interval=0.3) and outflow(state) returns [] — no file written.
    await _prime_upstreams(
        strong_refs, schedule_stream, all_teams_stream, standings_stream, hitting_stream, pitching_stream
    )

    [_ async for _ in tw.run()]

    # --- Assertions ---

    # 1. Output file written
    assert out_file.exists(), "pulse Fjord must have written al_east_pulse.ndjson"
    lines = [ln for ln in out_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert lines, "output file must not be empty"
    rows = [json.loads(ln) for ln in lines]

    # The Fjord may flush multiple ticks; take the last complete flush
    # (the final contiguous block of 5 rows sharing the same flush).
    # Simplest approach: find the last 5 rows.
    assert len(rows) >= 5, f"expected at least 5 rows, got {len(rows)}"
    last_five = rows[-5:]

    # 2. Exactly 5 rows when we look at the final flush
    assert len(last_five) == 5, f"final flush must have 5 rows, got {len(last_five)}"

    # 3. All inc_codes in the AL East set
    codes = {r["inc_code"] for r in last_five}
    assert codes == _AL_EAST_IDS, f"inc_codes must be exactly the AL East set, got {codes}"

    # 4. Sorted by power_index descending
    power_indices = [r["power_index"] for r in last_five]
    assert power_indices == sorted(power_indices, reverse=True), (
        f"rows must be sorted by power_index desc, got {power_indices}"
    )

    # 5. Required derived fields present and numeric
    for r in last_five:
        assert isinstance(r.get("power_index"), float), f"power_index must be float in {r}"
        assert isinstance(r.get("pythag"), float), f"pythag must be float in {r}"
        assert isinstance(r.get("pythag_delta"), float), f"pythag_delta must be float in {r}"
        assert r.get("power_rank") in range(1, 6), f"power_rank must be 1-5 in {r}"
