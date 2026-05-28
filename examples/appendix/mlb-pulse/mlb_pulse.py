"""
Appendix — MLB AL East Pulse: Tideweaver Orchestration Showcase
---------------------------------------------------------------
Companion script for ``examples/appendix/mlb-pulse/README.md``.

A single 25-second run produces five "Pulse Cards" — one per AL East
team — composed live from four MLB Stats API endpoints joined inside a
Tideweaver ``Watershed.diamond``.  Demonstrates the full
Incorporator+Tideweaver developer loop:

  Phase 1 — Pre-flight probe via ``Incorporator.architect(output="report")``
            + per-class ``test()`` schema validation.
  Phase 2 — Runtime via ``LoggedTideweaver`` so Tides + RejectEntries
            route to disk JSONL alongside the Power Cards.
  Phase 3 — Post-run feedback via ``architect.tune()`` for concrete
            knob-tuning hints.

Diamond shape:
                head: live MLB schedule
                          │
        ┌─────────┬───────┴───────┬───────────┐
        ▼         ▼               ▼           ▼
   all_teams  standings    hitting Custom  pitching Custom
   (30 teams)  (live)      (T5 drill)      (T5 drill)
        │         │               │           │
        └─────────┴───────┬───────┴───────────┘
                          ▼
                  TeamPulseCard Fjord
        (joins 4 graph maps; Power Index + Pythagorean)

Two CustomCurrents filter ``MLBAllTeam.inc_dict`` to the 5 AL East
teams at tick time, then fire five concurrent ``httpx`` fetches via
``asyncio.gather`` — one per team per current per tick.

All ``calc`` sites are lambda-free per AGENTS.md H3 idiom:
framework's ``is_garbage_value`` pre-check handles null guards.
Conversions use builtins (``str.lower``), ``operator.itemgetter``,
and named helpers (``derive_ops``, ``above_power_threshold``,
``_home_team_id``, ``_away_team_id``) defined at module top.

Run from this folder:
    python mlb_pulse.py
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from operator import itemgetter
from pathlib import Path

from incorporator import (
    Fjord,
    Incorporator,
    Stream,
    Tideweaver,
    Watershed,
    register_host_penstock,
)
from incorporator.io.penstock import SustainedPenstock
from incorporator.observability.tideweaver import CustomCurrent
from incorporator.observability.tideweaver.architect import tune as architect_tune
from incorporator.observability.tideweaver.logged import LoggedTideweaver
from incorporator.schema.converters import calc

HERE = Path(__file__).resolve().parent

# Make the outflow sidecar importable when running from any cwd.
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from pulse_outflow import (  # noqa: E402
    AL_EAST_DIVISION_ID,
    MLBAllTeam,
    MLBHitting,
    MLBPitching,
    MLBSchedule,
    MLBStandings,
    TeamPulseCard,
)

# ---------------------------------------------------------------------------
# Polite throttle for statsapi.mlb.com — 1 req/sec (60 req/min).
# Register before any incorp() call so every fetch through this host is paced.
# ---------------------------------------------------------------------------
register_host_penstock("statsapi.mlb.com", SustainedPenstock(rate_per_sec=1.0))

CURRENT_SEASON: int = datetime.now(timezone.utc).year
AL_EAST_LEAGUE_ID: int = 103  # American League

OUTPUT_DIR = HERE / "out"
LOG_DIR = OUTPUT_DIR / "logs"
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)
PULSE_FILE = OUTPUT_DIR / "al_east_pulse.ndjson"
OUTFLOW_PATH = HERE / "pulse_outflow.py"

LOGGER_NAME = "MLBPulse"


# ---------------------------------------------------------------------------
# Lambda-free helper functions (referenced from conv_dicts below)
# ---------------------------------------------------------------------------


def derive_ops(obp: float, slg: float) -> float:
    """OPS = OBP + SLG, rounded to 3 decimals (MLB convention)."""
    return round(obp + slg, 3)


def above_power_threshold(k_per_9: float) -> bool:
    """K/9 above 9.0 qualifies as 'power pitcher' tier (heuristic)."""
    return k_per_9 > 9.0


def _home_team_id(teams: dict) -> int:
    """Drill teams.home.team.id from a schedule game's nested ``teams`` field."""
    return teams["home"]["team"]["id"]


def _away_team_id(teams: dict) -> int:
    """Drill teams.away.team.id from a schedule game's nested ``teams`` field."""
    return teams["away"]["team"]["id"]


# ---------------------------------------------------------------------------
# CustomCurrents — filter MLBAllTeam down to AL East, run T5 drills
# ---------------------------------------------------------------------------


class HittingDrillCurrent(CustomCurrent):
    """Filter MLBAllTeam → AL East, then fetch hitting stats concurrently."""

    cls: type[Incorporator] = MLBHitting

    async def tick(self, scheduler) -> None:  # type: ignore[override]
        import httpx as _httpx

        # MLBAllTeam.inc_dict is a WeakValueDictionary — entries can be GC'd
        # between the all_teams Stream tick and this one if no strong refs exist.
        # Fall back to the _tideweaver_snapshot list (which holds strong refs)
        # when inc_dict has been cleared.
        all_teams = list(MLBAllTeam.inc_dict.values())
        if not all_teams:
            all_teams = list(getattr(MLBAllTeam, "_tideweaver_snapshot", None) or [])
        al_east = [t for t in all_teams if getattr(t, "division_id", None) == AL_EAST_DIVISION_ID]
        if len(al_east) < 5:
            return

        async def _fetch_one(team) -> MLBHitting | None:
            tid = team.inc_code
            url = (
                f"https://statsapi.mlb.com/api/v1/teams/{tid}/stats"
                f"?stats=season&group=hitting&season={CURRENT_SEASON}"
            )
            try:
                async with _httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.get(url)
                    resp.raise_for_status()
                    data = resp.json()
                split = data["stats"][0]["splits"][0]
            except Exception:
                return None
            team_sub = split.get("team") or {}
            stat_sub = split.get("stat") or {}
            try:
                obp = float(stat_sub.get("obp") or 0.0)
                slg = float(stat_sub.get("slg") or 0.0)
            except (TypeError, ValueError):
                obp, slg = 0.0, 0.0
            try:
                avg = float(stat_sub.get("avg") or 0.0)
            except (TypeError, ValueError):
                avg = 0.0
            try:
                home_runs = int(stat_sub.get("homeRuns") or 0)
            except (TypeError, ValueError):
                home_runs = 0
            return MLBHitting.model_construct(
                inc_code=team_sub.get("id") or tid,
                team_id=team_sub.get("id") or tid,
                team_name=team_sub.get("name") or "",
                avg=avg,
                obp=obp,
                slg=slg,
                ops=derive_ops(obp, slg),
                home_runs=home_runs,
            )

        results = await asyncio.gather(*(_fetch_one(t) for t in al_east), return_exceptions=True)
        instances = [r for r in results if isinstance(r, MLBHitting)]
        if not instances:
            return
        MLBHitting.inc_dict.clear()
        for inst in instances:
            MLBHitting.inc_dict[inst.inc_code] = inst
        MLBHitting._tideweaver_snapshot = instances  # type: ignore[attr-defined]


class PitchingDrillCurrent(CustomCurrent):
    """Filter MLBAllTeam → AL East, then fetch pitching stats concurrently."""

    cls: type[Incorporator] = MLBPitching

    async def tick(self, scheduler) -> None:  # type: ignore[override]
        import httpx as _httpx

        # MLBAllTeam.inc_dict is a WeakValueDictionary — entries can be GC'd
        # between the all_teams Stream tick and this one if no strong refs exist.
        # Fall back to the _tideweaver_snapshot list (which holds strong refs)
        # when inc_dict has been cleared.
        all_teams = list(MLBAllTeam.inc_dict.values())
        if not all_teams:
            all_teams = list(getattr(MLBAllTeam, "_tideweaver_snapshot", None) or [])
        al_east = [t for t in all_teams if getattr(t, "division_id", None) == AL_EAST_DIVISION_ID]
        if len(al_east) < 5:
            return

        async def _fetch_one(team) -> MLBPitching | None:
            tid = team.inc_code
            url = (
                f"https://statsapi.mlb.com/api/v1/teams/{tid}/stats"
                f"?stats=season&group=pitching&season={CURRENT_SEASON}"
            )
            try:
                async with _httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.get(url)
                    resp.raise_for_status()
                    data = resp.json()
                split = data["stats"][0]["splits"][0]
            except Exception:
                return None
            team_sub = split.get("team") or {}
            stat_sub = split.get("stat") or {}
            try:
                era = float(stat_sub.get("era") or 0.0)
            except (TypeError, ValueError):
                era = 0.0
            try:
                whip = float(stat_sub.get("whip") or 0.0)
            except (TypeError, ValueError):
                whip = 0.0
            try:
                k9 = float(stat_sub.get("strikeoutsPer9Inn") or 0.0)
            except (TypeError, ValueError):
                k9 = 0.0
            try:
                saves = int(stat_sub.get("saves") or 0)
            except (TypeError, ValueError):
                saves = 0
            power_pitchers = above_power_threshold(k9)
            return MLBPitching.model_construct(
                inc_code=team_sub.get("id") or tid,
                team_id=team_sub.get("id") or tid,
                team_name=team_sub.get("name") or "",
                era=era,
                whip=whip,
                strikeouts_per9inn=k9,
                saves=saves,
                power_pitchers=power_pitchers,
            )

        results = await asyncio.gather(*(_fetch_one(t) for t in al_east), return_exceptions=True)
        instances = [r for r in results if isinstance(r, MLBPitching)]
        if not instances:
            return
        MLBPitching.inc_dict.clear()
        for inst in instances:
            MLBPitching.inc_dict[inst.inc_code] = inst
        MLBPitching._tideweaver_snapshot = instances  # type: ignore[attr-defined]


class StandingsCurrent(CustomCurrent):
    """Live standings fetcher via direct httpx, drilling records[0].teamRecords manually.

    The /standings endpoint wraps team records under ``records[0].teamRecords``
    — an array-index path the framework's rec_path doesn't support.  This
    current fetches directly via httpx and walks the nested shape by hand.
    """

    cls: type[Incorporator] = MLBStandings

    async def tick(self, scheduler) -> None:  # type: ignore[override]
        import httpx as _httpx

        url = (
            f"https://statsapi.mlb.com/api/v1/standings"
            f"?leagueId={AL_EAST_LEAGUE_ID}&divisionId={AL_EAST_DIVISION_ID}&season={CURRENT_SEASON}"
        )
        # One-shot fetch via httpx directly.  Note: this call bypasses
        # register_host_penstock (which only gates incorporator.io.fetch.execute_request).
        # At 1 call per ~5s tick, no throttling needed.  If this demo scaled up, the
        # call would need to route through execute_request to inherit the host penstock.
        try:
            async with _httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
        except Exception:
            return

        # Drill records[0].teamRecords manually.
        records = data.get("records") or []
        if not records:
            return
        team_records = records[0].get("teamRecords") or []
        if not team_records:
            return

        # Clear and repopulate (fresh standings each tick).  Accumulate
        # strong refs in `instances` during the loop — MLBStandings.inc_dict
        # is a WeakValueDictionary and entries would be GC'd before we
        # parked the snapshot otherwise.
        MLBStandings.inc_dict.clear()
        instances: list[MLBStandings] = []
        for tr in team_records:
            team_obj = tr.get("team") or {}
            streak_obj = tr.get("streak") or {}
            tid = team_obj.get("id", 0)
            wins = int(tr.get("wins") or 0)
            losses = int(tr.get("losses") or 0)
            try:
                games_back = float(tr.get("gamesBack") or 0.0)
            except (TypeError, ValueError):
                games_back = 0.0
            instance = MLBStandings.model_construct(
                inc_code=tid,
                team_id=tid,
                team_name=team_obj.get("name") or "",
                wins=wins,
                losses=losses,
                win_pct=float(tr.get("winningPercentage") or 0.0),
                games_back=games_back,
                runs_scored=int(tr.get("runsScored") or 0),
                runs_allowed=int(tr.get("runsAllowed") or 0),
                streak=streak_obj.get("streakCode") or "",
                over_500=wins > losses,
            )
            instances.append(instance)
            MLBStandings.inc_dict[tid] = instance
        # Park the list of strong refs so subsequent ticks (and the
        # downstream Fjord's state["MLBStandings"]) can read them.
        MLBStandings._tideweaver_snapshot = instances  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Console rendering — final leaderboard
# ---------------------------------------------------------------------------


def _print_leaderboard(path: Path) -> None:
    """Render the Power Map as a fixed-width console table."""
    if not path.exists():
        print(f"\n⚠️  No Pulse Cards written to {path}.")
        return
    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    if not lines:
        print(f"\n⚠️  {path} is empty.")
        return

    # Take the latest flush group (the Fjord emits multiple times; the most
    # recent N rows where N == AL East team count are the freshest).
    rows = [json.loads(ln) for ln in lines]
    seen_teams: set[int] = set()
    latest: list[dict] = []
    for r in reversed(rows):
        tid = r.get("inc_code")
        if tid in seen_teams:
            continue
        seen_teams.add(tid)
        latest.append(r)
        if len(latest) >= 5:
            break
    latest.sort(key=itemgetter("power_index"), reverse=True)

    print()
    print("═" * 96)
    print(f"  🏟️  AL East Pulse — {CURRENT_SEASON} season (live MLB Stats API)")
    print("═" * 96)
    print(f"  {'Rank':<5} {'Team':<25} {'W-L':>9} {'PCT':>6} {'GB':>5} {'OPS':>6} {'ERA':>5} "
          f"{'PowerIdx':>9} {'Pythag':>7} {'±':>6}")
    print("  " + "─" * 94)
    for i, row in enumerate(latest, 1):
        team = row.get("team", "—")[:23]
        s = row.get("standing") or {}
        h = row.get("hitting") or {}
        p = row.get("pitching") or {}
        wl = f"{s.get('wins', 0)}-{s.get('losses', 0)}"
        pct = s.get("win_pct") or 0.0
        gb = s.get("games_back") or 0.0
        ops = h.get("ops") or 0.0
        era = p.get("era") or 0.0
        pidx = row.get("power_index", 0.0)
        pyth = row.get("pythag", 0.0)
        delta = row.get("pythag_delta", 0.0)
        delta_marker = f"{delta:+.3f}"
        print(
            f"  {i:<5} {team:<25} {wl:>9} {pct:>6.3f} {gb:>5.1f} {ops:>6.3f} {era:>5.2f} "
            f"{pidx:>9.3f} {pyth:>7.3f} {delta_marker:>6}"
        )
    print("═" * 96)
    print(f"  ± column: pythag − win_pct (positive = unlucky, negative = over-performing)")


# ---------------------------------------------------------------------------
# Main — three-phase loop
# ---------------------------------------------------------------------------


async def main() -> None:
    # Ensure emoji-containing prints work on Windows cp1252 terminals.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"⚾  MLB AL East Pulse — {today}")
    print(f"    Throttle: 1.0 req/sec on statsapi.mlb.com (60 req/min, polite).\n")

    # --- Phase 1: Pre-flight probe ------------------------------------------
    print("🔍 Phase 1 — Pre-flight schema probe via Incorporator.architect()...")
    print("    (multi-source overview: schemas + topology recommendation)\n")
    sources = {
        "schedule": f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}",
        "all_teams": "https://statsapi.mlb.com/api/v1/teams?sportId=1",
        "standings": (
            f"https://statsapi.mlb.com/api/v1/standings"
            f"?leagueId={AL_EAST_LEAGUE_ID}&divisionId={AL_EAST_DIVISION_ID}&season={CURRENT_SEASON}"
        ),
        "hitting_sample": (
            f"https://statsapi.mlb.com/api/v1/teams/147/stats"
            f"?stats=season&group=hitting&season={CURRENT_SEASON}"
        ),
        "pitching_sample": (
            f"https://statsapi.mlb.com/api/v1/teams/147/stats"
            f"?stats=season&group=pitching&season={CURRENT_SEASON}"
        ),
    }
    try:
        # excl_lst drops the MLB-wide "copyright" watermark field that
        # otherwise shows up as a spurious cross-source overlap signal
        # in architect()'s topology recommendation.
        report = await Incorporator.architect(
            sources=sources,
            output="report",
            shared_kwargs={"excl_lst": ["copyright"]},
        )
        if report:
            print(report if isinstance(report, str) else f"(architect plan: {report})")
    except Exception as exc:  # noqa: BLE001
        print(f"  ⚠️  architect() probe skipped: {type(exc).__name__}: {exc}")

    # --- Phase 2: Construct + run the diamond -------------------------------
    print("\n🌀 Phase 2 — Building Watershed.diamond...")

    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(seconds=25))

    schedule_stream = Stream(
        name="schedule",
        cls=MLBSchedule,
        interval=4.0,
        incorp_params={
            "inc_url": f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}",
            "inc_code": "gamePk",
            "rec_path": "dates.0.games",
            "conv_dict": {
                "game_status": calc(itemgetter("abstractGameState"), "status", default="", target_type=str),
                "home_team_id": calc(_home_team_id, "teams", default=0, target_type=int),
                "away_team_id": calc(_away_team_id, "teams", default=0, target_type=int),
            },
        },
    )

    all_teams_stream = Stream(
        name="all_teams",
        cls=MLBAllTeam,
        interval=10.0,  # fires twice in the 25s window; second tick unblocks pulse after hitting/pitching populate
        incorp_params={
            "inc_url": "https://statsapi.mlb.com/api/v1/teams?sportId=1",
            "inc_code": "id",
            "rec_path": "teams",
            "conv_dict": {
                "division_id": calc(itemgetter("id"), "division", default=0, target_type=int),
                "league_id": calc(itemgetter("id"), "league", default=0, target_type=int),
                "venue_name": calc(itemgetter("name"), "venue", default="", target_type=str),
                "league_name": calc(itemgetter("name"), "league", default="", target_type=str),
                "abbr_lower": calc(str.lower, "abbreviation", default="", target_type=str),
            },
        },
    )

    # standings: CustomCurrent (not Stream) because rec_path doesn't handle
    # the array-index drill records[0].teamRecords — see StandingsCurrent
    # class above for the manual walker.
    standings_current = StandingsCurrent(name="standings", interval=5.0)

    hitting_current = HittingDrillCurrent(name="hitting", interval=10.0)
    pitching_current = PitchingDrillCurrent(name="pitching", interval=10.0)

    pulse_fjord = Fjord(
        name="pulse",
        cls=TeamPulseCard,
        interval=3.0,
        export_params={
            "file_path": str(PULSE_FILE),
            "format": "ndjson",
            "if_exists": "append",
        },
    )

    watershed = Watershed.diamond(
        window=window,
        head=schedule_stream,
        middle=[all_teams_stream, standings_current, hitting_current, pitching_current],
        tail=pulse_fjord,
        outflow=str(OUTFLOW_PATH),
        gate_mode="weir",
        drain_timeout=12.0,
    )

    # Wipe stale outputs from a prior run for a clean log.
    if PULSE_FILE.exists():
        PULSE_FILE.unlink()

    # LoggedTideweaver routes Tides + RejectEntries to disk JSONL (v1.2.1).
    tw = LoggedTideweaver(
        watershed,
        pass_interval=0.1,
        enable_logging=True,
        logger_name=LOGGER_NAME,
    )

    print(f"    Window: 25 s · pass_interval: 0.1 s · gate_mode: weir")
    print(f"    Output: {PULSE_FILE.relative_to(HERE)}\n")
    print("🌀 Running the diamond:\n")

    tides_collected: list = []
    async for tide in tw.run():
        tides_collected.append(tide)
        print(
            f"  Tide {tide.tide_number:3d} | fired: {','.join(tide.fired) or '-':<48} "
            f"| skipped: {len(tide.skipped):2d} | {tide.duration_sec:.3f}s"
        )

    print()
    if PULSE_FILE.exists():
        lines = [ln for ln in PULSE_FILE.read_text(encoding="utf-8").splitlines() if ln.strip()]
        print(f"✅ Wrote {len(lines)} Pulse Card rows to {PULSE_FILE.relative_to(HERE)}")

    # --- Phase 3: Post-run tuning -------------------------------------------
    print("\n🔧 Phase 3 — Post-run feedback via architect.tune()...\n")
    try:
        report = architect_tune(
            rejects=list(tw.rejects),
            tides=tides_collected,
            waves=[],  # waves aren't collected by Tideweaver directly; defensive-skip happens internally
            pass_interval=0.1,
        )
        if report and getattr(report, "hints", None):
            for hint in report.hints:
                sev = getattr(hint, "severity", "info").upper()
                knob = getattr(hint, "knob", "?")
                signal = getattr(hint, "signal", "")
                rationale = getattr(hint, "rationale", "") or ""
                print(f"  [{sev:>4}] {knob}: {signal}")
                if rationale:
                    excerpt = rationale.replace("\n", " ").strip()
                    if len(excerpt) > 140:
                        excerpt = excerpt[:140] + "…"
                    print(f"         → {excerpt}")
        else:
            print("  ✅ No tuning hints — all knobs look well-tuned for this run.")
    except Exception as exc:  # noqa: BLE001
        print(f"  ⚠️  architect.tune() skipped: {type(exc).__name__}: {exc}")

    # Final human-readable leaderboard.
    _print_leaderboard(PULSE_FILE)


if __name__ == "__main__":
    asyncio.run(main())
