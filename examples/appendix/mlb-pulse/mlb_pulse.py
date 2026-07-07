"""
Appendix — Advanced Tideweaver: MLB AL Pulse
--------------------------------------------
Companion script for ``examples/appendix/mlb-pulse/README.md``.

Full developer loop in one script: **probe → run → tune → leaderboard**.

Phases:
  1. Pre-flight schema probe via ``Incorporator.architect()`` to verify
     rec_paths and field shapes before committing to the 25-second diamond run.
  2. ``LoggedTideweaver`` diamond:
       head   : MLBSchedule (today's game schedule)
       middle : [MLBAllTeam, MLBStandings, hitting_stream, pitching_stream]
       tail   : TeamPulseCard Fjord (joins 4 graph maps via outflow(state))
  3. Post-run ``architect.tune()`` feedback — emits concrete knob hints or
     "No tuning hints" on a clean run.
  4. Fixed-width console leaderboard of the 15 ranked AL Pulse Cards.

Row filtering: the parent ``al_teams`` Stream uses URL-level filtering
(``?sportId=1&leagueId=103``) to scope to the 15 American League teams
server-side. This is the framework's preferred row-filter primitive — see
the row-filter decision tree in the README sidebar.

Run with:
    python examples/appendix/mlb-pulse/mlb_pulse.py
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from incorporator import Incorporator, register_host_penstock
from incorporator.schema.converters import calc
from incorporator.tideweaver import (
    Fjord,
    LoggedTideweaver,
    Stream,
    Watershed,
)
from incorporator.tideweaver.architect import tune

HERE = Path(__file__).resolve().parent
OUTFLOW_PATH = HERE / "outflow.py"
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)

# Make the sidecar importable when this script is run via ``python -m`` or from
# a working directory other than HERE.  Python only auto-adds the script's
# directory to sys.path for ``python <script>`` invocations.
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

# Import class definitions + outflow() from the shared sidecar so both the
# Python entry and the CLI watershed.json form stay in lockstep.
from outflow import (  # noqa: E402
    MLBSTANDINGS_CONV_DICT,
    MLBAllTeam,
    MLBHitting,
    MLBPitching,
    MLBSchedule,
    MLBStandings,
    TeamPulseCard,
)

# ---------------------------------------------------------------------------
# Host throttle — also registered in outflow.py (CLI import path).
# Both registrations are idempotent; the last one wins the penstock registry.
# ---------------------------------------------------------------------------

register_host_penstock("statsapi.mlb.com", rate_per_sec=1.0)

# ---------------------------------------------------------------------------
# MLB Stats API endpoints — URL-level filtering scopes the parent to the
# 15 American League teams (leagueId=103). Children naturally drill that scope.
# ---------------------------------------------------------------------------

_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
_TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams?sportId=1&leagueId=103"
_STANDINGS_URL = "https://statsapi.mlb.com/api/v1/standings?leagueId=103"
_HITTING_SAMPLE_URL = "https://statsapi.mlb.com/api/v1/teams/147/stats?group=hitting&stats=season&season=2026"
_HITTING_URL = "https://statsapi.mlb.com/api/v1/teams/{}/stats?group=hitting&stats=season&season=2026"
_PITCHING_URL = "https://statsapi.mlb.com/api/v1/teams/{}/stats?group=pitching&stats=season&season=2026"


# ---------------------------------------------------------------------------
# Phase 1 — Pre-flight schema probe
# ---------------------------------------------------------------------------


async def _probe() -> None:
    """Run architect() against the five source endpoints and print schemas."""
    print("\nPhase 1 - Pre-flight schema probe via Incorporator.architect()...\n")
    sources = {
        "schedule": {
            "inc_url": _SCHEDULE_URL,
            "rec_path": "dates.0.games",
        },
        "al_teams": {
            "inc_url": _TEAMS_URL,
            "rec_path": "teams",
        },
        "standings": {
            "inc_url": _STANDINGS_URL,
            "rec_path": "records",
        },
        "hitting_sample": {
            "inc_url": _HITTING_SAMPLE_URL,
            "rec_path": "stats.0.splits.0",
        },
    }
    plan = await Incorporator.architect(sources, output="plan")
    if plan is None:
        print("  (probe skipped - architect returned None)")
        return
    for spec in plan.currents:
        field_count = len(spec.conv_dict_template)
        print(f"  {spec.class_name}: {field_count} fields inferred")
    print()


# ---------------------------------------------------------------------------
# Phase 2 — Diamond run
# ---------------------------------------------------------------------------


async def _run() -> list[dict]:
    """Build and run the Watershed diamond; return parsed output rows."""
    out_file = OUT / "al_pulse.ndjson"

    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(seconds=25))

    schedule_stream = Stream(
        name="schedule",
        cls=MLBSchedule,
        interval=8.0,
        on_error="isolate",
        # No conv_dict: outflow() does not read any MLBSchedule field, so no
        # source value requires coercion.  Pydantic accepts the raw payload as-is.
        incorp_params={
            "inc_url": _SCHEDULE_URL,
            "rec_path": "dates.0.games",
            "inc_code": "gamePk",
            "inc_name": "teams.home.team.name",
        },
    )
    al_teams_stream = Stream(
        name="al_teams",
        cls=MLBAllTeam,
        interval=20.0,
        on_error="isolate",
        # No conv_dict: the URL filter (?leagueId=103) scopes to the 15 AL teams
        # server-side, so no post-fetch row filtering is needed. outflow() reads
        # the parent's snapshot directly for team-name lookups.
        incorp_params={
            "inc_url": _TEAMS_URL,
            "rec_path": "teams",
            "inc_code": "id",
            "inc_name": "name",
        },
    )
    standings_stream = Stream(
        name="standings",
        cls=MLBStandings,
        interval=8.0,
        on_error="isolate",
        incorp_params={
            "inc_url": _STANDINGS_URL,
            "rec_path": "records",
            "inc_code": "division.id",
            "inc_name": "division.name",
            "conv_dict": MLBSTANDINGS_CONV_DICT,
        },
    )
    hitting_stream = Stream(
        name="hitting",
        cls=MLBHitting,
        interval=6.0,
        on_error="isolate",
        parent_current="al_teams",
        incorp_params={
            "inc_url": _HITTING_URL,
            "inc_child": "inc_code",
            "rec_path": "stats.0.splits.0",
            "inc_code": "team.id",
            "conv_dict": {"ops": calc(float, "stat.ops", default=0.0, target_type=float)},
        },
    )
    pitching_stream = Stream(
        name="pitching",
        cls=MLBPitching,
        interval=6.0,
        on_error="isolate",
        parent_current="al_teams",
        incorp_params={
            "inc_url": _PITCHING_URL,
            "inc_child": "inc_code",
            "rec_path": "stats.0.splits.0",
            "inc_code": "team.id",
            "conv_dict": {"era": calc(float, "stat.era", default=9.99, target_type=float)},
        },
    )
    pulse_fjord = Fjord(
        name="pulse",
        cls=TeamPulseCard,
        interval=4.0,
        on_error="isolate",
        export_params={
            "file_path": str(out_file),
            "format": "ndjson",
            "if_exists": "replace",
        },
    )

    watershed = Watershed.diamond(
        window=window,
        head=schedule_stream,
        middle=[al_teams_stream, standings_stream, hitting_stream, pitching_stream],
        tail=pulse_fjord,
        outflow=OUTFLOW_PATH,
        gate_mode="weir",
        drain_timeout=20.0,
    )

    print("Phase 2 - Building Watershed.diamond...\n")
    tw = LoggedTideweaver(watershed, enable_logging=True, logger_name="MLBPulse")
    collected_tides = []
    async for tide in tw.run():
        fired_str = ",".join(tide.fired) if tide.fired else "-"
        print(
            f"  Tide {tide.tide_number:3d} | fired: {fired_str:<48}"
            f" | skipped: {len(tide.skipped):2d} | {tide.duration_sec:.3f}s"
        )
        collected_tides.append(tide)

    rows: list[dict] = []
    if out_file.exists():
        lines = [ln for ln in out_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
        rows = [json.loads(ln) for ln in lines]
        print(f"\n  Wrote {len(rows)} Pulse Card rows to {out_file}\n")
    else:
        print("\n  (no output file produced - hitting/pitching streams may not have fired)\n")

    # Phase 3 — Post-run tuning feedback
    print("Phase 3 - Post-run feedback via architect.tune()...\n")
    report = tune(rejects=tw.rejects, tides=collected_tides, pass_interval=tw.pass_interval)
    meaningful_hints = [h for h in report.hints if h.severity in ("high", "med", "low")]
    if meaningful_hints:
        print(report.render())
    else:
        print("  No tuning hints - all knobs look well-tuned for this run.\n")

    return rows


# ---------------------------------------------------------------------------
# Phase 4 — Console leaderboard
# ---------------------------------------------------------------------------


def _print_leaderboard(rows: list[dict]) -> None:
    """Print a fixed-width leaderboard matching the README example block."""
    sep = "=" * 94
    thin = "-" * 94
    print(sep)
    print("  AL Pulse - 2026 season (live MLB Stats API)")
    print(sep)
    print(
        f"  {'Rank':<6}{'Team':<32}{'W-L':<8}{'PCT':<7}{'GB':<6}"
        f"{'OPS':<7}{'ERA':<7}{'PowerIdx':<10}{'Pythag':<8}{'+/-':>6}"
    )
    print(thin)
    for r in rows:
        wl = f"{r.get('wins', 0)}-{r.get('losses', 0)}"
        delta = r.get("pythag_delta", 0.0)
        delta_str = f"{'+' if delta >= 0 else ''}{delta:.3f}"
        print(
            f"  {r.get('power_rank', '?'):<6}{r.get('team_name', ''):<32}{wl:<8}"
            f"{r.get('win_pct', 0):.3f}  {r.get('games_back', 0):<6.1f}"
            f"{r.get('ops', 0):.3f}  {r.get('era', 0):.2f}   "
            f"{r.get('power_index', 0):<10.3f}{r.get('pythag', 0):.3f}  {delta_str:>6}"
        )
    print(sep)
    print("  +/- column: pythag - win_pct (positive = unlucky, negative = over-performing)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> None:
    await _probe()
    rows = await _run()
    if rows:
        _print_leaderboard(rows)
    else:
        print("(no rows to display - check out/logs/MLBPulse_error.log)")


if __name__ == "__main__":
    asyncio.run(main())
