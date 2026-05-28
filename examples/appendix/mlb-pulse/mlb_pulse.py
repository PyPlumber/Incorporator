"""
Appendix — Advanced Tideweaver: MLB AL East Pulse
--------------------------------------------------
Companion script for ``examples/appendix/mlb-pulse/README.md``.

Full developer loop in one script: **probe → run → tune → leaderboard**.

Phases:
  1. Pre-flight schema probe via ``Incorporator.architect()`` to verify
     rec_paths and field shapes before committing to the 25-second diamond run.
  2. ``LoggedTideweaver`` diamond:
       head   : MLBSchedule (today's game schedule)
       middle : [MLBAllTeam, MLBStandings, HittingDrillCurrent, PitchingDrillCurrent]
       tail   : TeamPulseCard Fjord (joins 4 graph maps via outflow(state))
  3. Post-run ``architect.tune()`` feedback — emits concrete knob hints or
     "No tuning hints" on a clean run.
  4. Fixed-width console leaderboard of the five ranked AL East Pulse Cards.

Run with:
    python examples/appendix/mlb-pulse/mlb_pulse.py
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from incorporator import Incorporator, SustainedPenstock, register_host_penstock
from incorporator.observability.tideweaver import (
    CustomCurrent,  # noqa: F401 — re-exported for sidecar
    Fjord,
    LoggedTideweaver,
    Stream,
    Watershed,
)
from incorporator.observability.tideweaver.architect import tune

HERE = Path(__file__).resolve().parent
OUTFLOW_PATH = HERE / "pulse_outflow.py"
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)

# Make the sidecar importable when this script is run via ``python -m`` or from
# a working directory other than HERE.  Python only auto-adds the script's
# directory to sys.path for ``python <script>`` invocations.
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

# Import class definitions + CustomCurrent subclasses + outflow() from the
# shared sidecar so both the Python entry and the CLI watershed.json form stay
# in lockstep.
from pulse_outflow import (  # noqa: E402
    ALL_TEAMS_CONV,
    SCHEDULE_CONV,
    STANDINGS_CONV,
    HittingDrillCurrent,
    MLBAllTeam,
    MLBHitting,
    MLBPitching,
    MLBSchedule,
    MLBStandings,
    PitchingDrillCurrent,
    TeamPulseCard,
)

# ---------------------------------------------------------------------------
# Host throttle — also registered in pulse_outflow.py (CLI import path).
# Both registrations are idempotent; the last one wins the penstock registry.
# ---------------------------------------------------------------------------

register_host_penstock("statsapi.mlb.com", SustainedPenstock(rate_per_sec=1.0))

# ---------------------------------------------------------------------------
# MLB Stats API endpoints
# ---------------------------------------------------------------------------

_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule?sportId=1"
_TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams?sportId=1"
_STANDINGS_URL = "https://statsapi.mlb.com/api/v1/standings?leagueId=103"
_HITTING_SAMPLE_URL = "https://statsapi.mlb.com/api/v1/teams/147/stats?group=hitting&stats=season&season=2026"


# ---------------------------------------------------------------------------
# Phase 1 — Pre-flight schema probe
# ---------------------------------------------------------------------------


async def _probe() -> None:
    """Run architect() against the five source endpoints and print schemas."""
    print("\nPhase 1 — Pre-flight schema probe via Incorporator.architect()...\n")
    sources = {
        "schedule": {
            "inc_url": _SCHEDULE_URL,
            "rec_path": "dates.0.games",
        },
        "all_teams": {
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
        print("  (probe skipped — architect returned None)")
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
    out_file = OUT / "al_east_pulse.ndjson"

    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(seconds=25))

    schedule_stream = Stream(
        name="schedule",
        cls=MLBSchedule,
        interval=8.0,
        on_error="isolate",
        incorp_params={
            "inc_url": _SCHEDULE_URL,
            "rec_path": "dates.0.games",
            "inc_code": "gamePk",
            "inc_name": "teams.home.team.name",
            "conv_dict": SCHEDULE_CONV,
        },
    )
    all_teams_stream = Stream(
        name="all_teams",
        cls=MLBAllTeam,
        interval=20.0,
        on_error="isolate",
        incorp_params={
            "inc_url": _TEAMS_URL,
            "rec_path": "teams",
            "inc_code": "id",
            "inc_name": "name",
            "conv_dict": ALL_TEAMS_CONV,
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
            "conv_dict": STANDINGS_CONV,
        },
    )
    hitting_current = HittingDrillCurrent(
        name="hitting",
        cls=MLBHitting,
        interval=6.0,
        on_error="isolate",
    )
    pitching_current = PitchingDrillCurrent(
        name="pitching",
        cls=MLBPitching,
        interval=6.0,
        on_error="isolate",
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
        middle=[all_teams_stream, standings_stream, hitting_current, pitching_current],
        tail=pulse_fjord,
        outflow=OUTFLOW_PATH,
        gate_mode="weir",
        drain_timeout=20.0,
    )

    print("Phase 2 — Building Watershed.diamond...\n")
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
        print("\n  (no output file produced — CustomCurrents may not have fired)\n")

    # Phase 3 — Post-run tuning feedback
    print("Phase 3 — Post-run feedback via architect.tune()...\n")
    report = tune(rejects=tw.rejects, tides=collected_tides, pass_interval=tw.pass_interval)
    meaningful_hints = [h for h in report.hints if h.severity in ("high", "med", "low")]
    if meaningful_hints:
        print(report.render())
    else:
        print("  No tuning hints — all knobs look well-tuned for this run.\n")

    return rows


# ---------------------------------------------------------------------------
# Phase 4 — Console leaderboard
# ---------------------------------------------------------------------------


def _print_leaderboard(rows: list[dict]) -> None:
    """Print a fixed-width leaderboard matching the README example block."""
    sep = "=" * 94
    thin = "-" * 94
    print(sep)
    print("  AL East Pulse — 2026 season (live MLB Stats API)")
    print(sep)
    print(
        f"  {'Rank':<6}{'Team':<32}{'W-L':<8}{'PCT':<7}{'GB':<6}"
        f"{'OPS':<7}{'ERA':<7}{'PowerIdx':<10}{'Pythag':<8}{'±':>6}"
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
        print("(no rows to display — check out/logs/MLBPulse_error.log)")


if __name__ == "__main__":
    asyncio.run(main())
