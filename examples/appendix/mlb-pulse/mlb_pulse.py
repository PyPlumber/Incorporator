"""
Appendix -- Advanced Tideweaver: MLB AL Pulse
----------------------------------------------
Companion script for ``examples/appendix/mlb-pulse/README.md``.

A Tideweaver ``diamond`` that fuses four live MLB Stats API endpoints into
15 ranked American League "Pulse Cards" (one row per AL team):

    head   : al_teams   (the 15 AL teams; scopes the whole diamond)
    middle : standings  (3 division records, each nesting a teamRecords list)
             hitting    (Stream(parent_current="al_teams") T5 drill, 15 calls)
             pitching   (Stream(parent_current="al_teams") T5 drill, 15 calls)
    tail   : pulse       (Fjord -- joins all 4 graph maps into ranked cards)

Row filtering: the head Stream's URL (``?sportId=1&leagueId=103``) scopes to
the 15 American League teams server-side -- the framework's preferred
row-filter primitive. ``hitting``/``pitching`` drill exactly that scope via
``parent_current``, so no post-fetch filter is needed anywhere.

``MLBAllTeam``/``MLBStandings``/``MLBHitting``/``MLBPitching``/``TeamPulseCard``
and the named helpers below are defined ONCE, here. ``outflow.py`` re-exports
them (rather than redefining them) so the CLI's class/token resolvers see the
same canonical objects this file's own ``main()`` uses -- see ``outflow.py``'s
docstring for why that matters.

Run with:
    python examples/appendix/mlb-pulse/mlb_pulse.py
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from incorporator import Fjord, Incorporator, Stream, Watershed, register_host_penstock
from incorporator.schema.converters import calc
from incorporator.tideweaver import LoggedTideweaver
from incorporator.tideweaver.architect import tune

HERE = Path(__file__).resolve().parent
OUTFLOW_PATH = HERE / "outflow.py"
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)

# When run as `python mlb_pulse.py`, this module executes as "__main__", so
# the classes below are NOT registered under sys.modules["mlb_pulse"]. The
# Tideweaver scheduler lazily loads outflow.py from inside the pulse Fjord's
# first tick, and outflow.py's "from mlb_pulse import ..." would otherwise
# re-execute this entire file under a fresh "mlb_pulse" module name -- a
# SECOND, DISTINCT copy of MLBAllTeam/MLBHitting/MLBPitching/TeamPulseCard
# whose inc_dict graph maps are never populated by the real Streams. Aliasing
# sys.modules["mlb_pulse"] to this already-executed module (before the
# Watershed's first Fjord tick) makes outflow.py's import resolve to these
# SAME canonical class objects instead. Only needed for direct script
# execution -- the CLI form never runs this file as __main__, so outflow.py's
# import is the first-ever import of "mlb_pulse" there and needs no alias.
if __name__ == "__main__":
    sys.modules.setdefault("mlb_pulse", sys.modules[__name__])

# MLB Stats API is unauthenticated and publishes no rate limit; 1 req/sec
# (60 req/min) is the polite default used elsewhere in this repo for it.
register_host_penstock("statsapi.mlb.com", rate_per_sec=1.0)

_TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams?sportId=1&leagueId=103"
_STANDINGS_URL = "https://statsapi.mlb.com/api/v1/standings?leagueId=103"
_HITTING_SAMPLE_URL = "https://statsapi.mlb.com/api/v1/teams/147/stats?group=hitting&stats=season&season=2026"
_HITTING_URL = "https://statsapi.mlb.com/api/v1/teams/{}/stats?group=hitting&stats=season&season=2026"
_PITCHING_URL = "https://statsapi.mlb.com/api/v1/teams/{}/stats?group=pitching&stats=season&season=2026"

# ---------------------------------------------------------------------------
# Incorporator subclasses -- defined ONCE, here; outflow.py re-imports them.
# ---------------------------------------------------------------------------


class MLBAllTeam(Incorporator):
    """American League teams -- /api/v1/teams?sportId=1&leagueId=103, rec_path 'teams'.

    URL-level filtering (``?leagueId=103``) scopes this, the diamond's head,
    to the 15 American League teams server-side. ``hitting``/``pitching``
    drill exactly this scope via ``parent_current="al_teams"``.
    """


class MLBStandings(Incorporator):
    """AL division standings -- /api/v1/standings?leagueId=103, rec_path 'records'.

    One row per division (East/Central/West). Each carries a raw
    ``teamRecords`` list that the framework auto-promotes to nested
    submodels with plain dotted attribute access (``tr.team.id``, ``tr.wins``,
    ``tr.winningPercentage``, ``tr.gamesBack``, ``tr.runsScored``,
    ``tr.runsAllowed``) -- no conv_dict needed on this class at all; the
    per-team flatten + derive happens read-time in ``outflow(state)``, where
    the framework's own advice puts export shaping.
    """


class MLBHitting(Incorporator):
    """Per-team season hitting stats -- Stream(parent_current='al_teams') T5 drill."""


class MLBPitching(Incorporator):
    """Per-team season pitching stats -- Stream(parent_current='al_teams') T5 drill."""


class TeamPulseCard(Incorporator):
    """Derived AL Pulse Card -- bare row class; ``outflow(state)``'s returned
    dict keys ARE the export shape (``Incorporator``'s ``extra='allow'``
    base means no field declarations are needed)."""


# ---------------------------------------------------------------------------
# Named helpers -- called directly from outflow(state), not routed through a
# conv_dict calc() (see the README's read-time-vs-build-time note for why).
# ---------------------------------------------------------------------------


def parse_games_back(value: str) -> float:
    """MLB's division-leader sentinel is the literal string '-' (not covered
    by ``is_garbage_value``'s GARBAGE_VALUES set) -- map it to 0.0."""
    return 0.0 if value == "-" else float(value)


def derive_pythag(runs_scored: float, runs_allowed: float) -> float:
    """Bill James Pythagorean win expectation: RS^2 / (RS^2 + RA^2)."""
    denom = runs_scored**2 + runs_allowed**2
    return round(runs_scored**2 / denom, 4) if denom > 0 else 0.5


def derive_power_index(ops: float, era: float, mean_ops: float, mean_era: float) -> float:
    """Peer-relative composite: (OPS / league-mean OPS) x (league-mean ERA / ERA).

    Higher is better -- teams with OPS above average AND ERA below average
    score above 1.0. League-mean normalisation makes the metric comparable
    across different scoring environments and seasons.
    """
    ops_ratio = ops / mean_ops if mean_ops > 0 else 0.0
    era_ratio = mean_era / era if era > 0 else 0.0
    return round(ops_ratio * era_ratio, 4)


def print_leaderboard(rows: list[dict]) -> None:
    """Fixed-width console leaderboard -- the print-twin of outflow(state)."""
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
        wl = f"{r['wins']}-{r['losses']}"
        delta = r["pythag_delta"]
        delta_str = f"{'+' if delta >= 0 else ''}{delta:.3f}"
        print(
            f"  {r['power_rank']:<6}{r['team_name']:<32}{wl:<8}"
            f"{r['win_pct']:.3f}  {r['games_back']:<6.1f}"
            f"{r['ops']:.3f}  {r['era']:.2f}   "
            f"{r['power_index']:<10.3f}{r['pythag']:.3f}  {delta_str:>6}"
        )
    print(sep)
    print("  +/- column: pythag - win_pct (positive = unlucky, negative = over-performing)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> None:
    print("Phase 1 - Pre-flight schema probe via Incorporator.architect()...\n")
    plan = await Incorporator.architect(
        {
            "al_teams": {"inc_url": _TEAMS_URL, "rec_path": "teams"},
            "standings": {"inc_url": _STANDINGS_URL, "rec_path": "records"},
            "hitting_sample": {"inc_url": _HITTING_SAMPLE_URL, "rec_path": "stats.0.splits.0"},
        },
        output="plan",
    )
    if plan is not None:
        for spec in plan.currents:
            print(f"  {spec.class_name}: {len(spec.conv_dict_template)} fields inferred")
    print()

    out_file = OUT / "al_pulse.ndjson"
    now = datetime.now(timezone.utc)
    window = (now, now + timedelta(seconds=50))

    watershed = Watershed.diamond(
        window=window,
        head=Stream(
            name="al_teams",
            cls=MLBAllTeam,
            interval=20.0,
            on_error="isolate",
            incorp_params={
                "inc_url": _TEAMS_URL,
                "rec_path": "teams",
                "inc_code": "id",
                "inc_name": "name",
            },
        ),
        middle=[
            Stream(
                name="standings",
                cls=MLBStandings,
                interval=20.0,
                on_error="isolate",
                incorp_params={
                    "inc_url": _STANDINGS_URL,
                    "rec_path": "records",
                    "inc_code": "division.id",
                    # No inc_name -- this endpoint's division sub-object has
                    # no "name" field (unlike /api/v1/teams's division).
                },
            ),
            Stream(
                name="hitting",
                cls=MLBHitting,
                interval=25.0,
                on_error="isolate",
                parent_current="al_teams",
                incorp_params={
                    "inc_url": _HITTING_URL,
                    "inc_child": "inc_code",
                    "rec_path": "stats.0.splits.0",
                    "inc_code": "team.id",
                    "conv_dict": {"ops": calc(float, "stat.ops", default=0.0, target_type=float)},
                },
            ),
            Stream(
                name="pitching",
                cls=MLBPitching,
                interval=25.0,
                on_error="isolate",
                parent_current="al_teams",
                incorp_params={
                    "inc_url": _PITCHING_URL,
                    "inc_child": "inc_code",
                    "rec_path": "stats.0.splits.0",
                    "inc_code": "team.id",
                    "conv_dict": {"era": calc(float, "stat.era", default=9.99, target_type=float)},
                },
            ),
        ],
        tail=Fjord(
            name="pulse",
            cls=TeamPulseCard,
            interval=5.0,
            on_error="isolate",
            export_params={
                "file_path": str(out_file),
                "format": "ndjson",
                "if_exists": "replace",
            },
        ),
        outflow=OUTFLOW_PATH,
        gate_mode="weir",
        drain_timeout=15.0,
    )

    print("Phase 2 - Building Watershed.diamond...\n")
    tw = LoggedTideweaver(watershed, enable_logging=True, logger_name="MLBPulse")
    collected_tides = []
    async for tide in tw.run():
        fired_str = ",".join(tide.fired) if tide.fired else "-"
        print(
            f"  Tide {tide.tide_number:3d} | fired: {fired_str:<32}"
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

    print("Phase 3 - Post-run feedback via architect.tune()...\n")
    report = tune(rejects=tw.rejects, tides=collected_tides, pass_interval=tw.pass_interval)
    meaningful_hints = [h for h in report.hints if h.severity in ("high", "med", "low")]
    if meaningful_hints:
        print(report.render())
    else:
        print("  No tuning hints - all knobs look well-tuned for this run.\n")

    if rows:
        print_leaderboard(rows)
    else:
        print("(no rows to display - check out/logs/MLBPulse_error.log)")


if __name__ == "__main__":
    asyncio.run(main())
