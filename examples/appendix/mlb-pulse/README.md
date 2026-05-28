***

> 📎 **Appendix — Full Incorporator + Tideweaver showcase: MLB AL East Pulse.**
> A single runnable script that walks every major framework capability
> in one coherent narrative — pre-flight schema probes, a Tideweaver
> `diamond` orchestrating four concurrent middles (including two
> `CustomCurrent` T5 drills), live disk-JSONL telemetry, and a post-run
> tuning feedback loop. If you're new to Tideweaver, read
> [Tutorial 11 — Tideweaver Diamond](../../11-tideweaver/README.md)
> first; if you're new to parent-child drilling, read
> [Tutorial 5 — Parent-Child Drilling](../../05-parent-child-drilling/README.md).

***

# ⚾ Advanced Tideweaver: MLB AL East Pulse

One 25-second run, five ranked "Pulse Cards" for the AL East division —
each composed live from four MLB Stats API endpoints joined inside a
single Tideweaver diamond. The cards carry **two independent composite
metrics side-by-side** (Power Index + Pythagorean win expectation) so
you can see at a glance which teams are over- vs under-performing
relative to their run differential.

The full developer loop runs end-to-end in one script: **probe → run →
measure → tune**.

```
🔍 Phase 1 — Pre-flight schema probe via Incorporator.architect()...
✅ MLBSchedule: 14 fields inferred
✅ MLBAllTeam: 17 fields inferred
✅ MLBHitting (sample team 147): 33 fields inferred

🌀 Phase 2 — Building Watershed.diamond...
  Tide   1 | fired: schedule                                          | skipped:  4 | 0.412s
  Tide   2 | fired: schedule,all_teams                                | skipped:  3 | 0.531s
  Tide   3 | fired: schedule,standings                                | skipped:  3 | 0.241s
  Tide   4 | fired: schedule,hitting,pitching                         | skipped:  2 | 5.193s
  Tide   5 | fired: schedule,standings,pulse                          | skipped:  2 | 3.018s
  ...

✅ Wrote 15 Pulse Card rows to out/al_east_pulse.ndjson

🔧 Phase 3 — Post-run feedback via architect.tune()...
  ✅ No tuning hints — all knobs look well-tuned for this run.

══════════════════════════════════════════════════════════════════════════════════════════════
  🏟️  AL East Pulse — 2026 season (live MLB Stats API)
══════════════════════════════════════════════════════════════════════════════════════════════
  Rank  Team                            W-L    PCT    GB    OPS   ERA  PowerIdx  Pythag      ±
  ──────────────────────────────────────────────────────────────────────────────────────────────
  1     Baltimore Orioles            91-71  0.562   0.0  0.773  3.87     1.247   0.558  -0.004
  2     New York Yankees             89-73  0.549   2.0  0.799  3.79     1.283   0.572  +0.023
  3     Toronto Blue Jays            74-88  0.457  17.0  0.741  4.42     0.671   0.464  +0.007
  4     Tampa Bay Rays               80-82  0.494  11.0  0.701  4.05     0.812   0.479  -0.015
  5     Boston Red Sox               78-84  0.481  13.0  0.764  4.21     0.871   0.500  +0.019
══════════════════════════════════════════════════════════════════════════════════════════════
  ± column: pythag − win_pct (positive = unlucky, negative = over-performing)
```

> **Why two metrics?** The Power Index measures *peer-relative* team
> strength (how OPS/ERA stack up against the division mean), while
> Pythagorean win expectation is a *self-relative* sabermetric (how
> many wins the run differential predicts). When they agree, you've
> found a genuinely strong team. When they disagree — Power Index
> high but pythag_delta positive — the team has been unlucky and
> may regress upward.

---

## 🎯 What this appendix demonstrates

| Capability | How |
|---|---|
| **Diamond shape** with **4 concurrent middles** | `Watershed.diamond(head, middle=[all_teams, standings, hitting, pitching], tail=pulse, gate_mode="weir")` — every middle ticks on its own interval; `weir` lets them progress without hard-mode in-flight starvation. |
| **T5 parent-child drilling** inside Tideweaver | Two `CustomCurrent` subclasses (`HittingDrillCurrent`, `PitchingDrillCurrent`) read `MLBAllTeam.inc_dict` at tick time, filter to AL East by `division_id == 201`, and fire `MLBHitting.incorp(inc_parent=al_east, inc_child="inc_code", inc_url="...{}/stats?group=...")` — five concurrent child fetches per drill. Two drills firing in parallel = **10 concurrent T5 child calls** per tick. Parent list comes from the upstream `all_teams` Stream's registry, never hardcoded. |
| **Six graph maps in state** | `MLBSchedule` (head) + `MLBAllTeam` (middle) + `MLBStandings` (middle) + `MLBHitting` (CustomCurrent) + `MLBPitching` (CustomCurrent) + `TeamPulseCard` (output). The Fjord's `outflow(state)` reads five of them. |
| **`conv_dict` with callable kinds, lambda-free** | Builtins (`str.lower`), `operator.itemgetter` (nested field drill), two named module-level helpers (`derive_ops`, `above_power_threshold`), and named id-extractors (`_home_team_id`, `_away_team_id`). **Zero lambdas anywhere** — see [AGENTS.md H3 idiom](../../../AGENTS.md). |
| **Schema discovery** via `Incorporator.architect()` + per-class `test()` | Pre-flight probe profiles all 5 source endpoints in parallel, prints schemas + field counts, and fails loudly BEFORE the 25-second diamond run if any `rec_path` or field is missing. No registry pollution — `test()` stops at the schema. |
| **`LoggedTideweaver`** runtime telemetry | Drops in for `Tideweaver`; routes each `Tide` and every canal-layer `RejectEntry` to disk JSONL via the queue-handler-backed logger thread. Inspect `out/logs/MLBPulse_error.log` + `out/logs/MLBPulse_debug.log` after the run. |
| **`architect.tune()`** post-run feedback | After the run, accumulated outcome records feed `architect.tune(rejects, tides, waves, pass_interval)` which emits concrete knob-tuning hints (or "no tuning needed" on a clean run — also a valid outcome). Closes the developer loop: **probe → run → measure → tune**. |
| **Polite host throttle** | `register_host_penstock("statsapi.mlb.com", SustainedPenstock(rate_per_sec=1.0))` at module top — 1 req/sec = 60 req/min, comfortably under any unstated MLB Stats API courtesy cap. |
| **Composite analytics** | Per-team Power Index AND Pythagorean win expectation computed in `outflow(state)`, joined across 4 upstream graph maps, pre-sorted by Power Index. Same insight in `pandas` = ~60 lines of merges + manual normalization + Pythag calc. |

---

## 🏗️ Diamond shape

```
                head: live MLB schedule Stream (today's games)
                                  │
        ┌─────────┬───────────────┼───────────────┬─────────┐
        ▼         ▼               ▼               ▼         ▼
   all_teams  standings    hitting (Custom)  pitching (Custom)
   Stream     Stream       Current           Current
   (1 tick:   (live,       (filters          (filters
    30 teams) ~4 ticks)     MLBAllTeam,       MLBAllTeam,
                            T5 drills 5      T5 drills 5
                            hitting calls)   pitching calls)
        │         │               │               │
        └─────────┴───────┬───────┴───────────────┘
                          ▼
                  TeamPulseCard Fjord
        (joins 4 graph maps; Power Index + Pythagorean)
                          │
                          ▼
                 out/al_east_pulse.ndjson
                 (+ console-printed leaderboard)
```

`Watershed.diamond(window, head, middle=[all_teams, standings, hitting, pitching], tail=pulse, gate_mode="weir")` — per [Tutorial 11](../../11-tideweaver/README.md), `diamond` is the canonical multi-source-into-one-aggregator shape.

---

## 🚦 Rate-limit note

```python
from incorporator import register_host_penstock
from incorporator.io.penstock import SustainedPenstock

register_host_penstock("statsapi.mlb.com", SustainedPenstock(rate_per_sec=1.0))
```

MLB Stats API is unauthenticated and publishes no rate limit — 1 req/sec
(60 req/min) is the polite default. Total wire time for this demo:
**~22 calls @ 1.0 r/s = ~22 seconds**, fits comfortably in the
25-second window with `gate_mode="weir"` letting middles progress in
parallel.

If you bump the throttle higher than 1.0 r/s without testing,
`architect.tune()` will tell you in Phase 3 — that's the post-run
feedback loop closing.

---

## ▶️ Run

```bash
cd examples/appendix/mlb-pulse
python mlb_pulse.py
```

Artifacts produced (all under `out/`, gitignored by repo policy):

| File | What |
|---|---|
| `out/al_east_pulse.ndjson` | 5 ranked Pulse Cards, sorted by Power Index descending — the headline deliverable |
| `out/logs/MLBPulse_error.log` | INFO/ERROR Tides from `LoggedTideweaver` (one line per scheduler pass) |
| `out/logs/MLBPulse_debug.log` | DEBUG Tides (no-op passes between firings) |

Plus console output:
- Pre-flight architect+test schemas
- Per-tick scheduler log (which currents fired vs skipped)
- Post-run architect.tune() hints
- Final fixed-width leaderboard table

---

## 🧱 File layout

```
examples/appendix/mlb-pulse/
  README.md                  (this file)
  mlb_pulse.py               (entry point: probe + run + tune)
  pulse_outflow.py           (Incorporator classes + outflow(state) join)
  watershed.json             (CLI-equivalent declarative form)
  fixtures/
    expected_schemas.json    (architect-probe baselines; documentation aid)
  out/                       (runtime artifacts; gitignored)
    al_east_pulse.ndjson
    logs/
      MLBPulse_error.log
      MLBPulse_debug.log
```

Matches the [Tutorial 11](../../11-tideweaver/) + [`nascar-tideweaver`](../nascar-tideweaver/) convention:
- Entry script named after the demo
- Outflow sidecar with prefix-style name (`pulse_outflow.py`, mirroring `arb_outflow.py` and `race_outflow.py`)
- Companion `watershed.json` for the CLI form
- `fixtures/` directory for static reference data

---

## ✋ What this appendix does NOT demonstrate

- **Per-player drilling.** Capping the demo at team-level stats (5 hitting + 5 pitching calls per drill) keeps the budget tight. Adding `/people/{id}?hydrate=stats` per player would be a third T5 layer; left as a follow-up appendix.
- **All 30 teams.** Scoped to AL East to keep the demo runnable in 25 seconds; the same shape extends to other divisions by changing `division_id` filter + league ID.
- **Historical comparisons.** Current-season snapshot only.
- **The `tests/test_tideweaver_routing_diamond.py` companion.** That test runs the same diamond shape with mocked endpoints + assertion-driven correctness checks. This appendix is the live-API counterpart; the test is the regression-prevention counterpart. They're complementary.

---

## 🔗 See also

- [Tutorial 11 — Tideweaver Diamond](../../11-tideweaver/README.md) — canonical introduction to the `Watershed.diamond` shape
- [Tutorial 5 — Parent-Child Drilling](../../05-parent-child-drilling/README.md) — the T5 `inc_parent` + `inc_child` pattern this appendix uses inside CustomCurrents
- [`nascar-tideweaver` appendix](../nascar-tideweaver/) — diamond across race telemetry
- [`pokeapi-etl` appendix](../pokeapi-etl/) — the other big T5-drill demo (PokéAPI, 150 children)
- [`tests/test_tideweaver_routing_diamond.py`](../../../tests/test_tideweaver_routing_diamond.py) — mocked counterpart with assertion-driven correctness checks
