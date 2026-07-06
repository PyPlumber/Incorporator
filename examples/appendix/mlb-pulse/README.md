***

> 📎 **Appendix — Full Incorporator + Tideweaver showcase: MLB AL Pulse.**
> A single runnable script that walks every major framework capability
> in one coherent narrative — pre-flight schema probes, a Tideweaver
> `diamond` orchestrating four concurrent middles (including two
> `Stream(parent_current=...)` T5 drills), live disk-JSONL telemetry, and a post-run
> tuning feedback loop. If you're new to Tideweaver, read
> [Tutorial 11 — Tideweaver Diamond](../../11-tideweaver/README.md)
> first; if you're new to parent-child drilling, read
> [Tutorial 5 — Parent-Child Drilling](../../05-parent-child-drilling/README.md).

***

# ⚾ Advanced Tideweaver: MLB AL Pulse

One ~30-second run, fifteen ranked "Pulse Cards" for the American League —
each composed live from four MLB Stats API endpoints joined inside a
single Tideweaver diamond. The cards carry **two independent composite
metrics side-by-side** (Power Index + Pythagorean win expectation) so
you can see at a glance which teams are over- vs under-performing
relative to their run differential.

The full developer loop runs end-to-end in one script: **probe → run →
measure → tune**.

```
🔍 Phase 1 — Pre-flight schema probe via Incorporator.architect()...
✅ Schedule: 14 fields inferred
✅ AlTeams: 17 fields inferred
✅ Standings: 7 fields inferred
✅ HittingSample: 33 fields inferred

🌀 Phase 2 — Building Watershed.diamond...
  Tide   1 | fired: schedule                                          | skipped:  4 | 0.412s
  Tide   2 | fired: schedule,al_teams                                 | skipped:  3 | 0.531s
  Tide   3 | fired: schedule,standings                                | skipped:  3 | 0.241s
  Tide   4 | fired: schedule,hitting,pitching                         | skipped:  2 | 15.6s
  Tide   5 | fired: schedule,standings,pulse                          | skipped:  2 | 3.018s
  ...

✅ Wrote 15 Pulse Card rows to out/al_pulse.ndjson

🔧 Phase 3 — Post-run feedback via architect.tune()...
  ✅ No tuning hints — all knobs look well-tuned for this run.

══════════════════════════════════════════════════════════════════════════════════════════════
  🏟️  AL Pulse — 2026 season (live MLB Stats API)
══════════════════════════════════════════════════════════════════════════════════════════════
  Rank  Team                            W-L    PCT    GB    OPS   ERA  PowerIdx  Pythag      ±
  ──────────────────────────────────────────────────────────────────────────────────────────────
  1     New York Yankees             89-73  0.549   0.0  0.799  3.79     1.283   0.572  +0.023
  2     Houston Astros               87-75  0.537   0.0  0.768  3.32     1.272   0.560  +0.023
  3     Cleveland Guardians          85-77  0.525   0.0  0.755  3.50     1.221   0.548  +0.023
  4     Baltimore Orioles            91-71  0.562   2.0  0.773  3.87     1.184   0.558  -0.004
  …    (12 more rows, all 15 AL teams ranked by Power Index desc)
══════════════════════════════════════════════════════════════════════════════════════════════
  +/- column: pythag - win_pct (positive = unlucky, negative = over-performing)
```

> **Why two metrics?** The Power Index measures *peer-relative* team
> strength (how OPS/ERA stack up against the league mean), while
> Pythagorean win expectation is a *self-relative* sabermetric (how
> many wins the run differential predicts). When they agree, you've
> found a genuinely strong team. When they disagree — Power Index
> high but pythag_delta positive — the team has been unlucky and
> may regress upward.

---

> 💡 **Read-time DX rule: coerce + join at build time; outflow reads plain attributes.**
> `MLBHitting.ops` and `MLBPitching.era` already coerced their raw JSON strings
> via `calc(float, "stat.ops", default=0.0, target_type=float)` in each
> Stream's own `conv_dict`. `MLBStandings` now does the same for the deeply
> nested `teamRecords[]` list: a single `calc(_flatten_team_records,
> "teamRecords", default=[])` entry flattens each division's raw team records
> into a clean `team_rows` list of pre-coerced dicts (`team_id`, `wins`,
> `losses`, `win_pct`, `games_back`, `runs_scored`, `runs_allowed`) — one call,
> once per tick, instead of `outflow()` doing hybrid `getattr`-vs-dict
> materialisation on every read.
>
> **Why the team/hitting/pitching join stays read-time.** A Tideweaver
> `diamond` has no `inflow(state)` seed hook the way a standalone
> fjord-with-inflow does (see
> [Tutorial 9](../../09-nascar-fantasy-fjord/README.md) /
> [Tutorial 10](../../10-multi-source-fjord/README.md)) — there is no
> build-time mechanism to inject a `link_to()`-resolved instance into
> `MLBStandings`' own `conv_dict` before the OTHER three middles (`al_teams`,
> `hitting`, `pitching`) have even ticked yet on a given wave. The three-way
> team/hitting/pitching join genuinely has to happen at the diamond's join
> point — the tail Fjord's `outflow(state)` — every tick. What moved to build
> time instead is everything the diamond's OWN topology allows: numeric/string
> coercion (`inc`/`calc` in each source's own `conv_dict`) and flattening the
> deeply-nested `teamRecords[]` list into clean per-team dicts (`MLBStandings`'
> `team_rows` conv_dict entry). The result: `outflow()` still does the join,
> but every field it reads off either side of that join is a plain,
> pre-coerced attribute or dict key — zero `_safe_*` calls, zero
> getattr-vs-dict branching.
>
> See `docs/api_atlas.md`'s "Build-time vs read-time: where coercion + joins
> belong" section for the general rule.

---

## 🔎 Row filtering: pick the right primitive

This appendix's load-bearing lesson: **filter at the source**. The
framework has no `parent_filter` / `parent_filters` field. The
declarative dependency primitive is `Stream(parent_current="<name>")`
+ the parent's own URL declaring its scope.

| Priority | Primitive | Where it runs | Use when |
|---|---|---|---|
| 1 | **URL query params** (`?leagueId=103`, `?status=active`) | HTTP server | Upstream API exposes the filter as a query param |
| 2 | **`SQLitePaginator(sql_query="...WHERE...")`** | Database | Source is SQL |
| 3 | **`outflow(state)` return-list filter** | User callable at fjord/stateful emit | Filter belongs with aggregation logic |
| 4 | **Separate URL-filtered parent Streams** | One parent per filter | Multi-child case with different filters |
| 5 | **`CustomCurrent`** (escape hatch) | User-overridden `tick()` | Computed-field filter the URL can't express |

This appendix uses **option 1**: the `al_teams` Stream's URL —
`?sportId=1&leagueId=103` — produces exactly the 15 American League
teams server-side, so the children just declare
`parent_current="al_teams"` and naturally drill that scope. No
post-fetch row filtering. No JSON sigils. No `import operator`.

The same idiom across the framework: T11 Kraken `?pair=XBTUSD,ETHUSD`;
crypto-graph `?vs_currency=usd&order=market_cap_desc&per_page=100`;
pokeapi `?limit=50&offset=0`; the same MLB appendix's `_STANDINGS_URL`
`?leagueId=103`.

---

## 🎯 What this appendix demonstrates

| Capability | How |
|---|---|
| **Diamond shape** with **4 concurrent middles** | `Watershed.diamond(head, middle=[al_teams, standings, hitting, pitching], tail=pulse, gate_mode="weir")` — every middle ticks on its own interval; `weir` lets them progress without hard-mode in-flight starvation. |
| **URL-level row filtering** | `al_teams` Stream's `inc_url` is `?sportId=1&leagueId=103` — server-side filter scopes to the 15 American League teams. Children drill that scope directly via `parent_current="al_teams"`. No post-fetch filter primitive. See "Row filtering" section above for the decision tree. |
| **T5 parent-child drilling** via `Stream(parent_current=...)` | Two `Stream` nodes (`hitting`, `pitching`) with `parent_current="al_teams"` read the upstream snapshot at tick time and fan-out `cls.incorp(inc_parent=<snapshot>, inc_child="inc_code", inc_url="...{}/stats?group=...")` — 15 concurrent child fetches per stream. Two streams firing in parallel = **30 concurrent T5 child calls** per tick. Parent list comes from the upstream `al_teams` Stream's `_tideweaver_snapshot`, never hardcoded. |
| **Six graph maps in state** | `MLBSchedule` (head) + `MLBAllTeam` (middle) + `MLBStandings` (middle) + `MLBHitting` (Stream, parent_current="al_teams") + `MLBPitching` (Stream, parent_current="al_teams") + `TeamPulseCard` (output). The Fjord's `outflow(state)` reads four of them (`MLBSchedule` is the head — never accessed in `outflow(state)`). |
| **`conv_dict` with named primitives, lambda-free** | Two `calc(float, "stat.ops", default=0.0, target_type=float)` / `calc(float, "stat.era", default=9.99, target_type=float)` calls — coercion only, no row predicates. Zero lambdas — named module-level helpers per the framework's lambda-free idiom. |
| **Schema discovery** via `Incorporator.architect()` + per-class `test()` | Pre-flight probe profiles all 4 source endpoints in parallel, prints schemas + field counts, and fails loudly BEFORE the 25-second diamond run if any `rec_path` or field is missing. No registry pollution — `test()` stops at the schema. |
| **`LoggedTideweaver`** runtime telemetry | Drops in for `Tideweaver`; routes each `Tide` and every `RejectEntry` to disk JSONL via the queue-handler-backed logger thread. Inspect `logs/MLBPulse_tide.log` (single-file source for `get_tides()`), `logs/MLBPulse_error.log` (codebase/canal rejects + scheduler events + tides), `logs/MLBPulse_api.log` (URL/HTTP errors), and `logs/MLBPulse_debug.log` (debug superset) after the run. `get_rejects()` unions `_error.log` + `_api.log`; `get_scheduler_events()` surfaces lifecycle events including `watershed_started`/`watershed_completed`. |
| **`architect.tune()`** post-run feedback | After the run, accumulated outcome records feed `architect.tune(rejects, tides, pass_interval)` which emits concrete knob-tuning hints (or "no tuning needed" on a clean run — also a valid outcome). Closes the developer loop: **probe → run → measure → tune**. |
| **Polite host throttle** | `register_host_penstock("statsapi.mlb.com", SustainedPenstock(rate_per_sec=1.0))` at module top — 1 req/sec = 60 req/min, comfortably under any unstated MLB Stats API courtesy cap. |
| **Composite analytics** | Per-team Power Index AND Pythagorean win expectation computed in `outflow(state)`, joined across 4 upstream graph maps, pre-sorted by Power Index. Same insight in `pandas` = ~60 lines of merges + manual normalization + Pythag calc. |

---

## 🏗️ Diamond shape

```
                head: live MLB schedule Stream (today's games)
                                  │
        ┌─────────┬───────────────┼───────────────┬─────────┐
        ▼         ▼               ▼               ▼         ▼
   al_teams   standings    hitting (Stream)  pitching (Stream)
   Stream     Stream       parent_current=   parent_current=
   (?leagueId (?leagueId   "al_teams"        "al_teams"
   =103;      =103;        T5 drills 15      T5 drills 15
   15 teams)  3 records)   hitting calls     pitching calls
        │         │               │               │
        └─────────┴───────┬───────┴───────────────┘
                          ▼
                  TeamPulseCard Fjord
        (joins 4 graph maps; Power Index + Pythagorean)
                          │
                          ▼
                 out/al_pulse.ndjson
                 (+ console-printed leaderboard)
```

`Watershed.diamond(window, head, middle=[al_teams_stream, standings_stream, hitting_stream, pitching_stream], tail=pulse, gate_mode="weir")` — per [Tutorial 11](../../11-tideweaver/README.md), `diamond` is the canonical multi-source-into-one-aggregator shape.

---

## 🚦 Rate-limit note

```python
from incorporator import register_host_penstock
from incorporator.io.penstock import SustainedPenstock

register_host_penstock("statsapi.mlb.com", SustainedPenstock(rate_per_sec=1.0))
```

MLB Stats API is unauthenticated and publishes no rate limit — 1 req/sec
(60 req/min) is the polite default. Total wire time for this demo:
**~31 calls @ 1.0 r/s ≈ 31 seconds**, which slightly exceeds the
25-second window. With `gate_mode="weir"` letting middles progress in
parallel, the first full hitting + pitching pass typically completes
within the window and the leaderboard prints from that pass. Stretching
the window to 35 seconds is the common live-run tweak.

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
| `out/al_pulse.ndjson` | 15 ranked Pulse Cards, sorted by Power Index descending — the headline deliverable |
| `logs/MLBPulse_tide.log` | Every yielded `Tide` (fired + no-op) — single-file source for `LoggedTideweaver.get_tides("MLBPulse")` |
| `logs/MLBPulse_error.log` | Codebase/canal rejects + scheduler events (`watershed_started`/`watershed_completed` + diagnostics) + non-API waves |
| `logs/MLBPulse_api.log` | URL/internet-traffic errors (`is_url_traffic_error=True`) — rate limits, HTTP errors, timeouts |
| `logs/MLBPulse_debug.log` | Superset of both error and api files + DEBUG lifecycle events — used by `get_current()` |

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
  outflow.py                 (Incorporator classes + outflow(state) join)
  watershed.json             (CLI-equivalent declarative form — partial; see warning below)
  out/                       (runtime artifacts; gitignored)
    al_pulse.ndjson
  logs/                      (runtime logs; gitignored)
    MLBPulse_tide.log
    MLBPulse_error.log
    MLBPulse_debug.log
```

Matches the [Tutorial 9](../../09-nascar-fantasy-fjord/), [Tutorial 11](../../11-tideweaver/), and [`nascar-tideweaver`](../nascar-tideweaver/) convention:
- Entry script named after the demo
- Outflow sidecar with bare semantic name (`outflow.py`) — all Tideweaver examples (T9, T11, nascar-tideweaver, mlb-pulse) use this naming
- Companion `watershed.json` for the CLI form

> ⚠️ **`watershed.json`'s CLI form cannot run this appendix end-to-end.**
> The build-time `team_rows` flattening (previous section) needs
> `calc(_flatten_team_records, "teamRecords", default=[])` in `standings`'s
> `conv_dict` — but Watershed's JSON loader resolves conv_dict strings
> against a fixed builtin/framework allow-list *before* `outflow.py` is even
> imported, so a user-defined helper (public or private) can never be
> referenced by name from `watershed.json`. Because the shared `outflow(state)`
> depends on `standings_row.team_rows` for every row it emits, this is not a
> single-node degradation — the whole diamond's tail join fails. `outflow(state)`
> now guards this explicitly: its first statement raises a `RuntimeError` with
> an actionable remediation message the moment it sees a `MLBStandings` row
> missing `team_rows`, so `incorporator tideweaver run watershed.json` fails
> LOUD (a WARNING-level "isolated tick failure" log line every pulse tick)
> instead of silently exiting 0 with an empty `out/al_pulse.ndjson`. See the
> `_doc_limitation_` field inside `watershed.json` itself for the full
> explanation. Use `python mlb_pulse.py` for a working run; the JSON form still
> demonstrates the declarative shape for every other node.

---

## ✋ What this appendix does NOT demonstrate

- **Per-player drilling.** Capping the demo at team-level stats (15 hitting + 15 pitching calls per drill) keeps the budget tight. Adding `/people/{id}?hydrate=stats` per player would be a third T5 layer; left as a follow-up appendix.
- **All 30 teams.** Scoped to the American League (15 teams) via `?leagueId=103` to keep the demo runnable in ~30 seconds; the same shape extends to the National League by changing the URL filter to `?leagueId=104`.
- **Historical comparisons.** Current-season snapshot only.
- **The `tests/test_tideweaver_routing_diamond.py` companion.** That test runs the same diamond shape with mocked endpoints + assertion-driven correctness checks. This appendix is the live-API counterpart; the test is the regression-prevention counterpart. They're complementary.

---

## 🔗 See also

- [Tutorial 11 — Tideweaver Diamond](../../11-tideweaver/README.md) — canonical introduction to the `Watershed.diamond` shape
- [Tutorial 5 — Parent-Child Drilling](../../05-parent-child-drilling/README.md) — the T5 `inc_parent` + `inc_child` pattern this appendix uses via `Stream(parent_current=...)` nodes
- [`nascar-tideweaver` appendix](../nascar-tideweaver/) — diamond across race telemetry
- [`pokeapi-etl` appendix](../pokeapi-etl/) — the other big T5-drill demo (PokéAPI, 150 children)
- [`tests/test_tideweaver_routing_diamond.py`](../../../tests/test_tideweaver_routing_diamond.py) — mocked counterpart with assertion-driven correctness checks
