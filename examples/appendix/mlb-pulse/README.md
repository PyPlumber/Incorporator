***

> 📎 **Appendix — Tideweaver diamond over a live sports API.** A four-source
> `Watershed.diamond` (one head Stream, three middle Streams — two of them
> T5 parent-child drills — one tail Fjord) fusing the MLB Stats API into a
> ranked leaderboard. If you're new to Tideweaver, read
> [Tutorial 11 — Tideweaver Diamond](../../11-tideweaver/README.md) first;
> if you're new to parent-child drilling, read
> [Tutorial 5 — Parent-Child Drilling](../../05-parent-child-drilling/README.md).

***

# Advanced Tideweaver: MLB AL Pulse

Fifteen ranked "Pulse Cards" for the American League — one row per team,
fused live from four MLB Stats API endpoints inside a single Tideweaver
diamond:

```
                    al_teams (head Stream)
                 ?sportId=1&leagueId=103, 15 teams
                              │
        ┌───────────────┬────┴────┬───────────────┐
        ▼                ▼                        ▼
   standings         hitting (Stream)        pitching (Stream)
   Stream            parent_current=          parent_current=
   ?leagueId=103     "al_teams"               "al_teams"
   3 division        T5 drill, 15 calls       T5 drill, 15 calls
   records
        │                │                        │
        └────────────────┴───────────┬────────────┘
                                      ▼
                              pulse (tail Fjord)
                    join + rank into 15 Pulse Cards
                                      │
                                      ▼
                          out/al_pulse.ndjson
```

Each card carries two independent composite metrics side by side — a
peer-relative **Power Index** (OPS vs. league mean, ERA vs. league mean)
and a self-relative **Pythagorean win expectation** (run differential) —
so you can see at a glance which teams are over- or under-performing
relative to their run differential.

```
Phase 1 - Pre-flight schema probe via Incorporator.architect()...

  AlTeams: 0 fields inferred
  Standings: 1 fields inferred
  HittingSample: 0 fields inferred

Phase 2 - Building Watershed.diamond...
  Tide   1 | fired: al_teams                         | skipped:  4 | 0.000s
  Tide   2 | fired: standings,hitting,pitching        | skipped:  2 | 0.000s
  Tide   5 | fired: pulse                             | skipped:  4 | 0.000s
  ...

  Wrote 15 Pulse Card rows to out/al_pulse.ndjson

Phase 3 - Post-run feedback via architect.tune()...
  No tuning hints - all knobs look well-tuned for this run.

==============================================================================================
  AL Pulse - 2026 season (live MLB Stats API)
==============================================================================================
  Rank  Team                            W-L     PCT    GB    OPS    ERA    PowerIdx  Pythag     +/-
  --------------------------------------------------------------------------------------------
  1     New York Yankees                54-43   0.557  2.5   0.740  3.38   1.285     0.606  +0.049
  2     Boston Red Sox                  48-48   0.500  8.0   0.707  3.55   1.169     0.551  +0.051
  3     Detroit Tigers                  45-52   0.464  6.5   0.710  3.62   1.151     0.531  +0.068
  ...   (12 more rows, all 15 AL teams ranked by Power Index desc)
==============================================================================================
  +/- column: pythag - win_pct (positive = unlucky, negative = over-performing)
```

Both entry forms below produced this exact 15-row leaderboard in a live
run against the real MLB Stats API — win/loss/OPS/ERA values will differ
run to run since the 2026 season is in progress.

---

## The redesign: no reducer for the nested standings list

`standings` returns one row per division, each nesting a raw
`teamRecords` list. The framework auto-promotes that list to nested
submodels with plain dotted attribute access — `tr.team.id`, `tr.wins`,
`tr.winningPercentage`, `tr.gamesBack`, `tr.runsScored`, `tr.runsAllowed`
— with **no `conv_dict` entry required on `MLBStandings` at all**. The
per-team flatten + derive (win-pct/games-back coercion, Pythagorean
calc) happens directly inside the tail Fjord's `outflow(state)`, which is
exactly where the framework's own verb doctrine puts export shaping:
*"fjords are meant for multi-source, in-state graph maps, and
manipulating the export."*

One live MLB Stats API quirk this design routes around: `gamesBack` is a
numeric string for every team **except** each division's current leader,
who gets the literal sentinel string `"-"`. That sentinel isn't covered
by the framework's own garbage-value set, so routing it through a
`conv_dict` `calc()` would hit the exception-fallback path and log a
warning on all 3 division leaders, every tick. `parse_games_back()`
(defined once in `mlb_pulse.py`, called directly from `outflow(state)`)
handles it with zero warnings:

```python
def parse_games_back(value: str) -> float:
    """MLB's division-leader sentinel is the literal string '-' -- map it to 0.0."""
    return 0.0 if value == "-" else float(value)
```

`ops`/`era` still coerce at their own source (`MLBHitting`/`MLBPitching`
each carry a one-entry `conv_dict` with `calc(float, "stat.ops", ...)` /
`calc(float, "stat.era", ...)`), since those values aren't nested inside
a list-of-dicts the way `teamRecords` is.

---

## Row filtering: filter at the source

| Priority | Primitive | Used here |
|---|---|---|
| 1 | **URL query params** | `al_teams`' `?sportId=1&leagueId=103` scopes to the 15 AL teams server-side; `standings`' `?leagueId=103` scopes to the 3 AL divisions. `hitting`/`pitching` drill exactly that scope via `parent_current="al_teams"` — no post-fetch filter anywhere. |

One upstream limitation worth knowing if you extend this appendix: the
`/standings` endpoint silently **ignores** `divisionId` whenever
`leagueId` is also present — `?divisionId=200` and `?divisionId=202`
both return all 3 AL divisions. That rules out drilling one division at
a time; `standings` stays a single-call Stream that fetches all 3
divisions at once.

---

## Reading each source, class-handle vs. plain list

`outflow(state)` runs inside a Tideweaver Fjord current (a
`Watershed`/diamond run, not a `cls.fjord()` daemon), so `state` values
are **plain lists** with no `.inc_dict` attribute:

```python
standings = state.get("MLBStandings", [])          # plain list -- the driving iteration
team = MLBAllTeam.inc_dict.get(team_id)             # class-handle O(1) lookup
hit = MLBHitting.inc_dict.get(team_id)
pit = MLBPitching.inc_dict.get(team_id)
```

`MLBStandings` is read straight off `state` because it's the list being
iterated; `MLBAllTeam`/`MLBHitting`/`MLBPitching` are read via their
class-level `inc_dict` graph map because the join needs O(1) lookups by
`team_id`, not a second linear scan per team.

---

## Direct script execution and class identity

`mlb_pulse.py` defines every `Incorporator` subclass exactly once;
`outflow.py` re-imports them via a guarded `sys.path.insert` +
`from mlb_pulse import (...)`, so the CLI's class/token resolvers and the
Python entry's own `Watershed` share the same canonical class objects.
That sharing depends on `sys.modules["mlb_pulse"]` already existing by
the time `outflow.py` first imports it — true automatically for the CLI
form (which never runs `mlb_pulse.py` itself), but **not** true for
`python mlb_pulse.py`, where this file executes as `sys.modules["__main__"]`
rather than `sys.modules["mlb_pulse"]`. Without the one-line alias below,
the Tideweaver scheduler's lazy `outflow.py` load re-executes this whole
file under a second, distinct `mlb_pulse` module — its own fresh copies
of `MLBAllTeam`/`MLBHitting`/`MLBPitching`, with empty `inc_dict` graph
maps that the real Streams never populate, silently producing a 0-row
`outflow(state)` result on every tick even though `state` itself is
correctly populated:

```python
if __name__ == "__main__":
    sys.modules.setdefault("mlb_pulse", sys.modules[__name__])
```

Verified live: removing this line reproduces a clean run with `fired:`
lines for every current and `(no output file produced)` at the end — no
exception, no warning, just an empty export every tick.

---

## What this appendix demonstrates

| Capability | How |
|---|---|
| **Diamond shape**, `gate_mode="weir"` | `Watershed.diamond(head=al_teams, middle=[standings, hitting, pitching], tail=pulse, gate_mode="weir")` — `standings` (1 call) and `hitting`/`pitching` (15 calls each, sharing one host penstock) run on very different cadences; `weir` lets each middle progress at its own pace instead of hard-gating on lockstep resync. |
| **T5 parent-child drilling** | `hitting`/`pitching` are `Stream(parent_current="al_teams")` — 15 concurrent per-team child fetches each, fanned out from the head's snapshot, never hardcoded. |
| **Auto-promoted nested lists** | `MLBStandings.teamRecords` needs no `conv_dict` at all — the framework promotes the raw list to nested submodels with plain dotted access. |
| **Read-time join in the tail Fjord** | `outflow(state)` joins 4 graph maps (`MLBStandings` list + `MLBAllTeam`/`MLBHitting`/`MLBPitching` class-handle lookups) into ranked Pulse Cards — exactly the verb doctrine's "fjords manipulate the export." |
| **Bare fjord row class** | `TeamPulseCard` declares no fields; `outflow(state)`'s returned dict keys are its export shape. |
| **`LoggedTideweaver` + `architect.tune()`** | Disk-JSONL telemetry (`logs/MLBPulse_*.log`) plus post-run tuning feedback closing the probe → run → tune loop. |
| **Polite host throttle** | `register_host_penstock("statsapi.mlb.com", rate_per_sec=1.0)` — MLB Stats API is unauthenticated and undocumented; 1 req/sec is the polite default. |

---

## Timing budget

Total live calls per full wave: 1 (`al_teams`) + 1 (`standings`) + 15
(`hitting`) + 15 (`pitching`) = 32, sharing one 1 req/sec host penstock.
Observed live: `hitting`+`pitching` together complete in roughly 14-15
seconds (interleaved through the shared bucket), well inside this
appendix's `window=50s` / `drain_timeout=15s` budget. `al_teams`/
`standings` re-fire every 20s; `hitting`/`pitching` re-fire every 25s
(deliberately longer than one drill's own completion time, so the
scheduler doesn't pile a second concurrent drill onto the shared
penstock); `pulse` re-checks every 5s and keeps only the freshest 15-row
snapshot (`if_exists="replace"`).

---

## File layout

```
examples/appendix/mlb-pulse/
  README.md            (this file)
  mlb_pulse.py         (entry point: classes + probe + diamond + leaderboard)
  outflow.py           (pure sidecar: re-exports classes/helpers, outflow(state))
  watershed.json       (CLI-equivalent declarative form)
  out/                 (runtime artifacts; gitignored)
    al_pulse.ndjson
  logs/                (runtime logs; gitignored)
    MLBPulse_tide.log
    MLBPulse_error.log
    MLBPulse_api.log
    MLBPulse_debug.log
```

---

## Run it

```bash
# Python entry
python examples/appendix/mlb-pulse/mlb_pulse.py

# Same diamond, same outflow.py, same 15 cards, from the CLI
cd examples/appendix/mlb-pulse
incorporator tideweaver run watershed.json
```

Run the CLI form from this directory (not the repo root) so
`out/al_pulse.ndjson` lands in `examples/appendix/mlb-pulse/out/` — its
`file_path` is resolved relative to the current working directory, not
`watershed.json`'s own location.

Also runs in Docker via the [central mount pattern](../../README.md#running-a-tutorial-in-docker) (not run or verified).

---

## See also

- [Tutorial 11 — Tideweaver Diamond](../../11-tideweaver/README.md) — canonical introduction to the `Watershed.diamond` shape
- [Tutorial 5 — Parent-Child Drilling](../../05-parent-child-drilling/README.md) — the T5 `inc_parent`/`inc_child` pattern `hitting`/`pitching` use via `Stream(parent_current=...)`
- [`crypto-graph-mapping` appendix](../crypto-graph-mapping/README.md) — the doctrine-canonical example for classes-once-in-main + pure sidecar re-import
- [`tests/public/api/test_mlb_pulse_etl.py`](../../../tests/public/api/test_mlb_pulse_etl.py) — mocked-endpoint regression counterpart to this live appendix

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/appendix/mlb-pulse/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
