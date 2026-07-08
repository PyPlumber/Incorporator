***

# 🏙️ Tutorial 6 — City Sports: Discover, Drill, Rank

**Prerequisites:** [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md).

Pick a city. Discover every team it fields across the NFL, NBA, MLB, and NHL. Drill
every roster. Rank the players by salary and by tenure. Find the ones who made it
home.

T5 introduced `inc_parent` / `inc_child` fan-out on CoinGecko — a top-N market list
drilling into per-coin detail. This tutorial reruns the same verbs on ESPN's public
site API, but the parent this time is a **single already-built instance**, not a
whole list: one city team drills its own roster, and you fan that pattern out over
every team the city fields. No auth, no API key, ~10 HTTP requests total.

```bash
python examples/06-city-sports/city_sports.py               # defaults to "Los Angeles"
python examples/06-city-sports/city_sports.py "New York"
```

---

## What's new here (beyond T5)

| T5 gave you | T6 adds |
|---|---|
| `inc_parent` / `inc_child` fan-out from a *list* of parents | Single-instance `inc_parent` — one `Team` drills its own roster |
| Flat parent rows | A deep `rec_path` envelope (`sports.0.leagues.0.teams`, each row wrapped in `{"team": {...}}`) |
| `inc_code="id"` | Dotted `inc_code="team.uid"` on a wrapped record — and the reason it can't be `team.id` |
| `pluck()` for a nested lift | `calc()` for a genuinely derived field (salary ÷ tenure, age − tenure) |
| One vertical (CoinGecko) | Four leagues fanned out concurrently, each with its own coverage gaps |

---

## The teams: a wrapped, dotted-PK record

ESPN's `/teams` endpoint wraps every row in `{"team": {...}}`:

```python
teams = await Team.incorp(
    inc_url=f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams",
    rec_path="sports.0.leagues.0.teams",
    inc_code="team.uid",          # dotted PK on the wrapped record
    inc_name="team.displayName",
    conv_dict={
        "location": pluck("team.location"),
        "team_id": pluck("team.id"),
        "abbreviation": pluck("team.abbreviation"),
    },
    timeout=8,
)
```

`inc_code` and `inc_name` support dot-notation drilling into nested structures —
`incorp()`'s own docstring documents `inc_code="team.id"` as the canonical example;
this design just goes one segment deeper.

**Why `inc_code="team.uid"` and not `inc_code="team.id"`.** ESPN's numeric `team.id`
is only unique *within* a league — the NBA's Lakers and the NFL's Raiders can both be
`id=13`. Pool four leagues' worth of teams under the same `Team` class with `id` as
the primary key and one league silently overwrites another's registration in
`Team.inc_dict`. `team.uid` (`"s:20~l:28~t:24"`) bakes the sport and league into the
string, so it's globally unique across every league fetched in this run.
`team_id` (the raw numeric `team.id`) is still lifted into `conv_dict` for display —
it's just not the key anything is looked up by.

**The raw `team` envelope survives the build.** `conv_dict` *adds* `location` /
`team_id` / `abbreviation` — it doesn't consume or drop the original `team` key. That
matters for the roster drill below, which reads `team.id` back off the already-built
instance. If a future edit added `excl_lst=["team"]` to shed payload weight, that
drill would silently return zero URLs and an empty roster with no exception raised —
watch for that if you extend this script.

---

## The rosters: a single-instance parent drill

`inc_parent` doesn't require a whole `IncorporatorList` — a bare instance works too:

```python
players = await Player.incorp(
    inc_url=f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams/{{}}?enable=roster",
    inc_parent=team,              # one Team instance, not a list
    inc_child="team.id",          # dotted drill off the still-present `team` attribute
    rec_path="team.athletes",
    inc_code="id",
    inc_name="fullName",
    conv_dict={...},
    timeout=8,
)
```

Run once per city team via `asyncio.gather`, this fans out 2–7 roster requests
concurrently — one per team, not per player.

**Why `?enable=roster` instead of the plain `/teams/{id}/roster` endpoint.** The plain
roster endpoint groups NFL/MLB/NHL players by position — a shape `rec_path` can't
flatten (it supports dotted keys and integer indices, not a wildcard across an
unknown number of position groups). `?enable=roster` returns a single flat
`team.athletes` array, uniform across all four leagues, and its contract data is
current (the plain endpoint's MLB `contracts` field is stale 2015 data).

**MLB's org-list quirk.** `team.athletes` for MLB returns the *entire organization*
(~250 players including minor-leaguers), not the 26-man active roster. Every league's
roster gets filtered on `active == True` before it touches any board — NFL/NBA/NHL
are all-active already, so the filter is a no-op there, but skipping it for MLB would
flood the boards with players who aren't on the big-league roster.

---

## The derived metrics: `calc()`, and its one sharp edge

```python
def salary_per_year(salary: int | None, tenure: int | None) -> float | None:
    if salary is None:
        return None
    return salary / max(tenure or 1, 1)


def turned_pro_at(age: int | None, tenure: int | None) -> int | None:
    if age is None:
        return None
    return age - (tenure or 0)
```

```python
"salary_per_year": calc(salary_per_year, "contract.salary", "experience.years"),
"turned_pro_at": calc(turned_pro_at, "age", "experience.years"),
```

Two things worth calling out:

**Read raw dotted paths, not the flattened conv_dict outputs.** `calc()` reads
`"contract.salary"` / `"experience.years"` directly off the row rather than the
already-flattened `salary` / `tenure` fields defined earlier in the same
`conv_dict`. `conv_dict` entries apply in insertion order — a field can only read
another field's *output* if that field was declared earlier in the dict literal.
Reading the raw paths sidesteps that ordering dependency entirely: `salary_per_year`
and `turned_pro_at` would work regardless of where they're written in the dict.

**Don't pass `target_type=` here.** `calc()`'s built-in "is this whole call garbage"
pre-check only short-circuits when *every* input is garbage — `salary_per_year(None,
3)` still calls the function (tenure is real), and the function's own `if salary is
None: return None` guard is what stops the crash. That's correct, and it's the one
place in this design where a manual `None`-guard belongs inside a `conv_dict`
callable. But if you *also* pass `target_type=float`, the coercion pass runs after
the function returns and doesn't know a clean `None` return was intentional — it logs
`"calc type coercion failed for key ... value None"` on every no-salary row. Roughly
half the players in this feed have no published salary, so that's 100+ warning lines
on a single clean run. Omit `target_type=`; the arithmetic already returns the right
native type when the inputs are real, and a clean `None` when they aren't.

---

## The city-string caveat

ESPN's `team.location` is its own metro label, not necessarily what you'd type.
A plain `t.location == city` comprehension is the whole filter — there's no
city/location query parameter on `/teams` to push this server-side, so an app-level
comprehension over the already-built list is the correct (and only) option here, not
a framework primitive.

| You type | ESPN's team plays as |
|---|---|
| `"Brooklyn"` | Nets are `"Brooklyn"`, not `"New York"` |
| `"New Jersey"` | Devils are `"New Jersey"`, a different metro string from the Giants/Jets' `"New York"` |
| `"New England"`, `"Golden State"`, `"Vegas"` | State/region names, not the city you'd guess |
| `"Arizona"`, `"Minnesota"`, `"Texas"` | State-named teams — no single city string covers them |

Try `"New York"`, `"Chicago"`, or `"Boston"` if you want to see the fan-out work on a
city with a different team count than the default.

---

## Sample output (Los Angeles, live run)

```text
Discovering Los Angeles's teams across NFL / NBA / MLB / NHL (ESPN site API)...
OK: Found 6 Los Angeles team(s): NFL Los Angeles Chargers, NFL Los Angeles Rams, NBA Los Angeles Lakers, MLB Los Angeles Angels, MLB Los Angeles Dodgers, NHL Los Angeles Kings
OK: Loaded 282 active players across 6 teams.

Los Angeles across NFL / NBA / MLB / NHL
======================================================================
NFL   2 team(s), 177 active players, salary known 93/177, payroll $494,301,100
NBA   1 team(s), 20 active players, salary known 17/20, payroll $155,080,847
MLB   2 team(s), 52 active players, salary known 0/52
NHL   1 team(s), 33 active players, salary known 0/33

PAYCHECK BOARD (NFL / NBA only -- ESPN publishes no MLB/NHL salaries in this feed)
RANK PLAYER                  LG   TEAM                  POS   TENURE        SALARY   $/YR-TENURE
------------------------------------------------------------------------------------------------
1    Luka Doncic             NBA  Los Angeles Lakers    G          7   $54,126,450    $7,732,350
2    Matthew Stafford        NFL  Los Angeles Rams      QB        18   $40,000,000    $2,222,222
...

VETERANS BOARD (all four leagues)
RANK PLAYER                  LG   TEAM                   TENURE TURNED-PRO-AT
-----------------------------------------------------------------------------
1    Corey Perry             NHL  Los Angeles Kings          21            20
2    Anze Kopitar            NHL  Los Angeles Kings          20            18
3    Matthew Stafford        NFL  Los Angeles Rams           18            20
...

HOMETOWN HEROES (Los Angeles metro, state-matched)
PLAYER                  LG   TEAM                  BORN
-------------------------------------------------------------------------------
Daiyan Henley           NFL  Los Angeles Chargers  Los Angeles, CA
Tuli Tuipulotu          NFL  Los Angeles Chargers  Hawthorne, CA
Alex Johnson            NFL  Los Angeles Rams      Carson, CA
Quentin Lake            NFL  Los Angeles Rams      Irvine, CA
Mark Redman             NFL  Los Angeles Rams      Newport Beach, CA
Coleman Shelton         NFL  Los Angeles Rams      Pasadena, CA
Wade Meckler            MLB  Los Angeles Angels    Anaheim, CA
Cole Guttman            NHL  Los Angeles Kings     Northridge, CA
```

**Two boards run across all four leagues on purpose.** Salary coverage in this feed
is NFL/NBA only (0/52 for MLB, 0/33 for NHL, verified live) — a salary-only
leaderboard would silently erase half the sports this tutorial fetches. The veterans
board (tenure) and the hometown-heroes board don't have that gap, so every league
gets a fair shot at the top of those two.

### The data-hygiene lesson behind "hometown heroes"

A naive hometown filter matching on city name alone has a real trap, verified live
on this very roster: Chargers running back **Jaret Patterson** was born in
**Glendale, Missouri** — not Glendale, California, the LA-metro city that rightly
sits in `HOMETOWN_METRO["Los Angeles"]`. A city-only match would wrongly crown him
an LA hometown hero. The `birth_state` guard alongside `birth_city` is what keeps
this board honest; both this script's `HOMETOWN_METRO` + `CITY_STATE` tables and any
extension you write should carry the state check, not just the city string.

### Reading the structured reject list

Team-list and roster drills both come back as `IncorporatorList` instances carrying
`.rejects` (structured `RejectEntry` records: source URI, error class, parsed
`Retry-After`, wave index) alongside the legacy `.failed_sources` string view. This
script prints both: an unreachable league's team list is reported and skipped before
any roster drill fires against it, and a roster drill that fails for one city team
doesn't sink the others running concurrently in the same `asyncio.gather()`.

```python
for entry in teams.rejects:
    print(f"   - {entry}")
```

---

## Going further

* **Cross-sport physical extremes.** The same active-player pool that feeds the
  veterans board also makes for a fun tallest/heaviest split — NBA centers run
  ~7'2", NFL linemen top 350 lbs. Sort `all_players` by `height` or `weight` and
  print the extremes per league.
* **`calc_all()` dense-rank.** `calc_all(func, *keys, ...)` computes a rank *within
  one `incorp()` call* — handy for a per-team salary rank, but the city-wide
  leaderboards in this script are cross-team, so they use a plain `sorted()`
  instead. See `docs/api_atlas.md` for `calc_all`'s window-aggregation shape.
* **Export a board.** These are console tables, not files — wire any of the sorted
  lists into `incorporator.pipeline.outflow` / `.export(...)` (see
  [Tutorial 3](../03-universal-formats/README.md)) to land them as NDJSON/CSV instead
  of print statements.
* **A recurring city-sports refresh** (salaries update, rosters change) is a
  [Tutorial 8](../08-streaming-daemon/README.md) / [Tutorial 10](../10-multi-source-fjord/README.md)
  -shaped follow-up, not this one — this tutorial is a one-shot discovery-and-drill
  demo, deliberately without a daemon.

---

## Where to Go Next

> 👉 **Up next: [Tutorial 7 — Stateful Refresh](../07-stateful-refresh/README.md).**
> T6 drilled a one-shot snapshot of every city team's roster; T7 takes a single
> live registry and keeps it fresh with `refresh()`, three different ways.

| Goal | Read |
|---|---|
| See the CoinGecko-spine version of parent-child drilling | [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md) |
| Keep a registry live with `refresh()` | [Tutorial 7 — Stateful Refresh](../07-stateful-refresh/README.md) |
| See the full streaming-daemon coverage | [Tutorial 8 — Streaming Daemons](../08-streaming-daemon/README.md) |
| Fuse multiple live sources into one derived metric | [Tutorial 10 — Multi-Source Fjord](../10-multi-source-fjord/README.md) |
| See another non-crypto domain in the curriculum | [Appendix — MLB Pulse](../appendix/mlb-pulse/README.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/06-city-sports/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
