***

# Tutorial 6 — State Sports: Two Chained Parent-Child Drills

**Prerequisites:** [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md).

Pick a US state (or Canadian province) code. Discover every team whose venue sits
there across the NFL, NBA, MLB, and NHL. Drill every roster. Rank the players by
salary and by tenure. Find the ones who made it home.

T5 introduced `inc_parent`/`inc_child` fan-out bound to a *whole list* of parents.
This tutorial reruns the exact same primitive on ESPN's public site API, twice,
chained, each time bound to a **single** parent instance in a plain `for` loop:
league discovery drills into team detail once per league (Drill 1), then every
region-filtered team drills straight into its own roster's `Player` rows once per
team (Drill 2). `inc_parent` accepts a single `Incorporator` instance just as
readily as a whole `IncorporatorList` — this is the curriculum's first canonical
example of that shape.

The per-parent loop isn't a convenience choice — it's inherent to ESPN's URL
taxonomy. ESPN's *site* API team objects carry only web-page links, no API
self-href, and the four leagues live at four different `{sport}/{league}` URL
prefixes. One whole-list `inc_parent` call takes one `inc_url` template; it can't
span four prefixes at once. The per-parent loop lets each league bring its own
prefix, read off the loop variable's own attributes as a plain f-string — no
composite-path reducer anywhere in this script.

**Drill 2 builds `Player` rows directly** — no intermediate roster class, no
in-memory hand-off. `rec_path="team.athletes"` reads straight past the roster
envelope into every athlete row, active and inactive alike (MLB's `athletes`
array is the whole organization, not the 26-man roster). Every board opens with
an `if p.active` filter before it sorts or compares, so inactive org players —
real tenure/birthplace data, no current-roster relevance — build cleanly but
never surface in a top-10.

A pure one-shot script — no Watershed, no files read or written at runtime, no
`CustomCurrent`s, `main()` fully inline and the only function in the file (read
top-to-bottom, no phase functions, no helpers). `calc`, `inc`, and `pluck` all
appear across the two drills. No auth, no API key, ~145 HTTP requests total,
~20-25s wall-clock (each drill's per-parent loop is sequential; each individual
`incorp()` call still fans its own child requests out concurrently underneath).

```bash
python examples/06-state-sports/state_sports.py      # defaults to "CA"
```

`main()` takes `region: str = "CA"` as a plain parameter — no CLI-arg parsing to
look up. To try another region, edit the `asyncio.run(main("CA"))` call in the
entry block (`main("ON")`, `main("California")`, `main("NJ")`, ...).

---

## Two chained drills, a plain series of `incorp()` calls

Unlike a Watershed (a fixed graph of nodes wired at construction time), this
tutorial's shape is two ordinary `await`ed `incorp()` calls in plain `for` loops,
threaded through a plain Python filter step and a plain Python post-drill stamp:

| Step | What runs | Shape |
|---|---|---|
| **1. Discover** | Fetch the state/province reference map, list every league's teams, drill venue detail | Drill 1: T5's `inc_parent`/`inc_child` fan-out, reused once **per league** in a `for` loop |
| **Filter** | Keep only teams whose venue sits in `region` | Plain Python comprehension — no server-side filter exists |
| **2. Roster → Player rows** | Drill every matched team's roster straight into `Player` rows | Drill 2: the same shape, reused once **per matched team** in a `for` loop |
| **Stamp** | After each drill, set `league`/`team_name` on the freshly built children from the loop's own parent | Plain Python loop over the just-returned rows |

No Watershed is needed here because nothing in this script requires a *time
window*; it runs once and exits. (T11 is this curriculum's Watershed capstone.)

---

## What's new here (beyond T5)

| T5 gave you | T6 adds |
|---|---|
| `inc_parent`/`inc_child` fan-out from a *list* of parents | The exact same primitive, reused **per-parent** in a plain `for` loop — once per `League` row (Drill 1), once per matched `Team` row (Drill 2) — stamping the parent's context onto the freshly built children after each call |
| Flat parent rows | A deep `rec_path` envelope (`sports.0`, each team/roster row wrapped in `{"team": {...}}`, roster rows one hop deeper still at `team.athletes`) |
| `inc_code="id"` | Top-level `inc_code="uid"` after `rec_path="team"` digs into the envelope — `id` collides across leagues, `uid` doesn't |
| `pluck()` for a nested lift | `pluck(key, chain=fn)` for a nested lift, backed by a live reference-data fetch, not a hardcoded table |
| One vertical (CoinGecko) | Four leagues drilled once each, each with its own coverage gaps |
| A single `incorp()` call per node | An in-memory dataset (`players`) holding every roster row, active and inactive; each board applies `if p.active` at report time |
| Composite child-value URLs built by a `calc()` reducer | The `{sport}/{league}` URL segments come straight off each parent row's own attributes, in an f-string template — no reducer needed |

---

## Reference data, fetched not hardcoded

ESPN reports state names inconsistently (MLB uses full names like
`"California"`; NFL/NBA/NHL already report `"CA"`). Normalizing that against a
hand-typed constant table is possible but has to be kept correct by hand
forever. [CountriesNow](https://countriesnow.space) publishes the same
US-state/Canada-province mapping as a free, no-auth API, so this tutorial
fetches it at runtime instead:

```python
COUNTRIESNOW_URLS = [
    "https://countriesnow.space/api/v0.1/countries/states/q?country=United%20States",
    "https://countriesnow.space/api/v0.1/countries/states/q?country=Canada",
]

states = await StateRef.incorp(
    inc_url=COUNTRIESNOW_URLS,
    rec_path="data.states",
    inc_code="state_code",
    inc_name="name",
    timeout=8,
)
state_code_map: dict[str, str] = (
    {s.inc_code: s.inc_code for s in states} | {s.inc_name: s.inc_code for s in states} | DC_SUPPLEMENT
)
if "California" not in state_code_map or "Ontario" not in state_code_map:
    sys.exit(REFERENCE_API_ERROR)
```

One `incorp()` call — `inc_url` accepts `str | list[str]`, so both countries fan
out under a single `IncorporatorList` — builds the full 50-state-plus-13-province
map at runtime, the same primitive every other fetch in this tutorial uses.

`DC_SUPPLEMENT = {"District of Columbia": "DC", "DC": "DC"}` patches the one
entry CountriesNow's feed omits, in both directions.

---

## Drill 1: league discovery, then venue detail per league

`/{sport}/{league}/teams` returns a nested envelope — one `sports[0]` row per
league holding a `slug` (sport segment) and a `leagues[0]` sub-object with its
own `slug` (league segment) and `abbreviation`. Drill 1a builds `League` rows
with **no `conv_dict` at all**: nothing needs deriving at build time, and the
schema factory's dynamic inference already exposes `lg.slug`,
`lg.leagues[0].slug`, and `lg.leagues[0].abbreviation` as attributes.

```python
league_urls = [f"{BASE}/{sport}/teams" for _, sport in SPORTS]
leagues = await League.incorp(inc_url=league_urls, rec_path="sports.0", timeout=8)
```

Drill 1b reuses T5's `inc_parent`/`inc_child` shape once per league:

```python
teams: list[Team] = []
for lg in leagues:
    part = await Team.incorp(
        inc_parent=lg,
        inc_child="leagues.teams.team.id",
        inc_url=f"{BASE}/{lg.slug}/{lg.leagues[0].slug}/teams/{{}}",
        rec_path="team",
        inc_code="uid",
        inc_name="displayName",
        conv_dict={
            "venue_city": pluck("franchise.venue.address.city"),
            "venue_state": pluck("franchise.venue.address.state", chain=state_code_map.get),
        },
        timeout=8,
    )
    for t in part:
        t.league = lg.leagues[0].abbreviation
    teams.extend(part)
```

`inc_parent=lg` is a **single** `League` instance, not a whole list — the same
primitive T5's `CoinDetail.incorp(inc_parent=coins, inc_child="id", ...)`
applies to a whole list, applied here one parent at a time.

**`rec_path="team"` scopes every `conv_dict` path to that team's own
sub-object** — `"franchise.venue.address.city"` has no `"team."` prefix, even
though the raw response is itself wrapped in `{"team": {...}}`.

---

## The filter: attribute equality, zero brand strings

```python
matched = [t for t in teams if t.venue_state == region]
if not matched:
    sys.exit(f"No {region} teams found - try 'NY', 'TX', or 'ON'.")
```

There's no `state=` query parameter on ESPN's detail endpoint and no bulk
"every team in state X" endpoint — the filter can't be pushed server-side, so a
plain comprehension over the already-built `Team` list is the only option. A
handful of teams have no reachable venue address at all (no `franchise` key in
the detail response); `venue_state` is simply `None` for those rows via
`pluck`'s missing-path handling, so they fall out of the filter without a
crash. An empty `matched` hard-exits with one ASCII line before Drill 2 makes
any roster request.

---

## Drill 2: the roster drill, straight into `Player` rows

Drill 2 reuses the same single-instance `inc_parent` shape again — this time
bound to **one matched `Team` at a time**, with a scalar `inc_child="id"` (one
`Team`, one `id`, one roster URL, exactly one URL per call) — and `rec_path`
drills one hop deeper than Drill 1's, straight past ESPN's roster envelope
into every athlete row on that team:

```python
slugs: dict[str, str] = {lg.leagues[0].abbreviation: f"{lg.slug}/{lg.leagues[0].slug}" for lg in leagues}

players: list[Player] = []
for team in matched:
    roster = await Player.incorp(
        inc_parent=team,
        inc_child="id",
        inc_url=f"{BASE}/{slugs[team.league]}/teams/{{}}?enable=roster",
        rec_path="team.athletes",
        inc_code="uid",
        inc_name="fullName",
        conv_dict={
            "active": inc(bool, default=False),
            "salary": calc(int, "contract.salary", default=0, target_type=int),
            "tenure": calc(functools.partial(max, 1), "experience.years", default=1, target_type=int),
            "age": calc(int, "age", default=0, target_type=int),
            "pos": calc(str, "position.abbreviation", default="-", target_type=str),
            "birth_city": calc(str, "birthPlace.city", default="-", target_type=str),
            "birth_state": calc(str, "birthPlace.state", default="-", target_type=str),
            "turned_pro_at": calc(operator.sub, "age", "tenure", default=0, target_type=int),
            "salary_per_year": calc(operator.truediv, "salary", "tenure", default=0.0, target_type=float),
        },
        timeout=10,
    )
    for p in roster:
        p.league, p.team_name = team.league, team.inc_name
    players.extend(roster)  # every row -- active and inactive; the boards filter later
```

`slugs` is a small runtime dict built from the `League` rows themselves, keyed
by the same abbreviation each `Team` was stamped with in Drill 1.
`rec_path="team.athletes"` drills two levels past the raw response, so every
`conv_dict` path (`contract.salary`, `experience.years`, `age`,
`position.abbreviation`, `birthPlace.city`, `birthPlace.state`, `active`) is
relative to each individual athlete dict. Right after each team's drill
returns, `p.league, p.team_name = team.league, team.inc_name` stamps every
player row from the loop variable — no `link_to()` join, because
`Player.incorp()` *is* the roster drill and the team is already the loop
variable.

Every athlete builds, active and inactive; the boards filter `if p.active` at
report time.

---

## The boards: filter active, then sort or compare

`players` — the flat list built by Drill 2 — holds every athlete row from
every matched team, active and inactive. Every board reads fields `conv_dict`
already computed (`p.salary`, `p.tenure`, `p.league`, `p.team_name`,
`p.birth_state`, `p.salary_per_year`, `p.turned_pro_at`, `p.active`) — no
`isinstance()` checks, no per-row derivation at report time. Each board
filters `if p.active` first, then sorts or compares on the raw field:

* **Paycheck board** — `p.active and p.salary > 0`, sorted by `p.salary`.
  NFL/NBA only: salary coverage in this feed doesn't include MLB or NHL, so a
  salary board that ran across all four leagues would silently erase half the
  sports fetched.
* **Veterans board** — `p.active`, sorted by `p.tenure`. Runs across all four
  leagues; tenure has no coverage gap the way salary does.
* **Homegrown board** — `p.active and p.birth_state == region`.
  `birthPlace.state` on players already uses 2-letter codes, so `birth_state`
  compares directly against the normalized `region` — no metro-alias table, no
  city matching.

**NY/NJ semantics.** The Giants and Jets play at MetLife Stadium in New
Jersey, so they land under `NJ`, not `NY`, under this filter's
physically-plays-in semantic. Call `main("NJ")` to see them show up there
instead.

Summary lines report total vs. active counts (`len(players)`, `active_count`)
— MLB's org-list quirk means the total runs far higher than active.

---

## Sample output (CA, live run)

```text
Fetching state/province reference data (CountriesNow)...
Discovering CA's teams across NFL / NBA / MLB / NHL (ESPN site API)...
OK: Found 15 CA team(s): NFL Los Angeles Chargers, NFL Los Angeles Rams, NFL San Francisco 49ers, NBA Golden State Warriors, NBA LA Clippers, NBA Los Angeles Lakers, NBA Sacramento Kings, MLB Athletics, MLB Los Angeles Angels, MLB Los Angeles Dodgers, MLB San Diego Padres, MLB San Francisco Giants, NHL Anaheim Ducks, NHL Los Angeles Kings, NHL San Jose Sharks
OK: Loaded 1730 players (580 active) across 15 teams.

CA across NFL / NBA / MLB / NHL
======================================================================
NFL   3 team(s), 272 players (272 active), salary known 145/272, payroll $789,114,970
NBA   4 team(s), 77 players (77 active), salary known 52/77, payroll $712,212,456
MLB   5 team(s), 1281 players (131 active), salary known 0/131
NHL   3 team(s), 100 players (100 active), salary known 0/100

PAYCHECK BOARD (NFL / NBA only -- ESPN publishes no MLB/NHL salaries in this feed)
RANK PLAYER                  LG   TEAM                  POS   TENURE        SALARY   $/YR-TENURE
------------------------------------------------------------------------------------------------
1    Stephen Curry           NBA  Golden State Warriors G         16   $59,606,817    $3,725,426
2    Jimmy Butler III        NBA  Golden State Warriors F         14   $54,126,450    $3,866,175
3    Luka Doncic             NBA  Los Angeles Lakers    G          7   $54,126,450    $7,732,350
4    Kawhi Leonard           NBA  LA Clippers           F         14   $50,000,000    $3,571,429
5    Zach LaVine             NBA  Sacramento Kings      G         11   $47,499,660    $4,318,151
6    Brock Purdy             NFL  San Francisco 49ers   QB         5   $46,996,000    $9,399,200
7    Domantas Sabonis        NBA  Sacramento Kings      F          9   $43,636,000    $4,848,444
8    Matthew Stafford        NFL  Los Angeles Rams      QB        18   $40,000,000    $2,222,222
9    Darius Garland          NBA  LA Clippers           G          6   $39,446,090    $6,574,348
10   Brandon Ingram          NBA  LA Clippers           F          9   $38,095,238    $4,232,804

VETERANS BOARD (all four leagues)
RANK PLAYER                  LG   TEAM                   TENURE TURNED-PRO-AT
-----------------------------------------------------------------------------
1    Corey Perry             NHL  Los Angeles Kings          21            20
2    Anze Kopitar            NHL  Los Angeles Kings          20            18
3    Matthew Stafford        NFL  Los Angeles Rams           18            20
4    Al Horford              NBA  Golden State Warriors      18            22
5    Drew Doughty            NHL  Los Angeles Kings          18            18
6    Jon Weeks               NFL  San Francisco 49ers        17            23
7    Trent Williams          NFL  San Francisco 49ers        17            20
8    Brook Lopez             NBA  LA Clippers                17            21
9    Russell Westbrook       NBA  Sacramento Kings           17            20
10   Freddie Freeman         MLB  Los Angeles Dodgers        17            19

HOMEGROWN BOARD (CA-born players on a CA team)
PLAYER                  LG   TEAM                  BORN
-------------------------------------------------------------------------------
Noah Avinger            NFL  Los Angeles Chargers  Cerritos, CA
Troy Dye                NFL  Los Angeles Chargers  Norco, CA
Daiyan Henley           NFL  Los Angeles Chargers  Los Angeles, CA
...
Cole Guttman            NHL  Los Angeles Kings     Northridge, CA
Trevor Moore            NHL  Los Angeles Kings     Thousand Oaks, CA
Andre Gasseau           NHL  San Jose Sharks       Garden Grove, CA
```

`main("ON")` finds 4 teams (Toronto Raptors, Toronto Blue Jays, Ottawa
Senators, Toronto Maple Leafs — the Blue Jays confirm the fetched Canada map
covers the full name `"Ontario"`); `main("California")` normalizes the full
name to `"CA"` through the same fetched map and produces the identical result
above.

---

## Going further

* **Cross-sport physical extremes.** The same player pool that feeds the
  veterans board also makes for a tallest/heaviest split — NBA centers run
  ~7'2", NFL linemen top 350 lbs. Filter to `p.active`, then sort by `height`
  or `weight` and print the extremes per league.
* **`calc_all()` dense-rank.** `calc_all(func, *keys, ...)` computes a rank
  *within one `incorp()` call* — handy for a per-team salary rank, but the
  state-wide leaderboards here are cross-team, so they use a plain `sorted()`
  instead. See `docs/api_atlas.md` for `calc_all`'s window-aggregation shape.
* **A recurring state-sports refresh** (salaries update, rosters change) is a
  [Tutorial 8](../08-streaming-daemon/README.md) /
  [Tutorial 10](../10-multi-source-fjord/README.md)-shaped follow-up, or — for
  a genuinely windowed, scheduled version of these same two drills — a
  [Tutorial 11](../11-tideweaver/README.md)-shaped `Watershed`.

---

## Run it

```bash
python examples/06-state-sports/state_sports.py    # pass a region by editing main("CA")
```

No CLI form fits this one: the drill templates a different `inc_url` for each
league discovered at runtime (`{sport}/{league}/teams/...`), and a declarative
`Stream(parent_current=)` issues one URL for the whole parent set — it can't
vary the URL per parent. So this per-parent-templated drill stays in Python.

---

## Where to Go Next

> **Up next: [Tutorial 7 — Stateful Refresh](../07-stateful-refresh/README.md).**
> T6 chained two `inc_parent` drills — each a plain per-parent `for` loop, the
> second drilling straight into `Player` rows and stamping each parent's
> context onto them — and let the boards filter `if p.active` at report time;
> `calc`, `inc`, and `pluck` all make an appearance along the way. T7 takes a
> single live registry and keeps it fresh with `refresh()`, three different
> ways.

| Goal | Read |
|---|---|
| See the CoinGecko-spine version of parent-child drilling | [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md) |
| Keep a registry live with `refresh()` | [Tutorial 7 — Stateful Refresh](../07-stateful-refresh/README.md) |
| See the full streaming-daemon coverage | [Tutorial 8 — Streaming Daemons](../08-streaming-daemon/README.md) |
| Fuse multiple live sources into one derived metric | [Tutorial 10 — Multi-Source Fjord](../10-multi-source-fjord/README.md) |
| See the full Tideweaver vocabulary (diamonds, penstocks, spillways, this curriculum's first `Watershed`) | [Tutorial 11 — Tideweaver](../11-tideweaver/README.md) |
| Paginated HATEOAS drill with `calc()` reductions (this tutorial's structural template) | [Appendix — PokéAPI ETL](../appendix/pokeapi-etl/README.md) |
| See another non-crypto domain in the curriculum | [Appendix — MLB Pulse](../appendix/mlb-pulse/README.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/06-state-sports/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
