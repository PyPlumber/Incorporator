***

# Tutorial 6 — State Sports: Discover, Drill, Rank

**Prerequisites:** [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md).

Pick a US state (or Canadian province) code. Discover every team whose venue sits
there across the NFL, NBA, MLB, and NHL. Drill every roster. Rank the players by
salary and by tenure. Find the ones who made it home.

T5 introduced `inc_parent` / `inc_child` fan-out on CoinGecko — a top-N market list
drilling into per-coin detail. This tutorial reruns that exact shape *twice* on
ESPN's public site API: a **whole-list** fan-out drills every league's team detail
records to read each team's real venue location, then a **single-instance** fan-out
(same verb, narrower width) drills each matching team's own roster. No auth, no API
key, ~143 HTTP requests total (~5-10s).

```bash
python examples/06-state-sports/state_sports.py               # defaults to "CA"
python examples/06-state-sports/state_sports.py ON
python examples/06-state-sports/state_sports.py "California"
```

---

## What's new here (beyond T5)

| T5 gave you | T6 adds |
|---|---|
| `inc_parent` / `inc_child` fan-out from a *list* of parents | The exact same whole-list fan-out, reused for a second detail-drill step — plus a **single-instance** `inc_parent` for the roster drill (same verb, narrower width) |
| Flat parent rows | A deep `rec_path` envelope (`sports.0.leagues.0.teams`, each row wrapped in `{"team": {...}}`) |
| `inc_code="id"` | Dotted `inc_code="team.uid"` on a wrapped record — and the reason it can't be `team.id` |
| `pluck()` for a nested lift | `pluck(key, chain=fn)` for a nested lift **plus build-time normalization** of an inconsistent source attribute |
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
instance.

---

## Brand labels vs. data attributes

An earlier version of this tutorial filtered on `team.location` — ESPN's own metro
*brand label*, not a real location field. It needed a small alias table just to catch
the one team ESPN brands abnormally (the Clippers are literally `location="LA"`, not
`"Los Angeles"`), plus a second pair of lookup tables on top of that to keep the
hometown board honest. None of that generalized past the handful of cities the tables
happened to name — Golden State, New England, Vegas, and every state-named team
(Arizona, Minnesota, Texas) had no city string that worked at all.

ESPN does publish a real structured location: `franchise.venue.address` on the
**per-team detail** endpoint (`/teams/{id}`), with `city` / `state` / (sometimes)
`zipCode` fields. It isn't on the `/teams` list endpoint — `?enable=franchise` and
`?enable=venue` are both silently ignored there — so getting at it costs a second,
per-team request. That's the trade this tutorial makes: one whole-list detail
fan-out (below) in exchange for deleting every brand-string table and gaining a
filter that generalizes to all 50 states, DC, and every Canadian province for free.

---

## The venue drill: T5's whole-list `inc_parent`, reused

```python
details = await TeamDetail.incorp(
    inc_url=f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams/{{}}",
    inc_parent=teams,          # the whole IncorporatorList Node 1 just built
    inc_child="team.id",
    rec_path="team",
    inc_code="uid",            # top-level post-rec_path -- same string as Team's "team.uid"
    inc_name="displayName",
    conv_dict={
        "venue_city": pluck("franchise.venue.address.city"),
        "venue_state": pluck("franchise.venue.address.state", chain=to_state_code),
        "venue_zip": pluck("franchise.venue.address.zipCode"),
    },
    timeout=8,
)
```

`inc_parent=teams` is a whole `IncorporatorList` here, not a single instance — this
is exactly T5's `CoinDetail.incorp(inc_parent=coins, inc_child="id", ...)` shape
([`examples/05-parent-child-drilling/parent_child_drilling.py`](../05-parent-child-drilling/parent_child_drilling.py)),
reused verbatim: the framework dedups every team in the league, fans the detail
requests out concurrently, and hands back one `TeamDetail` per team.

**`rec_path="team"` means every `conv_dict` path drills into *that team's own*
sub-object.** `"franchise.venue.address.city"` is relative to the post-`rec_path`
row — it does not need (and must not have) a `"team."` prefix, even though the raw
response is itself wrapped in `{"team": {...}}`.

**Why `inc_code="uid"` here and `inc_code="team.uid"` on `Team`.** Both resolve to
the *same string* (`"s:40~l:46~t:12"`) — `TeamDetail`'s `rec_path="team"` already
drilled into the wrapped envelope, so its own `uid` key sits at the top level of the
row it sees, whereas `Team` reads the raw, still-wrapped list response. This shared
string is the join key the filter step (below) uses to recover the original `Team`
instance.

---

## Data hygiene: `pluck(key, chain=fn)` for a build-time normalizer

ESPN's `franchise.venue.address.state` is **not normalized across leagues**:

```
NFL / NBA / NHL      ->  "CA", "ON", "TX", "DC", ...        (already 2-letter)
MLB                  ->  "California", "District of Columbia", "Ontario"  (full name)
```

The Wizards' NBA record says `"DC"`; the Nationals' MLB record for the same city
says `"District of Columbia"`. The Maple Leafs' NHL record says `"ON"`; the Blue
Jays' MLB record for the same city says `"Ontario"`. Verified live 2026-07-08 — this
is a closed, enumerable vocabulary (50 US states + DC + 13 Canadian
provinces/territories), so a lookup table normalizes it once, at build time:

```python
def to_state_code(value: str) -> str:
    return STATE_NAME_TO_CODE.get(value, value)
```

```python
"venue_state": pluck("franchise.venue.address.state", chain=to_state_code),
```

`pluck(key, chain=fn)` extracts the nested value, then runs `fn` on it before it
lands on the instance — the same pattern
[`examples/04-xml-post-audit/nhtsa_post_audit.py`](../04-xml-post-audit/nhtsa_post_audit.py)
uses for `pluck("Vehicle.Make", chain=str.upper)`. `pluck()`'s null-handling means
`chain` is never called on a missing/garbage value (see
`incorporator/schema/extractors.py`'s "Null handling" note), so `to_state_code`
itself needs no defensive `None`-guard — an already-2-letter code (or a Canadian
province ESPN already abbreviates) just passes straight through the `dict.get(...,
value)` fallback unchanged.

**`venue_zip` is informational only, never a filter gate.** It's absent on several
spot-checked teams across NFL/NBA/NHL (confirmed live: absent on the Clippers,
absent on every sampled NHL team) — only `venue_state` gates the region filter, and
`venue_zip` stays a plain `str` (no `target_type=int`; zip codes can carry meaningful
leading zeros in general, and coercing a routinely-`None` field would spam a
coercion-failure warning on every team missing it).

**Stale venues, right state.** A few of ESPN's venue records are out of date — the
Warriors' `franchise.venue` is still Oracle Arena in Oakland (they've played at
Chase Center in San Francisco since 2019); the Chargers' venue is Dignity Health
Sports Park in Carson (they've played at SoFi Stadium in Inglewood since 2020). Both
are "wrong building, right state" — the `state` field itself is accurate even where
the venue name/city drifted, and this filter only ever reads `state`.

---

## The filter: attribute equality, zero brand strings

```python
for detail in details:
    if detail.venue_state is None:
        no_venue_total += 1
        continue
    if detail.venue_state != region:
        continue
    team = Team.inc_dict.get(detail.inc_code)
    matched.append((league, sport, team))
```

There's no `state=` query parameter on ESPN's detail endpoint, and no bulk "every
team whose venue is in state X" endpoint exists at all — the filter genuinely can't
be pushed server-side, so an app-level comprehension over the already-built
`IncorporatorList[TeamDetail]` is the correct (and only) option here, not a
framework primitive. This mirrors T5 Phase 3's "two registries, manual join" idiom:
`Team.inc_dict.get(detail.inc_code)` recovers the *original* `Team` instance (not the
`TeamDetail` one) because `TeamDetail`'s `rec_path="team"` already consumed the
`team` envelope — it has no nested `"team"` key left, so the roster drill below
would silently 0-out if handed a `TeamDetail` instance instead.

A handful of teams have no reachable venue address at all (one NBA team in the
sample run, no `franchise` key present in the detail response) — those are excluded
and counted, not treated as errors; the script prints a single summary WARN line
rather than one per team.

---

## The rosters: a single-instance parent drill (unchanged shape, narrower width)

```python
players = await Player.incorp(
    inc_url=f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams/{{}}?enable=roster",
    inc_parent=team,              # one Team instance, not a list -- recovered via the join above
    inc_child="team.id",          # dotted drill off the still-present `team` attribute
    rec_path="team.athletes",
    inc_code="id",
    inc_name="fullName",
    conv_dict={...},
    timeout=8,
)
```

Run once per matched team via `asyncio.gather`, this fans out concurrently — one
request per team, not per player. It's the *same* `inc_parent`/`inc_child` verb as
the venue drill above, just a single instance instead of a whole list: T6 now
demonstrates both fan-out widths of the same primitive in one script.

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
Reading the raw paths sidesteps that ordering dependency entirely.

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

## The homegrown board: pure attribute equality

```python
heroes = [p for p in all_players if p.birth_state == region]
```

`birthPlace.state` on players uses 2-letter codes already (verified live), so it
compares directly against the normalized `region` — no metro-alias table, no
city-name matching at all. This is the direct payoff of pivoting the whole tutorial
onto a structured attribute: the exact same equality check that filters teams also
filters players, with zero brand-string bookkeeping either way.

**NY/NJ semantics, stated plainly.** The Giants and Jets play at MetLife Stadium in
East Rutherford — their venue's `state` is `"NJ"`, so they land under `NJ`, not `NY`,
under this filter's physically-plays-in semantic. The Knicks and Nets both play in
the five boroughs, so they stay under `NY`. Run `state_sports.py NJ` if you want to
see the Giants/Jets show up there instead.

---

## Sample output (CA, live run)

```text
Discovering CA's teams across NFL / NBA / MLB / NHL (ESPN site API)...
WARN: 1 team(s) had no reachable venue address - excluded from the region filter.
OK: Found 15 CA team(s): NFL Los Angeles Chargers, NFL Los Angeles Rams, NFL San Francisco 49ers, NBA Golden State Warriors, NBA LA Clippers, NBA Los Angeles Lakers, NBA Sacramento Kings, MLB Athletics, MLB Los Angeles Angels, MLB Los Angeles Dodgers, MLB San Diego Padres, MLB San Francisco Giants, NHL Anaheim Ducks, NHL Los Angeles Kings, NHL San Jose Sharks
OK: Loaded 580 active players across 15 teams.

CA across NFL / NBA / MLB / NHL
======================================================================
NFL   3 team(s), 272 active players, salary known 150/272, payroll $789,114,970
NBA   4 team(s), 77 active players, salary known 63/77, payroll $664,147,833
MLB   5 team(s), 131 active players, salary known 0/131
NHL   3 team(s), 100 active players, salary known 0/100

PAYCHECK BOARD (NFL / NBA only -- ESPN publishes no MLB/NHL salaries in this feed)
RANK PLAYER                  LG   TEAM                  POS   TENURE        SALARY   $/YR-TENURE
------------------------------------------------------------------------------------------------
1    Stephen Curry           NBA  Golden State Warriors G         16   $59,606,817    $3,725,426
2    Jimmy Butler III        NBA  Golden State Warriors F         14   $54,126,450    $3,866,175
3    Luka Doncic             NBA  Los Angeles Lakers    G          7   $54,126,450    $7,732,350
4    Zach LaVine             NBA  Sacramento Kings      G         11   $47,499,660    $4,318,151
5    Brock Purdy             NFL  San Francisco 49ers   QB         5   $46,996,000    $9,399,200
...

VETERANS BOARD (all four leagues)
RANK PLAYER                  LG   TEAM                   TENURE TURNED-PRO-AT
-----------------------------------------------------------------------------
1    Corey Perry             NHL  Los Angeles Kings          21            20
2    Anze Kopitar            NHL  Los Angeles Kings          20            18
3    Matthew Stafford        NFL  Los Angeles Rams           18            20
4    Al Horford              NBA  Golden State Warriors      18            22
5    Drew Doughty            NHL  Los Angeles Kings          18            18
...
8    Brook Lopez             NBA  LA Clippers                17            21
...

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

`state_sports.py ON` finds 4 teams (Toronto Raptors, Toronto Blue Jays, Ottawa
Senators, Toronto Maple Leafs — no NFL team plays in Ontario); `state_sports.py
"California"` normalizes the full name to `"CA"` and produces the identical result
above.

**Two boards run across all four leagues on purpose.** Salary coverage in this feed
is NFL/NBA only (0/131 for MLB, 0/100 for NHL, verified live) — a salary-only
leaderboard would silently erase half the sports this tutorial fetches. The veterans
board (tenure) and the homegrown board don't have that gap, so every league gets a
fair shot at the top of those two.

### Reading the structured reject list

Team-list and roster drills both come back as `IncorporatorList` instances carrying
`.rejects` (structured `RejectEntry` records: source URI, error class, parsed
`Retry-After`, wave index) alongside the legacy `.failed_sources` string view. This
script prints both: an unreachable league's team list is reported and skipped before
any detail or roster drill fires against it, and a roster drill that fails for one
team doesn't sink the others running concurrently in the same `asyncio.gather()`.

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
  one `incorp()` call* — handy for a per-team salary rank, but the state-wide
  leaderboards in this script are cross-team, so they use a plain `sorted()`
  instead. See `docs/api_atlas.md` for `calc_all`'s window-aggregation shape.
* **Export a board.** These are console tables, not files — wire any of the sorted
  lists into `incorporator.pipeline.outflow` / `.export(...)` (see
  [Tutorial 3](../03-universal-formats/README.md)) to land them as NDJSON/CSV instead
  of print statements.
* **A recurring state-sports refresh** (salaries update, rosters change) is a
  [Tutorial 8](../08-streaming-daemon/README.md) / [Tutorial 10](../10-multi-source-fjord/README.md)
  -shaped follow-up, not this one — this tutorial is a one-shot discovery-and-drill
  demo, deliberately without a daemon.

---

## Where to Go Next

> **Up next: [Tutorial 7 — Stateful Refresh](../07-stateful-refresh/README.md).**
> T6 drilled a one-shot snapshot of every state team's roster; T7 takes a single
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
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/06-state-sports/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
