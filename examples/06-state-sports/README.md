***

# Tutorial 6 — State Sports: Two Chained Parent-Child Drills, Gated Not Filtered

**Prerequisites:** [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md).

Pick a US state (or Canadian province) code. Discover every team whose venue sits
there across the NFL, NBA, MLB, and NHL. Drill every roster. Rank the players by
salary and by tenure. Find the ones who made it home.

T5 introduced `inc_parent` / `inc_child` fan-out on CoinGecko — a top-N market list
drilling into per-coin detail, with `inc_parent` bound to a *whole list* of parents.
This tutorial reruns that exact primitive on ESPN's public site API **twice,
chained** — but this time, once **per parent**, all parents concurrent via
`asyncio.gather`: league discovery drills into team detail, once per league (Drill
1), then every region-filtered team drills straight into its own roster's `Player`
rows, once per team (Drill 2). `inc_parent` accepts a single `Incorporator` instance
just as readily as a whole `IncorporatorList` — this tutorial is the curriculum's
first canonical example of that shape, and it's what lets ESPN's fixed
`{sport}/{league}/teams/{id}` URL taxonomy live in a plain f-string template read off
each parent row's own attributes, instead of a build-time composite-path reducer.

**Drill 2 builds `Player` rows directly** — `Player.incorp(inc_parent=team,
rec_path="team.athletes", ...)` reads straight past the roster envelope into every
athlete row, active and inactive alike. There's no intermediate roster class, no
in-memory hand-off, no active-only filter anywhere in the file. Instead, three
`conv_dict` entries (`active_tenure`, `active_salary`, `active_birth_state`) gate the
corresponding real field to a zero/empty sentinel whenever a row's `active` flag is
`False` — `bool` is an `int` subtype, so `operator.mul` zeroes a number and
empty-strings a string with the same primitive. Every board reads the *gated* field
for its sort key or filter predicate, so an inactive row structurally cannot win a
`sorted(..., reverse=True)[:10]` slice or an equality check, without a single
`if ...active` conditional in the pipeline. A pure one-shot script — no Watershed, no
files read or written at runtime, no `CustomCurrent`s, and `main()` is fully inline
(pokéapi-style: read top-to-bottom in dependency order, no phase-function
decomposition, and no named helper functions of any kind). `calc`, `inc`, and `pluck`
all make an appearance across the two calls. No auth, no API key, ~145 HTTP requests
total, ~14-15s wall-clock (both drills' per-parent calls run concurrently).

```bash
python examples/06-state-sports/state_sports.py      # defaults to "CA"
```

`main()` takes `region: str = "CA"` as a plain parameter — there's no CLI-arg
parsing to look up. To try another region, edit the `asyncio.run(main("CA"))`
call in the entry block at the bottom of the script (`main("ON")`,
`main("California")`, `main("NJ")`, ...).

---

## Two chained drills, gating instead of filtering

Unlike a Watershed (a fixed graph of nodes wired at construction time), this
tutorial's shape is two ordinary `await`ed `incorp()` calls (each fanned out
concurrently across its own parents), threaded through a plain Python filter step
and a plain Python post-drill stamp:

| Step | What runs | Shape |
|---|---|---|
| **1. Discover** | Fetch the state/province reference map, list every league's teams, drill venue detail | Drill 1: T5's `inc_parent`/`inc_child` fan-out, reused once **per league**, all four leagues concurrent (`asyncio.gather`) |
| **Filter** | Keep only teams whose venue sits in `region` | Plain Python comprehension — no server-side filter exists |
| **2. Roster → Player rows** | Drill every matched team's roster straight into `Player` rows, gating (not filtering) inactive rows' contribution to three derived fields | Drill 2: the *same* T5 shape, reused once **per matched team**, all teams concurrent |
| **Stamp** | `zip()` each drill result back to its parent for a one-line `league`/`team_name` context stamp | Plain Python loop over an already-built pair |

The two drills are the identical primitive — `cls.incorp(inc_parent=..., inc_child=...)`
— applied to two different verticals of the same domain, each one parent at a time
instead of a whole list at once, and each one's per-parent calls run concurrently
rather than sequentially. No Watershed is needed here because nothing in this script
requires a *time window*; it runs once and exits. (T11 is this curriculum's
Watershed capstone.)

---

## What's new here (beyond T5)

| T5 gave you | T6 adds |
|---|---|
| `inc_parent` / `inc_child` fan-out from a *list* of parents | The exact same primitive, reused **per-parent** — once per `League` row (Drill 1), once per matched `Team` row (Drill 2) — with every per-parent call in a drill running **concurrently** via `asyncio.gather`, `zip`-paired back to its parent afterward |
| Flat parent rows | A deep `rec_path` envelope (`sports.0`, each team/roster row wrapped in `{"team": {...}}`, roster rows one hop deeper still at `team.athletes`) |
| `inc_code="id"` | Top-level `inc_code="uid"` after `rec_path="team"` digs into the envelope (both drills) — and the reason it can't be the numeric `id` |
| `pluck()` for a nested lift | `pluck(key, chain=fn)` for a nested lift **plus build-time normalization** of an inconsistent source attribute — backed by a **live, identity-augmented reference-data fetch**, not a hardcoded table |
| One vertical (CoinGecko) | Four leagues drilled once each, each with its own coverage gaps |
| A single `incorp()` call per node | A row filter (`if athlete.active`) replaced by **gated `conv_dict` fields** — `operator.mul` on a `bool` zeroes a number and empty-strings a string, so an inactive row's contribution to a sort/filter key is always the sentinel, never excluded from the dataset itself |
| Composite child-value URLs built by a `calc()` reducer | The `{sport}/{league}` URL segments come straight off each parent row's own attributes, in an f-string **template**, read at loop time — no reducer needed at all |

---

## Reference data, fetched not hardcoded

Earlier versions of this tutorial normalized ESPN's inconsistent `venue_state`
strings (MLB reports full names like `"California"`; NFL/NBA/NHL already report
`"CA"`) against a 66-entry hardcoded `STATE_NAME_TO_CODE` constant. That table had to
be typed out and kept correct by hand.

[CountriesNow](https://countriesnow.space) publishes the same mapping as a free,
no-auth API — so this tutorial fetches it instead of hand-maintaining it:

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

One `incorp()` call — `inc_url` accepts `str | list[str]`, so both countries fan out
under a single `IncorporatorList` — builds the full 50-state-plus-13-province map at
**runtime**, using the same primitive every other fetch in this tutorial uses.
There's no special "reference data" mechanism; it's just another source. This whole
block, along with everything else in the script, lives directly inline in `main()` —
no phase functions.

**Identity-augmented, not a passthrough fallback.** `state_code_map` maps *both*
directions at once: every fetched code to itself (`"CA": "CA"`) and every fetched
full name to its code (`"California": "CA"`). That identity half matters because the
normalization step below reads this map through `chain=state_code_map.get` — a bare
bound method, not a wrapped helper with a `mapping.get(value, value)` passthrough. A
plain `dict.get(key)` returns `None` on a miss, not the input value, so an
already-abbreviated code (`"CA"`, `"TX"`, `"ON"`) only survives normalization because
the map itself already contains an identity entry for it — not because of any
fallback logic at the call site.

**A single multi-URL call needs a PARTIAL-failure check, not just an empty-list
check.** If one country's request fails and the other succeeds, `state_code_map` is
still non-empty — checking for a representative entry from *both* countries
(`"California"`, `"Ontario"`) catches a partial failure the same way an empty-map
check catches a total one.

**Even a live, structured reference API can have a hole.** CountriesNow's US-states
feed has no District of Columbia entry at all, under either spelling (verified live)
— but the NBA Wizards' own venue record reports the already-abbreviated `"DC"`
directly (no full-name variant to fall back on). Because `chain=state_code_map.get`
has no passthrough fallback, that gap needs an entry in **both** directions, or `"DC"`
resolves to `None` and folds into the no-venue bucket instead of matching a `DC`
region query:

```python
DC_SUPPLEMENT = {"District of Columbia": "DC", "DC": "DC"}
```

Even a live, structured, purpose-built reference dataset can have a hole — the fix is
the same size as the hole (two entries, one per direction), not a reason to distrust
the whole source. Canada's 13-province list has no equivalent gap — every NHL
Canadian team's province is covered.

**Fail fast, not silently, and to stderr.** If either CountriesNow call comes back
empty (network down, API changed shape), a silent empty map would produce a state
filter that matches nothing, with no explanation why. `sys.exit(str)` prints that
string to stderr and exits 1 — but only when it propagates all the way to the real
interpreter top level; inside a test's `pytest.raises(SystemExit)` nothing is written
to stderr by the exception itself, so tests assert on `exc_info.value.code` instead.
One clear ASCII line, one non-zero exit, no constant fallback, no partial map.

---

## Drill 1a: league discovery, no `conv_dict` at all

ESPN's `/{sport}/{league}/teams` endpoint returns a nested envelope: one `sports[0]`
row per league, holding a `slug` (the sport segment) and a `leagues[0]` sub-object
with its own `slug` (the league segment) and `abbreviation`:

```python
league_urls = [f"{BASE}/{sport}/teams" for _, sport in SPORTS]
leagues = await League.incorp(inc_url=league_urls, rec_path="sports.0", timeout=8)
```

No `conv_dict` here at all — nothing needs to be derived from a `League` row at
build time. The schema factory's dynamic-schema inference already exposes
`lg.slug`, `lg.leagues[0].slug`, and `lg.leagues[0].abbreviation` as attribute
access on every fetched row; Drill 1b reads them directly, per league, at loop time.

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
per-team request. That's the trade this tutorial makes: one detail drill per league
(below) in exchange for deleting every brand-string table and gaining a filter that
generalizes to all 50 states, DC, and every Canadian province for free.

---

## Drill 1b: the venue drill, T5's shape reused once per league, concurrent

```python
team_parts = await asyncio.gather(
    *(
        Team.incorp(
            inc_parent=lg,
            inc_child="leagues.teams.team.id",
            inc_url=f"{BASE}/{lg.slug}/{lg.leagues[0].slug}/teams/{{}}",
            rec_path="team",
            inc_code="uid",            # top-level post-rec_path -- same string as League's "team.uid"
            inc_name="displayName",
            conv_dict={
                "venue_city": pluck("franchise.venue.address.city"),
                "venue_state": pluck("franchise.venue.address.state", chain=state_code_map.get),
            },
            timeout=8,
        )
        for lg in leagues
    )
)
teams: list[Team] = []
for lg, part in zip(leagues, team_parts, strict=True):
    for t in part:
        t.league = lg.leagues[0].abbreviation
    teams.extend(part)
```

`inc_parent=lg` is a **single** `League` instance here, not a whole list — the same
primitive T5's `CoinDetail.incorp(inc_parent=coins, inc_child="id", ...)`
([`examples/05-parent-child-drilling/parent_child_drilling.py`](../05-parent-child-drilling/parent_child_drilling.py))
introduced, applied to one parent at a time. Four leagues means four calls, and
`asyncio.gather` runs all four concurrently — each one still fanning its own teams
out concurrently underneath — instead of the sequential `for ... await` a plain loop
would need. The `{sport}/{league}` URL segments come straight off `lg`'s own
attributes (`lg.slug`, `lg.leagues[0].slug`) as an f-string template, read fresh
inside each generator-expression iteration. There's no composite-path reducer
anywhere in this tutorial: the URL taxonomy question ("which of ESPN's 4 fixed path
segments do I hit next?") is answered by Python attribute access on the loop
variable, not by a build-time `calc()` derivation.

**`rec_path="team"` means every `conv_dict` path drills into *that team's own*
sub-object.** `"franchise.venue.address.city"` is relative to the post-`rec_path`
row — it does not need (and must not have) a `"team."` prefix, even though the raw
response is itself wrapped in `{"team": {...}}`.

**The one-line league stamp, now via `zip`.** `zip(leagues, team_parts)` pairs each
league back with its own drill result **after** `asyncio.gather` resolves all four
concurrently — `t.league = lg.leagues[0].abbreviation` still runs after the drill
returns, reading the loop variable `lg`, not the row's own data at all. The league
label was never something a `Team` row's own JSON could tell you (there's no reverse
pointer from a team detail response back to "which league URL fetched me"); it's
known for free from the `zip` pairing. This is the same tier of pattern as T5's own
post-drill stamping — a plain attribute set on an already-built `Incorporator`
instance, backed by Pydantic V2's `extra='allow'` on the base class.

**Why `inc_code="uid"` and not `id` — and why it isn't dotted.** Both drills set
`rec_path="team"` (Drill 2's goes one hop deeper, at `"team.athletes"`), so every row
the converters see has already been drilled inside the envelope. ESPN's numeric
`team.id` is only unique *within* a league — pooling four leagues' worth of teams
under one class with `id` as the primary key would let one league's registration
silently overwrite another's; `uid` (`"s:20~l:28~t:24"`) bakes the sport and league
into the string, so it's globally unique across every league fetched in this run.

---

## The full dotted `inc_child` path: why the short version doesn't work

The natural first instinct is a short `inc_child="teams.team.id"` — after all, `lg`
already *is* the league. That fails silently: `extract_parent_data` (the function
that drills `inc_child` paths off a parent) only auto-discovers a hop when it can
find that attribute on the current node — and `League` has no top-level `teams`
attribute at all. The teams array sits one level deeper, at `lg.leagues[0].teams`.

The fix is the **full** dotted path, `inc_child="leagues.teams.team.id"`:

```python
part = await Team.incorp(
    inc_parent=lg,
    inc_child="leagues.teams.team.id",
    ...
)
```

Walked one segment at a time off a single `League` parent: `"leagues"` (a list of
one, reached via dict/attribute access) → `"teams"` (a list, fanned out because it's
reached *through* the prior `"leagues"` segment) → `"team"` (a dict) → `"id"` (the
leaf). The BFS-fanout mechanics flatten this correctly in one pass, with **no**
list-of-lists intermediate — because the drill starts from a single parent object
instead of a whole `IncorporatorList`, there's no "list-valued leaf off a
non-list-of-parents" edge case to work around at all.

---

## The filter: attribute equality, zero brand strings

```python
matched = [t for t in teams if t.venue_state == region]
if not matched:
    sys.exit(f"No {region} teams found - try 'NY', 'TX', or 'ON'.")
```

There's no `state=` query parameter on ESPN's detail endpoint, and no bulk "every
team whose venue is in state X" endpoint exists at all — the filter genuinely can't
be pushed server-side, so an app-level comprehension over the already-built `Team`
list is the correct (and only) option here, not a framework primitive.

A handful of teams have no reachable venue address at all (one NBA team in the
sample run, no `franchise` key present in the detail response) — `venue_state` is
simply `None` for those rows via `pluck`'s missing-path handling, so they fall out
of the equality filter above without a crash.

**An empty `matched` hard-exits.** A region with no matching team would otherwise
feed Drill 2's `asyncio.gather` an empty parent list — `sys.exit(...)` stops the run
with one ASCII line instead, before any roster request is made.

---

## Drill 2: the roster drill, straight into `Player` rows, once per matched team

Drill 2 reuses T5's `inc_parent` shape again — this time bound to **one matched
`Team` instance at a time**, not a league-group of them, and `rec_path` drills one
hop deeper than Drill 1's, straight past ESPN's roster envelope into every athlete
row on that team:

```python
slugs: dict[str, str] = {lg.leagues[0].abbreviation: f"{lg.slug}/{lg.leagues[0].slug}" for lg in leagues}

rosters = await asyncio.gather(
    *(
        Player.incorp(
            inc_parent=team,
            inc_child="id",
            inc_url=f"{BASE}/{slugs[team.league]}/teams/{{}}?enable=roster",
            rec_path="team.athletes",
            inc_code="uid",  # globally unique across leagues (verified live)
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
                "active_tenure": calc(operator.mul, "active", "tenure", default=0, target_type=int),
                "active_salary": calc(operator.mul, "active", "salary", default=0, target_type=int),
                "active_birth_state": calc(operator.mul, "active", "birth_state", default="", target_type=str),
            },
            timeout=10,
        )
        for team in matched
    )
)
players: list[Player] = []
for team, roster in zip(matched, rosters, strict=True):
    for p in roster:
        p.league, p.team_name = team.league, team.inc_name
    players.extend(roster)  # ALL rows -- active and inactive, no filtering
```

`slugs` is a small **runtime** dict built from the `League` rows themselves — never
hardcoded — combining each league's `sport/league` pair into the single string the
URL template's `{}` segment needs, keyed by the same abbreviation each `Team` was
stamped with in Drill 1. There's no per-league-group bucketing step anymore: every
matched team drills its own roster independently, and `asyncio.gather` runs all of
them concurrently regardless of which league they belong to.

**`rec_path="team.athletes"` drills two levels past the raw response.** Every
`conv_dict` input path (`contract.salary`, `experience.years`, `age`,
`position.abbreviation`, `birthPlace.city`, `birthPlace.state`, `active`) is relative
to *each individual athlete dict*, not to the `team.athletes` array or the outer
`team` wrapper. This is the direct payoff of `rec_path` accepting a dotted path: one
drill produces one `Player` row per athlete, with zero hand-off code in between.

**`inc_parent=team` (singular) + `inc_child="id"` (scalar) = exactly one URL per
call.** This is the same `inc_parent`-accepts-a-single-instance primitive Drill 1
already demonstrates (`inc_parent=lg`), just with a scalar (non-list) `inc_child`
this time — one `Team`, one `id`, one roster URL. Contrast with Drill 1's
`inc_child="leagues.teams.team.id"` (a list-valued leaf off a single parent, fanning
to N URLs).

**The `league`/`team_name` stamp, via the same `zip` idiom as Drill 1.**
`zip(matched, rosters)` pairs each team back with its own roster result after
`asyncio.gather` resolves every team concurrently; `p.league, p.team_name = team.league,
team.inc_name` runs on every player row in that roster, reading the loop variables —
not anything the athlete's own JSON could tell you. There's no `link_to()` join here
at all anymore: the old roster-to-team join it powered only existed because the
prior revision's roster class was a separate `incorp()` hop from the player rows;
now that `Player.incorp()` *is* the roster drill, the team it came from is already
the loop variable, no re-linking needed.

### `active`, and the three gated fields that replace a row filter

```python
"active": inc(bool, default=False),
...
"active_tenure": calc(operator.mul, "active", "tenure", default=0, target_type=int),
"active_salary": calc(operator.mul, "active", "salary", default=0, target_type=int),
"active_birth_state": calc(operator.mul, "active", "birth_state", default="", target_type=str),
```

`"active"` is `inc(bool, default=False)`, not `calc()` — the raw athlete field is
literally `"active"` in ESPN's response, so the output key equals the source key,
which is exactly the case `inc(TYPE, ...)` is built for: a plain type coercion, not a
transform. It's declared **first** in the `conv_dict`, because every `active_*`
gated field below reads it.

The three gated fields are declared **last**, after `tenure`/`salary`/`birth_state`
have already been coerced by earlier entries in the same `conv_dict` — insertion
order matters twice over here: `active` must run before them (they all read it), and
`tenure`/`salary`/`birth_state` must run before them too (they read those
already-coerced values, never the raw JSON). `bool` is an `int` subtype in Python, so
`operator.mul` gates a number (`False * 13 == 0`, `True * 13 == 13`) and a string
via the string-repetition protocol (`False * "CA" == ""`, `True * "CA" == "CA"`) with
the *same* primitive — one `operator.*` reducer covering both cases, no
`isinstance()` branch needed.

**Why gating, not filtering.** `rec_path="team.athletes"` returns every athlete on
the roster — active and inactive alike — and `players` (built below) holds all of
them, with no `if athlete.active` anywhere in this file. MLB's roster feed in
particular reports its **entire organization** (~250 players including
minor-leaguers per team, verified live), not the 26-man active roster; those
inactive rows genuinely carry real `experience.years` and `birthPlace` data (verified
live: hundreds of inactive Dodgers organizational players have real tenure and
birthplace figures, zero have salaries) — which is exactly why gating instead of
filtering matters: the data is real, it's just not supposed to win a board. Because
every board below sorts or filters on the *gated* field (`active_tenure`,
`active_salary`, `active_birth_state`) rather than the raw one, an inactive row's
contribution is always the sentinel (`0` or `""`) — it structurally cannot outrank a
real active player's tenure, outrank a real salary, or match a birth-state equality
check, without a single row ever being excluded from the dataset.

### Why `calc(TYPE, "nested.path", default=..., target_type=TYPE)`, not `pluck()`

`pluck()` is the framework's nested-extraction primitive, but it has **no `default=`
parameter** — a missing `contract.salary` resolves to raw `None`, not a build-time
default. `inc(TYPE, default=...)` can't fill that gap either, since it reads
`d.get(key)` directly and can't drill a dotted path like `"contract.salary"`.
`calc(TYPE, "nested.path", default=..., target_type=TYPE)` — passing a bare type as
`calc()`'s callable — is the idiom that closes it: the exact same pattern
[`examples/11-tideweaver/arb_scanner.py`](../11-tideweaver/arb_scanner.py) uses for
`calc(float, "bidPrice", default=0.0, target_type=float)`.

**Insertion order is load-bearing.** `salary`/`age`/`pos`/`birth_city`/`birth_state`
all coerce independent raw paths (their order among themselves doesn't matter);
`tenure` is a floor-1 coercion (below); `turned_pro_at` and `salary_per_year` read
another entry's *output* (`age`/`tenure`, `salary`/`tenure`), so they run after those
coercions; the three `active_*` gated fields run last of all, since they read
`active` plus their own already-coerced sibling.

### Why `tenure` floors to 1, not 0

Saying a player has been on the roster for "0 years" doesn't describe anyone who
actually has a roster spot — the minimum meaningful tenure is one year.
`calc(functools.partial(max, 1), "experience.years", default=1, target_type=int)`
floors **both** cases to 1:

* **Missing `experience.years`** — `is_garbage_value(None)` is `True`, so `calc()`'s
  all-inputs-garbage short-circuit fires and the entry resolves straight to
  `default=1`, never calling `func` at all.
* **A genuine `experience.years: 0`** — `0` is not a garbage value (`is_garbage_value(0)`
  is `False`), so `func(0)` actually runs: `functools.partial(max, 1)(0) == max(1, 0) ==
  1`.

Either way, `tenure` is a real int `>= 1` by the time any later entry reads it —
including `active_tenure`, whose `operator.mul` never multiplies against a raw `None`.

### Why `salary_per_year` is a plain `calc()` entry now, zero-safe by construction

`calc()`'s `default=` only fires on missing/garbage input (per `is_garbage_value`),
never on a genuine `tenure=0` — that used to rule out a `calc(operator.truediv, ...)`
entry entirely (an unfloored `tenure=0` would raise a `ZeroDivisionError` inside the
division). Now that `tenure` is floored to `>= 1` by the entry immediately above it in
the same `conv_dict`, `calc(operator.truediv, "salary", "tenure", default=0.0,
target_type=float)` can never divide by zero — insertion order guarantees `tenure` is
already coerced-and-clamped by the time `salary_per_year` reads it. The `default=0.0`
only fires if both `salary` and `tenure` were simultaneously garbage, which can't
happen post-coercion since `tenure` is always a real int by then — a defensive floor,
not a load-bearing path.

### Why `turned_pro_at` surfaces a sentinel, not `"-"`

With `age` pre-defaulted to `0` (a real, non-garbage value — `calc()`'s all-inputs-
garbage short-circuit only fires when *every* input is missing), a row with a
genuinely missing age and a real `tenure` computes `turned_pro_at = 0 - tenure` — a
visibly **negative** integer, an honestly impossible age-turned-pro. That's the
deliberate choice here: an impossible sentinel that a reader immediately recognizes
as "this data point is missing," rather than a fabricated plausible-looking number,
and without a display-time `"-"` guard duplicating what `calc()`'s own
`default=`/pre-coercion already handles.

---

## The boards: sort/filter gated, display raw, zero derivation

`players` — the flat list built by the `zip(matched, rosters)` loop above — is every
athlete row from every matched team, active and inactive. Every board reads fields
that were already computed inside `conv_dict` — `p.salary`, `p.tenure`, `p.league`,
`p.team_name`, `p.birth_state`, `p.salary_per_year`, `p.turned_pro_at`, and the three
gated `active_*` fields. No `isinstance()` checks, no `None`-guard ladders, no
per-row derivation.

**Boards sort/filter on the gated field, but still display the raw one.** The
veterans board sorts by `active_tenure`; the paycheck board gates and sorts by
`active_salary`; the homegrown board filters by `active_birth_state == region` — but
all three still **display** the raw `p.tenure` / `p.turned_pro_at`, `p.salary` /
`p.salary_per_year`, `p.birth_city` / `p.birth_state` columns. This is safe by
construction: any row that survives a gated sort/filter is active, so its raw value
and its gated value are numerically identical there — only an inactive row (never
present past that point) would show the difference.

**Two boards run across all four leagues on purpose.** Salary coverage in this feed
is NFL/NBA only (verified live) — a salary-only leaderboard would silently erase
half the sports this tutorial fetches. The veterans board (tenure) and the homegrown
board don't have that gap, so every league gets a fair shot at the top of those two.

### The summary lines: total rows vs. active rows

```python
active_count = sum(1 for p in players if p.active)
print(f"OK: Loaded {len(players)} players ({active_count} active) across {len(matched)} teams.")
```

`players` now includes every roster row MLB's org-list quirk pulls in (hundreds of
inactive minor-leaguers per team), so `len(players)` is a genuinely large number —
the summary line reports both the raw total and the `active_count` explicitly,
rather than the old (now-inaccurate) "active players" phrasing. The per-league
summary line follows the same split:

```python
league_active_count = sum(1 for p in league_players if p.active)
salary_known_total = sum(1 for p in league_players if p.active_salary > 0)
payroll_total = sum(p.active_salary for p in league_players)
```

`salary_known_total` and `payroll_total` read `active_salary` rather than the raw
`salary` — an inactive row's salary is always gated to `0` first, so this stays the
semantically correct source even though, in this feed, no inactive MLB row happens to
carry a real salary anyway (verified live).

### The homegrown board: gated attribute equality

```python
heroes = [p for p in players if p.active_birth_state == region]
```

`birthPlace.state` on players uses 2-letter codes already (verified live), so
`active_birth_state` compares directly against the normalized `region` — no
metro-alias table, no city-name matching, and no inactive row can ever match, since
its `active_birth_state` is unconditionally `""`.

**NY/NJ semantics, stated plainly.** The Giants and Jets play at MetLife Stadium in
East Rutherford — their venue's `state` is `"NJ"`, so they land under `NJ`, not `NY`,
under this filter's physically-plays-in semantic. The Knicks and Nets both play in
the five boroughs, so they stay under `NY`. Call `main("NJ")` (edit the entry block's
`asyncio.run(...)` line) if you want to see the Giants/Jets show up there instead.

---

## Sample output (CA, live run)

```text
Fetching state/province reference data (CountriesNow)...
Discovering CA's teams across NFL / NBA / MLB / NHL (ESPN site API)...
OK: Found 15 CA team(s): NFL Los Angeles Chargers, NFL Los Angeles Rams, NFL San Francisco 49ers, NBA Golden State Warriors, NBA LA Clippers, NBA Los Angeles Lakers, NBA Sacramento Kings, MLB Athletics, MLB Los Angeles Angels, MLB Los Angeles Dodgers, MLB San Diego Padres, MLB San Francisco Giants, NHL Anaheim Ducks, NHL Los Angeles Kings, NHL San Jose Sharks
OK: Loaded 1731 players (581 active) across 15 teams.

CA across NFL / NBA / MLB / NHL
======================================================================
NFL   3 team(s), 272 players (272 active), salary known 145/272, payroll $789,114,970
NBA   4 team(s), 78 players (78 active), salary known 53/78, payroll $714,147,833
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

(Regenerated live 2026-07-09 against the gated-fields, `asyncio.gather` revision —
the veterans and homegrown boards are byte-for-byte identical to the prior
revision's output, proving the gating design: no inactive row surfaces anywhere. The
paycheck board's #4 entry shifted (Kawhi Leonard's fixture-independent live salary
changed since the prior sample) — an ESPN-side data drift, not a pipeline change.
`len(players)` jumped from 580 to 1731 because MLB's roster feed's inactive
organizational rows — always excluded by the old `if athlete.active` filter — are
now part of the dataset, gated instead of dropped; `active_count` (581) matches the
old "active players" figure within normal roster churn.)

`main("ON")` finds 4 teams (Toronto Raptors, Toronto Blue Jays, Ottawa Senators,
Toronto Maple Leafs — the Blue Jays prove the *fetched* Canada map covers the full
name `"Ontario"`); `main("California")` normalizes the full name to `"CA"` through
the same fetched map and produces the identical result above.

**If the reference API is unreachable**, the run stops immediately with one line —
`ERROR: reference API unreachable - cannot normalize state names.` — and a non-zero
exit, before any ESPN request is made.

**No files are read or written at runtime.** Every board above reads the `Team` /
`Player` instances `incorp()` returns directly, in-process —
`examples/06-state-sports/` is byte-identical before and after any run.

### The structured reject list, still there if you need it

Every `incorp()` call in this tutorial comes back as an `IncorporatorList`, carrying
`.rejects` (structured `RejectEntry` records: source URI, error class, parsed
`Retry-After`, wave index) — an unreachable league's team list or a failed
team-detail/roster drill would each land here. This script doesn't print them
proactively (the framework already surfaces failures; a per-call
`if X.rejects: print(...)` loop after every fetch would be pure ceremony), but the
data is one attribute access away for anyone who wants it:

```python
part = await Team.incorp(...)
if part.rejects:
    for entry in part.rejects:
        print(entry)
```

---

## Going further

* **Cross-sport physical extremes.** The same player pool that feeds the veterans
  board also makes for a fun tallest/heaviest split — NBA centers run ~7'2", NFL
  linemen top 350 lbs. Sort the roster rows by `height` or `weight` (gated the same
  way) and print the extremes per league.
* **`calc_all()` dense-rank.** `calc_all(func, *keys, ...)` computes a rank *within
  one `incorp()` call* — handy for a per-team salary rank, but the state-wide
  leaderboards in this script are cross-team, so they use a plain `sorted()`
  instead. See `docs/api_atlas.md` for `calc_all`'s window-aggregation shape.
* **A recurring state-sports refresh** (salaries update, rosters change) is a
  [Tutorial 8](../08-streaming-daemon/README.md) /
  [Tutorial 10](../10-multi-source-fjord/README.md) -shaped follow-up, or — if you
  want a genuinely windowed, scheduled version of these same two drills — a
  [Tutorial 11](../11-tideweaver/README.md)-shaped `Watershed`.

---

## Where to Go Next

> **Up next: [Tutorial 7 — Stateful Refresh](../07-stateful-refresh/README.md).**
> T6 chained two `inc_parent` drills — each reused once per parent and run
> concurrently via `asyncio.gather` — with the second drilling straight into
> `Player` rows and gating inactive contributions instead of filtering them out;
> `calc`, `inc`, and `pluck` all make an appearance along the way; T7 takes a single
> live registry and keeps it fresh with `refresh()`, three different ways.

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
