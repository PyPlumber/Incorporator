***

# Tutorial 6 — State Sports: Discover, Drill, Rank

**Prerequisites:** [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md).

Pick a US state (or Canadian province) code. Discover every team whose venue sits
there across the NFL, NBA, MLB, and NHL. Drill every roster through a single-pass
Tideweaver `Watershed`. Rank the players by salary and by tenure. Find the ones who
made it home.

T5 introduced `inc_parent` / `inc_child` fan-out on CoinGecko — a top-N market list
drilling into per-coin detail. This tutorial reruns that exact shape on ESPN's public
site API for team discovery (Phase 1), then hands the roster drill off to this
curriculum's **first Tideweaver `Watershed`** (Phase 2) — a two-current pipeline that
drills every matched team's roster and joins the results into an exportable board.
No auth, no API key, ~145 HTTP requests total (~10-30s for discovery, plus a fixed
~15s Watershed window).

```bash
python examples/06-state-sports/state_sports.py               # defaults to "CA"
python examples/06-state-sports/state_sports.py ON
python examples/06-state-sports/state_sports.py "California"
```

---

## Two phases

This script is genuinely two things stitched together, because Tideweaver's
current/edge topology is fixed at construction time: you cannot make "discover this
state's teams" a Watershed node whose *output* decides how many *other* nodes exist.
So the region filter is fully resolved in **plain Python first**, and only the
roster drill — whose *width* (how many teams) is now known — becomes a Watershed:

| Phase | What runs | Shape |
|---|---|---|
| **1. Discover** | Fetch the state/province reference map, list every league's teams, drill venue detail, filter to `region` | Plain `async`/`await`, T5's `inc_parent`/`inc_child` fan-out (unchanged from earlier versions of this tutorial) |
| **2. Drill & board** | Drill every matched team's roster, tag league/team-name context, join into one board, export NDJSON | A 2-current Tideweaver `Watershed` — this tutorial's new ground |

---

## What's new here (beyond T5)

| T5 gave you | T6 adds |
|---|---|
| `inc_parent` / `inc_child` fan-out from a *list* of parents | The exact same whole-list fan-out, reused for a venue-detail drill — plus a **single-instance** `inc_parent` for the roster drill (same verb, narrower width) |
| Flat parent rows | A deep `rec_path` envelope (`sports.0.leagues.0.teams`, each row wrapped in `{"team": {...}}`) |
| `inc_code="id"` | Dotted `inc_code="team.uid"` on a wrapped record — and the reason it can't be `team.id` |
| `pluck()` for a nested lift | `pluck(key, chain=fn)` for a nested lift **plus build-time normalization** of an inconsistent source attribute — now backed by a **live reference-data fetch**, not a hardcoded table |
| One vertical (CoinGecko) | Four leagues fanned out concurrently, each with its own coverage gaps |
| — | **This tutorial's first `Watershed`**: a `CustomCurrent` roster drill feeding a `Fjord` board, gated `"weir"`, single-pass |

---

## Reference data as a Current: fetching the state/province map

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


async def fetch_state_code_map() -> dict[str, str]:
    mapping: dict[str, str] = dict(DC_SUPPLEMENT)
    for url in COUNTRIESNOW_URLS:
        states = await StateRef.incorp(
            inc_url=url,
            rec_path="data.states",
            inc_code="state_code",
            inc_name="name",
            timeout=8,
        )
        if not states:
            print(REFERENCE_API_ERROR)
            sys.exit(1)
        for state in states:
            mapping[state.inc_name] = state.inc_code
    return mapping
```

Two calls — one per country — build the full 50-state-plus-13-province map at
**runtime**, using the same `incorp()` primitive every other fetch in this tutorial
uses. There's no special "reference data" mechanism; it's just another source.

**A live reference API still needs a hygiene check.** CountriesNow's US-states feed
has no District of Columbia entry (verified live 2026-07-08) — but the MLB Nationals'
venue record reports `"District of Columbia"`. A one-entry supplement closes that one
gap:

```python
DC_SUPPLEMENT = {"District of Columbia": "DC"}
```

Even a live, structured, purpose-built reference dataset can have a hole — the fix is
the same size as the hole (one entry), not a reason to distrust the whole source.

**Fail fast, not silently.** If either CountriesNow call comes back empty (network
down, API changed shape), a silent empty map would produce a state filter that
matches nothing, with no explanation why. Instead:

```python
if not states:
    print(REFERENCE_API_ERROR)
    sys.exit(1)
```

One clear ASCII line, one non-zero exit. No constant fallback, no partial map.

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
        "venue_state": pluck("franchise.venue.address.state", chain=to_code),
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

## Data hygiene: `pluck(key, chain=fn)`, now bound to a fetched map

ESPN's `franchise.venue.address.state` is **not normalized across leagues**:

```
NFL / NBA / NHL      ->  "CA", "ON", "TX", "DC", ...        (already 2-letter)
MLB                  ->  "California", "District of Columbia", "Ontario"  (full name)
```

The Wizards' NBA record says `"DC"`; the Nationals' MLB record for the same city
says `"District of Columbia"`. The Maple Leafs' NHL record says `"ON"`; the Blue
Jays' MLB record for the same city says `"Ontario"`. Verified live 2026-07-08 — this
is a closed, enumerable vocabulary (50 US states + DC + 13 Canadian
provinces/territories), so the fetched `state_code_map` (above) normalizes it once,
at build time:

```python
def to_state_code(mapping: dict[str, str], value: str) -> str:
    return mapping.get(value, value)
```

```python
to_code = functools.partial(to_state_code, state_code_map)
...
"venue_state": pluck("franchise.venue.address.state", chain=to_code),
```

`to_state_code` now takes the mapping as an explicit argument rather than reading a
module-level constant — `functools.partial(to_state_code, state_code_map)` binds the
fetched map as the first positional argument, producing the same unary
`value -> code` callable `pluck(..., chain=...)` expects, without a mutable global
the reader has to reason about lifetime for.

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
framework primitive. This runs entirely in Phase 1, before any Tideweaver `Current`
exists — `matched` (the list of teams whose roster gets drilled in Phase 2) has to be
known *before* the Watershed can be constructed. `Team.inc_dict.get(detail.inc_code)`
recovers the *original* `Team` instance (not the `TeamDetail` one) because
`TeamDetail`'s `rec_path="team"` already consumed the `team` envelope — it has no
nested `"team"` key left, so the roster drill below would silently 0-out if handed a
`TeamDetail` instance instead.

A handful of teams have no reachable venue address at all (one NBA team in the
sample run, no `franchise` key present in the detail response) — those are excluded
and counted, not treated as errors; the script prints a single summary WARN line
rather than one per team.

---

## Phase 2: this tutorial's first Watershed

Everything above is plain `async`/`await` — no orchestration primitive involved.
Phase 2 is where this tutorial introduces Tideweaver's vocabulary: a `Watershed` is a
declarative plan (a time window plus a graph of named `Current` nodes); a
`Tideweaver` runs that plan, ticking each current on its own interval; a `Wave` is one
current's per-tick output. This tutorial uses the simplest non-trivial shape —
`Watershed.chain(...)`, two currents, one edge — deliberately: T11 remains the
capstone that walks the full vocabulary (diamonds, penstocks, spillways, multi-source
fan-in).

```python
roster_drill = RosterDrill(
    name="roster_drill",
    cls=Player,
    interval=60.0,
    on_error="isolate",
    matched_teams=state_teams,
)
boards = Fjord(
    name="boards",
    cls=Roster,
    interval=60.0,
    on_error="isolate",
    export_params={"file_path": str(out_file), "format": "ndjson", "if_exists": "replace"},
)

window = (now, now + timedelta(seconds=15))
watershed = Watershed.chain(
    window=window,
    currents=[roster_drill, boards],
    gate_mode="weir",
    outflow=str(OUTFLOW_PATH),
    drain_timeout=15.0,
)

async for tide in Tideweaver(watershed).run():
    print(f"Tide {tide.tide_number:3d} | fired: {','.join(tide.fired) or '-'} | skipped: {len(tide.skipped)}")
```

### Why `roster_drill` is a `CustomCurrent`, not a `Stream(parent_current=...)`

ESPN's roster response (`team.athletes[i]`) carries **no back-reference to its
parent team at all** — no team id, no team name, nothing naming which team an
athlete belongs to. `team.displayName` / `team.location` are siblings of the
`athletes` array *one level up*, and are lost the moment `rec_path="team.athletes"`
selects the array. A `Stream(parent_current=...)` whole-list fan-out (T5's own shape)
would merge every matched team's roster into one undifferentiated `Player` registry
with **no way to know which player came from which team**.

The only way to attach `team_name` / `league` per player is a **per-team**
`incorp()` call, followed by a **post-hoc Python tagging loop** — exactly what
`drill_roster()` already does:

```python
class RosterDrill(CustomCurrent):
    matched_teams: list[tuple[str, str, Any]] = Field(default_factory=list)
    roster_rejects: list[tuple[str, list[str]]] = Field(default_factory=list)

    async def tick(self, scheduler: Any) -> None:
        rosters = await asyncio.gather(
            *(drill_roster(league, sport, team) for league, sport, team in self.matched_teams)
        )
        active_players: list[Player] = []
        rejects: list[tuple[str, list[str]]] = []
        for _league, team, active, failed_sources in rosters:
            active_players.extend(active)
            if failed_sources:
                rejects.append((team.inc_name, failed_sources))
        self.roster_rejects = rejects
        Player._tideweaver_snapshot = active_players
```

That's `asyncio.gather` over N per-team `incorp()` calls, a tagging loop, and an
explicit `_tideweaver_snapshot` reassignment — not a single declarative `incorp()`
call, which is precisely `CustomCurrent`'s documented use case. The manual assignment
also matters for a second reason: it must be a **new** list object, not a mutation of
the existing one. `CustomCurrent`'s auto-park identity check (`is pre`) would
otherwise snapshot the *entire raw* `Player.inc_dict` — including MLB's ~250-player
organization roster this active-only filter has already excluded.

### Why `boards` is a `Fjord`, even with one upstream

A `Fjord` current snapshots its upstream currents' registries, runs an `outflow(state)`
function, and materializes the result into an output class ready for export. Even
with a single upstream source, that's still a legitimate use: it hands the noisy,
weak-ref'd `Player` registry off to a stable, park-friendly `Roster` output class
and exports NDJSON "for free" — closing the earlier version of this tutorial's
"Going further: export a board" idea.

`outflow.py`:

```python
class Roster(Incorporator):
    league: str | None = None
    team_name: str | None = None
    salary: int | None = None
    tenure: int | None = None
    pos: str | None = None
    birth_city: str | None = None
    birth_state: str | None = None
    salary_per_year: float | None = None
    turned_pro_at: int | None = None


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for p in state.get("Player", []):
        rows.append(
            {
                "inc_code": f"{p.league}:{p.inc_code}",  # league-qualified
                "inc_name": p.inc_name,
                "league": p.league,
                "team_name": p.team_name,
                "salary": p.salary,
                ...
            }
        )
    return rows
```

**Why `Roster` declares every field explicitly, not `class Roster(Incorporator): pass`.**
This is a real, easy-to-hit trap: if a pre-declared Fjord output class is bare (no
fields beyond the base `Incorporator`), and the row dict carries keys the bare class
would drop, `incorporator/pipeline/outflow.py::flush` fires a one-time
`logger.warning` (which reaches stderr by default, breaking a "zero stderr"
expectation) **and silently swaps in a dynamically-built subclass** via
`infer_dynamic_schema` — a *different* Python class object than the `Roster` you
declared. Declaring every field explicitly keeps the class you wrote as the one the
flush actually uses.

**Why `inc_code` is league-qualified (`f"{league}:{p.inc_code}"`).** ESPN's athlete
ids are only guaranteed unique *within* one sport — the same caution as `Team`'s
`inc_code="team.uid"` earlier in this tutorial, applied preemptively here.

### The timing: a fixed window, not "however long the work takes"

`Tideweaver.run()` never exits early just because every current has fired its
one-and-only tick — it always blocks for the **full window length**. Both currents'
`interval=60.0` is set *longer* than the 15-second window on purpose: each current
fires exactly once (on the very first pass), and the window length becomes fixed dead
time the script pays regardless of how fast the actual work finishes. Once
`roster_drill` completes its tick, `boards` is woken **immediately** — Tideweaver
wakes on the earliest of its due-time heap or a just-completed upstream tick, not on
a fixed polling interval — so in practice both currents fire within the first few
seconds of the window opening; the remaining time is the (deliberately generous)
safety margin for real ESPN latency.

`gate_mode="weir"` — a `Weir` gate requires a *fresh* upstream wave but doesn't block
on in-flight status, which is exactly what a 2-node chain with a single upstream
needs (`"hard"` would add nothing here). `on_error="isolate"` on both currents: each
per-team roster fetch inside `drill_roster()`/`incorp()` already swallows its own
network failures into `failed_sources` rather than raising, so a top-level exception
reaching the scheduler means a genuine bug, not a transient blip — `"restart"`'s
5-attempt exponential backoff would just eat drain-timeout budget for no benefit.

### Reading the result: the exported file, not the class snapshot

```python
roster_rows: list[Any] = []
if out_file.exists():
    for line in out_file.read_text(encoding="utf-8").splitlines():
        if line.strip():
            roster_rows.append(SimpleNamespace(**json.loads(line)))
```

This script reads the just-written NDJSON export back, not
`Roster._tideweaver_snapshot`. The Fjord flush parks that snapshot on the `Roster`
class object *its own* `outflow.py` load resolves internally — a distinct Python
class object from the one this script imported at the top via a plain `sys.path`
import, because the framework loads sidecar files through its own hashed-module-name
cache, independent of any prior plain import of the same file. Re-reading the export
(the same pattern [`examples/11-tideweaver/arb_scanner.py`](../11-tideweaver/arb_scanner.py)
uses) sidesteps that cross-module identity split entirely — it's why `export_params`
uses `"if_exists": "replace"` here rather than `"append"`: this is a fresh
per-run snapshot, not an accumulating log.

---

## The rosters, unchanged: a single-instance parent drill

```python
async def drill_roster(league: str, sport: str, team: Team) -> tuple[str, Team, list[Player], list[str]]:
    players = await Player.incorp(
        inc_url=f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams/{{}}?enable=roster",
        inc_parent=team,              # one Team instance, not a list
        inc_child="team.id",
        rec_path="team.athletes",
        inc_code="id",
        inc_name="fullName",
        conv_dict={...},
        timeout=8,
    )
    active = [p for p in players if p.active]
    for p in active:
        p.league = league
        p.team_name = team.inc_name
    return league, team, active, players.failed_sources
```

This function itself hasn't changed since the pre-Watershed version of this
tutorial — it's still the *same* `inc_parent`/`inc_child` verb as the venue drill
above, just a single instance instead of a whole list. What changed is *who calls
it*: `RosterDrill.tick()` now runs it inside a Tideweaver current instead of a bare
`asyncio.gather()` in `main()`.

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
Fetching state/province reference data (CountriesNow)...
Discovering CA's teams across NFL / NBA / MLB / NHL (ESPN site API)...
WARN: 1 team(s) had no reachable venue address - excluded from the region filter.
OK: Found 15 CA team(s): NFL Los Angeles Chargers, NFL Los Angeles Rams, NFL San Francisco 49ers, NBA Golden State Warriors, NBA LA Clippers, NBA Los Angeles Lakers, NBA Sacramento Kings, MLB Athletics, MLB Los Angeles Angels, MLB Los Angeles Dodgers, MLB San Diego Padres, MLB San Francisco Giants, NHL Anaheim Ducks, NHL Los Angeles Kings, NHL San Jose Sharks

Running single-pass roster watershed for CA (15 teams)...
Tide   1 | fired: roster_drill             | skipped:  1
Tide   2 | fired: boards                   | skipped:  1
Tide   3 | fired: -                        | skipped:  2
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

Wrote 580 roster row(s) to examples/06-state-sports/out/state_sports_roster.ndjson

Going further: cross-sport tallest/heaviest splits and calc_all() dense-rank
leaderboards both live in the README.
```

`state_sports.py ON` finds 4 teams (Toronto Raptors, Toronto Blue Jays, Ottawa
Senators, Toronto Maple Leafs — the Blue Jays prove the *fetched* Canada map covers
the full name `"Ontario"`); `state_sports.py "California"` normalizes the full name
to `"CA"` through the same fetched map and produces the identical result above.

**Two boards run across all four leagues on purpose.** Salary coverage in this feed
is NFL/NBA only (0/131 for MLB, 0/100 for NHL, verified live) — a salary-only
leaderboard would silently erase half the sports this tutorial fetches. The veterans
board (tenure) and the homegrown board don't have that gap, so every league gets a
fair shot at the top of those two.

**If the reference API is unreachable**, the run stops immediately with one line —
`ERROR: reference API unreachable - cannot normalize state names.` — and a non-zero
exit, before any ESPN request is made.

### Reading the structured reject list

Team-list and roster drills both come back as `IncorporatorList` instances carrying
`.rejects` (structured `RejectEntry` records: source URI, error class, parsed
`Retry-After`, wave index) alongside the legacy `.failed_sources` string view. This
script prints both: an unreachable league's team list is reported and skipped before
any detail or roster drill fires against it, and a roster drill that fails for one
team doesn't sink the others running concurrently inside the same `roster_drill`
tick.

```python
for entry in teams.rejects:
    print(f"   - {entry}")
```

---

## Going further

* **Cross-sport physical extremes.** The same active-player pool that feeds the
  veterans board also makes for a fun tallest/heaviest split — NBA centers run
  ~7'2", NFL linemen top 350 lbs. Sort the roster rows by `height` or `weight` and
  print the extremes per league.
* **`calc_all()` dense-rank.** `calc_all(func, *keys, ...)` computes a rank *within
  one `incorp()` call* — handy for a per-team salary rank, but the state-wide
  leaderboards in this script are cross-team, so they use a plain `sorted()`
  instead. See `docs/api_atlas.md` for `calc_all`'s window-aggregation shape.
* **A recurring state-sports refresh** (salaries update, rosters change) would
  widen this tutorial's single-pass window into a genuinely long-running one — a
  [Tutorial 8](../08-streaming-daemon/README.md) / [Tutorial 10](../10-multi-source-fjord/README.md)
  -shaped follow-up, and the same 2-current `Watershed` shape here would carry over
  unchanged with a longer window and shorter intervals.

---

## Where to Go Next

> **Up next: [Tutorial 7 — Stateful Refresh](../07-stateful-refresh/README.md).**
> T6 introduced its first Tideweaver `Watershed` (a single-pass, 2-current chain); T7
> takes a single live registry and keeps it fresh with `refresh()`, three different
> ways — no Watershed yet, but the same "keep data current" problem this tutorial's
> Phase 2 first touched.

| Goal | Read |
|---|---|
| See the CoinGecko-spine version of parent-child drilling | [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md) |
| Keep a registry live with `refresh()` | [Tutorial 7 — Stateful Refresh](../07-stateful-refresh/README.md) |
| See the full streaming-daemon coverage | [Tutorial 8 — Streaming Daemons](../08-streaming-daemon/README.md) |
| Fuse multiple live sources into one derived metric | [Tutorial 10 — Multi-Source Fjord](../10-multi-source-fjord/README.md) |
| See the full Tideweaver vocabulary (diamonds, penstocks, spillways) | [Tutorial 11 — Tideweaver](../11-tideweaver/README.md) |
| See another non-crypto domain in the curriculum | [Appendix — MLB Pulse](../appendix/mlb-pulse/README.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/06-state-sports/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
