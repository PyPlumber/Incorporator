***

# Tutorial 6 — State Sports: Two Drills, Then a Third In-Memory `incorp()`

**Prerequisites:** [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md).

Pick a US state (or Canadian province) code. Discover every team whose venue sits
there across the NFL, NBA, MLB, and NHL. Drill every roster. Rank the players by
salary and by tenure. Find the ones who made it home.

T5 introduced `inc_parent` / `inc_child` fan-out on CoinGecko — a top-N market list
drilling into per-coin detail. This tutorial reruns that exact shape on ESPN's public
site API **twice, chained**: league discovery drills into team detail (Drill 1), then
the region-filtered team list drills into roster detail (Drill 2). A **third**
`incorp()` call closes the tutorial out: `Player.incorp(payload_list=...)` builds one
row per active player straight out of memory — no URL, no file, the exact same build
pipeline (`conv_dict`, PK-binding, schema inference) with zero network calls. A pure
one-shot script — no Watershed, no files read or written at runtime, no
`CustomCurrent`s, and `main()` is fully inline (pokéapi-style: read top-to-bottom in
dependency order, no phase-function decomposition). `link_to`, `calc`, and `pluck`
all still make an appearance across the three calls. No auth, no API key, ~145 HTTP
requests total, ~15s wall-clock.

```bash
python examples/06-state-sports/state_sports.py               # defaults to "CA"
python examples/06-state-sports/state_sports.py ON
python examples/06-state-sports/state_sports.py "California"
```

---

## Two drills, then a third in-memory call, one script

Unlike a Watershed (a fixed graph of nodes wired at construction time), this
tutorial's shape is three ordinary `await`ed `incorp()` calls, threaded through a
plain Python filter and a plain Python hand-off comprehension:

| Step | What runs | Shape |
|---|---|---|
| **1. Discover** | Fetch the state/province reference map, list every league's teams, drill venue detail | Drill 1: T5's whole-list `inc_parent`/`inc_child` fan-out |
| **Filter** | Keep only teams whose venue sits in `region` | Plain Python comprehension — no server-side filter exists |
| **2. Roster** | Drill every matched team's roster, join it back to Drill 1's team via `link_to` | Drill 2: the *same* T5 shape, reused a second time |
| **Hand-off** | Flatten active players out of each roster's own `athletes` array, stamping `league`/`team_name` | Plain Python comprehension over already-built rows |
| **3. Player rows** | Build one row per active player, with build-time defaults for every field | `Player.incorp(payload_list=...)` — a network-free in-memory passthrough |

The two drills are the identical primitive — `cls.incorp(inc_parent=..., inc_child=...)`
— applied to two different verticals of the same domain. The third call is a
*different* primitive doing the same job the pokéapi appendix's `calc()` array
reducer used to do by hand: build a nested array into its own first-class rows. No
Watershed is needed here because nothing in this script requires a *time window*; it
runs once and exits. (T11 is this curriculum's Watershed capstone.)

---

## What's new here (beyond T5)

| T5 gave you | T6 adds |
|---|---|
| `inc_parent` / `inc_child` fan-out from a *list* of parents | The exact same whole-list fan-out, **reused twice** — league → team, then team → roster |
| Flat parent rows | A deep `rec_path` envelope (`sports.0`, each team row wrapped in `{"team": {...}}`) |
| `inc_code="id"` | Top-level `inc_code="uid"` after `rec_path="team"` digs into the envelope (both drills) — and the reason it can't be the numeric `id` |
| `pluck()` for a nested lift | `pluck(key, chain=fn)` for a nested lift **plus build-time normalization** of an inconsistent source attribute — backed by a **live reference-data fetch**, not a hardcoded table |
| One vertical (CoinGecko) | Four leagues fanned out concurrently, each with its own coverage gaps |
| A single `incorp()` call per node | A **third** `incorp()` call that reads rows already sitting in memory — `Player.incorp(payload_list=...)`, zero network |
| — | A list-valued `inc_child` leaf (`team_paths`) that needs a second dotted segment to fan out correctly — see "The BFS-flatten gotcha" below |

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
state_code_map: dict[str, str] = dict(DC_SUPPLEMENT, **{s.inc_name: s.inc_code for s in states})
if "California" not in state_code_map or "Ontario" not in state_code_map:
    sys.exit(REFERENCE_API_ERROR)
```

One `incorp()` call — `inc_url` accepts `str | list[str]`, so both countries fan out
under a single `IncorporatorList` — builds the full 50-state-plus-13-province map at
**runtime**, using the same primitive every other fetch in this tutorial uses.
There's no special "reference data" mechanism; it's just another source. This whole
block, along with everything else in the script, lives directly inline in `main()` —
no phase functions.

**A single multi-URL call needs a PARTIAL-failure check, not just an empty-list
check.** If one country's request fails and the other succeeds, `state_code_map` is
still non-empty — checking for a representative entry from *both* countries
(`"California"`, `"Ontario"`) catches a partial failure the same way an empty-map
check catches a total one.

**A live reference API still needs a hygiene check.** CountriesNow's US-states feed
has no District of Columbia entry (verified live 2026-07-08) — but the MLB Nationals'
venue record reports `"District of Columbia"`. A one-entry supplement closes that one
gap:

```python
DC_SUPPLEMENT = {"District of Columbia": "DC"}
```

Even a live, structured, purpose-built reference dataset can have a hole — the fix is
the same size as the hole (one entry), not a reason to distrust the whole source.

**Fail fast, not silently, and to stderr.** If either CountriesNow call comes back
empty (network down, API changed shape), a silent empty map would produce a state
filter that matches nothing, with no explanation why. `sys.exit(str)` prints that
string to stderr and exits 1 — but only when it propagates all the way to the real
interpreter top level; inside a test's `pytest.raises(SystemExit)` nothing is written
to stderr by the exception itself, so tests assert on `exc_info.value.code` instead.
One clear ASCII line, one non-zero exit, no constant fallback, no partial map.

---

## Drill 1a: league discovery, one `calc()` array reduction

ESPN's `/{sport}/{league}/teams` endpoint returns a nested envelope: one `sports[0]`
row per league, holding a `leagues[0].teams` array of `{"team": {...}}` rows.
`League.incorp()` fetches all four leagues in one multi-URL call and reduces each
league's own team array into a list of drillable paths:

```python
def build_team_paths(sport_slug: str, leagues_array: list[dict]) -> list[dict[str, str]]:
    league = leagues_array[0]
    return [{"path": f"{sport_slug}/{league['slug']}/teams/{t['team']['id']}"} for t in league["teams"]]


leagues = await League.incorp(
    inc_url=[
        "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams",
        "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams",
        "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams",
        "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams",
    ],
    rec_path="sports.0",
    conv_dict={"team_paths": calc(build_team_paths, "slug", "leagues", default=[])},
    timeout=8,
)
```

This is the exact same `calc()` array-reduction idiom
[the PokéAPI ETL appendix](../appendix/pokeapi-etl/README.md) uses for Base Stat
Total — a nested JSON array reduced into a simpler shape via a named, module-level
function. Note there's no `isinstance()` ladder here: a malformed `leagues_array` (a
missing `leagues[0]`, a non-list `teams`) raises, and `calc()`'s own exception
fallback resolves the WHOLE league's `team_paths` to `default=[]` — no per-team
salvage on a partially-malformed league. Acceptable for a tutorial; a production
pipeline ingesting an untrusted feed might want the isinstance-guarded version
instead.

### The BFS-flatten gotcha: why `team_paths` is `list[dict]`, not `list[str]`

The natural first instinct is `team_paths: list[str]` (bare path strings). That
breaks silently. `extract_parent_data` — the function that drills `inc_child` paths
across a whole parent list — only fans a list out across items when a path segment
lands on a list *reached through a prior segment*. A list-valued leaf read directly
off a **non-list** parent node (a single `League` row) doesn't trigger that fanout —
it appends the *whole list* as one element, giving you a **list of lists**
(`[['a1', 'a2'], ['b1']]`) instead of a flat list of paths. `{}`-template URL-building
would then silently stringify each inner list into garbage.

The fix: wrap each path in a single-key dict (`{"path": "..."}`) and give the drill a
**second** dotted segment: `inc_child="team_paths.path"`. The second segment's
intermediate value — a list of per-league path-lists — genuinely *is* a list reached
via the "team_paths" segment, so the second segment's fanout branch fires correctly
and flattens everything in one pass:

```python
teams = await Team.incorp(
    inc_parent=leagues,
    inc_child="team_paths.path",
    inc_url="https://site.api.espn.com/apis/site/v2/sports/{}",
    rec_path="team",
    inc_code="uid",
    inc_name="displayName",
    conv_dict={...},
    timeout=8,
)
```

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

## Drill 1b: the venue drill, T5's whole-list `inc_parent`

```python
teams = await Team.incorp(
    inc_parent=leagues,
    inc_child="team_paths.path",
    inc_url="https://site.api.espn.com/apis/site/v2/sports/{}",
    rec_path="team",
    inc_code="uid",            # top-level post-rec_path -- same string as League's "team.uid"
    inc_name="displayName",
    conv_dict={
        "venue_city": pluck("franchise.venue.address.city"),
        "venue_state": pluck("franchise.venue.address.state", chain=to_code),
        "league": calc(league_from_links, "links", default=None, target_type=str),
        "roster_path": calc(build_roster_path, "league", "id", default="", target_type=str),
    },
    timeout=8,
)
```

`inc_parent=leagues` is a whole `IncorporatorList` here, not a single instance — this
is exactly T5's `CoinDetail.incorp(inc_parent=coins, inc_child="id", ...)` shape
([`examples/05-parent-child-drilling/parent_child_drilling.py`](../05-parent-child-drilling/parent_child_drilling.py)),
reused verbatim: the framework dedups every extracted path, fans the detail requests
out concurrently across all four leagues in ONE call, and hands back one `Team` per
team.

**`rec_path="team"` means every `conv_dict` path drills into *that team's own*
sub-object.** `"franchise.venue.address.city"` is relative to the post-`rec_path`
row — it does not need (and must not have) a `"team."` prefix, even though the raw
response is itself wrapped in `{"team": {...}}`. `rec_path="team"` also leaves `id`
sitting at the top level, unclaimed — `build_roster_path`'s second input key reads it
directly, no join back to `League` required.

### Deriving `league` from the row's own data — no lookup table

Every team detail (and roster) row's own `links` array embeds the league slug:

```python
def league_from_links(links: list[dict[str, str]]) -> str:
    return links[0]["href"].split("/")[3].upper()
```

`https://www.espn.com/nba/team/_/name/lac/la-clippers` → `"NBA"`. Verified live across
all four leagues (2026-07-08) — the slug segment index is stable. No `isinstance()`
ladder here either — malformed `links` raises, and `calc()`'s exception fallback
resolves to `default=None`.

### `roster_path`: reading THIS row's own already-mutated `league`

```python
LEAGUE_SPORT_SLUGS = {"NFL": "football", "NBA": "basketball", "MLB": "baseball", "NHL": "hockey"}


def build_roster_path(league: str, team_id: str) -> str:
    return f"{LEAGUE_SPORT_SLUGS.get(league, '')}/{str(league).lower()}/teams/{team_id}?enable=roster"
```

`conv_dict` entries run in insertion order, and each one sees the row **as already
mutated by earlier entries in the same dict** — `"roster_path"` is declared *after*
`"league"`, so `build_roster_path`'s first argument is the freshly-derived league
label, not the raw response's missing field. Confirmed live: `NFL` →
`football/nfl/teams/13?enable=roster`.

**Why `LEAGUE_SPORT_SLUGS` is honest plumbing, not a reintroduction of brand
strings.** `conv_dict` only ever sees the fetched *response*, never the *request*
that produced it — there's no way to recover "this row came from the `/football/nfl/`
path segment" from the row's own JSON. `LEAGUE_SPORT_SLUGS` is a small, closed,
four-entry map of ESPN's own fixed URL scheme (which of 4 path segments to hit next)
— categorically different from the deleted brand-string tables, which were about team
*identity/location* labels, not URL routing.

**Why `inc_code="uid"` and not `id` — and why it isn't dotted.** Both drills set
`rec_path="team"`, so every row the converters see has already been drilled inside
the `{"team": {...}}` envelope — `uid` sits at the TOP LEVEL of that row, no dotted
path needed. (`League.incorp()` sets no `inc_code` at all: nothing downstream ever
reads a league primary key, only its `team_paths` field.) ESPN's numeric `team.id`
is only unique *within* a league — pooling four leagues' worth of teams under one
class with `id` as the primary key would let one league's registration silently
overwrite another's; `uid` (`"s:20~l:28~t:24"`) bakes the sport and league into the
string, so it's globally unique across every league fetched in this run.

---

## Data hygiene: `pluck(key, chain=fn)`, bound to a fetched map

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

`to_state_code` takes the mapping as an explicit argument rather than reading a
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

**Stale venues, right state.** A few of ESPN's venue records are out of date — the
Warriors' `franchise.venue` is still Oracle Arena in Oakland (they've played at
Chase Center in San Francisco since 2019); the Chargers' venue is Dignity Health
Sports Park in Carson (they've played at SoFi Stadium in Inglewood since 2020). Both
are "wrong building, right state" — the `state` field itself is accurate even where
the venue name/city drifted, and this filter only ever reads `state`.

---

## The filter: attribute equality, zero brand strings

```python
matched = [t for t in teams if t.venue_state == region]
```

There's no `state=` query parameter on ESPN's detail endpoint, and no bulk "every
team whose venue is in state X" endpoint exists at all — the filter genuinely can't
be pushed server-side, so an app-level comprehension over the already-built
`IncorporatorList[Team]` is the correct (and only) option here, not a framework
primitive.

A handful of teams have no reachable venue address at all (one NBA team in the
sample run, no `franchise` key present in the detail response) — those are excluded
and counted, not treated as errors; the script prints a single summary WARN line
rather than one per team.

`matched` stays a strong local reference for Drill 2's entire `TeamRoster.incorp()`
call below — `link_to(matched)` builds its registry off `matched`'s own `inc_dict`,
and `inc_dict` is a `WeakValueDictionary`.

---

## Drill 2: the roster drill, now a lean 3-entry `conv_dict`

Drill 2 reuses T5's whole-list `inc_parent` shape a second time — over `matched`
instead of the full league lists. Its `conv_dict` used to carry eight entries
(a build-time join, four array-reduction reducers, a rename, and two more lifts).
Every one of those reducers is gone now — they've moved to a **third `incorp()`
call** below — so what's left is just enough to stamp each roster row with its
parent team's identity:

```python
rosters = await TeamRoster.incorp(
    inc_parent=matched,
    inc_child="roster_path",
    inc_url="https://site.api.espn.com/apis/site/v2/sports/{}",
    rec_path="team",
    inc_code="uid",
    inc_name="displayName",
    conv_dict={
        "team_ref": calc(link_to(matched), "uid"),
        "league": calc(operator.attrgetter("league"), "team_ref", default=None, target_type=str),
        "team_name": pluck("displayName"),
    },
    excl_lst=["record", "logos", "nextEvent", "standingSummary"],
    timeout=10,
)
```

### `link_to()`: the build-time join back to Drill 1

```python
"team_ref": calc(link_to(matched), "uid"),
```

`TeamRoster` and `Team` share the same underlying ESPN team identifier (`uid`).
`link_to(matched)` builds an O(1) registry keyed on `matched`'s own `inc_code`, and
resolves each roster row's raw `"uid"` field straight to the matching `Team`
instance — no re-fetch, no re-derivation. It's wrapped in `calc()` (rather than used
bare) so the **output** key (`"team_ref"`) can differ from the join's **source** key
(`"uid"`) — `calc()` reads its input via `DataPath`, not `d.get(output_key)`, so the
raw `"uid"` field PK-binding (`inc_code="uid"`) still needs stays completely
untouched. This is the exact pattern
[`examples/appendix/crypto-graph-mapping/crypto_graph_mapping.py`](../appendix/crypto-graph-mapping/crypto_graph_mapping.py)
uses for its four Binance sub-market joins.

Immediately after, `league` is read **through** that linked instance via
`operator.attrgetter` rather than a named one-line helper — `team_ref` was computed
one entry earlier in the same `conv_dict`, so `attrgetter("league")` reads its value
directly, insertion order guaranteeing it's already a real `Team` instance by the
time this entry runs. `"team_name"` is a genuinely fresh nested-free lift —
`pluck("displayName")` — needed because the hand-off below stamps it onto every
player row.

### What happened to `athletes`?

Nothing — and that's the point. `"athletes"` isn't touched by `conv_dict` at all
anymore. The schema factory's dynamic-schema inference runs on **every** nested
field, `conv_dict`-computed or raw, so `roster.athletes[0].contract.salary` is
already available as attribute access with zero conv_dict involvement. A bare
self-pluck (`"athletes": pluck("athletes")`) would be a documented no-op that adds a
line without adding behavior, so it's omitted entirely — see "Build rows from
memory" below for where those athlete rows actually get built into something.

---

## Build rows from memory: the third `incorp()` call

The four reducers that used to live inside `TeamRoster`'s `conv_dict` — flattening
`athletes` into active-only player dicts, summing payroll, counting salary coverage
— all did the same job by hand that
[`docs/api_atlas.md`'s "Build rows from memory" recipe](../../docs/api_atlas.md#build-rows-from-memory--the-payload-only-passthrough)
does declaratively: **if a `calc()` helper is walking a nested array and emitting a
list of dicts with derived per-element fields, that data wants to be its own class.**

The hand-off is a plain Python comprehension — active-only filter, parent stamp, and
flattening each re-inferred `athletes` sub-model back to a dict, all in one pass:

```python
roster_payload = [
    {**athlete.model_dump(), "league": team.league, "team_name": team.team_name}
    for team in rosters
    for athlete in team.athletes
    if athlete.active  # MLB's `athletes` array is the whole organization, not the active roster
]
```

`rosters` must stay a strong local reference until this comprehension finishes
reading `team.athletes` off every row — the same lifetime rule `matched` needed for
`link_to(matched)` above, since it's consumed synchronously in the same `main()`
body this is trivially true, but worth calling out on the pattern's first
appearance in this curriculum.

Then `Player.incorp(payload_list=roster_payload)` builds one row per active player —
no URL, no file, the exact same build pipeline everything else in this tutorial goes
through:

```python
players = await Player.incorp(
    payload_list=roster_payload,
    inc_code="uid",  # globally unique across leagues (verified live) -- no league-qualifying calc needed
    inc_name="fullName",
    conv_dict={
        "salary": calc(float, "contract.salary", default=0.0, target_type=float),
        "tenure": calc(int, "experience.years", default=0, target_type=int),
        "pos": calc(str, "position.abbreviation", default="-", target_type=str),
        "birth_city": calc(str, "birthPlace.city", default="-", target_type=str),
        "birth_state": calc(str, "birthPlace.state", default="-", target_type=str),
        "salary_per_year": calc(
            salary_per_year, "contract.salary", "experience.years", default="-", target_type=str
        ),
        "turned_pro_at": calc(turned_pro_at, "age", "experience.years", default="-", target_type=str),
    },
)
```

### Why `calc(TYPE, "nested.path", default=..., target_type=TYPE)`, not `pluck()`

`pluck()` is the framework's nested-extraction primitive, but it has **no `default=`
parameter** — a missing `contract.salary` resolves to raw `None`, not a build-time
default. `inc(TYPE, default=...)` can't fill that gap either, since it reads
`d.get(key)` directly and can't drill a dotted path like `"contract.salary"`.
`calc(TYPE, "nested.path", default=..., target_type=TYPE)` — passing a bare type as
`calc()`'s callable — is the idiom that closes it: the exact same pattern
[`examples/11-tideweaver/arb_scanner.py`](../11-tideweaver/arb_scanner.py) uses for
`calc(float, "bidPrice", default=0.0, target_type=float)`.

### Preformatted display strings, not raw numbers

```python
def salary_per_year(salary: float | None, tenure: int) -> str:
    if salary is None:
        return "-"
    return f"${salary / (tenure or 1):,.0f}"


def turned_pro_at(age: int | None, tenure: int) -> str:
    if age is None:
        return "-"
    return str(age - (tenure or 0))
```

Both fields are display-only — never sorted or aggregated — so they're built as
pre-formatted strings (`"$1,269,877"` / `"20"`, `"-"` on missing) rather than numbers
with a print-time ternary. Note the explicit `None` guard inside each function,
rather than leaning on `calc()`'s own exception-fallback: `tenure` defaults to `0`
upstream (a real, non-garbage value), so `calc()`'s all-inputs-garbage short-circuit
never fires when only `salary` (or `age`) is missing — and that's the *common* case
for MLB/NHL, which publish no salaries at all. Skipping the guard and letting the
division raise would log a warning on every one of those rows (confirmed against a
live run) — one line of defensive code here is cheaper than a noisy log for the
expected case.

**MLB's org-list quirk.** `roster.athletes` for MLB is the *entire organization*
(~250 players including minor-leaguers), not the 26-man active roster — the
`if athlete.active` filter in the hand-off comprehension above keeps the active-only
rule consistent. NFL/NBA/NHL are all-active already, so the filter is a no-op there.

---

## The boards: flatten, sort, format — zero derivation

```python
players = await Player.incorp(payload_list=roster_payload, ...)
```

`players` — the `IncorporatorList` the third `incorp()` call returns — *is* the flat
player pool; there's no `[p for team in rosters for p in team.players]` step anymore,
because the flattening already happened in the hand-off comprehension above.

Every board below reads fields that were already computed inside `conv_dict` —
`p.salary`, `p.tenure`, `p.league`, `p.team_name`, `p.birth_state`. No `isinstance()`
checks, no `None`-guard ladders, no per-row derivation, and — the gate this revision
is built against — **zero missing-data conditionals**: every printed field carries a
build-time `calc(..., default=...)`, numeric where sorting/aggregation needs numbers
(`tenure`/`salary` default to `0`), a preformatted `"-"` string where the field is
display-only. The league-summary board reads `p.salary`/`p.league`/`p.team_name`
straight off each `Player` row, grouped by `league`, with plain `sum()`/`len()`
comprehensions — no roster-level aggregates to join back to.

**Two boards run across all four leagues on purpose.** Salary coverage in this feed
is NFL/NBA only (0/131 for MLB, 0/100 for NHL, verified live) — a salary-only
leaderboard would silently erase half the sports this tutorial fetches. The veterans
board (tenure) and the homegrown board don't have that gap, so every league gets a
fair shot at the top of those two.

### The homegrown board: pure attribute equality

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
OK: Loaded 580 active players across 15 teams.

CA across NFL / NBA / MLB / NHL
======================================================================
NFL   3 team(s), 272 active players, salary known 145/272, payroll $789,114,970
NBA   4 team(s), 77 active players, salary known 52/77, payroll $664,147,833
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
6    Domantas Sabonis        NBA  Sacramento Kings      F          9   $43,636,000    $4,848,444
7    Matthew Stafford        NFL  Los Angeles Rams      QB        18   $40,000,000    $2,222,222
8    Darius Garland          NBA  LA Clippers           G          6   $39,446,090    $6,574,348
9    Brandon Ingram          NBA  LA Clippers           F          9   $38,095,238    $4,232,804
10   Trent Williams          NFL  San Francisco 49ers   OT        17   $33,060,000    $1,944,706

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

Going further: cross-sport tallest/heaviest splits and calc_all() dense-rank
leaderboards both live in the README.
```

(Regenerated live 2026-07-09 against this revision — boards are materially identical
to the pre-refactor output; the salary-known counts shift slightly run to run as
ESPN's roster feed updates.)

`state_sports.py ON` finds 4 teams (Toronto Raptors, Toronto Blue Jays, Ottawa
Senators, Toronto Maple Leafs — the Blue Jays prove the *fetched* Canada map covers
the full name `"Ontario"`); `state_sports.py "California"` normalizes the full name
to `"CA"` through the same fetched map and produces the identical result above.

**If the reference API is unreachable**, the run stops immediately with one line —
`ERROR: reference API unreachable - cannot normalize state names.` — and a non-zero
exit, before any ESPN request is made.

**No files are read or written at runtime.** Every board above reads the `Team` /
`TeamRoster` / `Player` instances `incorp()` returns directly, in-process —
`examples/06-state-sports/` is byte-identical before and after any run.

### The structured reject list, still there if you need it

Every `incorp()` call in this tutorial comes back as an `IncorporatorList`, carrying
`.rejects` (structured `RejectEntry` records: source URI, error class, parsed
`Retry-After`, wave index) — an unreachable league's team list, a failed team-detail
drill, or a failed roster drill would each land here. This script doesn't print them
proactively anymore (the framework already surfaces failures; a per-call
`if X.rejects: print(...)` loop after every fetch was pure ceremony this revision
deleted), but the data is one attribute access away for anyone who wants it:

```python
teams = await Team.incorp(...)
if teams.rejects:
    for entry in teams.rejects:
        print(entry)
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
* **A recurring state-sports refresh** (salaries update, rosters change) is a
  [Tutorial 8](../08-streaming-daemon/README.md) /
  [Tutorial 10](../10-multi-source-fjord/README.md) -shaped follow-up, or — if you
  want a genuinely windowed, scheduled version of these same two drills — a
  [Tutorial 11](../11-tideweaver/README.md)-shaped `Watershed`.

---

## Where to Go Next

> **Up next: [Tutorial 7 — Stateful Refresh](../07-stateful-refresh/README.md).**
> T6 chained two whole-list `inc_parent` drills, then handed the result off to a
> third, network-free `incorp(payload_list=...)` call — `link_to`, `calc`, and
> `pluck` all make an appearance along the way; T7 takes a single live registry and
> keeps it fresh with `refresh()`, three different ways.

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
