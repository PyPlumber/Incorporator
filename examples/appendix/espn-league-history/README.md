***

> 📎 **Appendix — Multi-season one-shot fjord ETL.** Fetches every reachable
> season of an ESPN Fantasy Football league exactly once and fuses
> standings, matchups, members, and drafts into six all-time analytical
> views via a one-shot `Incorporator.fjord()` (no Watershed -- see
> [Section 5](#5-why-a-one-shot-fjord-not-a-watershed) for why). If you're
> new to the `payload_list=` in-memory passthrough, read
> [Tutorial 5 -- Parent-Child Drilling](../../05-parent-child-drilling/README.md)
> first; if you're new to `fjord()` itself, read
> [Tutorial 9 -- NASCAR Fantasy Fjord](../../09-nascar-fantasy-fjord/README.md)
> first.

***

# ESPN Fantasy Football League History: Six-View Franchise Almanac

A research walkthrough over an entire league's history: standings, matchups,
members, drafts, and settings across every reachable season, fused into
per-franchise all-time rollups, a season-by-season timeline, a rivalry
matrix, a ten-kind records book, three-kind draft tendencies, and a
settings-evolution log -- six NDJSON files plus a console report.

```
League 899513 -- public (no cookies)
Discovering reachable seasons via status.previousSeasons ...

Fetched 7 season(s): [2020, 2021, 2022, 2023, 2024, 2025, 2026]
  unresolved seasons (no data available): [2018, 2019]
Running one-shot fjord: Owner/Standing/Matchup/DraftPick network-free, PlayerName fanned out ...

Wrote 6 views to .../out:
  franchise_cards.ndjson    13 rows
  season_timeline.ndjson    72 rows
  rivalry_matrix.ndjson     75 rows
  records_book.ndjson       10 rows
  draft_tendencies.ndjson   55 rows
  settings_evolution.ndjson 7 rows

FRANCHISE CARDS (all-time, sorted by win%)
FRANCHISE               W-L-T          WIN%  SEASONS  TITLES  PLAYOFF%
----------------------------------------------------------------------
longhorn0010            53-29-0       0.646        7       1     71.4%
...
```

(`draft_tendencies.ndjson`'s row count drifts slightly season to season --
2026's draft was still in progress at last run, so its `first_overall` pick
resolved to `Unknown`/`UNKNOWN` rather than a real name; this is expected,
not a bug.)

## 1. Two auth modes, one pipeline

| Env var | Required? | Effect |
|---|---|---|
| `ESPN_LEAGUE_ID` | No -- defaults to `899513` | The league to fetch. `899513` is a third-party public demo league; swap in your own league's numeric ID (visible in the ESPN Fantasy web app URL: `.../football/league?leagueId=<ID>`). |
| `ESPN_S2` | No | Browser cookie value. Unlocks a private league and the cookie-gated `leagueHistory` endpoint (seasons ESPN's `status.previousSeasons` reports but that 401 through the modern endpoint without cookies). |
| `ESPN_SWID` | No | Browser cookie value, paired with `ESPN_S2`. Both must be set together -- setting only one leaves the pipeline in public (no-cookie) mode. |

**Extracting `ESPN_S2` / `ESPN_SWID` from your browser** (Chrome/Edge DevTools):
1. Log into [fantasy.espn.com](https://fantasy.espn.com) and open your league.
2. Open DevTools (F12) -> **Application** tab -> **Cookies** -> `https://fantasy.espn.com`.
3. Copy the `espn_s2` value (long alphanumeric string) and the `SWID` value (a brace-wrapped GUID, e.g. `{ABCD1234-...}`) into `ESPN_S2` / `ESPN_SWID`.

The demo league `899513`'s own unauthenticated floor is season 2020 -- seasons
2018/2019 exist (ESPN's `status.previousSeasons` reports them) but 401
through the modern endpoint without cookies, so the public run leaves them
out of the final season list (printed once, as a set difference, at the end
of discovery -- never per-item). This is a fact about the demo league's own
history, not a hardcoded floor in the code -- **season discovery is entirely
server-declared** (Section 4).

## 2. File layout

```
examples/appendix/espn-league-history/
  espn_league_history.py   # season discovery (plain Python) + the fjord() call + the console report
  outflow.py                # 6 source classes + 6 bare view classes + outflow(state) -- the
                             # one declared field in the whole file is Season.roster_slots
                             # (Section 6), a source-side concern, not a view-class one
  README.md                # this file
  out/                      # gitignored -- six NDJSON views land here
```

No `watershed.json` -- see Section 5 for why this appendix is a one-shot
`Incorporator.fjord()` rather than a Watershed. Unlike a Watershed example,
where every `Incorporator` subclass lives in the main entry file and a
sidecar is a pure re-import store, a `cls.fjord()` daemon example (this one)
defines ALL its classes in `outflow.py` -- both the six source classes AND
the six derived view classes the fjord builds (see
`examples/09-nascar-fantasy-fjord/`, the template this rewrite follows).

`espn_league_history.py`'s own console report reads three of those view
classes' `inc_dict` back AFTER the fjord loop ends (Section 5), so the entry
file loads `outflow.py` via one explicit
`incorporator.usercode.load_outflow_module(...)` call rather than a bare
`from outflow import (...)` -- a bare import registers a second,
disconnected copy of the file under Python's own `sys.modules` cache,
distinct from the path-keyed cache `fjord()`'s internal loader uses to
build and park the view classes' rows, so a bare-imported class's
`inc_dict` would come back silently empty post-loop.
`load_outflow_module` shares that same path-keyed cache, so calling it
explicitly here, once, before `fjord()` runs, guarantees both loads
resolve to the identical module and class objects.

## 3. The six views

| View | File | Shape |
|---|---|---|
| 1. Franchise Cards | `franchise_cards.ndjson` | One row per owner, all-time: W-L-T, win%, PF/PA, seasons played, average finish, championships/runner-ups/last-places, playoff appearances + rate. |
| 2. Season Timeline | `season_timeline.ndjson` | One row per franchise-season: seed -> final rank, record, PF/PA, division, all-play expected wins, luck delta. |
| 3. Rivalry Matrix | `rivalry_matrix.ndjson` | One row per franchise pair, all-time: meetings, W-L, playoff meetings, biggest blowout, closest game. |
| 4. Records Book | `records_book.ndjson` | Ten all-time record kinds (Section 3a). |
| 5. Draft Tendencies | `draft_tendencies.ndjson` | Three kinds: round-1 position mix per franchise, all-time most-drafted players (top 15), first-overall honor roll. |
| 6. Settings Evolution | `settings_evolution.ndjson` | One row per season: league size, playoff format, PPR adoption, roster slots, division eras. |

### 3a. The ten records-book kinds

`highest_single_week_score`, `lowest_single_week_score`,
`largest_margin_of_victory`, `narrowest_margin_of_victory`,
`best_season_record`, `worst_season_record`, `highest_season_points_for`,
`lowest_season_points_for`, `longest_win_streak`, `longest_loss_streak`.

Every kind filters to **decided** matchups first (`winner != "UNDECIDED"`):
a playoff bye (`home`-only, no `away` key, permanently `UNDECIDED`) and an
in-progress week (both sides `0.0`, `UNDECIDED`) never register. A genuine
0-point week and a tiebreak-decided tie (`winner` still resolves to a side;
margin can legitimately be `0.0`) both DO register -- that distinction is
the whole reason the filter is on `winner`, not on a `totalPoints > 0`
check. Win/loss streaks are franchise-history streaks, chronological across
every fetched season (`(season, matchupPeriodId)` order), not reset at a
season boundary.

`RecordRow` is bare, like all six of `outflow.py`'s derived view classes
(Section 2). All ten kinds share one `value` column, but the column's
native type differs by kind: point/margin/ratio floats for seven kinds,
plain win/loss-streak game counts (ints) for the other two. Since
`outflow(state)` appends the float-valued kinds first and the two streak
kinds last, fjord's own dynamic-schema inference locks `value`'s type from
the first-sampled (float) row and coerces the trailing int streak values to
match -- `longest_win_streak` renders as `10.0`, not `10`, in the exported
NDJSON. This is expected, not a bug: fixing it would mean pre-declaring
`RecordRow` again, exactly the machinery this class no longer needs.

## 4. Season discovery is exactly two `Season.incorp` calls

No brute-force floor/ceiling guessing, and no per-year fan-out machinery.
Season discovery is exactly two `Season.incorp` call sites, both shared
identically across auth modes -- mode differences are argument ternaries on
a single fan-out call, never duplicated call sites:

1. **Bootstrap** -- one call for the in-progress current-year season
   (the modern endpoint). Reads `status.previousSeasons` off the response;
   that's the candidate-years list, load-bearing for the public branch's
   URL construction only (harmless, unused, in private mode).
2. **History/fan-out** -- one call, one statement, mode decided ONCE up
   front from whether cookies are present, no probe, no retry:
   - **With cookies**, ONE `Season.incorp(inc_url=<leagueHistory URL>)`
     call against the cookie-gated `leagueHistory/{league_id}` endpoint
     with NO `seasonId` query param -- its top-level JSON response IS the
     list of every OTHER completed season (excluding the in-progress
     current year already covered by the bootstrap), so `incorp()` treats
     the array itself as the record set with no `rec_path` drilling at
     all.
   - **With no cookies**, `leagueHistory` 401s uncookied, so `remaining_years`
     (from the bootstrap's `previousSeasons`) fans out against the modern
     per-season endpoint instead -- `Season.incorp(inc_url=[...])`, one URL
     per year. A modern-endpoint miss on an old season is terminal: ESPN
     gives back the successful years and each failed year's URL lands in
     `.failed_sources`, which the pipeline reports (via a plain
     set-difference against the candidate list) but never retries.

League size is read from each season's own `len(season.teams)` -- never
hardcoded, since a league's team count can and does drift across seasons.

A fetch failure surfaces through `IncorporatorList.rejects` /
`.failed_sources` (structured `RejectEntry` records with `.status_code`)
plus the framework's own WARNING-level log line -- never a raised exception,
and never suppressed.

## 5. Why a one-shot fjord, not a Watershed

Every season is fetched exactly once, ever -- there is no polling axis. A
Watershed's entire value proposition is repeated ticks against a moving
window, and nothing here moves: this is static historical data. But six
cardinality-reducing views fused from six fjord sources IS exactly what
`Incorporator.fjord()` is for, without needing a Watershed's tick/wave
scheduler at all: give every `stream_params` entry `refresh_params=None`
and omit the top-level `export_interval`, and `fjord()` seeds every source
once, flushes `outflow(state)` once, and exits.
[Tutorial 9 -- NASCAR Fantasy Fjord](../../09-nascar-fantasy-fjord/README.md)
is the shipped precedent this appendix follows for that exact one-shot
shape; [Tutorial 6 -- State Sports](../../06-state-sports/README.md) remains
the precedent for a *plain script*, no-fjord one-shot when there's no
multi-source fusion to do.

The six views are cardinality-reducing group-bys (per-owner rollups across
N seasons, per-pair rivalry aggregation, ten records-book extremes, top-N
draft counts). None of that lives in a `conv_dict` -- it's plain Python
inside `outflow.py`'s `outflow(state)`, the same shape
`examples/09-nascar-fantasy-fjord/outflow.py`'s manufacturer leaderboard
already uses: one fold over the live `state["Matchup"]` snapshot melts every
decided game into a `team_games` list (one row per team per game, home and
away perspective), a rivalry-candidate list, and a per-owner
WINNERS_BRACKET-season set, all in a single pass. From there, plain
`defaultdict` buckets and `Counter`s do the rest -- bucket `state["Standing"]`
by `primaryOwner` for Franchise Cards (`sum()`/`round()` inline per bucket),
bucket `team_games` by `(season, week)` for the all-play expected-wins
column, one chronological pass over `team_games` tracks each owner's running
win/loss streak, and `max()`/`min(key=operator.itemgetter(...))` picks over
`team_games` / `state["Standing"]` answer all ten records-book kinds.
`RivalryRow` buckets the fold's rivalry-candidate list by `(owner_a,
owner_b)` the same way. Every one of these is a plain Python data structure
built and read inside `outflow(state)` -- no group-collapsing second
registration of a source under an alternate `inc_code`, no build-time
broadcast pass.

What genuinely needs a read-time join across two DIFFERENT sources with no
seeding-order guarantee -- an owner GUID resolved to `Owner.display_name`,
and the draft-tendency `Counter` folds that need `PlayerName` -- reads
directly off the live `state[...]` snapshots `outflow(state)` is handed each
wave (the `cls.fjord()` daemon path). The owner-GUID lookup is a one-line
dict comprehension built once at the top of `outflow(state)` -- `names =
{o.inc_code: o.display_name for o in owners}`, the same lookup-dict-from-
instances idiom as [Tutorial 6 -- State Sports](../../06-state-sports/README.md)'s
`state_code_map` -- then every view site reads `names.get(owner_guid,
"Unknown")`; `player_names.inc_dict.get(player_id)` stays a direct registry
read since `PlayerName` only needs single-shot per-pick lookups, not a
prebuilt map. That's the return-twin of the six view-building blocks a
plain script would otherwise write inline in `main()`.

`outflow(state)` builds every view exactly once, here -- `main()`'s three
console tables (Franchise Cards, Records Book, First-Overall Honor Roll)
do NOT re-read the exported NDJSON files back afterward; they iterate the
built classes' own registries directly (`FranchiseCard.inc_dict.values()`,
`RecordRow.inc_dict.values()`, `DraftTendencyRow.inc_dict.values()`) after
the `async for wave in Incorporator.fjord(...)` loop ends. This is a step
further than [Tutorial 9 -- NASCAR Fantasy Fjord](../../09-nascar-fantasy-fjord/README.md)
takes -- T9's own `main()` never reads a derived view class back, only
prints the export file paths -- see Section 2 for why that requires loading
`outflow.py` via `load_outflow_module` explicitly rather than a bare
top-level import.

## 6. `payload_list=` and read-time joins

Every sub-collection (`teams`, `schedule`, `members`, `draftDetail.picks`)
is pulled off EVERY fetched `Season` row, flattened into one list spanning
every season in `main()` (each row stamped with `"season": s.season` inline
via dict-comprehension unpacking -- `{**t.model_dump(by_alias=True),
"season": s.season}`), and handed to a sibling fjord source's
`payload_list=` -- a network-free, in-memory passthrough
(`incorporator/base.py`'s payload-only mode): one dict entry in, one row
out, through the full `conv_dict` pipeline, seeded exactly ONCE per class
by `fjord()` rather than once per season. Every unmentioned payload field
auto-coerces through schema inference -- ESPN's own JSON is clean-typed, so
`season_conv_dict` lists only the fields that need a real transform
(`pluck()` for the two nested lists, a handful of settings-evolution
`calc()`s); the framework's dynamic schema builder auto-promotes each
nested dict/list into its own typed submodel (dotted attribute access) --
so `season.teams` is a list of submodel instances, not plain dicts, by the
time `main()` reads it. `t.model_dump(by_alias=True)` flattens each back to
a plain dict before the
`payload_list=` handoff. `by_alias=True` matters specifically for
`rosterSettings.lineupSlotCounts`: its keys are numeric STRINGS (`"0"`,
`"2"`, ..., `"23"`), and an UNDECLARED dict-valued field's auto-promoted
submodel sanitizes those into Python attribute names (`field_0`, `field_2`,
...). `Season.roster_slots` sidesteps this entirely by being a DECLARED
field (`dict[str, int] | None`) on the `Season` class itself --
`infer_dynamic_schema` skips inference for any field already present on the
base class, so the dict's original string keys survive untouched. Its
`season_conv_dict` entry drills the raw `settings.rosterSettings.
lineupSlotCounts` path directly (`calc(dict, ...)`, a plain shallow-copy
coercion), never touching the `settings` field's own (still-inferred,
still key-mangled) submodel. `Season.roster_slots` is a DECLARED field
throughout the whole pipeline, including the fjord's own reseed off
`[s.model_dump(by_alias=True, exclude_unset=True) for s in all_seasons]`
(no `conv_dict` on that reseed at all) -- `model_dump` already carries
whatever `roster_slots` the first pass computed, and a declared field is
copied through untouched rather than re-drilled a second time.

`outflow.py`'s `SettingsRow` -- View 6's derived row class -- does NOT need
the same declared-field treatment: it's bare, like all six derived view
classes (Section 2). Its bare inference does reintroduce one observable
effect at read time: a season whose `lineupSlotCounts` key set is smaller
than another season sharing the SAME flush wave gets its missing keys
null-padded in the exported `settings_evolution.ndjson` -- fjord's own
dynamic-schema inference union-merges an undeclared dict-valued field's keys
across every row `outflow(state)` returns in one wave, the derived-row-level
counterpart of the fan-out-batch union `Season.roster_slots`'s own
declaration sidesteps at the source level. This is accepted inference
behavior, not data loss -- the smaller season's own real values stay present
and correct; only the extra keys another row in the wave carries come back
`null`. Every real season in the live public-899513 run shares the same key
set, so the padding never surfaces there; the appendix's own private-mode
test (`OLD_YEAR`, a 3-key roster sharing a flush wave with an 8-key season)
asserts exactly that shape.

A bare ESPN `team.id` repeats every season -- once every season's `Standing`
rows share one fjord source, a bare `id` would collide in `inc_dict`.
`Standing`'s own `conv_dict` synthesizes a `"{season}:{id}"` composite join
key inline -- `calc("{}:{}".format, "season", "id", target_type=str)`, the
bound-method idiom `docs/api_atlas.md` documents for `calc()` -- and
`Standing`'s `inc_code="team_key"` makes that composite the registry key.
`main()`'s own pre-fjord Python builds the SAME composite as a plain
f-string (`f"{r['season']}:{r['id']}"`) to thread each team's owner GUID
directly onto its sibling schedule/pick rows -- `home_owner_guid`/
`away_owner_guid` on every schedule row, `owner_guid` on every pick row --
BEFORE `Matchup.incorp()`/`DraftPick.incorp()` ever run. Neither source
needs a `conv_dict` entry for that FK at all: both stay empty, letting
every other field auto-infer. What's left for `outflow.py`'s
`outflow(state)` is the join that genuinely can't move upstream -- an
owner GUID resolved to `Owner.display_name` -- since `Owner` is
a sibling `stream_params` entry with no seeding-order guarantee relative to
the sources that reference its GUIDs, so no build-time `link_to` is used
anywhere; see that file's own module docstring.

**`Matchup` is a fjord source exactly once** -- no pre-fjord typed build, no
second group-collapsing registration. `outflow(state)` melts
`state["Matchup"]` into everything team/rivalry-related in a single pass:
for each decided game (`winner != "UNDECIDED"`), it appends one row per
perspective to a local `team_games` list (`m.home.totalPoints` /
`m.away.totalPoints if m.away else 0.0` read directly off the live
snapshot's auto-promoted `home`/`away` submodels -- `m.away is None` on a
playoff bye), a canonicalized rivalry-candidate row (the lower owner GUID is
always side "a"), and, on a `WINNERS_BRACKET` game, both owners' season into
a `defaultdict[str, set]` that answers `playoff_appearances` directly.
Everything else -- Franchise Cards, the all-play expected-wins column,
win/loss streaks, all ten records-book kinds, the rivalry matrix -- is a
plain `defaultdict` bucket or a `max()`/`min(key=operator.itemgetter(...))`
pick over that one `team_games` list, read fresh on every fjord wave.

## 7. Player names: one shared fan-out, not two passes

Draft picks carry only a numeric `playerId` (negative for D/ST, e.g.
`-16003` for a Bears defense) -- resolving a name requires a season-matched
call to ESPN's `players_wl` endpoint (`GET
.../seasons/{S}/players?view=players_wl` with an `X-Fantasy-Filter` header
listing the wanted IDs); old IDs don't resolve against a newer season's
player universe. The pipeline makes exactly ONE `PlayerName.incorp(inc_url=
[...])` call, fanning out over EVERY discovered season's `players_wl` URL
concurrently on one client, sharing ONE `X-Fantasy-Filter` header whose
value is the UNION of every wanted id -- round-1 picks from every season,
plus the all-time top-15 most-drafted `playerId`s (by pick count, any
round), computed once via a single `collections.Counter` pass over every
fetched draft pick. Each season endpoint resolves only the ids it
recognises out of that shared union; `inc_code="id"` dedups the rest across
every URL's response into one `PlayerName.inc_dict`.

Season-matched calls are still genuinely required -- ESPN's player universe
is season-scoped, so this is the one part of the pipeline that can't batch
down below one HTTP request per season -- but sending every season the
SAME shared filter, in ONE `incorp()` call, replaces what used to be up to
two loops of per-season calls. `DraftPick.player` is now a READ-TIME join
inside `outflow.py`'s `outflow(state)` (`player_names.inc_dict.get(p.
playerId)`), not a build-time `link_to`, since `DraftPick` and `PlayerName`
are sibling fjord sources with no ordering guarantee between them (Section
6).

`defaultPositionId` (not `lineupSlotId`) is the verified position source --
`1=QB`, `2=RB`, `3=WR`, `4=TE`, `5=K`, `16=D/ST`, computed ONCE per player at
`PlayerName`'s own build time via `calc(position_name, "defaultPositionId",
...)`; an unmapped id falls back to a labelled placeholder (`POS_<id>`)
rather than crashing. Every read site inside `outflow(state)` then takes
`player.position if player else "UNKNOWN"` against the read-time
`player_names.inc_dict.get(playerId)` lookup -- a single conditional-dot
guard, not a repeated `position_name(...)` call. Some very old or vacated
draft slots resolve to a sentinel `playerId` ESPN doesn't map to a real
player at all -- that row's name/position fall back to
`"Unknown"`/`"UNKNOWN"` gracefully, the same fallback used for any
unresolved lookup.

## Run it

```bash
# Public demo league, no setup required
python examples/appendix/espn-league-history/espn_league_history.py

# Your own league, or a private one
ESPN_LEAGUE_ID=1234567 ESPN_S2=... ESPN_SWID='{...}' \
  python examples/appendix/espn-league-history/espn_league_history.py
```

Also runs in Docker via the
[central mount pattern](../../README.md#running-a-tutorial-in-docker) (not
run or verified) -- pass the three env vars through with `-e`.

## Where to Go Next

| Goal | Read |
|---|---|
| The one-shot `fjord()` shape this appendix follows | [Tutorial 9 -- NASCAR Fantasy Fjord](../../09-nascar-fantasy-fjord/README.md) |
| A plain one-shot script, no fjord, no Watershed | [Tutorial 6 -- State Sports](../../06-state-sports/README.md) |
| Build rows from data already in memory | [Tutorial 5 -- Parent-Child Drilling](../../05-parent-child-drilling/README.md) |
| Universal export formats (`Cls.export(...)`) | [Tutorial 3 -- Universal Formats](../../03-universal-formats/README.md) |
| A read-time multi-source join, as a Tideweaver Fjord current instead | [`crypto-graph-mapping`](../crypto-graph-mapping/README.md) |

***

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/appendix/espn-league-history/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
